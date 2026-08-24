// Package processor definisce i due seam di business logic di go-core-kafka — Handler (modalità handle)
// e Transformer (modalità EOS Kafka->Kafka) — e l'infrastruttura di registrazione via fx value group,
// modellata su go-core-batch (scheduler/registry.go, distributedjob/runner/runner.go). L'engine
// costruisce le mappe consumerName->Handler/Transformer consumando i due gruppi, quindi l'ordine di
// registrazione è indifferente.
package processor

import (
	"context"
	"errors"
	"fmt"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Gruppi fx in cui confluiscono le registrazioni.
const (
	HandlerGroup     = "kafka_handlers"
	TransformerGroup = "kafka_transformers"
)

// Handler è il contratto della modalità handle (at-least-once). Riceve un batch di record già pollati;
// NON committa gli offset (lo fa l'engine dopo il ritorno). Ritorno nil -> l'engine committa; errore
// -> l'engine applica la policy on-error del consumer (deadletter | fail-fast).
type Handler interface {
	Handle(ctx context.Context, batch []*message.Record) error
}

// Transformer è il contratto della modalità EOS Kafka->Kafka. Mappa il batch consumato nei record da
// produrre. L'engine produce e committa gli offset consumati nella STESSA transazione. Modello a esiti
// UNIFORME con Handler: ritorno (out, nil) -> produce out + commit; (out, *PoisonRecords via DeadLetter)
// -> produce out E instrada i record poison al deadletter-topic, tutto nella stessa transazione EOS,
// poi commit; (_, ErrFailFast) -> abort + replay; (_, altro errore) -> policy on-error dello spec.
type Transformer interface {
	Transform(ctx context.Context, batch []*message.Record) ([]*message.ProducerRecord, error)
}

// PoisonRecords, se ritornato da Handler.Handle, segnala all'engine che QUESTI specifici record sono
// "poison" (es. errore di parsing deterministico) mentre il resto del batch è stato elaborato con
// successo. L'engine instrada i record al DLQ (policy deadletter) — o esce (fail-fast) — e poi
// committa gli offset del batch. Un qualsiasi altro errore ritornato da Handle è invece trattato come
// transiente (es. sink irraggiungibile): l'engine NON committa e forza il replay.
type PoisonRecords struct {
	Records []*message.Record
	Cause   error
}

func (e *PoisonRecords) Error() string {
	if e.Cause != nil {
		return "poison records: " + e.Cause.Error()
	}
	return "poison records"
}

func (e *PoisonRecords) Unwrap() error { return e.Cause }

// ErrFailFast, se ritornato (anche wrappato) da Handler/Transformer, forza il fail-fast: l'engine NON
// committa e fa uscire l'applicazione (replay al riavvio), indipendentemente dalla policy on-error
// dello spec. Permette all'handler di scegliere fail-fast caso per caso.
var ErrFailFast = errors.New("corekafka: fail-fast requested by handler")

// DeadLetter costruisce l'errore *PoisonRecords con cui Handler O Transformer chiedono all'engine di
// instradare QUESTI record al DLQ (e committare/produrre il resto), a prescindere dalla policy
// on-error. In modalità handle il DLQ passa dal Producer condiviso; in modalità transform i record DLQ
// sono prodotti nella stessa transazione EOS. Richiede un deadletter-topic configurato sullo spec;
// altrimenti l'engine ripiega su fail-fast (nessuna perdita silenziosa).
func DeadLetter(cause error, recs ...*message.Record) *PoisonRecords {
	return &PoisonRecords{Records: recs, Cause: cause}
}

// Configurable è implementata opzionalmente da un Handler/Transformer che vuole ricevere le
// Properties del proprio consumer all'avvio (per precompute/validazione). Se implementata, l'engine
// chiama Configure dopo il binding nome→handler; un errore fa fail-fast (l'app non parte).
type Configurable interface {
	Configure(props core.Properties) error
}

// HandlerRegistration lega un Handler al nome del consumer (ConsumerSpec.Name).
type HandlerRegistration struct {
	Consumer string
	Handler  Handler
}

// TransformerRegistration lega un Transformer al nome del consumer (ConsumerSpec.Name).
type TransformerRegistration struct {
	Consumer    string
	Transformer Transformer
}

// Provide registra un costruttore che ritorna una HandlerRegistration nel value group kafka_handlers.
// Il costruttore può dichiarare qualunque dipendenza fx-iniettabile. modes opzionale (mode-gating).
func Provide(constructor any, modes ...string) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+HandlerGroup+`"`)), modes...)
}

// ProvideTransformer è l'analogo di Provide per il gruppo kafka_transformers.
func ProvideTransformer(constructor any, modes ...string) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+TransformerGroup+`"`)), modes...)
}

// RegisterHandler registra un tipo struct T come Handler per il consumer indicato. T deve
// implementare Handler (via receiver a puntatore) e dichiarare i suoi campi con i tag di go-core-app:
//
//	`inject:""` / `inject:"nome"` / `from:"gruppo"`  → dipendenza iniettata da fx
//	`prop:"chiave"`                                   → property del consumer (blocco `properties:`)
//	nessun tag                                        → campo di lavorazione, ignorato dal grafo
//
// Stesso idioma di runner.Register di go-core-batch; in dualità con RegisterTransformer.
//
//	func Register() {
//	    processor.RegisterHandler[myHandler]("condizione")
//	}
//
//	corekafka.Module(cfg, Register)
//
//	type myHandler struct {
//	    Svc        mypkg.IService `inject:""`
//	    Collection string         `prop:"collection" validate:"required"`
//	}
//	func (h *myHandler) Handle(ctx context.Context, batch []*message.Record) error { ... }
//
// Va chiamata SOLO dall'interno della funzione passata a Module (vedi Apply): panica altrimenti. Il
// costruttore (e quindi l'intero sotto-grafo di dipendenze di T — es. un data layer Mongo) viene
// fornito a fx SOLO se il consumer è attivo nella lista `consumers` di config. Un consumer
// disabilitato/assente non fa costruire nulla: le sue dipendenze non entrano nel grafo fx e non
// vengono mai connesse.
func RegisterHandler[T any, PT interface {
	*T
	Handler
}](consumerName string, modes ...string) {
	provideIfActive(consumerName, func(s spec.ConsumerSpec) {
		core.ProvideStruct(func(p *T) HandlerRegistration {
			return HandlerRegistration{Consumer: consumerName, Handler: PT(p)}
		}, owner(consumerName), s.Properties, HandlerGroup, modes...)
	})
}

// RegisterTransformer è l'analogo di RegisterHandler per la modalità EOS: T deve implementare Transformer.
func RegisterTransformer[T any, PT interface {
	*T
	Transformer
}](consumerName string, modes ...string) {
	provideIfActive(consumerName, func(s spec.ConsumerSpec) {
		core.ProvideStruct(func(p *T) TransformerRegistration {
			return TransformerRegistration{Consumer: consumerName, Transformer: PT(p)}
		}, owner(consumerName), s.Properties, TransformerGroup, modes...)
	})
}

// owner è l'etichetta con cui core.ProvideStruct contestualizza i suoi errori (dipendenza mancante,
// property non valida): senza, fx riporterebbe solo `reflect.makeFuncStub`.
func owner(consumerName string) string {
	return fmt.Sprintf("corekafka: consumer %q", consumerName)
}

// --- Apply: fornisce a fx solo i processor dei consumer ATTIVI --------------------------------------
//
// Il problema: fx costruisce EAGERLY tutti i membri di un value group (kafka_handlers/kafka_transformers)
// per poterlo iniettare nell'engine. Fornire direttamente ogni costruttore Handler/Transformer farebbe
// quindi costruire l'intero sotto-grafo di dipendenze di OGNI processor registrato (anche dei consumer
// disabilitati) — es. il LinkedService Mongo, che nel suo OnStart apre la connessione. Risultato: Mongo
// si connette anche a consumer tutti spenti.
//
// La soluzione: corekafka.Module riceve il riferimento alla funzione di registrazione dell'app (che
// chiama RegisterHandler/RegisterTransformer) e la invoca lui stesso dentro Apply, che nel frattempo sa
// già quali consumer sono attivi (da config). RegisterHandler/RegisterTransformer consultano
// l'insieme attivo corrente (activeConsumers, valido SOLO durante l'esecuzione sincrona di Apply) per
// decidere se fornire subito il costruttore a fx. Nessuna finestra temporale tra registrazione e
// applicazione: la funzione di registrazione gira sincronamente dentro Apply, sempre nello stesso
// punto in cui l'app chiama Module — non prima (init) né dopo (main).
// activeConsumers mappa nome->spec dei consumer attivi: serve lo spec (non il solo nome) perché il
// wrapper di registrazione mappa le sue Properties sui campi `prop:` del processor (core.BindProps).
var activeConsumers map[string]spec.ConsumerSpec // valido solo durante l'esecuzione sincrona di Apply; nil altrimenti

func provideIfActive(consumerName string, provide func(spec.ConsumerSpec)) {
	if activeConsumers == nil {
		panic("corekafka: RegisterHandler/RegisterTransformer chiamata fuori dalla funzione passata a Module")
	}
	if s, ok := activeConsumers[consumerName]; ok {
		provide(s)
		return
	}
	log.Info().Str("consumer", consumerName).Msg("corekafka: processor registrato ma consumer non attivo in config: costruzione saltata (dipendenze non istanziate)")
}

// Apply chiama register() con l'insieme dei consumer attivi disponibile a RegisterHandler/
// RegisterTransformer: le chiamate al loro interno forniscono a fx solo i processor attivi. Chiamata
// una sola volta da corekafka.Module.
func Apply(register func(), active map[string]spec.ConsumerSpec, modes []string) {
	if !core.IsMode(modes...) {
		return // sottosistema non attivo in questo Mode: non fornire nulla
	}
	activeConsumers = active
	defer func() { activeConsumers = nil }()
	register()
}
