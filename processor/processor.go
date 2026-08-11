// Package processor definisce i due seam di business logic di go-core-kafka — Handler (modalità handle)
// e Transformer (modalità EOS Kafka->Kafka) — e l'infrastruttura di registrazione via fx value group,
// modellata su go-core-batch (scheduler/registry.go, distributedjob/runner/runner.go). L'engine
// costruisce le mappe consumerName->Handler/Transformer consumando i due gruppi, quindi l'ordine di
// registrazione è indifferente.
package processor

import (
	"context"
	"errors"

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
	Configure(props spec.Properties) error
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

// RegisterHandler registra un tipo struct T come Handler per il consumer indicato. T deve incorporare
// core.In (per la dependency injection) e implementare Handler (via receiver a puntatore). Stesso
// idioma di runner.Register di go-core-batch; in dualità con RegisterTransformer.
//
//	func init() { processor.RegisterHandler[myHandler]("condizione") }
//
//	type myHandler struct {
//	    core.In
//	    Svc mypkg.IService
//	}
//	func (h *myHandler) Handle(ctx context.Context, batch []*message.Record) error { ... }
//
// COSTRUZIONE LAZY: la registrazione NON fornisce subito il costruttore a fx. Il costruttore (e quindi
// l'intero sotto-grafo di dipendenze di T — es. un data layer Mongo) viene fornito SOLO se il consumer
// è attivo nella lista `consumers` di config (vedi Configure). Un consumer disabilitato/assente non fa
// costruire nulla: le sue dipendenze non entrano nel grafo fx e non vengono mai connesse.
func RegisterHandler[T any, PT interface {
	*T
	Handler
}](consumerName string, modes ...string) {
	registerLazy(HandlerGroup, consumerName, func() {
		Provide(func(p T) HandlerRegistration {
			pp := PT(&p)
			return HandlerRegistration{Consumer: consumerName, Handler: pp}
		}, modes...)
	}, modes)
}

// RegisterTransformer è l'analogo di RegisterHandler per la modalità EOS: T deve implementare Transformer.
// Stessa costruzione lazy: il Transformer è fornito a fx solo se il consumer è attivo.
func RegisterTransformer[T any, PT interface {
	*T
	Transformer
}](consumerName string, modes ...string) {
	registerLazy(TransformerGroup, consumerName, func() {
		ProvideTransformer(func(p T) TransformerRegistration {
			pp := PT(&p)
			return TransformerRegistration{Consumer: consumerName, Transformer: pp}
		}, modes...)
	}, modes)
}

// --- Registry lazy: fornisce a fx solo i processor dei consumer ATTIVI ------------------------------
//
// Il problema: fx costruisce EAGERLY tutti i membri di un value group (kafka_handlers/kafka_transformers)
// per poterlo iniettare nell'engine. Fornire direttamente ogni costruttore Handler/Transformer farebbe
// quindi costruire l'intero sotto-grafo di dipendenze di OGNI processor registrato (anche dei consumer
// disabilitati) — es. il LinkedService Mongo, che nel suo OnStart apre la connessione. Risultato: Mongo
// si connette anche a consumer tutti spenti.
//
// La soluzione: RegisterHandler/RegisterTransformer accodano una registrazione "lazy" nel registry;
// corekafka.Module calcola l'insieme dei consumer attivi (da config) e chiama Configure. Solo i processor
// dei consumer attivi vengono forniti a fx (core.Provide nel value group). È order-independent: la
// materializzazione parte da qualunque delle due — registrazione o Configure — arrivi per ultima.

type lazyReg struct {
	consumer string
	provide  func() // fornisce a fx il costruttore annotato per il value group
	done     bool   // già fornito o già scartato (idempotenza)
}

var (
	handlerRegs     []*lazyReg
	transformerRegs []*lazyReg
	configured      bool            // Configure è già stata chiamata
	activeConsumers map[string]bool // consumer attivi (presenti in config e non disabled)
	subsystemModes  []string        // modes del sottosistema Kafka (WithModes)
)

func registerLazy(group, consumer string, provide func(), _ []string) {
	r := &lazyReg{consumer: consumer, provide: provide}
	if group == HandlerGroup {
		handlerRegs = append(handlerRegs, r)
	} else {
		transformerRegs = append(transformerRegs, r)
	}
	if configured {
		tryProvide(r)
	}
}

// Configure comunica al registry quali consumer sono attivi (e i modes del sottosistema). Chiamata da
// corekafka.Module. Fornisce a fx i processor già registrati per i consumer attivi; le registrazioni
// che arrivano dopo si forniscono da sole in registerLazy (order-independent). Tutto in init()/main
// single-thread, prima di core.Run: nessun problema di concorrenza.
func Configure(active map[string]bool, modes []string) {
	activeConsumers = active
	subsystemModes = modes
	configured = true
	if !core.IsMode(modes...) {
		return // sottosistema non attivo in questo Mode: non fornire nulla
	}
	for _, r := range handlerRegs {
		tryProvide(r)
	}
	for _, r := range transformerRegs {
		tryProvide(r)
	}
}

func tryProvide(r *lazyReg) {
	if r.done || !core.IsMode(subsystemModes...) {
		return
	}
	r.done = true
	if activeConsumers[r.consumer] {
		r.provide()
		return
	}
	log.Info().Str("consumer", r.consumer).Msg("corekafka: processor registrato ma consumer non attivo in config: costruzione saltata (dipendenze non istanziate)")
}
