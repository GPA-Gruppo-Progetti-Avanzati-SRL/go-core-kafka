// Package processor definisce i due seam di business logic di go-core-kafka — Handler (modalità handle)
// e Transformer (modalità EOS Kafka->Kafka) — e l'infrastruttura di registrazione via fx value group,
// modellata su go-core-batch (scheduler/registry.go, distributedjob/runner/runner.go). L'engine
// costruisce le mappe nomeProcessor->Handler/Transformer consumando i due gruppi, quindi l'ordine di
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
)

// Gruppi fx in cui confluiscono le registrazioni. Non esportati: l'unico sito di registrazione è
// RegisterHandler/RegisterTransformer, quindi fuori da qui il nome del gruppo non serve a nessuno —
// serviva alle vecchie Provide/ProvideTransformer, che annotavano a mano un costruttore dell'app.
// consumer.params li nomina come stringhe letterali nei tag `group:`, che è l'unica forma che un tag
// di struct ammette.
const (
	handlerGroup     = "kafka_handlers"
	transformerGroup = "kafka_transformers"
)

// Handler è il contratto della modalità handle (at-least-once). Riceve un batch di record già pollati;
// NON committa gli offset (lo fa l'engine dopo il ritorno). Ritorno nil -> l'engine committa; errore
// -> l'engine applica la policy `consumer.on-error` del processor (deadletter | fail-fast).
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
	// Causes, se valorizzata, porta la causa del SINGOLO record (stessa lunghezza e stesso ordine di
	// Records): è quella a finire nell'header corekafka-dlq-error di quel record. Un elemento nil, o
	// una Causes nil, fa ricadere il record su Cause. La costruisce DeadLetterEach (e quindi Convert);
	// DeadLetter(cause, recs...) la lascia nil, etichettando tutto il gruppo con l'unica Cause.
	Causes []error
	Cause  error
}

// CauseFor ritorna la causa da attribuire all'i-esimo record: quella specifica se presente, altrimenti
// la causa comune del gruppo.
func (e *PoisonRecords) CauseFor(i int) error {
	if i >= 0 && i < len(e.Causes) && e.Causes[i] != nil {
		return e.Causes[i]
	}
	return e.Cause
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
// Properties del proprio processor all'avvio (per precompute/validazione). Se implementata, l'engine
// chiama Configure dopo il binding nome→handler; un errore fa fail-fast (l'app non parte).
type Configurable interface {
	Configure(props core.Properties) error
}

// HandlerRegistration lega un Handler al nome del processor (ProcessorSpec.Name).
type HandlerRegistration struct {
	Consumer string
	Handler  Handler
}

// TransformerRegistration lega un Transformer al nome del processor (ProcessorSpec.Name).
type TransformerRegistration struct {
	Consumer    string
	Transformer Transformer
}

// RegisterHandler registra un tipo struct T come Handler per il processor indicato. T deve
// implementare Handler (via receiver a puntatore) e dichiarare i suoi campi con i tag di go-core-app:
//
//	`inject:""` / `inject:"nome"` / `from:"gruppo"`  → dipendenza iniettata da fx
//	`prop:"chiave"`                                   → property del processor (blocco `properties:`)
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
// fornito a fx SOLO se il processor è attivo nella lista `processors` di config. Un processor
// disabilitato/assente non fa costruire nulla: le sue dipendenze non entrano nel grafo fx e non
// vengono mai connesse.
//
// modes limita QUESTO processor ai core.Mode indicati; vuoto = attivo in ogni mode. È il gate
// per-processor, che sta fra corekafka.WithModes (spegne l'intero sottosistema) e `disabled:` in
// config (spegne il processor in ogni mode): serve quando un solo YAML alimenta più processi dello
// stesso deployment, uno per MODE, e ognuno consuma i propri topic. Un processor escluso da qui è
// DISATTIVATO come se fosse `disabled: true` — niente costruttore e niente runner, con un log Info che
// dice perché — e non un errore di avvio.
//
//	processor.RegisterHandler[myHandler]("condizione", engine.Worker)
func RegisterHandler[T any, PT interface {
	*T
	Handler
}](consumerName string, modes ...string) {
	provideIfActive(consumerName, modes, func(s spec.ProcessorSpec) {
		core.ProvideStruct(func(p *T) HandlerRegistration {
			return HandlerRegistration{Consumer: consumerName, Handler: PT(p)}
		}, owner(consumerName), s.Properties, handlerGroup)
	})
}

// RegisterTransformer è l'analogo di RegisterHandler per la modalità EOS: T deve implementare
// Transformer. Stessa semantica dei modes.
func RegisterTransformer[T any, PT interface {
	*T
	Transformer
}](consumerName string, modes ...string) {
	provideIfActive(consumerName, modes, func(s spec.ProcessorSpec) {
		core.ProvideStruct(func(p *T) TransformerRegistration {
			return TransformerRegistration{Consumer: consumerName, Transformer: PT(p)}
		}, owner(consumerName), s.Properties, transformerGroup)
	})
}

// owner è l'etichetta con cui core.ProvideStruct contestualizza i suoi errori (dipendenza mancante,
// property non valida): senza, fx riporterebbe solo `reflect.makeFuncStub`.
func owner(consumerName string) string {
	return fmt.Sprintf("corekafka: processor %q", consumerName)
}

// --- Apply: fornisce a fx solo i processor ATTIVI ---------------------------------------------------
//
// Il problema: fx costruisce EAGERLY tutti i membri di un value group (kafka_handlers/kafka_transformers)
// per poterlo iniettare nell'engine. Fornire direttamente ogni costruttore Handler/Transformer farebbe
// quindi costruire l'intero sotto-grafo di dipendenze di OGNI processor registrato (anche di quelli
// disabilitati) — es. il LinkedService Mongo, che nel suo OnStart apre la connessione. Risultato: Mongo
// si connette anche con tutti i processor spenti.
//
// La soluzione: corekafka.Module riceve il riferimento alla funzione di registrazione dell'app (che
// chiama RegisterHandler/RegisterTransformer) e la invoca lui stesso dentro Apply, che nel frattempo sa
// già quali processor sono attivi (da config). RegisterHandler/RegisterTransformer consultano
// l'insieme attivo corrente (activeConsumers, valido SOLO durante l'esecuzione sincrona di Apply) per
// decidere se fornire subito il costruttore a fx. Nessuna finestra temporale tra registrazione e
// applicazione: la funzione di registrazione gira sincronamente dentro Apply, sempre nello stesso
// punto in cui l'app chiama Module — non prima (init) né dopo (main).
// activeConsumers mappa nome->spec dei processor attivi: serve lo spec (non il solo nome) perché il
// wrapper di registrazione mappa le sue Properties sui campi `prop:` del processor (core.BindProps).
var activeConsumers map[string]spec.ProcessorSpec // valido solo durante l'esecuzione sincrona di Apply; nil altrimenti

// excludedByMode raccoglie i processor che il register ha escluso dal core.Mode corrente. Apply lo
// riporta a corekafka.Module, che li sottrae dalla lista degli spec: senza quel ritorno il processor
// resterebbe nella lista dei runner mentre il suo costruttore non e' stato fornito, e
// consumer.newRunner cadrebbe nel ramo "nessun processor registrato" — cioe' i modes qui erano l'unico
// gate capace di spegnere un lato solo dei due, e ogni valore non vuoto rompeva il boot.
var excludedByMode []string // valido solo durante l'esecuzione sincrona di Apply; nil altrimenti

// provideIfActive fornisce il costruttore del processor solo se e' attivo, dove "attivo" e' la
// congiunzione delle due condizioni che stanno in posti diversi: presente e non `disabled` in config
// (lo ha gia' deciso Config.ActiveProcessors, ed e' cio' che activeConsumers contiene) e ammesso dai
// modes passati al register. Il secondo caso non e' un errore: il processor viene DISATTIVATO, come se
// fosse `disabled: true`.
func provideIfActive(consumerName string, modes []string, provide func(spec.ProcessorSpec)) {
	if activeConsumers == nil {
		panic("corekafka: RegisterHandler/RegisterTransformer chiamata fuori dalla funzione passata a Module")
	}
	s, ok := activeConsumers[consumerName]
	if !ok {
		log.Info().Str("consumer", consumerName).Msg("corekafka: processor registrato ma consumer non attivo in config: costruzione saltata (dipendenze non istanziate)")
		return
	}
	if !core.IsMode(modes...) {
		excludedByMode = append(excludedByMode, consumerName)
		log.Info().Str("consumer", consumerName).Strs("modes", modes).
			Msg("corekafka: processor non attivo in questo MODE, non attivato (dipendenze non istanziate)")
		return
	}
	provide(s)
}

// Apply chiama register() con l'insieme dei processor attivi disponibile a RegisterHandler/
// RegisterTransformer: le chiamate al loro interno forniscono a fx solo i processor attivi. Chiamata
// una sola volta da corekafka.Module.
//
// Ritorna i nomi dei processor che il register ha escluso col proprio gating per mode, perche' sono
// l'unica parte della decisione che corekafka.Module non puo' conoscere da solo: la sua lista viene
// dalla config, i modes stanno nel codice di registrazione. Module li sottrae, e da li' in poi i due
// lati — costruttori e runner — dicono la stessa cosa.
//
// Il `modes` di questa funzione e' invece quello del SOTTOSISTEMA (corekafka.WithModes): se non
// corrisponde non si registra nulla, quindi non c'e' nemmeno una lista da sottrarre.
func Apply(register func(), active map[string]spec.ProcessorSpec, modes []string) []string {
	if !core.IsMode(modes...) {
		return nil // sottosistema non attivo in questo Mode: non fornire nulla
	}
	activeConsumers = active
	excludedByMode = nil
	defer func() { activeConsumers, excludedByMode = nil, nil }()
	register()
	return excludedByMode
}
