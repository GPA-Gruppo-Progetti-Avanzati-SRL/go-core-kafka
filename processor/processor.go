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
func RegisterHandler[T any, PT interface {
	*T
	Handler
}](consumerName string, modes ...string) {
	Provide(func(p T) HandlerRegistration {
		pp := PT(&p)
		return HandlerRegistration{Consumer: consumerName, Handler: pp}
	}, modes...)
}

// RegisterTransformer è l'analogo di RegisterHandler per la modalità EOS: T deve implementare Transformer.
func RegisterTransformer[T any, PT interface {
	*T
	Transformer
}](consumerName string, modes ...string) {
	ProvideTransformer(func(p T) TransformerRegistration {
		pp := PT(&p)
		return TransformerRegistration{Consumer: consumerName, Transformer: pp}
	}, modes...)
}
