package corekafka

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/batchsink"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// Ri-esporta i tipi neutri e gli helper dei sub-package, così l'app importa il solo package corekafka.
// (I sub-package restano usabili direttamente per casi avanzati.)

type (
	// Record è un messaggio consumato in forma neutra.
	Record = message.Record
	// ProducerRecord è un messaggio da produrre.
	ProducerRecord = message.ProducerRecord
	// Handler è il contratto della modalità sink.
	Handler = processor.Handler
	// Transformer è il contratto della modalità EOS Kafka->Kafka.
	Transformer = processor.Transformer
	// PoisonRecords segnala all'engine i record poison da instradare a DLQ.
	PoisonRecords = processor.PoisonRecords
	// KafkaConfig è la connessione Kafka condivisa.
	KafkaConfig = spec.KafkaConfig
	// ConsumerSpec è la specifica di un singolo consumer.
	ConsumerSpec = spec.ConsumerSpec
	// Properties sono le proprietà applicative per-consumer, lette dalla business logic.
	Properties = spec.Properties
	// Configurable è implementata da Handler/Transformer che vogliono le Properties all'avvio.
	Configurable = processor.Configurable
)

// ErrFailFast: ritornalo (anche wrappato) da Handler/Transformer per forzare il fail-fast (no commit,
// l'app esce → replay), a prescindere dalla policy on-error dello spec.
var ErrFailFast = processor.ErrFailFast

// DeadLetter costruisce l'esito con cui l'handler chiede di instradare QUESTI record al DLQ (e
// committare il resto). Da ritornare come error da Handle. Richiede un deadletter-topic configurato.
func DeadLetter(cause error, recs ...*Record) error {
	return processor.DeadLetter(cause, recs...)
}

// PropertiesFromContext ritorna le Properties del consumer corrente (dentro Handle/Transform/Mapper).
func PropertiesFromContext(ctx context.Context) Properties { return spec.PropertiesFromContext(ctx) }

// ConsumerNameFromContext ritorna il nome del consumer corrente.
func ConsumerNameFromContext(ctx context.Context) string { return spec.ConsumerNameFromContext(ctx) }

// Sink, Mapper, BatchSpooler della modalità sink (alias generici verso batchsink).
type (
	Sink[Op any]         = batchsink.Sink[Op]
	Mapper[Op any]       = batchsink.Mapper[Op]
	BatchSpooler[Op any] = batchsink.BatchSpooler[Op]
)

// Costanti di configurazione.
const (
	ModeSink          = spec.ModeSink
	ModeTransform     = spec.ModeTransform
	OnErrorDeadletter = spec.OnErrorDeadletter
	OnErrorFailFast   = spec.OnErrorFailFast
)

// Register registra un tipo struct T come Handler (modalità sink) per il consumer indicato.
func Register[T any, PT interface {
	*T
	processor.Handler
}](consumerName string, modes ...string) {
	processor.Register[T, PT](consumerName, modes...)
}

// RegisterTransformer registra un tipo struct T come Transformer (modalità EOS) per il consumer.
func RegisterTransformer[T any, PT interface {
	*T
	processor.Transformer
}](consumerName string, modes ...string) {
	processor.RegisterTransformer[T, PT](consumerName, modes...)
}

// RegisterSink wira un BatchSpooler[Op] come Handler: il Mapper è dell'app, il Sink[Op] è iniettato
// (es. da mongospooler.Module passato a WithSink).
func RegisterSink[Op any](consumerName string, mapper Mapper[Op], modes ...string) {
	batchsink.Register[Op](consumerName, mapper, modes...)
}

// ProvideHandler / ProvideTransformer registrano un costruttore fx che ritorna la relativa
// registrazione, per Handler/Transformer con dipendenze non banali.
func ProvideHandler(constructor any, modes ...string) { processor.Provide(constructor, modes...) }
func ProvideTransformer(constructor any, modes ...string) {
	processor.ProvideTransformer(constructor, modes...)
}
