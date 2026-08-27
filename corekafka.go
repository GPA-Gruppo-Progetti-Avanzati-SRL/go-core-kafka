package corekafka

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// Ri-esporta i tipi neutri e gli helper dei sub-package, così l'app importa il solo package corekafka.
// (I sub-package restano usabili direttamente per casi avanzati.)
//
// Due seam di business logic, nessun altro:
//   - Handler (modalità handle, at-least-once): business logic LIBERA (l'app fa ciò che vuole — Mongo,
//     SQL, chiamate esterne, ...); l'engine committa gli offset dopo il ritorno nil.
//   - Transformer (modalità transform, EOS Kafka->Kafka): ritorna la lista dei ProducerRecord da
//     produrre, con possibilità di MIX output + deadletter (basta impostare Topic sul topic DLQ per i
//     record "cattivi": vengono prodotti nella stessa transazione EOS).

type (
	// Record è un messaggio consumato in forma neutra.
	Record = message.Record
	// ProducerRecord è un messaggio da produrre (output della modalità transform; il campo Topic
	// permette di instradare, nella stessa lista, sia agli output "buoni" sia a un topic DLQ).
	ProducerRecord = message.ProducerRecord
	// Handler è il contratto della modalità handle (at-least-once): business logic libera nell'app.
	Handler = processor.Handler
	// Transformer è il contratto della modalità EOS Kafka->Kafka.
	Transformer = processor.Transformer
	// PoisonRecords segnala all'engine i record poison da instradare a DLQ.
	PoisonRecords = processor.PoisonRecords
	// PoisonRecord è un record scartato dalla conversione con la SUA causa (vedi Convert).
	PoisonRecord = processor.PoisonRecord
	// KafkaConfig è la connessione Kafka condivisa più il tuning globale di consumer e producer.
	KafkaConfig = spec.KafkaServer
	// ProcessorSpec è la specifica di un singolo processor (una voce di `processors`).
	ProcessorSpec = spec.ProcessorSpec
	// ConsumerTuning è il blocco `consumer`: si scrive in `server.consumer` e ogni processor può
	// sovrascriverne i singoli campi nel proprio blocco omonimo.
	ConsumerTuning = spec.ConsumerTuning
	// ProducerTuning è il blocco `producer`: `server.producer` per il producer condiviso del
	// processo, `processors[].producer` per il transazionale di un transform.
	ProducerTuning = spec.ProducerTuning
	// RestartSpec è il blocco `restart`: la supervisione del loop di consumo.
	RestartSpec = spec.RestartSpec
	// Properties sono le proprietà applicative per-processor (valori con il tipo YAML nativo). Il modo
	// raccomandato per leggerle è il mapping sui campi della struct del processor via tag `prop:`.
	// Da non confondere con `kafka-properties`, che sono le proprietà del client librdkafka.
	// È core.Properties: stesso tipo e stesso meccanismo dei task di go-core-batch.
	Properties = core.Properties
	// Configurable è implementata da Handler/Transformer che vogliono le Properties all'avvio.
	Configurable = processor.Configurable
)

// ErrFailFast: ritornalo (anche wrappato) da Handler/Transformer per forzare il fail-fast (no commit,
// l'app esce → replay), a prescindere dalla policy on-error dello spec.
var ErrFailFast = processor.ErrFailFast

// Converted è l'esito della prima passata su un batch (vedi Convert).
type Converted[T any] = processor.Converted[T]

// Compact / NoCompact sono il flag di compaction di Convert, in forma leggibile al sito di chiamata.
const (
	Compact   = processor.Compact
	NoCompact = processor.NoCompact
)

// Convert è la prima passata su un batch: salta i record vuoti, applica conv a ciascun record,
// separa i poison (con la LORO causa) dai risultati buoni, compatta opzionalmente per chiave Kafka, e
// logga/conta ciò che ha scartato. All'app resta la conversione di un singolo record.
//
//	func (h *Handler) Handle(ctx context.Context, batch []*corekafka.Record) error {
//	    res := corekafka.Convert(ctx, batch, corekafka.Compact, convertEvento)
//	    if err := h.Data.Apply(ctx, res.Items); err != nil {
//	        return err // transiente: niente commit -> replay
//	    }
//	    return res.DeadLetter() // nil se non c'è nessun poison
//	}
func Convert[T any](ctx context.Context, batch []*Record, compact bool, conv func(*Record) ([]T, error)) Converted[T] {
	return processor.Convert(ctx, batch, compact, conv)
}

// DeadLetterEach è l'equivalente per-record di DeadLetter: ogni record porta la sua causa fino
// all'header corekafka-dlq-error. È ciò che usa Converted.DeadLetter().
func DeadLetterEach(poison []PoisonRecord) error { return processor.DeadLetterEach(poison) }

// DeadLetter costruisce l'esito gestito con cui Handler O Transformer chiedono di instradare QUESTI
// record al DLQ. Da ritornare come error da Handle o da Transform: in handle il resto viene committato,
// in transform i record DLQ sono prodotti nella stessa transazione EOS degli output. Richiede un
// deadletter-topic configurato sullo spec.
func DeadLetter(cause error, recs ...*Record) error {
	return processor.DeadLetter(cause, recs...)
}

// BindProps mappa le properties di un processor sui campi di target taggati `prop:` (con `default:` e
// `validate:` per campo). È il meccanismo usato automaticamente da RegisterHandler/RegisterTransformer
// sui campi dell'Handler/Transformer: serve solo per il percorso manuale (un Configure scritto a mano o
// un costruttore passato a ProvideHandler/ProvideTransformer).
func BindProps(target any, props Properties) error { return core.BindProps(target, props) }

// PropertiesFromContext ritorna le Properties del processor corrente (dentro Handle/Transform/Mapper).
func PropertiesFromContext(ctx context.Context) Properties { return spec.PropertiesFromContext(ctx) }

// ConsumerNameFromContext ritorna il nome del processor corrente (la chiave di join con la voce di
// `processors` e con RegisterHandler/RegisterTransformer).
func ConsumerNameFromContext(ctx context.Context) string { return spec.ConsumerNameFromContext(ctx) }

// Costanti della policy on-error (la modalità handle/transform è derivata dalla registrazione, non
// è un valore di config).
const (
	OnErrorDeadletter = spec.OnErrorDeadletter
	OnErrorFailFast   = spec.OnErrorFailFast
)

// RegisterHandler registra un tipo struct T come Handler (modalità handle) per il consumer indicato.
// Va chiamata SOLO dall'interno della funzione passata a Module. In dualità con RegisterTransformer.
func RegisterHandler[T any, PT interface {
	*T
	processor.Handler
}](consumerName string, modes ...string) {
	processor.RegisterHandler[T, PT](consumerName, modes...)
}

// RegisterTransformer registra un tipo struct T come Transformer (modalità EOS) per il consumer
// indicato. Va chiamata SOLO dall'interno della funzione passata a Module.
func RegisterTransformer[T any, PT interface {
	*T
	processor.Transformer
}](consumerName string, modes ...string) {
	processor.RegisterTransformer[T, PT](consumerName, modes...)
}

// ProvideHandler / ProvideTransformer registrano un costruttore fx che ritorna la relativa
// registrazione, per Handler/Transformer con dipendenze non banali.
func ProvideHandler(constructor any, modes ...string) { processor.Provide(constructor, modes...) }
func ProvideTransformer(constructor any, modes ...string) {
	processor.ProvideTransformer(constructor, modes...)
}
