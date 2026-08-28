// Package producer è il producer Kafka del processo: quello con cui l'engine alimenta il DLQ della
// modalità handle e quello che l'app inietta per pubblicare. È sottile — avvolge un driver.Producer o
// un driver.TxProducer — così nessun tipo del client concreto compare nelle firme.
//
// Il wiring NON sta qui ma in corekafka (ProducerModule, e WithProducer per un'app che ha anche
// consumer): la Config è una sola per tutto go-core-kafka, e il posto in cui la si nomina è il
// package che la dichiara. Qui restano il contratto (IProducer) e i costruttori.
//
// Dove è registrato decide chi lo vede: dentro il core.ModuleClosed di corekafka.Module è privato al
// sottosistema (il DLQ, che l'app non deve poter usare per pubblicare — quello è il compito di un
// Transformer, l'unico seam con garanzie EOS); registrato fuori da ProducerModule/WithProducer è il
// producer dell'app.
package producer

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// abortTimeout limita l'abort di ripiego di TxProducer.Produce. Non è un knob di config: è la rete di
// sicurezza di un percorso di errore, e un valore configurabile qui darebbe da scegliere su qualcosa
// che nessuno può tarare meglio di così.
const abortTimeout = 10 * time.Second

// Ambit e codice del solo ApplicationError prodotto da corekafka: tutto il resto della
// libreria ritorna error, perché l'engine non risponde a un client HTTP ma decide se
// committare, replayare o ricostruire il consumer (vedi driver.Severity).
const (
	Ambit       = "go-core-kafka"
	CodeProduce = "KAFKA-PRODUCE" // Produce (o attesa dei delivery report) fallita
)

// IProducer è il contratto del producer: è QUESTO che si inietta, non la struct.
//
// È un'interfaccia per due ragioni che non sono di stile. La prima: il campo interno di Producer è un
// internal/driver.Producer, quindi fuori da go-core-kafka la struct non è costruibile con un fake — un
// consumatore come il job di notifica di go-core-batch non sarebbe testabile. La seconda: le due forme
// (idempotente e transazionale) sono lo stesso contratto per il chiamante, e quale sia in esercizio lo
// decide la config (`server.producer.transactional-id`), non il codice che produce.
type IProducer interface {
	// Produce invia i record e attende l'esito. Nella forma transazionale ogni chiamata è UNA
	// transazione: i record diventano visibili ai consumer read_committed tutti o nessuno.
	Produce(ctx context.Context, recs []*message.ProducerRecord) *core.ApplicationError
	// ProduceTo è Produce con il topic di destinazione per i record che non ne portano uno: serve a
	// chi pubblica su un topic deciso a runtime (una property di configurazione, il campo di un
	// job) e non vuole ripeterlo su ogni record. Un Topic già impostato sul record NON viene
	// sovrascritto, così resta possibile il fan-out nella stessa chiamata.
	ProduceTo(ctx context.Context, topic string, recs []*message.ProducerRecord) *core.ApplicationError
}

// Producer è il producer idempotente (non transazionale).
type Producer struct {
	d driver.Producer
}

// Produce invia i record e attende i delivery report.
func (p *Producer) Produce(ctx context.Context, recs []*message.ProducerRecord) *core.ApplicationError {
	if err := p.d.Produce(ctx, recs); err != nil {
		return core.TechnicalError().WithAmbit(Ambit).WithCode(CodeProduce).WithCause(err)
	}
	return nil
}

// ProduceTo vedi IProducer.ProduceTo.
func (p *Producer) ProduceTo(ctx context.Context, topic string, recs []*message.ProducerRecord) *core.ApplicationError {
	return p.Produce(ctx, withTopic(topic, recs))
}

// TxProducer è il producer transazionale: apre e chiude una transazione per ogni Produce. Lo
// costruisce chi ha scritto `server.producer.transactional-id`.
//
// Non c'è mutex: un producer transazionale ha UNA transazione per volta, quindi due Produce
// concorrenti non sono un problema di lock ma di semantica — la seconda entrerebbe nella transazione
// della prima e ne condividerebbe l'esito. Il chiamante previsto è un job (un tick per volta); se
// servisse la concorrenza servirebbero più producer con id diversi, non una sezione critica che
// serializza in silenzio.
type TxProducer struct {
	d driver.TxProducer
}

// Produce apre una transazione, invia i record, attende l'esito e committa. Su qualsiasi errore
// abortisce: i record prodotti non diventano visibili, e il chiamante può ritentare l'intero gruppo.
func (p *TxProducer) Produce(ctx context.Context, recs []*message.ProducerRecord) *core.ApplicationError {
	if len(recs) == 0 {
		return nil
	}
	if err := p.d.Begin(ctx); err != nil {
		return core.TechnicalError().WithAmbit(Ambit).WithCode(CodeProduce).WithCause(err)
	}
	if err := p.d.Produce(ctx, recs); err != nil {
		p.abort(ctx)
		return core.TechnicalError().WithAmbit(Ambit).WithCode(CodeProduce).WithCause(err)
	}
	if err := p.d.Commit(ctx); err != nil {
		// L'abort DOPO un commit fallito non è ridondante: se il commit non è andato a buon fine la
		// transazione è ancora aperta lato broker, e lasciarla tale blocca i consumer read_committed
		// su quelle partizioni fino al transaction.timeout.ms.
		p.abort(ctx)
		return core.TechnicalError().WithAmbit(Ambit).WithCode(CodeProduce).WithCause(err)
	}
	return nil
}

// ProduceTo vedi IProducer.ProduceTo.
func (p *TxProducer) ProduceTo(ctx context.Context, topic string, recs []*message.ProducerRecord) *core.ApplicationError {
	return p.Produce(ctx, withTopic(topic, recs))
}

// abort usa un context PROPRIO e non quello del chiamante: quando si arriva qui il motivo è spesso una
// deadline scaduta, e su un context già cancellato l'abort non partirebbe — lasciando aperta la
// transazione che stiamo cercando di chiudere. L'esito è loggato dal driver, e non esiste
// un'alternativa che il chiamante possa scegliere se anche l'abort fallisce.
func (p *TxProducer) abort(ctx context.Context) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	if err := p.d.Abort(actx); err != nil {
		log.Warn().Err(err).Msg("corekafka: abort della transazione fallito")
	}
}

// withTopic imposta il topic sui soli record che non ne hanno uno. Ritorna la stessa slice: i record
// sono costruiti dal chiamante per questa chiamata, quindi copiarli sarebbe una copia per nessuno.
func withTopic(topic string, recs []*message.ProducerRecord) []*message.ProducerRecord {
	if topic == "" {
		return recs
	}
	for _, r := range recs {
		if r.Topic == "" {
			r.Topic = topic
		}
	}
	return recs
}

// NewProducer costruisce il producer idempotente dalla Factory iniettata e registra la chiusura nel
// lifecycle fx. Una chiave riservata in kafka-properties fa fallire l'avvio (vedi
// spec.DeniedKafkaProperties).
func NewProducer(lc fx.Lifecycle, f driver.Factory, k spec.KafkaServer, p spec.ProducerTuning) (*Producer, error) {
	// Stessa validazione dell'engine, dalla stessa funzione: il Producer è registrabile anche da solo
	// (senza consumer), quindi non può appoggiarsi al fatto che qualcun altro l'abbia già fatta.
	if err := spec.ValidateServer(k); err != nil {
		return nil, err
	}
	d, err := f.NewProducer(k, p.WithDefaults())
	if err != nil {
		return nil, err
	}
	closeOnStop(lc, d)
	return &Producer{d: d}, nil
}

// NewTxProducer costruisce il producer transazionale. L'id arriva dal tuning
// (`server.producer.transactional-id`): è la sua presenza a far scegliere questa forma, quindi qui è
// una precondizione e non un parametro — se manca, il chiamante costruisce NewProducer.
func NewTxProducer(lc fx.Lifecycle, f driver.Factory, k spec.KafkaServer, p spec.ProducerTuning) (*TxProducer, error) {
	if err := spec.ValidateServer(k); err != nil {
		return nil, err
	}
	p = p.WithDefaults()
	d, err := f.NewTxProducer(k, p, p.TransactionalID)
	if err != nil {
		return nil, err
	}
	closeOnStop(lc, d)
	return &TxProducer{d: d}, nil
}

// closeOnStop registra la chiusura del client nel lifecycle. È condivisa dai due costruttori perché
// la disciplina è la stessa e l'unica cosa che cambia è il tipo — che qui interessa solo per il suo
// Close.
func closeOnStop(lc fx.Lifecycle, c interface{ Close() error }) {
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { return c.Close() },
	})
}
