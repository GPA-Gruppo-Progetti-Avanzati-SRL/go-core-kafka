package franzdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// txnProduceClient è il producer transazionale di franz-go visto dal driver (lo implementa
// *kgo.Client con l'opzione TransactionalID). È un'interfaccia per la stessa ragione di produceClient
// e txnClient: la disciplina di apertura/chiusura è logica nostra e va testata senza broker.
//
// Nota la differenza di firma rispetto a txnClient (GroupTransactSession): qui End ritorna il solo
// error, perché senza consumer group non esiste il caso "abortita per rebalance".
type txnProduceClient interface {
	BeginTransaction() error
	EndTransaction(ctx context.Context, commit kgo.TransactionEndTry) error
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Flush(ctx context.Context) error
	Close()
}

// txProducer implementa driver.TxProducer: il producer TRANSAZIONALE del processo, che produce senza
// consumare. Da non confondere con transactSession, che è l'EOS Kafka→Kafka e lega gli offset
// consumati all'esito della transazione: qui non c'è consumo, e la garanzia è che i record di un
// Produce diventino visibili ai consumer read_committed tutti o nessuno.
//
// franz-go fa la InitProducerID da sé al primo BeginTransaction, con i propri retry: non c'è una init
// separata da limitare (init-transactions-timeout è segnalato come non supportato al boot).
type txProducer struct {
	cl           txnProduceClient
	flushTimeout time.Duration
	// open dice se c'è una transazione da chiudere: Abort e Close sono chiamate anche quando nessuna
	// è stata aperta, e lì End darebbe un errore di stato invalido al posto di un no-op.
	open bool
}

func (t *txProducer) Begin(context.Context) error {
	if err := wrap("begin-transaction", t.cl.BeginTransaction()); err != nil {
		return err
	}
	t.open = true
	return nil
}

// Produce invia i record nella transazione corrente e ATTENDE l'esito: ProduceSync ritorna a delivery
// report ricevuti, quindi un errore di produzione è noto prima del commit e la transazione può essere
// abortita. Il bound dell'attesa è il RecordDeliveryTimeout del client (producer.delivery-timeout, con
// il default della libreria) più il context del chiamante.
func (t *txProducer) Produce(ctx context.Context, recs []*message.ProducerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	krecs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		krecs = append(krecs, toKgoRecord(r))
	}
	return wrap("produce", t.cl.ProduceSync(ctx, krecs...).FirstErr())
}

// Commit chiude la transazione. Su errore `open` resta true: la transazione è ancora da abortire, e
// azzerarlo la lascerebbe aperta fino al transaction.timeout.ms del broker.
func (t *txProducer) Commit(ctx context.Context) error {
	if err := endErr("commit-transaction", t.cl.EndTransaction(ctx, kgo.TryCommit)); err != nil {
		return err
	}
	t.open = false
	return nil
}

// Abort annulla la transazione corrente: i record prodotti non diventano visibili. No-op se nessuna
// transazione è aperta.
func (t *txProducer) Abort(ctx context.Context) error {
	if !t.open {
		return nil
	}
	t.open = false
	return endErr("abort-transaction", t.cl.EndTransaction(ctx, kgo.TryAbort))
}

// Close abortisce una transazione eventualmente aperta, poi concede il flush timeout configurato per
// svuotare la coda prima di chiudere.
//
// L'abort viene PRIMA: chiudere il client lasciando una transazione in volo la tiene aperta fino al
// transaction.timeout.ms del broker, e nel frattempo i consumatori read_committed a valle restano
// bloccati su quelle partizioni. L'esito è loggato e non ritornato: in chiusura non esiste
// un'alternativa che il chiamante possa scegliere.
func (t *txProducer) Close() error {
	if err := t.Abort(context.Background()); err != nil {
		log.Warn().Err(err).Msg("corekafka: abort della transazione fallito alla chiusura del producer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.flushTimeout)
	defer cancel()
	err := t.cl.Flush(ctx)
	t.cl.Close()
	return wrap("flush", err)
}
