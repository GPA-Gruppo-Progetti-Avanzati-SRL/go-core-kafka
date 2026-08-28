package confluentdriver

import (
	"context"
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/rs/zerolog/log"
)

// txProducer implementa driver.TxProducer: il producer TRANSAZIONALE del processo, che produce senza
// consumare. È la transazione nuda di txn — nessun offset da mandare in transazione, perché non c'è
// consumo — più la disciplina di chiusura del producer non transazionale.
//
// Da non confondere con transactSession, che è l'EOS Kafka->Kafka: là la transazione lega output e
// offset consumati. Qui la garanzia è più piccola e diversa: i record di un Produce diventano
// visibili ai consumer read_committed tutti o nessuno.
type txProducer struct {
	txn
	flushTimeout int // ms, da ProducerTuning.FlushTimeout
}

func (t *txProducer) Begin(ctx context.Context) error  { return t.begin(ctx) }
func (t *txProducer) Commit(ctx context.Context) error { return t.txn.commit(ctx) }
func (t *txProducer) Abort(ctx context.Context) error  { return t.txn.abort(ctx) }

// Produce accoda i record nella transazione corrente e attende i delivery report (vedi
// produceAndAwait): un errore di consegna è quindi noto PRIMA del commit, quando la transazione può
// ancora essere abortita.
func (t *txProducer) Produce(ctx context.Context, recs []*message.ProducerRecord) error {
	return t.produce(ctx, recs)
}

// Close abortisce una transazione eventualmente aperta, poi concede il flush timeout configurato per
// svuotare la coda di invio.
//
// L'abort viene PRIMA: chiudere il producer lasciando una transazione in volo la tiene aperta fino al
// transaction.timeout.ms del broker, e nel frattempo i consumatori read_committed a valle restano
// bloccati su quelle partizioni. L'esito è loggato e non ritornato: in chiusura non esiste
// un'alternativa che il chiamante possa scegliere.
func (t *txProducer) Close() error {
	if err := t.txn.abort(context.Background()); err != nil {
		log.Warn().Err(err).Msg("corekafka: abort della transazione fallito alla chiusura del producer")
	}
	remaining := t.p.Flush(t.flushTimeout)
	t.p.Close()
	if remaining > 0 {
		return fmt.Errorf("confluentdriver: flush incompleto alla chiusura: %d record ancora in coda dopo %dms", remaining, t.flushTimeout)
	}
	return nil
}
