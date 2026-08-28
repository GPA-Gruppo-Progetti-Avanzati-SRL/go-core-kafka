package confluentdriver

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/rs/zerolog/log"
)

// transactSession implementa driver.TransactSession (modalità EOS Kafka->Kafka). Consuma con il
// consumer group di groupSession (auto-commit off, read_committed) e produce con il producer
// transazionale di txn; Commit invia gli offset consumati e committa la transazione atomicamente.
//
// Le due metà sono entrambe condivise: groupSession con il consumer della modalità handle, txn con il
// producer transazionale del processo. Qui resta solo ciò che è davvero dell'EOS — legare gli offset
// consumati all'esito della transazione.
type transactSession struct {
	groupSession
	txn
}

// Begin apre la transazione (init lazy al primo giro, col timeout dello spec — vedi txn.begin).
func (t *transactSession) Begin(ctx context.Context) error {
	return t.begin(ctx)
}

// Produce invia i record di output nella transazione corrente e verifica i delivery report prima del
// commit (così un errore di produzione è rilevato e la transazione può essere abortita dall'engine).
// L'attesa dei report è la stessa del producer condiviso — vedi produceAndAwait: ha un bound, quindi
// un report che non arriva non appende più la goroutine del consumer.
func (t *transactSession) Produce(ctx context.Context, recs []*message.ProducerRecord) error {
	return t.produce(ctx, recs)
}

// Commit invia gli offset consumati alla transazione e la committa (atomico: output + offset). È
// l'unica parte che l'EOS aggiunge alla transazione nuda: senza gli offset dentro, i record prodotti
// sarebbero atomici fra loro ma non con il consumo che li ha generati.
func (t *transactSession) Commit(ctx context.Context) error {
	meta, err := t.c.GetConsumerGroupMetadata()
	if err != nil {
		return wrap("group-metadata", err)
	}
	if !t.offsets.empty() {
		if err := t.p.SendOffsetsToTransaction(ctx, t.offsets.commitOffsets(), meta); err != nil {
			return wrap("send-offsets", err)
		}
	}
	if err := t.txn.commit(ctx); err != nil {
		return err
	}
	t.offsets.reset()
	return nil
}

// Abort annulla la transazione corrente; gli offset non vengono committati (replay). È un no-op sulla
// transazione se non ce n'è una aperta, ma gli offset vanno scartati comunque: è l'altra metà del
// contratto di Discard.
func (t *transactSession) Abort(ctx context.Context) error {
	t.offsets.reset()
	return t.txn.abort(ctx)
}

// Discard scarta gli offset E abortisce la transazione aperta: in EOS le due cose sono la stessa
// operazione, perché sono gli offset dentro la transazione a dover restare non committati. L'esito
// dell'abort è loggato e non ritornato — vedi il contratto di driver.Session.Discard.
func (t *transactSession) Discard(ctx context.Context) {
	if err := t.Abort(ctx); err != nil {
		log.Warn().Err(err).Str("consumer", t.name).
			Msg("corekafka: abort della transazione fallito durante lo scarto del batch")
	}
}

// Close chiude la sessione. Abortisce prima una transazione eventualmente aperta: chiudere il
// producer lasciandola in volo la tiene aperta fino al transaction.timeout.ms del broker, e nel
// frattempo i consumatori read_committed a valle restano bloccati su quelle partizioni.
func (t *transactSession) Close() error {
	t.Discard(context.Background())
	t.p.Close()
	return t.c.Close()
}
