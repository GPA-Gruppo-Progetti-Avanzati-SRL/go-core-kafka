package confluentdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

// transactSession implementa driver.TransactSession (modalità EOS Kafka->Kafka). Consuma con il
// consumer group di groupSession (auto-commit off, read_committed) e produce con un producer
// transazionale; Commit invia gli offset consumati e committa la transazione atomicamente.
type transactSession struct {
	groupSession
	p           *kafka.Producer
	initTimeout time.Duration
	reportWait  time.Duration
	inited      bool
	// txnOpen dice se c'è una transazione da abortire. Serve perché Abort/Discard sono chiamate
	// anche su percorsi in cui nessuna transazione è stata aperta (un errore risalito da Poll prima
	// del primo Begin): abortire lì darebbe un errore di stato invalido al posto di un no-op.
	txnOpen bool
}

// Begin apre una transazione (init lazy al primo Begin, col timeout dello spec).
func (t *transactSession) Begin() error {
	if !t.inited {
		ctx, cancel := context.WithTimeout(context.Background(), t.initTimeout)
		defer cancel()
		if err := t.p.InitTransactions(ctx); err != nil {
			return wrap("init-transactions", err)
		}
		t.inited = true
	}
	if err := wrap("begin-transaction", t.p.BeginTransaction()); err != nil {
		return err
	}
	t.txnOpen = true
	return nil
}

// Produce invia i record di output nella transazione corrente e verifica i delivery report prima del
// commit (così un errore di produzione è rilevato e la transazione può essere abortita dall'engine).
// L'attesa dei report è la stessa del producer condiviso — vedi produceAndAwait: ha un bound, quindi
// un report che non arriva non appende più la goroutine del consumer.
func (t *transactSession) Produce(ctx context.Context, recs []*message.ProducerRecord) error {
	return produceAndAwait(ctx, t.p, recs, t.reportWait)
}

// Commit invia gli offset consumati alla transazione e la committa (atomico: output + offset).
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
	if err := t.p.CommitTransaction(ctx); err != nil {
		return wrap("commit-transaction", err)
	}
	t.txnOpen = false
	t.offsets.reset()
	return nil
}

// Abort annulla la transazione corrente; gli offset non vengono committati (replay). È un no-op se
// nessuna transazione è aperta.
func (t *transactSession) Abort(ctx context.Context) error {
	t.offsets.reset()
	if !t.txnOpen {
		return nil
	}
	t.txnOpen = false
	return wrap("abort-transaction", t.p.AbortTransaction(ctx))
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
