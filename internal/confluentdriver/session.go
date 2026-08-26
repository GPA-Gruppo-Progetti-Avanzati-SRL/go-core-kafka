package confluentdriver

import (
	"context"
	"errors"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// transactSession implementa driver.TransactSession (modalità EOS Kafka->Kafka). Consuma con un
// consumer group (auto-commit off, read_committed) e produce con un producer transazionale; Commit
// invia gli offset consumati e committa la transazione atomicamente.
type transactSession struct {
	c           *kafka.Consumer
	p           *kafka.Producer
	offsets     *offsetTracker
	rb          *rebalanceObserver
	initTimeout time.Duration
	inited      bool
}

// Poll ritorna il prossimo messaggio, (nil, nil) allo scadere del timeout, o SeverityReset dopo un
// rebalance (stessa semantica di groupConsumer.Poll).
func (t *transactSession) Poll(_ context.Context, timeout time.Duration) (*message.Record, error) {
	msg, err := t.c.ReadMessage(timeout)
	if err != nil {
		var ke kafka.Error
		if errors.As(err, &ke) && ke.Code() == kafka.ErrTimedOut {
			if t.rb.takeRevoked() {
				return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
			}
			return nil, nil
		}
		return nil, wrap("poll", err)
	}
	if t.rb.takeRevoked() {
		return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	}
	t.offsets.track(msg.TopicPartition)
	return toRecord(msg), nil
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
	return wrap("begin-transaction", t.p.BeginTransaction())
}

// Produce invia i record di output nella transazione corrente e verifica i delivery report prima del
// commit (così un errore di produzione è rilevato e la transazione può essere abortita dall'engine).
func (t *transactSession) Produce(_ context.Context, recs []*message.ProducerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	deliveryChan := make(chan kafka.Event, len(recs))
	for _, r := range recs {
		if err := t.p.Produce(toMessage(r), deliveryChan); err != nil {
			return wrap("produce", err)
		}
	}
	for range recs {
		ev := <-deliveryChan
		if m, ok := ev.(*kafka.Message); ok && m.TopicPartition.Error != nil {
			return wrap("delivery", m.TopicPartition.Error)
		}
	}
	return nil
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
	t.offsets.reset()
	return nil
}

// Abort annulla la transazione corrente; gli offset non vengono committati (replay).
func (t *transactSession) Abort(ctx context.Context) error {
	t.offsets.reset()
	return wrap("abort-transaction", t.p.AbortTransaction(ctx))
}

func (t *transactSession) Close() error {
	t.offsets.reset()
	t.p.Close()
	return t.c.Close()
}
