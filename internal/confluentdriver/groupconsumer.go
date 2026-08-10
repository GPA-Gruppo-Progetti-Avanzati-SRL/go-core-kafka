package confluentdriver

import (
	"context"
	"errors"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// groupConsumer implementa driver.GroupConsumer (modalità handle, at-least-once).
type groupConsumer struct {
	c       *kafka.Consumer
	offsets *offsetTracker
}

// Poll ritorna il prossimo messaggio o (nil, nil) allo scadere del timeout senza messaggi.
func (g *groupConsumer) Poll(_ context.Context, timeout time.Duration) (*message.Record, error) {
	msg, err := g.c.ReadMessage(timeout)
	if err != nil {
		var ke kafka.Error
		if errors.As(err, &ke) && ke.Code() == kafka.ErrTimedOut {
			return nil, nil
		}
		return nil, err
	}
	g.offsets.track(msg.TopicPartition)
	return toRecord(msg), nil
}

// Commit conferma gli offset (offset+1) dei messaggi ritornati da Poll dall'ultimo Commit.
func (g *groupConsumer) Commit(_ context.Context) error {
	if g.offsets.empty() {
		return nil
	}
	if _, err := g.c.CommitOffsets(g.offsets.commitOffsets()); err != nil {
		return err
	}
	g.offsets.reset()
	return nil
}

func (g *groupConsumer) Close() error {
	g.offsets.reset()
	return g.c.Close()
}
