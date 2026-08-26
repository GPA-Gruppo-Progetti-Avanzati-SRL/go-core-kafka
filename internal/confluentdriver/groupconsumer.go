package confluentdriver

import (
	"context"
	"errors"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// groupConsumer implementa driver.GroupConsumer (modalità handle, at-least-once).
type groupConsumer struct {
	c       *kafka.Consumer
	offsets *offsetTracker
	rb      *rebalanceObserver
}

// Poll ritorna il prossimo messaggio, (nil, nil) allo scadere del timeout senza messaggi, o un errore
// SeverityReset se nel frattempo è avvenuto un rebalance (vedi rebalance.go).
func (g *groupConsumer) Poll(_ context.Context, timeout time.Duration) (*message.Record, error) {
	msg, err := g.c.ReadMessage(timeout)
	if err != nil {
		var ke kafka.Error
		if errors.As(err, &ke) && ke.Code() == kafka.ErrTimedOut {
			// Il rebalance può essere avvenuto durante questo poll a vuoto: va segnalato comunque,
			// perché il batch accumulato prima resta da scartare.
			if g.rb.takeRevoked() {
				return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
			}
			return nil, nil
		}
		return nil, wrap("poll", err)
	}
	if g.rb.takeRevoked() {
		// Questo record appartiene già alla nuova assegnazione, ma il batch accumulato prima del
		// rebalance no. Lo scartiamo senza tracciarne l'offset: verrà riletto dopo che l'engine ha
		// buttato il batch — un duplicato, non un buco.
		return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	}
	g.offsets.track(msg.TopicPartition)
	return toRecord(msg), nil
}

// Commit conferma gli offset (offset+1) dei messaggi ritornati da Poll dall'ultimo Commit. Dopo una
// revoca il tracker è vuoto (l'observer lo ha azzerato) e il commit è un no-op: i record vengono
// riletti dal nuovo owner invece di essere dichiarati elaborati da chi non li possiede più.
func (g *groupConsumer) Commit(_ context.Context) error {
	if g.offsets.empty() {
		return nil
	}
	if _, err := g.c.CommitOffsets(g.offsets.commitOffsets()); err != nil {
		return wrap("commit", err)
	}
	g.offsets.reset()
	return nil
}

func (g *groupConsumer) Close() error {
	g.offsets.reset()
	return g.c.Close()
}
