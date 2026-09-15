package confluentdriver

import (
	"context"
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

// groupConsumer implementa driver.GroupConsumer (modalità handle, at-least-once): il consumer
// sottoscritto di groupSession più il commit manuale degli offset.
type groupConsumer struct {
	groupSession
}

// Commit conferma gli offset (offset+1) dei messaggi ritornati da Poll dall'ultimo Commit. Dopo una
// revoca — o dopo una Discard — il tracker è vuoto e il commit è un no-op: i record vengono riletti
// dal nuovo owner invece di essere dichiarati elaborati da chi non li possiede più.
func (g *groupConsumer) Commit(_ context.Context) error {
	if g.offsets.empty() {
		return nil
	}
	offsets := g.offsets.commitOffsets()
	err := g.commitWithRetry(offsets)
	if err != nil {
		return wrap("commit", err)
	}
	g.offsets.reset()
	return nil
}

// commitRetries / commitRetryBackoff: un rebalance cooperativo dura tipicamente poche centinaia di
// millisecondi, e committare mentre è in corso è un caso frequente, non teorico.
const (
	commitRetries      = 2
	commitRetryBackoff = 150 * time.Millisecond
)

// commitWithRetry ritenta il SOLO commit quando il broker lo rifiuta perché un rebalance è in corso.
//
// Non si può invece rigiocare il batch: a questo punto Handle (o Produce) è già avvenuto, quindi un
// replay ripubblicherebbe gli output. Il commit è idempotente, quindi è l'unica parte che si può
// ripetere — e ripeterla evita di buttare il progresso di record già elaborati, che altrimenti
// verrebbero rielaborati al prossimo avvio.
//
// Se i tentativi si esauriscono l'errore risale come prima: se la revoca riguardava davvero quelle
// partizioni il commit non riuscirà mai, ed è corretto — quei record li rilegge il nuovo owner.
func (g *groupConsumer) commitWithRetry(offsets []kafka.TopicPartition) error {
	var err error
	for attempt := 0; ; attempt++ {
		if _, err = g.c.CommitOffsets(offsets); err == nil {
			return nil
		}
		var ke kafka.Error
		if !errors.As(err, &ke) || ke.Code() != kafka.ErrRebalanceInProgress || attempt >= commitRetries {
			return err
		}
		log.Warn().Err(err).Str("consumer", g.name).Int("attempt", attempt+1).
			Int("partitions", len(offsets)).
			Msg("corekafka: commit rifiutato perché un rebalance è in corso, si ritenta")
		time.Sleep(commitRetryBackoff)
	}
}

func (g *groupConsumer) Close() error {
	g.offsets.reset()
	return g.c.Close()
}
