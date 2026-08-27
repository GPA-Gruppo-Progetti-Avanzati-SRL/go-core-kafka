package confluentdriver

import (
	"context"
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
