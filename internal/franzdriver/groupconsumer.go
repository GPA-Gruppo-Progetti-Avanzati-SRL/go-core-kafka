package franzdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// groupClient è ciò che serve al commit at-least-once oltre al consumo: lo implementa *kgo.Client, ed
// è un'interfaccia perché la disciplina del commit (no-op a tracker vuoto, rilascio del rebalance) è
// logica del driver e va verificata senza un broker.
type groupClient interface {
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	Close()
}

// groupConsumer implementa driver.GroupConsumer (modalità handle, at-least-once): il consumer di
// gruppo di session più il commit manuale degli offset.
type groupConsumer struct {
	session
	cl      groupClient
	offsets *offsetTracker
}

// Poll consegna il record all'engine e ne traccia l'offset per il commit successivo.
func (g *groupConsumer) Poll(ctx context.Context, timeout time.Duration) (*message.Record, error) {
	r, err := g.pollRaw(ctx, timeout)
	if r == nil || err != nil {
		return nil, err
	}
	g.offsets.track(r)
	return toRecord(r), nil
}

// Commit conferma gli offset dei record consegnati dall'ultimo Commit e sblocca i rebalance.
//
// Passa da CommitRecords e non da un commit di offset nudi perché è il record a portare con sé il
// leader epoch: committarlo senza epoch toglierebbe al broker il modo di rifiutare il commit di un
// membro rimasto indietro di una generazione. Dopo una revoca — o una Discard — il tracker è vuoto e
// il commit è un no-op: quei record li rilegge il nuovo owner, invece di essere dichiarati elaborati
// da chi non li possiede più.
func (g *groupConsumer) Commit(ctx context.Context) error {
	if g.offsets.empty() {
		g.release()
		return nil
	}
	if err := g.cl.CommitRecords(ctx, g.offsets.records()...); err != nil {
		return wrap("commit", err)
	}
	g.offsets.reset()
	g.release()
	return nil
}

// Discard scarta gli offset tracciati e non committati: l'engine la chiama quando butta il batch in
// volo, e senza di essa il Commit successivo confermerebbe record che nessuno ha elaborato (vedi il
// contratto di driver.Session.Discard).
func (g *groupConsumer) Discard(context.Context) {
	g.offsets.reset()
	g.dropAndRelease()
}

func (g *groupConsumer) Close() error {
	g.offsets.reset()
	g.cl.Close()
	return nil
}
