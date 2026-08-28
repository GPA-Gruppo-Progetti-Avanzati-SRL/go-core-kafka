package franzdriver

import (
	"context"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakePoller consegna una fetch per chiamata e conta i rilasci del rebalance: sono le due cose che la
// parte condivisa della sessione governa, e nessuna delle due è osservabile da fuori il driver.
type fakePoller struct {
	fetches  []kgo.Fetches
	polls    int
	released int
}

func (f *fakePoller) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	f.polls++
	if len(f.fetches) == 0 {
		<-ctx.Done() // nessun record: si comporta come una fetch che attende fino al timeout
		return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "t", Partitions: []kgo.FetchPartition{{Err: ctx.Err()}}}}}}
	}
	out := f.fetches[0]
	f.fetches = f.fetches[1:]
	return out
}

func (f *fakePoller) AllowRebalance() { f.released++ }

func records(topic string, offsets ...int64) kgo.Fetches {
	parts := make([]kgo.FetchPartition, 0, len(offsets))
	for _, o := range offsets {
		parts = append(parts, kgo.FetchPartition{Partition: 0, Records: []*kgo.Record{{Topic: topic, Partition: 0, Offset: o}}})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: topic, Partitions: parts}}}}
}

func newTestSession(f *fakePoller) *session {
	return &session{name: "ingest", p: f, rb: &rebalanceObserver{name: "ingest"}, maxPoll: 10}
}

// Una fetch riempie il buffer e i record vengono consegnati uno alla volta: l'engine chiede un record
// per volta, il client ne consegna molti, e la differenza la assorbe il buffer — non una fetch per
// record.
func TestPollRaw_BufferizzaLaFetch(t *testing.T) {
	f := &fakePoller{fetches: []kgo.Fetches{records("t", 1, 2, 3)}}
	s := newTestSession(f)

	for _, want := range []int64{1, 2, 3} {
		r, err := s.pollRaw(context.Background(), time.Millisecond)
		if err != nil || r == nil {
			t.Fatalf("poll offset %d: r=%v err=%v", want, r, err)
		}
		if r.Offset != want {
			t.Errorf("offset = %d, atteso %d", r.Offset, want)
		}
	}
	if f.polls != 1 {
		t.Errorf("polls = %d, attesa una sola fetch per tre record", f.polls)
	}
}

// Una fetch a vuoto non è un errore: è il modo in cui il loop dell'engine torna a osservare il ticker
// del taglio e la cancellazione.
func TestPollRaw_TimeoutSenzaRecord(t *testing.T) {
	f := &fakePoller{}
	s := newTestSession(f)
	r, err := s.pollRaw(context.Background(), 5*time.Millisecond)
	if r != nil || err != nil {
		t.Fatalf("poll a vuoto = (%v, %v), attesi (nil, nil)", r, err)
	}
}

// Alla revoca il batch in volo va scartato: i record consegnati vengono da partizioni che potrebbero
// non essere più nostre, e committarli significherebbe dichiarare elaborato ciò che il nuovo owner sta
// rileggendo. Anche il buffer va buttato, per la stessa ragione.
func TestPollRaw_RevocaScartaBufferEBatch(t *testing.T) {
	f := &fakePoller{fetches: []kgo.Fetches{records("t", 1, 2, 3)}}
	s := newTestSession(f)
	if _, err := s.pollRaw(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("primo poll: %v", err)
	}

	s.rb.onRevoked(context.Background(), nil, map[string][]int32{"t": {0}})

	_, err := s.pollRaw(context.Background(), time.Millisecond)
	if driver.SeverityOf(err) != driver.SeverityReset {
		t.Fatalf("severità = %s, attesa reset dopo una revoca", driver.SeverityOf(err))
	}
	if len(s.buf) != 0 {
		t.Errorf("buffer = %d record, atteso vuoto: appartengono a partizioni forse non più nostre", len(s.buf))
	}
	// Il flag si consuma una volta sola: l'engine scarta il batch e riprende a consumare.
	if s.rb.takeRevoked() {
		t.Error("il flag di revoca deve valere una sola volta")
	}
}

// Il rebalance resta bloccato finché l'engine ha un batch in mano (è la garanzia di
// BlockRebalanceOnPoll), e viene rilasciato prima di una fetch quando non c'è nulla in volo: senza
// quest'ultima parte un consumer IDLE bloccherebbe per sempre i rebalance del gruppo.
func TestPollRaw_RilascioSoloSenzaBatchInVolo(t *testing.T) {
	f := &fakePoller{fetches: []kgo.Fetches{records("t", 1), records("t", 2)}}
	s := newTestSession(f)

	if _, err := s.pollRaw(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("primo poll: %v", err)
	}
	if f.released != 1 {
		t.Fatalf("released = %d, atteso 1 (il primo poll avviene senza batch in volo)", f.released)
	}
	// Batch in volo: la fetch successiva NON deve rilasciare.
	if _, err := s.pollRaw(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("secondo poll: %v", err)
	}
	if f.released != 1 {
		t.Errorf("released = %d, atteso ancora 1: con un batch in volo il rebalance resta bloccato", f.released)
	}

	s.release()
	if f.released != 2 || s.holding {
		t.Errorf("dopo release: released=%d holding=%v, attesi 2 e false", f.released, s.holding)
	}
}

// release NON butta il buffer: quei record sono successivi agli offset appena committati e vanno
// consegnati al batch seguente. discard invece li butta, perché il batch è stato scartato.
func TestRelease_ConservaIlBuffer(t *testing.T) {
	f := &fakePoller{fetches: []kgo.Fetches{records("t", 1, 2)}}
	s := newTestSession(f)
	if _, err := s.pollRaw(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("poll: %v", err)
	}

	s.release()
	if len(s.buf) != 1 {
		t.Errorf("buffer dopo release = %d, atteso 1: i record già fetchati restano validi", len(s.buf))
	}
	s.dropAndRelease()
	if len(s.buf) != 0 {
		t.Errorf("buffer dopo discard = %d, atteso vuoto", len(s.buf))
	}
}

// Un errore di fetch è classificato come tutti gli altri: è la severità a dire all'engine se scartare
// il batch o ricostruire il client.
func TestPollRaw_ErroreDiFetch(t *testing.T) {
	f := &fakePoller{fetches: []kgo.Fetches{{{Topics: []kgo.FetchTopic{{
		Topic:      "t",
		Partitions: []kgo.FetchPartition{{Partition: 0, Err: kerr.NotLeaderForPartition}},
	}}}}}}
	s := newTestSession(f)
	_, err := s.pollRaw(context.Background(), time.Millisecond)
	if driver.SeverityOf(err) != driver.SeverityRetriable {
		t.Fatalf("severità = %s, attesa retriable per un leader in elezione", driver.SeverityOf(err))
	}
}
