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

// recordsOn è records() su una partizione scelta: serve ai test della revoca PARZIALE, dove ciò che
// conta è distinguere le partizioni perse da quelle ritenute.
func recordsOn(topic string, partition int32, offsets ...int64) kgo.Fetches {
	parts := make([]kgo.FetchPartition, 0, len(offsets))
	for _, o := range offsets {
		parts = append(parts, kgo.FetchPartition{
			Partition: partition,
			Records:   []*kgo.Record{{Topic: topic, Partition: partition, Offset: o}},
		})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: topic, Partitions: parts}}}}
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
	// La revoca si consuma una volta sola: l'engine scarta il batch e riprende a consumare.
	if len(s.rb.takeRevoked()) > 0 {
		t.Error("la revoca deve valere una sola volta")
	}
}

// Revoca PARZIALE (modalità handle): i record bufferizzati delle partizioni RITENUTE devono
// sopravvivere. Buttarli sarebbe una perdita — sono ancora nostri, seguono gli offset committati, e
// la posizione di fetch non torna indietro: nessuno li rileggerebbe.
func TestPollRaw_RevocaParzialeConservaLePartizioniRitenute(t *testing.T) {
	f := &fakePoller{fetches: []kgo.Fetches{recordsOn("t", 0, 1, 2), recordsOn("t", 1, 10, 11)}}
	s := newTestSession(f)
	s.partial = true

	// Due fetch: la prima riempie il buffer con la partizione 0, la seconda con la 1. Dopo i due poll
	// il buffer contiene un record di ciascuna.
	for i := 0; i < 3; i++ {
		if _, err := s.pollRaw(context.Background(), time.Millisecond); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}

	s.rb.onRevoked(context.Background(), nil, map[string][]int32{"t": {0}})

	_, err := s.pollRaw(context.Background(), time.Millisecond)
	if driver.SeverityOf(err) != driver.SeverityReset {
		t.Fatalf("severità = %s, attesa reset", driver.SeverityOf(err))
	}
	revoked, ok := driver.RevokedOf(err)
	if !ok || len(revoked) != 1 || revoked[0] != (driver.TopicPartition{Topic: "t", Partition: 0}) {
		t.Fatalf("il reset non porta le partizioni perse: %v (ok=%v)", revoked, ok)
	}
	for _, r := range s.buf {
		if r.Partition == 0 {
			t.Errorf("record della partizione REVOCATA rimasto nel buffer (offset %d): verrebbe consegnato come se fosse ancora nostro", r.Offset)
		}
	}
	if len(s.buf) == 0 {
		t.Error("buttati anche i record della partizione RITENUTA: nessuno li rileggerebbe, sono persi")
	}
}

// In EOS il batch è l'unità della transazione: una revoca la invalida per intero, quindi il buffer si
// butta tutto e il reset non porta partizioni da filtrare.
func TestPollRaw_InEOSLaRevocaRestaTotale(t *testing.T) {
	f := &fakePoller{fetches: []kgo.Fetches{recordsOn("t", 1, 10, 11)}}
	s := newTestSession(f) // partial = false

	if _, err := s.pollRaw(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("primo poll: %v", err)
	}
	s.rb.onRevoked(context.Background(), nil, map[string][]int32{"t": {0}})

	_, err := s.pollRaw(context.Background(), time.Millisecond)
	if driver.SeverityOf(err) != driver.SeverityReset {
		t.Fatalf("severità = %s, attesa reset", driver.SeverityOf(err))
	}
	if _, ok := driver.RevokedOf(err); ok {
		t.Error("in EOS il reset non deve portare le partizioni: il batch va scartato per intero")
	}
	if len(s.buf) != 0 {
		t.Errorf("buffer = %d record, atteso vuoto in EOS", len(s.buf))
	}
}

// Una revoca senza partizioni non deve generare un reset: non c'è nulla da invalidare, e scartare il
// batch butterebbe record che sono ancora interamente nostri.
func TestObserver_RevocaVuotaNonSegnalaNulla(t *testing.T) {
	o := &rebalanceObserver{name: "ingest"}
	o.onRevoked(context.Background(), nil, map[string][]int32{})
	if got := o.takeRevoked(); len(got) > 0 {
		t.Errorf("revoca segnalata senza partizioni: %v", got)
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
