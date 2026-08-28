package franzdriver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeGroupClient struct {
	committed [][]*kgo.Record
	err       error
	closed    bool
}

func (f *fakeGroupClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	if f.err != nil {
		return f.err
	}
	f.committed = append(f.committed, rs)
	return nil
}

func (f *fakeGroupClient) Close() { f.closed = true }

func newTestGroupConsumer(p *fakePoller, cl *fakeGroupClient) *groupConsumer {
	return &groupConsumer{session: *newTestSession(p), cl: cl, offsets: newOffsetTracker()}
}

// Il commit conferma UN record per partizione, quello più avanti: è CommitRecords a tradurlo in
// offset+1 conservando il leader epoch.
func TestGroupConsumer_Commit(t *testing.T) {
	p := &fakePoller{fetches: []kgo.Fetches{records("t", 1, 2, 3)}}
	cl := &fakeGroupClient{}
	g := newTestGroupConsumer(p, cl)

	for range 3 {
		if _, err := g.Poll(context.Background(), time.Millisecond); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	if err := g.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(cl.committed) != 1 || len(cl.committed[0]) != 1 || cl.committed[0][0].Offset != 3 {
		t.Fatalf("committed = %v, atteso il solo record con offset 3", cl.committed)
	}
	if !g.offsets.empty() {
		t.Error("dopo il commit il tracker va azzerato: un secondo commit sarebbe un no-op")
	}
	if p.released != 2 {
		t.Errorf("released = %d, atteso 2 (prima fetch + rilascio dopo il commit)", p.released)
	}
}

// Dopo uno scarto (o una revoca) il tracker è vuoto e il commit NON deve toccare il broker: quei
// record li rilegge chi possiede ora le partizioni, e confermarli sarebbe perdere messaggi.
func TestGroupConsumer_CommitVuotoNonTocca(t *testing.T) {
	p := &fakePoller{fetches: []kgo.Fetches{records("t", 1)}}
	cl := &fakeGroupClient{}
	g := newTestGroupConsumer(p, cl)

	if _, err := g.Poll(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("poll: %v", err)
	}
	g.Discard(context.Background())
	if err := g.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(cl.committed) != 0 {
		t.Errorf("committed = %v, atteso nessun commit dopo uno scarto", cl.committed)
	}
}

// Un commit fallito è classificato come tutto il resto: la severità decide se l'engine ricostruisce il
// client o scarta il batch.
func TestGroupConsumer_CommitFallito(t *testing.T) {
	p := &fakePoller{fetches: []kgo.Fetches{records("t", 1)}}
	g := newTestGroupConsumer(p, &fakeGroupClient{err: kerr.IllegalGeneration})
	if _, err := g.Poll(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if got := driver.SeverityOf(g.Commit(context.Background())); got != driver.SeverityReset {
		t.Errorf("severità = %s, attesa reset: la generazione è cambiata sotto di noi", got)
	}
}

type fakeTxnClient struct {
	begun      int
	produced   [][]*kgo.Record
	ends       []kgo.TransactionEndTry
	committed  bool
	endErr     error
	produceErr error
	closed     bool
}

func (f *fakeTxnClient) Begin() error { f.begun++; return nil }

func (f *fakeTxnClient) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.produced = append(f.produced, rs)
	out := make(kgo.ProduceResults, 0, len(rs))
	for _, r := range rs {
		out = append(out, kgo.ProduceResult{Record: r, Err: f.produceErr})
	}
	return out
}

func (f *fakeTxnClient) End(_ context.Context, commit kgo.TransactionEndTry) (bool, error) {
	f.ends = append(f.ends, commit)
	return f.committed, f.endErr
}

func (f *fakeTxnClient) Close() { f.closed = true }

func newTestTransactSession(p *fakePoller, cl *fakeTxnClient) *transactSession {
	return &transactSession{session: *newTestSession(p), sess: cl}
}

func TestTransactSession_CommitAtomico(t *testing.T) {
	p := &fakePoller{fetches: []kgo.Fetches{records("t", 1)}}
	cl := &fakeTxnClient{committed: true}
	s := newTestTransactSession(p, cl)

	if err := s.Begin(context.Background()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := s.Poll(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if err := s.Produce(context.Background(), []*message.ProducerRecord{{Topic: "out", Value: []byte("v")}}); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if err := s.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(cl.ends) != 1 || cl.ends[0] != kgo.TryCommit {
		t.Errorf("ends = %v, atteso un solo TryCommit", cl.ends)
	}
	if len(cl.produced) != 1 || len(cl.produced[0]) != 1 || cl.produced[0][0].Topic != "out" {
		t.Errorf("produced = %v, atteso un record su out", cl.produced)
	}
}

// Il caso che una lettura distratta di End lascerebbe passare: committed=false SENZA errore significa
// che franz ha abortito da sé perché il gruppo ha rebalanciato. Trattarlo come successo dichiarerebbe
// elaborato un batch che è stato buttato.
func TestTransactSession_CommitNonAvvenutoEReset(t *testing.T) {
	p := &fakePoller{fetches: []kgo.Fetches{records("t", 1, 2)}}
	cl := &fakeTxnClient{committed: false}
	s := newTestTransactSession(p, cl)

	if err := s.Begin(context.Background()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := s.Poll(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("poll: %v", err)
	}
	err := s.Commit(context.Background())
	if got := driver.SeverityOf(err); got != driver.SeverityReset {
		t.Fatalf("severità = %s, attesa reset: la transazione è stata abortita, non committata", got)
	}
	if len(s.buf) != 0 {
		t.Error("l'abort riporta indietro il consumo: i record già fetchati vanno buttati, non riconsegnati")
	}
}

// Abort su una sessione senza transazione aperta è un no-op: succede quando un errore risale da Poll
// prima del primo Begin, e chiudere una transazione che non esiste darebbe un errore di stato al posto
// di niente.
func TestTransactSession_AbortSenzaTransazione(t *testing.T) {
	cl := &fakeTxnClient{}
	s := newTestTransactSession(&fakePoller{}, cl)
	if err := s.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if len(cl.ends) != 0 {
		t.Errorf("ends = %v, atteso nessun End senza transazione aperta", cl.ends)
	}
}

// Chiudere la sessione con una transazione aperta la lascerebbe in volo fino al transaction.timeout.ms
// del broker, e nel frattempo i consumatori read_committed a valle restano fermi su quelle partizioni.
func TestTransactSession_CloseAbortisce(t *testing.T) {
	cl := &fakeTxnClient{}
	s := newTestTransactSession(&fakePoller{}, cl)
	if err := s.Begin(context.Background()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(cl.ends) != 1 || cl.ends[0] != kgo.TryAbort || !cl.closed {
		t.Errorf("ends = %v closed = %v, attesi un TryAbort e la chiusura", cl.ends, cl.closed)
	}
}

// Un errore di produzione deve emergere PRIMA del commit, altrimenti si committerebbe una transazione
// con output mancante.
func TestTransactSession_ProduceFallito(t *testing.T) {
	cl := &fakeTxnClient{produceErr: errors.New("broker giù")}
	s := newTestTransactSession(&fakePoller{}, cl)
	err := s.Produce(context.Background(), []*message.ProducerRecord{{Topic: "out"}})
	if err == nil {
		t.Fatal("un delivery fallito deve risalire da Produce")
	}
}
