package batchsink

import (
	"context"
	"errors"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
)

type fakeSink struct {
	flushed [][]int
	err     error
}

func (f *fakeSink) Flush(_ context.Context, ops []int) error {
	if f.err != nil {
		return f.err
	}
	cp := append([]int(nil), ops...)
	f.flushed = append(f.flushed, cp)
	return nil
}

// mapper: chiave = header "k"; valore int = len(Value); skip se Value == "skip"; poison se Value == "bad".
func mapper(_ context.Context, r *message.Record) (int, string, bool, error) {
	switch string(r.Value) {
	case "skip":
		return 0, "", true, nil
	case "bad":
		return 0, "", false, errors.New("parse error")
	}
	return len(r.Value), r.Headers["k"], false, nil
}

func rec(key, val string) *message.Record {
	return &message.Record{Value: []byte(val), Headers: map[string]string{"k": key}}
}

func TestHandle_DedupLastWins(t *testing.T) {
	s := &fakeSink{}
	b := &BatchSpooler[int]{Map: mapper, Sink: s}
	// stessa chiave "a" due volte: vince l'ultimo (len("thirty")=6); "b" una volta (len("bb")=2).
	batch := []*message.Record{rec("a", "x"), rec("b", "bb"), rec("a", "thirty")}
	if err := b.Handle(context.Background(), batch); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(s.flushed) != 1 {
		t.Fatalf("expected 1 flush, got %d", len(s.flushed))
	}
	got := s.flushed[0]
	// ordine di prima occorrenza: a, b; a aggiornato a 6.
	if len(got) != 2 || got[0] != 6 || got[1] != 2 {
		t.Fatalf("dedup last-wins errato: %v", got)
	}
}

func TestHandle_SkipAndPoison(t *testing.T) {
	s := &fakeSink{}
	b := &BatchSpooler[int]{Map: mapper, Sink: s}
	batch := []*message.Record{rec("a", "ok"), rec("", "skip"), rec("", "bad")}
	err := b.Handle(context.Background(), batch)

	var pr *processor.PoisonRecords
	if !errors.As(err, &pr) {
		t.Fatalf("atteso *processor.PoisonRecords, ottenuto %v", err)
	}
	if len(pr.Records) != 1 || string(pr.Records[0].Value) != "bad" {
		t.Fatalf("record poison errato: %+v", pr.Records)
	}
	// il record buono è stato comunque flushato; lo skip è stato ignorato.
	if len(s.flushed) != 1 || len(s.flushed[0]) != 1 || s.flushed[0][0] != 2 {
		t.Fatalf("flush dei buoni errato: %v", s.flushed)
	}
}

func TestHandle_FlushErrorIsTransient(t *testing.T) {
	sinkErr := errors.New("sink down")
	s := &fakeSink{err: sinkErr}
	b := &BatchSpooler[int]{Map: mapper, Sink: s}
	batch := []*message.Record{rec("a", "ok")}
	err := b.Handle(context.Background(), batch)

	var pr *processor.PoisonRecords
	if errors.As(err, &pr) {
		t.Fatalf("un errore di flush non deve essere PoisonRecords")
	}
	if !errors.Is(err, sinkErr) {
		t.Fatalf("atteso l'errore di flush grezzo, ottenuto %v", err)
	}
}
