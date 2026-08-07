package processor

import (
	"errors"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
)

// L'handler può ritornare DeadLetter(...) per instradare record specifici al DLQ: deve essere
// riconoscibile via errors.As come *PoisonRecords, con la causa preservata.
func TestDeadLetter_IsPoisonRecords(t *testing.T) {
	cause := errors.New("parse fallito")
	rec := &message.Record{Value: []byte("bad")}
	err := error(DeadLetter(cause, rec))

	var pr *PoisonRecords
	if !errors.As(err, &pr) {
		t.Fatalf("atteso *PoisonRecords, ottenuto %v", err)
	}
	if len(pr.Records) != 1 || string(pr.Records[0].Value) != "bad" {
		t.Fatalf("record errati: %+v", pr.Records)
	}
	if !errors.Is(err, cause) {
		t.Fatal("la causa deve essere preservata (Unwrap)")
	}
}

// ErrFailFast deve restare identificabile via errors.Is anche se wrappato dall'handler.
func TestErrFailFast_IsWrappable(t *testing.T) {
	wrapped := errors.Join(errors.New("contesto business"), ErrFailFast)
	if !errors.Is(wrapped, ErrFailFast) {
		t.Fatal("ErrFailFast deve essere riconoscibile anche se wrappato")
	}
}
