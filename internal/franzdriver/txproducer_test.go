package franzdriver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeTxnProduceClient registra la SEQUENZA delle chiamate: della transazione conta l'ordine — un
// commit senza begin, o un abort mancante dopo un produce fallito, sono i due modi di perdere o
// duplicare record senza che nulla lo segnali.
type fakeTxnProduceClient struct {
	fakeProduceClient
	calls    []string
	beginErr error
	endErr   error
}

func (f *fakeTxnProduceClient) BeginTransaction() error {
	f.calls = append(f.calls, "begin")
	return f.beginErr
}

func (f *fakeTxnProduceClient) EndTransaction(_ context.Context, commit kgo.TransactionEndTry) error {
	if commit == kgo.TryCommit {
		f.calls = append(f.calls, "commit")
	} else {
		f.calls = append(f.calls, "abort")
	}
	return f.endErr
}

func (f *fakeTxnProduceClient) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.calls = append(f.calls, "produce")
	return f.fakeProduceClient.ProduceSync(ctx, rs...)
}

func recs() []*message.ProducerRecord {
	return []*message.ProducerRecord{{Topic: "notifiche", Key: []byte("k"), Value: []byte("v")}}
}

func TestTxProducer_UnaTransazionePerProduce(t *testing.T) {
	cl := &fakeTxnProduceClient{}
	p := &txProducer{cl: cl, flushTimeout: time.Second}
	ctx := context.Background()

	if err := p.Begin(ctx); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := p.Produce(ctx, recs()); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if err := p.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := strings.Join(cl.calls, ","); got != "begin,produce,commit" {
		t.Fatalf("sequenza = %s, attesa begin,produce,commit", got)
	}
	if p.open {
		t.Error("dopo un commit riuscito la transazione risulta ancora aperta")
	}
}

func TestTxProducer_CommitFallitoLasciaLaTransazioneDaAbortire(t *testing.T) {
	cl := &fakeTxnProduceClient{endErr: errors.New("boom")}
	p := &txProducer{cl: cl, flushTimeout: time.Second}
	ctx := context.Background()
	_ = p.Begin(ctx)

	if err := p.Commit(ctx); err == nil {
		t.Fatal("Commit senza errore: l'errore di End deve risalire")
	}
	// open resta true: azzerarlo lascerebbe la transazione aperta fino al transaction.timeout.ms del
	// broker, con i consumer read_committed bloccati su quelle partizioni.
	if !p.open {
		t.Fatal("dopo un commit FALLITO la transazione risulta chiusa: nessuno la abortirà più")
	}
	_ = p.Close()
	if got := strings.Join(cl.calls, ","); got != "begin,commit,abort" {
		t.Fatalf("sequenza = %s: Close deve abortire la transazione rimasta aperta", got)
	}
}

func TestTxProducer_AbortSenzaTransazioneApertaEUnNoOp(t *testing.T) {
	cl := &fakeTxnProduceClient{}
	p := &txProducer{cl: cl, flushTimeout: time.Second}

	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort senza transazione aperta = %v, atteso nil", err)
	}
	if len(cl.calls) != 0 {
		t.Fatalf("Abort ha chiamato il client (%v): su un producer appena creato è uno stato invalido", cl.calls)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cl.flushed || !cl.closed {
		t.Error("Close deve fare flush e chiudere il client")
	}
}

func TestTxProducer_ErroreDiProduceRisaleClassificato(t *testing.T) {
	cl := &fakeTxnProduceClient{fakeProduceClient: fakeProduceClient{err: errors.New("broker giù")}}
	p := &txProducer{cl: cl, flushTimeout: time.Second}
	ctx := context.Background()
	_ = p.Begin(ctx)

	err := p.Produce(ctx, recs())
	if err == nil {
		t.Fatal("Produce senza errore: l'esito di ProduceSync va verificato PRIMA del commit")
	}
	if driver.SeverityOf(err) == driver.SeverityBusiness {
		t.Errorf("errore non classificato dal driver: %v", err)
	}
}
