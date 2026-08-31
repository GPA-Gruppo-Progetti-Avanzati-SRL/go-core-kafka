package franzdriver

import (
	"context"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProduceClient struct {
	produced [][]*kgo.Record
	err      error
	flushErr error
	flushed  bool
	closed   bool
}

func (f *fakeProduceClient) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.produced = append(f.produced, rs)
	out := make(kgo.ProduceResults, 0, len(rs))
	for _, r := range rs {
		out = append(out, kgo.ProduceResult{Record: r, Err: f.err})
	}
	return out
}

func (f *fakeProduceClient) Flush(context.Context) error { f.flushed = true; return f.flushErr }
func (f *fakeProduceClient) Close()                      { f.closed = true }

func TestProducer_Produce(t *testing.T) {
	cl := &fakeProduceClient{}
	p := &producer{cl: cl, flushTimeout: time.Second}

	if err := p.Produce(context.Background(), nil); err != nil {
		t.Fatalf("produce di un batch vuoto = %v, atteso nil (e nessuna chiamata al client)", err)
	}
	if len(cl.produced) != 0 {
		t.Error("un batch vuoto non deve raggiungere il client")
	}

	err := p.Produce(context.Background(), []*message.ProducerRecord{
		{Topic: "dlq", Value: []byte("v"), Headers: message.Headers{{Key: "corekafka-dlq-error", Value: []byte("boom")}}},
	})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(cl.produced) != 1 || cl.produced[0][0].Topic != "dlq" || len(cl.produced[0][0].Headers) != 1 {
		t.Errorf("produced = %v, atteso un record su dlq con il suo header", cl.produced)
	}
}

// L'esito del delivery è atteso dentro ProduceSync: un record non consegnato deve risalire con una
// severità, non sparire.
func TestProducer_DeliveryFallito(t *testing.T) {
	p := &producer{cl: &fakeProduceClient{err: kerr.NotEnoughReplicas}, flushTimeout: time.Second}
	err := p.Produce(context.Background(), []*message.ProducerRecord{{Topic: "dlq"}})
	if got := driver.SeverityOf(err); got != driver.SeverityRetriable {
		t.Errorf("severità = %s, attesa retriable: le repliche possono tornare", got)
	}
}

// Close concede il flush-timeout per svuotare la coda: quello che resta è perso, quindi un flush
// incompleto non va ingoiato.
func TestProducer_CloseFlusha(t *testing.T) {
	cl := &fakeProduceClient{}
	p := &producer{cl: cl, flushTimeout: 10 * time.Millisecond}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !cl.flushed || !cl.closed {
		t.Errorf("flushed=%v closed=%v, attesi entrambi", cl.flushed, cl.closed)
	}

	cl = &fakeProduceClient{flushErr: context.DeadlineExceeded}
	p = &producer{cl: cl, flushTimeout: time.Millisecond}
	if err := p.Close(); err == nil {
		t.Error("un flush incompleto alla chiusura va riportato: quei record sono persi")
	}
	if !cl.closed {
		t.Error("il client va chiuso anche se il flush fallisce")
	}
}

// La modalità transform richiede un transactional-id: senza, il client non è transazionale e ogni
// batch fallirebbe al primo Begin. Meglio non partire.
func TestFactory_TransactSenzaTransactionalID(t *testing.T) {
	_, err := Factory{}.NewTransactSession(processor(spec.ConsumerTuning{}), server())
	if err == nil {
		t.Fatal("una sessione EOS senza transactional-id deve fallire alla costruzione")
	}
}

// NewProcessorProducer è il producer non transazionale di UN processor (transform at-least-once):
// niente transactional.id — non c'è transazione — e il tuning è quello del processor, non di
// `server.producer`.
func TestFactory_NewProcessorProducer(t *testing.T) {
	s := spec.ProcessorSpec{
		Name: "alos", GroupID: "g", Topics: []string{"in"},
		Delivery: spec.DeliveryAtLeastOnce,
		Producer: spec.ProducerTuning{CompressionType: "zstd"},
	}.Resolve(spec.KafkaServer{})

	p, err := Factory{}.NewProcessorProducer(s, spec.KafkaServer{BootstrapServers: "b:9092"})
	if err != nil {
		t.Fatalf("NewProcessorProducer: %v", err)
	}
	defer func() { _ = p.Close() }()

	// La verifica sulle opzioni passa dal builder, che è dove la traduzione avviene.
	b, err := producerOpts("", "processor "+s.Name, s.Producer, spec.KafkaServer{BootstrapServers: "b:9092"})
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	if _, ok := b.applied["transactional.id"]; ok {
		t.Error("transactional.id impostato su un producer non transazionale")
	}
	if got := b.applied["compression.type"]; got != "zstd" {
		t.Errorf("compression.type = %q, atteso zstd (tuning del processor, non di server.producer)", got)
	}
}
