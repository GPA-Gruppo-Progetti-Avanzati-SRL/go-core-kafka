package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"go.uber.org/fx"
)

// Il Producer è il seam attraverso cui passa ogni record del DLQ in modalità handle, e il package era
// a copertura zero. I test dell'engine lo esercitano già, ma da un altro package: la copertura è
// per-package, e un package senza test propri è un package che nessuno verifica quando lo si tocca.

type fakeDriverProducer struct {
	produced []*message.ProducerRecord
	err      error
	closed   bool
	closeErr error
}

func (f *fakeDriverProducer) Produce(_ context.Context, recs []*message.ProducerRecord) error {
	if f.err != nil {
		return f.err
	}
	f.produced = append(f.produced, recs...)
	return nil
}

func (f *fakeDriverProducer) Close() error {
	f.closed = true
	return f.closeErr
}

type fakeFactory struct {
	p   driver.Producer
	err error
	// got registra il tuning ricevuto: serve a verificare che i default siano applicati PRIMA di
	// arrivare al driver.
	got spec.ProducerTuning
}

func (f *fakeFactory) NewGroupConsumer(spec.ProcessorSpec, spec.KafkaServer) (driver.GroupConsumer, error) {
	return nil, errors.New("non usata")
}

func (f *fakeFactory) NewTransactSession(spec.ProcessorSpec, spec.KafkaServer) (driver.TransactSession, error) {
	return nil, errors.New("non usata")
}

func (f *fakeFactory) NewProducer(_ spec.KafkaServer, p spec.ProducerTuning) (driver.Producer, error) {
	f.got = p
	return f.p, f.err
}

type fakeLifecycle struct{ hooks []fx.Hook }

func (l *fakeLifecycle) Append(h fx.Hook) { l.hooks = append(l.hooks, h) }

func validServer() spec.KafkaServer {
	return spec.KafkaServer{BootstrapServers: "broker:9092"}
}

func TestNewProducer_ValidaLaSezioneServer(t *testing.T) {
	// Il Producer è registrabile anche da solo, senza consumer: non può appoggiarsi al fatto che
	// l'engine abbia già validato. Se lo facesse, un'app di sola produzione partirebbe con una config
	// che l'engine avrebbe rifiutato.
	tests := []struct {
		name string
		k    spec.KafkaServer
	}{
		{"bootstrap-servers mancante", spec.KafkaServer{}},
		{"chiave riservata", spec.KafkaServer{
			BootstrapServers: "b:9092",
			Producer:         spec.ProducerTuning{KafkaProperties: map[string]string{"transactional.id": "x"}},
		}},
		{"enum con refuso", spec.KafkaServer{
			BootstrapServers: "b:9092",
			Producer:         spec.ProducerTuning{Acks: "tutti"},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeFactory{p: &fakeDriverProducer{}}
			if _, err := NewProducer(&fakeLifecycle{}, f, tc.k, spec.ProducerTuning{}); err == nil {
				t.Error("atteso errore: la config non doveva passare")
			}
		})
	}
}

func TestNewProducer_ApplicaIDefaultPrimaDelDriver(t *testing.T) {
	// delivery-timeout ha un default della LIBRERIA — è ciò che garantisce che un delivery report
	// arrivi — e va imposto al client, non solo atteso lato Go: se arrivasse a zero al driver, il
	// bound sull'attesa dei report tornerebbe a dipendere dal default di librdkafka.
	f := &fakeFactory{p: &fakeDriverProducer{}}
	if _, err := NewProducer(&fakeLifecycle{}, f, validServer(), spec.ProducerTuning{}); err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if f.got.DeliveryTimeout != spec.DefaultDeliveryTimeout {
		t.Errorf("delivery-timeout ricevuto dal driver = %v, atteso il default della libreria %v",
			f.got.DeliveryTimeout, spec.DefaultDeliveryTimeout)
	}
	if f.got.FlushTimeout != spec.DefaultFlushTimeout {
		t.Errorf("flush-timeout ricevuto dal driver = %v, atteso %v", f.got.FlushTimeout, spec.DefaultFlushTimeout)
	}
}

func TestNewProducer_ErroreDelDriverRisale(t *testing.T) {
	f := &fakeFactory{err: errors.New("librdkafka: config rifiutata")}
	if _, err := NewProducer(&fakeLifecycle{}, f, validServer(), spec.ProducerTuning{}); err == nil {
		t.Error("atteso errore risalito dalla factory")
	}
}

func TestProduce(t *testing.T) {
	d := &fakeDriverProducer{}
	p, err := NewProducer(&fakeLifecycle{}, &fakeFactory{p: d}, validServer(), spec.ProducerTuning{})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	recs := []*message.ProducerRecord{{Topic: "t", Value: []byte("v")}}
	if appErr := p.Produce(context.Background(), recs); appErr != nil {
		t.Fatalf("Produce = %v, atteso nil", appErr)
	}
	if len(d.produced) != 1 || d.produced[0].Topic != "t" {
		t.Errorf("record consegnati al driver = %v", d.produced)
	}
}

func TestProduce_ErroreDelDriverConservaLaCausa(t *testing.T) {
	// La causa deve restare raggiungibile con errors.Is: chi produce sul DLQ deve poter distinguere un
	// broker giù da un record rifiutato, e l'ApplicationError da solo non lo direbbe.
	boom := errors.New("all brokers down")
	d := &fakeDriverProducer{err: boom}
	p, err := NewProducer(&fakeLifecycle{}, &fakeFactory{p: d}, validServer(), spec.ProducerTuning{})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	appErr := p.Produce(context.Background(), []*message.ProducerRecord{{Topic: "t"}})
	if appErr == nil {
		t.Fatal("atteso errore")
	}
	if !errors.Is(appErr, boom) {
		t.Errorf("errors.Is non raggiunge la causa: %v", appErr)
	}
}

func TestOnStop_ChiudeIlProducer(t *testing.T) {
	// Senza la chiusura, i record ancora in coda allo shutdown non hanno nemmeno la possibilità di
	// essere svuotati dal flush-timeout.
	lc := &fakeLifecycle{}
	d := &fakeDriverProducer{}
	if _, err := NewProducer(lc, &fakeFactory{p: d}, validServer(), spec.ProducerTuning{}); err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if len(lc.hooks) != 1 {
		t.Fatalf("hook registrati = %d, atteso 1", len(lc.hooks))
	}
	if err := lc.hooks[0].OnStop(context.Background()); err != nil {
		t.Errorf("OnStop = %v", err)
	}
	if !d.closed {
		t.Error("il producer del driver non è stato chiuso")
	}
}
