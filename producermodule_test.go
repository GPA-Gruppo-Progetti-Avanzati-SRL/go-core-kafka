package corekafka

import (
	"context"
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// --- fake driver: la Factory è l'unico punto legato al client, quindi il wiring si verifica per intero
// senza broker. Registrarla è ciò che fa la Driver di un guscio pubblico (driver/confluent, driver/franz).

type fakeFactory struct {
	txID string // id ricevuto: dice QUALE dei due producer il wiring ha scelto
	tx   bool
}

func (f *fakeFactory) NewGroupConsumer(spec.ProcessorSpec, spec.KafkaServer) (driver.GroupConsumer, error) {
	return nil, nil
}

func (f *fakeFactory) NewTransactSession(spec.ProcessorSpec, spec.KafkaServer) (driver.TransactSession, error) {
	return nil, nil
}

func (f *fakeFactory) NewProducer(spec.KafkaServer, spec.ProducerTuning) (driver.Producer, error) {
	return &fakeDriverProducer{}, nil
}

func (f *fakeFactory) NewTxProducer(_ spec.KafkaServer, _ spec.ProducerTuning, id string) (driver.TxProducer, error) {
	f.tx = true
	f.txID = id
	return &fakeDriverTxProducer{}, nil
}

type fakeDriverProducer struct{}

func (fakeDriverProducer) Produce(context.Context, []*message.ProducerRecord) error { return nil }
func (fakeDriverProducer) Close() error                                             { return nil }

type fakeDriverTxProducer struct{}

func (fakeDriverTxProducer) Begin(context.Context) error                              { return nil }
func (fakeDriverTxProducer) Produce(context.Context, []*message.ProducerRecord) error { return nil }
func (fakeDriverTxProducer) Commit(context.Context) error                             { return nil }
func (fakeDriverTxProducer) Abort(context.Context) error                              { return nil }
func (fakeDriverTxProducer) Close() error                                             { return nil }

func producerConfig(txID string) *Config {
	return &Config{Server: spec.KafkaServer{
		BootstrapServers: "broker:9092",
		Producer:         spec.ProducerTuning{TransactionalID: txID},
	}}
}

// startApp costruisce l'app dalle registrazioni accumulate e la avvia, ritornando l'IProducer visto da
// ROOT: è ciò che l'app inietta, quindi è il solo punto di osservazione che conta.
func startApp(t *testing.T) (producer.IProducer, func()) {
	t.Helper()
	var got producer.IProducer
	core.Invoke(func(p producer.IProducer) { got = p })

	app, err := core.Start(context.Background())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return got, func() { _ = app.Stop(context.Background()) }
}

func TestProducerModule_SenzaDriverPanica(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("ProducerModule senza WithDriver deve panicare")
		}
		msg, _ := r.(string)
		// Il messaggio deve nominare l'entry-point sbagliato, non solo "corekafka": chi legge deve
		// sapere QUALE chiamata correggere.
		for _, want := range []string{"ProducerModule", "WithDriver", "confluentdriver.Driver", "franzdriver.Driver"} {
			if !strings.Contains(msg, want) {
				t.Errorf("panic = %q, atteso contenesse %q", msg, want)
			}
		}
	}()
	ProducerModule(producerConfig(""))
}

func TestProducerModule_IProducerIniettabileARoot(t *testing.T) {
	f := &fakeFactory{}
	ProducerModule(producerConfig(""), WithDriver(func() { core.Provide(func() driver.Factory { return f }) }))

	got, stop := startApp(t)
	defer stop()

	if got == nil {
		t.Fatal("IProducer non risolvibile a root: il seam pubblico non è uscito dal modulo")
	}
	if f.tx {
		t.Error("senza transactional-id il wiring deve costruire il producer idempotente")
	}
}

// La transazionalità è una scelta della CONFIG: presenza dell'id ⇒ transazionale, e l'id deve arrivare
// al driver esattamente come scritto (è quello che il broker usa per il fencing).
func TestProducerModule_TransactionalIDDecideLaForma(t *testing.T) {
	f := &fakeFactory{}
	ProducerModule(producerConfig("notifiche-pod-0"), WithDriver(func() { core.Provide(func() driver.Factory { return f }) }))

	got, stop := startApp(t)
	defer stop()

	if got == nil {
		t.Fatal("IProducer non risolvibile a root")
	}
	if !f.tx {
		t.Fatal("con transactional-id valorizzato il wiring deve costruire il producer transazionale")
	}
	if f.txID != "notifiche-pod-0" {
		t.Errorf("transactional-id arrivato al driver = %q, atteso notifiche-pod-0", f.txID)
	}
}

// I processor in config non sono un errore: la stessa sezione serve più processi dello stesso
// deployment — uno consuma, un altro pubblica soltanto — ed è il senso della config univoca. Qui il
// producer si wira e l'engine dei consumer no.
func TestProducerModule_IgnoraIProcessors(t *testing.T) {
	cfg := producerConfig("")
	cfg.Processors = []spec.ProcessorSpec{{Name: "ingest", Topics: []string{"t"}, GroupID: "g"}}
	f := &fakeFactory{}

	ProducerModule(cfg, WithDriver(func() { core.Provide(func() driver.Factory { return f }) }))

	got, stop := startApp(t)
	defer stop()

	if got == nil {
		t.Fatal("i processor in config hanno impedito il wiring del producer")
	}
}

// Il gating vale come per Module: in un mode che non è tra quelli indicati non si registra nulla — non
// si costruisce un producer che nessuno userà, e non si apre una connessione ai broker.
func TestProducerModule_GatingPerMode(t *testing.T) {
	f := &fakeFactory{}
	ProducerModule(producerConfig(""),
		WithDriver(func() { core.Provide(func() driver.Factory { return f }) }),
		WithModes("UN_MODE_CHE_NON_MATCHA"))

	var got producer.IProducer
	core.Invoke(func(p producer.IProducer) { got = p })

	app, err := core.Start(context.Background())
	if err == nil {
		_ = app.Stop(context.Background())
		t.Fatal("l'IProducer è risolvibile in un mode non abilitato: il gating non ha effetto")
	}
	if got != nil {
		t.Error("producer costruito in un mode non abilitato")
	}
}

// --- Module + WithProducer: il producer esce dal sottosistema ---

// Con WithProducer l'app inietta lo stesso producer di ProducerModule, dalla stessa Config: è ciò che
// evita a un'app con consumer di riscrivere la connessione in una seconda sezione.
func TestModule_WithProducer_EsponeIlProducer(t *testing.T) {
	f := &fakeFactory{}
	Module(producerConfig(""), func() {},
		WithDriver(func() { core.Provide(func() driver.Factory { return f }) }),
		WithProducer())

	got, stop := startApp(t)
	defer stop()

	if got == nil {
		t.Fatal("IProducer non risolvibile a root: WithProducer non ha esposto il producer")
	}
}

// Senza WithProducer il producer resta PRIVATO al sottosistema: è il producer del DLQ, e un Handler che
// pubblicasse con quello avrebbe due esiti indipendenti dal commit degli offset (per produrre dentro un
// consumer c'è il Transformer). Qui il DLQ serve — un processor lo dichiara — quindi il producer esiste,
// ma non deve essere iniettabile.
func TestModule_SenzaWithProducer_IlDLQRestaPrivato(t *testing.T) {
	dlq := "ingest.DLQ"
	cfg := producerConfig("")
	cfg.Processors = []spec.ProcessorSpec{{
		Name: "ingest", Topics: []string{"t"}, GroupID: "g",
		Consumer: spec.ConsumerTuning{DeadletterTopic: &dlq},
	}}
	f := &fakeFactory{}
	Module(cfg, func() {}, WithDriver(func() { core.Provide(func() driver.Factory { return f }) }))
	core.Invoke(func(producer.IProducer) {})

	app, err := core.Start(context.Background())
	if err == nil {
		_ = app.Stop(context.Background())
		t.Fatal("l'IProducer del DLQ è iniettabile dall'app: il sottosistema non è più chiuso")
	}
}

// TestProducerModule_IlDriverRestaNelModulo: la driver.Factory è l'ingranaggio del producer, non un
// servizio dell'app. Resta dentro con core.Private — se fosse esportata, un secondo sottoalbero che la
// registra (un altro ProducerModule, o un Module con consumer accanto) darebbe un duplicate provide, e
// il grafo dell'app conterrebbe un tipo che nessuna app può nemmeno nominare.
func TestProducerModule_IlDriverRestaNelModulo(t *testing.T) {
	f := &fakeFactory{}
	ProducerModule(producerConfig(""), WithDriver(func() { core.Provide(func() driver.Factory { return f }) }))
	core.Invoke(func(driver.Factory) {})

	app, err := core.Start(context.Background())
	if err == nil {
		_ = app.Stop(context.Background())
		t.Fatal("la driver.Factory è iniettabile a root: core.Private non ha effetto")
	}
}
