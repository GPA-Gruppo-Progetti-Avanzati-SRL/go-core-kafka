package producer

import (
	"context"
	"errors"
	"strings"
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
	tx  driver.TxProducer
	// got registra il tuning ricevuto: serve a verificare che i default siano applicati PRIMA di
	// arrivare al driver.
	got spec.ProducerTuning
	// gotTxID registra l'id passato al driver: è il valore che decide la transazionalità, quindi
	// deve arrivare al client esattamente come scritto in config.
	gotTxID string
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

func (f *fakeFactory) NewTxProducer(_ spec.KafkaServer, p spec.ProducerTuning, id string) (driver.TxProducer, error) {
	f.got = p
	f.gotTxID = id
	return f.tx, f.err
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

// --- ProduceTo ---

func TestProduceTo_NonSovrascriveIlTopicDelRecord(t *testing.T) {
	// Il topic passato è il DEFAULT per chi non ne ha uno: sovrascrivere quello già impostato
	// impedirebbe il fan-out nella stessa chiamata (un record verso il DLQ in mezzo agli altri), e lo
	// farebbe in silenzio.
	d := &fakeDriverProducer{}
	p, err := NewProducer(&fakeLifecycle{}, &fakeFactory{p: d}, validServer(), spec.ProducerTuning{})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	recs := []*message.ProducerRecord{{Value: []byte("a")}, {Topic: "suo", Value: []byte("b")}}
	if appErr := p.ProduceTo(context.Background(), "default", recs); appErr != nil {
		t.Fatalf("ProduceTo = %v", appErr)
	}
	if d.produced[0].Topic != "default" {
		t.Errorf("record senza topic: %q, atteso default", d.produced[0].Topic)
	}
	if d.produced[1].Topic != "suo" {
		t.Errorf("record con topic proprio: %q, atteso suo (non sovrascritto)", d.produced[1].Topic)
	}
}

// --- producer transazionale ---

// fakeTx registra la sequenza delle chiamate: della transazione conta l'ordine, e un abort mancante
// lascia la transazione aperta lato broker — con i consumer read_committed bloccati su quelle
// partizioni fino al transaction.timeout.ms.
type fakeTx struct {
	calls      []string
	produced   []*message.ProducerRecord
	beginErr   error
	produceErr error
	commitErr  error
	closed     bool
}

func (f *fakeTx) Begin(context.Context) error { f.calls = append(f.calls, "begin"); return f.beginErr }

func (f *fakeTx) Produce(_ context.Context, recs []*message.ProducerRecord) error {
	f.calls = append(f.calls, "produce")
	f.produced = append(f.produced, recs...)
	return f.produceErr
}

func (f *fakeTx) Commit(context.Context) error {
	f.calls = append(f.calls, "commit")
	return f.commitErr
}

func (f *fakeTx) Abort(context.Context) error { f.calls = append(f.calls, "abort"); return nil }
func (f *fakeTx) Close() error                { f.closed = true; return nil }

func (f *fakeTx) seq() string { return strings.Join(f.calls, ",") }

func newTx(t *testing.T, d *fakeTx, id string) (*TxProducer, *fakeFactory) {
	t.Helper()
	f := &fakeFactory{tx: d}
	p, err := NewTxProducer(&fakeLifecycle{}, f, validServer(), spec.ProducerTuning{TransactionalID: id})
	if err != nil {
		t.Fatalf("NewTxProducer: %v", err)
	}
	return p, f
}

func TestTxProducer_UnaTransazionePerProduce(t *testing.T) {
	d := &fakeTx{}
	p, f := newTx(t, d, "notifiche-pod-0")

	if appErr := p.Produce(context.Background(), []*message.ProducerRecord{{Topic: "t"}}); appErr != nil {
		t.Fatalf("Produce = %v", appErr)
	}
	if got := d.seq(); got != "begin,produce,commit" {
		t.Fatalf("sequenza = %s, attesa begin,produce,commit", got)
	}
	// L'id scritto in config deve arrivare al client: è ciò che il broker usa per il fencing, quindi
	// un id perso per strada significa un producer non transazionale che si crede transazionale.
	if f.gotTxID != "notifiche-pod-0" {
		t.Errorf("transactional-id passato al driver = %q, atteso notifiche-pod-0", f.gotTxID)
	}
}

func TestTxProducer_BatchVuotoNonApreTransazioni(t *testing.T) {
	d := &fakeTx{}
	p, _ := newTx(t, d, "id")

	if appErr := p.Produce(context.Background(), nil); appErr != nil {
		t.Fatalf("Produce di un batch vuoto = %v, atteso nil", appErr)
	}
	if len(d.calls) != 0 {
		t.Fatalf("un batch vuoto ha aperto una transazione: %v", d.calls)
	}
}

func TestTxProducer_AbortaSuOgniErrore(t *testing.T) {
	tests := []struct {
		name string
		d    *fakeTx
		seq  string
	}{
		{"produce fallito", &fakeTx{produceErr: errors.New("broker giù")}, "begin,produce,abort"},
		// Anche dopo un commit fallito: la transazione è ancora aperta lato broker, e lasciarla tale
		// blocca i consumer read_committed su quelle partizioni.
		{"commit fallito", &fakeTx{commitErr: errors.New("fenced")}, "begin,produce,commit,abort"},
		// Begin fallito: non c'è nulla da abortire, e chiederlo darebbe un errore di stato invalido.
		{"begin fallito", &fakeTx{beginErr: errors.New("init")}, "begin"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTx(t, tc.d, "id")
			if appErr := p.Produce(context.Background(), []*message.ProducerRecord{{Topic: "t"}}); appErr == nil {
				t.Fatal("atteso errore")
			}
			if got := tc.d.seq(); got != tc.seq {
				t.Fatalf("sequenza = %s, attesa %s", got, tc.seq)
			}
		})
	}
}

func TestTxProducer_AbortaAncheSuContextScaduto(t *testing.T) {
	// L'abort non può viaggiare sul context del chiamante: quando si arriva qui il motivo è spesso
	// proprio una deadline scaduta, e su un context cancellato l'abort non partirebbe — lasciando
	// aperta la transazione che si sta cercando di chiudere.
	d := &fakeTx{produceErr: context.DeadlineExceeded}
	p, _ := newTx(t, d, "id")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if appErr := p.Produce(ctx, []*message.ProducerRecord{{Topic: "t"}}); appErr == nil {
		t.Fatal("atteso errore")
	}
	if got := d.seq(); got != "begin,produce,abort" {
		t.Fatalf("sequenza = %s: l'abort non è stato tentato su un context cancellato", got)
	}
}

func TestTxProducer_OnStopChiudeIlClient(t *testing.T) {
	lc := &fakeLifecycle{}
	d := &fakeTx{}
	if _, err := NewTxProducer(lc, &fakeFactory{tx: d}, validServer(), spec.ProducerTuning{TransactionalID: "id"}); err != nil {
		t.Fatalf("NewTxProducer: %v", err)
	}
	if len(lc.hooks) != 1 {
		t.Fatalf("hook registrati = %d, atteso 1", len(lc.hooks))
	}
	if err := lc.hooks[0].OnStop(context.Background()); err != nil {
		t.Errorf("OnStop = %v", err)
	}
	if !d.closed {
		t.Error("il producer transazionale non è stato chiuso")
	}
}

// I due tipi devono soddisfare il contratto che l'app inietta: senza questa asserzione il mismatch
// comparirebbe solo al wiring, come "missing type" di fx.
var (
	_ IProducer = (*Producer)(nil)
	_ IProducer = (*TxProducer)(nil)
)
