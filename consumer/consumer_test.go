package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"go.uber.org/fx"
)

// --- fake driver -----------------------------------------------------------------------------
//
// Le interfacce di internal/driver sono l'unico confine fra engine e client: un fake che le
// implementa permette di pilotare esattamente la sequenza poll/commit/errore che l'engine deve
// gestire, cosa impossibile contro un broker reale (un rebalance o un fencing non si producono a
// comando).

type pollEvent struct {
	rec *message.Record
	err error
}

type fakeGroupConsumer struct {
	mu      sync.Mutex
	events  []pollEvent
	i       int
	exhaust error // ritornato quando gli eventi finiscono: fa terminare runOnce in modo controllato
	commits int
	batches [][]*message.Record
	closed  bool
	// discards conta le Discard ricevute e ops registra la sequenza delle operazioni: è su
	// quest'ordine che poggia la garanzia "duplicati, mai buchi" (uno scarto DEVE precedere il
	// commit successivo, altrimenti quel commit conferma gli offset del batch buttato).
	discards int
	ops      []string
}

func (f *fakeGroupConsumer) Poll(context.Context, time.Duration) (*message.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.i >= len(f.events) {
		return nil, f.exhaust
	}
	e := f.events[f.i]
	f.i++
	return e.rec, e.err
}

func (f *fakeGroupConsumer) Commit(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	f.ops = append(f.ops, "commit")
	return nil
}

// progress dice quanti eventi programmati sono già stati consumati: serve ai test che devono
// sincronizzarsi col loop prima di cancellare il context.
func (f *fakeGroupConsumer) progress() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.i
}

func (f *fakeGroupConsumer) Discard(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discards++
	f.ops = append(f.ops, "discard")
}

func (f *fakeGroupConsumer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// fakeFactory consegna un client per tentativo di run: così un test può far fallire il primo e
// verificare che il secondo riparta. Esaurita la lista, ritorna errFactoryExhausted.
type fakeFactory struct {
	mu        sync.Mutex
	consumers []driver.GroupConsumer
	sessions  []driver.TransactSession // modalità transform
	producers []driver.Producer        // producer condiviso del DLQ
	errs      []error                  // errore di creazione per tentativo (prevale sul consumer di pari indice)
	calls     int
}

var errFactoryExhausted = errors.New("fake: nessun altro client da consegnare")

func (f *fakeFactory) NewGroupConsumer(spec.ProcessorSpec, spec.KafkaServer) (driver.GroupConsumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i < len(f.consumers) {
		return f.consumers[i], nil
	}
	return nil, errFactoryExhausted
}

func (f *fakeFactory) NewTransactSession(spec.ProcessorSpec, spec.KafkaServer) (driver.TransactSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i < len(f.sessions) {
		return f.sessions[i], nil
	}
	return nil, errFactoryExhausted
}

func (f *fakeFactory) NewProducer(spec.KafkaServer, spec.ProducerTuning) (driver.Producer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.producers) == 0 {
		return nil, errors.New("nessun producer configurato in questo test")
	}
	p := f.producers[0]
	f.producers = f.producers[1:]
	return p, nil
}

func (f *fakeFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// handlerFunc adatta una funzione al contratto Handler.
type handlerFunc func(context.Context, []*message.Record) error

func (h handlerFunc) Handle(ctx context.Context, b []*message.Record) error { return h(ctx, b) }

// testSpec è uno spec con backoff volutamente minuscolo: i test verificano la LOGICA del
// supervisore, non la durata delle attese. Passa da Resolve — lo stesso percorso della produzione —
// con un blocco globale vuoto.
func testSpec() spec.ProcessorSpec {
	s := spec.ProcessorSpec{Name: "test", GroupID: "g", Topics: []string{"t"}}
	s.Consumer.MaxBatchSize = 2
	s.Restart = spec.RestartSpec{InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, Multiplier: ptr(2.0), ResetAfter: time.Hour}
	return s.Resolve(spec.KafkaServer{})
}

func ptr[T any](v T) *T { return &v }

func rec(topic string, partition int32, offset int64) *message.Record {
	return &message.Record{Topic: topic, Partition: partition, Offset: offset, Value: []byte("v")}
}

// --- classify --------------------------------------------------------------------------------

// classify è condivisa dalle due modalità proprio perché prima divergevano: handle valutava
// ErrFailFast prima di PoisonRecords, transform il contrario, quindi lo stesso errore composito
// veniva classificato in due modi. Questa tabella è il vincolo che impedisce di tornare indietro.
func TestClassify(t *testing.T) {
	batch := []*message.Record{rec("t", 0, 1), rec("t", 0, 2)}
	poison := []*message.Record{batch[0]}
	cause := errors.New("payload non valido")

	tests := []struct {
		name        string
		err         error
		onError     string
		wantOutcome outcome
		wantPoison  int
	}{
		{"nil committa", nil, spec.OnErrorFailFast, outcomeCommit, 0},
		{"ErrFailFast non committa", processor.ErrFailFast, spec.OnErrorDeadletter, outcomeFail, 0},
		{"ErrFailFast wrappato", fmt.Errorf("ctx: %w", processor.ErrFailFast), spec.OnErrorDeadletter, outcomeFail, 0},
		{"DeadLetter instrada i soli poison", processor.DeadLetter(cause, poison...), spec.OnErrorFailFast, outcomeDeadletter, 1},
		{"errore generico con policy fail-fast", cause, spec.OnErrorFailFast, outcomeFail, 0},
		{"errore generico con policy deadletter manda tutto il batch", cause, spec.OnErrorDeadletter, outcomeDeadletter, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oc, got, _ := classify(tc.err, batch, tc.onError)
			if oc != tc.wantOutcome {
				t.Errorf("outcome = %v, atteso %v", oc, tc.wantOutcome)
			}
			if len(poisonRecords(got)) != tc.wantPoison {
				t.Errorf("record poison = %d, attesi %d", len(poisonRecords(got)), tc.wantPoison)
			}
		})
	}
}

func TestClassify_ErrFailFastVinceSuDeadLetter(t *testing.T) {
	// Un errore che è insieme DeadLetter e ErrFailFast: la scelta esplicita di uscire vince. È il
	// caso su cui le due modalità divergevano.
	batch := []*message.Record{rec("t", 0, 1)}
	err := errors.Join(processor.DeadLetter(errors.New("x"), batch...), processor.ErrFailFast)

	if oc, _, _ := classify(err, batch, spec.OnErrorDeadletter); oc != outcomeFail {
		t.Errorf("outcome = %v, atteso outcomeFail", oc)
	}
}

func TestClassify_PreservaLaCausa(t *testing.T) {
	// La causa allegata a DeadLetter finisce negli header del record DLQ: perderla renderebbe il DLQ
	// indiagnosticabile.
	cause := errors.New("json: unexpected end of input")
	batch := []*message.Record{rec("t", 0, 1)}

	_, _, got := classify(processor.DeadLetter(cause, batch...), batch, spec.OnErrorFailFast)
	if !errors.Is(got, cause) {
		t.Errorf("causa = %v, attesa %v", got, cause)
	}
}

// --- supervisore -----------------------------------------------------------------------------

func TestRun_PermanentNonRiavvia(t *testing.T) {
	// Credenziali errate o config rifiutata: riprovare produce lo stesso errore all'infinito, quindi
	// l'errore deve risalire e far terminare il processo.
	permanent := driver.NewError(driver.SeverityPermanent, "connect", errors.New("auth fallita"))
	f := &fakeFactory{errs: []error{permanent}}
	r := &runner{spec: testSpec(), factory: f, handler: handlerFunc(func(context.Context, []*message.Record) error { return nil })}

	err := r.run(context.Background())
	if !errors.Is(err, permanent) {
		t.Fatalf("err = %v, atteso l'errore permanent", err)
	}
	if n := f.callCount(); n != 1 {
		t.Errorf("tentativi = %d, atteso 1 (nessun riavvio)", n)
	}
}

func TestRun_RetriableRiavviaFinoAMaxAttempts(t *testing.T) {
	retriable := driver.NewError(driver.SeverityRetriable, "connect", errors.New("broker giù"))
	f := &fakeFactory{errs: []error{retriable, retriable, retriable, retriable, retriable}}
	s := testSpec()
	s.Restart.MaxAttempts = ptr(3)
	r := &runner{spec: s, factory: f, handler: handlerFunc(func(context.Context, []*message.Record) error { return nil })}

	err := r.run(context.Background())
	if err == nil {
		t.Fatal("atteso errore dopo l'esaurimento dei tentativi")
	}
	if !errors.Is(err, retriable) {
		t.Errorf("l'errore finale deve avvolgere la causa: %v", err)
	}
	// 1 tentativo iniziale + 3 riavvii.
	if n := f.callCount(); n != 4 {
		t.Errorf("tentativi = %d, attesi 4 (1 + 3 riavvii)", n)
	}
}

func TestRun_RestartDisabledRipristinaIlComportamentoStorico(t *testing.T) {
	retriable := driver.NewError(driver.SeverityRetriable, "connect", errors.New("broker giù"))
	f := &fakeFactory{errs: []error{retriable, retriable}}
	s := testSpec()
	s.Restart.Disabled = ptr(true)
	r := &runner{spec: s, factory: f, handler: handlerFunc(func(context.Context, []*message.Record) error { return nil })}

	if err := r.run(context.Background()); !errors.Is(err, retriable) {
		t.Fatalf("err = %v, atteso l'errore retriable", err)
	}
	if n := f.callCount(); n != 1 {
		t.Errorf("tentativi = %d, atteso 1: con restart.disabled non si riavvia", n)
	}
}

func TestRun_ErroreDiBusinessNonRiavviaPerDefault(t *testing.T) {
	// on-error=fail-fast documenta "non committa ed esce": riprovare in-process un record poison
	// sarebbe un loop senza uscita. L'errore dell'handler NON è un driver.Error, quindi è Business.
	business := errors.New("record non valido")
	f := &fakeFactory{consumers: []driver.GroupConsumer{
		&fakeGroupConsumer{events: []pollEvent{{rec: rec("t", 0, 1)}, {rec: rec("t", 0, 2)}}, exhaust: errors.New("mai raggiunto")},
	}}
	r := &runner{spec: testSpec(), factory: f, handler: handlerFunc(func(context.Context, []*message.Record) error { return business })}

	if err := r.run(context.Background()); !errors.Is(err, business) {
		t.Fatalf("err = %v, atteso l'errore di business", err)
	}
	if n := f.callCount(); n != 1 {
		t.Errorf("tentativi = %d, atteso 1: un errore di business non riavvia per default", n)
	}
}

func TestRun_ErroreDiBusinessRiavviaSeRichiesto(t *testing.T) {
	// on-business-error: true serve quando la causa attesa è un'infrastruttura applicativa
	// transitoria (il DB irraggiungibile), non un payload malformato.
	business := errors.New("mongo irraggiungibile")
	f := &fakeFactory{consumers: []driver.GroupConsumer{
		&fakeGroupConsumer{events: []pollEvent{{rec: rec("t", 0, 1)}, {rec: rec("t", 0, 2)}}},
		&fakeGroupConsumer{events: []pollEvent{{rec: rec("t", 0, 3)}, {rec: rec("t", 0, 4)}}},
	}}
	s := testSpec()
	s.Restart.OnBusinessError = ptr(true)
	s.Restart.MaxAttempts = ptr(1)
	r := &runner{spec: s, factory: f, handler: handlerFunc(func(context.Context, []*message.Record) error { return business })}

	if err := r.run(context.Background()); err == nil {
		t.Fatal("atteso errore dopo l'esaurimento del tentativo")
	}
	if n := f.callCount(); n != 2 {
		t.Errorf("tentativi = %d, attesi 2 (1 + 1 riavvio)", n)
	}
}

func TestRun_ContextAnnullatoDuranteIlBackoffEscePulito(t *testing.T) {
	// Uno shutdown durante l'attesa non deve né propagare l'errore né restare appeso al backoff.
	s := testSpec()
	s.Restart.InitialBackoff = 30 * time.Second
	f := &fakeFactory{errs: []error{driver.NewError(driver.SeverityRetriable, "connect", errors.New("giù"))}}
	r := &runner{spec: s, factory: f, handler: handlerFunc(func(context.Context, []*message.Record) error { return nil })}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.run(ctx) }()

	time.Sleep(20 * time.Millisecond) // lascia entrare il run nel backoff
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("err = %v, atteso nil su shutdown cooperativo", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run non è uscito: il backoff non osserva la cancellazione del context")
	}
}

func TestRestartable(t *testing.T) {
	tests := []struct {
		sev             driver.Severity
		onBusinessError bool
		want            bool
	}{
		{driver.SeverityPermanent, false, false},
		{driver.SeverityPermanent, true, false}, // permanent resta letale anche con il flag
		{driver.SeverityBusiness, false, false},
		{driver.SeverityBusiness, true, true},
		{driver.SeverityFatal, false, true},
		{driver.SeverityRetriable, false, true},
		{driver.SeverityReset, false, true},
		{driver.SeverityAbort, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.sev.String()+"/onBusinessError="+strconv.FormatBool(tc.onBusinessError), func(t *testing.T) {
			s := testSpec()
			s.Restart.OnBusinessError = ptr(tc.onBusinessError)
			r := &runner{spec: s}
			if got := r.restartable(tc.sev); got != tc.want {
				t.Errorf("restartable(%v) = %v, atteso %v", tc.sev, got, tc.want)
			}
		})
	}
}

// --- rebalance / batch scartato ---------------------------------------------------------------

func TestRunHandle_RebalanceScartaIlBatchSenzaCommit(t *testing.T) {
	// Il cuore del fix sugli offset: dopo una revoca i record accumulati vengono da partizioni che
	// potrebbero non essere più nostre. Committarli li dichiarerebbe elaborati mentre il nuovo owner
	// li sta rileggendo — perdita di messaggi. Vanno scartati: duplicati, non buchi.
	reset := driver.NewError(driver.SeverityReset, "poll", errors.New("rebalance"))
	fc := &fakeGroupConsumer{
		events: []pollEvent{
			{rec: rec("t", 0, 1)}, // accumulato (MaxBatchSize=2, quindi nessun flush)
			{err: reset},          // rebalance: il record sopra va scartato
			{rec: rec("t", 0, 5)}, // nuova assegnazione: si riprende a consumare
		},
		exhaust: driver.NewError(driver.SeverityPermanent, "poll", errors.New("stop")),
	}
	var handled [][]*message.Record
	f := &fakeFactory{consumers: []driver.GroupConsumer{fc}}
	r := &runner{spec: testSpec(), factory: f, handler: handlerFunc(func(_ context.Context, b []*message.Record) error {
		handled = append(handled, append([]*message.Record(nil), b...))
		return nil
	})}

	_ = r.run(context.Background())

	// Nessun commit: l'unico batch mai chiuso sarebbe quello scartato.
	if fc.commits != 0 {
		t.Errorf("commit = %d, atteso 0: il batch scartato non va committato", fc.commits)
	}
	// L'handler non deve nemmeno vedere il batch invalidato.
	for _, b := range handled {
		for _, rr := range b {
			if rr.Offset == 1 {
				t.Errorf("l'handler ha ricevuto un record del batch invalidato dal rebalance (offset %d)", rr.Offset)
			}
		}
	}
	if !fc.closed {
		t.Error("il client non è stato chiuso all'uscita dal loop")
	}
}

func TestAbsorb(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantAbsorb bool
	}{
		{"reset (rebalance) assorbito", driver.NewError(driver.SeverityReset, "poll", errors.New("x")), true},
		{"abort transazionale assorbito", driver.NewError(driver.SeverityAbort, "commit", errors.New("x")), true},
		{"retriable non assorbito: serve un client nuovo", driver.NewError(driver.SeverityRetriable, "poll", errors.New("x")), false},
		{"fatal non assorbito", driver.NewError(driver.SeverityFatal, "poll", errors.New("x")), false},
		{"business non assorbito", errors.New("x"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &runner{spec: testSpec()}
			fc := &fakeGroupConsumer{}
			batch := []*message.Record{rec("t", 0, 1), rec("t", 0, 2)}
			got := r.absorb(context.Background(), tc.err, fc, &batch)
			if got != tc.wantAbsorb {
				t.Fatalf("absorb = %v, atteso %v", got, tc.wantAbsorb)
			}
			// Troncare la slice non basta: gli offset stanno nel driver, e restarci significa che il
			// prossimo commit li conferma senza che nessuno li abbia elaborati.
			if want := map[bool]int{true: 1, false: 0}[tc.wantAbsorb]; fc.discards != want {
				t.Errorf("Discard chiamata %d volte, attese %d", fc.discards, want)
			}
			if tc.wantAbsorb && len(batch) != 0 {
				t.Errorf("batch non svuotato: %d record residui", len(batch))
			}
			if !tc.wantAbsorb && len(batch) != 2 {
				t.Errorf("batch alterato per un errore non assorbito: %d record", len(batch))
			}
		})
	}
}

// --- header DLQ ------------------------------------------------------------------------------

func TestToDLQ_HeaderDiOrigine(t *testing.T) {
	ts := time.Date(2026, 8, 26, 10, 30, 0, 0, time.UTC)
	src := &message.Record{
		Topic: "eventi", Partition: 3, Offset: 42,
		Key: []byte("k"), Value: []byte("payload"),
		Headers:   message.Headers{{Key: "traceparent", Value: []byte("abc")}},
		Timestamp: ts,
	}
	s := testSpec()
	s.Consumer.DeadletterTopic = ptr("eventi.DLQ")
	r := &runner{spec: s}

	out := r.toDLQ(processor.DeadLetter(errors.New("boom"), src))
	if len(out) != 1 {
		t.Fatalf("record prodotti = %d, atteso 1", len(out))
	}
	got := out[0]

	if got.Topic != "eventi.DLQ" {
		t.Errorf("topic = %q, atteso eventi.DLQ", got.Topic)
	}
	if string(got.Key) != "k" || string(got.Value) != "payload" {
		t.Error("chiave o payload alterati: il DLQ deve preservare il record originale")
	}
	// Gli header originali sopravvivono: la correlazione di trace non deve rompersi nel DLQ.
	if got.Headers.Get("traceparent") != "abc" {
		t.Error("header originali non propagati")
	}
	want := map[string]string{
		HeaderDLQSourceTopic:     "eventi",
		HeaderDLQSourcePartition: "3",
		HeaderDLQSourceOffset:    "42",
		HeaderDLQProcessor:       "test",
		HeaderDLQError:           "boom",
		HeaderDeliveryAttempts:   "1",
	}
	for k, v := range want {
		if got.Headers.Get(k) != v {
			t.Errorf("header %s = %q, atteso %q", k, got.Headers.Get(k), v)
		}
	}
	if got.Headers.Get(HeaderDLQSourceTimestamp) != ts.Format(time.RFC3339Nano) {
		t.Errorf("timestamp di origine = %q", got.Headers.Get(HeaderDLQSourceTimestamp))
	}
	if got.Headers.Get(HeaderDLQErrorAt) == "" {
		t.Error("manca l'istante dell'errore")
	}
}

func TestToDLQ_IncrementaITentativi(t *testing.T) {
	// Un record ripescato dal DLQ e reimmesso porta il contatore: senza incremento, chi riprocessa
	// non ha modo di fermarsi.
	s := testSpec()
	s.Consumer.DeadletterTopic = ptr("dlq")
	r := &runner{spec: s}

	src := &message.Record{Topic: "t", Headers: message.Headers{{Key: HeaderDeliveryAttempts, Value: []byte("2")}}}
	if got := r.toDLQ(processor.DeadLetter(nil, src))[0].Headers.Get(HeaderDeliveryAttempts); got != "3" {
		t.Errorf("tentativi = %q, atteso 3", got)
	}

	// Un contatore illeggibile non deve far esplodere nulla: si riparte da 1.
	src = &message.Record{Topic: "t", Headers: message.Headers{{Key: HeaderDeliveryAttempts, Value: []byte("non-un-numero")}}}
	if got := r.toDLQ(processor.DeadLetter(nil, src))[0].Headers.Get(HeaderDeliveryAttempts); got != "1" {
		t.Errorf("tentativi = %q, atteso 1", got)
	}
}

func TestToDLQ_SenzaCausa(t *testing.T) {
	s := testSpec()
	s.Consumer.DeadletterTopic = ptr("dlq")
	r := &runner{spec: s}
	if r.toDLQ(processor.DeadLetter(nil, &message.Record{Topic: "t"}))[0].Headers.Has(HeaderDLQError) {
		t.Error("header di errore presente senza causa")
	}
}

// --- backoff ---------------------------------------------------------------------------------

func TestBackoff_Esponenziale(t *testing.T) {
	// max-attempts: -1 = illimitati, esplicito: il budget di default è finito (5), quindi la
	// sequenza sotto va oltre e senza l'opt-in i tentativi finirebbero.
	b := newBackoff(spec.RestartSpec{InitialBackoff: time.Second, MaxBackoff: 8 * time.Second, Multiplier: ptr(2.0), MaxAttempts: ptr(-1)})

	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for i, w := range want {
		got, ok := b.next()
		if !ok {
			t.Fatalf("tentativo %d rifiutato con max-attempts=-1 (illimitati)", i)
		}
		if got != w {
			t.Errorf("attesa %d = %v, attesa %v", i, got, w)
		}
	}
}

func TestBackoff_MaxAttempts(t *testing.T) {
	b := newBackoff(spec.RestartSpec{InitialBackoff: time.Millisecond, MaxBackoff: time.Second, Multiplier: ptr(2.0), MaxAttempts: ptr(2)})

	for i := range 2 {
		if _, ok := b.next(); !ok {
			t.Fatalf("tentativo %d rifiutato, MaxAttempts=2", i)
		}
	}
	if _, ok := b.next(); ok {
		t.Error("terzo tentativo concesso con MaxAttempts=2")
	}
}

func TestBackoff_Reset(t *testing.T) {
	b := newBackoff(spec.RestartSpec{InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: ptr(2.0), MaxAttempts: ptr(2)})
	b.next()
	b.next()

	b.reset() // un run sano per almeno ResetAfter azzera il credito

	got, ok := b.next()
	if !ok {
		t.Fatal("dopo reset i tentativi devono ripartire")
	}
	if got != time.Second {
		t.Errorf("attesa dopo reset = %v, atteso l'initial-backoff", got)
	}
}

// Prima delle cause per-record, toDLQ etichettava OGNI messaggio con la stessa stringa: chi leggeva il
// DLQ vedeva la causa del gruppo ("payload non valido"), mai il motivo del singolo record.
func TestToDLQ_CausaPerRecord(t *testing.T) {
	uno := &message.Record{Topic: "eventi", Partition: 1, Offset: 10, Value: []byte("a")}
	due := &message.Record{Topic: "eventi", Partition: 1, Offset: 11, Value: []byte("b")}
	tre := &message.Record{Topic: "eventi", Partition: 1, Offset: 12, Value: []byte("c")}

	s := testSpec()
	s.Consumer.DeadletterTopic = ptr("eventi.DLQ")
	r := &runner{spec: s}

	pr := processor.DeadLetterEach([]processor.PoisonRecord{
		{Record: uno, Cause: errors.New("CDSERINT non valido")},
		{Record: due, Cause: errors.New("AUD_ENTTYP non riconosciuto")},
		{Record: tre, Cause: nil}, // nessuna causa specifica: ricade su quella comune
	})
	out := r.toDLQ(pr)

	if got := out[0].Headers.Get(HeaderDLQError); got != "CDSERINT non valido" {
		t.Errorf("header d'errore del primo record = %q", got)
	}
	if got := out[1].Headers.Get(HeaderDLQError); got != "AUD_ENTTYP non riconosciuto" {
		t.Errorf("header d'errore del secondo record = %q", got)
	}
	if got := out[2].Headers.Get(HeaderDLQError); got != pr.Cause.Error() {
		t.Errorf("senza causa propria il record deve ricadere su quella comune, ha %q", got)
	}
}

// --- §7.1: lo scarto del batch deve scartare anche gli offset nel driver -----------------------

func TestRunHandle_ResetSenzaRevocaScartaGliOffsetDelDriver(t *testing.T) {
	// Il caso che sfuggiva alla protezione del rebalance callback: un SeverityReset può risalire da
	// Poll/Commit SENZA che una revoca sia avvenuta — resetCodes include ErrIllegalGeneration,
	// ErrUnknownMemberID, ErrMemberIDRequired, ErrMaxPollExceeded. Lì il tracker del driver resta
	// pieno, quindi il PRIMO commit successivo conferma anche gli offset del batch buttato: record
	// dichiarati elaborati che nessuno ha elaborato, cioè un buco.
	reset := driver.NewError(driver.SeverityReset, "poll", errors.New("illegal generation"))
	fc := &fakeGroupConsumer{
		events: []pollEvent{
			{rec: rec("t", 0, 1)}, // accumulato (MaxBatchSize=2: nessun flush)
			{err: reset},          // scarto, senza che il callback di revoca sia girato
			{rec: rec("t", 0, 5)}, // nuovo batch...
			{rec: rec("t", 0, 6)}, // ...pieno: flush + commit
		},
		exhaust: driver.NewError(driver.SeverityPermanent, "poll", errors.New("stop")),
	}
	f := &fakeFactory{consumers: []driver.GroupConsumer{fc}}
	r := &runner{spec: testSpec(), factory: f, handler: handlerFunc(func(context.Context, []*message.Record) error { return nil })}

	_ = r.run(context.Background())

	if fc.discards != 1 {
		t.Errorf("Discard chiamata %d volte, attesa 1: senza, gli offset del batch scartato sopravvivono", fc.discards)
	}
	if len(fc.ops) < 2 || fc.ops[0] != "discard" {
		t.Errorf("sequenza delle operazioni = %v, atteso lo scarto PRIMA del commit", fc.ops)
	}
	if fc.commits != 1 {
		t.Errorf("commit = %d, atteso 1 (il solo batch valido)", fc.commits)
	}
}

// --- §7.2: il flush finale gira sul context dell'arresto, non su quello del loop ---------------

// waitFor attende una condizione con un bound: un test non deve poter appendersi.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condizione non verificata entro il timeout")
}

func TestConsume_FlushFinaleUsaIlContextDellArresto(t *testing.T) {
	// Prima il flush finale girava sul context appena cancellato da OnStop: Handle, sendDeadletter e
	// Commit abortivano immediatamente, quindi a ogni SIGTERM si riprocessava un batch intero — con i
	// side-effect già eseguiti dall'handler nel tentativo precedente.
	fc := &fakeGroupConsumer{events: []pollEvent{{rec: rec("t", 0, 1)}}} // exhaust nil: poi Poll è a vuoto
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	sd := &shutdown{}
	sd.set(stopCtx)

	var mu sync.Mutex
	var seen error
	r := &runner{spec: testSpec(), stop: sd, handler: handlerFunc(func(ctx context.Context, _ []*message.Record) error {
		mu.Lock()
		defer mu.Unlock()
		seen = ctx.Err() // nil = il context del flush finale è vivo
		return nil
	})}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.consume(ctx, fc, handleFlusher{r: r, gc: fc}) }()
	waitFor(t, func() bool { return fc.progress() >= 1 }) // il record è nel batch
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume ha ritornato errore in arresto: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consume non è tornata dopo la cancellazione")
	}

	mu.Lock()
	defer mu.Unlock()
	if seen != nil {
		t.Errorf("il flush finale ha girato su un context già cancellato (%v): l'handler non può fare nulla e il commit non parte", seen)
	}
	if fc.commits != 1 {
		t.Errorf("commit = %d, atteso 1: il batch elaborato in arresto va confermato, altrimenti è riprocessato al riavvio", fc.commits)
	}
}

func TestConsume_FlushFinaleConArrestoGiaScadutoNonBlocca(t *testing.T) {
	// Se la deadline dell'arresto è già passata il flush fallisce: l'esito va loggato e il loop deve
	// uscire comunque (i record saranno riconsumati). Ciò che non deve fare è appendersi.
	fc := &fakeGroupConsumer{events: []pollEvent{{rec: rec("t", 0, 1)}}}
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	sd := &shutdown{}
	sd.set(expired)

	r := &runner{spec: testSpec(), stop: sd, handler: handlerFunc(func(ctx context.Context, _ []*message.Record) error {
		return ctx.Err() // un handler reale fallisce così: la chiamata al DB non parte
	})}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.consume(ctx, fc, handleFlusher{r: r, gc: fc}) }()
	waitFor(t, func() bool { return fc.progress() >= 1 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume deve uscire pulita anche se il flush finale fallisce, ha ritornato %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consume non è tornata")
	}
	if fc.commits != 0 {
		t.Errorf("commit = %d, atteso 0: un flush fallito non deve committare", fc.commits)
	}
}

func TestShutdown_FlushContextHaSempreUnaDeadline(t *testing.T) {
	// Un flush finale senza bound terrebbe il processo in piedi a tempo indefinito, che è l'opposto
	// di ciò che un SIGTERM chiede.
	for _, tc := range []struct {
		name string
		set  func(*shutdown)
	}{
		{"arresto non depositato", func(*shutdown) {}},
		{"hook senza deadline (fx.StopTimeout non impostato)", func(s *shutdown) { s.set(context.Background()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sd := &shutdown{}
			tc.set(sd)
			ctx, cancel := sd.flushContext()
			defer cancel()
			if _, ok := ctx.Deadline(); !ok {
				t.Error("il context del flush finale non ha deadline")
			}
		})
	}
}

// --- OnStart / OnStop ------------------------------------------------------------------------

type fakeLifecycle struct{ hooks []fx.Hook }

func (l *fakeLifecycle) Append(h fx.Hook) { l.hooks = append(l.hooks, h) }

type fakeShutdowner struct{}

func (fakeShutdowner) Shutdown(...fx.ShutdownOption) error { return nil }

// hangingConsumer ignora la cancellazione: modella un client CGo bloccato dentro ReadMessage, il
// caso in cui l'attesa dei runner in OnStop non tornava mai.
type hangingConsumer struct{ block chan struct{} }

func (h *hangingConsumer) Poll(context.Context, time.Duration) (*message.Record, error) {
	<-h.block
	return nil, nil
}
func (h *hangingConsumer) Commit(context.Context) error { return nil }
func (h *hangingConsumer) Discard(context.Context)      {}
func (h *hangingConsumer) Close() error                 { return nil }

func newEngine(t *testing.T, gc driver.GroupConsumer) *fakeLifecycle {
	t.Helper()
	lc := &fakeLifecycle{}
	s := spec.ProcessorSpec{Name: "test", GroupID: "g", Topics: []string{"t"}}
	_, err := NewConsumers(params{
		LC:         lc,
		Shutdowner: fakeShutdowner{},
		// bootstrap-servers è obbligatorio: NewConsumers valida ora la sezione `server` coi suoi tag,
		// e uno zero value non è più una config accettabile — che è il punto della validazione.
		Server:  spec.KafkaServer{BootstrapServers: "broker:9092"},
		Specs:   []spec.ProcessorSpec{s},
		Factory: &fakeFactory{consumers: []driver.GroupConsumer{gc}},
		Handlers: []processor.HandlerRegistration{{
			Consumer: "test",
			Handler:  handlerFunc(func(context.Context, []*message.Record) error { return nil }),
		}},
	})
	if err != nil {
		t.Fatalf("NewConsumers: %v", err)
	}
	if len(lc.hooks) != 1 {
		t.Fatalf("hook registrati = %d, atteso 1", len(lc.hooks))
	}
	return lc
}

func TestOnStop_SenzaOnStartNonSiBlocca(t *testing.T) {
	// OnStop gira anche quando OnStart non è mai stato eseguito (un costruttore successivo che
	// fallisce). Il canale dei runner era allocato dentro OnStart: attenderci sopra a nil era un
	// deadlock, non un no-op.
	lc := newEngine(t, &fakeGroupConsumer{})
	done := make(chan error, 1)
	go func() { done <- lc.hooks[0].OnStop(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("OnStop = %v, atteso nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnStop bloccata senza un OnStart precedente")
	}
}

func TestOnStop_ConsumerAppesoNonImpedisceLArresto(t *testing.T) {
	// Un runner che non torna (client bloccato, delivery report mai arrivato) non deve tenere in
	// piedi l'intera applicazione: l'attesa è limitata dalla deadline dell'hook.
	hc := &hangingConsumer{block: make(chan struct{})}
	defer close(hc.block)
	lc := newEngine(t, hc)

	if err := lc.hooks[0].OnStart(context.Background()); err != nil {
		t.Fatalf("OnStart: %v", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- lc.hooks[0].OnStop(stopCtx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("OnStop = %v, atteso nil (arresto forzato, non errore)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnStop non è tornata: l'arresto dipende ancora dal fatto che ogni runner termini")
	}
}

// resolveTopics è ciò che sta fra il Transformer e il broker: un record senza destinazione veniva
// prodotto verso il topic "", e il fallimento arrivava dal client senza nominare né il processor né
// la chiave di config da correggere.
func TestResolveTopics(t *testing.T) {
	r := &runner{spec: spec.ProcessorSpec{Name: "tx", DefaultOutputTopic: "out"}}

	recs := []*message.ProducerRecord{{}, {Topic: "esplicito"}}
	if err := r.resolveTopics(recs); err != nil {
		t.Fatalf("resolveTopics = %v, atteso nil", err)
	}
	if recs[0].Topic != "out" {
		t.Errorf("Topic = %q, atteso il default-output-topic", recs[0].Topic)
	}
	if recs[1].Topic != "esplicito" {
		t.Errorf("Topic = %q: il default non deve sovrascrivere quello scritto dal Transformer", recs[1].Topic)
	}
}

func TestResolveTopics_SenzaDestinazione(t *testing.T) {
	// Senza default-output-topic un record che non porta il proprio Topic non sa dove andare.
	senzaDefault := &runner{spec: spec.ProcessorSpec{Name: "tx"}}
	err := senzaDefault.resolveTopics([]*message.ProducerRecord{{Topic: "a"}, {}})
	if err == nil {
		t.Fatal("record senza Topic e senza default: atteso errore invece della produzione sul topic vuoto")
	}
	if !strings.Contains(err.Error(), "tx") || !strings.Contains(err.Error(), "default-output-topic") {
		t.Errorf("errore = %q, atteso che nomini il processor e la chiave di config", err)
	}

	// Il fan-out topic→topic — un Transformer che instrada da sé ogni record — resta legittimo e non
	// richiede alcun default: pretenderlo al boot vieterebbe il caso d'uso.
	if err := senzaDefault.resolveTopics([]*message.ProducerRecord{{Topic: "a"}, {Topic: "b"}}); err != nil {
		t.Errorf("fan-out con Topic su ogni record = %v, atteso nil", err)
	}
}

// --- fake sessione EOS -------------------------------------------------------------------------
//
// La modalità transform non aveva alcun test: fakeFactory non sapeva costruire una sessione, quindi
// runTransform, transformFlusher.flush, dlqRecords e resolveTopics erano a copertura zero — cioè
// tutto il percorso EOS e l'intera consegna al DLQ. È l'asimmetria che la fusione dei due loop di
// consumo doveva prevenire: le due modalità sono già divergite una volta (l'ordine di valutazione
// dell'esito, poi unificato in classify), e la protezione contro una nuova divergenza è il test.
//
// Ciò che questo fake permette di verificare non è "produce i record giusti" ma l'ORDINE delle
// operazioni, che in EOS È la garanzia: output, record DLQ e offset consumati devono stare nella
// STESSA transazione, e un errore in qualunque punto deve abortire senza mai arrivare al commit.
type fakeTransactSession struct {
	mu      sync.Mutex
	events  []pollEvent
	i       int
	exhaust error

	// errori iniettabili per fase: è il modo di produrre a comando un fencing o un broker che cade.
	beginErr, produceErr, commitErr error

	produced []*message.ProducerRecord // tutti i record passati a Produce, in ordine
	ops      []string                  // begin/produce/commit/abort/discard
	closed   bool
}

func (f *fakeTransactSession) Poll(context.Context, time.Duration) (*message.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.i >= len(f.events) {
		return nil, f.exhaust
	}
	e := f.events[f.i]
	f.i++
	return e.rec, e.err
}

func (f *fakeTransactSession) Begin(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "begin")
	return f.beginErr
}

func (f *fakeTransactSession) Produce(_ context.Context, recs []*message.ProducerRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "produce")
	if f.produceErr != nil {
		return f.produceErr
	}
	f.produced = append(f.produced, recs...)
	return nil
}

func (f *fakeTransactSession) Commit(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "commit")
	return f.commitErr
}

func (f *fakeTransactSession) Abort(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "abort")
	return nil
}

func (f *fakeTransactSession) Discard(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "discard")
}

func (f *fakeTransactSession) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeTransactSession) sequence() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

func (f *fakeTransactSession) sent() []*message.ProducerRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*message.ProducerRecord(nil), f.produced...)
}

// transformerFunc adatta una funzione al contratto Transformer.
type transformerFunc func(context.Context, []*message.Record) ([]*message.ProducerRecord, error)

func (t transformerFunc) Transform(ctx context.Context, b []*message.Record) ([]*message.ProducerRecord, error) {
	return t(ctx, b)
}

// transformSpec: come testSpec ma per la modalità EOS, con la supervisione disattivata perché questi
// test verificano UN ciclo di flush, non la politica di riavvio (che ha già i suoi test).
func transformSpec() spec.ProcessorSpec {
	s := spec.ProcessorSpec{
		Name: "tx", GroupID: "g", Topics: []string{"in"},
		TransactionalID: "tx-1", DefaultOutputTopic: "out",
	}
	s.Consumer.MaxBatchSize = 2
	s.Restart = spec.RestartSpec{Disabled: ptr(true)}
	return s.Resolve(spec.KafkaServer{})
}

// txRunner costruisce il runner transform e la sessione che gli verrà consegnata. exhaust è
// l'errore con cui il loop termina in modo controllato dopo aver consumato gli eventi.
func txRunner(s spec.ProcessorSpec, tr processor.Transformer, events []pollEvent) (*runner, *fakeTransactSession) {
	sess := &fakeTransactSession{
		events:  events,
		exhaust: driver.NewError(driver.SeverityPermanent, "poll", errors.New("fine eventi")),
	}
	return &runner{spec: s, factory: &fakeFactory{sessions: []driver.TransactSession{sess}}, transformer: tr}, sess
}

func opsEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// --- modalità transform (EOS) ------------------------------------------------------------------

func TestRunTransform_OrdineEOS(t *testing.T) {
	// L'invariante della modalità: Begin PRIMA di produrre, Commit DOPO, e nulla in mezzo che possa
	// rendere definitivo un pezzo senza l'altro.
	r, sess := txRunner(transformSpec(),
		transformerFunc(func(_ context.Context, b []*message.Record) ([]*message.ProducerRecord, error) {
			out := make([]*message.ProducerRecord, 0, len(b))
			for _, rr := range b {
				out = append(out, &message.ProducerRecord{Value: rr.Value}) // senza Topic: prende il default
			}
			return out, nil
		}),
		[]pollEvent{{rec: rec("in", 0, 1)}, {rec: rec("in", 0, 2)}})

	_ = r.run(context.Background())

	if got := sess.sequence(); !opsEqual(got, []string{"begin", "produce", "commit"}) {
		t.Errorf("sequenza = %v, attesa [begin produce commit]", got)
	}
	sent := sess.sent()
	if len(sent) != 2 {
		t.Fatalf("record prodotti = %d, attesi 2", len(sent))
	}
	for _, p := range sent {
		if p.Topic != "out" {
			t.Errorf("Topic = %q, atteso il default-output-topic", p.Topic)
		}
	}
	if !sess.closed {
		t.Error("la sessione non è stata chiusa all'uscita dal loop")
	}
}

func TestRunTransform_DeadLetterNellaStessaTransazione(t *testing.T) {
	// È il punto per cui la modalità transform esiste: output "buoni" e record poison finiscono nella
	// STESSA transazione degli offset. Due Produce separati, o un Commit fra i due, romperebbero
	// l'esattamente-una-volta senza che nulla lo segnali.
	s := transformSpec()
	dlq := "tx.DLQ"
	s.Consumer.DeadletterTopic = &dlq

	r, sess := txRunner(s,
		transformerFunc(func(_ context.Context, b []*message.Record) ([]*message.ProducerRecord, error) {
			// Il primo record è buono, il secondo è poison.
			return []*message.ProducerRecord{{Topic: "out", Value: b[0].Value}},
				processor.DeadLetter(errors.New("payload illeggibile"), b[1])
		}),
		[]pollEvent{{rec: rec("in", 0, 1)}, {rec: rec("in", 0, 2)}})

	_ = r.run(context.Background())

	if got := sess.sequence(); !opsEqual(got, []string{"begin", "produce", "commit"}) {
		t.Fatalf("sequenza = %v, attesa [begin produce commit]: il DLQ deve stare nella transazione", got)
	}
	sent := sess.sent()
	if len(sent) != 2 {
		t.Fatalf("record prodotti = %d, attesi 2 (output + DLQ) in un solo Produce", len(sent))
	}
	if sent[0].Topic != "out" || sent[1].Topic != dlq {
		t.Errorf("topic prodotti = [%q %q], attesi [out %s]", sent[0].Topic, sent[1].Topic, dlq)
	}
	// La causa del record poison deve arrivare a chi legge il DLQ.
	if got := sent[1].Headers.Get(HeaderDLQError); got != "payload illeggibile" {
		t.Errorf("header %s = %q, attesa la causa del record", HeaderDLQError, got)
	}
	if got := sent[1].Headers.Get(HeaderDLQProcessor); got != "tx" {
		t.Errorf("header %s = %q, atteso il nome del processor", HeaderDLQProcessor, got)
	}
}

func TestRunTransform_DeadLetterSenzaTopicAbortisce(t *testing.T) {
	// DeadLetter richiesto ma nessun deadletter-topic: i record non hanno dove andare. Committare
	// sarebbe una perdita silenziosa, quindi si abortisce e l'errore risale (replay).
	r, sess := txRunner(transformSpec(),
		transformerFunc(func(_ context.Context, b []*message.Record) ([]*message.ProducerRecord, error) {
			return nil, processor.DeadLetter(errors.New("poison"), b[0])
		}),
		[]pollEvent{{rec: rec("in", 0, 1)}, {rec: rec("in", 0, 2)}})

	err := r.run(context.Background())
	if err == nil {
		t.Fatal("atteso errore: un DeadLetter senza topic non deve committare")
	}
	if got := sess.sequence(); !opsEqual(got, []string{"begin", "abort"}) {
		t.Errorf("sequenza = %v, attesa [begin abort]: niente produce, niente commit", got)
	}
}

// Ogni fase può fallire, e in tutte l'esito deve essere lo stesso: abortire senza mai committare.
// Tabellato perché è UNA regola, e tre rami che la implementano tre volte sono tre modi di sbagliarla.
func TestRunTransform_OgniErroreAbortisceSenzaCommit(t *testing.T) {
	boom := errors.New("broker giù")
	tests := []struct {
		name    string
		inject  func(*fakeTransactSession)
		tErr    error
		wantOps []string
	}{
		{"Transform fallisce", nil, boom, []string{"begin", "abort"}},
		{"Produce fallisce", func(s *fakeTransactSession) { s.produceErr = boom }, nil, []string{"begin", "produce", "abort"}},
		{"Commit fallisce", func(s *fakeTransactSession) { s.commitErr = boom }, nil, []string{"begin", "produce", "commit", "abort"}},
		// Begin fallito non apre nulla: abortire darebbe un errore di stato invalido al posto di un no-op.
		{"Begin fallisce", func(s *fakeTransactSession) { s.beginErr = boom }, nil, []string{"begin"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, sess := txRunner(transformSpec(),
				transformerFunc(func(_ context.Context, b []*message.Record) ([]*message.ProducerRecord, error) {
					return []*message.ProducerRecord{{Topic: "out", Value: b[0].Value}}, tc.tErr
				}),
				[]pollEvent{{rec: rec("in", 0, 1)}, {rec: rec("in", 0, 2)}})
			if tc.inject != nil {
				tc.inject(sess)
			}

			if err := r.run(context.Background()); err == nil {
				t.Fatal("atteso errore risalito")
			}
			got := sess.sequence()
			if !opsEqual(got, tc.wantOps) {
				t.Errorf("sequenza = %v, attesa %v", got, tc.wantOps)
			}
			for i, op := range got {
				if op == "commit" && i != len(got)-2 {
					t.Errorf("commit in posizione %d: non deve mai precedere un errore non abortito", i)
				}
			}
		})
	}
}

func TestRunTransform_RecordSenzaDestinazioneAbortisce(t *testing.T) {
	// resolveTopics ha il suo test unitario; qui conta che il suo errore attraversi il flush
	// abortendo, invece di lasciar produrre verso il topic vuoto.
	s := transformSpec()
	s.DefaultOutputTopic = ""

	r, sess := txRunner(s,
		transformerFunc(func(_ context.Context, b []*message.Record) ([]*message.ProducerRecord, error) {
			return []*message.ProducerRecord{{Value: b[0].Value}}, nil // nessun Topic, nessun default
		}),
		[]pollEvent{{rec: rec("in", 0, 1)}, {rec: rec("in", 0, 2)}})

	err := r.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "default-output-topic") {
		t.Fatalf("errore = %v, atteso quello che nomina la config mancante", err)
	}
	if got := sess.sequence(); !opsEqual(got, []string{"begin", "abort"}) {
		t.Errorf("sequenza = %v, attesa [begin abort]", got)
	}
}

func TestRunTransform_ResetScartaIlBatchEAbortisce(t *testing.T) {
	// In EOS scartare un batch non è troncare una slice: Discard abortisce anche la transazione, che
	// altrimenti resterebbe aperta fino al transaction.timeout.ms del broker — e nel frattempo i
	// consumatori read_committed a valle restano fermi su quelle partizioni.
	reset := driver.NewError(driver.SeverityReset, "poll", errors.New("rebalance"))
	r, sess := txRunner(transformSpec(),
		transformerFunc(func(_ context.Context, b []*message.Record) ([]*message.ProducerRecord, error) {
			return []*message.ProducerRecord{{Topic: "out"}}, nil
		}),
		[]pollEvent{{rec: rec("in", 0, 1)}, {err: reset}})

	_ = r.run(context.Background())

	got := sess.sequence()
	if len(got) == 0 || got[0] != "discard" {
		t.Errorf("sequenza = %v, attesa una discard prima di qualunque altra operazione", got)
	}
	for _, op := range got {
		if op == "commit" {
			t.Error("commit dopo un reset: gli offset del batch scartato verrebbero confermati")
		}
	}
}

// --- consegna al DLQ in modalità handle --------------------------------------------------------
//
// toDLQ (la COSTRUZIONE dei record) aveva quattro test; sendDeadletter (la loro CONSEGNA) nessuno,
// in nessuna delle due modalità. Qui il DLQ passa dal Producer condiviso, che è un tipo concreto:
// costruirlo sul fake driver copre insieme il ramo dell'engine e il package producer, che era a zero.

type fakeDriverProducer struct {
	mu       sync.Mutex
	produced []*message.ProducerRecord
	err      error
	closed   bool
}

func (f *fakeDriverProducer) Produce(_ context.Context, recs []*message.ProducerRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.produced = append(f.produced, recs...)
	return nil
}

func (f *fakeDriverProducer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// newDLQ costruisce il Producer condiviso vero (non un fake) sopra un driver fake: è il percorso di
// produzione, compresa la validazione che NewProducer fa della sezione `server`.
func newDLQ(t *testing.T, d *fakeDriverProducer) *producer.Producer {
	t.Helper()
	p, err := producer.NewProducer(
		&fakeLifecycle{},
		&fakeFactory{producers: []driver.Producer{d}},
		spec.KafkaServer{BootstrapServers: "broker:9092"},
		spec.ProducerTuning{},
	)
	if err != nil {
		t.Fatalf("producer.NewProducer: %v", err)
	}
	return p
}

// handleSpec: modalità handle con un DLQ configurato.
func handleSpec(dlqTopic string) spec.ProcessorSpec {
	s := spec.ProcessorSpec{Name: "h", GroupID: "g", Topics: []string{"in"}}
	s.Consumer.MaxBatchSize = 2
	s.Consumer.OnError = spec.OnErrorDeadletter
	if dlqTopic != "" {
		s.Consumer.DeadletterTopic = &dlqTopic
	}
	s.Restart = spec.RestartSpec{Disabled: ptr(true)}
	return s.Resolve(spec.KafkaServer{})
}

func TestSendDeadletter_SenzaConfigurazioneNonPerdeIRecord(t *testing.T) {
	// Un deadletter richiesto senza DLQ configurato deve fallire, non committare in silenzio: è la
	// differenza fra un replay e una perdita.
	for _, tc := range []struct {
		name string
		r    *runner
	}{
		{"nessun Producer", &runner{spec: handleSpec("h.DLQ")}},
		{"nessun deadletter-topic", &runner{spec: handleSpec(""), dlq: newDLQ(t, &fakeDriverProducer{})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.r.sendDeadletter(context.Background(),
				processor.DeadLetter(errors.New("poison"), rec("in", 0, 1)))
			if err == nil {
				t.Fatal("atteso errore: senza DLQ i record non hanno dove andare")
			}
			if !strings.Contains(err.Error(), "deadletter") {
				t.Errorf("errore = %q, atteso che nomini il deadletter mancante", err)
			}
		})
	}
}

func TestRunHandle_DeadletterConsegnatoPoiCommit(t *testing.T) {
	// L'ordine conta: i poison vanno consegnati PRIMA di committare gli offset del batch, altrimenti
	// un crash fra le due operazioni li perde.
	dp := &fakeDriverProducer{}
	fc := &fakeGroupConsumer{
		events:  []pollEvent{{rec: rec("in", 3, 7)}, {rec: rec("in", 3, 8)}},
		exhaust: driver.NewError(driver.SeverityPermanent, "poll", errors.New("fine")),
	}
	r := &runner{
		spec:    handleSpec("h.DLQ"),
		factory: &fakeFactory{consumers: []driver.GroupConsumer{fc}},
		dlq:     newDLQ(t, dp),
		handler: handlerFunc(func(_ context.Context, b []*message.Record) error {
			return processor.DeadLetter(errors.New("payload illeggibile"), b[0])
		}),
	}

	_ = r.run(context.Background())

	if len(dp.produced) != 1 {
		t.Fatalf("record nel DLQ = %d, atteso 1 (solo il poison)", len(dp.produced))
	}
	got := dp.produced[0]
	if got.Topic != "h.DLQ" {
		t.Errorf("Topic = %q, atteso il deadletter-topic", got.Topic)
	}
	if got.Headers.Get(HeaderDLQSourceTopic) != "in" || got.Headers.Get(HeaderDLQSourceOffset) != "7" {
		t.Errorf("header di origine = %v, attesi topic/offset del record consumato", got.Headers)
	}
	if got.Headers.Get(HeaderDLQError) != "payload illeggibile" {
		t.Errorf("header %s = %q", HeaderDLQError, got.Headers.Get(HeaderDLQError))
	}
	// Il resto del batch è stato elaborato: gli offset vanno committati.
	if fc.commits != 1 {
		t.Errorf("commit = %d, atteso 1 dopo la consegna al DLQ", fc.commits)
	}
	if !opsEqual(fc.ops, []string{"commit"}) {
		t.Errorf("operazioni = %v, atteso il solo commit", fc.ops)
	}
}

func TestRunHandle_DLQIrraggiungibileNonCommitta(t *testing.T) {
	// Se il DLQ non accetta i record, committare li perderebbe. L'errore risale e il batch è replayato
	// — con i duplicati nel DLQ che il contratto della modalità ammette.
	dp := &fakeDriverProducer{err: errors.New("broker del DLQ giù")}
	fc := &fakeGroupConsumer{
		events:  []pollEvent{{rec: rec("in", 0, 1)}, {rec: rec("in", 0, 2)}},
		exhaust: driver.NewError(driver.SeverityPermanent, "poll", errors.New("fine")),
	}
	r := &runner{
		spec:    handleSpec("h.DLQ"),
		factory: &fakeFactory{consumers: []driver.GroupConsumer{fc}},
		dlq:     newDLQ(t, dp),
		handler: handlerFunc(func(_ context.Context, b []*message.Record) error {
			return processor.DeadLetter(errors.New("poison"), b[0])
		}),
	}

	if err := r.run(context.Background()); err == nil {
		t.Fatal("atteso errore risalito dal DLQ irraggiungibile")
	}
	if fc.commits != 0 {
		t.Errorf("commit = %d, atteso 0: senza consegna al DLQ il batch va replayato", fc.commits)
	}
}
