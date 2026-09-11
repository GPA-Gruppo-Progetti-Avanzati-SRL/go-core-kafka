package processor

import (
	"context"
	"errors"
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
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

// Apply deve eseguire register() fornendo (via provideIfActive) solo i consumer nell'insieme active,
// e saltare silenziosamente gli altri (consumer disabilitato/assente in config).
func TestApply_ProvidesOnlyActiveConsumers(t *testing.T) {
	var provided []string
	register := func() {
		provideIfActive("attivo", nil, func(spec.ProcessorSpec) { provided = append(provided, "attivo") })
		provideIfActive("spento", nil, func(spec.ProcessorSpec) { provided = append(provided, "spento") })
	}

	excluded := Apply(register, map[string]spec.ProcessorSpec{"attivo": {Name: "attivo"}}, nil)

	if len(provided) != 1 || provided[0] != "attivo" {
		t.Fatalf("atteso solo 'attivo' fornito, ottenuto %v", provided)
	}
	// "spento" è già fuori dalla lista che Module ha passato: sottrarlo una seconda volta non avrebbe
	// nulla da togliere. Gli esclusi sono SOLO quelli che il mode ha tolto a un processor che la config
	// ammetteva — è la parte che Module da solo non può sapere.
	if len(excluded) != 0 {
		t.Fatalf("excluded = %v, atteso vuoto: un processor assente dalla config non è un escluso per mode", excluded)
	}
}

// Apply non deve chiamare register() se il sottosistema non è attivo nel Mode corrente, anche se il
// consumer è nell'insieme active.
func TestApply_SkipsAllWhenSubsystemModeInactive(t *testing.T) {
	var called bool
	register := func() { called = true }

	Apply(register, map[string]spec.ProcessorSpec{"attivo": {Name: "attivo"}}, []string{"modo-non-attivo"})

	if called {
		t.Fatal("Apply non deve invocare register() se il sottosistema non è nel Mode corrente")
	}
}

// provideIfActive deve panicare se chiamata fuori dalla finestra sincrona aperta da Apply (cioè fuori
// dalla funzione di registrazione passata a Module) — RegisterHandler/RegisterTransformer si
// appoggiano a questo stesso meccanismo.
func TestProvideIfActive_PanicsOutsideApply(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("atteso panic se chiamata fuori dalla funzione passata ad Apply")
		}
	}()
	provideIfActive("x", nil, func(spec.ProcessorSpec) {})
}

// withMode imposta il core.Mode corrente per la durata del test. È una var di package di go-core-app,
// quindi va ripristinata: i test del pacchetto la condividono.
func withMode(t *testing.T, mode string) {
	t.Helper()
	prev := core.Mode
	core.Mode = mode
	t.Cleanup(func() { core.Mode = prev })
}

// Il gating per-processor deve spegnere ENTRAMBI i lati: niente costruttore (questo lo faceva già) e
// niente voce nella lista che comanda i runner. Il secondo passa dal valore di ritorno di Apply, ed è
// ciò che mancava: il processor restava negli spec e consumer.newRunner cadeva nel ramo "nessun
// processor registrato", cioè ogni uso dei modes del register rompeva l'avvio.
func TestApply_RiportaGliEsclusiDalRegister(t *testing.T) {
	withMode(t, "WORKER")

	var provided []string
	register := func() {
		provideIfActive("mio", []string{"API"}, func(spec.ProcessorSpec) { provided = append(provided, "mio") })
	}

	excluded := Apply(register, map[string]spec.ProcessorSpec{"mio": {Name: "mio"}}, nil)

	if len(provided) != 0 {
		t.Fatalf("provided = %v, atteso nessun costruttore: il mode non corrisponde", provided)
	}
	if len(excluded) != 1 || excluded[0] != "mio" {
		t.Fatalf("excluded = %v, atteso [mio]: senza questo il processor resterebbe fra i runner", excluded)
	}
}

// Il rovescio: col mode corrispondente il processor è attivo e non c'è nulla da sottrarre.
func TestApply_ModeCorrispondenteRegistra(t *testing.T) {
	withMode(t, "WORKER")

	var provided []string
	register := func() {
		provideIfActive("mio", []string{"API", "WORKER"}, func(spec.ProcessorSpec) { provided = append(provided, "mio") })
	}

	excluded := Apply(register, map[string]spec.ProcessorSpec{"mio": {Name: "mio"}}, nil)

	if len(provided) != 1 || provided[0] != "mio" {
		t.Fatalf("provided = %v, atteso [mio]", provided)
	}
	if len(excluded) != 0 {
		t.Fatalf("excluded = %v, atteso vuoto", excluded)
	}
}

// modes vuoto = attivo in ogni mode: è la retrocompatibilità di chi non passa nulla, e viene gratis da
// core.IsMode() senza argomenti. Se si rompesse, ogni app esistente smetterebbe di registrare.
func TestApply_ModesVuotoSempreAttivo(t *testing.T) {
	withMode(t, "UN-MODE-QUALSIASI")

	var provided []string
	register := func() {
		provideIfActive("mio", nil, func(spec.ProcessorSpec) { provided = append(provided, "mio") })
	}

	if excluded := Apply(register, map[string]spec.ProcessorSpec{"mio": {Name: "mio"}}, nil); len(excluded) != 0 {
		t.Fatalf("excluded = %v, atteso vuoto", excluded)
	}
	if len(provided) != 1 {
		t.Fatalf("provided = %v, atteso [mio]", provided)
	}
}

// --- integrazione col meccanismo di go-core-app -----------------------------------------------------

type propsHandler struct {
	Svc *fakeSvc `inject:""`

	Collection string `prop:"collection" validate:"required"`
	BatchLimit int    `prop:"batch-limit" default:"100"`

	scratch []byte // campo di lavorazione
}

type fakeSvc struct{}

func (h *propsHandler) Handle(context.Context, []*message.Record) error { return nil }

// Le properties dello spec finiscono sui campi `prop:` (il mapping vive in go-core-app: qui si
// verifica solo che il wrapper deleghi correttamente, default e validazione inclusi).
func TestProps_BoundOnProcessorStruct(t *testing.T) {
	var h propsHandler
	if err := core.BindProps(&h, core.Properties{"collection": "events"}); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if h.Collection != "events" || h.BatchLimit != 100 {
		t.Fatalf("properties non mappate: %+v", h)
	}
	if err := core.BindProps(&h, core.Properties{}); err == nil {
		t.Fatal("atteso errore di validazione per `collection` mancante")
	}
}

// RegisterHandler dentro Apply sintetizza il costruttore del consumer attivo: una struct non
// rappresentabile o una combinazione di tag illegale panicherebbe qui, al wiring.
func TestRegisterHandler_SynthesizesInsideApply(t *testing.T) {
	Apply(func() {
		RegisterHandler[propsHandler]("eventi")
		RegisterHandler[propsHandler]("spento") // non attivo: non deve nemmeno sintetizzare
	}, map[string]spec.ProcessorSpec{
		"eventi": {Name: "eventi", Properties: core.Properties{"collection": "events"}},
	}, nil)
}

// propsTransformer è il gemello EOS di propsHandler: stessi tag, altro seam.
type propsTransformer struct {
	Topic string `prop:"topic" validate:"required"`
}

func (t *propsTransformer) Transform(context.Context, []*message.Record) ([]*message.ProducerRecord, error) {
	return nil, nil
}

// Il gemello di TestRegisterHandler_SynthesizesInsideApply. Mancava, ed è la stessa asimmetria che
// lasciava senza test tutta la modalità transform: i due seam sono in dualità dichiarata, quindi una
// verifica su uno solo lascia scoperta metà del contratto.
func TestRegisterTransformer_SynthesizesInsideApply(t *testing.T) {
	Apply(func() {
		RegisterTransformer[propsTransformer]("routing")
		RegisterTransformer[propsTransformer]("spento") // non attivo: non deve nemmeno sintetizzare
	}, map[string]spec.ProcessorSpec{
		"routing": {Name: "routing", Properties: core.Properties{"topic": "out"}},
	}, nil)
}

// Il messaggio di PoisonRecords è ciò che un operatore legge nei log quando un batch finisce al DLQ:
// senza la causa non direbbe nulla di utile.
func TestPoisonRecords_Error(t *testing.T) {
	cause := errors.New("json non decodificabile")
	if got := DeadLetter(cause).Error(); !strings.Contains(got, cause.Error()) {
		t.Errorf("Error() = %q, atteso contenga la causa", got)
	}
	if got := (&PoisonRecords{}).Error(); got == "" {
		t.Error("Error() senza causa non deve essere vuoto")
	}
}
