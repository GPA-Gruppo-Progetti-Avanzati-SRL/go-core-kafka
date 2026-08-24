package processor

import (
	"context"
	"errors"
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
		provideIfActive("attivo", func(spec.ConsumerSpec) { provided = append(provided, "attivo") })
		provideIfActive("spento", func(spec.ConsumerSpec) { provided = append(provided, "spento") })
	}

	Apply(register, map[string]spec.ConsumerSpec{"attivo": {Name: "attivo"}}, nil)

	if len(provided) != 1 || provided[0] != "attivo" {
		t.Fatalf("atteso solo 'attivo' fornito, ottenuto %v", provided)
	}
}

// Apply non deve chiamare register() se il sottosistema non è attivo nel Mode corrente, anche se il
// consumer è nell'insieme active.
func TestApply_SkipsAllWhenSubsystemModeInactive(t *testing.T) {
	var called bool
	register := func() { called = true }

	Apply(register, map[string]spec.ConsumerSpec{"attivo": {Name: "attivo"}}, []string{"modo-non-attivo"})

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
	provideIfActive("x", func(spec.ConsumerSpec) {})
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
	}, map[string]spec.ConsumerSpec{
		"eventi": {Name: "eventi", Properties: core.Properties{"collection": "events"}},
	}, nil)
}
