package processor

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"go.uber.org/fx"
	"reflect"
)

type fakeDep struct{ name string }

// depsObject è una dipendenza che è essa stessa un param object fx (caso "gruppo di dipendenze
// embeddato"): dig deve costruirla come param object ANNIDATO anche se nella struct sintetica il campo
// non è più embeddato.
type depsObject struct {
	core.In
	Dep *fakeDep
}

// fxHandler è la forma reale di un consumer: core.In, dipendenze iniettate e campi property — questi
// ultimi SENZA `optional:"true"`, perché dig non li vede mai (li nasconde il costruttore sintetizzato).
type fxHandler struct {
	core.In
	Dep    *fakeDep
	Nested depsObject

	Collection string        `prop:"collection" validate:"required"`
	BatchLimit int           `prop:"batch-limit" default:"100"`
	Timeout    time.Duration `prop:"timeout" default:"5s"`

	privato string // non esportato: non iniettabile, resta a zero
}

func (h *fxHandler) Handle(context.Context, []*message.Record) error { return nil }

// handlerCtor sintetizza il costruttore come fa RegisterHandler.
func handlerCtor[T any, PT interface {
	*T
	Handler
}](t *testing.T, s spec.ConsumerSpec) any {
	t.Helper()
	ctor, err := synthCtor(reflect.TypeOf((*T)(nil)).Elem(), reflect.TypeOf(HandlerRegistration{}), HandlerGroup, s,
		func(ptr any) any {
			return HandlerRegistration{Consumer: s.Name, Handler: PT(ptr.(*T))}
		})
	if err != nil {
		t.Fatalf("synthCtor: %v", err)
	}
	return ctor
}

// consumeGroup replica il modo in cui l'engine consuma il value group dei registration.
type groupParams struct {
	fx.In
	Regs []HandlerRegistration `group:"kafka_handlers"`
}

// Il costruttore sintetizzato deve: iniettare le dipendenze (anche i param object annidati), mappare le
// properties sui campi taggati e finire nel value group — con l'app che NON scrive `optional:"true"`.
func TestSynthCtor_InjectsDepsAndBindsProps(t *testing.T) {
	var got *fxHandler
	app := fx.New(
		fx.NopLogger,
		fx.Supply(&fakeDep{name: "svc"}),
		fx.Provide(handlerCtor[fxHandler](t, spec.ConsumerSpec{
			Name:       "eventi",
			Properties: spec.Properties{"collection": "events", "batch-limit": 200},
		})),
		fx.Invoke(func(p groupParams) {
			if len(p.Regs) != 1 {
				t.Fatalf("atteso 1 registration nel gruppo, ottenuto %d", len(p.Regs))
			}
			if p.Regs[0].Consumer != "eventi" {
				t.Fatalf("consumer errato: %q", p.Regs[0].Consumer)
			}
			got = p.Regs[0].Handler.(*fxHandler)
		}),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("il grafo fx deve costruirsi senza optional:\"true\" sui campi prop: %v", err)
	}
	if got.Dep == nil || got.Dep.name != "svc" {
		t.Fatalf("dipendenza non iniettata: %+v", got)
	}
	if got.Nested.Dep == nil {
		t.Fatal("param object annidato non iniettato")
	}
	if got.Collection != "events" || got.BatchLimit != 200 || got.Timeout != 5*time.Second {
		t.Fatalf("properties non mappate: %+v", got)
	}
	if got.privato != "" {
		t.Fatalf("un campo non esportato deve restare a zero: %q", got.privato)
	}
}

// Una property invalida fa fallire la costruzione del grafo: l'app non parte e nessun client Kafka
// viene aperto.
func TestSynthCtor_FailFastOnInvalidProperty(t *testing.T) {
	app := fx.New(
		fx.NopLogger,
		fx.Supply(&fakeDep{}),
		fx.Provide(handlerCtor[fxHandler](t, spec.ConsumerSpec{Name: "eventi"})), // manca `collection`
		fx.Invoke(func(groupParams) {}),
	)
	err := app.Err()
	if err == nil {
		t.Fatal("atteso fallimento della build del grafo fx")
	}
	if !strings.Contains(err.Error(), "collection") {
		t.Fatalf("l'errore fx deve riportare la property: %v", err)
	}
}

// Una dipendenza nil-abile mancante produce un errore NOSTRO, che nomina consumer, campo e tipo (fx
// da solo direbbe "reflect.makeFuncStub": vedi il commento in synthCtor).
func TestSynthCtor_MissingDependency(t *testing.T) {
	app := fx.New(
		fx.NopLogger,
		// nessun *fakeDep fornito
		fx.Provide(handlerCtor[plainHandler](t, spec.ConsumerSpec{Name: "eventi"})),
		fx.Invoke(func(groupParams) {}),
	)
	err := app.Err()
	if err == nil {
		t.Fatal("atteso errore di dipendenza mancante")
	}
	for _, want := range []string{`consumer "eventi"`, "plainHandler", "campo Dep", "*processor.fakeDep"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("l'errore deve contenere %q: %v", want, err)
		}
	}
}

// Fallback: una dipendenza NON nil-abile (qui un param object annidato, che non possiamo rendere
// opzionale) resta obbligatoria per dig — l'errore è quello di fx, col tipo mancante.
func TestSynthCtor_MissingDependencyInNestedParamObject(t *testing.T) {
	type onlyNested struct {
		core.In
		Nested     depsObject
		Collection string `prop:"collection" default:"eventi"`
	}
	ctor, err := synthCtor(reflect.TypeOf(onlyNested{}), reflect.TypeOf(HandlerRegistration{}), HandlerGroup,
		spec.ConsumerSpec{Name: "eventi"}, func(ptr any) any { return HandlerRegistration{Consumer: "eventi"} })
	if err != nil {
		t.Fatalf("synthCtor: %v", err)
	}
	app := fx.New(fx.NopLogger, fx.Provide(ctor), fx.Invoke(func(groupParams) {}))
	if app.Err() == nil {
		t.Fatal("atteso errore: il param object annidato non è opzionale")
	}
	if !strings.Contains(app.Err().Error(), "*processor.fakeDep") {
		t.Fatalf("l'errore fx deve nominare il tipo mancante: %v", app.Err())
	}
}

// core.In nella struct del processor è ormai facoltativo: senza, il costruttore sintetizzato funziona
// identico (è lui il param object, non la struct dell'app).
type plainHandler struct {
	Dep        *fakeDep
	Collection string `prop:"collection" default:"eventi"`
}

func (h *plainHandler) Handle(context.Context, []*message.Record) error { return nil }

func TestSynthCtor_WithoutCoreIn(t *testing.T) {
	var got *plainHandler
	app := fx.New(
		fx.NopLogger,
		fx.Supply(&fakeDep{name: "svc"}),
		fx.Provide(handlerCtor[plainHandler](t, spec.ConsumerSpec{Name: "eventi"})),
		fx.Invoke(func(p groupParams) { got = p.Regs[0].Handler.(*plainHandler) }),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("core.In non deve essere obbligatorio: %v", err)
	}
	if got.Dep == nil || got.Collection != "eventi" {
		t.Fatalf("dipendenza/property errate: %+v", got)
	}
}

// Una dipendenza esplicitamente opzionale resta opzionale: i tag dig dei campi NON property sono
// preservati nel param object sintetico.
type optionalDepHandler struct {
	core.In
	Dep        *fakeDep `optional:"true"`
	Collection string   `prop:"collection" default:"eventi"`
}

func (h *optionalDepHandler) Handle(context.Context, []*message.Record) error { return nil }

func TestSynthCtor_PreservesDigTagsOnDependencies(t *testing.T) {
	var got *optionalDepHandler
	app := fx.New(
		fx.NopLogger,
		// *fakeDep NON fornito: il campo è optional, quindi il grafo deve costruirsi comunque
		fx.Provide(handlerCtor[optionalDepHandler](t, spec.ConsumerSpec{Name: "eventi"})),
		fx.Invoke(func(p groupParams) { got = p.Regs[0].Handler.(*optionalDepHandler) }),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("una dipendenza optional deve restare tale: %v", err)
	}
	if got.Dep != nil {
		t.Fatalf("atteso nil per la dipendenza optional non fornita: %+v", got.Dep)
	}
}

func TestSynthCtor_RejectsNonStruct(t *testing.T) {
	if _, err := synthCtor(reflect.TypeOf(0), reflect.TypeOf(HandlerRegistration{}), HandlerGroup, spec.ConsumerSpec{}, func(any) any { return nil }); err == nil {
		t.Fatal("atteso errore per un tipo non struct")
	}
}
