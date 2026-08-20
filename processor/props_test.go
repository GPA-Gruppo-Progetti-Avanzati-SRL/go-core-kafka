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
)

type nested struct {
	Inner string `prop:"inner"`
}

// props raccoglie tutti i tipi supportati dal mapping. In un handler reale questi campi convivono con
// le dipendenze fx e portano anche `optional:"true"` (vedi TestBindProps_FxParamObject).
type props struct {
	Collection string        `prop:"collection" validate:"required"`
	BatchLimit int           `prop:"batch-limit" default:"100"`
	Enabled    bool          `prop:"enabled"`
	Timeout    time.Duration `prop:"timeout" default:"5s"`
	Tags       []string      `prop:"tags"`
	Nested     nested        `prop:"nested"`
	NoTag      string        // nessun tag prop: mai toccato dal mapping
}

// I valori arrivano dal YAML col loro tipo nativo (int, bool, lista, mappa) oppure come stringa (per es.
// dopo una sostituzione ${ENV_VAR}): entrambi i casi devono convertirsi.
func TestBindProps_Types(t *testing.T) {
	var p props
	err := BindProps(&p, spec.Properties{
		"collection":  "events",
		"batch-limit": 200,                                 // int nativo
		"enabled":     "true",                              // stringa da ${ENV_VAR}
		"timeout":     "1500ms",                            // durata
		"tags":        []any{"a", "b"},                     // lista YAML
		"nested":      map[string]any{"inner": "profondo"}, // mappa annidata
	})
	if err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if p.Collection != "events" || p.BatchLimit != 200 || !p.Enabled {
		t.Fatalf("scalari errati: %+v", p)
	}
	if p.Timeout != 1500*time.Millisecond {
		t.Fatalf("durata errata: %v", p.Timeout)
	}
	if len(p.Tags) != 2 || p.Tags[0] != "a" || p.Tags[1] != "b" {
		t.Fatalf("lista errata: %v", p.Tags)
	}
	if p.Nested.Inner != "profondo" {
		t.Fatalf("mappa annidata errata: %+v", p.Nested)
	}
	if p.NoTag != "" {
		t.Fatalf("un campo senza tag prop non deve essere toccato: %q", p.NoTag)
	}
}

// Una lista può anche arrivare come stringa separata da virgole.
func TestBindProps_CommaSeparatedSlice(t *testing.T) {
	var p props
	if err := BindProps(&p, spec.Properties{"collection": "c", "tags": "a,b,c"}); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if len(p.Tags) != 3 {
		t.Fatalf("attesi 3 tag, ottenuto %v", p.Tags)
	}
}

// I default del tag valgono solo per le chiavi assenti: quelle presenti vincono.
func TestBindProps_Defaults(t *testing.T) {
	var p props
	if err := BindProps(&p, spec.Properties{"collection": "c"}); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if p.BatchLimit != 100 || p.Timeout != 5*time.Second {
		t.Fatalf("default non applicati: %+v", p)
	}

	var q props
	if err := BindProps(&q, spec.Properties{"collection": "c", "batch-limit": 7, "timeout": "1s"}); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if q.BatchLimit != 7 || q.Timeout != time.Second {
		t.Fatalf("il valore in config deve vincere sul default: %+v", q)
	}
}

// Un valore presente ma non convertibile è un ERRORE, non un fallback silenzioso al default (era il
// comportamento dei getter untyped: GetInt("batch-limit", 100) su "abc" ritornava 100).
func TestBindProps_UnconvertibleValueFails(t *testing.T) {
	var p props
	err := BindProps(&p, spec.Properties{"collection": "c", "batch-limit": "abc"})
	if err == nil {
		t.Fatal("atteso errore per un valore non convertibile")
	}
	if !strings.Contains(err.Error(), "batch-limit") {
		t.Fatalf("l'errore deve nominare la property: %v", err)
	}
}

// Chiave non reclamata da nessun campo: ignorata (solo un Warn), non un errore.
func TestBindProps_UnknownKeyIgnored(t *testing.T) {
	var p props
	if err := BindProps(&p, spec.Properties{"collection": "c", "colection": "typo"}); err != nil {
		t.Fatalf("una chiave sconosciuta non deve bloccare l'avvio: %v", err)
	}
	if p.Collection != "c" {
		t.Fatalf("collection errata: %q", p.Collection)
	}
}

// validate: per campo, incluso il caso senza properties (mappa nil): il required deve scattare.
func TestBindProps_ValidatePerField(t *testing.T) {
	for name, in := range map[string]spec.Properties{
		"chiave assente": {"batch-limit": 1},
		"properties nil": nil,
	} {
		var p props
		err := BindProps(&p, in)
		if err == nil {
			t.Fatalf("%s: atteso errore di validazione", name)
		}
		if !strings.Contains(err.Error(), "collection") || !strings.Contains(err.Error(), "Collection") {
			t.Fatalf("%s: l'errore deve nominare property e campo: %v", name, err)
		}
	}
}

// Un `optional:"true"` non impedisce l'iniezione: se il grafo fx fornisce davvero un valore di quel
// tipo, dig lo mette nel campo. Il mapping deve azzerarlo, così una property non riceve mai un valore
// arrivato dal grafo.
func TestBindProps_ZeroesPreexistingValue(t *testing.T) {
	p := props{Collection: "c", BatchLimit: 42, Tags: []string{"iniettato"}}
	if err := BindProps(&p, spec.Properties{"collection": "c"}); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if p.BatchLimit != 100 { // default, non il 42 preesistente
		t.Fatalf("il campo doveva essere azzerato e poi defaultato, ottenuto %d", p.BatchLimit)
	}
	if p.Tags != nil {
		t.Fatalf("il campo doveva essere azzerato, ottenuto %v", p.Tags)
	}
}

// Tag vuoto: la chiave è il nome del campo in minuscolo.
func TestBindProps_EmptyTagUsesFieldName(t *testing.T) {
	var s struct {
		Collection string `prop:""`
	}
	if err := BindProps(&s, spec.Properties{"collection": "events"}); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if s.Collection != "events" {
		t.Fatalf("chiave dedotta dal nome campo errata: %q", s.Collection)
	}
}

// Un processor che non usa il mapping (legge le properties dal context o da Configure) non deve
// ricevere errori né warning.
func TestBindProps_NoPropFields(t *testing.T) {
	var s struct {
		core.In
		Whatever string
	}
	if err := BindProps(&s, spec.Properties{"collection": "events"}); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}
	if s.Whatever != "" {
		t.Fatalf("nessun campo doveva essere toccato: %+v", s)
	}
}

func TestBindProps_RequiresPointerToStruct(t *testing.T) {
	if err := BindProps(props{}, nil); err == nil {
		t.Fatal("atteso errore passando un valore non-puntatore")
	}
}

// --- interazione con fx/dig ------------------------------------------------------------------------

type fakeDep struct{}

// fxHandler è la forma reale di un consumer: core.In, una dipendenza iniettata e i campi property, che
// per dig DEVONO portare `optional:"true"` (sono campi esportati di un param object, quindi altrimenti
// vengono cercati nel grafo).
type fxHandler struct {
	core.In
	Dep        *fakeDep
	Collection string `prop:"collection" optional:"true" validate:"required"`
	BatchLimit int    `prop:"batch-limit" optional:"true" default:"100"`
}

func (h *fxHandler) Handle(context.Context, []*message.Record) error { return nil }

// Come RegisterHandler: dig costruisce il param object, BindProps riempie i campi property.
func provideFxHandler(s spec.ConsumerSpec) fx.Option {
	return fx.Provide(func(p fxHandler) (HandlerRegistration, error) {
		pp := &p
		if err := BindProps(pp, s.Properties); err != nil {
			return HandlerRegistration{}, err
		}
		return HandlerRegistration{Consumer: s.Name, Handler: pp}, nil
	})
}

func TestBindProps_FxParamObject(t *testing.T) {
	var got *fxHandler
	app := fx.New(
		fx.NopLogger,
		fx.Supply(&fakeDep{}),
		provideFxHandler(spec.ConsumerSpec{Name: "eventi", Properties: spec.Properties{"collection": "events"}}),
		fx.Invoke(func(r HandlerRegistration) { got = r.Handler.(*fxHandler) }),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("il grafo fx deve costruirsi: %v", err)
	}
	if got == nil || got.Dep == nil {
		t.Fatal("la dipendenza doveva essere iniettata da fx")
	}
	if got.Collection != "events" || got.BatchLimit != 100 {
		t.Fatalf("properties non mappate: %+v", got)
	}
}

// Una property non valida fa fallire la costruzione del grafo: l'app non parte (nessun client Kafka
// aperto), stesso fail-fast del Configure.
func TestBindProps_FxFailFastOnInvalidProperty(t *testing.T) {
	app := fx.New(
		fx.NopLogger,
		fx.Supply(&fakeDep{}),
		provideFxHandler(spec.ConsumerSpec{Name: "eventi"}), // manca `collection` (required)
		fx.Invoke(func(HandlerRegistration) {}),
	)
	err := app.Err()
	if err == nil {
		t.Fatal("atteso fallimento della build del grafo fx")
	}
	if !strings.Contains(err.Error(), "collection") {
		t.Fatalf("l'errore fx deve riportare la property: %v", err)
	}
}

// Documenta la trappola: senza `optional:"true"` dig cerca un provider per il tipo del campo e fallisce
// con "missing type". È l'errore che si ottiene scordando il tag.
func TestBindProps_FxMissingOptionalTag(t *testing.T) {
	type badHandler struct {
		core.In
		Dep        *fakeDep
		Collection string `prop:"collection"` // manca optional:"true"
	}
	app := fx.New(
		fx.NopLogger,
		fx.Supply(&fakeDep{}),
		fx.Provide(func(p badHandler) HandlerRegistration { return HandlerRegistration{} }),
		fx.Invoke(func(HandlerRegistration) {}),
	)
	err := app.Err()
	if err == nil {
		t.Fatal("senza optional:\"true\" dig deve chiedere un provider per il campo")
	}
	if !strings.Contains(err.Error(), "missing type: string") {
		t.Fatalf("atteso 'missing type: string', ottenuto: %v", err)
	}
}
