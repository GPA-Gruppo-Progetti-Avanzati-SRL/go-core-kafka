package corekafka

import (
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

func dlqSpec(name, dlq string, disabled bool) spec.ProcessorSpec {
	s := spec.ProcessorSpec{Name: name, Topics: []string{"t"}, GroupID: "g", Disabled: disabled}
	if dlq != "" {
		s.Consumer.DeadletterTopic = &dlq
	}
	return s
}

// ActiveProcessors è l'UNICO punto in cui la lista `processors` viene filtrata: se il filtro tornasse
// a esistere anche a valle (nell'engine, come prima) le due copie potrebbero divergere.
func TestActiveProcessors(t *testing.T) {
	cfg := Config{
		Processors: []spec.ProcessorSpec{dlqSpec("a", "", false), dlqSpec("b", "", true), dlqSpec("c", "", false)},
	}
	got := cfg.ActiveProcessors()
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "c" {
		t.Fatalf("attivi = %v, attesi i due non disabilitati nell'ordine di config", got)
	}
}

// Gli spec ritornati sono GREZZI: l'engine ha bisogno dei blocchi non risolti per attribuire errori e
// avvisi a chi li ha scritti, e la risoluzione la rifà lui. Risolverli qui gliela toglierebbe.
func TestActiveProcessors_NonRisolve(t *testing.T) {
	comune := "comune.DLQ"
	cfg := Config{
		Server:     spec.KafkaServer{Consumer: spec.ConsumerTuning{DeadletterTopic: &comune}},
		Processors: []spec.ProcessorSpec{dlqSpec("a", "", false)},
	}
	got := cfg.ActiveProcessors()
	if got[0].Consumer.MaxBatchSize != 0 {
		t.Errorf("MaxBatchSize = %d, atteso 0: gli spec non devono essere risolti", got[0].Consumer.MaxBatchSize)
	}
	if got[0].Consumer.DeadletterTopic != nil {
		t.Error("il deadletter-topic ereditato non deve comparire su uno spec grezzo")
	}
}

// needsDeadletterProducer è l'unica decisione condizionale del Module. Al wiring la modalità di un
// processor non è ancora nota, quindi basta un deadletter-topic: un transform lascerebbe il Producer
// inutilizzato, che costa molto meno di un engine che al boot lo chiede e non lo trova.
func TestNeedsDeadletterProducer(t *testing.T) {
	tests := []struct {
		name   string
		active []spec.ProcessorSpec
		want   bool
	}{
		{"nessun processor", nil, false},
		{"nessun deadletter-topic", []spec.ProcessorSpec{dlqSpec("a", "", false)}, false},
		{"un processor con DLQ", []spec.ProcessorSpec{dlqSpec("a", "", false), dlqSpec("b", "b.DLQ", false)}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := needsDeadletterProducer(tc.active, spec.KafkaServer{}); got != tc.want {
				t.Errorf("needsDeadletterProducer = %v, atteso %v", got, tc.want)
			}
		})
	}
}

// Il caso per cui needsDeadletterProducer risolve invece di leggere il campo grezzo: un processor che
// eredita il DLQ da `server.consumer` non lo ha scritto su di sé, ma il Producer gli serve lo stesso.
func TestNeedsDeadletterProducer_DeadletterEreditato(t *testing.T) {
	comune := "comune.DLQ"
	server := spec.KafkaServer{Consumer: spec.ConsumerTuning{DeadletterTopic: &comune}}
	active := []spec.ProcessorSpec{dlqSpec("a", "", false)}

	if needsDeadletterProducer(active, spec.KafkaServer{}) {
		t.Fatal("senza globale non c'è alcun DLQ: il Producer non serve")
	}
	if !needsDeadletterProducer(active, server) {
		t.Error("il deadletter-topic ereditato dal globale deve far costruire il Producer")
	}
}

// Un processor disabilitato non deve tirarsi dietro il Producer: non consumerà nulla, e il suo
// deadletter-topic non ha destinatario. Il filtro sta in ActiveProcessors, quindi va verificato insieme.
func TestNeedsDeadletterProducer_IgnoraIDisabilitati(t *testing.T) {
	cfg := Config{Processors: []spec.ProcessorSpec{dlqSpec("spento", "spento.DLQ", true)}}
	if needsDeadletterProducer(cfg.ActiveProcessors(), cfg.Server) {
		t.Error("il DLQ di un processor disabilitato non deve far costruire il Producer")
	}
}

// WithDriver è OBBLIGATORIA: senza, non esiste alcun client Kafka da usare e il messaggio deve
// portare chi legge ai due import possibili. È un panic e non un errore fx perché senza driver non
// c'è nemmeno un grafo da costruire, e l'errore va a chi scrive la composition root.
func TestModule_SenzaDriverPanica(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Module senza WithDriver deve panicare")
		}
		msg, _ := r.(string)
		for _, want := range []string{"WithDriver", "confluentdriver.Driver", "franzdriver.Driver", "CGO_ENABLED=1"} {
			if !strings.Contains(msg, want) {
				t.Errorf("panic = %q, atteso contenesse %q", msg, want)
			}
		}
	}()
	Module(&Config{}, func() {})
}

// La Driver non prende i modes: li eredita dal Module che la invoca. Se li prendesse, un'app potrebbe
// registrare il driver in modi diversi da quelli dei suoi consumer — un grafo che a runtime non si
// costruisce, per una divergenza che nessuno ha motivo di volere.
func TestWithDriver(t *testing.T) {
	var o options
	called := 0
	WithDriver(func() { called++ })(&o)

	if o.driver == nil {
		t.Fatal("WithDriver non ha registrato il driver")
	}
	o.driver()
	if called != 1 {
		t.Errorf("driver invocato %d volte, attesa 1", called)
	}
}
