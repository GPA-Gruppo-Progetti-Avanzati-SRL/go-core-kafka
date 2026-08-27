package corekafka

import (
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

// ActiveSpecs è la passata unica sulla lista `processors` da cui il wiring prende le sue due
// decisioni. Risolve, perché deadletter-topic è ereditabile: un processor che lo prende dal globale
// non lo ha scritto su di sé, ma il Producer gli serve lo stesso.
func TestActiveSpecs(t *testing.T) {
	comune := "comune.DLQ"
	cfg := Config{
		Kafka:      spec.KafkaServer{Consumer: spec.ConsumerTuning{DeadletterTopic: &comune}},
		Processors: []spec.ProcessorSpec{dlqSpec("a", "", false), dlqSpec("b", "", true)},
	}
	got := cfg.ActiveSpecs()
	if len(got) != 1 || got[0].Name != "a" {
		t.Fatalf("attivi = %v, atteso il solo processor non disabilitato", got)
	}
	if got[0].Consumer.Deadletter() != comune {
		t.Errorf("deadletter-topic = %q, atteso quello ereditato dal globale", got[0].Consumer.Deadletter())
	}
	if got[0].Consumer.MaxBatchSize != spec.DefaultMaxBatchSize {
		t.Error("gli spec ritornati devono essere risolti (default applicati)")
	}
}

// needsDeadletterProducer è l'unica decisione condizionale del Module. Al wiring la modalità di un
// processor non è ancora nota, quindi basta un deadletter-topic: un transform lascerebbe il Producer
// inutilizzato, che costa molto meno di un engine che al boot lo chiede e non lo trova.
func TestNeedsDeadletterProducer(t *testing.T) {
	tests := []struct {
		name   string
		active []spec.ProcessorSpec
		force  bool
		want   bool
	}{
		{"nessun processor", nil, false, false},
		{"nessun deadletter-topic", []spec.ProcessorSpec{dlqSpec("a", "", false)}, false, false},
		{"un processor con DLQ", []spec.ProcessorSpec{dlqSpec("a", "", false), dlqSpec("b", "b.DLQ", false)}, false, true},
		{"WithProducer forza senza DLQ", []spec.ProcessorSpec{dlqSpec("a", "", false)}, true, true},
		{"WithProducer senza processor", nil, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := needsDeadletterProducer(tc.active, tc.force); got != tc.want {
				t.Errorf("needsDeadletterProducer = %v, atteso %v", got, tc.want)
			}
		})
	}
}

// Un processor disabilitato non deve tirarsi dietro il Producer: non consumerà nulla, e il suo
// deadletter-topic non ha destinatario. Il filtro sta in ActiveSpecs, quindi va verificato insieme.
func TestNeedsDeadletterProducer_IgnoraIDisabilitati(t *testing.T) {
	cfg := Config{Processors: []spec.ProcessorSpec{dlqSpec("spento", "spento.DLQ", true)}}
	if needsDeadletterProducer(cfg.ActiveSpecs(), false) {
		t.Error("il DLQ di un processor disabilitato non deve far costruire il Producer")
	}
}
