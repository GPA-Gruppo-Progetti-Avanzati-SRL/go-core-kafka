package corekafka

import (
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

func spec1(name string) spec.ProcessorSpec {
	return spec.ProcessorSpec{Name: name, GroupID: "g", Topics: []string{"t"}}
}

// `consumers` è la chiave storica: rinominarla in `processors` non deve rompere i config esistenti,
// ma le due non si fondono — una fusione silenziosa nasconderebbe una migrazione lasciata a metà.
func TestConfig_Processors(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantNames []string
	}{
		{
			name:      "solo processors (chiave corrente)",
			cfg:       Config{Processors: []spec.ProcessorSpec{spec1("a"), spec1("b")}},
			wantNames: []string{"a", "b"},
		},
		{
			name:      "solo consumers (chiave deprecata)",
			cfg:       Config{Consumers: []spec.ProcessorSpec{spec1("legacy")}},
			wantNames: []string{"legacy"},
		},
		{
			name: "entrambe: vince processors",
			cfg: Config{
				Processors: []spec.ProcessorSpec{spec1("nuovo")},
				Consumers:  []spec.ProcessorSpec{spec1("vecchio")},
			},
			wantNames: []string{"nuovo"},
		},
		{
			name:      "nessuna delle due",
			cfg:       Config{},
			wantNames: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.cfg.processors()
			if len(got) != len(tc.wantNames) {
				t.Fatalf("processor = %d, attesi %d", len(got), len(tc.wantNames))
			}
			for i, want := range tc.wantNames {
				if got[i].Name != want {
					t.Errorf("processor[%d] = %q, atteso %q", i, got[i].Name, want)
				}
			}
		})
	}
}
