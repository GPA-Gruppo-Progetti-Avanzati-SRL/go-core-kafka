package driver

import (
	"errors"
	"fmt"
	"testing"
)

// Questo package era a copertura ZERO pur essendo quello da cui dipende ogni decisione della
// supervisione: restartable, absorb e la scelta fra ricostruire il client e far uscire il processo
// leggono tutte una Severity, e la leggono da qui.

// Il caso che conta più di tutti: SeverityBusiness DEVE essere lo zero value. Un errore che non viene
// dal driver — cioè quello risalito da Handle/Transform — ricade lì per costruzione, ed è ciò che
// permette a SeverityOf di non avere un ramo "sconosciuto". Cambiare l'ordine delle costanti
// riclassificherebbe in silenzio ogni errore applicativo.
func TestSeverityBusinessEZeroValue(t *testing.T) {
	var s Severity
	if s != SeverityBusiness {
		t.Fatalf("zero value = %v, atteso SeverityBusiness: un errore non del driver ricadrebbe altrove", s)
	}
}

func TestSeverityOf(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Severity
	}{
		{"errore del driver", NewError(SeverityRetriable, "poll", errors.New("x")), SeverityRetriable},
		// La catena va attraversata: l'engine riceve errori già wrappati dai propri strati.
		{"wrappato", fmt.Errorf("contesto: %w", NewError(SeverityFatal, "commit", errors.New("x"))), SeverityFatal},
		{"errore dell'app", errors.New("mongo irraggiungibile"), SeverityBusiness},
		{"nil", nil, SeverityBusiness},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := SeverityOf(tc.err); got != tc.want {
				t.Errorf("SeverityOf = %v, atteso %v", got, tc.want)
			}
		})
	}
}

// String è usata come label Prometheus (corekafka_consumer_restarts_total{severity}): un valore
// vuoto o duplicato renderebbe la metrica illeggibile proprio nell'incidente in cui serve.
func TestSeverityString(t *testing.T) {
	all := []Severity{
		SeverityBusiness, SeverityPermanent, SeverityFatal,
		SeverityRetriable, SeverityAbort, SeverityReset,
	}
	seen := map[string]bool{}
	for _, s := range all {
		got := s.String()
		if got == "" || got == "unknown" {
			t.Errorf("String() di %d = %q", s, got)
		}
		if seen[got] {
			t.Errorf("etichetta duplicata %q: due severità indistinguibili come label", got)
		}
		seen[got] = true
	}
	if got := Severity(99).String(); got != "unknown" {
		t.Errorf("String() di una severità fuori enum = %q, atteso \"unknown\"", got)
	}
}

func TestError_MessaggioEUnwrap(t *testing.T) {
	cause := errors.New("connection refused")
	err := NewError(SeverityRetriable, "poll", cause)

	// Op finisce nel messaggio proprio per non dover risalire lo stack per sapere dove è nato.
	msg := err.Error()
	for _, want := range []string{"poll", "retriable", "connection refused"} {
		if !contains(msg, want) {
			t.Errorf("messaggio = %q, atteso contenga %q", msg, want)
		}
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is non attraversa il driver.Error: la causa originale sarebbe irraggiungibile")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
