package driver

import (
	"strings"
	"testing"
)

// Il punto per cui questo test esiste: un dump della configurazione è utile quanto è pericoloso se
// stampa le credenziali. `sasl.password` e `ssl.key.password` finiscono davvero nella ConfigMap del
// driver confluent, quindi senza redazione il dump le scriverebbe nei log dell'applicazione.
func TestFormatConfig_SegretiMascherati(t *testing.T) {
	segreti := []string{
		"sasl.password",
		"ssl.key.password",
		"sasl.oauthbearer.client.secret",
		"sasl.oauthbearer.config",
		"SASL.PASSWORD", // la redazione non deve dipendere dal case
		// L'escape hatch kafka-properties è aperto: una chiave segreta non prevista dai due
		// vocabolari deve essere coperta lo stesso.
		"custom.auth.token",
	}
	for _, k := range segreti {
		t.Run(k, func(t *testing.T) {
			got := FormatConfig(map[string]string{k: "s3gr3t0"})
			if len(got) != 1 {
				t.Fatalf("righe = %d, attesa 1", len(got))
			}
			if strings.Contains(got[0], "s3gr3t0") {
				t.Fatalf("il valore di %q compare in chiaro nel dump: %q", k, got[0])
			}
			if !strings.Contains(got[0], Redacted) {
				t.Errorf("riga = %q, atteso il marcatore %s", got[0], Redacted)
			}
		})
	}
}

func TestFormatConfig_NonMascheraCiOCheServe(t *testing.T) {
	// La redazione è per sottostringa: non deve mangiarsi le chiavi che servono a diagnosticare, e in
	// particolare i PATH dei file di chiave — che non sono segreti e sono la prima cosa da guardare
	// quando il TLS non parte.
	got := FormatConfig(map[string]string{
		"ssl.key.location":  "/etc/certs/client.key",
		"sasl.username":     "gpa-app",
		"bootstrap.servers": "broker:9092",
	})
	joined := strings.Join(got, "\n")
	for _, want := range []string{"/etc/certs/client.key", "gpa-app", "broker:9092"} {
		if !strings.Contains(joined, want) {
			t.Errorf("%q mascherato ma non è un segreto:\n%s", want, joined)
		}
	}
}

// L'ordine alfabetico è ciò che rende confrontabili con un diff i dump di due pod: l'iterazione di
// una mappa Go è randomizzata, quindi senza sort due avvii dello stesso processo danno liste diverse.
func TestFormatConfig_OrdineStabile(t *testing.T) {
	m := map[string]string{"zeta": "1", "alpha": "2", "mu": "3"}
	want := []string{"alpha = 2", "mu = 3", "zeta = 1"}
	for i := 0; i < 20; i++ {
		got := FormatConfig(m)
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("iterazione %d: got %v, atteso %v", i, got, want)
			}
		}
	}
}
