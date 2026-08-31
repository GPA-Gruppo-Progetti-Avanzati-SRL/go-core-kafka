package driver

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"
)

// Redacted sostituisce il valore delle chiavi che non devono comparire nei log.
const Redacted = "[redacted]"

// isSecret dice se il VALORE di una chiave di configurazione non va scritto nei log.
//
// Il match è su sottostringa e non su una lista chiusa di proprietà: le chiavi arrivano da due
// vocabolari diversi (librdkafka e franz-go) più l'escape hatch `kafka-properties`, che è per
// definizione aperto — una lista chiusa avrebbe mancato la prima proprietà segreta non prevista, e
// il modo in cui se ne sarebbe accorto qualcuno è leggendo una password nei log.
func isSecret(key string) bool {
	k := strings.ToLower(key)
	for _, marker := range []string{"password", "secret", "token", "sasl.oauthbearer.config"} {
		if strings.Contains(k, marker) {
			return true
		}
	}
	return false
}

// FormatConfig rende una configurazione una lista di righe `chiave = valore`, ordinate per chiave e
// con i segreti mascherati.
//
// L'ordine alfabetico non è estetica: senza, due avvii dello stesso processo producono liste in
// ordine diverso (l'iterazione di una mappa Go è randomizzata) e diventa impossibile confrontare la
// configurazione di due pod con un diff.
func FormatConfig(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, k := range keys {
		v := m[k]
		if isSecret(k) {
			v = Redacted
		}
		lines = append(lines, k+" = "+v)
	}
	return lines
}

// LogConfig emette a Debug la configurazione EFFETTIVA di un client Kafka, sul modello di ciò che il
// driver Java stampa al boot (`ConsumerConfig values: ...`).
//
// "Effettiva" con un limite da conoscere: è la configurazione che go-core-kafka IMPOSTA sul client,
// non quella che il client userà. I default che il client applica per conto suo non compaiono, perché
// né librdkafka né franz-go espongono la configurazione risolta. È il motivo per cui i knob su cui i
// due client non concordano hanno un default della LIBRERIA (vedi spec): quelli sono scritti, quindi
// nel dump si vedono.
//
// role è "consumer" o "producer"; owner è la sezione che possiede il tuning (`processor <nome>` o
// `server.producer`), cioè dove andare a correggere ciò che si legge.
func LogConfig(owner, role string, m map[string]string) {
	e := log.Debug()
	if !e.Enabled() {
		// La formattazione (copia, sort, redazione) non va pagata se nessuno leggerà il risultato.
		return
	}
	e.Str("owner", owner).Str("role", role).Strs("config", FormatConfig(m)).
		Msgf("corekafka: configurazione del client %s di %s", role, owner)
}

// LogConfigValues è LogConfig per una mappa di valori non stringa (la kafka.ConfigMap del driver
// confluent, che è una map[string]interface{}).
func LogConfigValues(owner, role string, m map[string]any) {
	if !log.Debug().Enabled() {
		return
	}
	s := make(map[string]string, len(m))
	for k, v := range m {
		s[k] = fmt.Sprint(v)
	}
	LogConfig(owner, role, s)
}
