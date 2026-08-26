package spec

import (
	"fmt"
	"sort"
	"strings"
)

// DeniedKafkaProperties elenca le chiavi che NON possono arrivare da `kafka-properties`: non sono
// default sovrascrivibili ma invarianti su cui l'engine si appoggia. Sovrascriverle non darebbe un
// comportamento diverso, ne darebbe uno rotto — e in silenzio.
//
//	bootstrap.servers  → la connessione la decide `server.bootstrap-servers`, non un knob sepolto altrove
//	group.id           → identità del consumer group, da `group-id` del processor
//	transactional.id   → identità EOS, da `transactional-id` del processor
//	enable.auto.commit → deve restare false: l'engine committa a mano dopo Handle o dentro la TX
//	isolation.level    → ha un campo tipizzato (e in transform vale read_committed)
var DeniedKafkaProperties = map[string]string{
	"bootstrap.servers":  "usare `server.bootstrap-servers`",
	"group.id":           "usare `group-id` del processor",
	"transactional.id":   "usare `transactional-id` del processor",
	"enable.auto.commit": "il commit degli offset è gestito dall'engine e non è configurabile",
	"isolation.level":    "usare `isolation-level`",
}

// ValidateKafkaProperties rifiuta le chiavi riservate. owner identifica la sezione nel messaggio
// d'errore (es. `server`, `server.producer`, processor "ingest"), così chi legge il log sa dove
// intervenire. Chiamata dai costruttori fx: una chiave riservata fa fallire l'avvio invece di essere
// ignorata.
func ValidateKafkaProperties(owner string, props map[string]string) error {
	if len(props) == 0 {
		return nil
	}
	var bad []string
	for k := range props {
		if reason, denied := DeniedKafkaProperties[normalizeKey(k)]; denied {
			bad = append(bad, fmt.Sprintf("%q (%s)", k, reason))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad) // ordine deterministico: l'iterazione della mappa non lo è
	return fmt.Errorf("%s: kafka-properties contiene chiavi riservate: %s", owner, strings.Join(bad, ", "))
}

// normalizeKey porta una chiave nella forma con cui è confrontata e scritta nella ConfigMap. È
// esportata attraverso l'uso che ne fa il driver: senza la stessa normalizzazione qui e là, una
// chiave scritta " Group.ID " passerebbe il controllo e verrebbe poi applicata comunque.
func normalizeKey(k string) string { return strings.ToLower(strings.TrimSpace(k)) }

// NormalizeKafkaProperties ritorna la mappa con le chiavi normalizzate, nell'ordine deterministico
// delle chiavi ordinate. Usata dal driver per applicarle alla ConfigMap.
func NormalizeKafkaProperties(props map[string]string) (keys []string, normalized map[string]string) {
	if len(props) == 0 {
		return nil, nil
	}
	normalized = make(map[string]string, len(props))
	for k, v := range props {
		normalized[normalizeKey(k)] = v
	}
	keys = make([]string, 0, len(normalized))
	for k := range normalized {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, normalized
}
