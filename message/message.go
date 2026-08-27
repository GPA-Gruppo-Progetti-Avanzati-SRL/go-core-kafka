// Package message contiene i tipi neutri scambiati attraverso i confini di go-core-kafka:
// il record consumato (Record) e il record da produrre (ProducerRecord). Sono deliberatamente
// privi di qualsiasi dipendenza dal client Kafka concreto (oggi confluent-kafka-go) così che la
// business logic dell'app e l'astrazione driver non ne siano accoppiate: è questo confine a rendere
// possibile un futuro switch a franz-go senza toccare le app.
package message

import "time"

// Header è una coppia header di un messaggio Kafka. Il valore è []byte e non string perché nel
// protocollo un header è opaco: può portare un payload binario (un id compresso, un timestamp
// codificato), non solo testo.
type Header struct {
	Key   string
	Value []byte
}

// Headers è la lista degli header di un messaggio.
//
// È una LISTA e non una mappa perché Kafka ammette chiavi RIPETUTE, ed è una possibilità usata
// (tracing, catene di reprocessing che accodano il proprio marcatore). Con una mappa il secondo
// valore sovrascrive il primo dentro il driver, prima che la business logic possa vederlo: informazione
// persa in silenzio, e non ricostruibile a valle. I metodi coprono il caso comune — chiave unica —
// senza costringere a scorrere lo slice a mano.
//
// Le chiavi sono confrontate byte per byte (case-sensitive), come nel protocollo.
type Headers []Header

// Get ritorna il PRIMO valore della chiave come stringa, "" se la chiave è assente. È l'accesso
// giusto quando la chiave è unica: per una chiave ripetuta usare Values.
func (h Headers) Get(key string) string {
	for _, hd := range h {
		if hd.Key == key {
			return string(hd.Value)
		}
	}
	return ""
}

// Has dice se la chiave è presente, distinguendo l'assenza da un header con valore vuoto — che Get
// non può fare.
func (h Headers) Has(key string) bool {
	for _, hd := range h {
		if hd.Key == key {
			return true
		}
	}
	return false
}

// Values ritorna TUTTI i valori della chiave, nell'ordine in cui compaiono nel messaggio.
func (h Headers) Values(key string) []string {
	var out []string
	for _, hd := range h {
		if hd.Key == key {
			out = append(out, string(hd.Value))
		}
	}
	return out
}

// Set imposta la chiave a un solo valore: sostituisce la prima occorrenza (mantenendone la
// posizione) e rimuove le eventuali successive. Se la chiave è assente l'header è accodato.
func (h *Headers) Set(key, value string) {
	out := (*h)[:0]
	done := false
	for _, hd := range *h {
		if hd.Key == key {
			if done {
				continue // duplicato: Set collassa su un solo valore
			}
			hd.Value = []byte(value)
			done = true
		}
		out = append(out, hd)
	}
	if !done {
		out = append(out, Header{Key: key, Value: []byte(value)})
	}
	*h = out
}

// Add accoda un header senza rimuovere le occorrenze esistenti della stessa chiave.
func (h *Headers) Add(key, value string) {
	*h = append(*h, Header{Key: key, Value: []byte(value)})
}

// Clone ritorna una copia indipendente: modificarla non tocca l'originale. Serve a chi deriva un
// messaggio da un altro (l'instradamento al DLQ) senza mutare il record consumato.
func (h Headers) Clone() Headers {
	if h == nil {
		return nil
	}
	out := make(Headers, len(h))
	copy(out, h)
	return out
}

// Record è un messaggio Kafka consumato, in forma neutra.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   Headers
	Timestamp time.Time
}

// ProducerRecord è un messaggio da produrre (output della modalità transform o del Producer pubblico).
// Topic per-record abilita il fan-out topic->topic; se vuoto l'engine usa il DefaultOutputTopic dello
// spec del consumer.
type ProducerRecord struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers Headers
}
