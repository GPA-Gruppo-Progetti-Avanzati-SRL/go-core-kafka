// Package message contiene i tipi neutri scambiati attraverso i confini di go-core-kafka:
// il record consumato (Record) e il record da produrre (ProducerRecord). Sono deliberatamente
// privi di qualsiasi dipendenza dal client Kafka concreto (oggi confluent-kafka-go) così che la
// business logic dell'app e l'astrazione driver non ne siano accoppiate: è questo confine a rendere
// possibile un futuro switch a franz-go senza toccare le app.
package message

import "time"

// Record è un messaggio Kafka consumato, in forma neutra.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// ProducerRecord è un messaggio da produrre (output della modalità transform o del Producer pubblico).
// Topic per-record abilita il fan-out topic->topic; se vuoto l'engine usa il DefaultOutputTopic dello
// spec del consumer.
type ProducerRecord struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}
