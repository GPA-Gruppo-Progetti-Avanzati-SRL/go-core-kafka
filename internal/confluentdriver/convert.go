package confluentdriver

import (
	"strconv"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// toRecord converte un *kafka.Message nel Record neutro.
func toRecord(m *kafka.Message) *message.Record {
	r := &message.Record{
		Partition: m.TopicPartition.Partition,
		Offset:    int64(m.TopicPartition.Offset),
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
	}
	if m.TopicPartition.Topic != nil {
		r.Topic = *m.TopicPartition.Topic
	}
	// Copia 1:1, chiavi ripetute incluse: message.Headers è una lista proprio per non perderle
	// (una mappa collasserebbe qui, prima che la business logic possa vederle).
	if len(m.Headers) > 0 {
		r.Headers = make(message.Headers, 0, len(m.Headers))
		for _, h := range m.Headers {
			r.Headers = append(r.Headers, message.Header{Key: h.Key, Value: h.Value})
		}
	}
	return r
}

// toMessage converte un ProducerRecord neutro in *kafka.Message.
func toMessage(r *message.ProducerRecord) *kafka.Message {
	topic := r.Topic
	m := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            r.Key,
		Value:          r.Value,
	}
	for _, h := range r.Headers {
		m.Headers = append(m.Headers, kafka.Header{Key: h.Key, Value: h.Value})
	}
	return m
}

// offsetTracker accumula, per topic-partition, l'offset più alto consumato dall'ultimo reset. Serve
// sia al commit at-least-once (GroupConsumer.Commit) sia a SendOffsetsToTransaction (EOS).
type offsetTracker struct {
	m map[string]kafka.TopicPartition
	// first è il PRIMO offset consegnato dall'ultimo commit, per partizione: è il punto da cui
	// quella partizione va riavvolta se il batch viene buttato. Il massimo dice fin dove si è
	// arrivati, il minimo da dove si deve tornare — servono entrambi.
	first map[string]kafka.TopicPartition
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{
		m:     make(map[string]kafka.TopicPartition),
		first: make(map[string]kafka.TopicPartition),
	}
}

func (t *offsetTracker) track(tp kafka.TopicPartition) {
	if tp.Topic == nil {
		return
	}
	key := *tp.Topic + "/" + strconv.Itoa(int(tp.Partition))
	if cur, ok := t.m[key]; !ok || tp.Offset > cur.Offset {
		t.m[key] = tp
	}
	if cur, ok := t.first[key]; !ok || tp.Offset < cur.Offset {
		t.first[key] = tp
	}
}

// rewindOffsets ritorna le partizioni da riportare al primo offset non committato: è ciò che serve a
// SeekPartitions quando il batch viene buttato per intero.
func (t *offsetTracker) rewindOffsets() []kafka.TopicPartition {
	out := make([]kafka.TopicPartition, 0, len(t.first))
	for _, tp := range t.first {
		out = append(out, tp)
	}
	return out
}

// commitOffsets ritorna le TopicPartition da committare (offset+1 = prossimo da leggere).
func (t *offsetTracker) commitOffsets() []kafka.TopicPartition {
	out := make([]kafka.TopicPartition, 0, len(t.m))
	for _, tp := range t.m {
		topic := tp.Topic
		out = append(out, kafka.TopicPartition{Topic: topic, Partition: tp.Partition, Offset: tp.Offset + 1})
	}
	return out
}

func (t *offsetTracker) empty() bool { return len(t.m) == 0 }

func (t *offsetTracker) reset() {
	t.m = make(map[string]kafka.TopicPartition)
	t.first = make(map[string]kafka.TopicPartition)
}

// resetPartitions scarta gli offset tracciati delle sole partizioni indicate. È lo scarto di una
// revoca PARZIALE: le partizioni che restano nostre conservano i loro offset, perché i record
// corrispondenti sono ancora nel batch dell'engine e verranno elaborati e committati normalmente.
func (t *offsetTracker) resetPartitions(parts []driver.TopicPartition) {
	for _, p := range parts {
		key := p.Topic + "/" + strconv.Itoa(int(p.Partition))
		delete(t.m, key)
		delete(t.first, key)
	}
}
