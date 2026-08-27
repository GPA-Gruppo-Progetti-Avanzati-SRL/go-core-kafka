package confluentdriver

import (
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func tp(topic string, partition int32, offset int64) kafka.TopicPartition {
	return kafka.TopicPartition{Topic: &topic, Partition: partition, Offset: kafka.Offset(offset)}
}

// find ritorna l'offset committato per una topic-partition, e se è presente.
func find(out []kafka.TopicPartition, topic string, partition int32) (int64, bool) {
	for _, p := range out {
		if p.Topic != nil && *p.Topic == topic && p.Partition == partition {
			return int64(p.Offset), true
		}
	}
	return 0, false
}

// L'offsetTracker è ciò su cui poggia l'intera garanzia at-least-once: se tenesse un offset più
// basso di quello consumato, i record fra i due sarebbero riletti (innocuo); se ne tenesse uno più
// alto, sarebbero saltati (perdita). Quindi: massimo per topic-partition, e commit a offset+1.
func TestOffsetTracker_TieneIlMassimoPerPartizione(t *testing.T) {
	tr := newOffsetTracker()
	if !tr.empty() {
		t.Fatal("un tracker nuovo deve essere vuoto: un commit su tracker vuoto è un no-op")
	}

	tr.track(tp("t", 0, 5))
	tr.track(tp("t", 0, 3)) // fuori ordine: non deve abbassare il massimo
	tr.track(tp("t", 0, 9))
	tr.track(tp("t", 1, 2)) // partizione diversa: indipendente
	tr.track(tp("altro", 0, 7))

	if tr.empty() {
		t.Fatal("tracker vuoto dopo track")
	}
	out := tr.commitOffsets()
	if len(out) != 3 {
		t.Fatalf("topic-partition da committare = %d, attese 3", len(out))
	}
	// commitOffsets ritorna il PROSSIMO offset da leggere, non l'ultimo letto.
	for _, c := range []struct {
		topic string
		part  int32
		want  int64
	}{{"t", 0, 10}, {"t", 1, 3}, {"altro", 0, 8}} {
		got, ok := find(out, c.topic, c.part)
		if !ok {
			t.Errorf("%s/%d assente dal commit", c.topic, c.part)
			continue
		}
		if got != c.want {
			t.Errorf("%s/%d offset committato = %d, atteso %d (ultimo letto + 1)", c.topic, c.part, got, c.want)
		}
	}
}

func TestOffsetTracker_IgnoraTopicNil(t *testing.T) {
	// Un messaggio senza topic non è indirizzabile: tracciarlo sotto una chiave vuota mescolerebbe
	// partizioni diverse.
	tr := newOffsetTracker()
	tr.track(kafka.TopicPartition{Partition: 0, Offset: 1})
	if !tr.empty() {
		t.Error("una TopicPartition senza topic non deve entrare nel tracker")
	}
}

func TestOffsetTracker_Reset(t *testing.T) {
	// reset è la primitiva su cui poggiano Discard e il rebalance callback: dopo di essa il commit
	// non deve confermare nulla.
	tr := newOffsetTracker()
	tr.track(tp("t", 0, 5))
	tr.reset()
	if !tr.empty() {
		t.Fatal("tracker non vuoto dopo reset")
	}
	if got := tr.commitOffsets(); len(got) != 0 {
		t.Errorf("commitOffsets dopo reset = %d elementi, attesi 0", len(got))
	}
	// Il tracker resta usabile: dopo lo scarto il loop continua a consumare.
	tr.track(tp("t", 0, 7))
	if got, _ := find(tr.commitOffsets(), "t", 0); got != 8 {
		t.Errorf("offset dopo il riuso = %d, atteso 8", got)
	}
}

func TestToRecord_ConservaGliHeaderRipetuti(t *testing.T) {
	// Kafka ammette chiavi ripetute. Con la vecchia map[string]string il secondo valore
	// sovrascriveva il primo QUI, prima che la business logic potesse vederli.
	ts := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	topic := "eventi"
	m := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 3, Offset: 42},
		Key:            []byte("k"),
		Value:          []byte("v"),
		Timestamp:      ts,
		Headers: []kafka.Header{
			{Key: "trace", Value: []byte("uno")},
			{Key: "trace", Value: []byte("due")},
			{Key: "altro", Value: []byte("x")},
		},
	}

	r := toRecord(m)
	if r.Topic != "eventi" || r.Partition != 3 || r.Offset != 42 {
		t.Errorf("coordinate del record errate: %s/%d@%d", r.Topic, r.Partition, r.Offset)
	}
	if !r.Timestamp.Equal(ts) {
		t.Errorf("timestamp = %v, atteso %v", r.Timestamp, ts)
	}
	if got := r.Headers.Values("trace"); len(got) != 2 || got[0] != "uno" || got[1] != "due" {
		t.Errorf("header ripetuti persi nella conversione: %v", got)
	}
	if got := r.Headers.Get("altro"); got != "x" {
		t.Errorf("header singolo = %q", got)
	}
}

func TestToMessage_RoundTrip(t *testing.T) {
	pr := &message.ProducerRecord{
		Topic: "out", Key: []byte("k"), Value: []byte("v"),
		Headers: message.Headers{
			{Key: "trace", Value: []byte("uno")},
			{Key: "trace", Value: []byte("due")},
		},
	}
	m := toMessage(pr)
	if m.TopicPartition.Topic == nil || *m.TopicPartition.Topic != "out" {
		t.Fatal("topic non impostato sul messaggio")
	}
	if m.TopicPartition.Partition != kafka.PartitionAny {
		t.Errorf("partizione = %d, attesa PartitionAny (la scelta è del partitioner)", m.TopicPartition.Partition)
	}
	if len(m.Headers) != 2 {
		t.Fatalf("header prodotti = %d, attesi 2 (le chiavi ripetute vanno sul wire)", len(m.Headers))
	}
	// Andata e ritorno: la conversione non deve perdere né riordinare.
	back := toRecord(&kafka.Message{TopicPartition: m.TopicPartition, Key: m.Key, Value: m.Value, Headers: m.Headers})
	if got := back.Headers.Values("trace"); len(got) != 2 || got[0] != "uno" || got[1] != "due" {
		t.Errorf("round trip degli header ripetuti = %v", got)
	}
}
