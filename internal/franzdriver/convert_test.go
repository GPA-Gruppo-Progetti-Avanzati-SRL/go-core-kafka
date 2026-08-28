package franzdriver

import (
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Gli header sono una LISTA e non una mappa proprio perché Kafka ammette chiavi ripetute: se la
// conversione le collassasse, la business logic non potrebbe più vederle (vedi message.Headers).
func TestToRecord_HeaderRipetuti(t *testing.T) {
	kr := &kgo.Record{
		Topic: "t", Partition: 3, Offset: 42,
		Key: []byte("k"), Value: []byte("v"),
		Timestamp: time.Unix(1, 0),
		Headers: []kgo.RecordHeader{
			{Key: "trace", Value: []byte("a")},
			{Key: "trace", Value: []byte("b")},
			{Key: "solo", Value: []byte("x")},
		},
	}
	r := toRecord(kr)

	if r.Topic != "t" || r.Partition != 3 || r.Offset != 42 {
		t.Fatalf("coordinate = %s/%d@%d, attese t/3@42", r.Topic, r.Partition, r.Offset)
	}
	if got := r.Headers.Values("trace"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Values(trace) = %v, attesi entrambi i valori nell'ordine del messaggio", got)
	}
	if r.Headers.Get("solo") != "x" {
		t.Errorf("Get(solo) = %q, atteso x", r.Headers.Get("solo"))
	}
}

// Una tombstone (Value nil) deve arrivare all'engine come tale: è Convert a decidere che farne, non
// il driver.
func TestToRecord_Tombstone(t *testing.T) {
	r := toRecord(&kgo.Record{Topic: "t", Key: []byte("k")})
	if r.Value != nil {
		t.Errorf("Value = %v, atteso nil: la tombstone non va inventata", r.Value)
	}
	if len(r.Headers) != 0 {
		t.Errorf("Headers = %v, attesi assenti", r.Headers)
	}
}

func TestToKgoRecord(t *testing.T) {
	pr := &message.ProducerRecord{
		Topic: "out", Key: []byte("k"), Value: []byte("v"),
		Headers: message.Headers{{Key: "a", Value: []byte("1")}, {Key: "a", Value: []byte("2")}},
	}
	kr := toKgoRecord(pr)
	if kr.Topic != "out" || string(kr.Key) != "k" || string(kr.Value) != "v" {
		t.Fatalf("record = %+v, atteso out/k/v", kr)
	}
	if len(kr.Headers) != 2 || kr.Headers[0].Key != "a" || string(kr.Headers[1].Value) != "2" {
		t.Errorf("Headers = %+v, attese entrambe le occorrenze", kr.Headers)
	}
}

// Il tracker tiene UN record per partizione, quello più avanti: è ciò che CommitRecords traduce in
// offset+1. Tenere il record e non l'offset è ciò che conserva il leader epoch.
func TestOffsetTracker(t *testing.T) {
	tr := newOffsetTracker()
	if !tr.empty() {
		t.Fatal("un tracker nuovo deve essere vuoto")
	}
	tr.track(&kgo.Record{Topic: "t", Partition: 0, Offset: 5})
	tr.track(&kgo.Record{Topic: "t", Partition: 0, Offset: 9})
	tr.track(&kgo.Record{Topic: "t", Partition: 0, Offset: 7}) // fuori ordine: non deve arretrare
	tr.track(&kgo.Record{Topic: "t", Partition: 1, Offset: 1})
	tr.track(&kgo.Record{Topic: "altro", Partition: 0, Offset: 3})

	recs := tr.records()
	if len(recs) != 3 {
		t.Fatalf("records = %d, attesi 3 (una per topic-partizione)", len(recs))
	}
	for _, r := range recs {
		if r.Topic == "t" && r.Partition == 0 && r.Offset != 9 {
			t.Errorf("offset t/0 = %d, atteso 9 (il più alto visto)", r.Offset)
		}
	}

	tr.reset()
	if !tr.empty() || len(tr.records()) != 0 {
		t.Error("dopo reset il tracker deve essere vuoto: è ciò che rende il commit un no-op dopo uno scarto")
	}
}
