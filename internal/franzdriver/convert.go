package franzdriver

import (
	"strconv"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// toRecord converte un *kgo.Record nel Record neutro.
func toRecord(r *kgo.Record) *message.Record {
	out := &message.Record{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
	}
	// Copia 1:1, chiavi ripetute incluse: message.Headers è una lista proprio per non perderle.
	if len(r.Headers) > 0 {
		out.Headers = make(message.Headers, 0, len(r.Headers))
		for _, h := range r.Headers {
			out.Headers = append(out.Headers, message.Header{Key: h.Key, Value: h.Value})
		}
	}
	return out
}

// toKgoRecord converte un ProducerRecord neutro in *kgo.Record. La partizione la sceglie il
// partitioner del client (default: sticky per chiave), come nel driver confluent con PartitionAny.
func toKgoRecord(r *message.ProducerRecord) *kgo.Record {
	out := &kgo.Record{
		Topic: r.Topic,
		Key:   r.Key,
		Value: r.Value,
	}
	for _, h := range r.Headers {
		out.Headers = append(out.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	return out
}

// offsetTracker conserva, per topic-partizione, il RECORD con l'offset più alto consumato dall'ultimo
// reset. Tiene il record e non l'offset perché è ciò che serve a kgo.Client.CommitRecords, che da un
// record ricava offset+1 E il leader epoch: committare un offset nudo perderebbe l'epoch, cioè la
// protezione contro il commit fatto da un membro di una generazione ormai superata.
//
// È l'equivalente franz dell'offsetTracker del driver confluent: stessa disciplina (massimo per
// partizione, azzeramento allo scarto), rappresentazione diversa perché diverso è ciò che il client
// vuole ricevere al commit.
type offsetTracker struct {
	m map[string]*kgo.Record
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{m: make(map[string]*kgo.Record)}
}

func (t *offsetTracker) track(r *kgo.Record) {
	key := r.Topic + "/" + strconv.Itoa(int(r.Partition))
	if cur, ok := t.m[key]; !ok || r.Offset > cur.Offset {
		t.m[key] = r
	}
}

// records ritorna un record per partizione, quello più avanti: è l'argomento di CommitRecords.
func (t *offsetTracker) records() []*kgo.Record {
	out := make([]*kgo.Record, 0, len(t.m))
	for _, r := range t.m {
		out = append(out, r)
	}
	return out
}

func (t *offsetTracker) empty() bool { return len(t.m) == 0 }

func (t *offsetTracker) reset() { t.m = make(map[string]*kgo.Record) }
