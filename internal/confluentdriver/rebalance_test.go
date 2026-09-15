package confluentdriver

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Il rebalance observer è metà della garanzia "duplicati, mai buchi": alla revoca butta gli offset
// tracciati delle partizioni perse e registra quali sono, così il Poll successivo può trasformarle in
// un SeverityReset parziale. Senza quella segnalazione l'engine committerebbe record che il nuovo
// owner sta rileggendo.
func TestRebalanceObserver_RevocaScartaGliOffsetEAlzaIlFlag(t *testing.T) {
	tr := newOffsetTracker()
	tr.track(tp("t", 0, 5))
	o := &rebalanceObserver{name: "test", offsets: tr}

	if parts, _ := o.takeRevoked(); len(parts) > 0 {
		t.Fatal("revoca segnalata senza revoca")
	}
	if err := o.callback(nil, kafka.RevokedPartitions{Partitions: []kafka.TopicPartition{tp("t", 0, 5)}}); err != nil {
		t.Fatalf("callback: %v", err)
	}
	if !tr.empty() {
		t.Error("offset non scartati alla revoca: committarli dichiarerebbe elaborati record che il nuovo owner sta rileggendo")
	}
	if parts, _ := o.takeRevoked(); len(parts) != 1 {
		t.Error("la revoca non è stata segnalata: l'engine non scarterebbe i record di quelle partizioni")
	}
	// La revoca si consuma: una sola revoca non deve far filtrare due batch.
	if parts, _ := o.takeRevoked(); len(parts) > 0 {
		t.Error("takeRevoked ha segnalato due volte la stessa revoca")
	}
}

func TestRebalanceObserver_AssegnazioneNonToccaGliOffset(t *testing.T) {
	// L'assegnazione è solo osservabilità: scartare gli offset qui butterebbe il lavoro di un batch
	// valido a ogni join di un nuovo membro.
	tr := newOffsetTracker()
	tr.track(tp("t", 0, 5))
	o := &rebalanceObserver{name: "test", offsets: tr}

	if err := o.callback(nil, kafka.AssignedPartitions{Partitions: []kafka.TopicPartition{tp("t", 0, 0)}}); err != nil {
		t.Fatalf("callback: %v", err)
	}
	if tr.empty() {
		t.Error("offset scartati su AssignedPartitions")
	}
	if parts, _ := o.takeRevoked(); len(parts) > 0 {
		t.Error("revoca segnalata da un'assegnazione")
	}
}
