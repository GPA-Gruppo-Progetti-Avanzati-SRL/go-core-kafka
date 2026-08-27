package confluentdriver

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Il rebalance observer è metà della garanzia "duplicati, mai buchi": alla revoca butta gli offset
// tracciati e alza il flag che il Poll successivo trasforma in SeverityReset. Se il flag non venisse
// alzato l'engine committerebbe un batch che il nuovo owner sta rileggendo.
func TestRebalanceObserver_RevocaScartaGliOffsetEAlzaIlFlag(t *testing.T) {
	tr := newOffsetTracker()
	tr.track(tp("t", 0, 5))
	o := &rebalanceObserver{name: "test", offsets: tr}

	if o.takeRevoked() {
		t.Fatal("flag alzato senza revoca")
	}
	if err := o.callback(nil, kafka.RevokedPartitions{Partitions: []kafka.TopicPartition{tp("t", 0, 5)}}); err != nil {
		t.Fatalf("callback: %v", err)
	}
	if !tr.empty() {
		t.Error("offset non scartati alla revoca: committarli dichiarerebbe elaborati record che il nuovo owner sta rileggendo")
	}
	if !o.takeRevoked() {
		t.Error("il flag di revoca non è stato alzato: l'engine non scarterebbe il batch in volo")
	}
	// Il flag si consuma: una sola revoca non deve far scartare due batch.
	if o.takeRevoked() {
		t.Error("takeRevoked ha ritornato true due volte per la stessa revoca")
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
	if o.takeRevoked() {
		t.Error("flag di revoca alzato da un'assegnazione")
	}
}
