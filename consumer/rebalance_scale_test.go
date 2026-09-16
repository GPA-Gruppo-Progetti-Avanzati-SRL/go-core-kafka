//go:build mockcluster

// Variante DI SCALA del test di rebalance loss: stesso scenario, stesse helper, stessa invariante —
// ma 12 partizioni invece di 4, otto membri invece di tre e churn vero (sei ingressi a scaglioni,
// due uscite graziose, due ingressi nuovi), cioè ~10 rebalance di gruppo invece di 2, al doppio del
// rate.
//
// Esiste per una ragione precisa: portando lo stesso scenario su tpm-kafka-common, la versione a 4
// partizioni e 2 rebalance dava zero buchi in tutte le combinazioni, e quella a 12 partizioni e 10
// rebalance ne dava 4-9 per corsa. Non era cambiata la libreria: era cambiato QUANTE VOLTE si passa
// dalla finestra. Un test che passa a bassa frequenza di rebalance non dimostra l'assenza del
// guasto, dimostra di non averlo incontrato — quindi la stessa scala va misurata anche qui.
//
// In più della variante base, questo test misura la FINESTRA LATENTE: quanti offset il gruppo ha
// dichiarato committati mentre l'Handler non li aveva ancora visti. Non è una perdita (il commit
// arriva dopo il flush, per costruzione), ed è esattamente ciò che ci si aspetta di misurare a zero:
// è il controllo che l'invariante "prima l'Handler, poi il commit" regga anche sotto churn. La
// lettura passa dall'AdminClient e non da un consumer, perché entrare nel gruppo aggiungerebbe un
// rebalance che il test non ha chiesto.
//
// go test -tags mockcluster -run TestRebalanceScale -v ./consumer/
package consumer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/confluentdriver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/franzdriver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

const (
	scalePartitions = 12
	scaleRate       = 2 * time.Millisecond // ~500 rec/s
	scaleJoinEvery  = 5 * time.Second
	scaleChurnPause = 5 * time.Second
	scaleTail       = 5 * time.Second
	scaleProbeEvery = 250 * time.Millisecond
)

var scaleMembers = []string{"A", "B", "C", "D", "E", "F"}

func TestRebalanceScaleRecordLoss(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	drivers := map[string]func() driver.Factory{
		"confluent": confluentdriver.New,
		"franz":     franzdriver.New,
	}
	for _, drv := range []string{"confluent", "franz"} {
		for _, strategy := range []string{"cooperative-sticky", "range"} {
			t.Run(drv+"/"+strategy, func(t *testing.T) {
				runScaleScenario(t, drivers[drv], drv, strategy)
			})
		}
	}
}

func runScaleScenario(t *testing.T, newFactory func() driver.Factory, drv, strategy string) {
	// Le helper condivise leggono lossPartitions/produceRate: qui valgono i numeri di scala.
	oldParts, oldRate := lossPartitions, produceRate
	lossPartitions, produceRate = scalePartitions, scaleRate
	t.Cleanup(func() { lossPartitions, produceRate = oldParts, oldRate })

	topic := fmt.Sprintf("scale-%s-%s-%d", drv, strategy, time.Now().UnixNano())
	boot := startBroker(t, topic)
	groupID := "g-" + topic

	produced, stopProducing := startProducer(t, boot, topic)
	rec := newRecorder()
	del := newDeliveredSet()

	probeStop := make(chan struct{})
	probe, probeDone := startCommitProbe(boot, topic, groupID, rec, probeStop)

	type member struct {
		cancel context.CancelFunc
		done   <-chan error
	}
	live := map[string]*member{}

	start := func(label string) {
		ctx, cancel := context.WithCancel(context.Background())
		live[label] = &member{cancel: cancel, done: startRunner(t, label, boot, topic, groupID, strategy, newFactory, rec, del, ctx)}
		t.Logf("[%s] consumer %s avviato (membri vivi: %d)", time.Now().Format("15:04:05.000"), label, len(live))
	}
	stop := func(label string) {
		m, ok := live[label]
		if !ok {
			return
		}
		m.cancel()
		if err := <-m.done; err != nil {
			t.Logf("runner %s terminato con errore: %v", label, err)
		}
		delete(live, label)
		t.Logf("[%s] consumer %s uscito (membri vivi: %d)", time.Now().Format("15:04:05.000"), label, len(live))
	}

	for i, label := range scaleMembers {
		if i > 0 {
			time.Sleep(scaleJoinEvery)
		}
		start(label)
	}

	// Churn: due uscite graziose e due ingressi nuovi, ognuno un rebalance in più.
	time.Sleep(scaleChurnPause)
	stop("A")
	time.Sleep(scaleChurnPause)
	stop("B")
	time.Sleep(scaleChurnPause)
	start("G")
	time.Sleep(scaleChurnPause)
	start("H")

	time.Sleep(scaleTail)
	stopProducing()
	t.Logf("produzione terminata: %d record", produced.total())

	drain(t, rec)
	close(probeStop)
	<-probeDone

	for label := range live {
		stop(label)
	}

	delivered, discards := del.snapshot()
	t.Logf("Discard() chiamate dall'engine: %d — record consegnati da Poll: %d, processati dall'Handler: %d",
		discards, len(delivered), rec.len())
	reportCommitProbe(t, probe)
	report(t, boot, topic, groupID, produced, rec.snapshot(), delivered)
}

// --- sonda sugli offset committati ---------------------------------------------------------------

type commitProbe struct {
	mu       sync.Mutex
	maxAhead map[int32]int
	samples  int
	nonZero  int
}

func (p *commitProbe) record(part int32, ahead int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ahead > p.maxAhead[part] {
		p.maxAhead[part] = ahead
	}
}

func (p *commitProbe) tick(any bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples++
	if any {
		p.nonZero++
	}
}

// startCommitProbe legge periodicamente gli offset committati dal gruppo e SUBITO DOPO guarda cosa
// l'Handler ha già visto. L'ordine è conservativo: l'insieme dei processati è più recente della
// lettura degli offset, quindi un record contato come "committato e non processato" lo era davvero.
func startCommitProbe(boot, topic, groupID string, rec *recorder, stop <-chan struct{}) (*commitProbe, <-chan struct{}) {
	p := &commitProbe{maxAhead: map[int32]int{}}
	done := make(chan struct{})

	go func() {
		defer close(done)
		a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": boot})
		if err != nil {
			return
		}
		defer a.Close()

		parts := make([]kafka.TopicPartition, 0, lossPartitions)
		for i := 0; i < lossPartitions; i++ {
			parts = append(parts, kafka.TopicPartition{Topic: &topic, Partition: int32(i)})
		}

		for {
			select {
			case <-stop:
				return
			case <-time.After(scaleProbeEvery):
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r, err := a.ListConsumerGroupOffsets(ctx, []kafka.ConsumerGroupTopicPartitions{{Group: groupID, Partitions: parts}})
			cancel()
			if err != nil || len(r.ConsumerGroupsTopicPartitions) == 0 {
				continue
			}

			seen := rec.snapshot()
			any := false
			for _, tp := range r.ConsumerGroupsTopicPartitions[0].Partitions {
				if tp.Offset < 0 {
					continue
				}
				ahead := 0
				for off := int64(tp.Offset) - 1; off >= 0; off-- {
					if _, ok := seen[offsetKey{tp.Partition, off}]; ok {
						break
					}
					ahead++
					if ahead > 5000 {
						break
					}
				}
				if ahead > 0 {
					any = true
					p.record(tp.Partition, ahead)
				}
			}
			p.tick(any)
		}
	}()

	return p, done
}

func reportCommitProbe(t *testing.T, p *commitProbe) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	tot := 0
	for _, v := range p.maxAhead {
		tot += v
	}
	t.Logf("FINESTRA LATENTE: campioni=%d  campioni-con-commit-in-anticipo=%d  max-record-committati-non-processati-per-partizione=%v  (somma=%d)",
		p.samples, p.nonZero, p.maxAhead, tot)
}
