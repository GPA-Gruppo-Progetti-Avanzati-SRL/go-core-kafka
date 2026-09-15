//go:build mockcluster

// Test di integrazione della modalità transform/EOS, gemello di rebalance_loss_test.go ma con una
// verità diversa: lì contava cosa l'Handler aveva visto, qui conta cosa esiste nel TOPIC DI OUTPUT.
//
// In EOS un batch può essere elaborato e poi abortito: il Transformer l'ha visto, ma la transazione
// annullata non ha prodotto nulla. Se dopo l'abort il consumo non torna indietro, quei record non li
// rilegge nessuno e il commit successivo ci passa sopra — input consumati che non hanno mai generato
// il loro output. Chiedere al Transformer cosa ha visto non lo rivelerebbe; leggere l'output sì.
//
// INVARIANTE: per ogni partizione dell'input, ogni offset sotto l'offset committato DEVE avere il
// suo record nel topic di output. I duplicati sono contati e riportati: con l'EOS non dovrebbero
// esserci (i consumer read_committed non vedono le transazioni abortite), ma non fanno fallire il
// test — l'at-least-once è il minimo garantito, l'exactly-once è ciò che si vuole verificare.
//
// go test -tags mockcluster -run TestEOS -v ./consumer/
package consumer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/confluentdriver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/franzdriver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// eosTransformer mappa ogni record di input in un record di output che ne PORTA LE COORDINATE:
// "partizione:offset" dell'input. È ciò che rende verificabile la copertura leggendo solo l'output.
type eosTransformer struct {
	outTopic string

	mu   sync.Mutex
	seen int // solo per misurare il progresso durante il drain
}

func (e *eosTransformer) Transform(_ context.Context, batch []*message.Record) ([]*message.ProducerRecord, error) {
	time.Sleep(handlerDelay)
	out := make([]*message.ProducerRecord, 0, len(batch))
	for _, r := range batch {
		out = append(out, &message.ProducerRecord{
			Topic: e.outTopic,
			Key:   r.Key,
			Value: []byte(fmt.Sprintf("%d:%d", r.Partition, r.Offset)),
		})
	}
	e.mu.Lock()
	e.seen += len(batch)
	e.mu.Unlock()
	return out, nil
}

func (e *eosTransformer) progress() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seen
}

func TestEOSRecordLoss(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	drivers := map[string]func() driver.Factory{
		"confluent": confluentdriver.New,
		"franz":     franzdriver.New,
	}
	for _, drv := range []string{"confluent", "franz"} {
		t.Run(drv, func(t *testing.T) {
			runEOSScenario(t, drivers[drv], drv)
		})
	}
}

func runEOSScenario(t *testing.T, newFactory func() driver.Factory, drv string) {
	stamp := time.Now().UnixNano()
	inTopic := fmt.Sprintf("eos-in-%s-%d", drv, stamp)
	outTopic := fmt.Sprintf("eos-out-%s-%d", drv, stamp)
	boot := startBroker(t, inTopic)
	createTopic(t, boot, outTopic)
	groupID := "g-" + inTopic

	produced, stopProducing := startProducer(t, boot, inTopic)
	tr := &eosTransformer{outTopic: outTopic}
	del := newDeliveredSet()

	type member struct {
		cancel context.CancelFunc
		done   <-chan error
	}
	var members []member
	start := func(label string) {
		ctx, cancel := context.WithCancel(context.Background())
		members = append(members, member{
			cancel: cancel,
			done:   startEOSRunner(t, label, boot, inTopic, outTopic, groupID, newFactory, tr, del, ctx),
		})
		t.Logf("[%s] consumer %s avviato", time.Now().Format("15:04:05.000"), label)
	}

	// Come nel test handle: i rebalance non sono simulati, sono provocati da membri che entrano.
	start("A")
	time.Sleep(joinSecond)
	start("B")
	time.Sleep(joinThird - joinSecond)
	start("C")

	time.Sleep(produceFor - joinThird)
	stopProducing()
	t.Logf("produzione terminata: %d record", produced.total())

	drainEOS(t, tr)
	for _, m := range members {
		m.cancel()
	}
	for _, m := range members {
		if err := <-m.done; err != nil {
			t.Logf("runner terminato con errore: %v", err)
		}
	}

	delivered, discards := del.snapshot()
	t.Logf("Discard() chiamate dall'engine: %d — record consegnati da Poll: %d, transform eseguiti: %d",
		discards, len(delivered), tr.progress())
	reportEOS(t, boot, inTopic, outTopic, groupID, produced)
}

func drainEOS(t *testing.T, tr *eosTransformer) {
	t.Helper()
	deadline := time.Now().Add(drainMax)
	last, stableSince := tr.progress(), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		if n := tr.progress(); n != last {
			last, stableSince = n, time.Now()
			continue
		}
		if time.Since(stableSince) >= drainQuiet {
			t.Logf("drenato: %d transform eseguiti, nessun progresso da %s", last, drainQuiet)
			return
		}
	}
	t.Logf("drain interrotto dal timeout: %d transform eseguiti", last)
}

func startEOSRunner(t *testing.T, label, boot, inTopic, outTopic, groupID string, newFactory func() driver.Factory, tf processor.Transformer, del *deliveredSet, ctx context.Context) <-chan error {
	t.Helper()
	server := spec.KafkaServer{BootstrapServers: boot}.WithDefaults()
	raw := spec.ProcessorSpec{
		Name:    lossProcessor,
		Topics:  []string{inTopic},
		GroupID: groupID,
		// L'id transazionale dev'essere UNIVOCO PER REPLICA: con lo stesso id i tre membri si
		// fencano a vicenda, che è il modo più rapido per non testare nulla.
		TransactionalID:    fmt.Sprintf("tx-%s-%s", groupID, label),
		DefaultOutputTopic: outTopic,
		Consumer: spec.ConsumerTuning{
			AutoOffsetReset:             "earliest",
			PartitionAssignmentStrategy: "cooperative-sticky",
			CutFrequency:                lossCutFrequency,
			MaxPollIntervalMs:           lossMaxPollIntervalMs,
			SessionTimeoutMs:            lossSessionTimeoutMs,
			HeartbeatIntervalMs:         lossHeartbeatMs,
		},
	}
	f := countingFactory{Factory: newFactory(), d: del}
	r, err := newRunner(raw, server, seams{transformers: map[string]processor.Transformer{lossProcessor: tf}}, f, nil)
	if err != nil {
		t.Fatalf("newRunner(%s): %v", label, err)
	}

	done := make(chan error, 1)
	go func() {
		log.Info().Str("member", label).Msg("test: avvio runner EOS")
		done <- r.run(ctx)
	}()
	return done
}

// readOutput legge TUTTO il topic di output in isolation read_committed e ritorna, per ogni coppia
// "partizione:offset" dell'input, quante volte compare. read_committed è essenziale: i record di una
// transazione abortita non devono essere contati come prodotti.
func readOutput(t *testing.T, boot, topic string) map[offsetKey]int {
	t.Helper()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  boot,
		"group.id":           "reader-" + topic,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	})
	if err != nil {
		t.Fatalf("consumer di lettura output: %v", err)
	}
	defer c.Close()

	parts := make([]kafka.TopicPartition, 0, lossPartitions)
	for i := 0; i < lossPartitions; i++ {
		parts = append(parts, kafka.TopicPartition{Topic: &topic, Partition: int32(i), Offset: kafka.OffsetBeginning})
	}
	if err := c.Assign(parts); err != nil {
		t.Fatalf("assign output: %v", err)
	}

	out := make(map[offsetKey]int)
	idle := 0
	for idle < 6 { // ~3s senza record = topic esaurito
		ev := c.Poll(500)
		m, ok := ev.(*kafka.Message)
		if !ok {
			idle++
			continue
		}
		idle = 0
		var p int32
		var off int64
		if _, err := fmt.Sscanf(string(m.Value), "%d:%d", &p, &off); err != nil {
			t.Fatalf("valore di output non riconosciuto %q: %v", string(m.Value), err)
		}
		out[offsetKey{p, off}]++
	}
	return out
}

func reportEOS(t *testing.T, boot, inTopic, outTopic, groupID string, produced *producedCounts) {
	t.Helper()

	output := readOutput(t, boot, outTopic)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": boot, "group.id": groupID})
	if err != nil {
		t.Fatalf("consumer di controllo: %v", err)
	}
	defer c.Close()

	parts := make([]kafka.TopicPartition, 0, lossPartitions)
	for i := 0; i < lossPartitions; i++ {
		parts = append(parts, kafka.TopicPartition{Topic: &inTopic, Partition: int32(i)})
	}
	committed, err := c.Committed(parts, 15000)
	if err != nil {
		t.Fatalf("lettura offset committati: %v", err)
	}
	sort.Slice(committed, func(i, j int) bool { return committed[i].Partition < committed[j].Partition })

	var totalHoles, totalDup, totalCommitted int64
	for _, tp := range committed {
		if tp.Offset < 0 {
			t.Logf("p%-2d  committed=-        prodotti=%d", tp.Partition, produced.get(tp.Partition))
			continue
		}
		var holes []int64
		var dup int
		for off := int64(0); off < int64(tp.Offset); off++ {
			n, ok := output[offsetKey{tp.Partition, off}]
			if !ok {
				holes = append(holes, off)
				continue
			}
			dup += n - 1
		}
		totalHoles += int64(len(holes))
		totalDup += int64(dup)
		totalCommitted += int64(tp.Offset)

		sample := holes
		if len(sample) > 12 {
			sample = sample[:12]
		}
		t.Logf("p%-2d  committed=%-7d prodotti=%-7d in output=%-7d buchi=%-6d duplicati=%-6d  %v",
			tp.Partition, int64(tp.Offset), produced.get(tp.Partition),
			countOutputFor(output, tp.Partition), len(holes), dup, sample)
	}

	t.Logf("TOTALE: prodotti=%d  offset committati=%d  record in output=%d  buchi=%d  duplicati=%d",
		produced.total(), totalCommitted, len(output), totalHoles, totalDup)

	if len(output) == 0 || totalCommitted == 0 {
		t.Fatalf("scenario non eseguito: %d record in output, %d offset committati — il confronto non dimostra nulla",
			len(output), totalCommitted)
	}
	if totalHoles > 0 {
		t.Errorf("PERDITA EOS: %d input sono stati committati senza che il loro output esista", totalHoles)
	}
	if totalDup > 0 {
		// Non fa fallire: l'at-least-once regge comunque. Ma con l'EOS non dovrebbe succedere, e
		// saperlo è metà del valore del test.
		t.Logf("ATTENZIONE: %d output duplicati — l'exactly-once non ha tenuto (l'at-least-once sì)", totalDup)
	}
}

func countOutputFor(out map[offsetKey]int, part int32) int {
	n := 0
	for k := range out {
		if k.partition == part {
			n++
		}
	}
	return n
}
