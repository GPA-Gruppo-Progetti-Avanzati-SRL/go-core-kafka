//go:build mockcluster

// Test di integrazione in-process che risponde a UNA domanda: un rebalance può far sparire record
// che nessuno ha processato?
//
// Non usa fake dell'engine: gira il runner vero (stesso loop, stessa absorb, stesso commit) con i
// driver veri contro un broker vero. La matrice è driver x protocollo di assegnazione, perché la
// domanda si pone identica per confluent e per franz.
//
// I REBALANCE NON SONO SIMULATI: sono provocati. Tre consumer dello stesso gruppo entrano a
// scaglioni (A a t0, B a t+8s, C a t+16s) mentre si produce senza sosta; ogni join costringe il
// coordinator a un rebalance vero, e i log "partizioni revocate/assegnate" che compaiono sono quelli
// del client, non del test. Il test non tocca né il protocollo né il driver: mette solo tre membri
// nello stesso gruppo e guarda cosa resta.
//
// L'INVARIANTE verificata è quella dell'at-least-once, e non "ho visto tutti i record": un record non
// ancora committato non è perso, verrà riletto. Quindi si confrontano gli offset COMMITTATI dal
// gruppo con quelli che l'Handler ha davvero ricevuto:
//
//	per ogni partizione, ogni offset < committed DEVE comparire almeno una volta nell'Handler.
//
// Un offset sotto il commit che l'Handler non ha mai visto è un record dichiarato elaborato da
// nessuno: un buco. I duplicati sono ammessi (sono il prezzo dell'at-least-once) e vengono riportati
// ma non fanno fallire il test.
//
// PERCHÉ LA CONFIGURAZIONE È QUESTA: il buco, se c'è, nasce quando la revoca cade mentre il batch si
// sta ACCUMULANDO (i record sono già usciti dal client ma l'Handler non li ha ancora visti). La
// larghezza di quella finestra è `cut-frequency`, quindi il test la mette a 3s — un valore di
// esercizio plausibile, non un artificio: alzarla o abbassarla cambia la PROBABILITÀ per rebalance,
// non l'esistenza della finestra. Il drain finale serve a far arrivare il watermark committato in
// fondo al topic: senza, la verifica esaminerebbe solo la parte di corsa sotto il commit.
//
// BROKER: di default kfake (franz-go, in-process, parla il protocollo su una porta TCP quindi serve
// entrambi i driver). LOSS_BROKER=mock usa il mock cluster di librdkafka — che però franz-go non sa
// interrogare ("unable to request api versions"). KAFKA_BOOTSTRAP=host:porta punta a un broker vero
// (Redpanda/Kafka) e ha la precedenza su tutto.
//
// ESITO AL 2026-09-15, codice senza rewind nello scarto, su BROKER VERO (Redpanda 24.2.7,
// KAFKA_BOOTSTRAP=127.0.0.1:9092). Tutte e quattro le combinazioni perdono record:
//
//	confluent/cooperative-sticky  320 buchi — range CONTIGUI sulle sole partizioni RITENUTE (p2,p3);
//	                              zero buchi sulle revocate (p0,p1), che infatti vengono rilette.
//	franz/cooperative-sticky       52 buchi — stessa firma: contigui sulle ritenute.
//	confluent/range                 3 buchi — record SINGOLI e isolati ([775] [336] [832]): uno per
//	                              revoca, il primo record della nuova assegnazione, scartato da
//	                              groupSession.Poll dopo takeRevoked.
//	franz/range                     7 buchi — piccoli gruppi, in parte MAI usciti da Poll: sono i
//	                              record fetchati in session.buf e buttati da dropAndRelease.
//
// Quindi: il protocollo eager RIDUCE la perdita di due ordini di grandezza ma NON la elimina, e il
// driver franz non è immune — BlockRebalanceOnPoll chiude la finestra grande (la revoca non cade
// durante l'accumulo) ma restano il buffer buttato e il record consegnato con la revoca in volo.
//
// DOPO il reset PARZIALE sulle partizioni revocate (entrambi i driver) + barriera e riavvolgimento
// su franz, stesso broker — TUTTE E QUATTRO a zero buchi:
//
//	confluent/cooperative-sticky  0 buchi (era 320), 0 duplicati
//	confluent/range               0 buchi (era 3),   0 duplicati
//	franz/cooperative-sticky      0 buchi (era 59),  0 duplicati
//	franz/range                   0 buchi (era 7),   1059 duplicati
//
// Il log del caso cooperativo mostra cosa fa il parziale: "records=237, kept=237", cioè dello stesso
// batch 237 record delle partizioni cedute tornano al nuovo owner (che li processa: zero buchi E zero
// duplicati) e 237 delle partizioni ritenute restano nel batch e proseguono.
//
// I 1059 duplicati di franz/range sono il prezzo dichiarato, e la loro sequenza nei log è la
// dimostrazione del meccanismo: "commit trattenuto" (la barriera impedisce di confermare oltre il
// buco) -> "partizioni riavvolte" (SetOffsets riporta il consumo all'ultimo commit) -> i record
// scartati vengono riletti ed elaborati. Servono entrambe le metà, e servono solo a franz: dopo un
// revoke+reassign eager franz riprende dalla propria posizione interna, librdkafka rilegge da sé.
//
// NOTA SUI FAKE: il mock cluster di librdkafka dava 0 buchi in eager — un FALSO NEGATIVO rispetto al
// broker vero. kfake riproduce invece la stessa firma del broker vero (3 / 51 / 4 buchi). Un fake può
// nascondere il problema: la corsa che conta è quella con KAFKA_BOOTSTRAP.
//
// go test -tags mockcluster -run TestRebalance -v ./consumer/
package consumer

import (
	"context"
	"fmt"
	"os"
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
	"github.com/twmb/franz-go/pkg/kfake"
)

const (
	lossPartitions = 4
	lossProcessor  = "lossproc"

	// Finestra di accumulo del batch: è la larghezza della finestra in cui una revoca può cogliere
	// record già usciti dal client e non ancora visti dall'Handler.
	lossCutFrequency = 3 * time.Second

	produceRate  = 4 * time.Millisecond // ~250 rec/s: i consumer stanno dietro e il topic si drena
	handlerDelay = 30 * time.Millisecond
	joinSecond   = 8 * time.Second  // quando entra il consumer B
	joinThird    = 16 * time.Second // quando entra il consumer C
	produceFor   = 24 * time.Second
	drainQuiet   = 5 * time.Second // quiete richiesta per dichiarare drenato
	drainMax     = 90 * time.Second

	// Timeout di gruppo accorciati: il rebalance timeout del coordinator è max.poll.interval.ms, e
	// col default della libreria (5 minuti) un join che si incastra tiene il gruppo fermo per tutta
	// la durata del test invece di risolversi.
	lossMaxPollIntervalMs = 12000
	lossSessionTimeoutMs  = 6000
	lossHeartbeatMs       = 1000
)

type offsetKey struct {
	partition int32
	offset    int64
}

// recorder è l'Handler del test: registra ogni record che l'engine gli consegna. È il ground truth
// di "questo record è stato processato almeno una volta". È condiviso dai consumer del gruppo,
// perché la domanda riguarda il gruppo, non il singolo membro.
type recorder struct {
	mu   sync.Mutex
	seen map[offsetKey]int
}

func newRecorder() *recorder { return &recorder{seen: make(map[offsetKey]int)} }

func (r *recorder) Handle(_ context.Context, batch []*message.Record) error {
	time.Sleep(handlerDelay)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range batch {
		r.seen[offsetKey{rec.Partition, rec.Offset}]++
	}
	return nil
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func (r *recorder) snapshot() map[offsetKey]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[offsetKey]int, len(r.seen))
	for k, v := range r.seen {
		out[k] = v
	}
	return out
}

func TestRebalanceRecordLoss(t *testing.T) {
	// Info: servono i log dell'engine — "partizioni revocate/assegnate" e "batch scartato senza
	// commit" sono la prova che lo scenario si è davvero verificato.
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	drivers := map[string]func() driver.Factory{
		"confluent": confluentdriver.New,
		"franz":     franzdriver.New,
	}
	// I due protocolli si comportano diversamente proprio sul punto in esame: la revoca eager toglie
	// TUTTE le partizioni (al riassegno le posizioni ripartono dall'ultimo commit), quella
	// cooperativa ne toglie un sottoinsieme e lascia le altre a consumare senza interruzione.
	for _, drv := range []string{"confluent", "franz"} {
		for _, strategy := range []string{"cooperative-sticky", "range"} {
			t.Run(drv+"/"+strategy, func(t *testing.T) {
				runLossScenario(t, drivers[drv], drv, strategy)
			})
		}
	}
}

func runLossScenario(t *testing.T, newFactory func() driver.Factory, drv, strategy string) {
	topic := fmt.Sprintf("loss-%s-%s-%d", drv, strategy, time.Now().UnixNano())
	boot := startBroker(t, topic)
	groupID := "g-" + topic

	produced, stopProducing := startProducer(t, boot, topic)
	rec := newRecorder()
	del := newDeliveredSet()

	type member struct {
		cancel context.CancelFunc
		done   <-chan error
	}
	var members []member
	start := func(label string) {
		ctx, cancel := context.WithCancel(context.Background())
		members = append(members, member{cancel: cancel, done: startRunner(t, label, boot, topic, groupID, strategy, newFactory, rec, del, ctx)})
		t.Logf("[%s] consumer %s avviato", time.Now().Format("15:04:05.000"), label)
	}

	// Ogni start aggiunge un membro al gruppo: è questo che provoca il rebalance, non una API di test.
	start("A")
	time.Sleep(joinSecond)
	start("B")
	time.Sleep(joinThird - joinSecond)
	start("C")

	time.Sleep(produceFor - joinThird)
	stopProducing()
	t.Logf("produzione terminata: %d record", produced.total())

	drain(t, rec)
	for _, m := range members {
		m.cancel()
	}
	for _, m := range members {
		if err := <-m.done; err != nil {
			t.Logf("runner terminato con errore: %v", err)
		}
	}

	delivered, discards := del.snapshot()
	t.Logf("Discard() chiamate dall'engine: %d — record consegnati da Poll: %d, processati dall'Handler: %d",
		discards, len(delivered), rec.len())
	report(t, boot, topic, groupID, produced, rec.snapshot(), delivered)
}

// drain attende che i consumer smettano di fare progressi: serve a portare il watermark committato
// in fondo al topic, così la verifica copre tutta la corsa e non solo la parte già committata.
func drain(t *testing.T, rec *recorder) {
	t.Helper()
	deadline := time.Now().Add(drainMax)
	last, stableSince := rec.len(), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		if n := rec.len(); n != last {
			last, stableSince = n, time.Now()
			continue
		}
		if time.Since(stableSince) >= drainQuiet {
			t.Logf("drenato: %d record processati, nessun progresso da %s", last, drainQuiet)
			return
		}
	}
	t.Logf("drain interrotto dal timeout: %d record processati", last)
}

// --- broker -------------------------------------------------------------------------------------

// startBroker avvia il broker su cui gira lo scenario e ritorna i bootstrap servers. kfake è il
// default perché è in-process ed è l'unico dei due fake che entrambi i client sanno interrogare.
func startBroker(t *testing.T, topic string) string {
	t.Helper()

	if ext := os.Getenv("KAFKA_BOOTSTRAP"); ext != "" {
		createTopic(t, ext, topic)
		t.Logf("broker esterno: %s", ext)
		return ext
	}

	if os.Getenv("LOSS_BROKER") == "mock" {
		mc, err := kafka.NewMockCluster(1)
		if err != nil {
			t.Fatalf("mock cluster: %v", err)
		}
		t.Cleanup(mc.Close)
		if err := mc.CreateTopic(topic, lossPartitions, 1); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		t.Logf("broker: mock cluster librdkafka su %s", mc.BootstrapServers())
		return mc.BootstrapServers()
	}

	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(lossPartitions, topic),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	addrs := c.ListenAddrs()
	t.Logf("broker: kfake su %v", addrs)
	return addrs[0]
}

func createTopic(t *testing.T, boot, topic string) {
	t.Helper()
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": boot})
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := a.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic: topic, NumPartitions: lossPartitions, ReplicationFactor: 1,
	}})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for _, r := range res {
		if r.Error.Code() != kafka.ErrNoError && r.Error.Code() != kafka.ErrTopicAlreadyExists {
			t.Fatalf("create topic %s: %v", r.Topic, r.Error)
		}
	}
}

// --- strumentazione al confine del driver --------------------------------------------------------
//
// Il set dei record CONSEGNATI da Poll all'engine è la seconda metà della misura: un record uscito dal
// client e mai arrivato all'Handler è un record scartato dall'engine. Se poi il suo offset finisce
// sotto il commit, è un buco.

type deliveredSet struct {
	mu       sync.Mutex
	rec      map[offsetKey]int
	discards int
}

func newDeliveredSet() *deliveredSet { return &deliveredSet{rec: make(map[offsetKey]int)} }

func (d *deliveredSet) add(k offsetKey) {
	d.mu.Lock()
	d.rec[k]++
	d.mu.Unlock()
}

func (d *deliveredSet) discard() {
	d.mu.Lock()
	d.discards++
	d.mu.Unlock()
}

func (d *deliveredSet) snapshot() (map[offsetKey]int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[offsetKey]int, len(d.rec))
	for k, v := range d.rec {
		out[k] = v
	}
	return out, d.discards
}

type countingFactory struct {
	driver.Factory
	d *deliveredSet
}

func (f countingFactory) NewGroupConsumer(s spec.ProcessorSpec, k spec.KafkaServer) (driver.GroupConsumer, error) {
	gc, err := f.Factory.NewGroupConsumer(s, k)
	if err != nil {
		return nil, err
	}
	return &countingConsumer{GroupConsumer: gc, d: f.d}, nil
}

type countingConsumer struct {
	driver.GroupConsumer
	d *deliveredSet
}

func (c *countingConsumer) Poll(ctx context.Context, timeout time.Duration) (*message.Record, error) {
	r, err := c.GroupConsumer.Poll(ctx, timeout)
	if r != nil {
		c.d.add(offsetKey{r.Partition, r.Offset})
	}
	return r, err
}

func (c *countingConsumer) Discard(ctx context.Context) {
	c.d.discard()
	c.GroupConsumer.Discard(ctx)
}

// --- produzione ---------------------------------------------------------------------------------

type producedCounts struct {
	mu sync.Mutex
	n  map[int32]int64 // partizione -> record prodotti
}

func (p *producedCounts) add(part int32) {
	p.mu.Lock()
	p.n[part]++
	p.mu.Unlock()
}

func (p *producedCounts) get(part int32) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n[part]
}

func (p *producedCounts) total() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var tot int64
	for _, v := range p.n {
		tot += v
	}
	return tot
}

func startProducer(t *testing.T, boot, topic string) (*producedCounts, func()) {
	t.Helper()
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": boot,
		"linger.ms":         5,
		"acks":              "all",
	})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}

	counts := &producedCounts{n: make(map[int32]int64)}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Delivery report: conta i record effettivamente scritti, per partizione.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error == nil {
				counts.add(m.TopicPartition.Partition)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("k%d", i%64)),
				Value:          []byte(fmt.Sprintf("v%d", i)),
			}
			if err := p.Produce(msg, nil); err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			time.Sleep(produceRate)
		}
	}()

	return counts, func() {
		close(stop)
		p.Flush(10000)
		p.Close() // chiude anche il canale Events()
		wg.Wait()
	}
}

// --- consumo ------------------------------------------------------------------------------------

func startRunner(t *testing.T, label, boot, topic, groupID, strategy string, newFactory func() driver.Factory, h processor.Handler, del *deliveredSet, ctx context.Context) <-chan error {
	t.Helper()
	server := spec.KafkaServer{BootstrapServers: boot}.WithDefaults()
	raw := spec.ProcessorSpec{
		Name:    lossProcessor,
		Topics:  []string{topic},
		GroupID: groupID,
		Consumer: spec.ConsumerTuning{
			AutoOffsetReset:             "earliest",
			PartitionAssignmentStrategy: strategy,
			CutFrequency:                lossCutFrequency,
			MaxPollIntervalMs:           lossMaxPollIntervalMs,
			SessionTimeoutMs:            lossSessionTimeoutMs,
			HeartbeatIntervalMs:         lossHeartbeatMs,
		},
	}
	f := countingFactory{Factory: newFactory(), d: del}
	r, err := newRunner(raw, server, seams{handlers: map[string]processor.Handler{lossProcessor: h}}, f, nil)
	if err != nil {
		t.Fatalf("newRunner(%s): %v", label, err)
	}

	done := make(chan error, 1)
	go func() {
		log.Info().Str("member", label).Msg("test: avvio runner")
		done <- r.run(ctx)
	}()
	return done
}

// --- verifica -----------------------------------------------------------------------------------

func report(t *testing.T, boot, topic, groupID string, produced *producedCounts, seen, delivered map[offsetKey]int) {
	t.Helper()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": boot,
		"group.id":          groupID,
	})
	if err != nil {
		t.Fatalf("consumer di controllo: %v", err)
	}
	defer c.Close()

	parts := make([]kafka.TopicPartition, 0, lossPartitions)
	for i := 0; i < lossPartitions; i++ {
		parts = append(parts, kafka.TopicPartition{Topic: &topic, Partition: int32(i)})
	}
	committed, err := c.Committed(parts, 15000)
	if err != nil {
		t.Fatalf("lettura offset committati: %v", err)
	}
	sort.Slice(committed, func(i, j int) bool { return committed[i].Partition < committed[j].Partition })

	var totalHoles, totalDup, totalCommitted, totalDropped int64
	for _, tp := range committed {
		if tp.Offset < 0 { // OffsetInvalid: il gruppo non ha mai committato questa partizione
			t.Logf("p%-2d  committed=-        prodotti=%d", tp.Partition, produced.get(tp.Partition))
			continue
		}
		var holes []int64
		for off := int64(0); off < int64(tp.Offset); off++ {
			if _, ok := seen[offsetKey{tp.Partition, off}]; !ok {
				holes = append(holes, off)
			}
		}
		var dup, processed int
		for k, n := range seen {
			if k.partition != tp.Partition {
				continue
			}
			processed++
			dup += n - 1
		}
		// Consegnati da Poll all'engine e mai arrivati all'Handler: sono i record scartati con il
		// batch. Sotto il commit sono buchi, sopra sono semplicemente da rileggere.
		var dropped int
		for k := range delivered {
			if k.partition != tp.Partition {
				continue
			}
			if _, ok := seen[k]; !ok {
				dropped++
			}
		}
		totalDropped += int64(dropped)
		totalHoles += int64(len(holes))
		totalDup += int64(dup)
		totalCommitted += int64(tp.Offset)

		sample := holes
		if len(sample) > 12 {
			sample = sample[:12]
		}
		t.Logf("p%-2d  committed=%-7d prodotti=%-7d processati=%-7d scartati=%-6d buchi=%-6d duplicati=%-6d  %v",
			tp.Partition, int64(tp.Offset), produced.get(tp.Partition), processed, dropped, len(holes), dup, sample)
	}

	t.Logf("TOTALE: prodotti=%d  offset committati=%d  scartati=%d  buchi=%d  duplicati=%d",
		produced.total(), totalCommitted, totalDropped, totalHoles, totalDup)

	// Guardia: senza consumo e senza commit il confronto è vuoto e passerebbe per finta. È successo
	// davvero — franz-go contro il mock di librdkafka non riesce nemmeno a negoziare le API version,
	// e il test "passava" con zero record.
	if len(seen) == 0 || totalCommitted == 0 {
		t.Fatalf("scenario non eseguito: %d record processati, %d offset committati — il confronto non dimostra nulla",
			len(seen), totalCommitted)
	}
	if totalHoles > 0 {
		t.Errorf("PERDITA: %d offset sono stati committati senza che l'Handler li abbia mai ricevuti", totalHoles)
	}
}
