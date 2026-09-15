package confluentdriver

import (
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// msg costruisce un messaggio del client su una topic-partition.
func msg(topic string, partition int32, offset int64) *kafka.Message {
	return &kafka.Message{TopicPartition: tp(topic, partition, offset)}
}

// newTestSession costruisce una groupSession senza client: decide non tocca g.c, ed è il motivo per
// cui la decisione è separata dall'I/O — i rami che contano non sono provocabili a comando contro un
// client vero.
func newTestSession(partial bool) *groupSession {
	offsets := newOffsetTracker()
	return &groupSession{
		name:    "test",
		offsets: offsets,
		rb:      &rebalanceObserver{name: "test", offsets: offsets},
		partial: partial,
	}
}

func part(topic string, partition int32) driver.TopicPartition {
	return driver.TopicPartition{Topic: topic, Partition: partition}
}

func TestDecide_MessaggioInsiemeAllaRevocaVieneConsegnato(t *testing.T) {
	// Quel record è GIÀ uscito dalla coda del client e la posizione di fetch non torna indietro: se
	// lo si butta e la sua partizione è fra quelle RITENUTE, nessuno lo rileggerà mai. Si consegna, e
	// il reset si rimanda al poll successivo — dove sarà il filtro sul batch a togliere i record
	// delle sole partizioni perse.
	g := newTestSession(true)
	m := msg("t", 2, 42) // partizione 2: RITENUTA

	rec, err := g.decide(m, []driver.TopicPartition{part("t", 0)}, false)

	if err != nil {
		t.Fatalf("decide = %v, atteso nil: il record va consegnato, non scartato", err)
	}
	if rec != m {
		t.Fatal("il messaggio consegnato insieme alla revoca è stato buttato: è la perdita di un record")
	}
	// Il reset non è perso: lo raccoglie il giro dopo.
	if got, _ := g.rb.takeRevoked(); len(got) != 1 || got[0] != part("t", 0) {
		t.Fatalf("revoca non rimandata al poll successivo: %v", got)
	}
}

func TestDecide_RevocaPortaLePartizioniPerse(t *testing.T) {
	g := newTestSession(true)
	revoked := []driver.TopicPartition{part("t", 0), part("t", 1)}

	_, err := g.decide(nil, revoked, false)

	if driver.SeverityOf(err) != driver.SeverityReset {
		t.Fatalf("severità = %v, attesa reset", driver.SeverityOf(err))
	}
	got, ok := driver.RevokedOf(err)
	if !ok {
		t.Fatal("il reset non porta le partizioni revocate: l'engine butterebbe tutto il batch, anche i record delle partizioni ancora sue")
	}
	if len(got) != 2 {
		t.Fatalf("partizioni revocate = %v, attese 2", got)
	}
}

func TestDecide_InEOSIlResetRestaTotale(t *testing.T) {
	// In transform il batch è l'unità della transazione: una revoca la invalida per intero, quindi
	// l'engine deve abortire e scartare tutto, non filtrare.
	g := newTestSession(false)

	_, err := g.decide(nil, []driver.TopicPartition{part("t", 0)}, false)

	if driver.SeverityOf(err) != driver.SeverityReset {
		t.Fatalf("severità = %v, attesa reset", driver.SeverityOf(err))
	}
	if _, ok := driver.RevokedOf(err); ok {
		t.Error("in EOS il reset non deve portare le partizioni: il batch va scartato per intero")
	}
}

func TestDecide_EventiCheNonRiguardanoLEngine(t *testing.T) {
	// ReadMessage li ingoiava al posto nostro; con Poll arrivano qui e vanno riconosciuti come
	// "nessun record", non come errori — un errore qui farebbe ricostruire il client per un
	// non-evento.
	cases := []struct {
		name string
		ev   kafka.Event
	}{
		{"timeout (nessun evento)", nil},
		{"offset committati", kafka.OffsetsCommitted{}},
		{"fine partizione", kafka.PartitionEOF(tp("t", 0, 10))},
		{"statistiche", &kafka.Stats{}},
		{"errore di timeout come evento", kafka.NewError(kafka.ErrTimedOut, "timed out", false)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newTestSession(true)
			rec, err := g.decide(c.ev, nil, false)
			if rec != nil || err != nil {
				t.Errorf("decide(%T) = (%v, %v), atteso (nil, nil)", c.ev, rec, err)
			}
		})
	}
}

func TestDecide_MessaggioConsegnato(t *testing.T) {
	g := newTestSession(true)
	m := msg("t", 1, 7)

	rec, err := g.decide(m, nil, false)
	if err != nil {
		t.Fatalf("decide = %v, atteso nil", err)
	}
	if rec != m {
		t.Fatal("decide non ha consegnato il messaggio")
	}
	// Il tracking NON sta in decide: lo fa Poll, dopo. Così decide resta verificabile senza client.
	if !g.offsets.empty() {
		t.Error("decide ha tracciato l'offset: è compito di Poll")
	}
}

func TestDecide_ErrorePerPartizioneSulMessaggio(t *testing.T) {
	// Un messaggio con TopicPartition.Error è un errore del consumo, non un record: ReadMessage lo
	// ritornava come errore e la parità va mantenuta.
	g := newTestSession(true)
	m := msg("t", 0, 3)
	m.TopicPartition.Error = kafka.NewError(kafka.ErrUnknownTopicOrPart, "unknown", false)

	rec, err := g.decide(m, nil, false)
	if rec != nil {
		t.Error("consegnato un messaggio che portava un errore di partizione")
	}
	if driver.SeverityOf(err) != driver.SeverityPermanent {
		t.Fatalf("severità = %v, attesa permanent (topic inesistente: la config è sbagliata)", driver.SeverityOf(err))
	}
}

func TestDecide_ErroreDelClientRisale(t *testing.T) {
	g := newTestSession(true)
	rec, err := g.decide(kafka.NewError(kafka.ErrAllBrokersDown, "down", false), nil, false)
	if rec != nil {
		t.Error("consegnato un record su evento di errore")
	}
	if driver.SeverityOf(err) != driver.SeverityRetriable {
		t.Fatalf("severità = %v, attesa retriable", driver.SeverityOf(err))
	}
}

func TestRebalanceObserver_ScartaSoloGliOffsetDellePartizioniRevocate(t *testing.T) {
	// È la metà driver del reset parziale: gli offset delle partizioni RITENUTE devono sopravvivere
	// alla revoca, perché i loro record restano nel batch dell'engine e verranno committati.
	offsets := newOffsetTracker()
	offsets.track(tp("t", 0, 10))
	offsets.track(tp("t", 1, 20))
	o := &rebalanceObserver{name: "test", offsets: offsets}

	topic := "t"
	o.callback(nil, kafka.RevokedPartitions{Partitions: []kafka.TopicPartition{
		{Topic: &topic, Partition: 0},
	}})

	if _, ok := find(offsets.commitOffsets(), "t", 0); ok {
		t.Error("l'offset della partizione revocata è ancora tracciato: verrebbe committato per conto del nuovo owner")
	}
	off, ok := find(offsets.commitOffsets(), "t", 1)
	if !ok {
		t.Fatal("scartato anche l'offset di una partizione RITENUTA: i suoi record non verrebbero mai committati")
	}
	if off != 21 {
		t.Errorf("offset da committare = %d, atteso 21 (20+1)", off)
	}
	if got, _ := o.takeRevoked(); len(got) != 1 || got[0] != part("t", 0) {
		t.Errorf("partizioni revocate = %v, attesa [t/0]", got)
	}
}

func TestDecide_MessaggioDiUnaPartizioneRevocataNonVaConsegnato(t *testing.T) {
	// Il rovescio del test precedente: se la partizione del messaggio è fra quelle PERSE, consegnarlo
	// sarebbe un errore. Tracciandone l'offset lo si rimetterebbe nel tracker — da cui il callback
	// l'ha appena tolto — e un taglio del batch prima del poll successivo committerebbe un offset di
	// una partizione che non è più nostra. Buttarlo è corretto: lo rilegge il nuovo owner.
	g := newTestSession(true)
	m := msg("t", 0, 42) // partizione 0: REVOCATA

	rec, err := g.decide(m, []driver.TopicPartition{part("t", 0)}, false)

	if rec != nil {
		t.Error("consegnato un record di una partizione revocata: il suo offset tornerebbe nel tracker e potrebbe essere committato")
	}
	if driver.SeverityOf(err) != driver.SeverityReset {
		t.Fatalf("severità = %v, attesa reset", driver.SeverityOf(err))
	}
	if got, _ := g.rb.takeRevoked(); len(got) > 0 {
		t.Error("revoca rimandata al poll successivo: non serve, il reset è già stato consegnato ora")
	}
}

func TestDecide_AssegnazionePersaRicostruisceLaSessione(t *testing.T) {
	// Perdere l'assegnazione non è cederla: le partizioni possono già essere di un altro membro e non
	// si può distinguere ciò che è ancora nostro. Un reset assorbibile lascerebbe in piedi una
	// sessione di cui non ci si può fidare; serve una severità che faccia ricostruire il client.
	g := newTestSession(true)

	rec, err := g.decide(msg("t", 0, 7), []driver.TopicPartition{part("t", 0)}, true)

	if rec != nil {
		t.Error("consegnato un record con l'assegnazione persa")
	}
	if driver.SeverityOf(err) != driver.SeverityFatal {
		t.Fatalf("severità = %v, attesa fatal (ricostruzione della sessione)", driver.SeverityOf(err))
	}
}
