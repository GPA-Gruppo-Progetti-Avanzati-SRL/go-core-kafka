package confluentdriver

import (
	"context"
	"errors"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

// groupSession è la parte comune ai due client del driver: il consumer sottoscritto, il tracker degli
// offset e l'observer del rebalance. Poll e Discard vivono qui perché la disciplina sugli offset è
// identica nelle due modalità — cambia solo COME li si conferma (commit diretto in handle,
// SendOffsetsToTransaction in EOS), e quello sta nei due tipi che la embeddano.
type groupSession struct {
	name    string
	c       *kafka.Consumer
	offsets *offsetTracker
	rb      *rebalanceObserver

	// partial abilita il reset PARZIALE: alla revoca l'engine scarta dal batch i soli record delle
	// partizioni perse, invece di buttarlo tutto. Vale in modalità handle; in EOS no, perché il
	// batch è l'unità della transazione e una revoca la invalida per intero.
	partial bool
}

// Poll ritorna il prossimo messaggio, (nil, nil) quando non c'è nulla da consegnare, o un errore
// SeverityReset se è avvenuto un rebalance (vedi rebalance.go).
//
// PERCHÉ Poll E NON ReadMessage. ReadMessage ha un ciclo interno: quando l'evento estratto non è un
// messaggio — ed è il caso dell'evento di rebalance, che il client "consuma" invocando il nostro
// callback e non restituisce al chiamante — ricicla col timeout residuo e può restituire un messaggio
// nella STESSA chiamata in cui la revoca è avvenuta. Quel messaggio è già uscito dalla coda del
// client: scartarlo (l'unica cosa che si può fare, visto che appartiene alla nuova assegnazione) lo
// perde, perché la posizione di fetch non torna indietro e il commit successivo ci passa sopra.
// Misurato su broker vero: un record per revoca, offset isolati.
//
// Consumer.Poll estrae invece UN SOLO evento per chiamata — eventPoll forza maxEvents=1 quando non
// c'è il canale degli eventi — quindi i due fatti non possono più capitare insieme: se l'evento era
// la revoca, qui ev è nil e il messaggio che la segue resta in coda, dove lo prenderà la Poll
// successiva dopo che l'engine ha scartato il batch. Il problema è tolto per costruzione, non
// arginato: non c'è niente da bufferizzare perché non c'è niente che è stato letto di troppo.
//
// Il prezzo è che gli eventi che ReadMessage ingoiava al posto nostro (offset committati, EOF di
// partizione, statistiche) arrivano qui e vanno riconosciuti: sono tutti "nessun record da
// consegnare", ma vanno distinti da un errore.
//
// Il context non è osservato QUI di proposito: Poll è una chiamata CGo bloccante che non lo accetta,
// e il suo bound è `timeout` (consumer.poll-timeout, 100ms di default). È il loop chiamante a
// osservare la cancellazione, fra un poll e il successivo. Il parametro resta nella firma perché
// appartiene al seam driver.Session, e un driver puramente Go (franz-go) può onorarlo.
func (g *groupSession) Poll(_ context.Context, timeout time.Duration) (*message.Record, error) {
	ms := int(timeout.Milliseconds())
	if ms <= 0 {
		// Un timeout sotto il millisecondo arrotonderebbe a 0, che per librdkafka significa poll non
		// bloccante: il loop dell'engine girerebbe a vuoto bruciando CPU.
		ms = 1
	}

	// L'ORDINE è vincolante: il callback del rebalance gira DENTRO la Poll, quindi la revoca si
	// raccoglie DOPO. Raccoglierla prima la sposterebbe di un giro, e il record consegnato in questa
	// stessa chiamata verrebbe tracciato senza sapere che una revoca è avvenuta.
	ev := g.c.Poll(ms)
	revoked, lost := g.rb.takeRevoked()
	msg, err := g.decide(ev, revoked, lost)
	if err != nil || msg == nil {
		return nil, err
	}
	g.offsets.track(msg.TopicPartition)
	return toRecord(msg), nil
}

// decide è la parte DECISIONALE del poll, separata dall'I/O: i rami che contano — revoca con e senza
// messaggio — non sono provocabili a comando contro un client vero, e senza questa separazione
// resterebbero gli unici non coperti da test proprio dove si perdono i record.
//
// Ritorna (msg, nil) se c'è un record da consegnare, (nil, err) per un errore o per il reset da
// rebalance, (nil, nil) quando non c'è nulla — timeout o evento che non riguarda l'engine.
func (g *groupSession) decide(ev kafka.Event, revoked []driver.TopicPartition, lost bool) (*kafka.Message, error) {
	msg, _ := ev.(*kafka.Message)

	if lost {
		// Assegnazione PERSA: le partizioni possono già essere di un altro membro, quindi non si può
		// distinguere ciò che è ancora nostro da ciò che non lo è più — nemmeno il messaggio appena
		// consegnato. Si butta tutto e si RICOSTRUISCE la sessione: un reset assorbibile lascerebbe
		// in piedi una sessione di cui non ci si può fidare. Il client nuovo riparte dagli ultimi
		// offset committati: duplicati ammessi, nessun buco.
		return nil, driver.NewError(driver.SeverityFatal, "poll", errAssignmentLost)
	}

	if len(revoked) > 0 {
		// Un messaggio consegnato insieme alla revoca si consegna SOLO se la sua partizione è ancora
		// nostra: è già uscito dalla coda del client, la posizione di fetch non torna indietro, e
		// buttarlo lo perderebbe perché nessuno rileggerà quella partizione. Il reset si segnala al
		// giro dopo.
		//
		// Se invece la partizione è fra le revocate il record si BUTTA, e consegnarlo sarebbe un
		// errore: tracciandone l'offset lo si rimetterebbe nel tracker — da cui il callback l'ha
		// appena tolto — e un taglio del batch prima del poll successivo committerebbe un offset di
		// una partizione che non è più nostra, cioè la perdita che il callback esiste per prevenire.
		// Buttarlo è corretto: lo rilegge il nuovo owner dall'ultimo commit.
		if msg != nil && !isRevoked(msg.TopicPartition, revoked) {
			g.rb.putBack(revoked)
			return g.deliver(msg)
		}
		return nil, g.resetError(revoked)
	}

	switch e := ev.(type) {
	case *kafka.Message:
		return g.deliver(e)

	case kafka.Error:
		if e.Code() == kafka.ErrTimedOut {
			// Non dovrebbe arrivare come evento (il timeout di Poll è ev == nil), ma classificarlo
			// come retriable farebbe ricostruire il client per un non-evento.
			return nil, nil
		}
		return nil, wrap("poll", e)
	}

	// Ogni altro evento (OffsetsCommitted, PartitionEOF, *Stats, OAuthBearerTokenRefresh) e il timeout
	// (ev == nil): nessun record da consegnare, il loop dell'engine riprova.
	return nil, nil
}

// isRevoked dice se la topic-partition del messaggio è fra quelle appena perse.
func isRevoked(tp kafka.TopicPartition, revoked []driver.TopicPartition) bool {
	if tp.Topic == nil {
		return false
	}
	for _, p := range revoked {
		if p.Topic == *tp.Topic && p.Partition == tp.Partition {
			return true
		}
	}
	return false
}

// deliver valida il messaggio prima di consegnarlo: un errore per-partizione arriva attaccato al
// messaggio ed è un errore del consumo, non un record da elaborare (parità con ReadMessage).
func (g *groupSession) deliver(m *kafka.Message) (*kafka.Message, error) {
	if m.TopicPartition.Error != nil {
		return nil, wrap("poll", m.TopicPartition.Error)
	}
	return m, nil
}

// resetError costruisce il reset da revoca. Porta con sé le partizioni perse SOLO in modalità handle
// (vedi groupSession.partial): in EOS il batch è l'unità della transazione, quindi una revoca lo
// invalida per intero e l'engine deve abortire, non filtrare.
func (g *groupSession) resetError(revoked []driver.TopicPartition) error {
	if !g.partial {
		return driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	}
	return driver.NewRevokeError("poll", errRebalanced, revoked)
}

// Discard scarta gli offset tracciati e non committati. L'engine la chiama quando butta il batch in
// volo: senza, il Commit successivo confermerebbe record che nessuno ha elaborato (vedi il contratto
// di driver.Session.Discard). La sessione transazionale la estende con l'abort della transazione.
func (g *groupSession) Discard(context.Context) {
	g.rewind()
	g.offsets.reset()
}

// rewind riporta la posizione di consumo al primo offset non committato delle partizioni del batch
// che si sta buttando. Senza, scartare un batch NON significa rileggerlo: la posizione di fetch resta
// avanti e il commit successivo passa sopra quei record — che nessuno ha elaborato.
//
// Per una revoca non serve (le partizioni perse le rilegge il nuovo owner, e quelle ritenute non
// vengono più buttate: c'è il reset parziale). Serve per gli scarti TOTALI, dove non c'è nessun
// rebalance a riposizionare il consumo: l'abort di una transazione EOS è il caso principale — la
// transazione annullata non ha prodotto nulla, quindi quei record vanno rielaborati, non saltati.
//
// L'errore per-partizione è ignorato di proposito: una partizione non più assegnata non si può
// riavvolgere, ed è giusto così — la rilegge chi la possiede ora.
func (g *groupSession) rewind() {
	parts := g.offsets.rewindOffsets()
	if len(parts) == 0 {
		return
	}
	res, err := g.c.SeekPartitions(parts)
	if err != nil {
		log.Warn().Err(err).Str("consumer", g.name).Int("partitions", len(parts)).
			Msg("corekafka: riavvolgimento fallito: i record del batch scartato potrebbero non essere riletti")
		return
	}
	for _, p := range res {
		if p.Error != nil && !isNotAssigned(p.Error) {
			log.Warn().Err(p.Error).Str("consumer", g.name).Str("topic", *p.Topic).
				Int32("partition", p.Partition).Msg("corekafka: riavvolgimento della partizione fallito")
		}
	}
	log.Info().Str("consumer", g.name).Int("partitions", len(parts)).
		Msg("corekafka: batch scartato, consumo riavvolto al primo offset non committato")
}

// isNotAssigned riconosce l'errore di una seek su una partizione che non è più nostra: è l'esito
// atteso dopo una revoca, non un guasto da segnalare.
func isNotAssigned(err error) bool {
	var ke kafka.Error
	return errors.As(err, &ke) && (ke.Code() == kafka.ErrState || ke.Code() == kafka.ErrUnknownPartition)
}
