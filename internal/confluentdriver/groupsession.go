package confluentdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

	// Il callback del rebalance gira DENTRO questa chiamata, se l'evento estratto era una revoca.
	msg, err := g.decide(g.c.Poll(ms), g.rb.takeRevoked())
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
func (g *groupSession) decide(ev kafka.Event, revoked []driver.TopicPartition) (*kafka.Message, error) {
	msg, _ := ev.(*kafka.Message)

	if len(revoked) > 0 {
		if msg != nil {
			// Un messaggio consegnato insieme alla revoca NON si butta: è già uscito dalla coda del
			// client, la posizione di fetch non torna indietro, e se appartiene a una partizione
			// RITENUTA nessuno lo rileggerà mai. Lo consegniamo, e il reset lo segnaliamo al giro
			// dopo: se la sua partizione era fra le revocate sarà il filtro sul batch a toglierlo, e
			// lì buttarlo è corretto perché lo rilegge il nuovo owner dall'ultimo commit.
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
	g.offsets.reset()
}
