package confluentdriver

import (
	"errors"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

// errRebalanced è la causa che accompagna il SeverityReset generato da un rebalance. Non è un guasto:
// è un evento di protocollo che invalida il batch in volo.
var errRebalanced = errors.New("rebalance: partizioni revocate, batch in volo scartato")

// errAssignmentLost accompagna la perdita involontaria dell'assegnazione: non è una revoca ordinata,
// e la sessione va ricostruita invece che riusata.
var errAssignmentLost = errors.New("rebalance: assegnazione persa, sessione da ricostruire")

// rebalanceObserver è il rebalance callback passato a SubscribeTopics. Esiste per un motivo di
// CORRETTEZZA, non di osservabilità: senza callback, l'offsetTracker può conservare gli offset di
// partizioni già revocate e committarli (o inviarli alla transazione) quando l'engine chiude il
// batch. Quegli offset appartengono ormai a un altro consumer del gruppo, che sta rileggendo gli
// stessi record dall'ultimo commit: confermarli significa dichiarare elaborati record che nessuno ha
// elaborato — cioè perdere messaggi.
//
// Alla revoca l'observer fa due cose: scarta gli offset tracciati (che NON sono ancora stati
// processati dall'handler — committarli sarebbe il bug appena descritto) e alza un flag che il
// prossimo Poll trasforma in un errore SeverityReset, con cui l'engine scarta il batch in volo.
// L'esito è un replay dal nuovo owner: duplicati, mai buchi. È la scelta corretta per
// l'at-least-once, e in EOS il batch viene abortito prima di essere committato.
//
// L'observer NON chiama Assign/Unassign: se il callback non riassegna, il client Kafka lo fa da sé
// scegliendo il protocollo giusto (incremental_assign quando il gruppo è cooperative-sticky, assign
// altrimenti). Duplicare qui quella scelta significherebbe solo poterla sbagliare.
//
// Sincronizzazione: il callback è invocato dal client SINCRONAMENTE dentro Poll/ReadMessage, sulla
// stessa goroutine del consumer — c'è una sola goroutine per consumer — quindi il flag non ha
// bisogno di lock.
type rebalanceObserver struct {
	name    string
	offsets *offsetTracker
	// revoked sono le partizioni perse dall'ultima revoca non ancora consegnata all'engine. nil =
	// nessuna revoca in attesa. La lista è il valore, non un booleano, perché con il protocollo
	// cooperativo la revoca è PARZIALE: sapere QUALI partizioni sono andate è ciò che permette di
	// non buttare i record delle altre.
	revoked []driver.TopicPartition
	// lost dice che le partizioni non sono state cedute ma PERSE (sessione scaduta, poll interval
	// superato, fencing): possono già essere di un altro membro, quindi non solo il batch è da
	// buttare — la sessione non è più affidabile e va ricostruita.
	lost bool
}

func (o *rebalanceObserver) callback(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		log.Info().Str("consumer", o.name).Int("partitions", len(e.Partitions)).
			Str("assignment", e.String()).Msg("corekafka: partizioni assegnate")
	case kafka.RevokedPartitions:
		parts := toTopicPartitions(e.Partitions)
		// AssignmentLost va letto QUI: la doc di librdkafka dice che il flag è consultabile solo
		// dentro il rebalance callback, e che le assegnazioni perse sono revocate immediatamente.
		if c != nil && c.AssignmentLost() {
			o.lost = true
		}
		// Scarto degli offset delle SOLE partizioni revocate: committarli dichiarerebbe elaborati
		// record che il nuovo owner sta rileggendo. Le altre partizioni sono ancora nostre e i loro
		// offset restano tracciati — i record corrispondenti sono nel batch dell'engine, che li
		// elaborerà e committerà normalmente.
		o.offsets.resetPartitions(parts)
		// Le revoche si accumulano finché l'engine non le raccoglie: due rebalance fra due poll sono
		// improbabili ma non impossibili, e perderne una significherebbe non filtrare quei record.
		o.revoked = append(o.revoked, parts...)
		log.Info().Str("consumer", o.name).Int("partitions", len(e.Partitions)).
			Str("assignment", e.String()).
			Msg("corekafka: partizioni revocate")
	}
	return nil
}

// takeRevoked consuma le partizioni revocate: le ritorna una sola volta per rebalance, così l'engine
// filtra il batch una volta e riprende a consumare. Il secondo valore dice che l'assegnazione è stata
// PERSA, non ceduta: lì non c'è nulla da filtrare, perché non si è più owner di niente.
func (o *rebalanceObserver) takeRevoked() ([]driver.TopicPartition, bool) {
	parts, lost := o.revoked, o.lost
	o.revoked, o.lost = nil, false
	return parts, lost
}

// putBack rimette le partizioni non ancora consegnate all'engine: serve quando il client ha
// consegnato un messaggio nella stessa chiamata della revoca. Il messaggio si consegna comunque (è
// già uscito dalla coda: buttarlo sarebbe perderlo) e il reset si segnala al giro successivo.
func (o *rebalanceObserver) putBack(parts []driver.TopicPartition) {
	o.revoked = append(parts, o.revoked...)
}

// toTopicPartitions traduce le partizioni del client nei termini neutri del driver.
func toTopicPartitions(parts []kafka.TopicPartition) []driver.TopicPartition {
	out := make([]driver.TopicPartition, 0, len(parts))
	for _, p := range parts {
		if p.Topic == nil {
			continue
		}
		out = append(out, driver.TopicPartition{Topic: *p.Topic, Partition: p.Partition})
	}
	return out
}
