package confluentdriver

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

// errRebalanced è la causa che accompagna il SeverityReset generato da un rebalance. Non è un guasto:
// è un evento di protocollo che invalida il batch in volo.
var errRebalanced = errors.New("rebalance: partizioni revocate, batch in volo scartato")

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
	revoked bool
}

func (o *rebalanceObserver) callback(_ *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		log.Info().Str("consumer", o.name).Int("partitions", len(e.Partitions)).
			Str("assignment", e.String()).Msg("corekafka: partizioni assegnate")
	case kafka.RevokedPartitions:
		o.offsets.reset()
		o.revoked = true
		log.Warn().Str("consumer", o.name).Int("partitions", len(e.Partitions)).
			Str("assignment", e.String()).
			Msg("corekafka: partizioni revocate")
	}
	return nil
}

// takeRevoked consuma il flag: ritorna true una sola volta per rebalance, così l'engine scarta il
// batch una volta e riprende a consumare.
func (o *rebalanceObserver) takeRevoked() bool {
	if !o.revoked {
		return false
	}
	o.revoked = false
	return true
}
