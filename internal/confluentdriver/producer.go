package confluentdriver

import (
	"context"
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// producer implementa driver.Producer (non transazionale): usato per il DLQ e come servizio pubblico.
type producer struct {
	p            *kafka.Producer
	flushTimeout int // ms, da ProducerSpec.FlushTimeout
}

// Produce invia i record e attende i delivery report, ritornando errore al primo fallimento.
func (p *producer) Produce(_ context.Context, recs []*message.ProducerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	deliveryChan := make(chan kafka.Event, len(recs))
	for _, r := range recs {
		if err := p.p.Produce(toMessage(r), deliveryChan); err != nil {
			return wrap("produce", err)
		}
	}
	var firstErr error
	// Si drenano TUTTI i report anche dopo un errore: il canale è per-chiamata e lasciarlo pieno
	// bloccherebbe la goroutine di delivery del client.
	for range recs {
		ev := <-deliveryChan
		if m, ok := ev.(*kafka.Message); ok && m.TopicPartition.Error != nil && firstErr == nil {
			firstErr = wrap("delivery", m.TopicPartition.Error)
		}
	}
	return firstErr
}

// Close concede al producer il flush timeout configurato per svuotare la coda di invio prima di
// chiudere: quello che resta in coda allo scadere è perso, quindi il valore va commisurato al
// volume prodotto.
func (p *producer) Close() error {
	remaining := p.p.Flush(p.flushTimeout)
	p.p.Close()
	if remaining > 0 {
		return fmt.Errorf("confluentdriver: flush incompleto alla chiusura: %d record ancora in coda dopo %dms", remaining, p.flushTimeout)
	}
	return nil
}
