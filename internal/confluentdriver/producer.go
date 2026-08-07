package confluentdriver

import (
	"context"
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// producer implementa driver.Producer (non transazionale): usato per il DLQ e come servizio pubblico.
type producer struct {
	p *kafka.Producer
}

// Produce invia i record e attende i delivery report, ritornando errore al primo fallimento.
func (p *producer) Produce(_ context.Context, recs []*message.ProducerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	deliveryChan := make(chan kafka.Event, len(recs))
	for _, r := range recs {
		if err := p.p.Produce(toMessage(r), deliveryChan); err != nil {
			return fmt.Errorf("confluentdriver: Produce: %w", err)
		}
	}
	var firstErr error
	for range recs {
		ev := <-deliveryChan
		if m, ok := ev.(*kafka.Message); ok && m.TopicPartition.Error != nil && firstErr == nil {
			firstErr = fmt.Errorf("confluentdriver: delivery: %w", m.TopicPartition.Error)
		}
	}
	return firstErr
}

func (p *producer) Close() error {
	p.p.Flush(5000)
	p.p.Close()
	return nil
}
