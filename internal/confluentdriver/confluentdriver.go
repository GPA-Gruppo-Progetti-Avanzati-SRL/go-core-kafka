// Package confluentdriver è l'implementazione di internal/driver basata su confluent-kafka-go/v2.
// È l'UNICO package di go-core-kafka che importa il client Kafka concreto: engine, producer pubblico
// e API pubblica dipendono solo da internal/driver. Un futuro internal/franzdriver sostituirebbe
// questo package cambiando una sola riga nel package root (driversel.go), senza impatto sulle app.
//
// NOTA CGo: confluent-kafka-go/v2 è un binding CGo su librdkafka → richiede CGO_ENABLED=1.
package confluentdriver

import (
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Factory implementa driver.Factory usando confluent-kafka-go.
type Factory struct{}

// New ritorna la Factory Confluent come driver.Factory (usata di default dal package root).
func New() driver.Factory { return Factory{} }

// NewGroupConsumer crea un consumer di consumer-group per la modalità handle (at-least-once).
func (Factory) NewGroupConsumer(s spec.ConsumerSpec, k spec.KafkaServer) (driver.GroupConsumer, error) {
	cm := consumerConfigMap(s, k)
	c, err := kafka.NewConsumer(cm)
	if err != nil {
		return nil, fmt.Errorf("confluentdriver: NewConsumer %q: %w", s.Name, err)
	}
	if err := c.SubscribeTopics(s.Topics, nil); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("confluentdriver: SubscribeTopics %q: %w", s.Name, err)
	}
	return &groupConsumer{c: c, offsets: newOffsetTracker()}, nil
}

// NewTransactSession crea la sessione EOS Kafka->Kafka (consumer + producer transazionale).
func (Factory) NewTransactSession(s spec.ConsumerSpec, k spec.KafkaServer) (driver.TransactSession, error) {
	if s.TransactionalID == "" {
		return nil, fmt.Errorf("confluentdriver: transactional-id mancante per il consumer %q (modalità transform)", s.Name)
	}
	// EOS: il consumer legge solo record committati.
	cm := consumerConfigMap(s, k)
	_ = cm.SetKey("isolation.level", "read_committed")
	c, err := kafka.NewConsumer(cm)
	if err != nil {
		return nil, fmt.Errorf("confluentdriver: NewConsumer %q: %w", s.Name, err)
	}
	if err := c.SubscribeTopics(s.Topics, nil); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("confluentdriver: SubscribeTopics %q: %w", s.Name, err)
	}
	p, err := kafka.NewProducer(producerConfigMap(s.TransactionalID, k))
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("confluentdriver: NewProducer (tx) %q: %w", s.Name, err)
	}
	return &transactSession{c: c, p: p, offsets: newOffsetTracker()}, nil
}

// NewProducer crea un producer non transazionale (DLQ / servizio pubblico).
func (Factory) NewProducer(k spec.KafkaServer) (driver.Producer, error) {
	p, err := kafka.NewProducer(producerConfigMap("", k))
	if err != nil {
		return nil, fmt.Errorf("confluentdriver: NewProducer: %w", err)
	}
	return &producer{p: p}, nil
}
