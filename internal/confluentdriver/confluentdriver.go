// Package confluentdriver è l'implementazione di internal/driver basata su confluent-kafka-go/v2.
// È l'UNICO package di go-core-kafka che importa il client Kafka concreto: engine, producer pubblico
// e API pubblica dipendono solo da internal/driver. Un futuro internal/franzdriver sostituirebbe
// questo package cambiando una sola riga nel package root (driversel.go), senza impatto sulle app.
//
// È anche l'unico package che interpreta un kafka.Error: la traduzione in driver.Severity sta in
// errors.go.
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
func (Factory) NewGroupConsumer(s spec.ProcessorSpec, k spec.KafkaServer) (driver.GroupConsumer, error) {
	offsets := newOffsetTracker()
	c, rb, err := newSubscribedConsumer(s, k, offsets)
	if err != nil {
		return nil, err
	}
	return &groupConsumer{c: c, offsets: offsets, rb: rb}, nil
}

// NewTransactSession crea la sessione EOS Kafka->Kafka (consumer + producer transazionale). Il
// producer transazionale è l'unico che appartiene a un processor, quindi prende il tuning da
// s.Producer — cioè `server.producer` con gli override del processor già applicati da Resolve.
func (Factory) NewTransactSession(s spec.ProcessorSpec, k spec.KafkaServer) (driver.TransactSession, error) {
	if s.TransactionalID == "" {
		return nil, fmt.Errorf("confluentdriver: transactional-id mancante per il processor %q (modalità transform)", s.Name)
	}
	// EOS: il consumer legge solo record committati. È un'invariante della modalità, non un default:
	// leggere record non ancora committati romperebbe l'esattamente-una-volta a valle.
	s.Consumer.IsolationLevel = "read_committed"

	offsets := newOffsetTracker()
	c, rb, err := newSubscribedConsumer(s, k, offsets)
	if err != nil {
		return nil, err
	}
	prod, err := kafka.NewProducer(producerConfigMap(s.TransactionalID, "processor "+s.Name, s.Producer, k))
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("confluentdriver: NewProducer (tx) %q: %w", s.Name, err)
	}
	return &transactSession{
		c: c, p: prod, offsets: offsets, rb: rb,
		initTimeout: s.Producer.InitTransactionsTimeout,
	}, nil
}

// NewProducer crea il producer condiviso del processo, non transazionale (DLQ / servizio pubblico).
// Non appartiene a nessun processor, quindi il tuning arriva direttamente da `server.producer`.
func (Factory) NewProducer(k spec.KafkaServer, p spec.ProducerTuning) (driver.Producer, error) {
	p = p.WithDefaults()
	prod, err := kafka.NewProducer(producerConfigMap("", "server.producer", p, k))
	if err != nil {
		return nil, fmt.Errorf("confluentdriver: NewProducer: %w", err)
	}
	return &producer{p: prod, flushTimeout: int(p.FlushTimeout.Milliseconds())}, nil
}

// newSubscribedConsumer crea il consumer e lo iscrive ai topic con il rebalance callback che protegge
// gli offset (vedi rebalance.go). È condiviso dalle due modalità: la sottoscrizione e la disciplina
// sugli offset sono identiche, cambia solo cosa ci si fa sopra.
func newSubscribedConsumer(s spec.ProcessorSpec, k spec.KafkaServer, offsets *offsetTracker) (*kafka.Consumer, *rebalanceObserver, error) {
	c, err := kafka.NewConsumer(consumerConfigMap(s, k))
	if err != nil {
		return nil, nil, fmt.Errorf("confluentdriver: NewConsumer %q: %w", s.Name, err)
	}
	rb := &rebalanceObserver{name: s.Name, offsets: offsets}
	if err := c.SubscribeTopics(s.Topics, rb.callback); err != nil {
		_ = c.Close()
		return nil, nil, fmt.Errorf("confluentdriver: SubscribeTopics %q: %w", s.Name, err)
	}
	return c, rb, nil
}
