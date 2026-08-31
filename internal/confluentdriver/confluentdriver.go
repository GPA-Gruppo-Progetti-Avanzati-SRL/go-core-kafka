// Package confluentdriver è l'implementazione di internal/driver basata su confluent-kafka-go/v2.
// Engine, producer pubblico e API pubblica dipendono solo da internal/driver: qui dentro è confinato
// tutto ciò che sa del client. L'alternativa è internal/franzdriver, e la scelta la fa l'app
// importando il guscio driver/confluent (questo) o driver/franz e passandone la Driver a
// corekafka.WithDriver.
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
	"github.com/rs/zerolog/log"
)

// Factory implementa driver.Factory usando confluent-kafka-go.
type Factory struct{}

// New ritorna la Factory Confluent come driver.Factory. La registra il guscio pubblico
// driver/confluent, che l'app sceglie con corekafka.WithDriver.
//
// Il log all'avvio non è decorativo: con due driver selezionabili, quale sia in esercizio è la prima
// cosa da sapere leggendo i log di un processo che si comporta in modo inatteso.
func New() driver.Factory {
	log.Info().Msg("corekafka: driver confluent-kafka-go/v2 (librdkafka, CGo)")
	return Factory{}
}

// NewGroupConsumer crea un consumer di consumer-group per la modalità handle (at-least-once).
func (Factory) NewGroupConsumer(s spec.ProcessorSpec, k spec.KafkaServer) (driver.GroupConsumer, error) {
	gs, err := newGroupSession(s, k)
	if err != nil {
		return nil, err
	}
	return &groupConsumer{groupSession: gs}, nil
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

	gs, err := newGroupSession(s, k)
	if err != nil {
		return nil, err
	}
	prod, err := kafka.NewProducer(producerConfigMap(s.TransactionalID, "processor "+s.Name, s.Producer, k))
	if err != nil {
		_ = gs.c.Close()
		return nil, fmt.Errorf("confluentdriver: NewProducer (tx) %q: %w", s.Name, err)
	}
	return &transactSession{
		groupSession: gs,
		txn: txn{
			p:           prod,
			initTimeout: s.Producer.InitTransactionsTimeout,
			reportWait:  reportWait(s.Producer),
		},
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
	return &producer{p: prod, flushTimeout: int(p.FlushTimeout.Milliseconds()), reportWait: reportWait(p)}, nil
}

// NewProcessorProducer crea il producer non transazionale di un processor (transform at-least-once).
// A differenza di NewProducer il tuning è quello del processor — già risolto, quindi senza
// WithDefaults — e l'owner degli avvisi è il processor, non `server.producer`.
func (Factory) NewProcessorProducer(s spec.ProcessorSpec, k spec.KafkaServer) (driver.Producer, error) {
	p := s.Producer
	prod, err := kafka.NewProducer(producerConfigMap("", "processor "+s.Name, p, k))
	if err != nil {
		return nil, fmt.Errorf("confluentdriver: NewProcessorProducer %q: %w", s.Name, err)
	}
	return &producer{p: prod, flushTimeout: int(p.FlushTimeout.Milliseconds()), reportWait: reportWait(p)}, nil
}

// NewTxProducer crea il producer TRANSAZIONALE del processo (una transazione per Produce). Come il
// non transazionale non appartiene a nessun processor, quindi il tuning arriva da `server.producer` —
// da cui viene anche l'id, che è ciò che ha fatto scegliere questa forma al chiamante
// (`server.producer.transactional-id`).
//
// L'id non è ri-validato qui: senza, il chiamante avrebbe costruito il non transazionale. È lo stesso
// contratto di NewTransactSession, che invece lo pretende perché in EOS l'id sta sullo spec del
// processor e la sua assenza è una misconfig.
func (Factory) NewTxProducer(k spec.KafkaServer, p spec.ProducerTuning, transactionalID string) (driver.TxProducer, error) {
	p = p.WithDefaults()
	prod, err := kafka.NewProducer(producerConfigMap(transactionalID, "server.producer", p, k))
	if err != nil {
		return nil, fmt.Errorf("confluentdriver: NewProducer (tx): %w", err)
	}
	return &txProducer{
		txn: txn{
			p:           prod,
			initTimeout: p.InitTransactionsTimeout,
			reportWait:  reportWait(p),
		},
		flushTimeout: int(p.FlushTimeout.Milliseconds()),
	}, nil
}

// newGroupSession crea il consumer, lo iscrive ai topic con il rebalance callback che protegge gli
// offset (vedi rebalance.go) e ne compone la groupSession. È condivisa dalle due modalità: la
// sottoscrizione e la disciplina sugli offset sono identiche, cambia solo come li si conferma.
func newGroupSession(s spec.ProcessorSpec, k spec.KafkaServer) (groupSession, error) {
	offsets := newOffsetTracker()
	c, err := kafka.NewConsumer(consumerConfigMap(s, k))
	if err != nil {
		return groupSession{}, fmt.Errorf("confluentdriver: NewConsumer %q: %w", s.Name, err)
	}
	rb := &rebalanceObserver{name: s.Name, offsets: offsets}
	if err := c.SubscribeTopics(s.Topics, rb.callback); err != nil {
		_ = c.Close()
		return groupSession{}, fmt.Errorf("confluentdriver: SubscribeTopics %q: %w", s.Name, err)
	}
	return groupSession{name: s.Name, c: c, offsets: offsets, rb: rb}, nil
}
