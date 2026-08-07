package corekafka

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"

// Config è la configurazione unificata di go-core-kafka: la connessione condivisa e la lista dei
// consumer. L'app la carica come singola sezione YAML (core.ReadConfig) e la passa a Module.
type Config struct {
	Kafka     spec.KafkaConfig    `yaml:"kafka" mapstructure:"kafka" json:"kafka"`
	Consumers []spec.ConsumerSpec `yaml:"consumers" mapstructure:"consumers" json:"consumers"`
}
