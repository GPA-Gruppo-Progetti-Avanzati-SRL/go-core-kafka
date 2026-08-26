package corekafka

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"

// Config è la configurazione unificata di go-core-kafka. Ha due sole sezioni:
//
//	server:     tutto ciò che riguarda "come parliamo con Kafka" — la connessione, più i tre blocchi
//	            globali `restart` (supervisione del loop), `consumer` e `producer` (tuning dei due
//	            client), da cui ogni processor eredita.
//	processors: una voce per processor, con la sola identità (nome, topic, group, identità EOS), le
//	            properties applicative e i blocchi omonimi che sovrascrivono i globali.
//
// L'app la carica come singola sezione YAML (core.ReadConfig) e la passa a Module.
type Config struct {
	Kafka spec.KafkaServer `yaml:"server" mapstructure:"server" json:"server"`
	// Processors è la lista dei processor attivabili: ogni voce si lega per nome a un Handler o
	// Transformer registrato.
	Processors []spec.ProcessorSpec `yaml:"processors" mapstructure:"processors" json:"processors"`
}
