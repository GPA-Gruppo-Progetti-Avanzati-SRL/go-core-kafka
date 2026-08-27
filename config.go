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

// ActiveSpecs ritorna i processor ATTIVI (presenti nella lista e non disabled) già RISOLTI, nell'ordine
// di config. È una passata sola su `processors`, usata dal wiring per due decisioni che prima ne
// facevano due: quali processor fornire a fx e se serve il Producer condiviso del DLQ.
//
// Risolti perché `deadletter-topic` è EREDITABILE: un processor che lo prende da `server.consumer` non
// lo ha scritto su di sé, ma il Producer gli serve lo stesso. L'engine rifà la risoluzione per conto
// suo — Resolve è idempotente — e i campi che gli servono grezzi (kafka-properties del processor,
// blocco producer scritto o assente) li legge dalla lista non risolta.
func (c Config) ActiveSpecs() []spec.ProcessorSpec {
	out := make([]spec.ProcessorSpec, 0, len(c.Processors))
	for _, s := range c.Processors {
		if s.Disabled {
			continue
		}
		out = append(out, s.Resolve(c.Kafka))
	}
	return out
}
