package corekafka

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
)

// Config è la configurazione unificata di go-core-kafka. Ha due sole sezioni:
//
//	server:     tutto ciò che riguarda "come parliamo con Kafka" — la connessione, più i due blocchi
//	            di tuning globale `consumer` (ereditato da ogni processor) e `producer`.
//	processors: una voce per processor, con la sola identità (nome, topic, group, identità EOS), le
//	            properties applicative e gli eventuali override del tuning globale.
//
// L'app la carica come singola sezione YAML (core.ReadConfig) e la passa a Module.
type Config struct {
	Kafka spec.KafkaServer `yaml:"server" mapstructure:"server" json:"server"`
	// Processors è la lista dei processor attivabili: ogni voce si lega per nome a un Handler o
	// Transformer registrato.
	Processors []spec.ProcessorSpec `yaml:"processors" mapstructure:"processors" json:"processors"`

	// Consumers è il nome storico di Processors. Deprecato: `consumer` è ormai il blocco di tuning
	// globale del client (`server.consumer`), e usare la stessa parola per la lista dei processor
	// rendeva le due cose indistinguibili a colpo d'occhio. Resta letta per non rompere i config
	// esistenti; se valorizzata, Module logga un warning.
	//
	// Deprecated: usare Processors (chiave YAML `processors`).
	Consumers []spec.ProcessorSpec `yaml:"consumers" mapstructure:"consumers" json:"consumers"`
}

// processors ritorna la lista effettiva dei processor, gestendo la chiave deprecata. Le due chiavi
// non si fondono: se entrambe sono valorizzate vince `processors` e l'altra è segnalata, perché una
// fusione silenziosa nasconderebbe una migrazione lasciata a metà.
func (c *Config) processors() []spec.ProcessorSpec {
	switch {
	case len(c.Processors) > 0 && len(c.Consumers) > 0:
		log.Warn().Int("processors", len(c.Processors)).Int("consumers", len(c.Consumers)).
			Msg("corekafka: sono valorizzate sia `processors` sia la deprecata `consumers`: viene usata solo `processors`")
		return c.Processors
	case len(c.Consumers) > 0:
		log.Warn().
			Msg("corekafka: la chiave `consumers` è deprecata, rinominarla in `processors` (`consumer` è ora il tuning globale sotto `server`)")
		return c.Consumers
	default:
		return c.Processors
	}
}
