package corekafka

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
)

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
	// Server porta lo stesso nome della sua chiave YAML e del suo tipo (spec.KafkaServer): è la
	// sezione che si corregge quando un errore la nomina, e tre nomi diversi per raggiungerla erano
	// due traduzioni a carico di chi legge il messaggio.
	Server spec.KafkaServer `yaml:"server" mapstructure:"server" json:"server"`
	// Processors è la lista dei processor attivabili: ogni voce si lega per nome a un Handler o
	// Transformer registrato.
	Processors []spec.ProcessorSpec `yaml:"processors" mapstructure:"processors" json:"processors"`
}

// ActiveProcessors ritorna i processor ATTIVI — presenti nella lista e non disabled — nell'ordine di
// config e NON risolti.
//
// È l'UNICO punto in cui vive la regola di attivazione: da qui in poi "attivo" è un fatto acquisito e
// non una condizione da ri-valutare. Prima il filtro era scritto due volte — qui e all'ingresso
// dell'engine — e una regola duplicata è una regola che può divergere.
//
// Non risolti di proposito: l'engine ha bisogno dei blocchi GREZZI per attribuire errori e avvisi a
// chi li ha scritti (le kafka-properties del processor, il blocco `producer` scritto o assente), e la
// risoluzione la rifà lui — Resolve è idempotente. Chi qui ha bisogno di un campo ereditabile lo
// risolve sul posto: è il caso di needsDeadletterProducer.
func (c Config) ActiveProcessors() []spec.ProcessorSpec {
	out := make([]spec.ProcessorSpec, 0, len(c.Processors))
	for _, s := range c.Processors {
		if s.Disabled {
			log.Info().Str("processor", s.Name).Msg("corekafka: processor disabilitato (disabled=true), non attivato")
			continue
		}
		out = append(out, s)
	}
	return out
}
