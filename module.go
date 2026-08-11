// Package corekafka è l'orchestratore e la superficie pubblica di go-core-kafka: espone Config e
// Module (mirror di batch.Module) e ri-esporta i tipi neutri e gli helper di registrazione dei
// sub-package (vedi corekafka.go), così l'app importa un solo package. Il backend sink è iniettato
// come ModuleFunc (WithSink), quindi corekafka non importa alcun backend: un'app non-Mongo non
// trascina mongo-driver.
package corekafka

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/consumer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
)

// ModuleFunc è la firma comune dei Module() componibili (backend sink e componenti extra): modes-only,
// il config è iniettato da fx. Si passano per riferimento diretto (niente closure), come in go-core-batch.
type ModuleFunc func(modes ...string)

// Option configura Module.
type Option func(*options)

type options struct {
	modes    []string
	producer bool
	modules  []ModuleFunc
}

// WithModes limita i consumer (e i backend collegati) ai core.Mode indicati. Vuoto = sempre attivi.
func WithModes(modes ...string) Option {
	return func(o *options) { o.modes = modes }
}

// WithProducer abilita esplicitamente il Producer pubblico. Viene comunque abilitato in automatico se
// almeno un consumer usa on-error=deadletter (serve per il DLQ).
func WithProducer() Option {
	return func(o *options) { o.producer = true }
}

// WithModule aggiunge componenti extra (accumula), gate-ati sugli stessi modes dei consumer.
func WithModule(m ...ModuleFunc) Option {
	return func(o *options) { o.modules = append(o.modules, m...) }
}

// Module wira il sottosistema Kafka a partire da una singola Config. Fornisce a fx la connessione e la
// lista consumer (core.Supply), la driver.Factory (provideDriver), l'eventuale Producer/DLQ e i
// backend iniettati, poi registra e avvia l'engine. Il gating è per-registrazione via i modes.
func Module(cfg *Config, opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	core.Module("kafka", func() {
		core.Supply(cfg.Kafka, o.modes...)
		core.Supply(cfg.Consumers, o.modes...)

		provideDriver(o.modes...)

		needDLQ := o.producer
		for _, s := range cfg.Consumers {
			// Il Producer condiviso (non transazionale) serve al DLQ della modalità handle. A questo
			// punto (wiring) la modalità non è ancora nota (dipende dalla registrazione fx), quindi lo
			// abilitiamo per qualsiasi spec ATTIVO con deadletter-topic: un eventuale consumer transform
			// lo lascerebbe inutilizzato (il transform produce il DLQ nella sua sessione EOS).
			if !s.Disabled && s.HasDeadletter() {
				needDLQ = true
			}
		}
		if needDLQ {
			producer.Module(o.modes...)
		}

		for _, m := range o.modules {
			m(o.modes...)
		}

		core.Provide(consumer.NewConsumers, o.modes...)
		core.Invoke(func(*consumer.Consumers) {}, o.modes...)
	})
}
