package corekafka

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/rs/zerolog/log"
)

// ProducerModule wira il SOLO producer: un processo che pubblica su Kafka e non consuma. È il gemello
// di Module e prende la stessa Config, le stesse Option e lo stesso driver — perché la configurazione
// Kafka di un'applicazione è una e sola, e l'unica differenza fra i due casi è la sezione
// `processors:`.
//
//	corekafka.ProducerModule(&svc.Kafka,
//	    corekafka.WithDriver(franzdriver.Driver),
//	    corekafka.WithModes(engine.Scheduler))
//
// I `processors:` eventualmente presenti nella Config sono IGNORATI, non un errore: la stessa sezione
// serve più processi dello stesso deployment — uno consuma (Module) e un altro pubblica soltanto — e
// farla fallire qui vorrebbe dire che la config univoca non si può usare. Il conteggio è loggato,
// perché "perché non consuma?" deve avere una risposta nei log.
//
// L'app inietta il risultato come producer.IProducer (ri-esportato: corekafka.IProducer).
func ProducerModule(cfg *Config, opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	requireDriver(&o, "corekafka.ProducerModule")

	if n := len(cfg.Processors); n > 0 {
		log.Info().Int("processors", n).
			Msg("corekafka: ProducerModule wira il solo producer, i processor di config sono ignorati (li attiva corekafka.Module)")
	}
	provideProducer(cfg, o)
}

// provideProducer registra il sottoalbero del producer pubblico. È chiamata da ProducerModule e da
// Module quando ha WithProducer(), e in entrambi i casi FUORI dal core.ModuleClosed del sottosistema:
// è così che si esprime un seam pubblico (lo stesso fa batch con lo store), perché da dentro un modulo
// chiuso non esce nulla.
//
// core.Module e non ModuleClosed: il *Producer deve uscire. Ma il driver resta dentro, con
// core.Private — è l'ingranaggio del producer, non un servizio dell'app, e se fosse esportato due
// sottoalberi che lo registrano (due ProducerModule, o un ProducerModule accanto a un Module con
// consumer) darebbero un duplicate provide.
func provideProducer(cfg *Config, o options) {
	core.Module("kafka-producer", func() {
		core.Supply(cfg.Server, o.modes...)
		core.Supply(cfg.Server.Producer, o.modes...)

		if core.IsMode(o.modes...) {
			core.Private(o.driver)
		}

		// La transazionalità è una scelta della CONFIG, non del codice: `transactional-id`
		// valorizzato ⇒ una transazione per Produce. Assente ⇒ idempotente, con un avviso che dice
		// cosa non si sta ottenendo — è l'unico punto in cui quella domanda ha una risposta, e va
		// nei log del boot.
		if cfg.Server.Producer.TransactionalID != "" {
			core.ProvideAs[producer.IProducer](producer.NewTxProducer, o.modes...)
			return
		}
		if core.IsMode(o.modes...) {
			log.Warn().Msg("corekafka: producer NON transazionale (server.producer.transactional-id assente): " +
				"i record di un Produce possono diventare visibili parzialmente ai consumer read_committed. " +
				"Per averlo, valorizzare server.producer.transactional-id con un valore univoco per replica (es. nome-${HOSTNAME})")
		}
		core.ProvideAs[producer.IProducer](producer.NewProducer, o.modes...)
	})
}
