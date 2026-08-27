// Package corekafka è l'orchestratore e la superficie pubblica di go-core-kafka: espone Config e
// Module (mirror di batch.Module) e ri-esporta i tipi neutri e gli helper di registrazione dei
// sub-package (vedi corekafka.go), così l'app importa un solo package.
//
// corekafka non importa NESSUN backend di persistenza, e non perché li inietti: perché non ne ha
// bisogno. La business logic di un Handler è libera e sta nell'app — è l'app a portarsi il proprio
// data layer — quindi un'app non-Mongo non trascina mongo-driver semplicemente perché qui non ne
// esiste traccia.
package corekafka

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/consumer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// Option configura Module.
type Option func(*options)

type options struct {
	modes    []string
	producer bool
}

// WithModes limita i consumer (e i backend collegati) ai core.Mode indicati. Vuoto = sempre attivi.
func WithModes(modes ...string) Option {
	return func(o *options) { o.modes = modes }
}

// WithProducer forza la costruzione del Producer interno anche quando nessuno spec ha un
// deadletter-topic (in quel caso è già abilitato in automatico, serve per il DLQ). Il Producer è
// privato al sottosistema — vedi Module — quindi l'Option non lo rende iniettabile dall'app: un
// consumer che deve produrre lo fa con un Transformer (EOS Kafka→Kafka), che è il seam previsto.
func WithProducer() Option {
	return func(o *options) { o.producer = true }
}

// Module wira il sottosistema Kafka a partire da una singola Config e dalla funzione di registrazione
// dell'app (che chiama RegisterHandler/RegisterTransformer per ogni consumer). Fornisce a fx la
// connessione e la lista dei processor attivi (core.Supply), la driver.Factory (provideDriver),
// l'eventuale Producer/DLQ, poi registra e avvia l'engine. Il gating è per-registrazione via i modes.
//
// È un core.ModuleClosed: Kafka è un sottosistema chiuso — consuma i seam dell'app (Handler e
// Transformer) e non le espone nulla in cambio, quindi spec.KafkaServer, []spec.ProcessorSpec,
// driver.Factory, *producer.Producer e *consumer.Consumers sono privati al modulo. Gli Handler
// restano forniti a root: il value group li porta dentro il modulo (root → discendenti), mentre le
// loro dipendenze applicative sono risolte a root come sempre.
func Module(cfg *Config, register func(), opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// Un solo filtro per tutto il wiring (vedi Config.ActiveProcessors): ciò che passa di qui è
	// attivo, e nessuno più a valle deve chiederselo.
	//
	// Solo i processor attivi vengono forniti a fx, così le dipendenze di un processor spento (es. il
	// data layer Mongo) non entrano nel grafo e non vengono mai connesse. Fatto fuori dallo scope
	// core.ModuleClosed("kafka") perché i processor sono sempre stati forniti a root e il value group
	// aggrega comunque root + modulo (l'engine li vede lo stesso). register() gira sincronamente qui
	// dentro: RegisterHandler/RegisterTransformer forniscono subito a fx solo i processor attivi,
	// nessuna finestra temporale con l'esterno.
	active := cfg.ActiveProcessors()
	byName := make(map[string]spec.ProcessorSpec, len(active))
	for _, s := range active {
		byName[s.Name] = s
	}
	processor.Apply(register, byName, o.modes)

	core.ModuleClosed("kafka", func() {
		core.Supply(cfg.Server, o.modes...)
		core.Supply(cfg.Server.Producer, o.modes...)
		// La lista è GREZZA — l'engine ispeziona i blocchi non risolti per attribuire errori e avvisi
		// a chi li ha scritti, e la risoluzione la rifà lui — ma già FILTRATA: un processor
		// disabilitato non arriva nemmeno all'engine.
		core.Supply(active, o.modes...)

		provideDriver(o.modes...)

		if needsDeadletterProducer(active, cfg.Server, o.producer) {
			producer.Module(o.modes...)
		}

		core.Provide(consumer.NewConsumers, o.modes...)
		core.Invoke(func(*consumer.Consumers) {}, o.modes...)
	})
}

// needsDeadletterProducer dice se serve il Producer condiviso (non transazionale), che alimenta il DLQ
// della modalità handle. Al momento del wiring la modalità non è ancora nota — dipende dalla
// registrazione fx — quindi basta UN processor attivo con deadletter-topic: un eventuale processor
// transform lo lascerebbe inutilizzato (produce il proprio DLQ dentro la sessione EOS), ed è un costo
// molto minore di un engine che al boot lo chiede e non lo trova.
//
// Risolve gli spec perché `deadletter-topic` è EREDITABILE: un processor che lo prende da
// `server.consumer` non lo ha scritto su di sé, ma il Producer gli serve lo stesso. La risoluzione sta
// QUI, nell'unico punto del wiring che legge un campo ereditabile, e non a monte su tutta la lista.
//
// force è WithProducer(): lo abilita anche senza alcun deadletter-topic.
//
// È una funzione e non due righe dentro la closure del wiring perché è l'unica decisione condizionale
// del Module, quindi la sola parte testabile senza costruire un grafo fx.
func needsDeadletterProducer(active []spec.ProcessorSpec, server spec.KafkaServer, force bool) bool {
	if force {
		return true
	}
	for _, s := range active {
		if s.Resolve(server).HasDeadletter() {
			return true
		}
	}
	return false
}
