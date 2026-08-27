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

// ModuleFunc è la firma comune dei Module() componibili passati a WithModule: modes-only, il config è
// iniettato da fx. Si passano per riferimento diretto (niente closure), come in go-core-batch.
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

// WithProducer forza la costruzione del Producer interno anche quando nessuno spec ha un
// deadletter-topic (in quel caso è già abilitato in automatico, serve per il DLQ). Il Producer è
// privato al sottosistema — vedi Module — quindi l'Option non lo rende iniettabile dall'app: un
// consumer che deve produrre lo fa con un Transformer (EOS Kafka→Kafka), che è il seam previsto.
func WithProducer() Option {
	return func(o *options) { o.producer = true }
}

// WithModule aggiunge componenti opzionali al sottosistema (accumula), gate-ati sugli stessi modes dei
// consumer: è il punto di estensione per un Module() di terze parti che debba vivere e spegnersi con
// l'engine. Nessun componente in-tree lo usa oggi — i backend che lo giustificavano non esistono più —
// e resta perché è l'unico modo, per un'app, di agganciare qualcosa al ciclo di vita di questo
// sottosistema chiuso.
func WithModule(m ...ModuleFunc) Option {
	return func(o *options) { o.modules = append(o.modules, m...) }
}

// Module wira il sottosistema Kafka a partire da una singola Config e dalla funzione di registrazione
// dell'app (che chiama RegisterHandler/RegisterTransformer per ogni consumer). Fornisce a fx la
// connessione e la lista consumer (core.Supply), la driver.Factory (provideDriver), l'eventuale
// Producer/DLQ e i backend iniettati, poi registra e avvia l'engine. Il gating è per-registrazione via
// i modes.
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

	// Solo i processor ATTIVI (presenti nella lista `processors` e non disabled) vengono forniti a fx,
	// così le dipendenze di un processor spento (es. il data layer Mongo) non entrano nel grafo e non
	// vengono mai connesse. Fatto fuori dallo scope core.ModuleClosed("kafka") perché i processor sono
	// sempre stati forniti a root e il value group aggrega comunque root + modulo (l'engine li vede lo
	// stesso). register() gira sincronamente qui dentro: RegisterHandler/RegisterTransformer forniscono
	// subito a fx solo i processor attivi, nessuna finestra temporale con l'esterno.
	activeSpecs := cfg.ActiveSpecs()
	active := make(map[string]spec.ProcessorSpec, len(activeSpecs))
	for _, s := range activeSpecs {
		active[s.Name] = s
	}
	processor.Apply(register, active, o.modes)

	core.ModuleClosed("kafka", func() {
		core.Supply(cfg.Kafka, o.modes...)
		core.Supply(cfg.Kafka.Producer, o.modes...)
		// La lista GREZZA: l'engine ispeziona i blocchi non risolti (le kafka-properties scritte dal
		// processor, il blocco `producer` scritto o assente) per attribuire errori e avvisi a chi li
		// ha scritti. La risoluzione la rifà lui, ed è idempotente.
		core.Supply(cfg.Processors, o.modes...)

		provideDriver(o.modes...)

		if needsDeadletterProducer(activeSpecs, o.producer) {
			producer.Module(o.modes...)
		}

		for _, m := range o.modules {
			m(o.modes...)
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
// force è WithProducer(): lo abilita anche senza alcun deadletter-topic.
//
// È una funzione e non due righe dentro la closure del wiring perché è l'unica decisione condizionale
// del Module, quindi la sola parte testabile senza costruire un grafo fx.
func needsDeadletterProducer(active []spec.ProcessorSpec, force bool) bool {
	if force {
		return true
	}
	for _, s := range active {
		if s.HasDeadletter() {
			return true
		}
	}
	return false
}
