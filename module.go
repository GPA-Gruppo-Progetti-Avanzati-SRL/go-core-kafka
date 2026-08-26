// Package corekafka è l'orchestratore e la superficie pubblica di go-core-kafka: espone Config e
// Module (mirror di batch.Module) e ri-esporta i tipi neutri e gli helper di registrazione dei
// sub-package (vedi corekafka.go), così l'app importa un solo package. Il backend sink è iniettato
// come ModuleFunc (WithSink), quindi corekafka non importa alcun backend: un'app non-Mongo non
// trascina mongo-driver.
package corekafka

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/consumer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
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

// WithProducer forza la costruzione del Producer interno anche quando nessuno spec ha un
// deadletter-topic (in quel caso è già abilitato in automatico, serve per il DLQ). Il Producer è
// privato al sottosistema — vedi Module — quindi l'Option non lo rende iniettabile dall'app: un
// consumer che deve produrre lo fa con un Transformer (EOS Kafka→Kafka), che è il seam previsto.
func WithProducer() Option {
	return func(o *options) { o.producer = true }
}

// WithModule aggiunge componenti extra (accumula), gate-ati sugli stessi modes dei consumer.
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
	//
	// Qui gli spec servono grezzi: `properties` non è ereditabile, quindi il blocco globale non
	// aggiungerebbe nulla al binding dei campi `prop:`.
	specs := cfg.processors()
	active := make(map[string]spec.ProcessorSpec, len(specs))
	for _, s := range specs {
		if !s.Disabled {
			active[s.Name] = s
		}
	}
	processor.Apply(register, active, o.modes)

	core.ModuleClosed("kafka", func() {
		core.Supply(cfg.Kafka, o.modes...)
		core.Supply(cfg.Kafka.Producer, o.modes...)
		core.Supply(specs, o.modes...)

		provideDriver(o.modes...)

		needDLQ := o.producer
		for _, raw := range specs {
			// Il Producer condiviso (non transazionale) serve al DLQ della modalità handle. A questo
			// punto (wiring) la modalità non è ancora nota (dipende dalla registrazione fx), quindi lo
			// abilitiamo per qualsiasi spec ATTIVO con deadletter-topic; un eventuale processor
			// transform lo lascerebbe inutilizzato (il transform produce il DLQ nella sua sessione EOS).
			//
			// Il controllo gira sullo spec RISOLTO perché `deadletter-topic` è ereditabile: un
			// processor che lo prende da `server.consumer` non lo ha scritto su di sé, ma il Producer
			// gli serve lo stesso — altrimenti l'engine fallirebbe al boot chiedendolo. È l'unico
			// punto del wiring che deve guardare i valori ereditati; la risoluzione vera, quella che
			// l'engine usa, la fa consumer.NewConsumers.
			if !raw.Disabled && raw.Resolve(cfg.Kafka).HasDeadletter() {
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
