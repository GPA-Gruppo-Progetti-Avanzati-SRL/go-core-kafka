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

// DriverFunc è la registrazione di un driver Kafka: qualunque `func()` va bene. È un alias e non un
// defined type perché i due gusci pubblici (driver/confluent, driver/franz) possano dichiarare la
// propria Driver senza importare corekafka.
//
// Senza modes di proposito: il driver appartiene a QUESTO Module e ne eredita il gating. Ripassarli
// non aggiungerebbe nulla e permetterebbe di sbagliarli, ottenendo un driver registrato in modi
// diversi da quelli dei consumer che lo iniettano.
type DriverFunc = func()

type options struct {
	modes    []string
	producer bool
	driver   DriverFunc
}

// WithModes limita i consumer (e i backend collegati) ai core.Mode indicati. Vuoto = sempre attivi.
func WithModes(modes ...string) Option {
	return func(o *options) { o.modes = modes }
}

// WithProducer aggiunge a Module il producer PUBBLICO: un'app che consuma e in più pubblica su Kafka
// per conto suo (un'API che accoda, un job che notifica). Il producer è lo stesso di
// ProducerModule — stessa Config, stesso driver, stessa scelta di transazionalità
// (`server.producer.transactional-id`) — e l'app lo inietta come corekafka.IProducer.
//
// Registrato fuori dal ModuleClosed, quindi serve anche il DLQ dei processor in modalità handle: un
// solo client, una sola connessione, un solo tuning.
//
// Non è il modo di produrre DENTRO un consumer: per quello c'è il Transformer (EOS Kafka→Kafka), il
// solo seam in cui output e offset consumati sono atomici. Un Handler che pubblica con questo producer
// ha due esiti indipendenti, e a un replay del batch ripubblica.
func WithProducer() Option {
	return func(o *options) { o.producer = true }
}

// WithDriver sceglie il client Kafka concreto. È OBBLIGATORIA e non ha un default: un default
// costringerebbe QUESTO package a importare un driver, e con confluent significherebbe librdkafka
// via CGo per ogni app — compresa quella che ha scelto franz. La scelta è quindi un import dell'app.
//
//	corekafka.WithDriver(confluentdriver.Driver)  // go-core-kafka/driver/confluent — CGO_ENABLED=1
//	corekafka.WithDriver(franzdriver.Driver)      // go-core-kafka/driver/franz     — puro Go
func WithDriver(d DriverFunc) Option {
	return func(o *options) { o.driver = d }
}

// Module wira il sottosistema Kafka a partire da una singola Config e dalla funzione di registrazione
// dell'app (che chiama RegisterHandler/RegisterTransformer per ogni consumer). Fornisce a fx la
// connessione e la lista dei processor attivi (core.Supply), la driver.Factory (il driver scelto
// con WithDriver), l'eventuale Producer/DLQ, poi registra e avvia l'engine. Il gating è
// per-registrazione via i modes.
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
	requireDriver(&o, "corekafka.Module")

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
		// WithDefaults sui soli campi di connessione (client-id): è l'unico punto attraversato da
		// tutti i client del sottosistema, quindi l'unico in cui scriverlo una volta sola.
		core.Supply(cfg.Server.WithDefaults(), o.modes...)
		core.Supply(cfg.Server.Producer, o.modes...)
		// La lista è GREZZA — l'engine ispeziona i blocchi non risolti per attribuire errori e avvisi
		// a chi li ha scritti, e la risoluzione la rifà lui — ma già FILTRATA: un processor
		// disabilitato non arriva nemmeno all'engine.
		core.Supply(active, o.modes...)

		// Il driver eredita il gating del Module: la stessa condizione che core.Provide applicherebbe
		// ai suoi modes, valutata qui perché la Driver non li prende.
		if core.IsMode(o.modes...) {
			o.driver()
		}

		// Il producer del DLQ, privato al sottosistema. Con WithProducer() NON si registra qui: il
		// producer è già a root (vedi sotto) e da lì il modulo — che ne è discendente — lo vede. Un
		// secondo client per lo stesso lavoro sarebbe una connessione in più e due tuning da
		// tenere allineati.
		if !o.producer && needsDeadletterProducer(active, cfg.Server) {
			core.ProvideAs[producer.IProducer](producer.NewProducer, o.modes...)
		}

		core.Provide(consumer.NewConsumers, o.modes...)
		core.Invoke(func(*consumer.Consumers) {}, o.modes...)
	})

	// WithProducer(): il producer esce dal sottosistema e diventa iniettabile dall'app. Registrato
	// FUORI dal ModuleClosed, che è il solo modo di esporre un seam (da dentro non esce nulla), e con
	// la stessa Config — l'app scrive la connessione una volta sola. Serve anche il DLQ, perché dal
	// modulo si vede ciò che sta a root.
	if o.producer {
		provideProducer(cfg, o)
	}
}

// requireDriver è il fail-fast condiviso dai due entry-point: senza driver non c'è nulla da wirare.
//
// Panic e non errore fx: il messaggio deve arrivare a chi scrive la composition root, non finire
// dentro un grafo che non verrà mai costruito. Non ha un default di proposito — un default
// costringerebbe QUESTO package a importare un driver, e con confluent significherebbe librdkafka via
// CGo per ogni app, compresa quella che ha scelto franz.
func requireDriver(o *options, entryPoint string) {
	if o.driver == nil {
		panic(entryPoint + ": WithDriver è obbligatoria — corekafka.WithDriver(confluentdriver.Driver) " +
			"(go-core-kafka/driver/confluent, richiede CGO_ENABLED=1) oppure " +
			"corekafka.WithDriver(franzdriver.Driver) (go-core-kafka/driver/franz, puro Go)")
	}
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
// Non prende più un `force`: con WithProducer() il producer è registrato a root — quindi il DLQ lo
// trova comunque — e questa funzione decide solo se ne serve uno PRIVATO al sottosistema.
//
// È una funzione e non due righe dentro la closure del wiring perché è l'unica decisione condizionale
// del Module, quindi la sola parte testabile senza costruire un grafo fx.
func needsDeadletterProducer(active []spec.ProcessorSpec, server spec.KafkaServer) bool {
	for _, s := range active {
		if s.Resolve(server).HasDeadletter() {
			return true
		}
	}
	return false
}
