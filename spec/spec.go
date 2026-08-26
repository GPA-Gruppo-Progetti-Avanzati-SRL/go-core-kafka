// Package spec contiene la configurazione neutra di go-core-kafka. Vive in un package foglia perché
// sia l'astrazione driver (che ne ha bisogno per costruire i client) sia l'engine sia il Producer la
// referenziano: tenerla qui evita cicli di import e la mantiene priva di tipi del client concreto.
//
// La forma della config è simmetrica su due livelli:
//
//	server:                 come parliamo con Kafka
//	  <connessione>
//	  restart: {...}        supervisione del loop di consumo (politica di processo)
//	  consumer: {...}       default del client consumer
//	  producer: {...}       default del client producer
//	processors:
//	  - name/topics/...     identità del processor
//	    consumer: {...}     override dei default consumer
//	    producer: {...}     override dei default producer
//	    restart: {...}      override della supervisione
//	    properties: {...}   properties APPLICATIVE (campi `prop:` di Handler/Transformer)
//
// I blocchi `consumer`/`producer`/`restart` sono gli STESSI tipi nei due livelli, quindi hanno per
// costruzione le stesse chiavi: non esiste una lista di "campi sovrascrivibili" da tenere allineata.
//
// Convenzione di tutti i knob di tuning: un campo NON valorizzato (zero, "", nil) significa "lascia
// il default", non "imposta zero". Fanno eccezione i campi con un default esplicito in WithDefaults,
// che governano l'engine e non il client.
package spec

import (
	"context"
	"maps"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// La modalità (handle vs transform) NON è in config: è DERIVATA dalla registrazione — RegisterHandler
// mette il processore nel gruppo kafka_handlers, RegisterTransformer in kafka_transformers. L'engine
// deriva il tipo dal gruppo di appartenenza (unica fonte di verità, niente mismatch con la config).

// Policy sull'errore business (record "poison") in modalità handle.
const (
	OnErrorDeadletter = "deadletter" // produce su DeadletterTopic, poi committa e prosegue
	OnErrorFailFast   = "fail-fast"  // non committa ed esce: replay al riavvio (default)
)

// Default applicati da WithDefaults quando i campi non sono valorizzati né sul processor né nel
// corrispondente blocco globale sotto `server`.
const (
	DefaultMaxBatchSize            = 500
	DefaultCutFrequency            = time.Second
	DefaultAutoOffsetReset         = "earliest"
	DefaultFlushTimeout            = 60 * time.Second
	DefaultPollTimeout             = 100 * time.Millisecond
	DefaultInitTransactionsTimeout = 60 * time.Second

	DefaultRestartInitialBackoff = time.Second
	DefaultRestartMaxBackoff     = 30 * time.Second
	DefaultRestartMultiplier     = 2.0
	DefaultRestartResetAfter     = 2 * time.Minute
)

// KafkaServer è tutto ciò che riguarda "come parliamo con Kafka": la connessione condivisa da tutti i
// client del processo, la politica di supervisione e i default dei due client.
type KafkaServer struct {
	BootstrapServers string  `yaml:"bootstrap-servers" mapstructure:"bootstrap-servers" json:"bootstrap-servers" validate:"required"`
	SecurityProtocol string  `yaml:"security-protocol" mapstructure:"security-protocol" json:"security-protocol" validate:"omitempty,oneof=PLAINTEXT SSL SASL_PLAINTEXT SASL_SSL"`
	SSL              SSLCfg  `yaml:"ssl" mapstructure:"ssl" json:"ssl"`
	SASL             SaslCfg `yaml:"sasl" mapstructure:"sasl" json:"sasl"`

	// ClientID è il client.id riportato dal broker nei log e nelle metriche: valorizzarlo col nome
	// dell'applicazione rende identificabile chi è connesso.
	ClientID string `yaml:"client-id" mapstructure:"client-id" json:"client-id"`
	// Debug è la lista dei contesti di debug di librdkafka (es. "cgrp,fetch", "broker,security").
	// Assente = nessun debug. Verboso: da usare per la diagnosi, non in esercizio.
	Debug string `yaml:"debug" mapstructure:"debug" json:"debug"`

	// Tuning di connessione, applicato sia al consumer sia al producer.
	MetadataMaxAgeMs      int  `yaml:"metadata-max-age-ms" mapstructure:"metadata-max-age-ms" json:"metadata-max-age-ms"`
	SocketKeepaliveEnable bool `yaml:"socket-keepalive-enable" mapstructure:"socket-keepalive-enable" json:"socket-keepalive-enable"`
	ConnectionsMaxIdleMs  int  `yaml:"connections-max-idle-ms" mapstructure:"connections-max-idle-ms" json:"connections-max-idle-ms"`

	// Restart è la politica di supervisione del loop di consumo. Sta QUI e non dentro `consumer`
	// perché non è tuning del client: non finisce in nessuna ConfigMap e non descrive come si
	// consuma, ma cosa fa il processo quando un loop muore. È una decisione di esercizio, uguale per
	// tutti salvo eccezioni motivate — che si scrivono come override sul singolo processor.
	Restart RestartSpec `yaml:"restart" mapstructure:"restart" json:"restart"`

	// Consumer e Producer sono i DEFAULT dei due client: ogni processor li eredita e può
	// sovrascriverne i singoli campi nei propri blocchi omonimi.
	Consumer ConsumerTuning `yaml:"consumer" mapstructure:"consumer" json:"consumer"`
	Producer ProducerTuning `yaml:"producer" mapstructure:"producer" json:"producer"`

	// KafkaProperties è l'escape hatch a livello di connessione: chiavi dotted di librdkafka passate
	// as-is sia al consumer sia al producer, applicate DOPO i campi tipizzati — quindi hanno
	// l'ultima parola. Serve per le proprietà non previste da queste struct, senza attendere un
	// rilascio della libreria. Le chiavi che l'engine deve controllare (vedi DeniedKafkaProperties)
	// sono rifiutate al boot: sono invarianti, non default sovrascrivibili.
	KafkaProperties map[string]string `yaml:"kafka-properties" mapstructure:"kafka-properties" json:"kafka-properties"`
}

// SSLCfg raccoglie i parametri TLS. CaLocation da solo basta per il TLS server-side; i tre campi
// Certificate*/Key* servono al TLS mutuo (mTLS), dove il broker autentica anche il client.
type SSLCfg struct {
	CaLocation string `yaml:"ca-location" mapstructure:"ca-location" json:"ca-location"`
	SkipVerify bool   `yaml:"skip-verify,omitempty" mapstructure:"skip-verify,omitempty" json:"skip-verify,omitempty"`

	CertificateLocation string `yaml:"certificate-location,omitempty" mapstructure:"certificate-location,omitempty" json:"certificate-location,omitempty"`
	KeyLocation         string `yaml:"key-location,omitempty" mapstructure:"key-location,omitempty" json:"key-location,omitempty"`
	KeyPassword         string `yaml:"key-password,omitempty" mapstructure:"key-password,omitempty" json:"key-password,omitempty"`
}

// SaslCfg raccoglie i parametri SASL (PLAIN, SCRAM-SHA-256/512).
type SaslCfg struct {
	Mechanisms string `yaml:"mechanisms" mapstructure:"mechanisms" json:"mechanisms" validate:"omitempty,oneof=PLAIN SCRAM-SHA-256 SCRAM-SHA-512 GSSAPI OAUTHBEARER"`
	Username   string `yaml:"username" mapstructure:"username" json:"username"`
	Password   string `yaml:"password" mapstructure:"password" json:"password"`
}

// ConsumerTuning è il blocco `consumer`: come si consuma. Contiene il tuning del client Kafka, quello
// dell'engine (batch e poll) e la policy sull'esito di un batch.
//
// Lo stesso tipo vive in `server.consumer` (default) e in `processors[].consumer` (override).
type ConsumerTuning struct {
	// --- tuning dell'engine: governa go-core-kafka, non il client ---
	MaxBatchSize int           `yaml:"max-batch-size" mapstructure:"max-batch-size" json:"max-batch-size"`
	CutFrequency time.Duration `yaml:"cut-frequency" mapstructure:"cut-frequency" json:"cut-frequency"`
	// PollTimeout è quanto attende una singola poll prima di tornare a mani vuote: è la granularità
	// con cui l'engine osserva il ticker di CutFrequency e la cancellazione del context.
	PollTimeout time.Duration `yaml:"poll-timeout" mapstructure:"poll-timeout" json:"poll-timeout"`

	// --- tuning del consumer Kafka (mappato su ConfigMap dal driver) ---
	AutoOffsetReset             string `yaml:"auto-offset-reset" mapstructure:"auto-offset-reset" json:"auto-offset-reset" validate:"omitempty,oneof=earliest latest none"`
	SessionTimeoutMs            int    `yaml:"session-timeout-ms" mapstructure:"session-timeout-ms" json:"session-timeout-ms"`
	HeartbeatIntervalMs         int    `yaml:"heartbeat-interval-ms" mapstructure:"heartbeat-interval-ms" json:"heartbeat-interval-ms"`
	FetchMinBytes               int    `yaml:"fetch-min-bytes" mapstructure:"fetch-min-bytes" json:"fetch-min-bytes"`
	FetchMaxBytes               int    `yaml:"fetch-max-bytes" mapstructure:"fetch-max-bytes" json:"fetch-max-bytes"`
	FetchWaitMaxMs              int    `yaml:"fetch-wait-max-ms" mapstructure:"fetch-wait-max-ms" json:"fetch-wait-max-ms"`
	MaxPartitionFetchBytes      int    `yaml:"max-partition-fetch-bytes" mapstructure:"max-partition-fetch-bytes" json:"max-partition-fetch-bytes"`
	QueuedMaxMessagesKbytes     int    `yaml:"queued-max-messages-kbytes" mapstructure:"queued-max-messages-kbytes" json:"queued-max-messages-kbytes"`
	MaxPollIntervalMs           int    `yaml:"max-poll-interval-ms" mapstructure:"max-poll-interval-ms" json:"max-poll-interval-ms"`
	PartitionAssignmentStrategy string `yaml:"partition-assignment-strategy" mapstructure:"partition-assignment-strategy" json:"partition-assignment-strategy" validate:"omitempty,oneof=range roundrobin cooperative-sticky"`
	// IsolationLevel: assente = read_committed in modalità transform (l'EOS lo richiede),
	// read_uncommitted in modalità handle (default di librdkafka).
	IsolationLevel string `yaml:"isolation-level" mapstructure:"isolation-level" json:"isolation-level" validate:"omitempty,oneof=read_uncommitted read_committed"`

	// --- policy sull'esito di un batch ---
	OnError string `yaml:"on-error" mapstructure:"on-error" json:"on-error" validate:"omitempty,oneof=deadletter fail-fast"`
	// DeadletterTopic è ereditabile: più processor che condividono lo stesso DLQ sono la norma, e i
	// record ci arrivano comunque etichettati con il processor di origine (header
	// corekafka-dlq-processor).
	//
	// È un puntatore per poter essere DISATTIVATO da un processor quando il globale lo valorizza:
	// `deadletter-topic: ""` significa "nessun DLQ per questo processor" (i record poison forzano il
	// fail-fast), che con una stringa semplice sarebbe indistinguibile da "eredita". Usare Deadletter()
	// per leggerlo.
	DeadletterTopic *string `yaml:"deadletter-topic" mapstructure:"deadletter-topic" json:"deadletter-topic"`

	// KafkaProperties: escape hatch del consumer (vedi KafkaServer.KafkaProperties). Le mappe di
	// `server.consumer` e del processor si FONDONO chiave per chiave, con il processor a vincere sui
	// conflitti: aggiungere una proprietà a un processor non gli fa perdere quelle comuni.
	KafkaProperties map[string]string `yaml:"kafka-properties" mapstructure:"kafka-properties" json:"kafka-properties"`
}

// inherit riempie i campi non valorizzati con quelli del blocco globale `server.consumer`.
func (t ConsumerTuning) inherit(g ConsumerTuning) ConsumerTuning {
	if t.MaxBatchSize <= 0 {
		t.MaxBatchSize = g.MaxBatchSize
	}
	if t.CutFrequency <= 0 {
		t.CutFrequency = g.CutFrequency
	}
	if t.PollTimeout <= 0 {
		t.PollTimeout = g.PollTimeout
	}
	if t.AutoOffsetReset == "" {
		t.AutoOffsetReset = g.AutoOffsetReset
	}
	if t.SessionTimeoutMs <= 0 {
		t.SessionTimeoutMs = g.SessionTimeoutMs
	}
	if t.HeartbeatIntervalMs <= 0 {
		t.HeartbeatIntervalMs = g.HeartbeatIntervalMs
	}
	if t.FetchMinBytes <= 0 {
		t.FetchMinBytes = g.FetchMinBytes
	}
	if t.FetchMaxBytes <= 0 {
		t.FetchMaxBytes = g.FetchMaxBytes
	}
	if t.FetchWaitMaxMs <= 0 {
		t.FetchWaitMaxMs = g.FetchWaitMaxMs
	}
	if t.MaxPartitionFetchBytes <= 0 {
		t.MaxPartitionFetchBytes = g.MaxPartitionFetchBytes
	}
	if t.QueuedMaxMessagesKbytes <= 0 {
		t.QueuedMaxMessagesKbytes = g.QueuedMaxMessagesKbytes
	}
	if t.MaxPollIntervalMs <= 0 {
		t.MaxPollIntervalMs = g.MaxPollIntervalMs
	}
	if t.PartitionAssignmentStrategy == "" {
		t.PartitionAssignmentStrategy = g.PartitionAssignmentStrategy
	}
	if t.IsolationLevel == "" {
		t.IsolationLevel = g.IsolationLevel
	}
	if t.OnError == "" {
		t.OnError = g.OnError
	}
	if t.DeadletterTopic == nil {
		t.DeadletterTopic = g.DeadletterTopic
	}
	t.KafkaProperties = mergeProps(g.KafkaProperties, t.KafkaProperties)
	return t
}

// Deadletter ritorna il topic DLQ effettivo: "" se non configurato o disattivato esplicitamente.
func (t ConsumerTuning) Deadletter() string {
	if t.DeadletterTopic == nil {
		return ""
	}
	return *t.DeadletterTopic
}

// WithDefaults applica i default della libreria ai campi rimasti non valorizzati.
func (t ConsumerTuning) WithDefaults() ConsumerTuning {
	if t.MaxBatchSize <= 0 {
		t.MaxBatchSize = DefaultMaxBatchSize
	}
	if t.CutFrequency <= 0 {
		t.CutFrequency = DefaultCutFrequency
	}
	if t.PollTimeout <= 0 {
		t.PollTimeout = DefaultPollTimeout
	}
	if t.AutoOffsetReset == "" {
		t.AutoOffsetReset = DefaultAutoOffsetReset
	}
	if t.OnError == "" {
		t.OnError = OnErrorFailFast
	}
	return t
}

// ProducerTuning è il blocco `producer`: come si produce. Vale per il producer condiviso del processo
// (DLQ della modalità handle, Producer pubblico) quando sta in `server.producer`, e per il producer
// TRANSAZIONALE di un singolo processor quando sta in `processors[].producer` — è quello l'unico
// producer che appartiene a un processor, e per questo l'override ha senso solo in modalità transform.
type ProducerTuning struct {
	// Acks: "0" (nessuna conferma), "1" (solo leader), "all"/"-1" (tutte le replica in-sync).
	// Assente = default di librdkafka, che con l'idempotenza attiva è comunque "all".
	Acks string `yaml:"acks" mapstructure:"acks" json:"acks" validate:"omitempty,oneof=0 1 -1 all"`
	// CompressionType comprime i batch prodotti: riduce la banda e lo spazio sul broker al costo di CPU.
	CompressionType string `yaml:"compression-type" mapstructure:"compression-type" json:"compression-type" validate:"omitempty,oneof=none gzip snappy lz4 zstd"`
	// LingerMs è un puntatore per distinguere "linger-ms: 0" (esplicito: invia subito, latenza minima)
	// da "campo assente" (usa il default di librdkafka): con un int semplice i due casi coincidono.
	LingerMs              *int          `yaml:"linger-ms" mapstructure:"linger-ms" json:"linger-ms"`
	BatchSize             int           `yaml:"batch-size" mapstructure:"batch-size" json:"batch-size"`
	BatchNumMessages      int           `yaml:"batch-num-messages" mapstructure:"batch-num-messages" json:"batch-num-messages"`
	MessageMaxBytes       int           `yaml:"message-max-bytes" mapstructure:"message-max-bytes" json:"message-max-bytes"`
	MessageSendMaxRetries int           `yaml:"max-retries" mapstructure:"max-retries" json:"max-retries"`
	MaxInFlight           int           `yaml:"max-in-flight" mapstructure:"max-in-flight" json:"max-in-flight"`
	RetryBackoff          time.Duration `yaml:"retry-backoff" mapstructure:"retry-backoff" json:"retry-backoff"`
	DeliveryTimeout       time.Duration `yaml:"delivery-timeout" mapstructure:"delivery-timeout" json:"delivery-timeout"`
	RequestTimeoutMs      int           `yaml:"request-timeout-ms" mapstructure:"request-timeout-ms" json:"request-timeout-ms"`
	MetadataMaxIdleMs     int           `yaml:"metadata-max-idle-ms" mapstructure:"metadata-max-idle-ms" json:"metadata-max-idle-ms"`
	// EnableIdempotence è un puntatore per permettere il disattivamento esplicito: assente = true
	// (il default storico di go-core-kafka, che l'idempotenza la imponeva). Ignorato sui producer
	// transazionali, dove l'idempotenza è implicita.
	EnableIdempotence *bool `yaml:"enable-idempotence" mapstructure:"enable-idempotence" json:"enable-idempotence"`
	// FlushTimeout è il tempo concesso alla chiusura del producer per svuotare la coda di invio.
	FlushTimeout time.Duration `yaml:"flush-timeout" mapstructure:"flush-timeout" json:"flush-timeout"`

	// --- transazioni (modalità transform) ---
	// TransactionTimeoutMs limita la durata di una transazione EOS: se un batch impiega più di così,
	// il broker la considera abbandonata e fa fencing del producer.
	TransactionTimeoutMs int `yaml:"transaction-timeout-ms" mapstructure:"transaction-timeout-ms" json:"transaction-timeout-ms"`
	// InitTransactionsTimeout limita la InitTransactions all'apertura della sessione EOS.
	InitTransactionsTimeout time.Duration `yaml:"init-transactions-timeout" mapstructure:"init-transactions-timeout" json:"init-transactions-timeout"`

	// KafkaProperties: escape hatch del producer (vedi KafkaServer.KafkaProperties). Come per il
	// consumer, le mappe globale e per-processor si fondono chiave per chiave.
	KafkaProperties map[string]string `yaml:"kafka-properties" mapstructure:"kafka-properties" json:"kafka-properties"`
}

// inherit riempie i campi non valorizzati con quelli del blocco globale `server.producer`.
func (p ProducerTuning) inherit(g ProducerTuning) ProducerTuning {
	if p.Acks == "" {
		p.Acks = g.Acks
	}
	if p.CompressionType == "" {
		p.CompressionType = g.CompressionType
	}
	if p.LingerMs == nil {
		p.LingerMs = g.LingerMs
	}
	if p.BatchSize <= 0 {
		p.BatchSize = g.BatchSize
	}
	if p.BatchNumMessages <= 0 {
		p.BatchNumMessages = g.BatchNumMessages
	}
	if p.MessageMaxBytes <= 0 {
		p.MessageMaxBytes = g.MessageMaxBytes
	}
	if p.MessageSendMaxRetries <= 0 {
		p.MessageSendMaxRetries = g.MessageSendMaxRetries
	}
	if p.MaxInFlight <= 0 {
		p.MaxInFlight = g.MaxInFlight
	}
	if p.RetryBackoff <= 0 {
		p.RetryBackoff = g.RetryBackoff
	}
	if p.DeliveryTimeout <= 0 {
		p.DeliveryTimeout = g.DeliveryTimeout
	}
	if p.RequestTimeoutMs <= 0 {
		p.RequestTimeoutMs = g.RequestTimeoutMs
	}
	if p.MetadataMaxIdleMs <= 0 {
		p.MetadataMaxIdleMs = g.MetadataMaxIdleMs
	}
	if p.EnableIdempotence == nil {
		p.EnableIdempotence = g.EnableIdempotence
	}
	if p.FlushTimeout <= 0 {
		p.FlushTimeout = g.FlushTimeout
	}
	if p.TransactionTimeoutMs <= 0 {
		p.TransactionTimeoutMs = g.TransactionTimeoutMs
	}
	if p.InitTransactionsTimeout <= 0 {
		p.InitTransactionsTimeout = g.InitTransactionsTimeout
	}
	p.KafkaProperties = mergeProps(g.KafkaProperties, p.KafkaProperties)
	return p
}

// WithDefaults applica i default della libreria ai campi rimasti non valorizzati.
func (p ProducerTuning) WithDefaults() ProducerTuning {
	if p.FlushTimeout <= 0 {
		p.FlushTimeout = DefaultFlushTimeout
	}
	if p.InitTransactionsTimeout <= 0 {
		p.InitTransactionsTimeout = DefaultInitTransactionsTimeout
	}
	return p
}

// Idempotent indica se il producer va configurato come idempotente (default: sì).
func (p ProducerTuning) Idempotent() bool {
	return p.EnableIdempotence == nil || *p.EnableIdempotence
}

// IsZero indica che il blocco non è stato scritto affatto. Serve a distinguere "il processor non ha
// un blocco producer" da "ne ha uno con tutti i campi al default", per poter segnalare un override
// che non avrà effetto (vedi l'avviso in modalità handle).
func (p ProducerTuning) IsZero() bool {
	return p.Acks == "" && p.CompressionType == "" && p.LingerMs == nil && p.BatchSize == 0 &&
		p.BatchNumMessages == 0 && p.MessageMaxBytes == 0 && p.MessageSendMaxRetries == 0 &&
		p.MaxInFlight == 0 && p.RetryBackoff == 0 && p.DeliveryTimeout == 0 &&
		p.RequestTimeoutMs == 0 && p.MetadataMaxIdleMs == 0 && p.EnableIdempotence == nil &&
		p.FlushTimeout == 0 && p.TransactionTimeoutMs == 0 && p.InitTransactionsTimeout == 0 &&
		len(p.KafkaProperties) == 0
}

// mergeProps fonde due mappe di proprietà: `over` vince sulle chiavi comuni. Ritorna sempre una
// mappa nuova, così l'eredità non muta il blocco globale condiviso da tutti i processor.
func mergeProps(base, over map[string]string) map[string]string {
	if len(base) == 0 && len(over) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(over))
	maps.Copy(out, base)
	maps.Copy(out, over)
	return out
}

// RestartSpec è il blocco `restart`: cosa fare quando il loop di consumo di un processor termina con
// errore. La classificazione dell'errore la fa il driver (vedi internal/driver.Severity); qui c'è
// solo la politica di riavvio.
//
// Il restart in-process esiste perché il client Kafka non sopravvive a tutto: un fencing EOS o un
// broker che cade richiedono di RICOSTRUIRE consumer e producer, non solo di riprovare la chiamata.
// Senza supervisione l'unico recovery è la morte del processo, che su un rolling restart dei broker
// diventa un CrashLoopBackOff.
//
// Sta in `server.restart` perché è una decisione di esercizio del processo, non tuning di un client;
// un processor con esigenze diverse la sovrascrive campo per campo nel proprio blocco `restart`.
type RestartSpec struct {
	// Disabled = true ripristina il comportamento storico: qualunque errore fa terminare il processo
	// (recovery delegata all'orchestratore). È *bool per poter essere disattivato da un processor
	// quando il blocco globale lo attiva (e viceversa).
	Disabled *bool `yaml:"disabled" mapstructure:"disabled" json:"disabled"`
	// MaxAttempts: 0 = illimitati (default). Superato il limite l'errore risale e il processo esce.
	MaxAttempts    int           `yaml:"max-attempts" mapstructure:"max-attempts" json:"max-attempts"`
	InitialBackoff time.Duration `yaml:"initial-backoff" mapstructure:"initial-backoff" json:"initial-backoff"`
	MaxBackoff     time.Duration `yaml:"max-backoff" mapstructure:"max-backoff" json:"max-backoff"`
	Multiplier     float64       `yaml:"multiplier" mapstructure:"multiplier" json:"multiplier"`
	// ResetAfter: un run durato almeno questo tempo prima di fallire azzera il contatore dei
	// tentativi. Senza, un processor che gira bene per giorni e poi fallisce erediterebbe i tentativi
	// consumati mesi prima.
	ResetAfter time.Duration `yaml:"reset-after" mapstructure:"reset-after" json:"reset-after"`
	// OnBusinessError estende il restart agli errori risaliti da Handle/Transform (quelli NON del
	// client Kafka). Default false: sotto on-error=fail-fast la semantica documentata è "non committa
	// ed esce", e un payload malformato riprovato in-process sarebbe un loop infinito senza uscita.
	// Va messo a true quando la causa attesa è un'infrastruttura applicativa transitoria (il DB
	// irraggiungibile), non un record poison.
	OnBusinessError *bool `yaml:"on-business-error" mapstructure:"on-business-error" json:"on-business-error"`
}

// inherit riempie i campi non valorizzati con quelli del blocco globale `server.restart`.
func (r RestartSpec) inherit(g RestartSpec) RestartSpec {
	if r.Disabled == nil {
		r.Disabled = g.Disabled
	}
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = g.MaxAttempts
	}
	if r.InitialBackoff <= 0 {
		r.InitialBackoff = g.InitialBackoff
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = g.MaxBackoff
	}
	if r.Multiplier <= 1 {
		r.Multiplier = g.Multiplier
	}
	if r.ResetAfter <= 0 {
		r.ResetAfter = g.ResetAfter
	}
	if r.OnBusinessError == nil {
		r.OnBusinessError = g.OnBusinessError
	}
	return r
}

// WithDefaults ritorna una copia con i default applicati ai campi non valorizzati.
func (r RestartSpec) WithDefaults() RestartSpec {
	if r.InitialBackoff <= 0 {
		r.InitialBackoff = DefaultRestartInitialBackoff
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = DefaultRestartMaxBackoff
	}
	if r.Multiplier <= 1 {
		r.Multiplier = DefaultRestartMultiplier
	}
	if r.ResetAfter <= 0 {
		r.ResetAfter = DefaultRestartResetAfter
	}
	return r
}

// IsDisabled: assente = false (supervisione attiva).
func (r RestartSpec) IsDisabled() bool { return r.Disabled != nil && *r.Disabled }

// RestartsOnBusinessError: assente = false (vedi OnBusinessError).
func (r RestartSpec) RestartsOnBusinessError() bool {
	return r.OnBusinessError != nil && *r.OnBusinessError
}

// ProcessorSpec è la specifica di un singolo processor (una voce di `processors`): la sua IDENTITÀ —
// il nome a cui è associato un Handler o un Transformer registrato, i topic sorgente, il gruppo e
// l'identità transazionale — le properties applicative, e gli override dei tre blocchi di `server`.
type ProcessorSpec struct {
	Name string `yaml:"name" mapstructure:"name" json:"name" validate:"required"`
	// Disabled=true: il processor NON viene attivato (utile per spegnerlo senza rimuoverlo dalla
	// config). È comunque la presenza in questa lista `processors` a comandare l'attivazione: un
	// processore registrato ma non presente qui non viene istanziato.
	Disabled bool     `yaml:"disabled" mapstructure:"disabled" json:"disabled"`
	Topics   []string `yaml:"topics" mapstructure:"topics" json:"topics" validate:"required,min=1"`
	GroupID  string   `yaml:"group-id" mapstructure:"group-id" json:"group-id" validate:"required"`
	// NB: nessun campo "mode" — la modalità è derivata da RegisterHandler/RegisterTransformer.

	// Identità EOS (modalità transform): non ereditabili, distinguono un processor dall'altro.
	TransactionalID    string `yaml:"transactional-id" mapstructure:"transactional-id" json:"transactional-id"`
	DefaultOutputTopic string `yaml:"default-output-topic" mapstructure:"default-output-topic" json:"default-output-topic"`

	// Override dei blocchi omonimi di `server`. Ogni campo non valorizzato eredita.
	Consumer ConsumerTuning `yaml:"consumer" mapstructure:"consumer" json:"consumer"`
	// Producer ha effetto solo in modalità transform, dove il producer transazionale appartiene a
	// questo processor. In modalità handle il producer è quello condiviso del processo e un override
	// qui non avrebbe destinatario: l'engine lo segnala con un warning.
	Producer ProducerTuning `yaml:"producer" mapstructure:"producer" json:"producer"`
	Restart  RestartSpec    `yaml:"restart" mapstructure:"restart" json:"restart"`

	// Properties applicative del processor. Il modo raccomandato per leggerle è il mapping sui campi
	// della struct dell'Handler/Transformer via tag `prop:` (fatto al wiring, con default e validazione
	// per campo: vedi core.BindProps); restano leggibili a runtime dal context o all'avvio tramite
	// l'interfaccia Configurable. È lo stesso tipo usato dai task di go-core-batch.
	//
	// NB: sono le properties del BUSINESS, non del client Kafka — quelle sono `kafka-properties`
	// dentro i blocchi `consumer`/`producer`.
	Properties core.Properties `yaml:"properties" mapstructure:"properties" json:"properties"`
}

// Resolve produce lo spec effettivo: eredita dai blocchi di `server` i campi non valorizzati, poi
// applica i default della libreria. L'ordine è vincolante — un valore globale deve battere il
// default, non esserne sovrascritto — ed è la ragione per cui è un metodo unico invece di chiamate
// separate che il chiamante potrebbe invertire. È idempotente: la percorrono sia il wiring sia
// l'engine.
func (s ProcessorSpec) Resolve(server KafkaServer) ProcessorSpec {
	s.Consumer = s.Consumer.inherit(server.Consumer).WithDefaults()
	s.Producer = s.Producer.inherit(server.Producer).WithDefaults()
	s.Restart = s.Restart.inherit(server.Restart).WithDefaults()
	return s
}

// HasDeadletter indica se è configurato un topic DLQ (abilita il deadletter, sia da policy di default
// sia da scelta dell'handler/transformer a runtime).
func (s ProcessorSpec) HasDeadletter() bool {
	return s.Consumer.Deadletter() != ""
}

type ctxKey int

const (
	propertiesKey ctxKey = iota
	consumerNameKey
)

// ContextWithProperties arricchisce ctx con le Properties e il nome del processor. L'engine lo chiama
// una volta per goroutine-processor; la business logic (Handler/Transformer/Mapper) le legge da ctx.
func ContextWithProperties(ctx context.Context, name string, p core.Properties) context.Context {
	ctx = context.WithValue(ctx, propertiesKey, p)
	ctx = context.WithValue(ctx, consumerNameKey, name)
	return ctx
}

// PropertiesFromContext ritorna le Properties del processor corrente (o una mappa vuota).
func PropertiesFromContext(ctx context.Context) core.Properties {
	if p, ok := ctx.Value(propertiesKey).(core.Properties); ok {
		return p
	}
	return core.Properties{}
}

// ConsumerNameFromContext ritorna il nome del processor corrente (o "").
func ConsumerNameFromContext(ctx context.Context) string {
	if n, ok := ctx.Value(consumerNameKey).(string); ok {
		return n
	}
	return ""
}
