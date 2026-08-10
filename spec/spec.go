// Package spec contiene la configurazione neutra di go-core-kafka: la connessione condivisa
// (KafkaConfig) e la specifica per-consumer (ConsumerSpec). Vive in un package foglia perché sia
// l'astrazione driver (che ne ha bisogno per costruire i client) sia l'engine sia il Producer la
// referenziano: tenerla qui evita cicli di import e la mantiene priva di tipi del client concreto.
package spec

import (
	"context"
	"strconv"
	"time"
)

// Modalità di elaborazione di un consumer.
const (
	ModeHandle    = "handle"    // at-least-once: l'Handler fa business logic libera; commit degli offset dopo Handle nil
	ModeTransform = "transform" // EOS Kafka->Kafka: produce + commit offset nella stessa transazione
)

// Policy sull'errore business (record "poison") in modalità handle.
const (
	OnErrorDeadletter = "deadletter" // produce su DeadletterTopic, poi committa e prosegue (default)
	OnErrorFailFast   = "fail-fast"  // non committa ed esce: replay al riavvio
)

// Default applicati da ConsumerSpec.WithDefaults quando i campi non sono valorizzati.
const (
	DefaultMaxBatchSize    = 500
	DefaultCutFrequency    = time.Second
	DefaultAutoOffsetReset = "earliest"
	DefaultFlushTimeout    = 60 * time.Second
	DefaultPollTimeout     = 100 * time.Millisecond
)

// KafkaConfig è la connessione condivisa da tutti i consumer/producer di un processo.
type KafkaConfig struct {
	BootstrapServers string  `yaml:"bootstrap-servers" mapstructure:"bootstrap-servers" json:"bootstrap-servers" validate:"required"`
	SecurityProtocol string  `yaml:"security-protocol" mapstructure:"security-protocol" json:"security-protocol"`
	SSL              SSLCfg  `yaml:"ssl" mapstructure:"ssl" json:"ssl"`
	SASL             SaslCfg `yaml:"sasl" mapstructure:"sasl" json:"sasl"`
}

// SSLCfg raccoglie i parametri TLS.
type SSLCfg struct {
	CaLocation string `yaml:"ca-location" mapstructure:"ca-location" json:"ca-location"`
	SkipVerify bool   `yaml:"skip-verify,omitempty" mapstructure:"skip-verify,omitempty" json:"skip-verify,omitempty"`
}

// SaslCfg raccoglie i parametri SASL (PLAIN, SCRAM-SHA-256/512).
type SaslCfg struct {
	Mechanisms string `yaml:"mechanisms" mapstructure:"mechanisms" json:"mechanisms"`
	Username   string `yaml:"username" mapstructure:"username" json:"username"`
	Password   string `yaml:"password" mapstructure:"password" json:"password"`
}

// ConsumerSpec è la specifica di un singolo consumer/spooler: lega un nome (a cui è associato un
// Handler o Transformer registrato) a uno o più topic sorgente e alla policy di consegna/errore.
type ConsumerSpec struct {
	Name    string   `yaml:"name" mapstructure:"name" json:"name" validate:"required"`
	Topics  []string `yaml:"topics" mapstructure:"topics" json:"topics" validate:"required,min=1"`
	GroupID string   `yaml:"group-id" mapstructure:"group-id" json:"group-id" validate:"required"`
	Mode    string   `yaml:"mode" mapstructure:"mode" json:"mode"` // ModeHandle (default) | ModeTransform

	// Tuning consumer (mappato su ConfigMap dal driver).
	MaxBatchSize      int           `yaml:"max-batch-size" mapstructure:"max-batch-size" json:"max-batch-size"`
	CutFrequency      time.Duration `yaml:"cut-frequency" mapstructure:"cut-frequency" json:"cut-frequency"`
	AutoOffsetReset   string        `yaml:"auto-offset-reset" mapstructure:"auto-offset-reset" json:"auto-offset-reset"`
	SessionTimeoutMs  int           `yaml:"session-timeout-ms" mapstructure:"session-timeout-ms" json:"session-timeout-ms"`
	FetchMinBytes     int           `yaml:"fetch-min-bytes" mapstructure:"fetch-min-bytes" json:"fetch-min-bytes"`
	FetchMaxBytes     int           `yaml:"fetch-max-bytes" mapstructure:"fetch-max-bytes" json:"fetch-max-bytes"`
	MaxPollIntervalMs int           `yaml:"max-poll-interval-ms" mapstructure:"max-poll-interval-ms" json:"max-poll-interval-ms"`

	// Modalità sink.
	OnError         string        `yaml:"on-error" mapstructure:"on-error" json:"on-error"`
	DeadletterTopic string        `yaml:"deadletter-topic" mapstructure:"deadletter-topic" json:"deadletter-topic"`
	FlushTimeout    time.Duration `yaml:"flush-timeout" mapstructure:"flush-timeout" json:"flush-timeout"`

	// Modalità transform (EOS).
	TransactionalID    string `yaml:"transactional-id" mapstructure:"transactional-id" json:"transactional-id"`
	DefaultOutputTopic string `yaml:"default-output-topic" mapstructure:"default-output-topic" json:"default-output-topic"`

	// Properties applicative arbitrarie, lette dalla business logic a runtime (via context o
	// tramite l'interfaccia Configurable). Come i Properties dei job di go-core-batch.
	Properties Properties `yaml:"properties" mapstructure:"properties" json:"properties"`
}

// WithDefaults ritorna una copia dello spec con i default applicati ai campi non valorizzati.
func (s ConsumerSpec) WithDefaults() ConsumerSpec {
	if s.Mode == "" {
		s.Mode = ModeHandle
	}
	if s.MaxBatchSize <= 0 {
		s.MaxBatchSize = DefaultMaxBatchSize
	}
	if s.CutFrequency <= 0 {
		s.CutFrequency = DefaultCutFrequency
	}
	if s.AutoOffsetReset == "" {
		s.AutoOffsetReset = DefaultAutoOffsetReset
	}
	if s.OnError == "" {
		s.OnError = OnErrorFailFast
	}
	if s.FlushTimeout <= 0 {
		s.FlushTimeout = DefaultFlushTimeout
	}
	return s
}

// UsesDeadletter indica se lo spec (in modalità handle) instrada i record poison a un DLQ per policy
// di default. Il DLQ serve comunque anche quando l'handler sceglie deadletter a runtime: vedi
// HasDeadletter.
func (s ConsumerSpec) UsesDeadletter() bool {
	return s.Mode != ModeTransform && s.OnError == OnErrorDeadletter
}

// HasDeadletter indica se è configurato un topic DLQ (abilita il deadletter, sia da policy di default
// sia da scelta dell'handler/transformer a runtime).
func (s ConsumerSpec) HasDeadletter() bool {
	return s.DeadletterTopic != ""
}

// NeedsProducerDLQ indica se lo spec richiede il *producer.Producer condiviso (non transazionale) per
// il DLQ. Solo la modalità handle lo usa: il transform instrada a DLQ dentro la propria sessione EOS.
func (s ConsumerSpec) NeedsProducerDLQ() bool {
	return s.Mode != ModeTransform && s.DeadletterTopic != ""
}

// Properties è una mappa di configurazione applicativa per-consumer, con getter tipizzati. È
// deliberatamente stringa→stringa per restare neutra e YAML-friendly.
type Properties map[string]string

// Has indica se la chiave è presente.
func (p Properties) Has(key string) bool { _, ok := p[key]; return ok }

// GetString ritorna il valore o def se assente.
func (p Properties) GetString(key, def string) string {
	if v, ok := p[key]; ok {
		return v
	}
	return def
}

// GetInt ritorna il valore intero o def se assente/non parsabile.
func (p Properties) GetInt(key string, def int) int {
	if v, ok := p[key]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// GetBool ritorna il valore booleano o def se assente/non parsabile.
func (p Properties) GetBool(key string, def bool) bool {
	if v, ok := p[key]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// GetDuration ritorna la durata (es. "5s") o def se assente/non parsabile.
func (p Properties) GetDuration(key string, def time.Duration) time.Duration {
	if v, ok := p[key]; ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

type ctxKey int

const (
	propertiesKey ctxKey = iota
	consumerNameKey
)

// ContextWithProperties arricchisce ctx con le Properties e il nome del consumer. L'engine lo chiama
// una volta per goroutine-consumer; la business logic (Handler/Transformer/Mapper) le legge da ctx.
func ContextWithProperties(ctx context.Context, name string, p Properties) context.Context {
	ctx = context.WithValue(ctx, propertiesKey, p)
	ctx = context.WithValue(ctx, consumerNameKey, name)
	return ctx
}

// PropertiesFromContext ritorna le Properties del consumer corrente (o una mappa vuota).
func PropertiesFromContext(ctx context.Context) Properties {
	if p, ok := ctx.Value(propertiesKey).(Properties); ok {
		return p
	}
	return Properties{}
}

// ConsumerNameFromContext ritorna il nome del consumer corrente (o "").
func ConsumerNameFromContext(ctx context.Context) string {
	if n, ok := ctx.Value(consumerNameKey).(string); ok {
		return n
	}
	return ""
}
