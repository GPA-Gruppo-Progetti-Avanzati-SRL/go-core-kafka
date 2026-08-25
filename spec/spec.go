// Package spec contiene la configurazione neutra di go-core-kafka: la connessione condivisa
// (KafkaConfig) e la specifica per-consumer (ConsumerSpec). Vive in un package foglia perché sia
// l'astrazione driver (che ne ha bisogno per costruire i client) sia l'engine sia il Producer la
// referenziano: tenerla qui evita cicli di import e la mantiene priva di tipi del client concreto.
package spec

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// La modalità (handle vs transform) NON è in config: è DERIVATA dalla registrazione — RegisterHandler
// mette il processore nel gruppo kafka_handlers, RegisterTransformer in kafka_transformers. L'engine
// deriva il tipo dal gruppo di appartenenza (unica fonte di verità, niente mismatch con la config).

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
type KafkaServer struct {
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
	Name string `yaml:"name" mapstructure:"name" json:"name" validate:"required"`
	// Disabled=true: il consumer NON viene attivato (utile per spegnere un consumer senza rimuoverlo
	// dalla config). È comunque la presenza in questa lista `consumers` a comandare l'attivazione:
	// un processore registrato ma non presente qui non viene istanziato.
	Disabled bool     `yaml:"disabled" mapstructure:"disabled" json:"disabled"`
	Topics   []string `yaml:"topics" mapstructure:"topics" json:"topics" validate:"required,min=1"`
	GroupID  string   `yaml:"group-id" mapstructure:"group-id" json:"group-id" validate:"required"`
	// NB: nessun campo "mode" — la modalità è derivata da RegisterHandler/RegisterTransformer.

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

	// Properties applicative del consumer. Il modo raccomandato per leggerle è il mapping sui campi
	// della struct dell'Handler/Transformer via tag `prop:` (fatto al wiring, con default e validazione
	// per campo: vedi core.BindProps); restano leggibili a runtime dal context o all'avvio tramite
	// l'interfaccia Configurable. È lo stesso tipo usato dai task di go-core-batch.
	Properties core.Properties `yaml:"properties" mapstructure:"properties" json:"properties"`
}

// WithDefaults ritorna una copia dello spec con i default applicati ai campi non valorizzati.
func (s ConsumerSpec) WithDefaults() ConsumerSpec {
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

// HasDeadletter indica se è configurato un topic DLQ (abilita il deadletter, sia da policy di default
// sia da scelta dell'handler/transformer a runtime).
func (s ConsumerSpec) HasDeadletter() bool {
	return s.DeadletterTopic != ""
}

type ctxKey int

const (
	propertiesKey ctxKey = iota
	consumerNameKey
)

// ContextWithProperties arricchisce ctx con le Properties e il nome del consumer. L'engine lo chiama
// una volta per goroutine-consumer; la business logic (Handler/Transformer/Mapper) le legge da ctx.
func ContextWithProperties(ctx context.Context, name string, p core.Properties) context.Context {
	ctx = context.WithValue(ctx, propertiesKey, p)
	ctx = context.WithValue(ctx, consumerNameKey, name)
	return ctx
}

// PropertiesFromContext ritorna le Properties del consumer corrente (o una mappa vuota).
func PropertiesFromContext(ctx context.Context) core.Properties {
	if p, ok := ctx.Value(propertiesKey).(core.Properties); ok {
		return p
	}
	return core.Properties{}
}

// ConsumerNameFromContext ritorna il nome del consumer corrente (o "").
func ConsumerNameFromContext(ctx context.Context) string {
	if n, ok := ctx.Value(consumerNameKey).(string); ok {
		return n
	}
	return ""
}
