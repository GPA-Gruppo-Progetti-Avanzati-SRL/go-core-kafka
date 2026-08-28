package franzdriver

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// `kafka-properties` è l'escape hatch scritto nel vocabolario di librdkafka: chiavi dotted passate
// as-is al client. Sotto franz-go quelle chiavi non hanno un destinatario diretto, quindi qui c'è una
// TABELLA di traduzione verso le kgo.Opt equivalenti — e le chiavi non traducibili FANNO FALLIRE
// L'AVVIO.
//
// Fallire e non ignorare: chi scrive una proprietà si aspetta che abbia effetto, e una property che
// non ha destinatario è indistinguibile — a runtime — da una che non è stata scritta. È la stessa
// ragione per cui i tag `validate:` sugli enum fermano l'avvio invece di lasciar degradare un refuso.
//
// Nota la differenza con i campi TIPIZZATI senza equivalente (vedi optBuilder.markUnsupported): quelli
// sono il vocabolario della libreria, comune ai due driver, e un knob non esprimibile è un limite
// documentato del driver — lì un avviso è la risposta giusta.

// translators traduce una chiave dotted di librdkafka nella kgo.Opt corrispondente. I convertitori dei
// valori enumerati sono gli STESSI usati dai campi tipizzati (options.go): un valore ammesso da un
// campo e rifiutato dall'escape hatch sarebbe una seconda semantica da tenere allineata.
var translators = map[string]func(string) (kgo.Opt, error){
	// connessione
	"client.id":               func(v string) (kgo.Opt, error) { return kgo.ClientID(v), nil },
	"metadata.max.age.ms":     msOpt(kgo.MetadataMaxAge),
	"connections.max.idle.ms": msOpt(kgo.ConnIdleTimeout),

	// consumer
	"session.timeout.ms":            msOpt(kgo.SessionTimeout),
	"heartbeat.interval.ms":         msOpt(kgo.HeartbeatInterval),
	"max.poll.interval.ms":          msOpt(kgo.RebalanceTimeout),
	"fetch.min.bytes":               bytesOpt(kgo.FetchMinBytes),
	"fetch.max.bytes":               bytesOpt(kgo.FetchMaxBytes),
	"max.partition.fetch.bytes":     bytesOpt(kgo.FetchMaxPartitionBytes),
	"fetch.wait.max.ms":             msOpt(kgo.FetchMaxWait),
	"auto.offset.reset":             autoOffsetResetOpt,
	"partition.assignment.strategy": balancerOpt,

	// producer
	"acks":             acksOpt,
	"compression.type": compressionOpt,
	"linger.ms": func(v string) (kgo.Opt, error) {
		// linger.ms: 0 è un valore legittimo (invia subito), quindi non passa da msOpt che scarta i
		// valori non positivi.
		ms, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || ms < 0 {
			return nil, fmt.Errorf("valore %q non valido (millisecondi >= 0)", v)
		}
		return kgo.ProducerLinger(time.Duration(ms) * time.Millisecond), nil
	},
	"batch.size":                            bytesOpt(kgo.ProducerBatchMaxBytes),
	"message.send.max.retries":              countOpt(kgo.RecordRetries),
	"retries":                               countOpt(kgo.RecordRetries),
	"max.in.flight.requests.per.connection": countOpt(kgo.MaxProduceRequestsInflightPerBroker),
	"request.timeout.ms":                    msOpt(kgo.ProduceRequestTimeout),
	"delivery.timeout.ms":                   msOpt(kgo.RecordDeliveryTimeout),
	"transaction.timeout.ms":                msOpt(kgo.TransactionTimeout),
	"retry.backoff.ms": func(v string) (kgo.Opt, error) {
		d, err := parseMs(v)
		if err != nil {
			return nil, err
		}
		return kgo.RetryBackoffFn(func(int) time.Duration { return d }), nil
	},
	"enable.idempotence": func(v string) (kgo.Opt, error) {
		on, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("valore %q non valido (true/false)", v)
		}
		if on {
			// È già il default di franz-go: non esiste (né serve) un'opzione per riattivarla.
			return nil, nil
		}
		return kgo.DisableIdempotentWrite(), nil
	},
}

// kafkaProperties traduce e applica un blocco `kafka-properties`. È l'ULTIMA scrittura, quindi vince
// sui campi tipizzati (in franz-go l'ultima opzione applicata sovrascrive la precedente): è l'escape
// hatch, non un default. Una sovrascrittura è loggata a Warn perché lo stesso valore configurato in
// due posti è quasi sempre un residuo, non un'intenzione.
//
// La normalizzazione (lowercase + trim) è quella di spec, la stessa usata da ValidateKafkaProperties:
// se divergessero, una chiave scritta " Acks " passerebbe il controllo delle chiavi riservate e
// verrebbe poi applicata comunque.
func (b *optBuilder) kafkaProperties(owner string, props map[string]string) error {
	keys, normalized := spec.NormalizeKafkaProperties(props)
	var unknown []string
	for _, key := range keys {
		mk, ok := translators[key]
		if !ok {
			unknown = append(unknown, key)
			continue
		}
		value := normalized[key]
		opt, err := mk(value)
		if err != nil {
			return fmt.Errorf("%s: kafka-properties %q: %w", owner, key, err)
		}
		if prev, dup := b.applied[key]; dup {
			log.Warn().Str("owner", owner).Str("property", key).
				Str("overridden", prev).Str("value", value).
				Msg("corekafka: kafka-properties sovrascrive una proprietà già impostata dalla config tipizzata")
		}
		b.set(key, value, opt)
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("%s: kafka-properties %s non traducibili nel driver franz-go: sono chiavi di librdkafka e non hanno un'opzione equivalente. "+
			"Usare i campi tipizzati dei blocchi consumer/producer, rimuoverle, oppure scegliere il driver confluent (corekafka.WithDriver(confluentdriver.Driver))",
			owner, quoteAll(unknown))
	}
	return nil
}

func quoteAll(keys []string) string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, strconv.Quote(k))
	}
	return strings.Join(out, ", ")
}

// --- helper della tabella -----------------------------------------------------------------------

func parseMs(v string) (time.Duration, error) {
	ms, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || ms <= 0 {
		return 0, fmt.Errorf("valore %q non valido (millisecondi > 0)", v)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func msOpt[O kgo.Opt](mk func(time.Duration) O) func(string) (kgo.Opt, error) {
	return func(v string) (kgo.Opt, error) {
		d, err := parseMs(v)
		if err != nil {
			return nil, err
		}
		return mk(d), nil
	}
}

func bytesOpt[O kgo.Opt](mk func(int32) O) func(string) (kgo.Opt, error) {
	return func(v string) (kgo.Opt, error) {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("valore %q non valido (byte > 0)", v)
		}
		return mk(int32(n)), nil
	}
}

func countOpt[O kgo.Opt](mk func(int) O) func(string) (kgo.Opt, error) {
	return func(v string) (kgo.Opt, error) {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n < 0 {
			return nil, fmt.Errorf("valore %q non valido (intero >= 0)", v)
		}
		return mk(n), nil
	}
}
