package confluentdriver

import (
	"sort"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

// Regola di tutto questo file: un knob NON valorizzato non viene scritto nella ConfigMap, così resta
// il default di librdkafka. Scrivere lo zero al posto di omettere la chiave significherebbe imporre
// "0" a proprietà dove zero è un valore legittimo e molto diverso dal default.
//
// Sugli errori: `set` esiste perché kafka.ConfigMap.SetKey ritorna un error che è STRUTTURALMENTE
// sempre nil — è un assegnamento di mappa — e tredici `set(cm, ...)` sparsi nel file lasciavano
// intendere che qui si stesse ingoiando qualcosa. Non è così: una chiave sconosciuta o un valore non
// accettato sono rifiutati da librdkafka al momento della costruzione del client, dove
// kafka.NewConsumer/NewProducer ritornano un ErrInvalidArg che cita la proprietà per nome. wrap lo
// classifica SeverityPermanent, quindi il processo esce — che è il fail-fast voluto, e avviene già.

// consumerConfigMap traduce KafkaServer + ProcessorSpec nella kafka.ConfigMap del consumer. Lo spec
// arriva GIÀ RISOLTO (ProcessorSpec.Resolve): il tuning sta in s.Consumer e qui non si conosce
// l'eredità. enable.auto.commit è sempre false: il commit degli offset è manuale (dopo l'handle, o
// dentro la transazione EOS) e non è configurabile — vedi spec.DeniedKafkaProperties.
func consumerConfigMap(s spec.ProcessorSpec, k spec.KafkaServer) *kafka.ConfigMap {
	c := s.Consumer
	cm := &kafka.ConfigMap{
		"bootstrap.servers":  k.BootstrapServers,
		"group.id":           s.GroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  c.AutoOffsetReset,
	}
	typed := map[string]any{
		"session.timeout.ms":            c.SessionTimeoutMs,
		"heartbeat.interval.ms":         c.HeartbeatIntervalMs,
		"fetch.min.bytes":               c.FetchMinBytes,
		"fetch.max.bytes":               c.FetchMaxBytes,
		"fetch.wait.max.ms":             c.FetchWaitMaxMs,
		"max.partition.fetch.bytes":     c.MaxPartitionFetchBytes,
		"queued.max.messages.kbytes":    c.QueuedMaxMessagesKbytes,
		"max.poll.interval.ms":          c.MaxPollIntervalMs,
		"partition.assignment.strategy": c.PartitionAssignmentStrategy,
		"isolation.level":               c.IsolationLevel,
	}
	setIfSet(cm, typed)
	applyCommon(cm, k)
	applySecurity(cm, k)
	// Escape hatch: prima quello della connessione (comune a consumer e producer), poi quello del
	// processor — già fuso con `server.consumer` da ConsumerTuning.inherit — che è il più specifico
	// e quindi l'ultimo a scrivere.
	applyKafkaProperties(cm, "server", k.KafkaProperties)
	applyKafkaProperties(cm, "processor "+s.Name, c.KafkaProperties)
	return cm
}

// producerConfigMap traduce KafkaServer + ProducerTuning nella kafka.ConfigMap del producer. owner
// identifica la sezione nei log (`server.producer` per quello condiviso, `processor <nome>` per il
// transazionale di un transform). Se transactionalID != "" il producer è transazionale (EOS) e
// l'idempotenza è implicita.
func producerConfigMap(transactionalID, owner string, p spec.ProducerTuning, k spec.KafkaServer) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{"bootstrap.servers": k.BootstrapServers}

	if transactionalID != "" {
		set(cm, "transactional.id", transactionalID)
		if p.TransactionTimeoutMs > 0 {
			set(cm, "transaction.timeout.ms", p.TransactionTimeoutMs)
		}
	} else {
		// enable.idempotence forza acks=all e il retry ordinato: è il default storico di
		// go-core-kafka, disattivabile solo esplicitamente.
		set(cm, "enable.idempotence", p.Idempotent())
	}

	typed := map[string]any{
		"acks":                                  p.Acks,
		"compression.type":                      p.CompressionType,
		"batch.size":                            p.BatchSize,
		"batch.num.messages":                    p.BatchNumMessages,
		"message.max.bytes":                     p.MessageMaxBytes,
		"message.send.max.retries":              p.MessageSendMaxRetries,
		"max.in.flight.requests.per.connection": p.MaxInFlight,
		"request.timeout.ms":                    p.RequestTimeoutMs,
		"metadata.max.idle.ms":                  p.MetadataMaxIdleMs,
		"retry.backoff.ms":                      int(p.RetryBackoff.Milliseconds()),
		"delivery.timeout.ms":                   int(p.DeliveryTimeout.Milliseconds()),
	}
	setIfSet(cm, typed)
	// linger.ms è un *int proprio per poter impostare 0 (invia subito): setIfSet lo scarterebbe.
	if p.LingerMs != nil {
		set(cm, "linger.ms", *p.LingerMs)
	}

	applyCommon(cm, k)
	applySecurity(cm, k)
	applyKafkaProperties(cm, "server", k.KafkaProperties)
	applyKafkaProperties(cm, owner, p.KafkaProperties)
	return cm
}

// applyCommon imposta le proprietà di connessione comuni a consumer e producer.
func applyCommon(cm *kafka.ConfigMap, k spec.KafkaServer) {
	setIfSet(cm, map[string]any{
		"client.id":               k.ClientID,
		"debug":                   k.Debug,
		"metadata.max.age.ms":     k.MetadataMaxAgeMs,
		"connections.max.idle.ms": k.ConnectionsMaxIdleMs,
	})
	// socket.keepalive.enable si imposta solo se true: false è già il default.
	if k.SocketKeepaliveEnable {
		set(cm, "socket.keepalive.enable", true)
	}
}

// applySecurity mappa security-protocol / SASL / TLS sulle chiavi dotted di librdkafka.
func applySecurity(cm *kafka.ConfigMap, k spec.KafkaServer) {
	if k.SecurityProtocol != "" {
		set(cm, "security.protocol", k.SecurityProtocol)
	}
	if k.SASL.Mechanisms != "" {
		set(cm, "sasl.mechanism", k.SASL.Mechanisms)
		set(cm, "sasl.username", k.SASL.Username)
		set(cm, "sasl.password", k.SASL.Password)
	}
	setIfSet(cm, map[string]any{
		"ssl.ca.location": k.SSL.CaLocation,
		// mTLS: il broker autentica anche il client tramite questo certificato.
		"ssl.certificate.location": k.SSL.CertificateLocation,
		"ssl.key.location":         k.SSL.KeyLocation,
		"ssl.key.password":         k.SSL.KeyPassword,
	})
	if k.SSL.SkipVerify {
		set(cm, "enable.ssl.certificate.verification", false)
	}
}

// applyKafkaProperties riversa nella ConfigMap le chiavi dotted date as-is dalla config. È l'ULTIMA
// scrittura, quindi vince sui campi tipizzati: è l'escape hatch, non un default. Una sovrascrittura è
// loggata a Warn perché avere lo stesso valore configurato in due posti (il campo tipizzato e la
// mappa raw) è quasi sempre un residuo, non un'intenzione.
//
// Le chiavi riservate non arrivano qui: sono rifiutate al boot da spec.ValidateKafkaProperties. La
// normalizzazione (lowercase + trim) è quella di spec, la stessa usata dal controllo: se divergessero,
// una chiave scritta " Group.ID " passerebbe il controllo e verrebbe poi applicata comunque.
func applyKafkaProperties(cm *kafka.ConfigMap, owner string, props map[string]string) {
	keys, normalized := spec.NormalizeKafkaProperties(props)
	for _, key := range keys {
		if prev, err := cm.Get(key, nil); err == nil && prev != nil {
			log.Warn().Str("owner", owner).Str("property", key).
				Interface("overridden", prev).Str("value", normalized[key]).
				Msg("corekafka: kafka-properties sovrascrive una proprietà già impostata dalla config tipizzata")
		}
		set(cm, key, normalized[key])
	}
}

// set scrive una chiave. Ignora l'error di SetKey perché non ne esiste uno: vedi il commento in testa
// al file per dove avviene il rifiuto reale.
func set(cm *kafka.ConfigMap, key string, value kafka.ConfigValue) {
	_ = cm.SetKey(key, value)
}

// setIfSet scrive solo le chiavi con un valore non-zero: stringa non vuota, int > 0. Uno zero
// significa "campo non valorizzato" e va lasciato al default di librdkafka (vedi il commento in
// testa al file).
func setIfSet(cm *kafka.ConfigMap, values map[string]any) {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		switch v := values[k].(type) {
		case string:
			if v != "" {
				set(cm, k, v)
			}
		case int:
			if v > 0 {
				set(cm, k, v)
			}
		}
	}
}
