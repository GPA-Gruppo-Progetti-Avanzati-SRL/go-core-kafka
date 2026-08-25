package confluentdriver

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// consumerConfigMap traduce KafkaConfig + ConsumerSpec nella kafka.ConfigMap del consumer.
// enable.auto.commit è sempre false: il commit degli offset è manuale (dopo l'handle, o dentro la
// transazione EOS). read_committed per la modalità transform è impostato da NewTransactSession.
func consumerConfigMap(s spec.ConsumerSpec, k spec.KafkaServer) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		"bootstrap.servers":  k.BootstrapServers,
		"group.id":           s.GroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  s.AutoOffsetReset,
	}
	if s.SessionTimeoutMs > 0 {
		_ = cm.SetKey("session.timeout.ms", s.SessionTimeoutMs)
	}
	if s.FetchMinBytes > 0 {
		_ = cm.SetKey("fetch.min.bytes", s.FetchMinBytes)
	}
	if s.FetchMaxBytes > 0 {
		_ = cm.SetKey("fetch.max.bytes", s.FetchMaxBytes)
	}
	if s.MaxPollIntervalMs > 0 {
		_ = cm.SetKey("max.poll.interval.ms", s.MaxPollIntervalMs)
	}
	applySecurity(cm, k)
	return cm
}

// producerConfigMap traduce KafkaConfig nella kafka.ConfigMap del producer. Il producer è sempre
// idempotente; se transactionalID != "" diventa transazionale (EOS).
func producerConfigMap(transactionalID string, k spec.KafkaServer) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		"bootstrap.servers":  k.BootstrapServers,
		"enable.idempotence": true,
	}
	if transactionalID != "" {
		_ = cm.SetKey("transactional.id", transactionalID)
	}
	applySecurity(cm, k)
	return cm
}

// applySecurity mappa security-protocol / SASL / TLS sulle chiavi dotted di librdkafka.
func applySecurity(cm *kafka.ConfigMap, k spec.KafkaServer) {
	if k.SecurityProtocol != "" {
		_ = cm.SetKey("security.protocol", k.SecurityProtocol)
	}
	if k.SASL.Mechanisms != "" {
		_ = cm.SetKey("sasl.mechanism", k.SASL.Mechanisms)
		_ = cm.SetKey("sasl.username", k.SASL.Username)
		_ = cm.SetKey("sasl.password", k.SASL.Password)
	}
	if k.SSL.CaLocation != "" {
		_ = cm.SetKey("ssl.ca.location", k.SSL.CaLocation)
	}
	if k.SSL.SkipVerify {
		_ = cm.SetKey("enable.ssl.certificate.verification", false)
	}
}
