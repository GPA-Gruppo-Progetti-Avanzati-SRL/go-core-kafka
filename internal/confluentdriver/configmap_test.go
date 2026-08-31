package confluentdriver

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// get ritorna il valore di una chiave e se è presente. cm.Get con default nil distingue i due casi:
// una chiave assente torna nil, ed è esattamente la condizione che i test devono poter asserire
// (assente = default di librdkafka, non zero imposto).
func get(t *testing.T, cm *kafka.ConfigMap, key string) (kafka.ConfigValue, bool) {
	t.Helper()
	v, err := cm.Get(key, nil)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v, v != nil
}

func mustEqual(t *testing.T, cm *kafka.ConfigMap, key string, want any) {
	t.Helper()
	got, ok := get(t, cm, key)
	if !ok {
		t.Fatalf("chiave %q assente, atteso %v", key, want)
	}
	if got != want {
		t.Errorf("%s = %v (%T), atteso %v (%T)", key, got, got, want, want)
	}
}

func mustAbsent(t *testing.T, cm *kafka.ConfigMap, key string) {
	t.Helper()
	if v, ok := get(t, cm, key); ok {
		t.Errorf("chiave %q presente con %v, attesa assente (default librdkafka)", key, v)
	}
}

func TestConsumerConfigMap_Minimale(t *testing.T) {
	s := spec.ProcessorSpec{Name: "ingest", GroupID: "g", Topics: []string{"t"}}.Resolve(spec.KafkaServer{})
	cm := consumerConfigMap(s, spec.KafkaServer{BootstrapServers: "b:9092"})

	mustEqual(t, cm, "bootstrap.servers", "b:9092")
	mustEqual(t, cm, "group.id", "g")
	mustEqual(t, cm, "auto.offset.reset", "earliest")
	// Invariante dell'engine: il commit è manuale, sempre.
	mustEqual(t, cm, "enable.auto.commit", false)

	// Default della LIBRERIA: questi tre sono scritti anche senza che l'app li configuri, perché è
	// su di essi che librdkafka e franz-go NON concordano (franz-go: read_uncommitted,
	// cooperative-sticky, rebalance timeout 60s). Lasciarli al default del client significava che
	// lo stesso YAML cambiava semantica cambiando driver.
	mustEqual(t, cm, "isolation.level", "read_committed")
	mustEqual(t, cm, "partition.assignment.strategy", "cooperative-sticky")
	mustEqual(t, cm, "max.poll.interval.ms", 300000)

	// Gli altri knob non valorizzati NON devono comparire: scriverne lo zero imporrebbe "0" dove
	// zero ha un significato diverso dal default.
	for _, k := range []string{
		"session.timeout.ms", "heartbeat.interval.ms", "fetch.min.bytes", "fetch.max.bytes",
		"fetch.wait.max.ms", "max.partition.fetch.bytes", "queued.max.messages.kbytes",
		"client.id", "debug", "metadata.max.age.ms", "connections.max.idle.ms",
		"socket.keepalive.enable", "security.protocol", "sasl.mechanism", "ssl.ca.location",
	} {
		mustAbsent(t, cm, k)
	}
}

func TestConsumerConfigMap_TuttiIKnob(t *testing.T) {
	s := spec.ProcessorSpec{
		Name: "ingest", GroupID: "g", Topics: []string{"t"},
		Consumer: spec.ConsumerTuning{
			AutoOffsetReset:             "latest",
			SessionTimeoutMs:            10000,
			HeartbeatIntervalMs:         3000,
			FetchMinBytes:               1024,
			FetchMaxBytes:               52428800,
			FetchWaitMaxMs:              500,
			MaxPartitionFetchBytes:      1048576,
			QueuedMaxMessagesKbytes:     65536,
			MaxPollIntervalMs:           300000,
			PartitionAssignmentStrategy: "cooperative-sticky",
			IsolationLevel:              "read_committed",
		},
	}
	k := spec.KafkaServer{
		BootstrapServers:      "b:9092",
		ClientID:              "svc",
		Debug:                 "cgrp,fetch",
		MetadataMaxAgeMs:      180000,
		SocketKeepaliveEnable: true,
		ConnectionsMaxIdleMs:  540000,
	}
	cm := consumerConfigMap(s, k)

	mustEqual(t, cm, "auto.offset.reset", "latest")
	mustEqual(t, cm, "session.timeout.ms", 10000)
	mustEqual(t, cm, "heartbeat.interval.ms", 3000)
	mustEqual(t, cm, "fetch.min.bytes", 1024)
	mustEqual(t, cm, "fetch.max.bytes", 52428800)
	mustEqual(t, cm, "fetch.wait.max.ms", 500)
	mustEqual(t, cm, "max.partition.fetch.bytes", 1048576)
	mustEqual(t, cm, "queued.max.messages.kbytes", 65536)
	mustEqual(t, cm, "max.poll.interval.ms", 300000)
	mustEqual(t, cm, "partition.assignment.strategy", "cooperative-sticky")
	mustEqual(t, cm, "isolation.level", "read_committed")
	mustEqual(t, cm, "client.id", "svc")
	mustEqual(t, cm, "debug", "cgrp,fetch")
	mustEqual(t, cm, "metadata.max.age.ms", 180000)
	mustEqual(t, cm, "socket.keepalive.enable", true)
	mustEqual(t, cm, "connections.max.idle.ms", 540000)
}

func TestApplySecurity(t *testing.T) {
	t.Run("SASL_SSL con CA", func(t *testing.T) {
		cm := consumerConfigMap(spec.ProcessorSpec{Name: "c", GroupID: "g"}, spec.KafkaServer{
			BootstrapServers: "b:9092",
			SecurityProtocol: "SASL_SSL",
			SASL:             spec.SaslCfg{Mechanisms: "SCRAM-SHA-512", Username: "u", Password: "p"},
			SSL:              spec.SSLCfg{CaLocation: "/ca.pem"},
		})
		mustEqual(t, cm, "security.protocol", "SASL_SSL")
		mustEqual(t, cm, "sasl.mechanism", "SCRAM-SHA-512")
		mustEqual(t, cm, "sasl.username", "u")
		mustEqual(t, cm, "sasl.password", "p")
		mustEqual(t, cm, "ssl.ca.location", "/ca.pem")
		mustAbsent(t, cm, "enable.ssl.certificate.verification")
	})

	t.Run("mTLS", func(t *testing.T) {
		cm := consumerConfigMap(spec.ProcessorSpec{Name: "c", GroupID: "g"}, spec.KafkaServer{
			BootstrapServers: "b:9092",
			SecurityProtocol: "SSL",
			SSL: spec.SSLCfg{
				CaLocation:          "/ca.pem",
				CertificateLocation: "/client.pem",
				KeyLocation:         "/client.key",
				KeyPassword:         "segreto",
			},
		})
		mustEqual(t, cm, "ssl.certificate.location", "/client.pem")
		mustEqual(t, cm, "ssl.key.location", "/client.key")
		mustEqual(t, cm, "ssl.key.password", "segreto")
	})

	t.Run("skip-verify", func(t *testing.T) {
		cm := consumerConfigMap(spec.ProcessorSpec{Name: "c", GroupID: "g"}, spec.KafkaServer{
			BootstrapServers: "b:9092", SSL: spec.SSLCfg{SkipVerify: true},
		})
		mustEqual(t, cm, "enable.ssl.certificate.verification", false)
	})
}

func TestKafkaProperties_VincolanoSuiTipizzati(t *testing.T) {
	// L'escape hatch è l'ultima scrittura: deve vincere sul campo tipizzato, altrimenti non
	// permetterebbe di correggere un valore già coperto dalla struct.
	s := spec.ProcessorSpec{
		Name: "ingest", GroupID: "g",
		Consumer: spec.ConsumerTuning{
			SessionTimeoutMs: 10000,
			KafkaProperties:  map[string]string{"session.timeout.ms": "45000"},
		},
	}
	cm := consumerConfigMap(s, spec.KafkaServer{BootstrapServers: "b:9092"})
	mustEqual(t, cm, "session.timeout.ms", "45000")
}

func TestKafkaProperties_ConsumerVinceSulServer(t *testing.T) {
	// Il più specifico vince: la mappa del consumer è applicata dopo quella del server.
	s := spec.ProcessorSpec{Name: "ingest", GroupID: "g", Consumer: spec.ConsumerTuning{KafkaProperties: map[string]string{"debug": "cgrp"}}}
	k := spec.KafkaServer{BootstrapServers: "b:9092", KafkaProperties: map[string]string{"debug": "broker"}}
	cm := consumerConfigMap(s, k)
	mustEqual(t, cm, "debug", "cgrp")
}

func TestKafkaProperties_ChiaviNormalizzate(t *testing.T) {
	s := spec.ProcessorSpec{Name: "ingest", GroupID: "g", Consumer: spec.ConsumerTuning{KafkaProperties: map[string]string{" Socket.Keepalive.Enable ": "true"}}}
	cm := consumerConfigMap(s, spec.KafkaServer{BootstrapServers: "b:9092"})
	mustEqual(t, cm, "socket.keepalive.enable", "true")
}

func TestProducerConfigMap_NonTransazionale(t *testing.T) {
	p := spec.ProducerTuning{}.WithDefaults()
	cm := producerConfigMap("", "server.producer", p, spec.KafkaServer{BootstrapServers: "b:9092"})

	mustEqual(t, cm, "bootstrap.servers", "b:9092")
	// Default storico: il producer è idempotente se non lo si disattiva esplicitamente.
	mustEqual(t, cm, "enable.idempotence", true)
	mustAbsent(t, cm, "transactional.id")
	for _, k := range []string{"acks", "linger.ms", "batch.size"} {
		mustAbsent(t, cm, k)
	}
	// compression.type è scritto anche senza configurazione: franz-go offre snappy di default,
	// quindi senza un default della libreria lo stesso YAML avrebbe scritto sul topic record
	// compressi o no a seconda del driver.
	mustEqual(t, cm, "compression.type", "none")
	// transaction.timeout.ms ha un default della libreria (i due client divergono, 60s vs 40s) ma
	// resta fuori dal producer NON transazionale: là non ha destinatario.
	mustAbsent(t, cm, "transaction.timeout.ms")
	// delivery.timeout.ms è l'ECCEZIONE alla regola "un knob non valorizzato resta al default di
	// librdkafka": WithDefaults lo riempie, perché è ciò che garantisce che un delivery report arrivi
	// — chi lo attende non deve poter restare appeso per una config incompleta.
	mustEqual(t, cm, "delivery.timeout.ms", int(spec.DefaultDeliveryTimeout.Milliseconds()))
}

func TestProducerConfigMap_IdempotenzaDisattivabile(t *testing.T) {
	no := false
	cm := producerConfigMap("", "server.producer", spec.ProducerTuning{EnableIdempotence: &no}, spec.KafkaServer{BootstrapServers: "b:9092"})
	mustEqual(t, cm, "enable.idempotence", false)
}

func TestProducerConfigMap_Transazionale(t *testing.T) {
	cm := producerConfigMap("tx-1", "processor t", spec.ProducerTuning{TransactionTimeoutMs: 100000}, spec.KafkaServer{BootstrapServers: "b:9092"})

	mustEqual(t, cm, "transactional.id", "tx-1")
	mustEqual(t, cm, "transaction.timeout.ms", 100000)
	// Con transactional.id l'idempotenza è implicita: impostarla esplicitamente è ridondante e
	// librdkafka la considererebbe un conflitto se qualcuno la mettesse a false.
	mustAbsent(t, cm, "enable.idempotence")
}

func TestProducerConfigMap_TuttiIKnob(t *testing.T) {
	linger := 20
	p := spec.ProducerTuning{
		Acks:                  "all",
		CompressionType:       "snappy",
		LingerMs:              &linger,
		BatchSize:             65536,
		BatchNumMessages:      10000,
		MessageMaxBytes:       1048576,
		MessageSendMaxRetries: 5,
		MaxInFlight:           5,
		RetryBackoff:          250 * time.Millisecond,
		DeliveryTimeout:       2 * time.Minute,
		RequestTimeoutMs:      60000,
		MetadataMaxIdleMs:     300000,
	}
	cm := producerConfigMap("", "server.producer", p, spec.KafkaServer{BootstrapServers: "b:9092"})

	mustEqual(t, cm, "acks", "all")
	mustEqual(t, cm, "compression.type", "snappy")
	mustEqual(t, cm, "linger.ms", 20)
	mustEqual(t, cm, "batch.size", 65536)
	mustEqual(t, cm, "batch.num.messages", 10000)
	mustEqual(t, cm, "message.max.bytes", 1048576)
	mustEqual(t, cm, "message.send.max.retries", 5)
	mustEqual(t, cm, "max.in.flight.requests.per.connection", 5)
	mustEqual(t, cm, "retry.backoff.ms", 250)
	mustEqual(t, cm, "delivery.timeout.ms", 120000)
	mustEqual(t, cm, "request.timeout.ms", 60000)
	mustEqual(t, cm, "metadata.max.idle.ms", 300000)
}

func TestProducerConfigMap_LingerZeroEsplicito(t *testing.T) {
	// linger.ms è un *int proprio per questo: 0 significa "invia subito" ed è diverso dal default.
	zero := 0
	cm := producerConfigMap("", "server.producer", spec.ProducerTuning{LingerMs: &zero}, spec.KafkaServer{BootstrapServers: "b:9092"})
	mustEqual(t, cm, "linger.ms", 0)

	cm = producerConfigMap("", "server.producer", spec.ProducerTuning{}, spec.KafkaServer{BootstrapServers: "b:9092"})
	mustAbsent(t, cm, "linger.ms")
}

// Il dump di configurazione al boot stampa la ConfigMap: questo test lega il percorso reale
// (applySecurity riempie la mappa → FormatConfig la formatta) e verifica che la password non ci
// arrivi in chiaro. È l'unico modo per cui una diagnostica utile non diventi una fuga di credenziali.
func TestConfigMap_DumpNonEsponeLeCredenziali(t *testing.T) {
	k := spec.KafkaServer{
		BootstrapServers: "b:9092",
		SecurityProtocol: "SASL_SSL",
		SASL:             spec.SaslCfg{Mechanisms: "PLAIN", Username: "gpa-app", Password: "s3gr3t0"},
		SSL:              spec.SSLCfg{KeyLocation: "/etc/certs/client.key", KeyPassword: "chiave-s3gr3ta"},
	}
	s := spec.ProcessorSpec{Name: "ingest", GroupID: "g", Topics: []string{"t"}}.Resolve(k)

	m := make(map[string]string)
	for key, v := range *consumerConfigMap(s, k) {
		m[key] = fmt.Sprint(v)
	}
	joined := strings.Join(driver.FormatConfig(m), "\n")

	for _, segreto := range []string{"s3gr3t0", "chiave-s3gr3ta"} {
		if strings.Contains(joined, segreto) {
			t.Errorf("credenziale %q in chiaro nel dump:\n%s", segreto, joined)
		}
	}
	// Ciò che serve a diagnosticare deve invece esserci: username, path della chiave, protocollo.
	for _, atteso := range []string{"gpa-app", "/etc/certs/client.key", "SASL_SSL", "b:9092"} {
		if !strings.Contains(joined, atteso) {
			t.Errorf("%q manca dal dump, ma non è un segreto:\n%s", atteso, joined)
		}
	}
}
