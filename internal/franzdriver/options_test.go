package franzdriver

import (
	"strings"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/twmb/franz-go/pkg/kgo"
)

func server() spec.KafkaServer {
	return spec.KafkaServer{BootstrapServers: "broker-1:9092,broker-2:9092"}
}

func processor(c spec.ConsumerTuning) spec.ProcessorSpec {
	return spec.ProcessorSpec{Name: "ingest", Topics: []string{"in"}, GroupID: "g", Consumer: c}
}

// Le opzioni di franz-go finiscono in una struct non esportata: la traccia è l'unico modo di
// verificare che un knob sia stato tradotto, ed è la stessa che serve all'avviso su una
// kafka-properties che sovrascrive un campo tipizzato.
func TestConsumerOpts_KnobTradotti(t *testing.T) {
	b, err := consumerOpts(processor(spec.ConsumerTuning{
		MaxBatchSize:                100,
		AutoOffsetReset:             "earliest",
		SessionTimeoutMs:            30000,
		HeartbeatIntervalMs:         3000,
		MaxPollIntervalMs:           300000,
		FetchMinBytes:               1024,
		FetchMaxBytes:               1048576,
		MaxPartitionFetchBytes:      262144,
		FetchWaitMaxMs:              500,
		PartitionAssignmentStrategy: "cooperative-sticky",
		IsolationLevel:              "read_committed",
	}), server())
	if err != nil {
		t.Fatalf("consumerOpts: %v", err)
	}

	want := map[string]string{
		"auto.offset.reset":             "earliest",
		"session.timeout.ms":            "30000",
		"heartbeat.interval.ms":         "3000",
		"max.poll.interval.ms":          "300000",
		"fetch.min.bytes":               "1024",
		"fetch.max.bytes":               "1048576",
		"max.partition.fetch.bytes":     "262144",
		"fetch.wait.max.ms":             "500",
		"partition.assignment.strategy": "cooperative-sticky",
		"isolation.level":               "read_committed",
	}
	for k, v := range want {
		if got := b.applied[k]; got != v {
			t.Errorf("%s = %q, atteso %q", k, got, v)
		}
	}
	// Il controllo finale è che il client ACCETTI il set completo: la traccia dice cosa abbiamo
	// tradotto, non che franz-go lo consideri una configurazione valida (NewClient non si connette,
	// valida e basta).
	cl, err := kgo.NewClient(b.opts...)
	if err != nil {
		t.Errorf("kgo.NewClient rifiuta le opzioni prodotte: %v", err)
	} else {
		cl.Close()
	}
}

// Un knob non valorizzato non deve produrre alcuna opzione: scrivere lo zero significherebbe imporlo
// al posto del default del client, che è una cosa diversa dal non averlo scritto.
func TestConsumerOpts_NonValorizzatoNonScrive(t *testing.T) {
	b, err := consumerOpts(processor(spec.ConsumerTuning{MaxBatchSize: 10}), server())
	if err != nil {
		t.Fatalf("consumerOpts: %v", err)
	}
	for _, k := range []string{"session.timeout.ms", "fetch.min.bytes", "auto.offset.reset", "isolation.level"} {
		if v, ok := b.applied[k]; ok {
			t.Errorf("%s = %q: un campo non valorizzato non va scritto", k, v)
		}
	}
}

// I knob che franz-go non sa esprimere non fermano l'avvio — il blocco tipizzato è il vocabolario
// della libreria, comune ai due driver — ma devono comparire nell'avviso, altrimenti chi li ha scritti
// crede che abbiano effetto.
func TestConsumerOpts_KnobNonSupportati(t *testing.T) {
	k := server()
	k.Debug = "cgrp,fetch"
	k.SocketKeepaliveEnable = true
	b, err := consumerOpts(processor(spec.ConsumerTuning{QueuedMaxMessagesKbytes: 65536}), k)
	if err != nil {
		t.Fatalf("consumerOpts: %v", err)
	}
	want := []string{"debug", "socket-keepalive-enable", "consumer.queued-max-messages-kbytes"}
	if len(b.unsupported) != len(want) {
		t.Fatalf("unsupported = %v, attesi %v", b.unsupported, want)
	}
	for i, w := range want {
		if b.unsupported[i] != w {
			t.Errorf("unsupported[%d] = %q, atteso %q", i, b.unsupported[i], w)
		}
	}
}

// Un enum sbagliato ferma l'avvio e il messaggio dice DI CHI è la sezione: senza owner, con dieci
// processor, il messaggio non porta a nessun file da correggere.
func TestConsumerOpts_ValoreNonValido(t *testing.T) {
	_, err := consumerOpts(processor(spec.ConsumerTuning{AutoOffsetReset: "earlist"}), server())
	if err == nil {
		t.Fatal("un auto-offset-reset non valido deve far fallire la costruzione")
	}
	if !strings.Contains(err.Error(), "processor ingest") || !strings.Contains(err.Error(), "earlist") {
		t.Errorf("errore = %q, atteso il nome del processor e il valore rifiutato", err)
	}
}

func TestProducerOpts_Transazionale(t *testing.T) {
	b, err := producerOpts("tx-ingest", "processor ingest", spec.ProducerTuning{
		Acks:                 "all",
		CompressionType:      "zstd",
		BatchSize:            65536,
		DeliveryTimeout:      30 * time.Second,
		TransactionTimeoutMs: 60000,
	}, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	want := map[string]string{
		"transactional.id":       "tx-ingest",
		"transaction.timeout.ms": "60000",
		"acks":                   "all",
		"compression.type":       "zstd",
		"batch.size":             "65536",
		"delivery.timeout.ms":    "30000",
	}
	for k, v := range want {
		if got := b.applied[k]; got != v {
			t.Errorf("%s = %q, atteso %q", k, got, v)
		}
	}
	// L'idempotenza è implicita nel producer transazionale: disattivarla sarebbe una contraddizione,
	// e franz-go rifiuterebbe il client.
	if _, ok := b.applied["enable.idempotence"]; ok {
		t.Error("enable.idempotence non va toccata su un producer transazionale")
	}
}

// enable-idempotence è un puntatore proprio per poter essere DISATTIVATA esplicitamente: è l'unico
// caso in cui il driver scrive qualcosa, perché attiva lo è già di default.
func TestProducerOpts_IdempotenzaDisattivata(t *testing.T) {
	no := false
	b, err := producerOpts("", "server.producer", spec.ProducerTuning{EnableIdempotence: &no}, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	if b.applied["enable.idempotence"] != "false" {
		t.Errorf("enable.idempotence = %q, atteso false", b.applied["enable.idempotence"])
	}

	b, err = producerOpts("", "server.producer", spec.ProducerTuning{}, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	if _, ok := b.applied["enable.idempotence"]; ok {
		t.Error("senza enable-idempotence esplicito non va scritto nulla: attiva è già il default")
	}
}

// linger-ms è un *int per distinguere "0 esplicito" (invia subito) da "assente": con un int semplice
// i due casi coincidono, ed è la ragione per cui non passa dal setter che scarta i valori non positivi.
func TestProducerOpts_LingerZeroEsplicito(t *testing.T) {
	zero := 0
	b, err := producerOpts("", "server.producer", spec.ProducerTuning{LingerMs: &zero}, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	if b.applied["linger.ms"] != "0" {
		t.Errorf("linger.ms = %q, atteso 0 (esplicito)", b.applied["linger.ms"])
	}
}

// Il TLS non degrada mai in silenzio: una CA illeggibile ferma l'avvio invece di far proseguire la
// connessione in chiaro.
func TestSecurity_TLSNonDegrada(t *testing.T) {
	k := server()
	k.SecurityProtocol = "SSL"
	k.SSL.CaLocation = "/percorso/che/non/esiste.pem"
	if _, err := producerOpts("", "server.producer", spec.ProducerTuning{}, k); err == nil {
		t.Fatal("una ca-location illeggibile deve far fallire la costruzione del client")
	}
}

// Un meccanismo SASL non riconosciuto è un errore e non un fallback su PLAIN: un downgrade silenzioso
// dell'autenticazione è l'ultimo posto in cui accettare una degradazione.
func TestSecurity_SASLSconosciuto(t *testing.T) {
	k := server()
	k.SecurityProtocol = "SASL_PLAINTEXT"
	k.SASL = spec.SaslCfg{Mechanisms: "GSSAPI", Username: "u", Password: "p"}
	_, err := producerOpts("", "server.producer", spec.ProducerTuning{}, k)
	if err == nil || !strings.Contains(err.Error(), "GSSAPI") {
		t.Fatalf("errore = %v, atteso il rifiuto esplicito del meccanismo", err)
	}
}

// I default della LIBRERIA devono raggiungere il client anche quando l'app non configura nulla: è
// l'unico modo perché lo stesso YAML abbia la stessa semantica sui due driver. Il test parte da uno
// spec RISOLTO, che è ciò che il driver riceve sempre in produzione — a differenza di
// TestConsumerOpts_NonValorizzatoNonScrive, che verifica il builder in isolamento.
func TestConsumerOpts_DefaultDellaLibreriaSempreApplicati(t *testing.T) {
	s := processor(spec.ConsumerTuning{}).Resolve(server())
	b, err := consumerOpts(s, server())
	if err != nil {
		t.Fatalf("consumerOpts: %v", err)
	}
	want := map[string]string{
		"isolation.level":               spec.DefaultIsolationLevel,
		"partition.assignment.strategy": spec.DefaultAssignmentStrategy,
		"max.poll.interval.ms":          "300000",
		"auto.offset.reset":             spec.DefaultAutoOffsetReset,
	}
	for k, v := range want {
		if got := b.applied[k]; got != v {
			t.Errorf("%s = %q, atteso %q: senza, franz-go ricadrebbe sul PROPRIO default (read_uncommitted, 60s) e divergerebbe da librdkafka", k, got, v)
		}
	}
}

func TestProducerOpts_DefaultDellaLibreriaSempreApplicati(t *testing.T) {
	p := spec.ProducerTuning{}.WithDefaults()
	b, err := producerOpts("", "server.producer", p, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	// franz-go negozia snappy di default: senza questo, lo stesso YAML scriveva sul topic record
	// compressi o no a seconda del driver.
	if got := b.applied["compression.type"]; got != spec.DefaultCompressionType {
		t.Errorf("compression.type = %q, atteso %q", got, spec.DefaultCompressionType)
	}
}

// L'avviso sui knob non supportati deve nominare ciò che l'UTENTE ha scritto. init-transactions-timeout
// è riempito sempre da WithDefaults, quindi confrontarlo con lo zero faceva comparire l'avviso a ogni
// avvio transazionale, per un valore che nessuno aveva configurato — e un avviso che compare sempre
// smette di essere letto.
func TestProducerOpts_InitTransactionsTimeoutAlDefaultNonAvvisa(t *testing.T) {
	p := spec.ProducerTuning{}.WithDefaults()
	b, err := producerOpts("tx-ingest", "processor ingest", p, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	for _, u := range b.unsupported {
		if u == "producer.init-transactions-timeout" {
			t.Fatal("avviso emesso per un valore che l'app non ha configurato (è il default della libreria)")
		}
	}

	// Un valore scritto a mano, invece, va segnalato: il driver non può onorarlo.
	p.InitTransactionsTimeout = 90 * time.Second
	b, err = producerOpts("tx-ingest", "processor ingest", p, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	var found bool
	for _, u := range b.unsupported {
		if u == "producer.init-transactions-timeout" {
			found = true
		}
	}
	if !found {
		t.Error("un init-transactions-timeout configurato a mano deve comparire fra i knob non supportati")
	}
}
