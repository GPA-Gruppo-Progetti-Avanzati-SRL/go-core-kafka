package spec

import (
	"reflect"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

func boolPtr(b bool) *bool    { return &b }
func intPtr(i int) *int       { return &i }
func strPtr(s string) *string { return &s }

// validSpec è uno ProcessorSpec che supera la validazione: i test dei singoli campi partono da qui e
// ne guastano uno solo, così un fallimento identifica il campo e non l'insieme.
func validSpec() ProcessorSpec {
	return ProcessorSpec{Name: "ingest", Topics: []string{"t"}, GroupID: "g"}
}

// --- default ------------------------------------------------------------------------------------

func TestResolve_DefaultDellaLibreria(t *testing.T) {
	got := ProcessorSpec{}.Resolve(KafkaServer{})

	if got.Consumer.MaxBatchSize != DefaultMaxBatchSize {
		t.Errorf("MaxBatchSize = %d, atteso %d", got.Consumer.MaxBatchSize, DefaultMaxBatchSize)
	}
	if got.Consumer.CutFrequency != DefaultCutFrequency {
		t.Errorf("CutFrequency = %v, atteso %v", got.Consumer.CutFrequency, DefaultCutFrequency)
	}
	if got.Consumer.PollTimeout != DefaultPollTimeout {
		t.Errorf("PollTimeout = %v, atteso %v", got.Consumer.PollTimeout, DefaultPollTimeout)
	}
	if got.Consumer.AutoOffsetReset != DefaultAutoOffsetReset {
		t.Errorf("AutoOffsetReset = %q, atteso %q", got.Consumer.AutoOffsetReset, DefaultAutoOffsetReset)
	}
	if got.Consumer.OnError != OnErrorFailFast {
		t.Errorf("OnError = %q, atteso %q", got.Consumer.OnError, OnErrorFailFast)
	}
	if got.Producer.FlushTimeout != DefaultFlushTimeout {
		t.Errorf("FlushTimeout = %v, atteso %v", got.Producer.FlushTimeout, DefaultFlushTimeout)
	}
	if got.Producer.InitTransactionsTimeout != DefaultInitTransactionsTimeout {
		t.Errorf("InitTransactionsTimeout = %v, atteso %v", got.Producer.InitTransactionsTimeout, DefaultInitTransactionsTimeout)
	}
	if got.Restart.InitialBackoff != DefaultRestartInitialBackoff || got.Restart.MaxBackoff != DefaultRestartMaxBackoff ||
		got.Restart.BackoffMultiplier() != DefaultRestartMultiplier || got.Restart.ResetAfter != DefaultRestartResetAfter {
		t.Errorf("default del restart non applicati: %+v", got.Restart)
	}
	// Il budget dei tentativi è FINITO per default: l'illimitato è una scelta esplicita (-1), non
	// ciò che si ottiene non scrivendo nulla.
	if got.Restart.Attempts() != DefaultRestartMaxAttempts || got.Restart.Unlimited() {
		t.Errorf("max-attempts di default = %d (illimitati=%v), atteso %d finito", got.Restart.Attempts(), got.Restart.Unlimited(), DefaultRestartMaxAttempts)
	}
}

// --- precedenza: processor > server > default ----------------------------------------------------

// È l'intera ragion d'essere dei blocchi globali: sbagliarne l'ordine significherebbe che un default
// della libreria batte una scelta esplicita dell'operatore.
func TestResolve_Precedenza(t *testing.T) {
	server := KafkaServer{
		Consumer: ConsumerTuning{
			SessionTimeoutMs: 10000,
			MaxBatchSize:     100,
			AutoOffsetReset:  "latest",
			OnError:          OnErrorDeadletter,
			DeadletterTopic:  strPtr("comune.DLQ"),
		},
		Producer: ProducerTuning{Acks: "all", CompressionType: "snappy"},
		Restart:  RestartSpec{MaxAttempts: ptr(5), MaxBackoff: time.Minute},
	}
	s := validSpec()
	s.Consumer.SessionTimeoutMs = 30000 // override esplicito
	s.Producer.Acks = "1"               // override esplicito
	s.Restart.MaxAttempts = ptr(2)      // override esplicito

	got := s.Resolve(server)

	// 1) l'override del processor batte il globale
	if got.Consumer.SessionTimeoutMs != 30000 {
		t.Errorf("consumer.session-timeout-ms = %d, atteso l'override", got.Consumer.SessionTimeoutMs)
	}
	if got.Producer.Acks != "1" {
		t.Errorf("producer.acks = %q, atteso l'override", got.Producer.Acks)
	}
	if got.Restart.Attempts() != 2 {
		t.Errorf("restart.max-attempts = %d, atteso l'override", got.Restart.Attempts())
	}
	// 2) il globale batte il default della libreria
	if got.Consumer.MaxBatchSize != 100 {
		t.Errorf("consumer.max-batch-size = %d, atteso 100 dal globale (default %d)", got.Consumer.MaxBatchSize, DefaultMaxBatchSize)
	}
	if got.Consumer.AutoOffsetReset != "latest" || got.Consumer.OnError != OnErrorDeadletter {
		t.Errorf("policy non ereditate dal globale: %+v", got.Consumer)
	}
	if got.Consumer.Deadletter() != "comune.DLQ" {
		t.Errorf("deadletter-topic = %q: il DLQ è ereditabile", got.Consumer.Deadletter())
	}
	if got.Producer.CompressionType != "snappy" {
		t.Errorf("producer.compression-type = %q, atteso snappy dal globale", got.Producer.CompressionType)
	}
	if got.Restart.MaxBackoff != time.Minute {
		t.Errorf("restart.max-backoff = %v, atteso 1m dal globale", got.Restart.MaxBackoff)
	}
	// 3) ciò che nessuno valorizza resta al default della libreria...
	if got.Consumer.CutFrequency != DefaultCutFrequency {
		t.Errorf("cut-frequency = %v, atteso il default %v", got.Consumer.CutFrequency, DefaultCutFrequency)
	}
	// ...o assente, se un default non c'è: il driver non scriverà la chiave nella ConfigMap.
	if got.Consumer.HeartbeatIntervalMs != 0 {
		t.Errorf("heartbeat-interval-ms = %d, atteso 0 (default librdkafka)", got.Consumer.HeartbeatIntervalMs)
	}
}

func TestResolve_NonToccaLIdentita(t *testing.T) {
	// I campi che distinguono un processor dall'altro non sono ereditabili: se lo fossero, due
	// processor finirebbero sullo stesso gruppo o sulla stessa identità transazionale.
	s := validSpec()
	s.TransactionalID = "tx-1"
	s.DefaultOutputTopic = "out"

	got := s.Resolve(KafkaServer{})

	if got.Name != "ingest" || got.GroupID != "g" || got.TransactionalID != "tx-1" ||
		got.DefaultOutputTopic != "out" || len(got.Topics) != 1 {
		t.Errorf("identità alterata da Resolve: %+v", got)
	}
}

// restart sta a livello `server` — non dentro `consumer` — perché non è tuning di un client: non
// finisce in nessuna ConfigMap. Resta però sovrascrivibile campo per campo sul singolo processor.
func TestResolve_RestartEreditatoCampoPerCampo(t *testing.T) {
	server := KafkaServer{Restart: RestartSpec{
		MaxAttempts:     ptr(5),
		InitialBackoff:  2 * time.Second,
		MaxBackoff:      time.Minute,
		Multiplier:      ptr(3.0),
		ResetAfter:      time.Hour,
		OnBusinessError: boolPtr(true),
	}}
	s := validSpec()
	s.Restart = RestartSpec{InitialBackoff: 5 * time.Second} // override di UN solo campo

	got := s.Resolve(server).Restart

	if got.InitialBackoff != 5*time.Second {
		t.Errorf("InitialBackoff = %v, atteso l'override", got.InitialBackoff)
	}
	// L'override di un campo non deve azzerare gli altri del blocco.
	if got.Attempts() != 5 || got.MaxBackoff != time.Minute || got.BackoffMultiplier() != 3 || got.ResetAfter != time.Hour {
		t.Errorf("l'override di un campo ha azzerato gli altri: %+v", got)
	}
	if !got.RestartsOnBusinessError() {
		t.Error("OnBusinessError non ereditato dal globale")
	}
}

// I due flag di RestartSpec sono *bool proprio per questo: con un bool semplice un `true` globale non
// sarebbe più disattivabile da un singolo processor.
func TestResolve_BooleaniDisattivabiliDalProcessor(t *testing.T) {
	server := KafkaServer{Restart: RestartSpec{Disabled: boolPtr(true), OnBusinessError: boolPtr(true)}}
	s := validSpec()
	s.Restart = RestartSpec{Disabled: boolPtr(false), OnBusinessError: boolPtr(false)}

	got := s.Resolve(server).Restart

	if got.IsDisabled() {
		t.Error("il processor deve poter riattivare il restart che il globale disabilita")
	}
	if got.RestartsOnBusinessError() {
		t.Error("il processor deve poter disattivare on-business-error che il globale abilita")
	}
}

// Stessa ragione per i puntatori del producer.
func TestResolve_ProducerPuntatoriDisattivabili(t *testing.T) {
	server := KafkaServer{Producer: ProducerTuning{EnableIdempotence: boolPtr(true), LingerMs: intPtr(20)}}
	s := validSpec()
	s.Producer = ProducerTuning{EnableIdempotence: boolPtr(false), LingerMs: intPtr(0)}

	got := s.Resolve(server).Producer

	if got.Idempotent() {
		t.Error("il processor deve poter disattivare l'idempotenza abilitata dal globale")
	}
	if got.LingerMs == nil || *got.LingerMs != 0 {
		t.Errorf("linger-ms = %v, atteso lo 0 esplicito del processor", got.LingerMs)
	}
}

// Il DLQ è un *string proprio per questo: un processor deve poter dire "io non ne ho" quando il
// globale ne dichiara uno, e con una stringa semplice "" sarebbe indistinguibile da "eredita".
func TestResolve_DeadletterDisattivabileDalProcessor(t *testing.T) {
	server := KafkaServer{Consumer: ConsumerTuning{DeadletterTopic: strPtr("comune.DLQ")}}

	tests := []struct {
		name string
		on   *string
		want string
	}{
		{"assente: eredita il globale", nil, "comune.DLQ"},
		{"stringa vuota: nessun DLQ per questo processor", strPtr(""), ""},
		{"valorizzato: override", strPtr("suo.DLQ"), "suo.DLQ"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := validSpec()
			s.Consumer.DeadletterTopic = tc.on

			got := s.Resolve(server)
			if got.Consumer.Deadletter() != tc.want {
				t.Errorf("Deadletter() = %q, atteso %q", got.Consumer.Deadletter(), tc.want)
			}
			if got.HasDeadletter() != (tc.want != "") {
				t.Errorf("HasDeadletter() = %v con topic %q", got.HasDeadletter(), tc.want)
			}
		})
	}
}

func TestRestartSpec_FlagAssenti(t *testing.T) {
	var r RestartSpec
	if r.IsDisabled() {
		t.Error("disabled assente deve valere false: la supervisione è attiva per default")
	}
	if r.RestartsOnBusinessError() {
		t.Error("on-business-error assente deve valere false: fail-fast mantiene la sua semantica")
	}
}

func TestProducerTuning_Idempotent(t *testing.T) {
	tests := []struct {
		name string
		in   *bool
		want bool
	}{
		{"assente = idempotente (default storico)", nil, true},
		{"true esplicito", boolPtr(true), true},
		{"false esplicito disattiva", boolPtr(false), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (ProducerTuning{EnableIdempotence: tc.in}).Idempotent(); got != tc.want {
				t.Errorf("Idempotent() = %v, atteso %v", got, tc.want)
			}
		})
	}
}

// Le mappe si fondono chiave per chiave invece di essere sostituite: aggiungere una proprietà a un
// processor non deve fargli perdere quelle comuni a tutti.
func TestResolve_KafkaPropertiesFuse(t *testing.T) {
	server := KafkaServer{
		Consumer: ConsumerTuning{KafkaProperties: map[string]string{"fetch.error.backoff.ms": "1000", "debug": "broker"}},
		Producer: ProducerTuning{KafkaProperties: map[string]string{"queue.buffering.max.kbytes": "1048576"}},
	}
	s := validSpec()
	s.Consumer.KafkaProperties = map[string]string{"debug": "cgrp"} // sovrascrive solo questa
	s.Producer.KafkaProperties = map[string]string{"sticky.partitioning.linger.ms": "10"}

	got := s.Resolve(server)

	if got.Consumer.KafkaProperties["fetch.error.backoff.ms"] != "1000" {
		t.Errorf("chiave comune persa: %v", got.Consumer.KafkaProperties)
	}
	if got.Consumer.KafkaProperties["debug"] != "cgrp" {
		t.Errorf("debug = %q, atteso l'override del processor", got.Consumer.KafkaProperties["debug"])
	}
	if got.Producer.KafkaProperties["queue.buffering.max.kbytes"] != "1048576" ||
		got.Producer.KafkaProperties["sticky.partitioning.linger.ms"] != "10" {
		t.Errorf("fusione producer errata: %v", got.Producer.KafkaProperties)
	}
	// L'eredità non deve mutare i blocchi globali, condivisi da tutti i processor.
	if server.Consumer.KafkaProperties["debug"] != "broker" || len(server.Producer.KafkaProperties) != 1 {
		t.Error("i blocchi globali sono stati mutati dall'eredità")
	}
}

func TestResolve_Idempotente(t *testing.T) {
	// Resolve viene chiamata sul percorso di wiring e su quello dell'engine: applicarla due volte
	// deve dare lo stesso risultato, altrimenti i due percorsi divergerebbero.
	server := KafkaServer{
		Consumer: ConsumerTuning{SessionTimeoutMs: 10000, MaxBatchSize: 100},
		Producer: ProducerTuning{Acks: "all"},
		Restart:  RestartSpec{MaxAttempts: ptr(3)},
	}
	once := validSpec().Resolve(server)
	twice := once.Resolve(server)

	if !reflect.DeepEqual(once, twice) {
		t.Errorf("Resolve non idempotente:\n una volta: %+v\n due volte: %+v", once, twice)
	}
}

func TestProducerTuning_IsZero(t *testing.T) {
	// IsZero distingue "il processor non ha un blocco producer" da "ne ha uno vuoto": serve a
	// segnalare un override che in modalità handle non avrebbe destinatario.
	if !(ProducerTuning{}).IsZero() {
		t.Error("un blocco non scritto deve risultare zero")
	}
	if (ProducerTuning{Acks: "all"}).IsZero() {
		t.Error("un blocco con acks non è zero")
	}
	if (ProducerTuning{LingerMs: intPtr(0)}).IsZero() {
		t.Error("linger-ms: 0 è una scelta esplicita, non un blocco vuoto")
	}
	if (ProducerTuning{KafkaProperties: map[string]string{"a": "b"}}).IsZero() {
		t.Error("un blocco con kafka-properties non è zero")
	}
}

// --- validazione --------------------------------------------------------------------------------

// I tag validate: sono applicati da core.ValidateStruct sull'albero di config dell'app. Prima di
// questa suite un `on-error: dealetter` (typo) degradava in silenzio a fail-fast, perché l'engine
// confronta solo == OnErrorDeadletter.
func TestProcessorSpec_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ProcessorSpec)
		wantErr bool
	}{
		{"spec valido", func(*ProcessorSpec) {}, false},
		{"name mancante", func(s *ProcessorSpec) { s.Name = "" }, true},
		{"topics vuoto", func(s *ProcessorSpec) { s.Topics = nil }, true},
		{"group-id mancante", func(s *ProcessorSpec) { s.GroupID = "" }, true},

		{"on-error assente", func(s *ProcessorSpec) { s.Consumer.OnError = "" }, false},
		{"on-error fail-fast", func(s *ProcessorSpec) { s.Consumer.OnError = OnErrorFailFast }, false},
		{"on-error deadletter", func(s *ProcessorSpec) { s.Consumer.OnError = OnErrorDeadletter }, false},
		{"on-error typo", func(s *ProcessorSpec) { s.Consumer.OnError = "dealetter" }, true},

		{"auto-offset-reset earliest", func(s *ProcessorSpec) { s.Consumer.AutoOffsetReset = "earliest" }, false},
		{"auto-offset-reset none", func(s *ProcessorSpec) { s.Consumer.AutoOffsetReset = "none" }, false},
		{"auto-offset-reset ignoto", func(s *ProcessorSpec) { s.Consumer.AutoOffsetReset = "beginning" }, true},

		{"assignment-strategy cooperative", func(s *ProcessorSpec) { s.Consumer.PartitionAssignmentStrategy = "cooperative-sticky" }, false},
		{"assignment-strategy ignota", func(s *ProcessorSpec) { s.Consumer.PartitionAssignmentStrategy = "sticky" }, true},

		{"isolation-level read_committed", func(s *ProcessorSpec) { s.Consumer.IsolationLevel = "read_committed" }, false},
		{"isolation-level ignoto", func(s *ProcessorSpec) { s.Consumer.IsolationLevel = "committed" }, true},

		{"producer.acks all", func(s *ProcessorSpec) { s.Producer.Acks = "all" }, false},
		{"producer.acks ignoto", func(s *ProcessorSpec) { s.Producer.Acks = "quorum" }, true},
		{"producer.compression zstd", func(s *ProcessorSpec) { s.Producer.CompressionType = "zstd" }, false},
		{"producer.compression ignota", func(s *ProcessorSpec) { s.Producer.CompressionType = "brotli" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := validSpec()
			tc.mutate(&s)
			err := core.ValidateStruct(s)
			if tc.wantErr != (err != nil) {
				t.Fatalf("wantErr=%v, err=%v", tc.wantErr, err)
			}
		})
	}
}

func TestKafkaServer_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*KafkaServer)
		wantErr bool
	}{
		{"server valido", func(*KafkaServer) {}, false},
		{"bootstrap-servers mancante", func(k *KafkaServer) { k.BootstrapServers = "" }, true},
		{"security-protocol SASL_SSL", func(k *KafkaServer) { k.SecurityProtocol = "SASL_SSL" }, false},
		{"security-protocol ignoto", func(k *KafkaServer) { k.SecurityProtocol = "SASL-SSL" }, true},
		{"sasl SCRAM-SHA-512", func(k *KafkaServer) { k.SASL.Mechanisms = "SCRAM-SHA-512" }, false},
		{"sasl meccanismo ignoto", func(k *KafkaServer) { k.SASL.Mechanisms = "SCRAM-SHA-1" }, true},
		// I blocchi globali sono validati come quelli per-processor: sono gli stessi tipi.
		{"server.consumer.on-error typo", func(k *KafkaServer) { k.Consumer.OnError = "dealetter" }, true},
		{"server.producer.acks ignoto", func(k *KafkaServer) { k.Producer.Acks = "quorum" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k := KafkaServer{BootstrapServers: "b:9092"}
			tc.mutate(&k)
			err := core.ValidateStruct(k)
			if tc.wantErr != (err != nil) {
				t.Fatalf("wantErr=%v, err=%v", tc.wantErr, err)
			}
		})
	}
}

func TestValidateKafkaProperties(t *testing.T) {
	tests := []struct {
		name    string
		props   map[string]string
		wantErr bool
	}{
		{"nil", nil, false},
		{"chiavi libere", map[string]string{"queue.buffering.max.kbytes": "1048576", "debug": "cgrp"}, false},
		{"bootstrap.servers riservata", map[string]string{"bootstrap.servers": "altro:9092"}, true},
		{"group.id riservata", map[string]string{"group.id": "altro"}, true},
		{"enable.auto.commit riservata", map[string]string{"enable.auto.commit": "true"}, true},
		{"transactional.id riservata", map[string]string{"transactional.id": "x"}, true},
		{"isolation.level riservata", map[string]string{"isolation.level": "read_uncommitted"}, true},
		// La normalizzazione conta: senza, "Group.ID " passerebbe il controllo e verrebbe poi
		// applicata comunque (il driver abbassa e trimma con la stessa funzione).
		{"riservata con case e spazi", map[string]string{" Group.ID ": "altro"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateKafkaProperties("processor test", tc.props)
			if tc.wantErr != (err != nil) {
				t.Fatalf("wantErr=%v, err=%v", tc.wantErr, err)
			}
		})
	}
}

// I default della LIBRERIA — quelli che esistono perché librdkafka e franz-go NON concordano — sono
// pinnati qui in tabella: è il posto in cui aggiungere il prossimo knob divergente, e l'unico in cui
// si vede in un colpo solo quali siano.
//
// La colonna "sovrascritto" è metà del contratto: un default che vincesse sul valore scritto
// dall'app non sarebbe un default, sarebbe un'imposizione.
func TestWithDefaults_KnobDivergentiFraDriver(t *testing.T) {
	c := ConsumerTuning{}.WithDefaults()
	p := ProducerTuning{}.WithDefaults()

	tests := []struct {
		knob      string
		got, want any
	}{
		{"consumer.isolation-level", c.IsolationLevel, DefaultIsolationLevel},
		{"consumer.partition-assignment-strategy", c.PartitionAssignmentStrategy, DefaultAssignmentStrategy},
		{"consumer.max-poll-interval-ms", c.MaxPollIntervalMs, DefaultMaxPollIntervalMs},
		{"producer.compression-type", p.CompressionType, DefaultCompressionType},
		{"producer.transaction-timeout-ms", p.TransactionTimeoutMs, DefaultTransactionTimeoutMs},
	}
	for _, tc := range tests {
		t.Run(tc.knob, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("%s = %v, atteso %v: senza questo default il knob resterebbe a quello del client, e i due client non concordano", tc.knob, tc.got, tc.want)
			}
		})
	}

	// Un valore esplicito dell'app vince sempre.
	c2 := ConsumerTuning{
		IsolationLevel:              "read_uncommitted",
		PartitionAssignmentStrategy: "range",
		MaxPollIntervalMs:           120000,
	}.WithDefaults()
	p2 := ProducerTuning{CompressionType: "zstd", TransactionTimeoutMs: 15000}.WithDefaults()
	for _, tc := range []struct {
		knob      string
		got, want any
	}{
		{"consumer.isolation-level", c2.IsolationLevel, "read_uncommitted"},
		{"consumer.partition-assignment-strategy", c2.PartitionAssignmentStrategy, "range"},
		{"consumer.max-poll-interval-ms", c2.MaxPollIntervalMs, 120000},
		{"producer.compression-type", p2.CompressionType, "zstd"},
		{"producer.transaction-timeout-ms", p2.TransactionTimeoutMs, 15000},
	} {
		t.Run("sovrascritto/"+tc.knob, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("%s = %v, atteso %v: il default non deve vincere sul valore scritto dall'app", tc.knob, tc.got, tc.want)
			}
		})
	}
}

// client-id: senza, ci si presenta al broker come "rdkafka" o "kgo" a seconda del driver. Il default
// è l'AppName, e non tocca i blocchi Consumer/Producer — che sono le sorgenti dell'eredità, dove un
// default renderebbe indistinguibile "non scritto" da "scritto al default".
func TestKafkaServerWithDefaults_SoloClientID(t *testing.T) {
	core.AppName = "mia-app"
	k := KafkaServer{BootstrapServers: "b:9092"}.WithDefaults()
	if k.ClientID != "mia-app" {
		t.Errorf("client-id = %q, atteso mia-app", k.ClientID)
	}
	if !core.IsZeroStruct(k.Consumer) || !k.Producer.IsZero() {
		t.Error("WithDefaults non deve toccare i blocchi consumer/producer: sono le sorgenti dell'eredità")
	}

	k2 := KafkaServer{BootstrapServers: "b:9092", ClientID: "esplicito"}.WithDefaults()
	if k2.ClientID != "esplicito" {
		t.Errorf("client-id = %q, atteso esplicito", k2.ClientID)
	}
}

// `delivery` è un campo di IDENTITÀ del processor: non eredita da `server` (non esiste lì) e
// sopravvive a Resolve, che tocca solo i tre blocchi di tuning.
func TestDelivery_IdentitaNonEreditabile(t *testing.T) {
	s := ProcessorSpec{
		Name: "alos", GroupID: "g", Topics: []string{"in"},
		Delivery: DeliveryAtLeastOnce,
	}.Resolve(KafkaServer{})

	if s.Delivery != DeliveryAtLeastOnce {
		t.Errorf("Delivery = %q, atteso %q: Resolve non deve toccare i campi di identità", s.Delivery, DeliveryAtLeastOnce)
	}
	if !s.AtLeastOnce() {
		t.Error("AtLeastOnce() = false su delivery=at-least-once")
	}

	// Il vuoto vale exactly-once: la garanzia FORTE è quella che si ottiene non scrivendo nulla.
	if (ProcessorSpec{}).AtLeastOnce() {
		t.Error("AtLeastOnce() = true su delivery vuoto: il default deve essere l'EOS")
	}
}

func TestDelivery_ValoreNonValidoFermaAvvio(t *testing.T) {
	// Il tag `oneof` gira su ValidateProcessor, cioè sullo spec GREZZO: un typo non degrada in
	// silenzio nel regime debole, ferma l'avvio.
	base := ProcessorSpec{Name: "p", GroupID: "g", Topics: []string{"in"}}
	for _, tc := range []struct {
		delivery string
		wantErr  bool
	}{
		{"", false},
		{DeliveryExactlyOnce, false},
		{DeliveryAtLeastOnce, false},
		{"at-leastonce", true},
		{"atleast-once", true},
		{"AT-LEAST-ONCE", true},
	} {
		t.Run(tc.delivery, func(t *testing.T) {
			s := base
			s.Delivery = tc.delivery
			if err := ValidateProcessor(s); tc.wantErr != (err != nil) {
				t.Fatalf("delivery=%q: wantErr=%v, err=%v", tc.delivery, tc.wantErr, err)
			}
		})
	}
}
