package franzdriver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Regola di tutto questo file, la stessa del driver confluent (internal/confluentdriver/configmap.go):
// un knob NON valorizzato non produce alcuna kgo.Opt, così resta il default di franz-go. Imporre lo
// zero al posto di omettere significherebbe scrivere "0" dove zero è un valore legittimo e molto
// diverso dal default.
//
// Le chiavi della traccia (`applied`) sono i nomi DOTTED di librdkafka: sono il vocabolario in cui è
// scritto l'escape hatch `kafka-properties`, quindi è in quel vocabolario che va detto "questa
// proprietà sovrascrive un campo tipizzato già impostato".

// optBuilder accumula le opzioni del client insieme alla traccia di ciò che è stato impostato.
// La traccia serve a due cose reali: l'avviso su una kafka-properties che sovrascrive un campo
// tipizzato, e i test — che altrimenti non avrebbero alcun modo di ispezionare una []kgo.Opt, i cui
// effetti stanno tutti dentro una struct non esportata di franz.
type optBuilder struct {
	owner       string
	opts        []kgo.Opt
	applied     map[string]string
	unsupported []string
}

func newOptBuilder(owner string) *optBuilder {
	return &optBuilder{owner: owner, applied: make(map[string]string)}
}

// add registra un'opzione strutturale, che non corrisponde a un knob di configurazione (topic,
// group, disabilitazione dell'auto-commit): non entra nella traccia perché non è sovrascrivibile.
func (b *optBuilder) add(opts ...kgo.Opt) { b.opts = append(b.opts, opts...) }

// set registra un'opzione derivata da un knob, con la sua chiave dotted. Una opt nil è ammessa e
// significa "il valore richiesto è già il comportamento del client": resta nella traccia — è stato
// scritto e non è stato ignorato — senza aggiungere nulla alla configurazione.
func (b *optBuilder) set(key string, value any, opt kgo.Opt) {
	b.applied[key] = fmt.Sprint(value)
	if opt != nil {
		b.opts = append(b.opts, opt)
	}
}

// I setter sono metodi GENERICI perché ogni famiglia di opzioni franz-go ha il proprio tipo
// (kgo.Opt, ConsumerOpt, GroupOpt, ProducerOpt): tutti implementano kgo.Opt, ma func(int) GroupOpt
// non è assegnabile a func(int) kgo.Opt. Il type param assorbe la differenza e lascia i call site
// puliti — `b.setMs("session.timeout.ms", c.SessionTimeoutMs, kgo.SessionTimeout)`.
func (b *optBuilder) setInt[O kgo.Opt](key string, v int, mk func(int) O) {
	if v > 0 {
		b.set(key, v, mk(v))
	}
}

func (b *optBuilder) setInt32[O kgo.Opt](key string, v int, mk func(int32) O) {
	if v > 0 {
		b.set(key, v, mk(int32(v)))
	}
}

// setMs traduce un knob espresso in millisecondi (i campi `*Ms` dello spec).
func (b *optBuilder) setMs[O kgo.Opt](key string, ms int, mk func(time.Duration) O) {
	if ms > 0 {
		b.set(key, ms, mk(time.Duration(ms)*time.Millisecond))
	}
}

func (b *optBuilder) setDur[O kgo.Opt](key string, d time.Duration, mk func(time.Duration) O) {
	if d > 0 {
		b.set(key, int(d.Milliseconds()), mk(d))
	}
}

// setStr applica un knob stringa attraverso un convertitore che può rifiutarlo. Il convertitore è lo
// STESSO usato dalla tabella di traduzione di kafka-properties: un valore ammesso dal campo tipizzato
// e rifiutato dall'escape hatch (o viceversa) sarebbe una seconda semantica da tenere allineata.
func (b *optBuilder) setStr(key, v string, mk func(string) (kgo.Opt, error)) error {
	if v == "" {
		return nil
	}
	opt, err := mk(v)
	if err != nil {
		return fmt.Errorf("%s: %w", b.owner, err)
	}
	b.set(key, v, opt)
	return nil
}

// markUnsupported annota un knob valorizzato che franz-go non sa esprimere. Non è un errore: il blocco
// tipizzato è il vocabolario della LIBRERIA, comune ai due driver, quindi un knob non esprimibile è un
// limite documentato del driver — non una richiesta impossibile di chi ha scritto la config. Diverso
// il caso di kafka-properties, che è l'escape hatch di librdkafka: lì una chiave non traducibile
// ferma l'avvio (vedi kafkaprops.go).
func (b *optBuilder) markUnsupported(cond bool, key string) {
	if cond {
		b.unsupported = append(b.unsupported, key)
	}
}

// warnUnsupported emette UN avviso con l'elenco: N righe di log per N knob si perdono, una riga con
// la lista si legge.
func (b *optBuilder) warnUnsupported() {
	if len(b.unsupported) == 0 {
		return
	}
	log.Warn().Str("owner", b.owner).Strs("knob", b.unsupported).
		Msg("corekafka: knob di configurazione senza equivalente nel driver franz-go, ignorati (vedi la tabella nel README)")
}

// common applica le opzioni di connessione condivise da consumer e producer.
func (b *optBuilder) common(k spec.KafkaServer) error {
	b.add(kgo.SeedBrokers(strings.Split(k.BootstrapServers, ",")...))
	b.add(observabilityOpts(b.owner)...)

	if k.ClientID != "" {
		b.set("client.id", k.ClientID, kgo.ClientID(k.ClientID))
	}
	b.setMs("metadata.max.age.ms", k.MetadataMaxAgeMs, kgo.MetadataMaxAge)
	b.setMs("connections.max.idle.ms", k.ConnectionsMaxIdleMs, kgo.ConnIdleTimeout)

	// `debug` è la lista dei contesti di librdkafka: franz-go non ha contesti, la verbosità la decide
	// il livello di zerolog (vedi kgoLogger). `socket.keepalive.enable` è governato dal dialer del
	// client, non da un knob.
	b.markUnsupported(k.Debug != "", "debug")
	b.markUnsupported(k.SocketKeepaliveEnable, "socket-keepalive-enable")

	return b.security(k)
}

// security mappa security-protocol / SASL / TLS sulle opzioni franz-go.
//
// Differenza voluta rispetto al producer di go-core-batch, da cui questo codice è modellato: là una
// tls.Config che non si costruisce viene loggata a Error e la connessione PROSEGUE in chiaro; qui
// l'errore risale e fa fallire l'avvio. Degradare in silenzio la sicurezza del canale è l'ultimo posto
// in cui è accettabile.
func (b *optBuilder) security(k spec.KafkaServer) error {
	proto := strings.ToUpper(k.SecurityProtocol)
	useTLS := proto == "SSL" || proto == "SASL_SSL"
	useSASL := strings.HasPrefix(proto, "SASL") || k.SASL.Mechanisms != ""

	if useTLS {
		cfg, err := buildTLSConfig(k.SSL)
		if err != nil {
			return fmt.Errorf("%s: %w", b.owner, err)
		}
		b.set("security.protocol", proto, kgo.DialTLSConfig(cfg))
	}
	if useSASL {
		m, err := saslMechanism(k.SASL)
		if err != nil {
			return fmt.Errorf("%s: %w", b.owner, err)
		}
		b.set("sasl.mechanism", k.SASL.Mechanisms, kgo.SASL(m))
	}
	return nil
}

// buildTLSConfig costruisce la tls.Config: CA privata (truststore) e, per il TLS mutuo, il certificato
// con cui il broker autentica il client.
func buildTLSConfig(s spec.SSLCfg) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: s.SkipVerify} //nolint:gosec // skip-verify è una scelta esplicita della config
	if s.CaLocation != "" {
		pem, err := os.ReadFile(s.CaLocation)
		if err != nil {
			return nil, fmt.Errorf("lettura ssl.ca-location %q: %w", s.CaLocation, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("nessun certificato valido in ssl.ca-location %q", s.CaLocation)
		}
		cfg.RootCAs = pool
	}
	if s.CertificateLocation != "" || s.KeyLocation != "" {
		if s.CertificateLocation == "" || s.KeyLocation == "" {
			return nil, fmt.Errorf("mTLS incompleto: ssl.certificate-location e ssl.key-location vanno impostati insieme")
		}
		// La chiave cifrata da password non è supportata: x509.DecryptPEMBlock è deprecata e insicura,
		// e una chiave in chiaro montata da un secret è la forma normale in Kubernetes.
		if s.KeyPassword != "" {
			return nil, fmt.Errorf("ssl.key-password non è supportata dal driver franz: usare una chiave non cifrata")
		}
		cert, err := tls.LoadX509KeyPair(s.CertificateLocation, s.KeyLocation)
		if err != nil {
			return nil, fmt.Errorf("caricamento del certificato client (mTLS): %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// saslMechanism costruisce il meccanismo SASL. Un meccanismo non supportato è un ERRORE e non un
// fallback su PLAIN (come fa oggi go-core-batch): un downgrade silenzioso del meccanismo di
// autenticazione è esattamente ciò che non deve succedere senza che nessuno lo veda.
func saslMechanism(s spec.SaslCfg) (sasl.Mechanism, error) {
	switch strings.ToUpper(s.Mechanisms) {
	case "PLAIN":
		return plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("sasl.mechanisms %q non supportato dal driver franz (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", s.Mechanisms)
	}
}

// consumerOpts traduce KafkaServer + ProcessorSpec (GIÀ RISOLTO) nelle opzioni del group consumer.
// enable.auto.commit non compare: il commit è manuale e non è configurabile (spec.DeniedKafkaProperties).
func consumerOpts(s spec.ProcessorSpec, k spec.KafkaServer) (*optBuilder, error) {
	c := s.Consumer
	b := newOptBuilder("processor " + s.Name)

	if err := b.common(k); err != nil {
		return nil, err
	}
	b.add(
		kgo.ConsumerGroup(s.GroupID),
		kgo.ConsumeTopics(s.Topics...),
		kgo.DisableAutoCommit(),
		// Il rebalance non può avvenire mentre un batch è in volo: il client lo trattiene finché non
		// chiamiamo AllowRebalance, cioè dopo il commit o lo scarto. È la stessa garanzia che nel
		// driver confluent si ottiene a posteriori (revoca → scarto del batch), qui ottenuta prima.
		// La finestra è limitata dal taglio del batch (cut-frequency, 1s di default), quindi molto
		// sotto il rebalance timeout.
		kgo.BlockRebalanceOnPoll(),
	)

	if err := b.setStr("auto.offset.reset", c.AutoOffsetReset, autoOffsetResetOpt); err != nil {
		return nil, err
	}
	if err := b.setStr("partition.assignment.strategy", c.PartitionAssignmentStrategy, balancerOpt); err != nil {
		return nil, err
	}
	if err := b.setStr("isolation.level", c.IsolationLevel, isolationOpt); err != nil {
		return nil, err
	}
	b.setMs("session.timeout.ms", c.SessionTimeoutMs, kgo.SessionTimeout)
	b.setMs("heartbeat.interval.ms", c.HeartbeatIntervalMs, kgo.HeartbeatInterval)
	// max.poll.interval.ms è il tempo concesso all'elaborazione fra due poll: in franz-go è il
	// rebalance timeout, cioè quanto il gruppo attende che questo membro completi il lavoro in corso.
	b.setMs("max.poll.interval.ms", c.MaxPollIntervalMs, kgo.RebalanceTimeout)
	b.setInt32("fetch.min.bytes", c.FetchMinBytes, kgo.FetchMinBytes)
	b.setInt32("fetch.max.bytes", c.FetchMaxBytes, kgo.FetchMaxBytes)
	b.setInt32("max.partition.fetch.bytes", c.MaxPartitionFetchBytes, kgo.FetchMaxPartitionBytes)
	b.setMs("fetch.wait.max.ms", c.FetchWaitMaxMs, kgo.FetchMaxWait)

	// queued.max.messages.kbytes limita la coda di prefetch di librdkafka; franz-go dimensiona il
	// prefetch per fetch (fetch.max.bytes) e per numero di fetch concorrenti, non con una coda a byte.
	b.markUnsupported(c.QueuedMaxMessagesKbytes > 0, "consumer.queued-max-messages-kbytes")

	if err := b.kafkaProperties("server", k.KafkaProperties); err != nil {
		return nil, err
	}
	if err := b.kafkaProperties(b.owner+" (consumer)", c.KafkaProperties); err != nil {
		return nil, err
	}
	b.warnUnsupported()
	return b, nil
}

// producerOpts traduce KafkaServer + ProducerTuning nelle opzioni del producer. owner identifica la
// sezione nei log (`server.producer` per quello condiviso, `processor <nome>` per il transazionale);
// transactionalID != "" costruisce il producer EOS, dove l'idempotenza è implicita.
func producerOpts(transactionalID, owner string, p spec.ProducerTuning, k spec.KafkaServer) (*optBuilder, error) {
	b := newOptBuilder(owner)
	if err := b.common(k); err != nil {
		return nil, err
	}

	if transactionalID != "" {
		b.set("transactional.id", transactionalID, kgo.TransactionalID(transactionalID))
		b.setMs("transaction.timeout.ms", p.TransactionTimeoutMs, kgo.TransactionTimeout)
		// init-transactions-timeout non ha destinatario: franz-go fa la InitProducerID da sé al primo
		// Begin, con i propri retry, e non espone un timeout dedicato.
		b.markUnsupported(p.InitTransactionsTimeout > 0, "producer.init-transactions-timeout")
	} else if !p.Idempotent() {
		// L'idempotenza è ATTIVA di default sia in franz-go sia in go-core-kafka: si tocca solo per
		// disattivarla esplicitamente.
		b.set("enable.idempotence", false, kgo.DisableIdempotentWrite())
	}

	if err := b.setStr("acks", p.Acks, acksOpt); err != nil {
		return nil, err
	}
	if err := b.setStr("compression.type", p.CompressionType, compressionOpt); err != nil {
		return nil, err
	}
	// linger.ms è un *int proprio per poter imporre 0 (invia subito): setDur lo scarterebbe.
	if p.LingerMs != nil {
		b.set("linger.ms", *p.LingerMs, kgo.ProducerLinger(time.Duration(*p.LingerMs)*time.Millisecond))
	}
	b.setInt32("batch.size", p.BatchSize, kgo.ProducerBatchMaxBytes)
	b.setInt("message.send.max.retries", p.MessageSendMaxRetries, kgo.RecordRetries)
	b.setInt("max.in.flight.requests.per.connection", p.MaxInFlight, kgo.MaxProduceRequestsInflightPerBroker)
	b.setMs("request.timeout.ms", p.RequestTimeoutMs, kgo.ProduceRequestTimeout)
	b.setDur("delivery.timeout.ms", p.DeliveryTimeout, kgo.RecordDeliveryTimeout)
	if p.RetryBackoff > 0 {
		d := p.RetryBackoff
		b.set("retry.backoff.ms", int(d.Milliseconds()), kgo.RetryBackoffFn(func(int) time.Duration { return d }))
	}

	// message.max.bytes: in franz-go il limite per record coincide con quello del batch, già coperto da
	// batch.size — mapparlo anche qui significherebbe due knob che si sovrascrivono a vicenda.
	// batch.num.messages non esiste: franz batcha a byte. metadata.max.idle.ms non ha equivalente
	// (esistono MetadataMaxAge/MetadataMinAge, semantica diversa).
	b.markUnsupported(p.MessageMaxBytes > 0, "producer.message-max-bytes")
	b.markUnsupported(p.BatchNumMessages > 0, "producer.batch-num-messages")
	b.markUnsupported(p.MetadataMaxIdleMs > 0, "producer.metadata-max-idle-ms")

	if err := b.kafkaProperties("server", k.KafkaProperties); err != nil {
		return nil, err
	}
	if err := b.kafkaProperties(owner+" (producer)", p.KafkaProperties); err != nil {
		return nil, err
	}
	b.warnUnsupported()
	return b, nil
}

// --- convertitori condivisi con la tabella di kafkaprops.go ------------------------------------

func autoOffsetResetOpt(v string) (kgo.Opt, error) {
	switch strings.ToLower(v) {
	case "earliest", "beginning", "smallest":
		return kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), nil
	case "latest", "end", "largest":
		return kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), nil
	case "none":
		// Nessun offset committato → il consumo fallisce invece di ripartire da un capo arbitrario.
		return kgo.ConsumeResetOffset(kgo.NewOffset().AtCommitted()), nil
	default:
		return nil, fmt.Errorf("auto-offset-reset %q non valido (earliest, latest, none)", v)
	}
}

func balancerOpt(v string) (kgo.Opt, error) {
	switch strings.ToLower(v) {
	case "range":
		return kgo.Balancers(kgo.RangeBalancer()), nil
	case "roundrobin":
		return kgo.Balancers(kgo.RoundRobinBalancer()), nil
	case "sticky":
		return kgo.Balancers(kgo.StickyBalancer()), nil
	case "cooperative-sticky":
		return kgo.Balancers(kgo.CooperativeStickyBalancer()), nil
	default:
		return nil, fmt.Errorf("partition-assignment-strategy %q non valida (range, roundrobin, cooperative-sticky)", v)
	}
}

func isolationOpt(v string) (kgo.Opt, error) {
	switch strings.ToLower(v) {
	case "read_committed":
		return kgo.FetchIsolationLevel(kgo.ReadCommitted()), nil
	case "read_uncommitted":
		return kgo.FetchIsolationLevel(kgo.ReadUncommitted()), nil
	default:
		return nil, fmt.Errorf("isolation-level %q non valido (read_committed, read_uncommitted)", v)
	}
}

func acksOpt(v string) (kgo.Opt, error) {
	switch strings.ToLower(v) {
	case "all", "-1":
		return kgo.RequiredAcks(kgo.AllISRAcks()), nil
	case "1":
		return kgo.RequiredAcks(kgo.LeaderAck()), nil
	case "0":
		return kgo.RequiredAcks(kgo.NoAck()), nil
	default:
		return nil, fmt.Errorf("acks %q non valido (0, 1, -1, all)", v)
	}
}

func compressionOpt(v string) (kgo.Opt, error) {
	switch strings.ToLower(v) {
	case "none", "uncompressed":
		return kgo.ProducerBatchCompression(kgo.NoCompression()), nil
	case "gzip":
		return kgo.ProducerBatchCompression(kgo.GzipCompression()), nil
	case "snappy":
		return kgo.ProducerBatchCompression(kgo.SnappyCompression()), nil
	case "lz4":
		return kgo.ProducerBatchCompression(kgo.Lz4Compression()), nil
	case "zstd":
		return kgo.ProducerBatchCompression(kgo.ZstdCompression()), nil
	default:
		return nil, fmt.Errorf("compression-type %q non valido (none, gzip, snappy, lz4, zstd)", v)
	}
}
