// Package franzdriver è l'implementazione di internal/driver basata su franz-go (twmb/franz-go).
// È puro Go: un'applicazione che sceglie questo driver builda con CGO_ENABLED=0 e non ha bisogno di
// librdkafka.
//
// Come internal/confluentdriver, è un package chiuso: l'engine, il Producer e le app dipendono solo
// da internal/driver, e la selezione avviene con l'import del guscio pubblico driver/franz passato a
// corekafka.WithDriver. È anche l'unico package che interpreta un errore di franz-go — la traduzione
// in driver.Severity sta in errors.go.
package franzdriver

import (
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Factory implementa driver.Factory usando franz-go.
type Factory struct{}

// New ritorna la Factory franz-go come driver.Factory. La registra il guscio pubblico driver/franz,
// che l'app sceglie con corekafka.WithDriver.
//
// Il log all'avvio non è decorativo: con due driver selezionabili, quale sia in esercizio è la prima
// cosa da sapere leggendo i log di un processo che si comporta in modo inatteso.
func New() driver.Factory {
	log.Info().Msg("corekafka: driver franz-go (puro Go)")
	return Factory{}
}

// NewGroupConsumer crea il consumer di gruppo della modalità handle (at-least-once).
func (Factory) NewGroupConsumer(s spec.ProcessorSpec, k spec.KafkaServer) (driver.GroupConsumer, error) {
	rb := &rebalanceObserver{name: s.Name}
	b, err := consumerOpts(s, k)
	if err != nil {
		return nil, err
	}
	b.add(kgo.OnPartitionsRevoked(rb.onRevoked), kgo.OnPartitionsLost(rb.onLost))

	cl, err := kgo.NewClient(b.opts...)
	if err != nil {
		// Una configurazione rifiutata dal client è un errore di config: nessun retry la corregge.
		return nil, driver.NewError(driver.SeverityPermanent, "new-consumer",
			fmt.Errorf("franzdriver: NewClient %q: %w", s.Name, err))
	}
	return &groupConsumer{
		session: newSession(s, cl, rb),
		cl:      cl,
		offsets: newOffsetTracker(),
	}, nil
}

// NewTransactSession crea la sessione EOS Kafka→Kafka (consumer di gruppo + producer transazionale in
// un solo client). Il producer transazionale è l'unico che appartiene a un processor, quindi prende il
// tuning da s.Producer — cioè `server.producer` con gli override del processor già applicati da
// Resolve.
func (Factory) NewTransactSession(s spec.ProcessorSpec, k spec.KafkaServer) (driver.TransactSession, error) {
	if s.TransactionalID == "" {
		return nil, fmt.Errorf("franzdriver: transactional-id mancante per il processor %q (modalità transform)", s.Name)
	}
	// EOS: il consumer legge solo record committati. È un'invariante della modalità, non un default:
	// leggere record non ancora committati romperebbe l'esattamente-una-volta a valle.
	s.Consumer.IsolationLevel = "read_committed"

	rb := &rebalanceObserver{name: s.Name}
	b, err := consumerOpts(s, k)
	if err != nil {
		return nil, err
	}
	pb, err := producerOpts(s.TransactionalID, "processor "+s.Name, s.Producer, k)
	if err != nil {
		return nil, err
	}
	// In EOS consumer e producer sono lo STESSO client franz: le opzioni del producer si aggiungono a
	// quelle del consumer. Le chiavi comuni (connessione, sicurezza, osservabilità) sono le stesse su
	// entrambi i lati e riapplicarle è idempotente.
	b.add(pb.opts...)
	b.add(kgo.OnPartitionsRevoked(rb.onRevoked), kgo.OnPartitionsLost(rb.onLost))

	sess, err := kgo.NewGroupTransactSession(b.opts...)
	if err != nil {
		return nil, driver.NewError(driver.SeverityPermanent, "new-transact-session",
			fmt.Errorf("franzdriver: NewGroupTransactSession %q: %w", s.Name, err))
	}
	return &transactSession{
		session: newSession(s, sess, rb),
		sess:    sess,
	}, nil
}

// NewProducer crea il producer condiviso del processo, non transazionale (DLQ). Non appartiene a
// nessun processor, quindi il tuning arriva direttamente da `server.producer`.
func (Factory) NewProducer(k spec.KafkaServer, p spec.ProducerTuning) (driver.Producer, error) {
	p = p.WithDefaults()
	b, err := producerOpts("", "server.producer", p, k)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(b.opts...)
	if err != nil {
		return nil, driver.NewError(driver.SeverityPermanent, "new-producer",
			fmt.Errorf("franzdriver: NewClient (producer): %w", err))
	}
	return &producer{cl: cl, flushTimeout: p.FlushTimeout}, nil
}

// NewTxProducer crea il producer TRANSAZIONALE del processo (una transazione per Produce). Come il non
// transazionale non appartiene a nessun processor, quindi il tuning arriva da `server.producer` — da
// cui viene anche l'id, che è ciò che ha fatto scegliere questa forma al chiamante
// (`server.producer.transactional-id`).
//
// L'id non è ri-validato qui: senza, il chiamante avrebbe costruito il non transazionale.
func (Factory) NewTxProducer(k spec.KafkaServer, p spec.ProducerTuning, transactionalID string) (driver.TxProducer, error) {
	p = p.WithDefaults()
	b, err := producerOpts(transactionalID, "server.producer", p, k)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(b.opts...)
	if err != nil {
		return nil, driver.NewError(driver.SeverityPermanent, "new-tx-producer",
			fmt.Errorf("franzdriver: NewClient (producer tx): %w", err))
	}
	return &txProducer{cl: cl, flushTimeout: p.FlushTimeout}, nil
}

// newSession compone la parte comune ai due client. Il buffer di poll è dimensionato sul batch
// dell'engine: chiedere più record di quanti ne entrano in un batch significherebbe tenerli fermi nel
// driver mentre il batch precedente viene elaborato.
func newSession(s spec.ProcessorSpec, p poller, rb *rebalanceObserver) session {
	maxPoll := s.Consumer.MaxBatchSize
	if maxPoll <= 0 {
		// Lo spec arriva risolto, quindi qui il valore c'è sempre; la guardia serve perché per
		// PollRecords un massimo non positivo significa "nessun limite", cioè l'opposto.
		maxPoll = spec.DefaultMaxBatchSize
	}
	return session{name: s.Name, p: p, rb: rb, maxPoll: maxPoll}
}
