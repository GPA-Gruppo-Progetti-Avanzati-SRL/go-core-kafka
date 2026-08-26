// Package consumer è l'engine di go-core-kafka: per ogni ConsumerSpec attivo avvia una goroutine che
// consuma dal client (dietro internal/driver) ed esegue il Handler (modalità handle, at-least-once) o
// il Transformer (modalità transform, EOS Kafka->Kafka). Non importa mai il client concreto: usa solo
// le interfacce driver.* ottenute dalla driver.Factory iniettata.
//
// Ogni consumer è supervisionato: il loop di consumo può terminare con errore e, a seconda della
// severità classificata dal driver, essere riavviato in-process con backoff invece di far terminare
// il processo. Vedi runner.run.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// LabelConsumer è l'etichetta pprof applicata alla goroutine di ogni consumer.
const LabelConsumer = "kafka_consumer"

// Consumers è il valore fx che tiene vivo l'engine (nessuna API pubblica: la sua costruzione avvia i
// consumer via lifecycle).
type Consumers struct{}

type params struct {
	fx.In
	LC           fx.Lifecycle
	Shutdowner   fx.Shutdowner
	Specs        []spec.ProcessorSpec
	Kafka        spec.KafkaServer
	Producer     spec.ProducerTuning
	Factory      driver.Factory
	Handlers     []processor.HandlerRegistration     `group:"kafka_handlers"`
	Transformers []processor.TransformerRegistration `group:"kafka_transformers"`
	DLQ          *producer.Producer                  `optional:"true"`
}

// runner incapsula uno spec e il suo processor; apre il proprio client a ogni tentativo di run.
type runner struct {
	spec        spec.ProcessorSpec
	kafka       spec.KafkaServer
	factory     driver.Factory
	handler     processor.Handler
	transformer processor.Transformer
	dlq         *producer.Producer
}

// NewConsumers valida gli spec contro i processor registrati (fail-fast dal costruttore) e registra
// nel lifecycle fx l'avvio/arresto cooperativo di una goroutine per consumer.
func NewConsumers(p params) (*Consumers, error) {
	handlers := make(map[string]processor.Handler, len(p.Handlers))
	for _, h := range p.Handlers {
		handlers[h.Consumer] = h.Handler
	}
	transformers := make(map[string]processor.Transformer, len(p.Transformers))
	for _, t := range p.Transformers {
		transformers[t.Consumer] = t.Transformer
	}

	// Le chiavi riservate in kafka-properties fermano l'avvio: sono invarianti dell'engine, non
	// default sovrascrivibili (vedi spec.DeniedKafkaProperties).
	for owner, props := range map[string]map[string]string{
		"server":          p.Kafka.KafkaProperties,
		"server.consumer": p.Kafka.Consumer.KafkaProperties,
		"server.producer": p.Kafka.Producer.KafkaProperties,
	} {
		if err := spec.ValidateKafkaProperties(owner, props); err != nil {
			return nil, err
		}
	}

	runners := make([]*runner, 0, len(p.Specs))
	for _, raw := range p.Specs {
		// Qui gli spec diventano quelli EFFETTIVI: Resolve eredita dai blocchi di `server` i campi
		// non valorizzati e poi applica i default della libreria. Da questa riga in poi nessuno
		// consulta più i blocchi globali — un campo letto da r.spec è già il valore giusto.
		s := raw.Resolve(p.Kafka)

		// È la lista `processors` di config a comandare l'attivazione; disabled=true la salta.
		if s.Disabled {
			log.Info().Str("processor", s.Name).Msg("corekafka: processor disabilitato (disabled=true), non attivato")
			continue
		}

		for owner, props := range map[string]map[string]string{
			"processor " + s.Name + " (consumer)": raw.Consumer.KafkaProperties,
			"processor " + s.Name + " (producer)": raw.Producer.KafkaProperties,
		} {
			if err := spec.ValidateKafkaProperties(owner, props); err != nil {
				return nil, err
			}
		}

		r := &runner{spec: s, kafka: p.Kafka, factory: p.Factory, dlq: p.DLQ}

		// La modalità è DERIVATA dalla registrazione: nome nel gruppo kafka_handlers -> handle, in
		// kafka_transformers -> transform. In entrambi -> ambiguo; in nessuno -> non registrato.
		h, isHandler := handlers[s.Name]
		t, isTransformer := transformers[s.Name]
		switch {
		case isHandler && isTransformer:
			return nil, fmt.Errorf("processor %q: registrato sia come Handler sia come Transformer (ambiguo)", s.Name)
		case isHandler:
			r.handler = h
			// In modalità handle il producer è quello CONDIVISO del processo (serve al DLQ): un
			// blocco `producer` sul processor non ha un destinatario. Warning e non errore perché
			// la modalità è derivata dalla registrazione, quindi chi scrive la config non ha modo di
			// saperlo guardando solo il YAML.
			if !raw.Producer.IsZero() {
				log.Warn().Str("processor", s.Name).
					Msg("corekafka: il blocco `producer` su un processor in modalità handle è ignorato (il producer del DLQ è condiviso): usare `server.producer`")
			}
			if s.Consumer.OnError == spec.OnErrorDeadletter && s.Consumer.Deadletter() == "" {
				return nil, fmt.Errorf("processor %q: consumer.on-error=deadletter richiede consumer.deadletter-topic", s.Name)
			}
			// Il DLQ (via Producer condiviso) serve sia per la policy di default deadletter sia quando
			// l'handler sceglie il deadletter a runtime (processor.DeadLetter): se c'è un
			// deadletter-topic, il Producer deve essere presente.
			if s.HasDeadletter() && p.DLQ == nil {
				return nil, fmt.Errorf("processor %q: consumer.deadletter-topic impostato richiede il Producer (usare corekafka.WithProducer)", s.Name)
			}
		case isTransformer:
			r.transformer = t
			if s.TransactionalID == "" {
				return nil, fmt.Errorf("processor %q (transform): transactional-id obbligatorio", s.Name)
			}
		default:
			return nil, fmt.Errorf("processor %q: nessun processor registrato (usare corekafka.RegisterHandler o RegisterTransformer con questo nome)", s.Name)
		}

		// Configure opzionale: passa le Properties dello spec all'handler/transformer che le vuole
		// all'avvio (precompute/validazione). Un errore fa fail-fast: l'app non parte.
		if c, ok := r.handler.(processor.Configurable); ok {
			if err := c.Configure(s.Properties); err != nil {
				return nil, fmt.Errorf("processor %q: Configure: %w", s.Name, err)
			}
		}
		if c, ok := r.transformer.(processor.Configurable); ok {
			if err := c.Configure(s.Properties); err != nil {
				return nil, fmt.Errorf("processor %q: Configure: %w", s.Name, err)
			}
		}

		runners = append(runners, r)
	}

	// Nota: i processor dei consumer non attivi non arrivano nemmeno qui — la costruzione lazy
	// (processor.Configure) fornisce a fx solo i processor dei consumer attivi, quindi i due value group
	// contengono già solo quelli attivati. Lo skip è loggato dal registry al momento della registrazione.

	ctx, cancel := context.WithCancel(context.Background())
	var done chan struct{}

	p.LC.Append(fx.Hook{
		OnStart: func(context.Context) error {
			done = make(chan struct{}, len(runners))
			for _, r := range runners {
				// pprof.Do etichetta la goroutine col nome del consumer: da Go 1.27 la
				// label compare anche nei traceback e rende leggibili i profili
				// goroutine/goroutineleak di un processo con più consumer.
				go pprof.Do(ctx, pprof.Labels(LabelConsumer, r.spec.Name), func(context.Context) {
					defer func() { done <- struct{}{} }()
					if err := r.run(ctx); err != nil && ctx.Err() == nil {
						log.Error().Err(err).Str("consumer", r.spec.Name).Msg("corekafka: consumer terminato con errore, arresto dell'applicazione")
						_ = p.Shutdowner.Shutdown()
					}
				})
			}
			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			for range runners {
				<-done
			}
			return nil
		},
	})

	return &Consumers{}, nil
}

// run supervisiona il consumo: esegue il loop e, se termina con errore, decide se ricostruire il
// client e riprovare dopo un backoff o lasciare risalire l'errore (che fa terminare il processo).
//
// La distinzione la fa la severità assegnata dal driver. L'indisponibilità di un broker o un fencing
// EOS sono condizioni da cui si esce ricostruendo consumer e producer: senza questo livello l'unico
// recovery sarebbe la morte del processo, che su un rolling restart dei broker diventa un
// CrashLoopBackOff. Restano invece letali gli errori su cui nessun retry può aiutare — credenziali
// sbagliate, config rifiutata — e, per default, gli errori di business sotto fail-fast (vedi
// RestartSpec.OnBusinessError).
func (r *runner) run(ctx context.Context) error {
	ctx = spec.ContextWithProperties(ctx, r.spec.Name, r.spec.Properties)
	b := newBackoff(r.spec.Restart)

	for {
		start := time.Now()
		err := r.runOnce(ctx)
		if err == nil || ctx.Err() != nil {
			return nil // arresto pulito (shutdown cooperativo)
		}

		sev := driver.SeverityOf(err)
		if !r.restartable(sev) {
			return err
		}
		// Un run lungo e sano non deve ereditare i tentativi bruciati da un guasto vecchio.
		if time.Since(start) >= r.spec.Restart.ResetAfter {
			b.reset()
		}
		wait, ok := b.next()
		if !ok {
			return fmt.Errorf("processor %q: esauriti i %d tentativi di riavvio: %w", r.spec.Name, r.spec.Restart.MaxAttempts, err)
		}

		restartsTotal.WithLabelValues(r.spec.Name, sev.String()).Inc()
		log.Warn().Err(err).Str("consumer", r.spec.Name).Str("severity", sev.String()).
			Int("attempt", b.count()).Dur("backoff", wait).
			Msg("corekafka: consumer terminato con errore, riavvio dopo backoff")

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// restartable dice se una severità giustifica la ricostruzione del client.
func (r *runner) restartable(sev driver.Severity) bool {
	if r.spec.Restart.IsDisabled() {
		return false
	}
	switch sev {
	case driver.SeverityPermanent:
		// Config o credenziali: riprovare produce lo stesso errore all'infinito.
		return false
	case driver.SeverityBusiness:
		// Errore risalito da Handle/Transform sotto fail-fast: la semantica documentata è "non
		// committa ed esce". Riprovare in-process un record poison sarebbe un loop senza uscita.
		return r.spec.Restart.RestartsOnBusinessError()
	default:
		return true
	}
}

// runOnce esegue un ciclo di vita completo del client: lo crea, consuma finché può, lo chiude.
func (r *runner) runOnce(ctx context.Context) error {
	if r.transformer != nil {
		return r.runTransform(ctx)
	}
	return r.runHandle(ctx)
}

// outcome è la decisione presa sul batch dopo il ritorno del processor.
type outcome int

const (
	outcomeCommit     outcome = iota // tutto elaborato: committa
	outcomeDeadletter                // instrada i record indicati al DLQ, poi committa
	outcomeFail                      // non committare: l'errore risale (replay)
)

// classify traduce il valore ritornato da Handle/Transform nella decisione sul batch, ed è UNICA per
// le due modalità: prima handle valutava ErrFailFast prima di PoisonRecords e transform il contrario,
// quindi lo stesso errore composito veniva classificato in due modi diversi.
//
// Ritorna anche i record da mandare al DLQ e la causa da allegarvi: cambia solo COME i due modi li
// consegnano (producer condiviso in handle, stessa transazione EOS in transform), non quali siano.
func classify(err error, batch []*message.Record, onError string) (outcome, []*message.Record, error) {
	switch {
	case err == nil:
		return outcomeCommit, nil, nil
	case errors.Is(err, processor.ErrFailFast):
		// Scelta esplicita del processor: vince su qualunque altra indicazione, policy inclusa.
		return outcomeFail, nil, err
	}
	// Il processor ha indicato QUALI record sono poison: il resto del batch è stato elaborato.
	if pr, ok := errors.AsType[*processor.PoisonRecords](err); ok {
		return outcomeDeadletter, pr.Records, pr.Cause
	}
	// Errore generico: decide la policy di default dello spec.
	if onError == spec.OnErrorDeadletter {
		return outcomeDeadletter, batch, err
	}
	return outcomeFail, nil, err
}

// absorb gestisce le severità che NON richiedono di ricostruire il client: il batch in volo viene
// scartato senza commit e il loop prosegue. Ritorna true se l'errore è stato assorbito.
//
// È il caso del rebalance (SeverityReset): i record accumulati vengono da partizioni che potrebbero
// non essere più nostre, quindi vanno riletti dal nuovo owner — duplicati, non buchi. E dell'abort
// transazionale richiesto dal broker (SeverityAbort), dopo il quale la sessione resta utilizzabile.
func (r *runner) absorb(err error, batch *[]*message.Record) bool {
	sev := driver.SeverityOf(err)
	if sev != driver.SeverityReset && sev != driver.SeverityAbort {
		return false
	}
	n := len(*batch)
	*batch = (*batch)[:0]
	batchDiscardedTotal.WithLabelValues(r.spec.Name, sev.String()).Add(float64(n))
	log.Warn().Err(err).Str("consumer", r.spec.Name).Int("records", n).
		Msg("corekafka: batch scartato senza commit, il consumo prosegue")
	return true
}

// sendDeadletter produce i record sul topic DLQ dello spec. Ritorna errore (→ fail-fast) se il DLQ
// non è configurato, così una richiesta di deadletter senza DLQ non perde silenziosamente i dati.
func (r *runner) sendDeadletter(ctx context.Context, recs []*message.Record, cause error) error {
	if r.dlq == nil || r.spec.Consumer.Deadletter() == "" {
		return fmt.Errorf("processor %q: deadletter richiesto ma non configurato (manca deadletter-topic/Producer): %w", r.spec.Name, cause)
	}
	if appErr := r.dlq.Produce(ctx, r.toDLQ(recs, cause)); appErr != nil {
		return appErr
	}
	return nil
}

// runHandle: modalità at-least-once. poll -> accumula -> (cut size/tempo) -> Handle -> commit dopo il ritorno.
func (r *runner) runHandle(ctx context.Context) error {
	gc, err := r.factory.NewGroupConsumer(r.spec, r.kafka)
	if err != nil {
		return err
	}
	defer gc.Close()

	ticker := time.NewTicker(r.spec.Consumer.CutFrequency)
	defer ticker.Stop()
	batch := make([]*message.Record, 0, r.spec.Consumer.MaxBatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		start := time.Now()
		hErr := r.handler.Handle(ctx, batch)
		batchDuration.WithLabelValues(r.spec.Name).Observe(time.Since(start).Seconds())

		oc, poison, cause := classify(hErr, batch, r.spec.Consumer.OnError)
		switch oc {
		case outcomeFail:
			return cause // niente commit → replay
		case outcomeDeadletter:
			if err := r.sendDeadletter(ctx, poison, cause); err != nil {
				return err // DLQ non configurato/irraggiungibile → replay
			}
			deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(poison)))
			processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch) - len(poison)))
		case outcomeCommit:
			processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch)))
		}

		if err := gc.Commit(ctx); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// Ultimo flush: sono record già elaborati, non committarli significherebbe rielaborarli
			// al riavvio. Se fallisce non c'è più nulla da fare se non renderlo visibile.
			if err := flush(); err != nil {
				log.Error().Err(err).Str("consumer", r.spec.Name).
					Msg("corekafka: flush finale fallito in arresto, i record del batch saranno riconsumati")
			}
			return nil
		case <-ticker.C:
			if err := flush(); err != nil {
				if r.absorb(err, &batch) {
					continue
				}
				return err
			}
		default:
			rec, err := gc.Poll(ctx, r.spec.Consumer.PollTimeout)
			if err != nil {
				if r.absorb(err, &batch) {
					continue
				}
				return err
			}
			if rec == nil {
				continue
			}
			consumedTotal.WithLabelValues(r.spec.Name).Inc()
			batch = append(batch, rec)
			if len(batch) >= r.spec.Consumer.MaxBatchSize {
				if err := flush(); err != nil {
					if r.absorb(err, &batch) {
						continue
					}
					return err
				}
			}
		}
	}
}

// runTransform: modalità EOS Kafka->Kafka. poll -> accumula -> Begin -> Transform -> Produce -> Commit
// (atomico) o Abort. Un errore abortisce e forza il replay.
func (r *runner) runTransform(ctx context.Context) error {
	sess, err := r.factory.NewTransactSession(r.spec, r.kafka)
	if err != nil {
		return err
	}
	defer sess.Close()

	ticker := time.NewTicker(r.spec.Consumer.CutFrequency)
	defer ticker.Stop()
	batch := make([]*message.Record, 0, r.spec.Consumer.MaxBatchSize)

	// abort annulla la transazione in corso. L'esito è loggato e non ritornato: stiamo già gestendo
	// l'errore che ha reso necessario l'abort, ed è quello che deve risalire.
	abort := func() {
		if err := sess.Abort(ctx); err != nil {
			log.Warn().Err(err).Str("consumer", r.spec.Name).Msg("corekafka: abort della transazione fallito")
		}
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		start := time.Now()
		defer func() { batchDuration.WithLabelValues(r.spec.Name).Observe(time.Since(start).Seconds()) }()

		if err := sess.Begin(); err != nil {
			return err
		}
		out, tErr := r.transformer.Transform(ctx, batch)
		resolveTopics(out, r.spec.DefaultOutputTopic)

		// Stesso modello a esiti della modalità handle (classify è condivisa), ma la consegna DLQ
		// avviene DENTRO la sessione transazionale (append agli output) così l'EOS resta intatto:
		// output "buoni" + record DLQ + commit offset sono atomici.
		oc, poison, cause := classify(tErr, batch, r.spec.Consumer.OnError)
		switch oc {
		case outcomeFail:
			abort()
			return cause
		case outcomeDeadletter:
			dlq, derr := r.dlqRecords(poison, cause)
			if derr != nil {
				abort()
				return derr
			}
			out = append(out, dlq...)
			deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(poison)))
		}

		if err := sess.Produce(ctx, out); err != nil {
			abort()
			return err
		}
		if err := sess.Commit(ctx); err != nil {
			abort()
			return err
		}
		producedTotal.WithLabelValues(r.spec.Name).Add(float64(len(out)))
		processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch) - len(poison)))
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil // in EOS non si committa un batch parziale in shutdown: replay pulito
		case <-ticker.C:
			if err := flush(); err != nil {
				if r.absorb(err, &batch) {
					continue
				}
				return err
			}
		default:
			rec, err := sess.Poll(ctx, r.spec.Consumer.PollTimeout)
			if err != nil {
				if r.absorb(err, &batch) {
					continue
				}
				return err
			}
			if rec == nil {
				continue
			}
			consumedTotal.WithLabelValues(r.spec.Name).Inc()
			batch = append(batch, rec)
			if len(batch) >= r.spec.Consumer.MaxBatchSize {
				if err := flush(); err != nil {
					if r.absorb(err, &batch) {
						continue
					}
					return err
				}
			}
		}
	}
}

// dlqRecords converte i record poison in ProducerRecord verso il deadletter-topic dello spec (usati
// dalla modalità transform per instradare a DLQ dentro la stessa transazione EOS). Ritorna errore se
// il topic DLQ non è configurato, così una richiesta di DeadLetter senza DLQ non perde dati.
func (r *runner) dlqRecords(recs []*message.Record, cause error) ([]*message.ProducerRecord, error) {
	if r.spec.Consumer.Deadletter() == "" {
		return nil, fmt.Errorf("processor %q: DeadLetter richiesto ma deadletter-topic assente", r.spec.Name)
	}
	return r.toDLQ(recs, cause), nil
}

// Header applicati ai record instradati al DLQ. Servono a diagnosticare (da dove veniva il record,
// perché è fallito, quando) e a riprocessare (il consumatore del DLQ sa il topic di origine e quante
// volte il record ci è già passato). Kafka-Delivery-Attempts riprende il nome usato da
// tpm-kafka-common, così gli strumenti di reprocessing esistenti lo riconoscono.
const (
	HeaderDLQSourceTopic     = "corekafka-dlq-source-topic"
	HeaderDLQSourcePartition = "corekafka-dlq-source-partition"
	HeaderDLQSourceOffset    = "corekafka-dlq-source-offset"
	HeaderDLQSourceTimestamp = "corekafka-dlq-source-timestamp"
	HeaderDLQProcessor       = "corekafka-dlq-processor"
	HeaderDLQError           = "corekafka-dlq-error"
	HeaderDLQErrorAt         = "corekafka-dlq-error-at"
	HeaderDeliveryAttempts   = "Kafka-Delivery-Attempts"
)

// toDLQ costruisce i ProducerRecord per il DLQ preservando payload e origine del record poison.
func (r *runner) toDLQ(recs []*message.Record, cause error) []*message.ProducerRecord {
	at := time.Now().UTC().Format(time.RFC3339Nano)
	out := make([]*message.ProducerRecord, 0, len(recs))
	for _, rec := range recs {
		h := make(map[string]string, len(rec.Headers)+8)
		maps.Copy(h, rec.Headers)
		h[HeaderDLQSourceTopic] = rec.Topic
		h[HeaderDLQSourcePartition] = strconv.Itoa(int(rec.Partition))
		h[HeaderDLQSourceOffset] = strconv.FormatInt(rec.Offset, 10)
		h[HeaderDLQProcessor] = r.spec.Name
		h[HeaderDLQErrorAt] = at
		if !rec.Timestamp.IsZero() {
			h[HeaderDLQSourceTimestamp] = rec.Timestamp.UTC().Format(time.RFC3339Nano)
		}
		if cause != nil {
			h[HeaderDLQError] = cause.Error()
		}
		// Un record già passato dal DLQ e reimmesso porta il contatore: incrementarlo permette a chi
		// riprocessa di fermarsi invece di girare all'infinito.
		h[HeaderDeliveryAttempts] = strconv.Itoa(attempts(rec.Headers) + 1)
		out = append(out, &message.ProducerRecord{Topic: r.spec.Consumer.Deadletter(), Key: rec.Key, Value: rec.Value, Headers: h})
	}
	return out
}

// attempts legge il contatore dei tentativi da un record; 0 se assente o illeggibile.
func attempts(headers map[string]string) int {
	n, err := strconv.Atoi(headers[HeaderDeliveryAttempts])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// resolveTopics assegna il DefaultOutputTopic ai record di output privi di Topic.
func resolveTopics(recs []*message.ProducerRecord, def string) {
	for _, r := range recs {
		if r.Topic == "" {
			r.Topic = def
		}
	}
}
