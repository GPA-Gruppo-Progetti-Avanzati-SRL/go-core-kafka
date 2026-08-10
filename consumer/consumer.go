// Package consumer è l'engine di go-core-kafka: per ogni ConsumerSpec attivo avvia una goroutine che
// consuma dal client (dietro internal/driver) ed esegue il Handler (modalità handle, at-least-once) o
// il Transformer (modalità transform, EOS Kafka->Kafka). Non importa mai il client concreto: usa solo
// le interfacce driver.* ottenute dalla driver.Factory iniettata.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Consumers è il valore fx che tiene vivo l'engine (nessuna API pubblica: la sua costruzione avvia i
// consumer via lifecycle).
type Consumers struct{}

type params struct {
	fx.In
	LC           fx.Lifecycle
	Shutdowner   fx.Shutdowner
	Specs        []spec.ConsumerSpec
	Kafka        spec.KafkaConfig
	Factory      driver.Factory
	Handlers     []processor.HandlerRegistration     `group:"kafka_handlers"`
	Transformers []processor.TransformerRegistration `group:"kafka_transformers"`
	DLQ          *producer.Producer                  `optional:"true"`
}

// runner incapsula uno spec e il suo processor; apre il proprio client all'avvio.
type runner struct {
	spec        spec.ConsumerSpec
	kafka       spec.KafkaConfig
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

	runners := make([]*runner, 0, len(p.Specs))
	for _, raw := range p.Specs {
		s := raw.WithDefaults()
		r := &runner{spec: s, kafka: p.Kafka, factory: p.Factory, dlq: p.DLQ}
		switch s.Mode {
		case spec.ModeHandle:
			h, ok := handlers[s.Name]
			if !ok {
				return nil, fmt.Errorf("consumer %q (mode=handle): nessun Handler registrato (usare corekafka.RegisterHandler con questo nome)", s.Name)
			}
			r.handler = h
			if s.OnError == spec.OnErrorDeadletter && s.DeadletterTopic == "" {
				return nil, fmt.Errorf("consumer %q: on-error=deadletter richiede deadletter-topic", s.Name)
			}
			// Il DLQ (via Producer) serve sia per la policy di default deadletter sia quando l'handler
			// sceglie il deadletter a runtime (processor.DeadLetter): se c'è un deadletter-topic, il
			// Producer deve essere presente.
			if s.HasDeadletter() && p.DLQ == nil {
				return nil, fmt.Errorf("consumer %q: deadletter-topic impostato richiede il Producer (usare corekafka.WithProducer)", s.Name)
			}
		case spec.ModeTransform:
			t, ok := transformers[s.Name]
			if !ok {
				return nil, fmt.Errorf("consumer %q (mode=transform): nessun Transformer registrato (usare processor.RegisterTransformer con questo nome)", s.Name)
			}
			if s.TransactionalID == "" {
				return nil, fmt.Errorf("consumer %q (mode=transform): transactional-id obbligatorio", s.Name)
			}
			r.transformer = t
		default:
			return nil, fmt.Errorf("consumer %q: mode %q non valido (handle|transform)", s.Name, s.Mode)
		}

		// Configure opzionale: passa le Properties dello spec all'handler/transformer che le vuole
		// all'avvio (precompute/validazione). Un errore fa fail-fast: l'app non parte.
		if c, ok := r.handler.(processor.Configurable); ok {
			if err := c.Configure(s.Properties); err != nil {
				return nil, fmt.Errorf("consumer %q: Configure: %w", s.Name, err)
			}
		}
		if c, ok := r.transformer.(processor.Configurable); ok {
			if err := c.Configure(s.Properties); err != nil {
				return nil, fmt.Errorf("consumer %q: Configure: %w", s.Name, err)
			}
		}

		runners = append(runners, r)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var done chan struct{}

	p.LC.Append(fx.Hook{
		OnStart: func(context.Context) error {
			done = make(chan struct{}, len(runners))
			for _, r := range runners {
				go func(r *runner) {
					defer func() { done <- struct{}{} }()
					if err := r.run(ctx); err != nil && ctx.Err() == nil {
						log.Error().Err(err).Str("consumer", r.spec.Name).Msg("corekafka: consumer terminato con errore, arresto dell'applicazione")
						_ = p.Shutdowner.Shutdown()
					}
				}(r)
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

// run arricchisce il ctx con le Properties/nome del consumer (leggibili dalla business logic) e
// smista sulla modalità dello spec.
func (r *runner) run(ctx context.Context) error {
	ctx = spec.ContextWithProperties(ctx, r.spec.Name, r.spec.Properties)
	if r.spec.Mode == spec.ModeTransform {
		return r.runTransform(ctx)
	}
	return r.runHandle(ctx)
}

// sendDeadletter produce i record sul topic DLQ dello spec. Ritorna errore (→ fail-fast) se il DLQ
// non è configurato, così una richiesta di deadletter senza DLQ non perde silenziosamente i dati.
func (r *runner) sendDeadletter(ctx context.Context, recs []*message.Record, cause error) error {
	if r.dlq == nil || r.spec.DeadletterTopic == "" {
		return fmt.Errorf("consumer %q: deadletter richiesto ma non configurato (manca deadletter-topic/Producer): %w", r.spec.Name, cause)
	}
	if appErr := r.dlq.Produce(ctx, toDLQ(r.spec.DeadletterTopic, recs, cause)); appErr != nil {
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

	ticker := time.NewTicker(r.spec.CutFrequency)
	defer ticker.Stop()
	batch := make([]*message.Record, 0, r.spec.MaxBatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		start := time.Now()
		hErr := r.handler.Handle(ctx, batch)
		batchDuration.WithLabelValues(r.spec.Name).Observe(time.Since(start).Seconds())

		commitReset := func() error {
			if err := gc.Commit(ctx); err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}

		switch {
		case hErr == nil:
			processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch)))
			return commitReset()

		case errors.Is(hErr, processor.ErrFailFast):
			// L'handler ha scelto esplicitamente il fail-fast: niente commit → replay.
			return hErr
		}

		// L'handler ha scelto il deadletter per record specifici (processor.DeadLetter): i record
		// buoni sono già stati elaborati; instrada i poison al DLQ e committa il batch.
		var pr *processor.PoisonRecords
		if errors.As(hErr, &pr) {
			if err := r.sendDeadletter(ctx, pr.Records, pr.Cause); err != nil {
				return err // DLQ non configurato/irraggiungibile → replay
			}
			deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(pr.Records)))
			processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch) - len(pr.Records)))
			return commitReset()
		}

		// Errore generico → policy di DEFAULT dello spec (l'handler non ha scelto).
		if r.spec.OnError == spec.OnErrorDeadletter {
			if err := r.sendDeadletter(ctx, batch, hErr); err != nil {
				return err
			}
			deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch)))
			return commitReset()
		}
		// default fail-fast: non committare → replay.
		return hErr
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush()
			return nil
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		default:
			rec, err := gc.Poll(ctx, spec.DefaultPollTimeout)
			if err != nil {
				return err
			}
			if rec == nil {
				continue
			}
			consumedTotal.WithLabelValues(r.spec.Name).Inc()
			batch = append(batch, rec)
			if len(batch) >= r.spec.MaxBatchSize {
				if err := flush(); err != nil {
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

	ticker := time.NewTicker(r.spec.CutFrequency)
	defer ticker.Stop()
	batch := make([]*message.Record, 0, r.spec.MaxBatchSize)

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

		// Stesso modello a esiti della modalità handle, ma la consegna DLQ avviene DENTRO la sessione
		// transazionale (append agli output) così l'EOS resta intatto: output "buoni" + record DLQ +
		// commit offset sono atomici.
		var pr *processor.PoisonRecords
		switch {
		case tErr == nil:
			// solo output
		case errors.As(tErr, &pr):
			// DeadLetter gestito: produce gli output E instrada i poison al DLQ, poi committa.
			dlq, derr := r.dlqRecords(pr.Records, pr.Cause)
			if derr != nil {
				_ = sess.Abort(ctx)
				return derr
			}
			out = append(out, dlq...)
			deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(pr.Records)))
		case errors.Is(tErr, processor.ErrFailFast):
			_ = sess.Abort(ctx)
			return tErr
		default:
			// Errore generico → policy di default dello spec.
			if r.spec.OnError == spec.OnErrorDeadletter {
				dlq, derr := r.dlqRecords(batch, tErr)
				if derr != nil {
					_ = sess.Abort(ctx)
					return derr
				}
				out = append(out, dlq...)
				deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch)))
			} else {
				_ = sess.Abort(ctx) // fail-fast (default)
				return tErr
			}
		}

		if err := sess.Produce(ctx, out); err != nil {
			_ = sess.Abort(ctx)
			return err
		}
		if err := sess.Commit(ctx); err != nil {
			_ = sess.Abort(ctx)
			return err
		}
		producedTotal.WithLabelValues(r.spec.Name).Add(float64(len(out)))
		processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch)))
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil // in EOS non si committa un batch parziale in shutdown: replay pulito
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		default:
			rec, err := sess.Poll(ctx, spec.DefaultPollTimeout)
			if err != nil {
				return err
			}
			if rec == nil {
				continue
			}
			consumedTotal.WithLabelValues(r.spec.Name).Inc()
			batch = append(batch, rec)
			if len(batch) >= r.spec.MaxBatchSize {
				if err := flush(); err != nil {
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
	if r.spec.DeadletterTopic == "" {
		return nil, fmt.Errorf("consumer %q: DeadLetter richiesto ma deadletter-topic assente", r.spec.Name)
	}
	return toDLQ(r.spec.DeadletterTopic, recs, cause), nil
}

// toDLQ costruisce i ProducerRecord per il DLQ preservando payload e origine del record poison.
func toDLQ(topic string, recs []*message.Record, cause error) []*message.ProducerRecord {
	out := make([]*message.ProducerRecord, 0, len(recs))
	for _, r := range recs {
		h := make(map[string]string, len(r.Headers)+2)
		for k, v := range r.Headers {
			h[k] = v
		}
		h["corekafka-dlq-source-topic"] = r.Topic
		if cause != nil {
			h["corekafka-dlq-error"] = cause.Error()
		}
		out = append(out, &message.ProducerRecord{Topic: topic, Key: r.Key, Value: r.Value, Headers: h})
	}
	return out
}

// resolveTopics assegna il DefaultOutputTopic ai record di output privi di Topic.
func resolveTopics(recs []*message.ProducerRecord, def string) {
	for _, r := range recs {
		if r.Topic == "" {
			r.Topic = def
		}
	}
}
