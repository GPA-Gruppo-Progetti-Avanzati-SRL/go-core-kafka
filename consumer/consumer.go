// Package consumer è l'engine di go-core-kafka: per ogni ConsumerSpec attivo avvia una goroutine che
// consuma dal client (dietro internal/driver) ed esegue il Handler (modalità sink, at-least-once) o
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
		case spec.ModeSink:
			h, ok := handlers[s.Name]
			if !ok {
				return nil, fmt.Errorf("consumer %q (mode=sink): nessun Handler registrato (usare processor.Register/batchsink.Register con questo nome)", s.Name)
			}
			r.handler = h
			if s.OnError == spec.OnErrorDeadletter {
				if s.DeadletterTopic == "" {
					return nil, fmt.Errorf("consumer %q: on-error=deadletter richiede deadletter-topic", s.Name)
				}
				if p.DLQ == nil {
					return nil, fmt.Errorf("consumer %q: on-error=deadletter richiede il Producer DLQ (usare corekafka.WithProducer)", s.Name)
				}
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
			return nil, fmt.Errorf("consumer %q: mode %q non valido (sink|transform)", s.Name, s.Mode)
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

// run smista sulla modalità dello spec.
func (r *runner) run(ctx context.Context) error {
	if r.spec.Mode == spec.ModeTransform {
		return r.runTransform(ctx)
	}
	return r.runSink(ctx)
}

// runSink: modalità at-least-once. poll -> accumula -> (cut size/tempo) -> Handle -> commit dopo il sink.
func (r *runner) runSink(ctx context.Context) error {
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

		if hErr == nil {
			processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch)))
			if err := gc.Commit(ctx); err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}

		var pr *processor.PoisonRecords
		if errors.As(hErr, &pr) {
			// I record buoni sono già stati scritti sul sink; gestisci i poison.
			if r.spec.OnError == spec.OnErrorFailFast {
				return hErr
			}
			if err := r.dlq.Produce(ctx, toDLQ(r.spec.DeadletterTopic, pr.Records, pr.Cause)); err != nil {
				return err // DLQ irraggiungibile -> transiente: replay
			}
			deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(pr.Records)))
			processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch) - len(pr.Records)))
			if err := gc.Commit(ctx); err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}

		// Errore transiente (es. sink irraggiungibile): non committare, forza il replay.
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
		if tErr != nil {
			_ = sess.Abort(ctx)
			return tErr
		}
		resolveTopics(out, r.spec.DefaultOutputTopic)
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
