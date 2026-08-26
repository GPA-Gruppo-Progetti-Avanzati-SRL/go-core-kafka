// Package producer espone un Producer Kafka pubblico (idempotente / transazionale), usato dall'engine
// per il DLQ della modalità handle e disponibile alle app. È sottile: incapsula un driver.Producer, così
// nessun tipo del client concreto compare nella firma pubblica.
package producer

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"go.uber.org/fx"
)

// Producer è il servizio di produzione pubblico.
type Producer struct {
	d driver.Producer
}

// Produce invia i record e attende i delivery report.
func (p *Producer) Produce(ctx context.Context, recs []*message.ProducerRecord) *core.ApplicationError {
	if err := p.d.Produce(ctx, recs); err != nil {
		return core.TechnicalErrorWithError(err)
	}
	return nil
}

// NewProducer costruisce il Producer dalla Factory iniettata e registra la chiusura nel lifecycle fx.
// Una chiave riservata in kafka-properties fa fallire l'avvio (vedi spec.DeniedKafkaProperties).
func NewProducer(lc fx.Lifecycle, f driver.Factory, k spec.KafkaServer, p spec.ProducerTuning) (*Producer, error) {
	if err := spec.ValidateKafkaProperties("server.producer", p.KafkaProperties); err != nil {
		return nil, err
	}
	d, err := f.NewProducer(k, p.WithDefaults())
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { return d.Close() },
	})
	return &Producer{d: d}, nil
}

// Module registra il Producer pubblico. modes opzionale (mode-gating).
func Module(modes ...string) {
	core.Provide(NewProducer, modes...)
}
