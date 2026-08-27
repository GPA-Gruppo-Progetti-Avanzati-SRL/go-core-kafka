// Package producer incapsula il producer Kafka non transazionale (idempotente) con cui l'engine
// alimenta il DLQ della modalità handle. È sottile: avvolge un driver.Producer, così nessun tipo del
// client concreto compare nella firma.
//
// NON è iniettabile dall'app: corekafka.Module registra questo Module dentro un core.ModuleClosed,
// che rende privati i suoi Provide. È deliberato — un consumer che deve produrre lo fa con un
// Transformer (EOS Kafka→Kafka), che è il seam previsto e l'unico con garanzie transazionali.
package producer

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"go.uber.org/fx"
)

// Producer è il servizio di produzione usato dall'engine (vedi il doc del package).
type Producer struct {
	d driver.Producer
}

// Produce invia i record e attende i delivery report.
func (p *Producer) Produce(ctx context.Context, recs []*message.ProducerRecord) *core.ApplicationError {
	if err := p.d.Produce(ctx, recs); err != nil {
		return core.TechnicalError().WithCause(err)
	}
	return nil
}

// NewProducer costruisce il Producer dalla Factory iniettata e registra la chiusura nel lifecycle fx.
// Una chiave riservata in kafka-properties fa fallire l'avvio (vedi spec.DeniedKafkaProperties).
func NewProducer(lc fx.Lifecycle, f driver.Factory, k spec.KafkaServer, p spec.ProducerTuning) (*Producer, error) {
	// Stessa validazione dell'engine, dalla stessa funzione: il Producer è registrabile anche da solo
	// (senza consumer), quindi non può appoggiarsi al fatto che qualcun altro l'abbia già fatta.
	if err := spec.ValidateServerProperties(k); err != nil {
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
