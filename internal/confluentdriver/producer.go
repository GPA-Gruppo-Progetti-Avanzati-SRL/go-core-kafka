package confluentdriver

import (
	"context"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// producer implementa driver.Producer (non transazionale): usato per il DLQ e come servizio pubblico.
type producer struct {
	p            *kafka.Producer
	flushTimeout int           // ms, da ProducerTuning.FlushTimeout
	reportWait   time.Duration // bound dell'attesa dei delivery report
}

// Produce invia i record e attende i delivery report (vedi produceAndAwait).
func (p *producer) Produce(ctx context.Context, recs []*message.ProducerRecord) error {
	return produceAndAwait(ctx, p.p, recs, p.reportWait)
}

// Close concede al producer il flush timeout configurato per svuotare la coda di invio prima di
// chiudere: quello che resta in coda allo scadere è perso, quindi il valore va commisurato al
// volume prodotto.
func (p *producer) Close() error {
	remaining := p.p.Flush(p.flushTimeout)
	p.p.Close()
	if remaining > 0 {
		return fmt.Errorf("confluentdriver: flush incompleto alla chiusura: %d record ancora in coda dopo %dms", remaining, p.flushTimeout)
	}
	return nil
}
