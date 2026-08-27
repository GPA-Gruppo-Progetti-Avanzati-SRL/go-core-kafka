package confluentdriver

import (
	"context"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// reportWaitMargin è il margine concesso oltre al delivery.timeout.ms configurato: è il client che
// deve fallire il record per primo (con la sua causa, e con i suoi retry), non l'attesa lato Go.
// Quest'ultima è la rete di sicurezza per il caso in cui il report non arrivi affatto.
const reportWaitMargin = 5 * time.Second

// reportWait è il bound dell'attesa dei delivery report, derivato dal delivery-timeout del producer.
func reportWait(p spec.ProducerTuning) time.Duration {
	d := p.DeliveryTimeout
	if d <= 0 {
		d = spec.DefaultDeliveryTimeout
	}
	return d + reportWaitMargin
}

// produceAndAwait accoda i record e attende i loro delivery report. È condivisa dal producer
// condiviso del processo e dalla sessione transazionale: la disciplina di attesa è UNA — prima le due
// erano scritte due volte e divergevano già (il path transazionale non drenava i report residui).
func produceAndAwait(ctx context.Context, p *kafka.Producer, recs []*message.ProducerRecord, wait time.Duration) error {
	if len(recs) == 0 {
		return nil
	}
	// Il canale è per-chiamata e bufferizzato a len(recs): la goroutine di delivery del client non si
	// blocca mai scrivendoci, nemmeno se noi smettiamo di leggere per un errore o un timeout.
	deliveryChan := make(chan kafka.Event, len(recs))
	for _, r := range recs {
		if err := p.Produce(toMessage(r), deliveryChan); err != nil {
			return wrap("produce", err)
		}
	}
	return awaitReports(ctx, deliveryChan, len(recs), wait)
}

// awaitReports attende n delivery report, ritornando il PRIMO errore di consegna dopo aver drenato
// tutti i report (un errore su un record non dice nulla degli altri, e il loro esito va comunque
// letto prima di dichiarare fallito il batch).
//
// L'attesa ha due uscite oltre ai report: il context — che in shutdown è quello del drain, quindi
// limitato — e un timer. Senza di esse un report che non arriva (broker partizionato, delivery
// timeout non configurato) blocca la goroutine del consumer per sempre, e con lei l'OnStop che la
// attende: l'intero processo resta appeso a un SIGTERM.
func awaitReports(ctx context.Context, ch <-chan kafka.Event, n int, wait time.Duration) error {
	if wait <= 0 {
		wait = spec.DefaultDeliveryTimeout + reportWaitMargin
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	var firstErr error
	for got := 0; got < n; got++ {
		select {
		case ev := <-ch:
			if m, ok := ev.(*kafka.Message); ok && m.TopicPartition.Error != nil && firstErr == nil {
				firstErr = wrap("delivery", m.TopicPartition.Error)
			}
		case <-ctx.Done():
			// Retriable: i record senza report non sono né confermati né perduti, e il batch non va
			// committato. L'engine ricostruisce il client e li riproduce.
			return driver.NewError(driver.SeverityRetriable, "delivery",
				fmt.Errorf("attesa dei delivery report interrotta con %d/%d ricevuti: %w", got, n, ctx.Err()))
		case <-timer.C:
			return driver.NewError(driver.SeverityRetriable, "delivery",
				fmt.Errorf("delivery report non ricevuti entro %s: %d/%d", wait, got, n))
		}
	}
	return firstErr
}
