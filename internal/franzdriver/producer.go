package franzdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceClient è il producer di franz-go visto dal driver (lo implementa *kgo.Client): interfaccia
// per poter verificare senza broker la disciplina di chiusura, che è la parte con una conseguenza
// (i record ancora in coda allo scadere del flush sono persi).
type produceClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Flush(ctx context.Context) error
	Close()
}

// producer implementa driver.Producer (non transazionale): alimenta il DLQ della modalità handle.
type producer struct {
	cl           produceClient
	flushTimeout time.Duration
}

// Produce invia i record e ne attende l'esito. ProduceSync ritorna a delivery report ricevuti, quindi
// il batch è confermato (o fallito) quando la funzione ritorna: l'attesa è limitata dal
// RecordDeliveryTimeout del client e dal context, mai illimitata.
func (p *producer) Produce(ctx context.Context, recs []*message.ProducerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	krecs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		krecs = append(krecs, toKgoRecord(r))
	}
	return wrap("produce", p.cl.ProduceSync(ctx, krecs...).FirstErr())
}

// Close concede al producer il flush timeout configurato per svuotare la coda prima di chiudere:
// quello che resta in coda allo scadere è perso, quindi il valore va commisurato al volume prodotto.
func (p *producer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.flushTimeout)
	defer cancel()
	err := p.cl.Flush(ctx)
	p.cl.Close()
	return wrap("flush", err)
}
