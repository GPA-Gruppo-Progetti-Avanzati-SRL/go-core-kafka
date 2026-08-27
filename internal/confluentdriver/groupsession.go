package confluentdriver

import (
	"context"
	"errors"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// groupSession è la parte comune ai due client del driver: il consumer sottoscritto, il tracker degli
// offset e l'observer del rebalance. Poll e Discard vivono qui perché la disciplina sugli offset è
// identica nelle due modalità — cambia solo COME li si conferma (commit diretto in handle,
// SendOffsetsToTransaction in EOS), e quello sta nei due tipi che la embeddano.
type groupSession struct {
	name    string
	c       *kafka.Consumer
	offsets *offsetTracker
	rb      *rebalanceObserver
}

// Poll ritorna il prossimo messaggio, (nil, nil) allo scadere del timeout senza messaggi, o un errore
// SeverityReset se nel frattempo è avvenuto un rebalance (vedi rebalance.go).
//
// Il context non è osservato QUI di proposito: ReadMessage è una chiamata CGo bloccante che non lo
// accetta, e il suo bound è `timeout` (consumer.poll-timeout, 100ms di default). È il loop chiamante
// a osservare la cancellazione, fra un poll e il successivo. Il parametro resta nella firma perché
// appartiene al seam driver.Session, e un driver puramente Go (franz-go) potrà onorarlo.
func (g *groupSession) Poll(_ context.Context, timeout time.Duration) (*message.Record, error) {
	msg, err := g.c.ReadMessage(timeout)
	if err != nil {
		var ke kafka.Error
		if errors.As(err, &ke) && ke.Code() == kafka.ErrTimedOut {
			// Il rebalance può essere avvenuto durante questo poll a vuoto: va segnalato comunque,
			// perché il batch accumulato prima resta da scartare.
			if g.rb.takeRevoked() {
				return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
			}
			return nil, nil
		}
		return nil, wrap("poll", err)
	}
	if g.rb.takeRevoked() {
		// Questo record appartiene già alla nuova assegnazione, ma il batch accumulato prima del
		// rebalance no. Lo scartiamo senza tracciarne l'offset: verrà riletto dopo che l'engine ha
		// buttato il batch — un duplicato, non un buco.
		return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	}
	g.offsets.track(msg.TopicPartition)
	return toRecord(msg), nil
}

// Discard scarta gli offset tracciati e non committati. L'engine la chiama quando butta il batch in
// volo: senza, il Commit successivo confermerebbe record che nessuno ha elaborato (vedi il contratto
// di driver.Session.Discard). La sessione transazionale la estende con l'abort della transazione.
func (g *groupSession) Discard(context.Context) {
	g.offsets.reset()
}
