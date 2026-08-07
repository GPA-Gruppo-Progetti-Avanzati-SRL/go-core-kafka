// Package batchsink fornisce l'helper generico della modalità sink: trasforma un Mapper[Op]
// (la business logic per-record dell'app) + un Sink[Op] (backend di scrittura pluggable, iniettato
// via corekafka.WithSink) in un processor.Handler. Cattura il pattern dei due spooler di riferimento
// (tpm-spooler, condizioni-k2m): per ogni record -> operazione + chiave di dedup; dedup last-wins nel
// batch; flush unico sul sink. La business logic dell'app resta il solo Mapper, sink-agnostica.
package batchsink

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
)

// Sink[Op] è il backend di scrittura pluggable (mongo/sql/...). Flush riceve le operazioni del batch
// già dedotte (nell'ordine di prima occorrenza della chiave). Un errore di Flush fa fallire l'Handle
// -> l'engine non committa gli offset e il batch viene riprocessato (idempotenza a carico del sink).
type Sink[Op any] interface {
	Flush(ctx context.Context, ops []Op) error
}

// Mapper[Op] è la business logic per-record: dato un Record produce l'operazione da scrivere sul sink
// e la sua chiave di dedup. skip=true scarta il record; err marca il record come "poison" (l'engine
// lo instrada a DLQ o esce, secondo la policy on-error dello spec).
type Mapper[Op any] func(ctx context.Context, r *message.Record) (op Op, dedupKey string, skip bool, err error)

// BatchSpooler[Op] implementa processor.Handler a partire da Map + Sink.
type BatchSpooler[Op any] struct {
	Map  Mapper[Op]
	Sink Sink[Op]
}

// Handle mappa il batch (saltando e raccogliendo i record poison, senza abortire), dedup last-wins
// per chiave, poi Flush sul sink dei soli record buoni. Semantica d'errore verso l'engine:
//   - Flush fallito -> ritorna l'errore "grezzo" (transiente): l'engine non committa -> replay;
//   - solo record poison -> ritorna *processor.PoisonRecords: l'engine li instrada a DLQ e committa;
//   - tutto ok -> nil.
func (b *BatchSpooler[Op]) Handle(ctx context.Context, batch []*message.Record) error {
	index := make(map[string]int, len(batch))
	ops := make([]Op, 0, len(batch))
	var poison []*message.Record

	for _, r := range batch {
		op, key, skip, err := b.Map(ctx, r)
		if err != nil {
			poison = append(poison, r)
			continue
		}
		if skip {
			continue
		}
		if key == "" {
			ops = append(ops, op) // senza chiave ogni operazione è distinta
			continue
		}
		if pos, ok := index[key]; ok {
			ops[pos] = op // last-wins
			continue
		}
		index[key] = len(ops)
		ops = append(ops, op)
	}

	if len(ops) > 0 {
		if err := b.Sink.Flush(ctx, ops); err != nil {
			return err // transiente: nessun commit
		}
	}
	if len(poison) > 0 {
		return &processor.PoisonRecords{Records: poison}
	}
	return nil
}

// Register wira un BatchSpooler[Op] come Handler per il consumer indicato: il Mapper è fornito
// dall'app, il Sink[Op] è risolto da fx (fornito da un WithSink, es. mongospooler.Module).
//
//	batchsink.Register[mongo.WriteModel]("condizione", condizioneMapper)
func Register[Op any](consumerName string, mapper Mapper[Op], modes ...string) {
	processor.Provide(func(s Sink[Op]) processor.HandlerRegistration {
		return processor.HandlerRegistration{
			Consumer: consumerName,
			Handler:  &BatchSpooler[Op]{Map: mapper, Sink: s},
		}
	}, modes...)
}

// compile-time: BatchSpooler implementa processor.Handler.
var _ processor.Handler = (*BatchSpooler[int])(nil)
