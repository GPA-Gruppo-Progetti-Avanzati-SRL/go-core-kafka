// Package mongospooler è il backend Sink[mongo.WriteModel] per la modalità sink: scrive il batch
// dedotto con una singola BulkWrite. È opt-in (passato a corekafka.WithSink), così solo le app che lo
// usano trascinano mongo-driver. L'app fornisce a fx la *mongo.Collection di destinazione (tipicamente
// ottenuta dal LinkedService di go-core-mongo).
package mongospooler

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/batchsink"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// sink implementa batchsink.Sink[mongo.WriteModel] su una collezione MongoDB.
type sink struct {
	coll *mongo.Collection
}

// Flush esegue una BulkWrite delle operazioni del batch. Un errore fa fallire il flush -> l'engine non
// committa gli offset e il batch viene riprocessato (idempotenza a carico delle WriteModel, es. upsert).
func (s *sink) Flush(ctx context.Context, ops []mongo.WriteModel) error {
	if len(ops) == 0 {
		return nil
	}
	_, err := s.coll.BulkWrite(ctx, ops)
	return err
}

func newSink(coll *mongo.Collection) batchsink.Sink[mongo.WriteModel] {
	return &sink{coll: coll}
}

// Module registra il Sink Mongo. Richiede via fx una *mongo.Collection (fornita dall'app). modes
// opzionale (mode-gating).
func Module(modes ...string) {
	core.Provide(newSink, modes...)
}
