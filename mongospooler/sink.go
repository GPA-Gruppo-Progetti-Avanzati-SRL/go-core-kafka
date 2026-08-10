// Package mongospooler è il backend Sink[mongo.WriteModel] per la modalità sink: scrive il batch
// dedotto con una singola BulkWrite. È opt-in (passato a corekafka.WithSink), così solo le app che lo
// usano trascinano mongo-driver. L'app fornisce una CollectionFunc che risolve la *mongo.Collection di
// destinazione: è invocata la prima volta alla Flush (non alla costruzione fx), perché il client
// Mongo (es. il LinkedService di go-core-mongo) si connette solo a lifecycle avviato (OnStart).
package mongospooler

import (
	"context"
	"fmt"
	"sync"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/batchsink"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// CollectionFunc risolve (lazy) la *mongo.Collection di destinazione del sink.
type CollectionFunc func() *mongo.Collection

// sink implementa batchsink.Sink[mongo.WriteModel] su una collezione MongoDB risolta lazy.
type sink struct {
	get  CollectionFunc
	once sync.Once
	coll *mongo.Collection
}

// Flush esegue una BulkWrite delle operazioni del batch. Un errore fa fallire il flush -> l'engine non
// committa gli offset e il batch viene riprocessato (idempotenza a carico delle WriteModel, es. upsert).
func (s *sink) Flush(ctx context.Context, ops []mongo.WriteModel) error {
	if len(ops) == 0 {
		return nil
	}
	s.once.Do(func() { s.coll = s.get() })
	if s.coll == nil {
		return fmt.Errorf("mongospooler: collezione di destinazione non disponibile")
	}
	_, err := s.coll.BulkWrite(ctx, ops)
	return err
}

func newSink(get CollectionFunc) batchsink.Sink[mongo.WriteModel] {
	return &sink{get: get}
}

// Module registra il Sink Mongo. Richiede via fx una mongospooler.CollectionFunc (fornita dall'app).
// modes opzionale (mode-gating).
func Module(modes ...string) {
	core.Provide(newSink, modes...)
}
