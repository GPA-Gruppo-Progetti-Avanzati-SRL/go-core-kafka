// Package driver è l'astrazione client-agnostic di go-core-kafka. L'engine e il Producer pubblico
// dipendono SOLO da queste interfacce (mai dal client concreto): l'unica implementazione oggi è
// internal/confluentdriver (confluent-kafka-go). Aggiungere in futuro internal/franzdriver e cambiare
// la Factory di default (driversel.go nel package root) non impatta né l'engine né le app.
//
// Le interfacce usano i tipi neutri di message/spec, quindi nessun tipo del client Kafka attraversa
// questo confine. L'EOS è esposto come "sessione" (TransactSession) a un livello in cui sia il modello
// confluent (Begin/Produce/SendOffsetsToTransaction/Commit) sia quello franz-go
// (GroupTransactSession.Begin/Produce/End) si mappano senza attriti.
package driver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// GroupConsumer è un consumer di consumer-group per la modalità sink (at-least-once). Il driver tiene
// traccia internamente degli offset dei record ritornati da Poll dall'ultimo Commit; Commit li
// conferma (offset+1). Poll ritorna (nil, nil) allo scadere del timeout senza messaggi.
type GroupConsumer interface {
	Poll(ctx context.Context, timeout time.Duration) (*message.Record, error)
	Commit(ctx context.Context) error
	Close() error
}

// TransactSession è la sessione EOS Kafka->Kafka: consuma, produce e committa gli offset consumati in
// un'unica transazione. L'engine chiama Begin all'inizio di ogni batch, Produce per i record di
// output e Commit (atomico: record prodotti + offset consumati) o Abort in caso di errore.
type TransactSession interface {
	Poll(ctx context.Context, timeout time.Duration) (*message.Record, error)
	Begin() error
	Produce(ctx context.Context, recs []*message.ProducerRecord) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
	Close() error
}

// Producer è un producer non transazionale, usato per il DLQ (modalità sink) e come servizio pubblico.
type Producer interface {
	Produce(ctx context.Context, recs []*message.ProducerRecord) error
	Close() error
}

// Factory è l'unico punto legato all'implementazione del client. La Factory attiva è scelta a
// compile-time nel package root (driversel.go).
type Factory interface {
	NewGroupConsumer(s spec.ConsumerSpec, k spec.KafkaConfig) (GroupConsumer, error)
	NewTransactSession(s spec.ConsumerSpec, k spec.KafkaConfig) (TransactSession, error)
	NewProducer(k spec.KafkaConfig) (Producer, error)
}
