// Package driver è l'astrazione client-agnostic di go-core-kafka. L'engine e il Producer pubblico
// dipendono SOLO da queste interfacce (mai dal client concreto): l'unica implementazione oggi è
// internal/confluentdriver (confluent-kafka-go). Aggiungere in futuro internal/franzdriver e cambiare
// la Factory di default (driversel.go nel package root) non impatta né l'engine né le app.
//
// Le interfacce usano i tipi neutri di message/spec, quindi nessun tipo del client Kafka attraversa
// questo confine. L'EOS è esposto come "sessione" (TransactSession) a un livello in cui sia il modello
// confluent (Begin/Produce/SendOffsetsToTransaction/Commit) sia quello franz-go
// (GroupTransactSession.Begin/Produce/End) si mappano senza attriti.
//
// CONTRATTO SUGLI ERRORI: ogni errore ritornato da questi metodi è un *driver.Error con una Severity
// (vedi errors.go). È quella severità — non il tipo dell'errore sottostante, che l'engine non può
// ispezionare — a dire all'engine se scartare il batch, ricostruire il client o far terminare il
// processo. Un errore senza severità (SeverityBusiness) è per definizione un errore dell'app.
package driver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// GroupConsumer è un consumer di consumer-group per la modalità handle (at-least-once). Il driver tiene
// traccia internamente degli offset dei record ritornati da Poll dall'ultimo Commit; Commit li
// conferma (offset+1). Poll ritorna (nil, nil) allo scadere del timeout senza messaggi.
//
// Poll può ritornare un errore SeverityReset dopo un rebalance: gli offset tracciati sono stati
// scartati dal driver (le partizioni potrebbero non essere più assegnate) e l'engine deve scartare il
// batch in volo senza committare. Il consumer resta valido e il loop continua.
type GroupConsumer interface {
	Poll(ctx context.Context, timeout time.Duration) (*message.Record, error)
	Commit(ctx context.Context) error
	Close() error
}

// TransactSession è la sessione EOS Kafka->Kafka: consuma, produce e committa gli offset consumati in
// un'unica transazione. L'engine chiama Begin all'inizio di ogni batch, Produce per i record di
// output e Commit (atomico: record prodotti + offset consumati) o Abort in caso di errore.
//
// Vale lo stesso contratto di Poll di GroupConsumer. Un errore SeverityAbort chiede di abortire la
// transazione mantenendo la sessione; un SeverityFatal (tipicamente il fencing del producer dopo un
// rebalance) richiede di chiudere la sessione e ricostruirla.
type TransactSession interface {
	Poll(ctx context.Context, timeout time.Duration) (*message.Record, error)
	Begin() error
	Produce(ctx context.Context, recs []*message.ProducerRecord) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
	Close() error
}

// Producer è un producer non transazionale, usato per il DLQ (modalità handle) e come servizio pubblico.
type Producer interface {
	Produce(ctx context.Context, recs []*message.ProducerRecord) error
	Close() error
}

// Factory è l'unico punto legato all'implementazione del client. La Factory attiva è scelta a
// compile-time nel package root (driversel.go).
//
// NewGroupConsumer e NewTransactSession ricevono uno spec GIÀ RISOLTO (ProcessorSpec.Resolve): il
// tuning che serve loro sta in s.Consumer e s.Producer, e il driver non conosce l'eredità. Il
// producer condiviso del processo — quello del DLQ, che non appartiene a nessun processor — riceve
// invece il suo tuning direttamente, da `server.producer`.
type Factory interface {
	NewGroupConsumer(s spec.ProcessorSpec, k spec.KafkaServer) (GroupConsumer, error)
	NewTransactSession(s spec.ProcessorSpec, k spec.KafkaServer) (TransactSession, error)
	NewProducer(k spec.KafkaServer, p spec.ProducerTuning) (Producer, error)
}
