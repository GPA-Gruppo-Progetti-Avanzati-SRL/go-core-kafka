// Package driver è l'astrazione client-agnostic di go-core-kafka. L'engine e il Producer pubblico
// dipendono SOLO da queste interfacce (mai dal client concreto). Le implementazioni sono due —
// internal/confluentdriver (confluent-kafka-go, CGo) e internal/franzdriver (twmb/franz-go, puro Go) —
// e quale sia attiva lo decide l'APP, importando il guscio driver/confluent o driver/franz e
// passandone la Driver a corekafka.WithDriver. Aggiungerne una terza non impatta engine né app.
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

// Session è ciò che l'engine usa in ENTRAMBE le modalità: consuma record, e sa buttare via quelli
// consumati senza confermarli. Il driver tiene traccia internamente degli offset dei record ritornati
// da Poll dall'ultimo Commit; Poll ritorna (nil, nil) allo scadere del timeout senza messaggi.
//
// Poll può ritornare un errore SeverityReset (rebalance) o SeverityAbort: l'engine scarta il batch in
// volo e chiama Discard. Il client resta valido e il loop continua.
type Session interface {
	Poll(ctx context.Context, timeout time.Duration) (*message.Record, error)

	// Discard scarta gli offset tracciati e non ancora committati — e in EOS abortisce la
	// transazione eventualmente aperta.
	//
	// È l'altra metà dello scarto di un batch: l'engine tronca la sua slice di record, ma se gli
	// offset restassero nel driver il Commit successivo li confermerebbe, dichiarando elaborati
	// record che nessuno ha elaborato. Il rebalance callback fa già questo azzeramento alla revoca,
	// ma un SeverityReset può risalire da Poll/Commit SENZA revoca (ErrIllegalGeneration,
	// ErrUnknownMemberID, ErrMaxPollExceeded): è per quei casi che l'engine deve poterlo chiedere.
	//
	// Non ritorna errore: non esiste un'alternativa che l'engine possa scegliere se lo scarto
	// fallisce, e un errore qui maschererebbe quello che ha reso necessario lo scarto.
	Discard(ctx context.Context)

	Close() error
}

// GroupConsumer è un consumer di consumer-group per la modalità handle (at-least-once). Commit
// conferma (offset+1) i record ritornati da Poll dall'ultimo Commit.
type GroupConsumer interface {
	Session
	Commit(ctx context.Context) error
}

// TransactSession è la sessione EOS Kafka->Kafka: consuma, produce e committa gli offset consumati in
// un'unica transazione. L'engine chiama Begin all'inizio di ogni batch, Produce per i record di
// output e Commit (atomico: record prodotti + offset consumati) o Abort in caso di errore.
//
// Un errore SeverityAbort chiede di abortire la transazione mantenendo la sessione; un SeverityFatal
// (tipicamente il fencing del producer dopo un rebalance) richiede di chiudere la sessione e
// ricostruirla.
type TransactSession interface {
	Session
	Begin(ctx context.Context) error
	Produce(ctx context.Context, recs []*message.ProducerRecord) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

// Producer è un producer non transazionale, usato per il DLQ (modalità handle) e come servizio pubblico.
type Producer interface {
	Produce(ctx context.Context, recs []*message.ProducerRecord) error
	Close() error
}

// Factory è l'unico punto legato all'implementazione del client. Quale Factory sia attiva lo decide
// l'app con corekafka.WithDriver: la scelta è un import, quindi il client non scelto non entra
// nemmeno nel binario.
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
