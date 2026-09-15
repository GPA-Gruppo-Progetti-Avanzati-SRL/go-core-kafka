package confluentdriver

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestTransactSession_AbortSenzaTransazioneApertaEUnNoOp(t *testing.T) {
	// Discard è chiamata su OGNI scarto di batch, anche prima del primo Begin (un errore risalito da
	// Poll). Senza il guard su txnOpen chiederebbe al client di abortire una transazione che non
	// esiste: errore di stato invalido al posto di un no-op, e su una sessione appena creata un nil
	// deref sul producer.
	// Il consumer serve perché Abort ora RIAVVOLGE il consumo prima di scartare gli offset: senza un
	// client la seek non avrebbe destinatario. NewConsumer non contatta il broker, e la seek su una
	// partizione non assegnata fallisce per-partizione — che è l'esito atteso e ignorato.
	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:9", "group.id": "test"})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close()

	s := &transactSession{groupSession: groupSession{name: "test", c: c, offsets: newOffsetTracker()}}
	s.offsets.track(tp("t", 0, 5))

	if err := s.Abort(context.Background()); err != nil {
		t.Fatalf("Abort senza transazione aperta = %v, atteso nil", err)
	}
	// Gli offset vanno scartati comunque: è l'altra metà del contratto di Discard.
	if !s.offsets.empty() {
		t.Error("Abort non ha scartato gli offset tracciati")
	}
	s.Discard(context.Background()) // non deve andare in panic
}
