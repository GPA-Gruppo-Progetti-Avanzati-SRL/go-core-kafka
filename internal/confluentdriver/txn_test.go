package confluentdriver

import (
	"context"
	"testing"
)

// Il producer concreto di confluent-kafka-go non è fakeable (è un tipo, non un'interfaccia, e chiede
// un broker), quindi qui si verifica la sola parte con una conseguenza che NON richiede il client: il
// guard su `open`. È lo stesso invariante che TestTransactSession_AbortSenzaTransazioneApertaEUnNoOp
// copre per la sessione EOS — ora entrambi passano dalla stessa txn.
func TestTxn_AbortSenzaTransazioneApertaNonToccaIlClient(t *testing.T) {
	// p è nil: se il guard non ci fosse, AbortTransaction andrebbe in nil deref invece di essere il
	// no-op che il chiamante si aspetta (Discard è invocata su OGNI scarto di batch, anche prima del
	// primo Begin).
	var tx txn

	if err := tx.abort(context.Background()); err != nil {
		t.Fatalf("abort senza transazione aperta = %v, atteso nil", err)
	}
	if tx.open {
		t.Error("abort ha marcato la transazione come aperta")
	}
}
