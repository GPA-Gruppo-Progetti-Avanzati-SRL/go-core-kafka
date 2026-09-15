package franzdriver

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Scarto EOS senza transazione aperta: il consumo va riavvolto agli ultimi offset committati. Con una
// transazione aperta lo fa End(TryAbort); senza, nessuno lo farebbe — e quei record, già usciti dal
// client e mai elaborati (la transazione non ha prodotto nulla), non li rileggerebbe nessuno.
func TestAbort_SenzaTransazioneApertaRiavvolgeAgliOffsetCommittati(t *testing.T) {
	cl := &fakeTxnClient{committedOffsets: map[string]map[int32]kgo.EpochOffset{
		"t": {0: {Epoch: 1, Offset: 42}},
	}}
	s := &transactSession{session: session{name: "test", p: &fakePoller{}, rb: &rebalanceObserver{name: "test"}}, sess: cl}

	if err := s.Abort(context.Background()); err != nil {
		t.Fatalf("Abort = %v, atteso nil", err)
	}
	if len(cl.ends) != 0 {
		t.Error("chiamata End senza transazione aperta")
	}
	if len(cl.setOffsets) != 1 {
		t.Fatalf("riavvolgimenti = %d, atteso 1", len(cl.setOffsets))
	}
	if got := cl.setOffsets[0]["t"][0].Offset; got != 42 {
		t.Errorf("riavvolto a %d, atteso 42 (ultimo offset committato)", got)
	}
}

// Con una transazione aperta il riavvolgimento NON va fatto a mano: lo fa End(TryAbort), e farlo due
// volte significherebbe riportare indietro il consumo di un batch che franz ha già ripristinato.
func TestAbort_ConTransazioneApertaNonRiavvolgeAMano(t *testing.T) {
	cl := &fakeTxnClient{committedOffsets: map[string]map[int32]kgo.EpochOffset{
		"t": {0: {Epoch: 1, Offset: 42}},
	}}
	s := &transactSession{session: session{name: "test", p: &fakePoller{}, rb: &rebalanceObserver{name: "test"}}, sess: cl, txnOpen: true}

	if err := s.Abort(context.Background()); err != nil {
		t.Fatalf("Abort = %v, atteso nil", err)
	}
	if len(cl.ends) != 1 || cl.ends[0] != kgo.TryAbort {
		t.Fatalf("End = %v, atteso un TryAbort", cl.ends)
	}
	if len(cl.setOffsets) != 0 {
		t.Error("riavvolgimento manuale eseguito nonostante End(TryAbort) lo faccia già")
	}
}
