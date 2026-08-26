package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
)

// rec costruisce un record con chiave e valore, sulle coordinate progressive di un topic finto.
func rec(key, value string, offset int64) *message.Record {
	return &message.Record{Topic: "t", Partition: 0, Offset: offset, Key: []byte(key), Value: []byte(value)}
}

// identity è la conversione minima: un item per record, il valore come stringa.
func identity(r *message.Record) ([]string, error) { return []string{string(r.Value)}, nil }

func TestConvert_TombstoneNonArrivaAllaConversione(t *testing.T) {
	called := 0
	batch := []*message.Record{rec("a", "uno", 0), {Topic: "t", Key: []byte("b")}, rec("c", "tre", 2)}

	res := Convert(context.Background(), batch, NoCompact, func(r *message.Record) ([]string, error) {
		called++
		return identity(r)
	})

	if called != 2 {
		t.Errorf("conv chiamata %d volte, attese 2 (la tombstone non deve passarci)", called)
	}
	if res.Tombstones != 1 {
		t.Errorf("Tombstones = %d, atteso 1", res.Tombstones)
	}
	if len(res.Items) != 2 {
		t.Errorf("Items = %v, attesi 2 elementi", res.Items)
	}
	if err := res.DeadLetter(); err != nil {
		t.Errorf("DeadLetter() = %v, atteso nil: nessun poison", err)
	}
}

func TestConvert_PoisonPortaLaPropriaCausa(t *testing.T) {
	batch := []*message.Record{rec("a", "buono", 0), rec("b", "rotto", 1), rec("c", "pure-rotto", 2)}

	res := Convert(context.Background(), batch, NoCompact, func(r *message.Record) ([]string, error) {
		if string(r.Value) == "buono" {
			return identity(r)
		}
		return nil, fmt.Errorf("payload %q non valido", r.Value)
	})

	if len(res.Items) != 1 {
		t.Fatalf("Items = %v, atteso il solo record buono", res.Items)
	}
	if len(res.Poison) != 2 {
		t.Fatalf("Poison = %d record, attesi 2", len(res.Poison))
	}
	// La causa è di QUEL record, non una comune al gruppo: è ciò che finisce nel suo header DLQ.
	if got := res.Poison[0].Cause.Error(); got != `payload "rotto" non valido` {
		t.Errorf("causa del primo poison = %q", got)
	}
	if got := res.Poison[1].Cause.Error(); got != `payload "pure-rotto" non valido` {
		t.Errorf("causa del secondo poison = %q", got)
	}

	pr := &PoisonRecords{}
	if !errors.As(res.DeadLetter(), &pr) {
		t.Fatalf("DeadLetter() non è un *PoisonRecords")
	}
	if len(pr.Causes) != 2 || pr.CauseFor(1).Error() != `payload "pure-rotto" non valido` {
		t.Errorf("Causes = %v, attesa una causa per record", pr.Causes)
	}
	if pr.CauseFor(99) != pr.Cause {
		t.Errorf("CauseFor fuori range deve ricadere sulla causa comune")
	}
}

func TestConvert_SkipQuandoLaConversioneNonProduceNulla(t *testing.T) {
	batch := []*message.Record{rec("a", "uno", 0), rec("b", "niente", 1)}

	res := Convert(context.Background(), batch, NoCompact, func(r *message.Record) ([]string, error) {
		if string(r.Value) == "niente" {
			return nil, nil
		}
		return identity(r)
	})

	if res.Skipped != 1 {
		t.Errorf("Skipped = %d, atteso 1", res.Skipped)
	}
	if len(res.Items) != 1 || len(res.Poison) != 0 {
		t.Errorf("un (nil, nil) non è né un item né un poison: Items=%v Poison=%d", res.Items, len(res.Poison))
	}
}

func TestConvert_FanOutUnRecordPiuItem(t *testing.T) {
	batch := []*message.Record{rec("a", "uno", 0), rec("b", "due", 1)}

	res := Convert(context.Background(), batch, NoCompact, func(r *message.Record) ([]string, error) {
		return []string{string(r.Value) + "-vecchio", string(r.Value) + "-nuovo"}, nil
	})

	want := []string{"uno-vecchio", "uno-nuovo", "due-vecchio", "due-nuovo"}
	if fmt.Sprint(res.Items) != fmt.Sprint(want) {
		t.Errorf("Items = %v, attesi %v (ordine del batch, gruppi non spezzati)", res.Items, want)
	}
}

func TestConvert_CompactTieneLUltimoPerChiave(t *testing.T) {
	batch := []*message.Record{rec("a", "a1", 0), rec("b", "b1", 1), rec("a", "a2", 2)}

	res := Convert(context.Background(), batch, Compact, identity)

	// Ordine di PRIMA apparizione della chiave, contenuto dell'ULTIMO record.
	want := []string{"a2", "b1"}
	if fmt.Sprint(res.Items) != fmt.Sprint(want) {
		t.Errorf("Items = %v, attesi %v", res.Items, want)
	}
	if res.Compacted != 1 {
		t.Errorf("Compacted = %d, atteso 1", res.Compacted)
	}
}

func TestConvert_CompactNonToccaIRecordSenzaChiave(t *testing.T) {
	batch := []*message.Record{
		{Topic: "t", Offset: 0, Value: []byte("uno")},
		{Topic: "t", Offset: 1, Value: []byte("due")},
	}

	res := Convert(context.Background(), batch, Compact, identity)

	if len(res.Items) != 2 {
		t.Errorf("Items = %v: senza chiave non c'è identità, i record non vanno collassati", res.Items)
	}
	if res.Compacted != 0 {
		t.Errorf("Compacted = %d, atteso 0", res.Compacted)
	}
}

func TestConvert_NoCompactLasciaTuttiIDuplicati(t *testing.T) {
	batch := []*message.Record{rec("a", "a1", 0), rec("a", "a2", 1)}

	res := Convert(context.Background(), batch, NoCompact, identity)

	want := []string{"a1", "a2"}
	if fmt.Sprint(res.Items) != fmt.Sprint(want) {
		t.Errorf("Items = %v, attesi %v", res.Items, want)
	}
}

func TestConvert_BatchVuoto(t *testing.T) {
	res := Convert(context.Background(), nil, Compact, identity)
	if len(res.Items) != 0 || res.DeadLetter() != nil {
		t.Errorf("batch vuoto: Items=%v DeadLetter=%v", res.Items, res.DeadLetter())
	}
}
