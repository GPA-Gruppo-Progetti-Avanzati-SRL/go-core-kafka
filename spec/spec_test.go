package spec

import (
	"context"
	"testing"
	"time"
)

func TestProperties_Getters(t *testing.T) {
	p := Properties{"s": "hello", "n": "42", "b": "true", "d": "5s"}

	if !p.Has("s") || p.Has("missing") {
		t.Fatal("Has errato")
	}
	if p.GetString("s", "def") != "hello" || p.GetString("missing", "def") != "def" {
		t.Fatal("GetString errato")
	}
	if p.GetInt("n", -1) != 42 || p.GetInt("missing", -1) != -1 || p.GetInt("s", -1) != -1 {
		t.Fatal("GetInt errato")
	}
	if p.GetBool("b", false) != true || p.GetBool("missing", true) != true {
		t.Fatal("GetBool errato")
	}
	if p.GetDuration("d", 0) != 5*time.Second || p.GetDuration("missing", time.Minute) != time.Minute {
		t.Fatal("GetDuration errato")
	}
}

func TestProperties_Context(t *testing.T) {
	// senza properties nel ctx: mappa vuota, nome vuoto.
	if got := PropertiesFromContext(context.Background()); len(got) != 0 {
		t.Fatalf("attesa mappa vuota, ottenuto %v", got)
	}
	if got := ConsumerNameFromContext(context.Background()); got != "" {
		t.Fatalf("atteso nome vuoto, ottenuto %q", got)
	}

	ctx := ContextWithProperties(context.Background(), "condizione", Properties{"collection": "condizioni"})
	if got := PropertiesFromContext(ctx).GetString("collection", ""); got != "condizioni" {
		t.Fatalf("property da ctx errata: %q", got)
	}
	if got := ConsumerNameFromContext(ctx); got != "condizione" {
		t.Fatalf("nome consumer da ctx errato: %q", got)
	}
}
