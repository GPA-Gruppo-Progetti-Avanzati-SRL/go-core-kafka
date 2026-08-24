package spec

import (
	"context"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

func TestProperties_Context(t *testing.T) {
	// senza properties nel ctx: mappa vuota, nome vuoto.
	if got := PropertiesFromContext(context.Background()); len(got) != 0 {
		t.Fatalf("attesa mappa vuota, ottenuto %v", got)
	}
	if got := ConsumerNameFromContext(context.Background()); got != "" {
		t.Fatalf("atteso nome vuoto, ottenuto %q", got)
	}

	ctx := ContextWithProperties(context.Background(), "condizione", core.Properties{"collection": "condizioni"})
	if got := PropertiesFromContext(ctx).GetString("collection", ""); got != "condizioni" {
		t.Fatalf("property da ctx errata: %q", got)
	}
	if got := ConsumerNameFromContext(ctx); got != "condizione" {
		t.Fatalf("nome consumer da ctx errato: %q", got)
	}
}
