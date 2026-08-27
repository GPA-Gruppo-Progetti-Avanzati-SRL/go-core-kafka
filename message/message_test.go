package message

import "testing"

func TestHeaders_ChiaviRipetute(t *testing.T) {
	// La ragione d'essere del tipo: Kafka ammette chiavi ripetute, una mappa le collassa.
	h := Headers{
		{Key: "trace", Value: []byte("uno")},
		{Key: "trace", Value: []byte("due")},
		{Key: "altro", Value: []byte("x")},
	}
	if got := h.Get("trace"); got != "uno" {
		t.Errorf("Get = %q, atteso il PRIMO valore", got)
	}
	if got := h.Values("trace"); len(got) != 2 || got[0] != "uno" || got[1] != "due" {
		t.Errorf("Values = %v, attesi entrambi i valori nell'ordine del messaggio", got)
	}
	if got := h.Values("assente"); got != nil {
		t.Errorf("Values su chiave assente = %v, atteso nil", got)
	}
}

func TestHeaders_GetEHasDistinguonoIlValoreVuoto(t *testing.T) {
	// Get non può distinguere "assente" da "presente e vuoto": è per questo che Has esiste.
	h := Headers{{Key: "vuoto", Value: nil}}
	if h.Get("vuoto") != "" {
		t.Error("Get su header vuoto deve dare la stringa vuota")
	}
	if !h.Has("vuoto") {
		t.Error("Has deve vedere un header con valore vuoto")
	}
	if h.Has("assente") {
		t.Error("Has su chiave assente = true")
	}
}

func TestHeaders_SetSostituisceMantenendoLaPosizione(t *testing.T) {
	h := Headers{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}
	h.Set("a", "nuovo")
	if len(h) != 2 {
		t.Fatalf("lunghezza = %d, attesa 2: Set sostituisce, non accoda", len(h))
	}
	if h[0].Key != "a" || string(h[0].Value) != "nuovo" {
		t.Errorf("Set ha spostato l'header invece di sostituirlo in place: %+v", h)
	}
	// Chiave assente: accodata.
	h.Set("c", "3")
	if len(h) != 3 || h[2].Key != "c" {
		t.Errorf("Set su chiave assente non ha accodato: %+v", h)
	}
}

func TestHeaders_SetCollassaLeChiaviRipetute(t *testing.T) {
	// Set impone UN valore: gli header DLQ non devono poter finire duplicati su un record che ne
	// aveva già uno con la stessa chiave.
	h := Headers{
		{Key: "trace", Value: []byte("uno")},
		{Key: "x", Value: []byte("-")},
		{Key: "trace", Value: []byte("due")},
	}
	h.Set("trace", "solo")
	if got := h.Values("trace"); len(got) != 1 || got[0] != "solo" {
		t.Errorf("Values dopo Set = %v, atteso un solo valore", got)
	}
	if !h.Has("x") {
		t.Error("Set ha rimosso header di altre chiavi")
	}
}

func TestHeaders_AddNonRimuove(t *testing.T) {
	var h Headers
	h.Add("trace", "uno")
	h.Add("trace", "due")
	if got := h.Values("trace"); len(got) != 2 {
		t.Errorf("Add ha sostituito invece di accodare: %v", got)
	}
}

func TestHeaders_CloneÈIndipendente(t *testing.T) {
	// toDLQ deriva gli header di un nuovo record da quelli consumati: mutare l'originale
	// significherebbe modificare un record che l'handler può avere ancora in mano.
	orig := Headers{{Key: "a", Value: []byte("1")}}
	c := orig.Clone()
	c.Set("a", "modificato")
	c.Add("b", "2")

	if orig.Get("a") != "1" {
		t.Error("la modifica del clone ha alterato l'originale")
	}
	if len(orig) != 1 {
		t.Errorf("lunghezza dell'originale = %d, attesa 1", len(orig))
	}
	if Headers(nil).Clone() != nil {
		t.Error("il clone di header nil deve essere nil")
	}
}
