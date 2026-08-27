package spec

import (
	"strings"
	"testing"
)

func TestValidateServerProperties_OrdineDeterministico(t *testing.T) {
	// Con più sezioni sbagliate l'errore riportato deve essere SEMPRE lo stesso: prima si iterava una
	// mappa di blocchi, quindi quale comparisse cambiava da un avvio all'altro — e con esso il
	// messaggio che l'operatore leggeva nei log.
	k := KafkaServer{
		KafkaProperties: map[string]string{"bootstrap.servers": "x"},
		Consumer:        ConsumerTuning{KafkaProperties: map[string]string{"group.id": "x"}},
		Producer:        ProducerTuning{KafkaProperties: map[string]string{"transactional.id": "x"}},
	}
	first := ""
	for i := range 20 {
		err := ValidateServerProperties(k)
		if err == nil {
			t.Fatal("nessun errore con tre sezioni contenenti chiavi riservate")
		}
		if i == 0 {
			first = err.Error()
			continue
		}
		if err.Error() != first {
			t.Fatalf("errore variabile fra due esecuzioni:\n%q\n%q", first, err.Error())
		}
	}
	// L'ordine è quello di scrittura della config: la sezione più esterna per prima.
	if !strings.HasPrefix(first, "server:") {
		t.Errorf("primo errore = %q, atteso quello di `server`", first)
	}
}

func TestValidateServerProperties_OgniSezioneEControllata(t *testing.T) {
	// Il Producer condiviso è registrabile anche da solo, quindi la funzione è chiamata da due
	// percorsi: nessuna sezione deve restare fuori.
	for _, tc := range []struct {
		name  string
		k     KafkaServer
		owner string
	}{
		{"server", KafkaServer{KafkaProperties: map[string]string{"group.id": "x"}}, "server:"},
		{"consumer", KafkaServer{Consumer: ConsumerTuning{KafkaProperties: map[string]string{"enable.auto.commit": "true"}}}, "server.consumer:"},
		{"producer", KafkaServer{Producer: ProducerTuning{KafkaProperties: map[string]string{"transactional.id": "x"}}}, "server.producer:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateServerProperties(tc.k)
			if err == nil {
				t.Fatal("chiave riservata non rilevata")
			}
			if !strings.HasPrefix(err.Error(), tc.owner) {
				t.Errorf("errore = %q, atteso attribuito a %s", err.Error(), tc.owner)
			}
		})
	}
	if err := ValidateServerProperties(KafkaServer{}); err != nil {
		t.Errorf("config senza kafka-properties = %v, atteso nil", err)
	}
}
