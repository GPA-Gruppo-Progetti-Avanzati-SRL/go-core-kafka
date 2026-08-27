package spec

import (
	"strings"
	"testing"
)

func TestValidateServer_OrdineDeterministico(t *testing.T) {
	// Con più sezioni sbagliate l'errore riportato deve essere SEMPRE lo stesso: prima si iterava una
	// mappa di blocchi, quindi quale comparisse cambiava da un avvio all'altro — e con esso il
	// messaggio che l'operatore leggeva nei log.
	k := KafkaServer{
		BootstrapServers: "broker:9092",
		KafkaProperties:  map[string]string{"bootstrap.servers": "x"},
		Consumer:         ConsumerTuning{KafkaProperties: map[string]string{"group.id": "x"}},
		Producer:         ProducerTuning{KafkaProperties: map[string]string{"transactional.id": "x"}},
	}
	first := ""
	for i := range 20 {
		err := ValidateServer(k)
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

func TestValidateServer_OgniSezioneEControllata(t *testing.T) {
	// Il Producer condiviso è registrabile anche da solo, quindi la funzione è chiamata da due
	// percorsi: nessuna sezione deve restare fuori.
	for _, tc := range []struct {
		name  string
		k     KafkaServer
		owner string
	}{
		{"server", KafkaServer{BootstrapServers: "b:9092", KafkaProperties: map[string]string{"group.id": "x"}}, "server:"},
		{"consumer", KafkaServer{BootstrapServers: "b:9092", Consumer: ConsumerTuning{KafkaProperties: map[string]string{"enable.auto.commit": "true"}}}, "server.consumer:"},
		{"producer", KafkaServer{BootstrapServers: "b:9092", Producer: ProducerTuning{KafkaProperties: map[string]string{"transactional.id": "x"}}}, "server.producer:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateServer(tc.k)
			if err == nil {
				t.Fatal("chiave riservata non rilevata")
			}
			if !strings.HasPrefix(err.Error(), tc.owner) {
				t.Errorf("errore = %q, atteso attribuito a %s", err.Error(), tc.owner)
			}
		})
	}
	if err := ValidateServer(KafkaServer{BootstrapServers: "b:9092"}); err != nil {
		t.Errorf("config senza kafka-properties = %v, atteso nil", err)
	}
}

// I tag `validate:` non si applicano da soli: prima li eseguiva solo la core.ReadConfig dell'app,
// quindi valevano per chi ci passava e per nessun altro. Ora la libreria li garantisce dai propri
// costruttori fx, ed è questa la regressione che lo blocca.
func TestValidateServer_ApplicaITagValidate(t *testing.T) {
	if err := ValidateServer(KafkaServer{}); err == nil {
		t.Error("bootstrap-servers assente: atteso errore, il tag `validate:\"required\"` non è stato eseguito")
	}
	k := KafkaServer{BootstrapServers: "b:9092", Consumer: ConsumerTuning{OnError: "deadleter"}}
	if err := ValidateServer(k); err == nil {
		t.Error("on-error con un refuso: atteso errore dall'enum `oneof`")
	}
}

// La voce di `processors` è validata GREZZA: un blocco ereditato è già stato controllato al livello
// di `server`, e attribuire al processor un valore che non ha scritto manda a correggere il file
// sbagliato.
func TestValidateProcessor(t *testing.T) {
	valid := func() ProcessorSpec {
		return ProcessorSpec{Name: "ingest", Topics: []string{"t"}, GroupID: "g"}
	}
	tests := []struct {
		name    string
		mutate  func(*ProcessorSpec)
		wantErr bool
	}{
		{"spec valido", func(*ProcessorSpec) {}, false},
		{"name mancante", func(s *ProcessorSpec) { s.Name = "" }, true},
		{"topics mancanti", func(s *ProcessorSpec) { s.Topics = nil }, true},
		{"topics vuoti", func(s *ProcessorSpec) { s.Topics = []string{} }, true},
		{"group-id mancante", func(s *ProcessorSpec) { s.GroupID = "" }, true},
		{"auto-offset-reset con refuso", func(s *ProcessorSpec) { s.Consumer.AutoOffsetReset = "earlest" }, true},
		{"acks non ammesso", func(s *ProcessorSpec) { s.Producer.Acks = "2" }, true},
		{"chiave riservata nel consumer", func(s *ProcessorSpec) {
			s.Consumer.KafkaProperties = map[string]string{"group.id": "x"}
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := valid()
			tc.mutate(&s)
			if err := ValidateProcessor(s); (err != nil) != tc.wantErr {
				t.Errorf("ValidateProcessor = %v, atteso errore = %v", err, tc.wantErr)
			}
		})
	}
}
