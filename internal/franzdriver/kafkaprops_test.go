package franzdriver

import (
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// La tabella traduce le chiavi dotted di librdkafka che hanno un equivalente franz-go: l'escape hatch
// resta utilizzabile passando da un driver all'altro, senza riscrivere la config.
func TestKafkaProperties_Tradotte(t *testing.T) {
	// Sullo spec del processor perché è lì che arrivano: le mappe di `server.consumer` e del
	// processor le ha già fuse Resolve, prima che il driver veda qualcosa.
	b, err := consumerOpts(processor(spec.ConsumerTuning{KafkaProperties: map[string]string{
		"session.timeout.ms": "45000",
		"fetch.min.bytes":    "2048",
	}}), server())
	if err != nil {
		t.Fatalf("consumerOpts: %v", err)
	}
	if b.applied["session.timeout.ms"] != "45000" || b.applied["fetch.min.bytes"] != "2048" {
		t.Errorf("traccia = %v, attese le due proprietà tradotte", b.applied)
	}
}

// Una chiave senza equivalente FERMA L'AVVIO: chi la scrive si aspetta che abbia effetto, e una
// property ignorata è indistinguibile a runtime da una mai scritta. Il messaggio deve dire dov'è
// scritta e che alternative ci sono.
func TestKafkaProperties_ChiaveNonTraducibile(t *testing.T) {
	_, err := consumerOpts(processor(spec.ConsumerTuning{KafkaProperties: map[string]string{
		"queued.min.messages":    "1000",
		"fetch.error.backoff.ms": "500",
	}}), server())
	if err == nil {
		t.Fatal("una kafka-properties non traducibile deve far fallire l'avvio")
	}
	msg := err.Error()
	for _, want := range []string{"queued.min.messages", "fetch.error.backoff.ms", "processor ingest", "confluentdriver.Driver"} {
		if !strings.Contains(msg, want) {
			t.Errorf("messaggio = %q, atteso contenesse %q", msg, want)
		}
	}
}

// L'escape hatch è l'ULTIMA parola: vince sul campo tipizzato. Che sia scritto in due posti è quasi
// sempre un residuo, quindi la sovrascrittura si vede nel log — ma non ferma nulla.
func TestKafkaProperties_VinceSulCampoTipizzato(t *testing.T) {
	b, err := consumerOpts(processor(spec.ConsumerTuning{
		SessionTimeoutMs: 30000,
		KafkaProperties:  map[string]string{"session.timeout.ms": "45000"},
	}), server())
	if err != nil {
		t.Fatalf("consumerOpts: %v", err)
	}
	if b.applied["session.timeout.ms"] != "45000" {
		t.Errorf("session.timeout.ms = %q, atteso 45000: l'escape hatch è applicato per ultimo",
			b.applied["session.timeout.ms"])
	}
}

// Le chiavi sono normalizzate come in spec (lowercase + trim), la stessa normalizzazione usata dal
// controllo sulle chiavi riservate: se divergessero, una chiave scritta " Acks " passerebbe il
// controllo e verrebbe poi applicata comunque.
func TestKafkaProperties_ChiaviNormalizzate(t *testing.T) {
	b, err := producerOpts("", "server.producer", spec.ProducerTuning{
		KafkaProperties: map[string]string{"  Acks  ": "all"},
	}, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	if b.applied["acks"] != "all" {
		t.Errorf("traccia = %v, attesa la chiave normalizzata in acks", b.applied)
	}
}

// Un valore non convertibile è un errore di avvio come lo sarebbe sul campo tipizzato: l'escape hatch
// salta il tipo, non la validazione.
func TestKafkaProperties_ValoreNonValido(t *testing.T) {
	_, err := producerOpts("", "server.producer", spec.ProducerTuning{
		KafkaProperties: map[string]string{"acks": "due"},
	}, server())
	if err == nil || !strings.Contains(err.Error(), "acks") {
		t.Fatalf("errore = %v, atteso il rifiuto del valore", err)
	}
}

// enable.idempotence: true chiede il comportamento che franz-go ha già. Non produce opzioni, ma resta
// nella traccia: è stata scritta ed è stata onorata, non ignorata.
func TestKafkaProperties_IdempotenzaGiaDefault(t *testing.T) {
	b, err := producerOpts("", "server.producer", spec.ProducerTuning{
		KafkaProperties: map[string]string{"enable.idempotence": "true"},
	}, server())
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}
	if b.applied["enable.idempotence"] != "true" {
		t.Errorf("traccia = %v, attesa la proprietà registrata", b.applied)
	}
}
