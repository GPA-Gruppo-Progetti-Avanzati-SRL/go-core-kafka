package confluentdriver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// report costruisce un delivery report, con o senza errore di consegna.
func report(err error) kafka.Event {
	topic := "out"
	m := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic}}
	if err != nil {
		m.TopicPartition.Error = err
	}
	return m
}

func TestAwaitReports_TuttiConsegnati(t *testing.T) {
	ch := make(chan kafka.Event, 3)
	for range 3 {
		ch <- report(nil)
	}
	if err := awaitReports(context.Background(), ch, 3, time.Second); err != nil {
		t.Fatalf("attesa fallita con tutti i report ok: %v", err)
	}
}

func TestAwaitReports_PrimoErroreConservatoEResiduiDrenati(t *testing.T) {
	// Un errore su un record non dice nulla degli altri: i report vanno letti tutti prima di
	// dichiarare fallito il batch. Il path transazionale non lo faceva, quello non transazionale sì:
	// l'asimmetria è ciò che questa funzione condivisa elimina.
	ch := make(chan kafka.Event, 3)
	ch <- report(nil)
	ch <- report(errors.New("primo guasto"))
	ch <- report(errors.New("secondo guasto"))

	err := awaitReports(context.Background(), ch, 3, time.Second)
	if err == nil {
		t.Fatal("nessun errore ritornato con un report in errore")
	}
	if !contains(err.Error(), "primo guasto") {
		t.Errorf("errore ritornato = %v, atteso il PRIMO guasto", err)
	}
	if len(ch) != 0 {
		t.Errorf("report residui non drenati: %d", len(ch))
	}
}

func TestAwaitReports_ContextAnnullatoNonBlocca(t *testing.T) {
	// È il caso che appendeva l'applicazione: un report che non arriva. Senza il select su ctx la
	// goroutine del consumer restava bloccata per sempre, e con lei l'OnStop che la attende.
	ch := make(chan kafka.Event, 2)
	ch <- report(nil) // uno arriva, l'altro no
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- awaitReports(ctx, ch, 2, time.Minute) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("nessun errore con context annullato")
		}
		if sev := driver.SeverityOf(err); sev != driver.SeverityRetriable {
			t.Errorf("severità = %v, attesa retriable (i record non sono né confermati né perduti: si replaya)", sev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("awaitReports non è tornata: il context non viene osservato")
	}
}

func TestAwaitReports_TimeoutScaduto(t *testing.T) {
	// Seconda linea di difesa: anche con un context senza deadline l'attesa ha un bound.
	ch := make(chan kafka.Event, 1)
	err := awaitReports(context.Background(), ch, 1, 20*time.Millisecond)
	if err == nil {
		t.Fatal("nessun errore allo scadere dell'attesa")
	}
	if sev := driver.SeverityOf(err); sev != driver.SeverityRetriable {
		t.Errorf("severità = %v, attesa retriable", sev)
	}
	if !contains(err.Error(), "0/1") {
		t.Errorf("l'errore deve dire quanti report mancano, ha %q", err)
	}
}

func TestReportWait_DerivaDalDeliveryTimeout(t *testing.T) {
	// L'attesa lato Go deve essere PIÙ LUNGA del delivery timeout del client: è il client che deve
	// fallire il record per primo, con la sua causa e i suoi retry.
	p := spec.ProducerTuning{DeliveryTimeout: 30 * time.Second}
	if got := reportWait(p); got <= p.DeliveryTimeout {
		t.Errorf("reportWait = %v, atteso > delivery-timeout (%v)", got, p.DeliveryTimeout)
	}
	// Senza configurazione si ricade sul default della libreria, non su zero (che sarebbe "attendi
	// per sempre" o "non attendere affatto", entrambi sbagliati).
	if got := reportWait(spec.ProducerTuning{}); got <= spec.DefaultDeliveryTimeout {
		t.Errorf("reportWait senza config = %v, atteso > %v", got, spec.DefaultDeliveryTimeout)
	}
}
