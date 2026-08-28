package franzdriver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// La severità NON è una scala di gravità ma un verbo: dice all'engine se scartare il batch,
// ricostruire il client o far uscire il processo. Questa tabella è il contratto fra il driver franz e
// la supervisione, e vale la pena bloccarla: sbagliare una riga significa un CrashLoopBackOff su un
// rolling restart dei broker, o un guasto stabile mascherato all'infinito.
func TestWrap_Severita(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want driver.Severity
	}{
		{"nessun errore", nil, driver.SeverityBusiness}, // wrap ritorna nil: SeverityOf(nil) è business
		{"auth SASL fallita", kerr.SaslAuthenticationFailed, driver.SeverityPermanent},
		{"ACL sul topic", kerr.TopicAuthorizationFailed, driver.SeverityPermanent},
		{"topic inesistente", kerr.UnknownTopicOrPartition, driver.SeverityPermanent},
		{"rebalance in corso", kerr.RebalanceInProgress, driver.SeverityReset},
		{"generazione superata", kerr.IllegalGeneration, driver.SeverityReset},
		{"member id sconosciuto", kerr.UnknownMemberID, driver.SeverityReset},
		{"producer fenced", kerr.ProducerFenced, driver.SeverityFatal},
		{"epoch invalido", kerr.InvalidProducerEpoch, driver.SeverityFatal},
		{"stato TX invalido", kerr.InvalidTxnState, driver.SeverityFatal},
		{"transazione abortibile", kerr.TransactionAbortable, driver.SeverityAbort},
		{"leader non disponibile", kerr.LeaderNotAvailable, driver.SeverityRetriable},
		{"coordinator in load", kerr.CoordinatorLoadInProgress, driver.SeverityRetriable},
		{"client chiuso", kgo.ErrClientClosed, driver.SeverityFatal},
		{"record scaduti", kgo.ErrRecordTimeout, driver.SeverityRetriable},
		{"retry esauriti", kgo.ErrRecordRetries, driver.SeverityRetriable},
		{"abort dei buffered", kgo.ErrAborting, driver.SeverityAbort},
		{"context scaduto", context.DeadlineExceeded, driver.SeverityRetriable},
		{"errore di rete", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, driver.SeverityFatal},
		{"errore sconosciuto", errors.New("boh"), driver.SeverityFatal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := driver.SeverityOf(wrap("poll", tc.err))
			if got != tc.want {
				t.Errorf("severità = %s, attesa %s", got, tc.want)
			}
		})
	}
}

// wrap deve reggere l'errore AVVOLTO: gli errori risalgono da franz dentro contesti ("fetch: %w"),
// e una classificazione che funzionasse solo sull'errore nudo fallirebbe proprio in esercizio.
func TestWrap_ErroreAvvolto(t *testing.T) {
	err := fmt.Errorf("fetch da broker 3: %w", kerr.RebalanceInProgress)
	if got := driver.SeverityOf(wrap("poll", err)); got != driver.SeverityReset {
		t.Errorf("severità = %s, attesa reset anche sull'errore avvolto", got)
	}
}

// Un errore già classificato (risalito da un nostro helper) non va riclassificato: perderebbe la
// severità decisa dove si sapeva cosa stava succedendo.
func TestWrap_NonRiclassifica(t *testing.T) {
	orig := driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	if got := wrap("commit", orig); got != error(orig) {
		t.Errorf("wrap ha riclassificato un *driver.Error: %v", got)
	}
}

// nil resta nil, così i call site possono scriverci sopra un return diretto.
func TestWrap_Nil(t *testing.T) {
	if err := wrap("commit", nil); err != nil {
		t.Errorf("wrap(nil) = %v, atteso nil", err)
	}
}

// Un errore ricostruito dal codice di risposta è lo stesso errore ma non lo stesso puntatore: il
// confronto per codice è ciò che tiene in piedi la tabella con gli errori che arrivano dal broker.
func TestWrap_ConfrontoPerCodice(t *testing.T) {
	fromWire := kerr.ErrorForCode(kerr.ProducerFenced.Code)
	if got := driver.SeverityOf(wrap("commit", fromWire)); got != driver.SeverityFatal {
		t.Errorf("severità = %s, attesa fatal per un ProducerFenced ricostruito dal codice", got)
	}
}

// End di franz-go non ritorna mai un errore ritentabile: l'unica risposta è ricostruire la sessione.
// L'eccezione è l'errore di configurazione/autorizzazione, dove ricostruire non cambia nulla.
func TestEndErr(t *testing.T) {
	if got := driver.SeverityOf(endErr("commit-transaction", kerr.CoordinatorNotAvailable)); got != driver.SeverityFatal {
		t.Errorf("severità = %s, attesa fatal: dopo un End fallito la sessione va ricostruita", got)
	}
	if got := driver.SeverityOf(endErr("commit-transaction", kerr.TransactionalIDAuthorizationFailed)); got != driver.SeverityPermanent {
		t.Errorf("severità = %s, attesa permanent: un problema di ACL non si risolve ricostruendo", got)
	}
	if err := endErr("commit-transaction", nil); err != nil {
		t.Errorf("endErr(nil) = %v, atteso nil", err)
	}
}
