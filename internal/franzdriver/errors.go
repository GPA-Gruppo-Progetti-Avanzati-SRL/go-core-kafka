package franzdriver

import (
	"context"
	"errors"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Questo file è l'UNICO punto del driver che interpreta un errore di franz-go: tutto ciò che esce di
// qui è un *driver.Error con una driver.Severity, e da lì in poi l'engine decide cosa fare senza
// sapere quale client sta usando. È il gemello di internal/confluentdriver/errors.go, con la stessa
// struttura a tabella e le stesse scelte di classificazione — le differenze sono solo nei nomi degli
// errori, non nella semantica.

// permanentErrs: errori di configurazione o autorizzazione. Riprovare non cambia l'esito, va corretta
// la config → il processo esce.
var permanentErrs = []*kerr.Error{
	kerr.SaslAuthenticationFailed,
	kerr.UnsupportedSaslMechanism,
	kerr.IllegalSaslState,
	kerr.TopicAuthorizationFailed,
	kerr.GroupAuthorizationFailed,
	kerr.ClusterAuthorizationFailed,
	kerr.TransactionalIDAuthorizationFailed,
	kerr.SecurityDisabled,
	kerr.UnsupportedVersion,
	kerr.InvalidConfig,
	kerr.InvalidRequest,
	kerr.InvalidGroupID,
	kerr.InvalidSessionTimeout,
	kerr.UnsupportedAssignor,
	// UnknownTopicOrPartition è Retriable per Kafka, ma per un consumer sottoscritto a un topic che
	// non esiste il retry non converge mai: è la stessa scelta del driver confluent
	// (kafka.ErrUnknownTopicOrPart fra i permanentCodes).
	kerr.UnknownTopicOrPartition,
}

// resetErrs: eventi del protocollo di consumer group. Il client resta valido; va scartato il batch in
// volo perché le partizioni da cui proviene potrebbero non essere più assegnate a noi.
var resetErrs = []*kerr.Error{
	kerr.RebalanceInProgress,
	kerr.IllegalGeneration,
	kerr.UnknownMemberID,
	kerr.MemberIDRequired,
	kerr.FencedMemberEpoch,
	kerr.StaleMemberEpoch,
	kerr.UnknownSubscriptionID,
}

// fatalErrs: il client corrente è compromesso (tipicamente il fencing del producer transazionale dopo
// un rebalance, o una transazione scaduta) ma un client NUOVO riparte pulito.
var fatalErrs = []*kerr.Error{
	kerr.ProducerFenced,
	kerr.InvalidProducerEpoch,
	kerr.FencedInstanceID,
	kerr.InvalidTxnState,
	kerr.InvalidProducerIDMapping,
	kerr.TransactionCoordinatorFenced,
	kerr.UnknownProducerID,
	kerr.OutOfOrderSequenceNumber,
}

// abortErrs: il broker chiede esplicitamente di abortire la transazione in corso; la sessione resta
// utilizzabile dopo l'abort.
var abortErrs = []*kerr.Error{
	kerr.TransactionAbortable,
	kerr.ConcurrentTransactions,
}

// wrap traduce un errore di franz-go in un driver.Error con la sua severità. Ritorna nil per err nil,
// così i call site possono scriverci sopra un return diretto.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var de *driver.Error
	if errors.As(err, &de) {
		return err // già classificato (es. un errore nostro risalito da un helper)
	}

	switch {
	case errors.Is(err, kgo.ErrClientClosed):
		// Il client è chiuso: solo un client nuovo può ripartire.
		return driver.NewError(driver.SeverityFatal, op, err)
	case errors.Is(err, kgo.ErrAborting):
		// Produce durante l'abort dei record bufferizzati: la transazione va chiusa, non il client.
		return driver.NewError(driver.SeverityAbort, op, err)
	case errors.Is(err, kgo.ErrRecordTimeout), errors.Is(err, kgo.ErrRecordRetries),
		errors.Is(err, kgo.ErrMaxBuffered), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		// I record non sono né confermati né perduti: niente commit, si replaya (vedi il contratto di
		// SeverityRetriable). Un context cancellato è l'arresto o il bound dell'attesa, non un guasto
		// del client.
		return driver.NewError(driver.SeverityRetriable, op, err)
	}

	var ke *kerr.Error
	if !errors.As(err, &ke) {
		// Non viene dal protocollo Kafka: è un errore di rete, nostro o della stdlib. Conservativo
		// come nel driver confluent: ricostruire il client è innocuo, proseguire con uno in stato
		// ignoto no.
		return driver.NewError(driver.SeverityFatal, op, err)
	}

	switch {
	case hasErr(permanentErrs, ke):
		return driver.NewError(driver.SeverityPermanent, op, ke)
	case hasErr(resetErrs, ke):
		return driver.NewError(driver.SeverityReset, op, ke)
	case hasErr(fatalErrs, ke):
		return driver.NewError(driver.SeverityFatal, op, ke)
	case hasErr(abortErrs, ke):
		return driver.NewError(driver.SeverityAbort, op, ke)
	case ke.Retriable:
		return driver.NewError(driver.SeverityRetriable, op, ke)
	default:
		return driver.NewError(driver.SeverityFatal, op, ke)
	}
}

// hasErr confronta per CODICE e non per identità del puntatore: un errore ricostruito dal codice di
// risposta (kerr.ErrorForCode) è lo stesso errore, ma non lo stesso oggetto.
func hasErr(list []*kerr.Error, ke *kerr.Error) bool {
	for _, e := range list {
		if e.Code == ke.Code {
			return true
		}
	}
	return false
}
