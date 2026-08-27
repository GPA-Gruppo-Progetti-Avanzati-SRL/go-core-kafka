package confluentdriver

import (
	"errors"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Questo file è l'UNICO punto di go-core-kafka che interpreta un kafka.Error. Tutti gli errori che
// escono dal driver passano da wrap, che li traduce in driver.Error con una driver.Severity: da lì in
// poi l'engine decide cosa fare senza sapere nulla del client concreto.
//
// La classificazione a tre livelli — tabella di codici espliciti, poi i predicati di librdkafka
// (TxnRequiresAbort/IsFatal/IsRetriable), poi un default conservativo — è modellata su
// kafkautil.LogKafkaError di tpm-kafka-common, con una differenza sostanziale: là la classificazione
// scegleva solo il livello di log, qui guida il control flow.

// permanentCodes: errori di configurazione o di autorizzazione. Riprovare non cambia l'esito, va
// corretta la config → il processo esce.
var permanentCodes = map[kafka.ErrorCode]struct{}{
	kafka.ErrAuthentication:                     {},
	kafka.ErrSaslAuthenticationFailed:           {},
	kafka.ErrUnsupportedSaslMechanism:           {},
	kafka.ErrTopicAuthorizationFailed:           {},
	kafka.ErrGroupAuthorizationFailed:           {},
	kafka.ErrClusterAuthorizationFailed:         {},
	kafka.ErrTransactionalIDAuthorizationFailed: {},
	kafka.ErrInvalidConfig:                      {},
	kafka.ErrInvalidArg:                         {},
	kafka.ErrSecurityDisabled:                   {},
	kafka.ErrUnsupportedVersion:                 {},
	kafka.ErrUnknownTopicOrPart:                 {},
}

// resetCodes: eventi del protocollo di consumer group. Il client resta valido; va scartato il batch
// in volo perché le partizioni da cui proviene potrebbero non essere più assegnate a noi.
var resetCodes = map[kafka.ErrorCode]struct{}{
	kafka.ErrRebalanceInProgress: {},
	kafka.ErrIllegalGeneration:   {},
	kafka.ErrUnknownMemberID:     {},
	kafka.ErrMemberIDRequired:    {},
	kafka.ErrMaxPollExceeded:     {},
}

// retriableCodes: indisponibilità transitorie. librdkafka si riconnette da sé, ma se l'errore è
// risalito fino a noi la chiamata è fallita: ricostruiamo il client dopo il backoff.
var retriableCodes = map[kafka.ErrorCode]struct{}{
	kafka.ErrAllBrokersDown:            {},
	kafka.ErrTransport:                 {},
	kafka.ErrBrokerNotAvailable:        {},
	kafka.ErrNetworkException:          {},
	kafka.ErrLeaderNotAvailable:        {},
	kafka.ErrNotLeaderForPartition:     {},
	kafka.ErrRequestTimedOut:           {},
	kafka.ErrTimedOut:                  {},
	kafka.ErrTimedOutQueue:             {},
	kafka.ErrCoordinatorNotAvailable:   {},
	kafka.ErrNotCoordinator:            {},
	kafka.ErrCoordinatorLoadInProgress: {},
	kafka.ErrQueueFull:                 {},
}

// fatalCodes: il client corrente è compromesso (tipicamente fencing del producer transazionale dopo
// un rebalance o una transazione scaduta) ma un client nuovo riparte pulito.
var fatalCodes = map[kafka.ErrorCode]struct{}{
	kafka.ErrFenced:                   {},
	kafka.ErrInvalidProducerEpoch:     {},
	kafka.ErrFencedInstanceID:         {},
	kafka.ErrProducerFenced:           {},
	kafka.ErrInvalidTxnState:          {},
	kafka.ErrInvalidProducerIDMapping: {},
	kafka.ErrFatal:                    {},
}

// wrap traduce un errore del client in un driver.Error con la sua severità. Ritorna nil per err nil,
// così i call site possono scriverci sopra un return diretto.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var de *driver.Error
	if errors.As(err, &de) {
		return err // già classificato (es. un errore nostro risalito da un helper)
	}

	var ke kafka.Error
	if !errors.As(err, &ke) {
		// Non viene dal client Kafka: è un errore nostro o della stdlib. Conservativo: ricostruire il
		// client è innocuo, proseguire con uno in stato ignoto no.
		return driver.NewError(driver.SeverityFatal, op, err)
	}

	code := ke.Code()
	switch {
	case has(permanentCodes, code):
		return driver.NewError(driver.SeverityPermanent, op, ke)
	case has(resetCodes, code):
		return driver.NewError(driver.SeverityReset, op, ke)
	case has(fatalCodes, code):
		return driver.NewError(driver.SeverityFatal, op, ke)
	case ke.TxnRequiresAbort():
		// Il broker chiede esplicitamente l'abort: la sessione EOS resta utilizzabile dopo.
		return driver.NewError(driver.SeverityAbort, op, ke)
	case ke.IsFatal():
		return driver.NewError(driver.SeverityFatal, op, ke)
	case ke.IsRetriable(), has(retriableCodes, code):
		return driver.NewError(driver.SeverityRetriable, op, ke)
	default:
		return driver.NewError(driver.SeverityFatal, op, ke)
	}
}

func has(m map[kafka.ErrorCode]struct{}, c kafka.ErrorCode) bool {
	_, ok := m[c]
	return ok
}
