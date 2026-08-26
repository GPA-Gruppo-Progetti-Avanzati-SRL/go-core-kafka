package driver

import (
	"errors"
	"fmt"
)

// Severity classifica un errore risalito dal driver in base a COSA DEVE FARE l'engine. Non è una
// scala di gravità: è un verbo. Il client Kafka distingue errori da cui si esce ricostruendo il
// client, errori da cui non si esce affatto, e condizioni che sono solo un evento del protocollo
// (un rebalance) — trattarli tutti come "il consumer è morto" è ciò che rende un rolling restart dei
// broker un CrashLoopBackOff.
type Severity int

const (
	// SeverityBusiness: NON è un errore del client. È un errore risalito da Handle/Transform sotto
	// policy fail-fast. Default: l'engine esce (semantica documentata di on-error=fail-fast), a meno
	// di restart.on-business-error. È lo zero value: un errore non prodotto dal driver ricade qui.
	SeverityBusiness Severity = iota
	// SeverityPermanent: nessun retry può aiutare (credenziali errate, meccanismo SASL non
	// supportato, config rifiutata dal client). L'engine esce e il processo termina: va corretta la
	// configurazione, non riprovato.
	SeverityPermanent
	// SeverityFatal: il client non è più utilizzabile (fencing EOS, epoch invalido, errore marcato
	// fatal da librdkafka) ma un client NUOVO può funzionare. L'engine ricrea consumer/sessione.
	SeverityFatal
	// SeverityRetriable: indisponibilità transitoria dell'infrastruttura (transport, tutti i broker
	// giù, leader non disponibile). L'engine ricrea il client dopo il backoff.
	SeverityRetriable
	// SeverityAbort: la transazione EOS in corso va abortita, ma la sessione resta valida. L'engine
	// abortisce, scarta il batch in volo e continua senza ricostruire nulla.
	SeverityAbort
	// SeverityReset: evento di protocollo, non un guasto (rebalance in corso, partizioni revocate,
	// generation superata). Il batch in volo va scartato senza commit — le partizioni potrebbero non
	// essere più nostre — ma il client è vivo e il loop continua.
	SeverityReset
)

// String rende la Severity leggibile nei log e usabile come label Prometheus.
func (s Severity) String() string {
	switch s {
	case SeverityBusiness:
		return "business"
	case SeverityPermanent:
		return "permanent"
	case SeverityFatal:
		return "fatal"
	case SeverityRetriable:
		return "retriable"
	case SeverityAbort:
		return "abort"
	case SeverityReset:
		return "reset"
	default:
		return "unknown"
	}
}

// Error è l'errore del driver con la sua severità. Op è l'operazione che ha fallito ("poll",
// "commit", "produce", "begin", ...): finisce nel messaggio e nel log, così un errore non richiede
// di risalire lo stack per capire dove è nato.
type Error struct {
	Sev Severity
	Op  string
	Err error
}

func (e *Error) Error() string {
	return fmt.Sprintf("kafka %s (%s): %v", e.Op, e.Sev, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// SeverityOf estrae la severità di un errore. Un errore che non viene dal driver — tipicamente
// quello risalito dalla business logic dell'app — è SeverityBusiness: è la ragione per cui quel
// valore è lo zero della enum.
func SeverityOf(err error) Severity {
	var de *Error
	if errors.As(err, &de) {
		return de.Sev
	}
	return SeverityBusiness
}

// NewError costruisce un errore del driver. Usata dalle implementazioni (internal/confluentdriver):
// è il solo modo di dare una severità a un errore, così la classificazione resta confinata nel
// package che conosce il client concreto.
func NewError(sev Severity, op string, err error) *Error {
	return &Error{Sev: sev, Op: op, Err: err}
}
