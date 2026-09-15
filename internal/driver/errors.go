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

// TopicPartition identifica una partizione nei termini neutri del driver: è ciò che serve all'engine
// per sapere QUALI record del batch in volo non sono più suoi.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Error è l'errore del driver con la sua severità. Op è l'operazione che ha fallito ("poll",
// "commit", "produce", "begin", ...): finisce nel messaggio e nel log, così un errore non richiede
// di risalire lo stack per capire dove è nato.
type Error struct {
	Sev Severity
	Op  string
	Err error

	// Revoked è valorizzato SOLO su un SeverityReset nato da una revoca di partizioni, e cambia il
	// significato del reset: non "scarta tutto il batch" ma "di quel batch hai perso QUESTE
	// partizioni, il resto è ancora tuo".
	//
	// La differenza non è un'ottimizzazione: con un rebalance cooperativo la revoca è PARZIALE, e i
	// record delle partizioni RITENUTE non tornano indietro se li si butta — la posizione di fetch
	// non si riavvolge e il commit successivo ci passa sopra. Scartarli è perdere messaggi
	// (misurato: 305 record su una corsa contro un broker vero).
	//
	// Contratto: quando è valorizzato, il driver ha GIÀ scartato gli offset tracciati delle
	// partizioni elencate (lo fa il rebalance callback, che è il solo posto in cui la lista esiste),
	// quindi l'engine NON deve chiamare Discard — che butterebbe anche il resto — ma solo filtrare
	// il proprio batch. Vuoto o assente = semantica di sempre: non si sa cosa si è perso, si scarta
	// tutto.
	Revoked []TopicPartition
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

// NewRevokeError costruisce il reset PARZIALE di una revoca: porta con sé le partizioni perse.
// Vedi Error.Revoked per il contratto.
func NewRevokeError(op string, err error, revoked []TopicPartition) *Error {
	return &Error{Sev: SeverityReset, Op: op, Err: err, Revoked: revoked}
}

// RevokedOf ritorna le partizioni revocate portate da un reset parziale. ok=false significa "scarta
// tutto": o l'errore non viene dal driver, o il driver non sa quali partizioni siano coinvolte
// (generation superata, poll interval scaduto, abort EOS).
func RevokedOf(err error) ([]TopicPartition, bool) {
	var de *Error
	if !errors.As(err, &de) || len(de.Revoked) == 0 {
		return nil, false
	}
	return de.Revoked, true
}

// NewError costruisce un errore del driver. Usata dalle implementazioni (internal/confluentdriver):
// è il solo modo di dare una severità a un errore, così la classificazione resta confinata nel
// package che conosce il client concreto.
func NewError(sev Severity, op string, err error) *Error {
	return &Error{Sev: sev, Op: op, Err: err}
}
