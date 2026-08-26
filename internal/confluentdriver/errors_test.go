package confluentdriver

import (
	"errors"
	"fmt"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// La severità è ciò che decide se il consumer viene ricostruito, se il batch viene scartato o se il
// processo muore: una classificazione sbagliata non dà un log diverso, dà un comportamento diverso.
func TestWrap_Severita(t *testing.T) {
	tests := []struct {
		name string
		code kafka.ErrorCode
		want driver.Severity
	}{
		// Config/credenziali: nessun retry aiuta, va corretta la configurazione.
		{"autenticazione", kafka.ErrAuthentication, driver.SeverityPermanent},
		{"SASL fallita", kafka.ErrSaslAuthenticationFailed, driver.SeverityPermanent},
		{"meccanismo SASL non supportato", kafka.ErrUnsupportedSaslMechanism, driver.SeverityPermanent},
		{"topic non autorizzato", kafka.ErrTopicAuthorizationFailed, driver.SeverityPermanent},
		{"gruppo non autorizzato", kafka.ErrGroupAuthorizationFailed, driver.SeverityPermanent},
		{"config invalida", kafka.ErrInvalidConfig, driver.SeverityPermanent},

		// Eventi del protocollo di gruppo: il client è vivo, va scartato il batch in volo.
		{"rebalance in corso", kafka.ErrRebalanceInProgress, driver.SeverityReset},
		{"generation superata", kafka.ErrIllegalGeneration, driver.SeverityReset},
		{"member id ignoto", kafka.ErrUnknownMemberID, driver.SeverityReset},
		{"max poll superato", kafka.ErrMaxPollExceeded, driver.SeverityReset},

		// Client compromesso: ricostruirlo risolve.
		{"producer fenced", kafka.ErrFenced, driver.SeverityFatal},
		{"epoch invalido", kafka.ErrInvalidProducerEpoch, driver.SeverityFatal},
		{"stato TX invalido", kafka.ErrInvalidTxnState, driver.SeverityFatal},

		// Indisponibilità transitorie: backoff e riprova.
		{"tutti i broker giù", kafka.ErrAllBrokersDown, driver.SeverityRetriable},
		{"transport", kafka.ErrTransport, driver.SeverityRetriable},
		{"broker non disponibile", kafka.ErrBrokerNotAvailable, driver.SeverityRetriable},
		{"leader non disponibile", kafka.ErrLeaderNotAvailable, driver.SeverityRetriable},
		{"richiesta scaduta", kafka.ErrRequestTimedOut, driver.SeverityRetriable},
		{"coordinator non disponibile", kafka.ErrCoordinatorNotAvailable, driver.SeverityRetriable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := wrap("poll", kafka.NewError(tc.code, tc.name, false))
			if got := driver.SeverityOf(err); got != tc.want {
				t.Errorf("SeverityOf(%v) = %v, atteso %v", tc.code, got, tc.want)
			}
		})
	}
}

func TestWrap_Nil(t *testing.T) {
	if err := wrap("poll", nil); err != nil {
		t.Errorf("wrap(nil) = %v, atteso nil", err)
	}
}

func TestWrap_ErroreNonKafka(t *testing.T) {
	// Conservativo: ricostruire il client è innocuo, proseguire con uno in stato ignoto no.
	err := wrap("commit", errors.New("boom"))
	if got := driver.SeverityOf(err); got != driver.SeverityFatal {
		t.Errorf("severità = %v, atteso fatal", got)
	}
}

func TestWrap_NonRiclassifica(t *testing.T) {
	// Un errore che ha già una severità (es. il Reset generato dal rebalance) non deve essere
	// riclassificato attraversando un altro wrap: perderebbe il "scarta il batch" diventando fatal.
	orig := driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	if got := driver.SeverityOf(wrap("commit", orig)); got != driver.SeverityReset {
		t.Errorf("severità = %v, atteso reset", got)
	}
}

func TestWrap_ErroreWrappato(t *testing.T) {
	// La classificazione deve funzionare anche su un kafka.Error annidato: le chiamate del client
	// spesso lo restituiscono già avvolto.
	inner := kafka.NewError(kafka.ErrAllBrokersDown, "giù", false)
	err := wrap("poll", fmt.Errorf("contesto: %w", inner))
	if got := driver.SeverityOf(err); got != driver.SeverityRetriable {
		t.Errorf("severità = %v, atteso retriable", got)
	}
}

func TestDriverError_MessaggioEUnwrap(t *testing.T) {
	inner := kafka.NewError(kafka.ErrAllBrokersDown, "giù", false)
	err := wrap("poll", inner)

	// L'operazione deve comparire nel messaggio: senza, capire dove è nato l'errore richiede lo stack.
	if msg := err.Error(); msg == "" || !contains(msg, "poll") || !contains(msg, "retriable") {
		t.Errorf("messaggio poco informativo: %q", msg)
	}
	// Unwrap deve preservare l'errore del client, altrimenti errors.Is/As dell'app smettono di funzionare.
	var ke kafka.Error
	if !errors.As(err, &ke) {
		t.Error("errors.As non trova il kafka.Error sottostante")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
