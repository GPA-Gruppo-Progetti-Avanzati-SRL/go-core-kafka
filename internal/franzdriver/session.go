package franzdriver

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// errRebalanced è la causa che accompagna il SeverityReset generato da un rebalance. Non è un guasto:
// è un evento di protocollo che invalida il batch in volo.
var errRebalanced = errors.New("rebalance: partizioni revocate, batch in volo scartato")

// poller è ciò che i due client franz hanno in comune per il consumo: il *kgo.Client (modalità
// handle) e il *kgo.GroupTransactSession (modalità transform) espongono entrambi queste due
// operazioni, ed è tutto ciò che serve alla parte condivisa.
type poller interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	AllowRebalance()
}

// rebalanceObserver traduce le callback di revoca in un flag che il Poll successivo trasforma in un
// SeverityReset. Serve per CORRETTEZZA: i record già consegnati all'engine provengono da partizioni
// che potrebbero non essere più nostre, e committarli significherebbe dichiarare elaborati record che
// il nuovo owner sta rileggendo — cioè perdere messaggi. Scartare il batch dà duplicati, mai buchi.
//
// Il flag è atomico perché la callback gira sulla goroutine di gestione del gruppo, non sulla nostra:
// con BlockRebalanceOnPoll le due sono serializzate (la callback attende che rilasciamo), ma la
// sincronizzazione la garantisce il client, non il nostro codice, e appoggiarsi a quel dettaglio
// sarebbe una data race in attesa di un cambio di versione.
type rebalanceObserver struct {
	name    string
	revoked atomic.Bool
}

func (o *rebalanceObserver) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	o.revoked.Store(true)
	log.Info().Str("consumer", o.name).Int("topics", len(revoked)).
		Interface("assignment", revoked).Msg("corekafka: partizioni revocate")
}

func (o *rebalanceObserver) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	o.revoked.Store(true)
	log.Warn().Str("consumer", o.name).Int("topics", len(lost)).
		Interface("assignment", lost).Msg("corekafka: partizioni perse (sessione scaduta)")
}

// takeRevoked consuma il flag: ritorna true una sola volta per rebalance, così l'engine scarta il
// batch una volta e riprende a consumare.
func (o *rebalanceObserver) takeRevoked() bool { return o.revoked.Swap(false) }

// session è la parte comune ai due client: il buffer dei record fetchati, il flag di revoca e la
// disciplina del blocco dei rebalance. Poll vive qui perché è identico nelle due modalità — cambia
// solo COME si conferma ciò che si è consumato, e quello sta nei due tipi che la embeddano.
type session struct {
	name    string
	p       poller
	rb      *rebalanceObserver
	maxPoll int

	buf []*kgo.Record
	// holding dice che l'engine ha in mano record non ancora committati né scartati. Governa il
	// rilascio dei rebalance: finché è true il gruppo resta bloccato (è la garanzia di
	// BlockRebalanceOnPoll — nessuna revoca fra il poll e il commit), quando è false il rilascio
	// avviene prima di ogni fetch. Senza quest'ultima parte un consumer IDLE bloccherebbe per sempre i
	// rebalance del gruppo: non avendo mai un batch da committare, non chiamerebbe mai AllowRebalance.
	holding bool
}

// pollRaw ritorna il prossimo record del buffer, riempiendolo con una fetch quando è esaurito, oppure
// (nil, nil) allo scadere del timeout senza messaggi.
//
// A differenza del driver confluent il context è osservato DAVVERO: PollRecords lo accetta, quindi un
// arresto interrompe la fetch invece di attenderne il timeout.
func (s *session) pollRaw(ctx context.Context, timeout time.Duration) (*kgo.Record, error) {
	// Prima di tutto: un rebalance avvenuto nel frattempo invalida sia il buffer sia il batch che
	// l'engine sta accumulando. I record bufferizzati vengono buttati senza essere consegnati — li
	// rileggerà chi possiede ora quelle partizioni.
	if s.rb.takeRevoked() {
		s.buf = nil
		return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	}
	if len(s.buf) > 0 {
		r := s.buf[0]
		s.buf = s.buf[1:]
		s.holding = true
		return r, nil
	}

	if !s.holding {
		// Nessun batch in volo: è il punto sicuro per lasciar avvenire un rebalance eventualmente in
		// attesa. Se ne è in corso uno, la fetch qui sotto attende che finisca.
		s.p.AllowRebalance()
	}

	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fetches := s.p.PollRecords(fctx, s.maxPoll)

	if fetches.IsClientClosed() {
		return nil, driver.NewError(driver.SeverityFatal, "poll", kgo.ErrClientClosed)
	}
	for _, fe := range fetches.Errors() {
		// La scadenza del timeout della singola poll non è un errore: è il modo in cui il loop
		// dell'engine torna a osservare il ticker del taglio e la cancellazione.
		if errors.Is(fe.Err, context.DeadlineExceeded) || errors.Is(fe.Err, context.Canceled) {
			continue
		}
		return nil, wrap("poll", fe.Err)
	}
	if fetches.Empty() {
		// Il rebalance può essere avvenuto durante questa fetch a vuoto: va segnalato comunque, perché
		// il batch accumulato prima resta da scartare.
		if s.rb.takeRevoked() {
			return nil, driver.NewError(driver.SeverityReset, "poll", errRebalanced)
		}
		return nil, nil
	}

	s.buf = fetches.Records()
	r := s.buf[0]
	s.buf = s.buf[1:]
	s.holding = true
	return r, nil
}

// release dichiara chiuso il batch in volo (committato o scartato) e sblocca i rebalance.
//
// NON butta il buffer: i record fetchati e non ancora consegnati all'engine sono quelli SUCCESSIVI
// agli offset appena committati, quindi restano validi e vanno consegnati al batch seguente. Buttarli
// costerebbe una fetch in più a ogni taglio a tempo. Se nel frattempo arriva una revoca, il flag la
// segnala e pollRaw il buffer lo butta lì — dove è giusto farlo.
func (s *session) release() {
	s.holding = false
	s.p.AllowRebalance()
}

// dropAndRelease è release più lo scarto dei record fetchati e non consegnati: si usa quando il batch
// in volo viene buttato, e quei record — non committati — verranno rifetchati dall'ultimo commit.
func (s *session) dropAndRelease() {
	s.buf = nil
	s.release()
}
