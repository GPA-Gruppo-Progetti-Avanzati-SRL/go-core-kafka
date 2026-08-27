package consumer

import (
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

// backoff è l'attesa esponenziale fra due tentativi di riavvio di un consumer, con un tetto e un
// numero massimo di tentativi. Deliberatamente senza jitter: i consumer di un gruppo che ripartono
// insieme dopo un'indisponibilità dei broker provocano comunque un rebalance, e sfalsarli non lo
// evita — mentre un ritardo prevedibile è più facile da leggere in un incidente.
type backoff struct {
	cfg      spec.RestartSpec
	attempts int
	current  time.Duration
}

func newBackoff(cfg spec.RestartSpec) *backoff {
	return &backoff{cfg: cfg, current: cfg.InitialBackoff}
}

// next ritorna l'attesa prima del prossimo tentativo. ok=false quando il budget dei tentativi è
// esaurito: a quel punto l'errore risale e il processo termina, lasciando il recovery
// all'orchestratore. Il budget è illimitato solo su scelta esplicita (max-attempts negativo).
func (b *backoff) next() (time.Duration, bool) {
	if !b.cfg.Unlimited() && b.attempts >= b.cfg.Attempts() {
		return 0, false
	}
	b.attempts++
	d := b.current
	b.current = min(time.Duration(float64(b.current)*b.cfg.BackoffMultiplier()), b.cfg.MaxBackoff)
	return d, true
}

// reset riporta l'attesa e il contatore ai valori iniziali. Chiamato quando un run è durato almeno
// ResetAfter: un consumer sano per ore non deve ereditare i tentativi consumati da un guasto vecchio.
func (b *backoff) reset() {
	b.attempts = 0
	b.current = b.cfg.InitialBackoff
}

// attempts esposto per i log.
func (b *backoff) count() int { return b.attempts }
