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

// next ritorna l'attesa prima del prossimo tentativo. ok=false quando MaxAttempts è esaurito: a quel
// punto l'errore risale e il processo termina.
func (b *backoff) next() (time.Duration, bool) {
	if b.cfg.MaxAttempts > 0 && b.attempts >= b.cfg.MaxAttempts {
		return 0, false
	}
	b.attempts++
	d := b.current
	b.current = min(time.Duration(float64(b.current)*b.cfg.Multiplier), b.cfg.MaxBackoff)
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
