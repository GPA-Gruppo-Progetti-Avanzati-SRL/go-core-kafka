package franzdriver

import (
	"context"
	"errors"
	"sync"
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

// rebalanceObserver traduce le callback di revoca nella LISTA delle partizioni perse, che il Poll
// successivo trasforma in un SeverityReset. Serve per CORRETTEZZA: i record già consegnati all'engine
// provengono da partizioni che potrebbero non essere più nostre, e committarli significherebbe
// dichiarare elaborati record che il nuovo owner sta rileggendo — cioè perdere messaggi.
//
// È la lista e non un booleano perché con il protocollo cooperativo la revoca è PARZIALE: sapere
// QUALI partizioni sono andate è ciò che permette di non buttare i record delle altre, che restano
// nostre e che nessuno rileggerebbe (la posizione di fetch non si riavvolge).
//
// L'accesso è sotto mutex perché la callback gira sulla goroutine di gestione del gruppo, non sulla
// nostra: con BlockRebalanceOnPoll le due sono serializzate (la callback attende che rilasciamo), ma
// la sincronizzazione la garantisce il client, non il nostro codice, e appoggiarsi a quel dettaglio
// sarebbe una data race in attesa di un cambio di versione.
type rebalanceObserver struct {
	name string

	mu      sync.Mutex
	revoked []driver.TopicPartition
	// gap è, per topic-partizione, il primo offset che è stato SCARTATO senza essere elaborato — dal
	// batch dell'engine o dal buffer del client. Un solo numero con due usi, che sono le due metà
	// della stessa garanzia:
	//
	//   - BARRIERA: il commit di quella partizione non lo supera. Vale sempre e non dipende dal
	//     client: quei record non verranno mai dichiarati elaborati, quindi il peggio che può
	//     succedere è che il commit resti indietro e il prossimo avvio rilegga.
	//   - RIAVVOLGIMENTO: quando la partizione ci viene riassegnata, ci si riporta la posizione di
	//     consumo. Serve perché franz riprende dalla PROPRIA posizione interna e non dall'ultimo
	//     commit — misurato: commit a 471, record 471/472/473 scartati, ripartenza da 474. librdkafka
	//     invece rilegge da sé, ed è per questo che il driver confluent non ha bisogno di nulla di
	//     tutto ciò.
	//
	// La barriera è la correttezza, il riavvolgimento è la liquidità: senza la prima si perdono
	// record, senza il secondo il commit resta fermo e si rilegge tutto al riavvio.
	gap map[driver.TopicPartition]kgo.EpochOffset
	// pending sono i riavvolgimenti già maturati (partizione riassegnata) e non ancora applicati: si
	// applicano al primo poll utile, non nella callback, perché dentro la callback il client non ha
	// ancora finito di stabilire le posizioni e sovrascriverebbe la nostra SetOffsets.
	pending map[driver.TopicPartition]kgo.EpochOffset
}

// noteGap registra un record scartato e mai elaborato. Tiene il minimo per partizione: è il primo
// buco, e committare oltre quello sarebbe una perdita.
func (o *rebalanceObserver) noteGap(r *kgo.Record) {
	tp := driver.TopicPartition{Topic: r.Topic, Partition: r.Partition}
	o.mu.Lock()
	defer o.mu.Unlock()
	if cur, ok := o.gap[tp]; ok && cur.Offset <= r.Offset {
		return
	}
	if o.gap == nil {
		o.gap = make(map[driver.TopicPartition]kgo.EpochOffset)
	}
	o.gap[tp] = kgo.EpochOffset{Epoch: r.LeaderEpoch, Offset: r.Offset}
}

// capCommit dice se l'offset che si sta per committare supera un buco. Committare oltre
// dichiarerebbe elaborato un record che nessuno ha visto.
func (o *rebalanceObserver) capCommit(tp driver.TopicPartition, next int64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	at, ok := o.gap[tp]
	return ok && at.Offset < next
}

// clearGap toglie la barriera quando il buco è stato riletto: il record consegnato è a un offset non
// superiore al buco, quindi da lì in poi la sequenza è di nuovo completa.
func (o *rebalanceObserver) clearGap(tp driver.TopicPartition, offset int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if at, ok := o.gap[tp]; ok && offset <= at.Offset {
		delete(o.gap, tp)
	}
}

// onAssigned segna che le partizioni riassegnate con un buco aperto vanno riavvolte. NON chiama
// SetOffsets qui: dentro la callback il client non ha ancora finito di stabilire le posizioni (fa la
// sua OffsetFetch subito dopo) e sovrascriverebbe la nostra — misurato, il riavvolgimento risultava
// eseguito nei log e senza effetto. L'applicazione avviene al poll successivo, sulla nostra
// goroutine, che è anche ciò che la doc di franz raccomanda per SetOffsets.
func (o *rebalanceObserver) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	log.Info().Str("consumer", o.name).Int("topics", len(assigned)).
		Interface("assignment", assigned).Msg("corekafka: partizioni assegnate")

	o.mu.Lock()
	defer o.mu.Unlock()
	for topic, ps := range assigned {
		for _, p := range ps {
			tp := driver.TopicPartition{Topic: topic, Partition: p}
			if at, ok := o.gap[tp]; ok {
				if o.pending == nil {
					o.pending = make(map[driver.TopicPartition]kgo.EpochOffset)
				}
				o.pending[tp] = at
			}
		}
	}
}

// takePending consegna i riavvolgimenti da applicare, nella forma che vuole SetOffsets. Il buco NON
// si cancella qui: si cancella quando il record è stato davvero riletto (clearGap), perché è quello
// l'evento che chiude la falla.
func (o *rebalanceObserver) takePending() map[string]map[int32]kgo.EpochOffset {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.pending) == 0 {
		return nil
	}
	set := make(map[string]map[int32]kgo.EpochOffset)
	for tp, at := range o.pending {
		if set[tp.Topic] == nil {
			set[tp.Topic] = make(map[int32]kgo.EpochOffset)
		}
		set[tp.Topic][tp.Partition] = at
	}
	o.pending = nil
	return set
}

func (o *rebalanceObserver) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	o.add(revoked)
	log.Info().Str("consumer", o.name).Int("topics", len(revoked)).
		Interface("assignment", revoked).Msg("corekafka: partizioni revocate")
}

func (o *rebalanceObserver) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	o.add(lost)
	log.Warn().Str("consumer", o.name).Int("topics", len(lost)).
		Interface("assignment", lost).Msg("corekafka: partizioni perse (sessione scaduta)")
}

// add accumula le partizioni perse finché l'engine non le raccoglie: due rebalance fra due poll sono
// improbabili ma non impossibili, e perderne uno significherebbe non filtrare quei record.
func (o *rebalanceObserver) add(parts map[string][]int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for topic, ps := range parts {
		for _, p := range ps {
			o.revoked = append(o.revoked, driver.TopicPartition{Topic: topic, Partition: p})
		}
	}
	// Una revoca senza partizioni non lascia traccia: non c'è nulla da invalidare, e segnalare un
	// reset farebbe scartare un batch che è ancora interamente nostro.
}

// takeRevoked consuma le partizioni revocate: le ritorna una sola volta per rebalance, così l'engine
// filtra il batch una volta e riprende a consumare.
func (o *rebalanceObserver) takeRevoked() []driver.TopicPartition {
	o.mu.Lock()
	defer o.mu.Unlock()
	parts := o.revoked
	o.revoked = nil
	return parts
}

// session è la parte comune ai due client: il buffer dei record fetchati, il flag di revoca e la
// disciplina del blocco dei rebalance. Poll vive qui perché è identico nelle due modalità — cambia
// solo COME si conferma ciò che si è consumato, e quello sta nei due tipi che la embeddano.
type session struct {
	name    string
	p       poller
	rb      *rebalanceObserver
	maxPoll int

	// partial abilita il reset PARZIALE: alla revoca si scartano i soli record delle partizioni
	// perse, invece di buttare tutto. Vale in modalità handle; in EOS no, perché il batch è l'unità
	// della transazione e una revoca la invalida per intero.
	partial bool

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
	// Prima di tutto: un rebalance avvenuto nel frattempo invalida il batch che l'engine sta
	// accumulando e la parte di buffer che appartiene alle partizioni perse.
	if revoked := s.rb.takeRevoked(); len(revoked) > 0 {
		return nil, s.onRevoked(revoked)
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
		if revoked := s.rb.takeRevoked(); len(revoked) > 0 {
			return nil, s.onRevoked(revoked)
		}
		return nil, nil
	}

	s.buf = fetches.Records()
	r := s.buf[0]
	s.buf = s.buf[1:]
	s.holding = true
	return r, nil
}

// onRevoked ripulisce il buffer dalle partizioni perse e costruisce il reset da consegnare
// all'engine. I record bufferizzati delle partizioni REVOCATE si buttano senza essere consegnati —
// li rileggerà dall'ultimo commit chi le possiede ora — mentre quelli delle partizioni RITENUTE
// restano: sono ancora nostri, seguono gli offset già committati e buttarli li perderebbe, perché la
// posizione di fetch non torna indietro.
//
// In EOS (partial == false) il buffer si butta tutto e il reset non porta partizioni: là il batch è
// l'unità della transazione, che una revoca invalida per intero.
func (s *session) onRevoked(revoked []driver.TopicPartition) error {
	if !s.partial {
		s.buf = nil
		return driver.NewError(driver.SeverityReset, "poll", errRebalanced)
	}
	before := len(s.buf)
	dropped := droppedOffsets(s.buf, revoked)
	// Prima del filtro, finché i record da buttare ci sono ancora: ognuno è un buco, e registrarlo è
	// ciò che impedisce al commit di scavalcarlo.
	lost := make(map[driver.TopicPartition]struct{}, len(revoked))
	for _, p := range revoked {
		lost[p] = struct{}{}
	}
	for _, r := range s.buf {
		if _, gone := lost[driver.TopicPartition{Topic: r.Topic, Partition: r.Partition}]; gone {
			s.rb.noteGap(r)
		}
	}
	s.buf = dropRevokedRecords(s.buf, revoked)
	if n := before - len(s.buf); n > 0 {
		// Record fetchati dal client e mai consegnati all'engine: li rileggerà dall'ultimo commit chi
		// possiede ora quelle partizioni. Vale la pena contarli, perché è l'unico punto in cui la
		// libreria butta record che nessuno ha visto.
		log.Info().Str("consumer", s.name).Int("records", n).Int("kept", len(s.buf)).
			Interface("dropped", dropped).
			Msg("corekafka: record bufferizzati e non consegnati, scartati alla revoca")
	}
	return driver.NewRevokeError("poll", errRebalanced, revoked)
}

// dropRevokedRecords toglie dal buffer i record delle partizioni perse, conservando l'ordine degli
// altri. Filtra in place: il buffer è quello del poll, e riallocarlo a ogni rebalance sarebbe un
// costo inutile su un cammino che deve solo perdere meno.
func dropRevokedRecords(buf []*kgo.Record, revoked []driver.TopicPartition) []*kgo.Record {
	lost := make(map[driver.TopicPartition]struct{}, len(revoked))
	for _, p := range revoked {
		lost[p] = struct{}{}
	}
	kept := buf[:0]
	for _, r := range buf {
		if _, gone := lost[driver.TopicPartition{Topic: r.Topic, Partition: r.Partition}]; gone {
			continue
		}
		kept = append(kept, r)
	}
	return kept
}

// droppedOffsets elenca gli offset che lo scarto sta per buttare, per partizione: è la diagnostica
// dell'unico punto in cui la libreria butta record che nessuno ha visto.
func droppedOffsets(buf []*kgo.Record, revoked []driver.TopicPartition) map[int32][]int64 {
	lost := make(map[driver.TopicPartition]struct{}, len(revoked))
	for _, p := range revoked {
		lost[p] = struct{}{}
	}
	out := map[int32][]int64{}
	for _, r := range buf {
		if _, gone := lost[driver.TopicPartition{Topic: r.Topic, Partition: r.Partition}]; gone {
			out[r.Partition] = append(out[r.Partition], r.Offset)
		}
	}
	return out
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
