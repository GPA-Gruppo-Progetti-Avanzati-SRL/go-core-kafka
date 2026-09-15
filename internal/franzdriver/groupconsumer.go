package franzdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// groupClient è ciò che serve al commit at-least-once oltre al consumo: lo implementa *kgo.Client, ed
// è un'interfaccia perché la disciplina del commit (no-op a tracker vuoto, rilascio del rebalance) è
// logica del driver e va verificata senza un broker.
type groupClient interface {
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	// SetOffsets riavvolge la posizione di consumo delle partizioni indicate: serve dopo una revoca,
	// perché franz riprende dalla propria posizione interna e non dall'ultimo commit.
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	Close()
}

// groupConsumer implementa driver.GroupConsumer (modalità handle, at-least-once): il consumer di
// gruppo di session più il commit manuale degli offset.
type groupConsumer struct {
	session
	cl      groupClient
	offsets *offsetTracker
}

// Poll consegna il record all'engine e ne traccia l'offset per il commit successivo.
func (g *groupConsumer) Poll(ctx context.Context, timeout time.Duration) (*message.Record, error) {
	// Riavvolgimenti maturati in una callback di assegnazione: si applicano QUI, sulla nostra
	// goroutine e fuori dalla callback, dove il client ha già stabilito le sue posizioni e non le
	// sovrascrive più.
	if set := g.rb.takePending(); set != nil {
		g.cl.SetOffsets(set)
		log.Info().Str("consumer", g.name).Interface("rewind", set).
			Msg("corekafka: partizioni riavvolte all'ultimo commit (lavoro non confermato prima della revoca)")
	}

	r, err := g.pollRaw(ctx, timeout)
	if err != nil {
		// Revoca parziale: gli offset delle partizioni perse vanno scartati, o il Commit successivo
		// li confermerebbe per conto di chi le possiede ora. Si fa QUI e non nella callback del
		// rebalance perché quella gira sulla goroutine del client: toccare il tracker da lì sarebbe
		// una data race. Gli offset delle partizioni ritenute restano, perché i loro record sono
		// ancora nel batch dell'engine.
		if revoked, ok := driver.RevokedOf(err); ok {
			// I record consegnati e non ancora committati di quelle partizioni li sta buttando
			// l'engine: ognuno è un buco, e il primo di essi è insieme la barriera al commit e il
			// punto da cui riavvolgere.
			for _, r := range g.offsets.firstOf(revoked) {
				g.rb.noteGap(r)
			}
			g.offsets.resetPartitions(revoked)
		}
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	// Il buco è stato riletto: da qui la sequenza è di nuovo completa e il commit può riprendere.
	g.rb.clearGap(driver.TopicPartition{Topic: r.Topic, Partition: r.Partition}, r.Offset)
	g.offsets.track(r)
	return toRecord(r), nil
}

// Commit conferma gli offset dei record consegnati dall'ultimo Commit e sblocca i rebalance.
//
// Passa da CommitRecords e non da un commit di offset nudi perché è il record a portare con sé il
// leader epoch: committarlo senza epoch toglierebbe al broker il modo di rifiutare il commit di un
// membro rimasto indietro di una generazione. Dopo una revoca — o una Discard — il tracker è vuoto e
// il commit è un no-op: quei record li rilegge il nuovo owner, invece di essere dichiarati elaborati
// da chi non li possiede più.
func (g *groupConsumer) Commit(ctx context.Context) error {
	if g.offsets.empty() {
		g.release()
		return nil
	}
	// La barriera vince sul massimo consegnato: se un record di quella partizione è stato buttato
	// senza essere elaborato, quella partizione non si committa affatto finché il buco non è riletto.
	recs := g.capped(g.offsets.records())
	if err := g.cl.CommitRecords(ctx, recs...); err != nil {
		return wrap("commit", err)
	}
	g.offsets.reset()
	g.release()
	return nil
}

// Discard scarta gli offset tracciati e non committati: l'engine la chiama quando butta il batch in
// volo, e senza di essa il Commit successivo confermerebbe record che nessuno ha elaborato (vedi il
// contratto di driver.Session.Discard).
func (g *groupConsumer) Discard(context.Context) {
	g.offsets.reset()
	g.dropAndRelease()
}

// capped toglie dal commit i record la cui partizione ha una barriera più bassa: committarli
// dichiarerebbe elaborato ciò che nessuno ha visto. Non si "abbassa" l'offset — CommitRecords lavora
// sui record, e l'unico modo di non superare la barriera è non committare affatto quella partizione,
// lasciando che sia la rilettura a far ripartire il commit da sotto il buco.
func (g *groupConsumer) capped(recs []*kgo.Record) []*kgo.Record {
	out := recs[:0]
	for _, r := range recs {
		tp := driver.TopicPartition{Topic: r.Topic, Partition: r.Partition}
		if g.rb.capCommit(tp, r.Offset+1) {
			log.Warn().Str("consumer", g.name).Str("topic", r.Topic).Int32("partition", r.Partition).
				Int64("offset", r.Offset).
				Msg("corekafka: commit trattenuto: un record di questa partizione è stato scartato senza essere elaborato e va riletto prima di poter confermare oltre")
			continue
		}
		out = append(out, r)
	}
	return out
}

// Close rilascia il rebalance PRIMA di chiudere il client, e non è un dettaglio di cortesia: con
// BlockRebalanceOnPoll ogni poll registra un poller, e la poll finale — quella che ritorna subito
// perché il context del loop è stato cancellato — lo registra dentro franz senza rilasciarlo (il
// ramo `ctx.Done()` di PollRecords chiama waitAndAddPoller e ritorna un fetch sintetico). Close fa
// LeaveGroup, che attende che i poller scendano a zero: senza il rilascio resta appeso fino alla
// deadline dell'arresto, e con lui l'OnStop dell'engine — un SIGTERM che finisce in "stop failed:
// context deadline exceeded" invece che in un LeaveGroup pulito.
//
// È lo stesso motivo per cui franz espone Client.CloseAllowingRebalance; qui il rilascio passa da
// dropAndRelease perché butta anche i record fetchati e non consegnati, che nessuno consumerà più.
func (g *groupConsumer) Close() error {
	g.offsets.reset()
	g.dropAndRelease()
	g.cl.Close()
	return nil
}
