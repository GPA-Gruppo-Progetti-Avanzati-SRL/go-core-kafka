// Package consumer è l'engine di go-core-kafka: per ogni ConsumerSpec attivo avvia una goroutine che
// consuma dal client (dietro internal/driver) ed esegue il Handler (modalità handle, at-least-once) o
// il Transformer (modalità transform, EOS Kafka->Kafka). Non importa mai il client concreto: usa solo
// le interfacce driver.* ottenute dalla driver.Factory iniettata.
//
// Ogni consumer è supervisionato: il loop di consumo può terminare con errore e, a seconda della
// severità classificata dal driver, essere riavviato in-process con backoff invece di far terminare
// il processo. Vedi runner.run.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/processor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// LabelConsumer è l'etichetta pprof applicata alla goroutine di ogni consumer.
const LabelConsumer = "kafka_consumer"

// DefaultFinalFlushTimeout è il bound del flush finale quando il context dell'OnStop non ne ha uno
// (fx.StopTimeout non impostato). Un flush senza bound terrebbe il processo in piedi a tempo
// indefinito, che è esattamente ciò che un SIGTERM chiede di non fare.
const DefaultFinalFlushTimeout = 15 * time.Second

// Consumers è il valore fx che tiene vivo l'engine (nessuna API pubblica: la sua costruzione avvia i
// consumer via lifecycle).
type Consumers struct{}

type params struct {
	fx.In
	LC           fx.Lifecycle
	Shutdowner   fx.Shutdowner
	Specs        []spec.ProcessorSpec
	Server       spec.KafkaServer
	Factory      driver.Factory
	Handlers     []processor.HandlerRegistration     `group:"kafka_handlers"`
	Transformers []processor.TransformerRegistration `group:"kafka_transformers"`
	DLQ          producer.IProducer                  `optional:"true"`
}

// runner incapsula uno spec e il suo processor; apre il proprio client a ogni tentativo di run.
type runner struct {
	spec        spec.ProcessorSpec
	server      spec.KafkaServer
	factory     driver.Factory
	handler     processor.Handler
	transformer processor.Transformer
	dlq         producer.IProducer
	// stop porta al flush finale il context dell'arresto (vedi shutdown); è condiviso da tutti i
	// runner dello stesso engine.
	stop *shutdown
}

// shutdown consegna al flush finale il context dell'OnStop di fx — quello con la deadline
// dell'arresto — al posto del context del loop, che a quel punto è cancellato per costruzione.
//
// Il passaggio avviene per riferimento condiviso e non per parametro perché i due lati stanno su
// goroutine diverse: OnStop lo deposita e poi cancella, il loop lo raccoglie quando osserva la
// cancellazione. Il mutex rende l'ordine evidente senza doverlo dedurre dalla cancellazione.
type shutdown struct {
	mu  sync.Mutex
	ctx context.Context
}

func (s *shutdown) set(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx = ctx
}

// flushContext ritorna il context per il flush finale: quello dell'OnStop se depositato, altrimenti
// (arresto non passato dal lifecycle) uno nuovo con DefaultFinalFlushTimeout. La cancel va sempre
// invocata dal chiamante.
func (s *shutdown) flushContext() (context.Context, context.CancelFunc) {
	if s == nil {
		// Runner costruito fuori dall'engine (test, wiring manuale): non c'è un arresto da cui
		// ereditare la deadline, ma il flush finale deve comunque averne una.
		return context.WithTimeout(context.Background(), DefaultFinalFlushTimeout)
	}
	s.mu.Lock()
	ctx := s.ctx
	s.mu.Unlock()

	if ctx == nil {
		return context.WithTimeout(context.Background(), DefaultFinalFlushTimeout)
	}
	if _, ok := ctx.Deadline(); !ok {
		return context.WithTimeout(ctx, DefaultFinalFlushTimeout)
	}
	return context.WithCancel(ctx)
}

// NewConsumers valida gli spec contro i processor registrati (fail-fast dal costruttore) e registra
// nel lifecycle fx l'avvio/arresto cooperativo di una goroutine per consumer.
// seams sono i due value group indicizzati per nome del processor. La modalità di un processor è
// DERIVATA da quale dei due lo contiene, quindi qui vivono insieme.
type seams struct {
	handlers     map[string]processor.Handler
	transformers map[string]processor.Transformer
}

func NewConsumers(p params) (*Consumers, error) {
	sm := seams{
		handlers:     make(map[string]processor.Handler, len(p.Handlers)),
		transformers: make(map[string]processor.Transformer, len(p.Transformers)),
	}
	for _, h := range p.Handlers {
		sm.handlers[h.Consumer] = h.Handler
	}
	for _, t := range p.Transformers {
		sm.transformers[t.Consumer] = t.Transformer
	}

	// La sezione `server` è validata QUI, dal costruttore fx, e non dall'app: i tag `validate:` non si
	// applicano da soli, e finché a eseguirli era solo la core.ReadConfig dell'app le regole dichiarate
	// sullo spec valevano per chi passava di lì e per nessun altro. Copre anche le chiavi riservate in
	// kafka-properties, che sono invarianti dell'engine e non default sovrascrivibili (vedi
	// spec.DeniedKafkaProperties).
	if err := spec.ValidateServer(p.Server); err != nil {
		return nil, err
	}

	// Gli spec arrivano già FILTRATI da Config.ActiveProcessors — che è l'unico punto in cui la
	// regola di attivazione è scritta — quindi qui non si ri-decide chi è attivo: si costruisce.
	runners := make([]*runner, 0, len(p.Specs))
	for _, raw := range p.Specs {
		r, err := newRunner(raw, p.Server, sm, p.Factory, p.DLQ)
		if err != nil {
			return nil, err
		}
		runners = append(runners, r)
	}

	// Nota: nemmeno i processor dei consumer non attivi arrivano qui — la costruzione lazy
	// (processor.Configure) fornisce a fx solo i processor dei consumer attivi, quindi i due value group
	// contengono già solo quelli attivati. Lo skip è loggato dal registry al momento della registrazione.

	ctx, cancel := context.WithCancel(context.Background())
	sd := &shutdown{}
	for _, r := range runners {
		r.stop = sd
	}
	// done è allocato QUI e non dentro OnStart: OnStop gira anche quando OnStart non è mai stato
	// eseguito (un costruttore successivo che fallisce), e attendere su un canale nil sarebbe un
	// deadlock invece di un no-op.
	done := make(chan struct{}, len(runners))
	started := 0

	p.LC.Append(fx.Hook{
		OnStart: func(context.Context) error {
			for _, r := range runners {
				// pprof.Do etichetta la goroutine col nome del consumer: da Go 1.27 la
				// label compare anche nei traceback e rende leggibili i profili
				// goroutine/goroutineleak di un processo con più consumer.
				go pprof.Do(ctx, pprof.Labels(LabelConsumer, r.spec.Name), func(context.Context) {
					defer func() { done <- struct{}{} }()
					if err := r.run(ctx); err != nil && ctx.Err() == nil {
						log.Error().Err(err).Str("consumer", r.spec.Name).Msg("corekafka: consumer terminato con errore, arresto dell'applicazione")
						_ = p.Shutdowner.Shutdown()
					}
				})
			}
			started = len(runners)
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			// Il context dell'hook va depositato PRIMA della cancellazione: è la cancellazione che
			// sveglia i loop, ed è lì che lo raccolgono per il flush finale.
			sd.set(stopCtx)
			cancel()
			for i := 0; i < started; i++ {
				select {
				case <-done:
				case <-stopCtx.Done():
					// Un consumer appeso non deve impedire al processo di terminare: si registra
					// quanti non hanno chiuso e si prosegue. Senza questo bound l'arresto
					// dell'intera applicazione dipendeva dal fatto che ogni runner tornasse.
					log.Error().Str("consumer", "*").Int("pending", started-i).
						Msg("corekafka: arresto forzato, alcuni consumer non hanno terminato entro il timeout")
					return nil
				}
			}
			return nil
		},
	})

	return &Consumers{}, nil
}

// newRunner costruisce il runner di un processor attivo: risolve lo spec, ne verifica la coerenza,
// deriva la modalità dalla registrazione e passa le properties a chi le vuole all'avvio.
//
// Sta fuori da NewConsumers perché è la parte PER-PROCESSOR: lì dentro il ciclo di vita fx — la
// ragione per cui quel costruttore esiste — restava sepolto sotto cento righe di validazione.
func newRunner(raw spec.ProcessorSpec, k spec.KafkaServer, sm seams, f driver.Factory, dlq producer.IProducer) (*runner, error) {
	// Sullo spec GREZZO, prima di risolverlo: i tag `validate:` e le chiavi riservate vanno attribuiti
	// a chi li ha scritti, e un blocco ereditato è già stato validato al livello di `server`.
	if err := spec.ValidateProcessor(raw); err != nil {
		return nil, err
	}

	// Qui lo spec diventa quello EFFETTIVO: Resolve eredita dai blocchi di `server` i campi non
	// valorizzati e poi applica i default della libreria. Da questa riga in poi nessuno consulta più
	// i blocchi globali — un campo letto da r.spec è già il valore giusto.
	s := raw.Resolve(k)
	owner := "processor " + s.Name

	// Le RELAZIONI fra i knob si verificano invece sul risolto, dove il valore effettivo è quello che
	// conta: sono combinazioni in cui ogni campo preso da solo è legittimo e l'insieme non lo è.
	for _, check := range []func(string) error{
		s.Consumer.Validate, s.Producer.Validate, s.Restart.Validate,
	} {
		if err := check(owner); err != nil {
			return nil, err
		}
	}
	if s.Restart.Unlimited() && !s.Restart.IsDisabled() {
		log.Warn().Str("processor", s.Name).
			Msg("corekafka: restart.max-attempts negativo = riavvii ILLIMITATI: un guasto stabile verrà mascherato indefinitamente invece di far uscire il processo. Guardare corekafka_consumer_restarts_total: una crescita continua senza record consumati è quel caso")
	}

	r := &runner{spec: s, server: k, factory: f, dlq: dlq}

	// La modalità è DERIVATA dalla registrazione: nome nel gruppo kafka_handlers -> handle, in
	// kafka_transformers -> transform. In entrambi -> ambiguo; in nessuno -> non registrato.
	h, isHandler := sm.handlers[s.Name]
	t, isTransformer := sm.transformers[s.Name]
	switch {
	case isHandler && isTransformer:
		return nil, fmt.Errorf("corekafka: processor %q: registrato sia come Handler sia come Transformer (ambiguo)", s.Name)
	case isHandler:
		r.handler = h
		// In modalità handle il producer è quello CONDIVISO del processo (serve al DLQ): un blocco
		// `producer` sul processor non ha un destinatario. Warning e non errore perché la modalità è
		// derivata dalla registrazione, quindi chi scrive la config non ha modo di saperlo guardando
		// solo il YAML.
		if !raw.Producer.IsZero() {
			log.Warn().Str("processor", s.Name).
				Msg("corekafka: il blocco `producer` su un processor in modalità handle è ignorato (il producer del DLQ è condiviso): usare `server.producer`")
		}
		if s.Consumer.OnError == spec.OnErrorDeadletter && s.Consumer.Deadletter() == "" {
			return nil, fmt.Errorf("corekafka: processor %q: consumer.on-error=deadletter richiede consumer.deadletter-topic", s.Name)
		}
		// Il DLQ (via Producer condiviso) serve sia per la policy di default deadletter sia quando
		// l'handler sceglie il deadletter a runtime (processor.DeadLetter): se c'è un
		// deadletter-topic, il Producer deve essere presente.
		if s.HasDeadletter() && dlq == nil {
			return nil, fmt.Errorf("corekafka: processor %q: consumer.deadletter-topic impostato richiede il Producer (usare corekafka.WithProducer)", s.Name)
		}
	case isTransformer:
		r.transformer = t
		if s.TransactionalID == "" {
			return nil, fmt.Errorf("corekafka: processor %q (transform): transactional-id obbligatorio", s.Name)
		}
		// La transazione copre un batch: se scade prima che il batch venga anche solo CHIUSO, il broker
		// la considera abbandonata e fa fencing del producer a ogni giro — un loop di riavvii in cui
		// non viene mai committato nulla. Il controllo sta qui e non in ProducerTuning.Validate perché
		// incrocia i due blocchi ed è specifico della modalità, che a quel livello non è nota.
		if ms := s.Producer.TransactionTimeoutMs; ms > 0 && time.Duration(ms)*time.Millisecond <= s.Consumer.CutFrequency {
			return nil, fmt.Errorf("corekafka: processor %q (transform): producer.transaction-timeout-ms (%dms) <= consumer.cut-frequency (%s): la transazione scadrebbe prima della chiusura del batch, con fencing a ogni giro",
				s.Name, ms, s.Consumer.CutFrequency)
		}
	default:
		return nil, fmt.Errorf("corekafka: processor %q: nessun processor registrato (usare corekafka.RegisterHandler o RegisterTransformer con questo nome)", s.Name)
	}

	// Configure opzionale: passa le Properties dello spec al seam che le vuole all'avvio
	// (precompute/validazione). Un errore fa fail-fast: l'app non parte. Il loop copre i due seam
	// insieme — solo uno è valorizzato, e l'assertion su un'interfaccia nil è false — perché scritto
	// due volte era una coppia da ricordarsi di aggiornare in parallelo.
	for _, seam := range []any{r.handler, r.transformer} {
		c, ok := seam.(processor.Configurable)
		if !ok {
			continue
		}
		if err := c.Configure(s.Properties); err != nil {
			return nil, fmt.Errorf("corekafka: processor %q: Configure: %w", s.Name, err)
		}
	}

	return r, nil
}

// run supervisiona il consumo: esegue il loop e, se termina con errore, decide se ricostruire il
// client e riprovare dopo un backoff o lasciare risalire l'errore (che fa terminare il processo).
//
// La distinzione la fa la severità assegnata dal driver. L'indisponibilità di un broker o un fencing
// EOS sono condizioni da cui si esce ricostruendo consumer e producer: senza questo livello l'unico
// recovery sarebbe la morte del processo, che su un rolling restart dei broker diventa un
// CrashLoopBackOff. Restano invece letali gli errori su cui nessun retry può aiutare — credenziali
// sbagliate, config rifiutata — e, per default, gli errori di business sotto fail-fast (vedi
// RestartSpec.OnBusinessError).
func (r *runner) run(ctx context.Context) error {
	ctx = spec.ContextWithProperties(ctx, r.spec.Name, r.spec.Properties)
	b := newBackoff(r.spec.Restart)

	for {
		start := time.Now()
		err := r.runOnce(ctx)
		if err == nil || ctx.Err() != nil {
			return nil // arresto pulito (shutdown cooperativo)
		}

		sev := driver.SeverityOf(err)
		if !r.restartable(sev) {
			return err
		}
		// Un run lungo e sano non deve ereditare i tentativi bruciati da un guasto vecchio.
		if time.Since(start) >= r.spec.Restart.ResetAfter {
			b.reset()
		}
		wait, ok := b.next()
		if !ok {
			return fmt.Errorf("corekafka: processor %q: esauriti i %d tentativi di riavvio: %w", r.spec.Name, r.spec.Restart.Attempts(), err)
		}

		restartsTotal.WithLabelValues(r.spec.Name, sev.String()).Inc()
		log.Warn().Err(err).Str("consumer", r.spec.Name).Str("severity", sev.String()).
			Int("attempt", b.count()).Dur("backoff", wait).
			Msg("corekafka: consumer terminato con errore, riavvio dopo backoff")

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// restartable dice se una severità giustifica la ricostruzione del client.
func (r *runner) restartable(sev driver.Severity) bool {
	if r.spec.Restart.IsDisabled() {
		return false
	}
	switch sev {
	case driver.SeverityPermanent:
		// Config o credenziali: riprovare produce lo stesso errore all'infinito.
		return false
	case driver.SeverityBusiness:
		// Errore risalito da Handle/Transform sotto fail-fast: la semantica documentata è "non
		// committa ed esce". Riprovare in-process un record poison sarebbe un loop senza uscita.
		return r.spec.Restart.RestartsOnBusinessError()
	default:
		return true
	}
}

// runOnce esegue un ciclo di vita completo del client: lo crea, consuma finché può, lo chiude.
func (r *runner) runOnce(ctx context.Context) error {
	if r.transformer != nil {
		return r.runTransform(ctx)
	}
	return r.runHandle(ctx)
}

// outcome è la decisione presa sul batch dopo il ritorno del processor.
type outcome int

const (
	outcomeCommit     outcome = iota // tutto elaborato: committa
	outcomeDeadletter                // instrada i record indicati al DLQ, poi committa
	outcomeFail                      // non committare: l'errore risale (replay)
)

// classify traduce il valore ritornato da Handle/Transform nella decisione sul batch, ed è UNICA per
// le due modalità: prima handle valutava ErrFailFast prima di PoisonRecords e transform il contrario,
// quindi lo stesso errore composito veniva classificato in due modi diversi.
//
// Ritorna anche COSA mandare al DLQ: il *PoisonRecords intero e non la sola lista di record, perché
// è lì che vivono le cause per-record prodotte da Convert — servono a toDLQ per scrivere un
// corekafka-dlq-error diverso su ogni messaggio. Cambia solo COME i due modi consegnano (producer
// condiviso in handle, stessa transazione EOS in transform), non quali siano.
func classify(err error, batch []*message.Record, onError string) (outcome, *processor.PoisonRecords, error) {
	switch {
	case err == nil:
		return outcomeCommit, nil, nil
	case errors.Is(err, processor.ErrFailFast):
		// Scelta esplicita del processor: vince su qualunque altra indicazione, policy inclusa.
		return outcomeFail, nil, err
	}
	// Il processor ha indicato QUALI record sono poison: il resto del batch è stato elaborato.
	if pr, ok := errors.AsType[*processor.PoisonRecords](err); ok {
		return outcomeDeadletter, pr, pr.Cause
	}
	// Errore generico: decide la policy di default dello spec. Nessuna causa per-record da attribuire:
	// l'intero batch fallisce per lo stesso motivo.
	if onError == spec.OnErrorDeadletter {
		return outcomeDeadletter, &processor.PoisonRecords{Records: batch, Cause: err}, err
	}
	return outcomeFail, nil, err
}

// poisonRecords estrae i record da un esito di classify, tollerando il nil dei rami che non
// deadletterano.
func poisonRecords(pr *processor.PoisonRecords) []*message.Record {
	if pr == nil {
		return nil
	}
	return pr.Records
}

// absorb gestisce le severità che NON richiedono di ricostruire il client: il batch in volo viene
// scartato senza commit e il loop prosegue. Ritorna true se l'errore è stato assorbito.
//
// È il caso del rebalance (SeverityReset): i record accumulati vengono da partizioni che potrebbero
// non essere più nostre, quindi vanno riletti dal nuovo owner — duplicati, non buchi. E dell'abort
// transazionale richiesto dal broker (SeverityAbort), dopo il quale la sessione resta utilizzabile.
func (r *runner) absorb(ctx context.Context, err error, s driver.Session, batch *[]*message.Record) bool {
	sev := driver.SeverityOf(err)
	if sev != driver.SeverityReset && sev != driver.SeverityAbort {
		return false
	}
	// Scartare il batch non è solo troncare la slice: gli offset di quei record sono tracciati DENTRO
	// il driver, e senza Discard il Commit successivo li confermerebbe — record dichiarati elaborati
	// che nessuno ha elaborato, cioè un buco, l'opposto di quello che questa funzione promette.
	// Il rebalance callback azzera già il tracker alla revoca, ma un SeverityReset può risalire da
	// Poll/Commit SENZA revoca (ErrIllegalGeneration, ErrUnknownMemberID, ErrMaxPollExceeded); in EOS
	// Discard abortisce anche la transazione, che altrimenti resterebbe aperta.
	s.Discard(ctx)
	n := len(*batch)
	*batch = (*batch)[:0]
	batchDiscardedTotal.WithLabelValues(r.spec.Name, sev.String()).Add(float64(n))
	log.Info().Err(err).Str("consumer", r.spec.Name).Int("records", n).
		Msg("corekafka: batch scartato senza commit, il consumo prosegue")
	return true
}

// sendDeadletter produce i record sul topic DLQ dello spec. Ritorna errore (→ fail-fast) se il DLQ
// non è configurato, così una richiesta di deadletter senza DLQ non perde silenziosamente i dati.
func (r *runner) sendDeadletter(ctx context.Context, pr *processor.PoisonRecords) error {
	if r.dlq == nil || r.spec.Consumer.Deadletter() == "" {
		return fmt.Errorf("corekafka: processor %q: deadletter richiesto ma non configurato (manca deadletter-topic/Producer): %w", r.spec.Name, pr.Cause)
	}
	if appErr := r.dlq.Produce(ctx, r.toDLQ(pr)); appErr != nil {
		return appErr
	}
	return nil
}

// flusher è ciò che distingue le due modalità: elabora il batch e lo rende definitivo — commit degli
// offset in handle, transazione atomica in transform. Ritorna l'errore da far risalire; nil significa
// batch consumato, e il loop lo tronca.
type flusher interface {
	flush(ctx context.Context, batch []*message.Record) error
}

// consume è il loop di consumo, UNICO per le due modalità: poll, accumula, chiudi il batch a
// dimensione o a tempo, assorbi gli eventi di protocollo, arrestati in modo cooperativo.
//
// Prima erano due loop copiati (uno in runHandle, uno in runTransform) identici tranne il corpo del
// flush: ogni correzione andava applicata due volte, ed è già così che le due modalità sono divergite
// una volta — l'ordine di valutazione dell'esito, poi unificato in classify.
func (r *runner) consume(ctx context.Context, s driver.Session, f flusher) error {
	ticker := time.NewTicker(r.spec.Consumer.CutFrequency)
	defer ticker.Stop()
	batch := make([]*message.Record, 0, r.spec.Consumer.MaxBatchSize)

	// flush misura, delega al flusher e tronca il batch SOLO se è stato reso definitivo.
	flush := func(fctx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		start := time.Now()
		err := f.flush(fctx, batch)
		batchDuration.WithLabelValues(r.spec.Name).Observe(time.Since(start).Seconds())
		if err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	// cut chiude il batch e assorbe gli eventi di protocollo. È una closure perché la stessa sequenza
	// serve al taglio a tempo e a quello a dimensione, e scriverla due volte è il modo in cui due rami
	// dello stesso loop divergono.
	cut := func(c context.Context) error {
		err := flush(c)
		if err == nil || r.absorb(c, err, s, &batch) {
			return nil
		}
		return err
	}

	for {
		select {
		case <-ctx.Done():
			// Ultimo flush col context dell'ARRESTO, non con quello del loop: quest'ultimo è appena
			// stato cancellato, quindi un Handle che tocchi un DB o un HTTP abortirebbe subito e il
			// commit non partirebbe nemmeno — il flush finale era destinato a fallire sempre, e ogni
			// SIGTERM riprocessava un batch intero (con i side-effect parziali già eseguiti).
			//
			// Vale anche in transform: il flush EOS è atomico, quindi committarlo qui è preferibile a
			// replayarlo, e se la deadline dell'arresto scade la transazione abortisce da sé — si
			// torna esattamente al replay di prima, senza un ramo di shutdown per modalità.
			fctx, cancel := r.stop.flushContext()
			err := flush(fctx)
			cancel()
			if err != nil {
				log.Error().Err(err).Str("consumer", r.spec.Name).
					Msg("corekafka: flush finale fallito in arresto, i record del batch saranno riconsumati")
			}
			return nil
		case <-ticker.C:
			if err := cut(ctx); err != nil {
				return err
			}
		default:
			rec, err := s.Poll(ctx, r.spec.Consumer.PollTimeout)
			if err != nil {
				// Qui non c'è nessun flush di mezzo: non è la sequenza di cut.
				if r.absorb(ctx, err, s, &batch) {
					continue
				}
				return err
			}
			if rec == nil {
				continue
			}
			consumedTotal.WithLabelValues(r.spec.Name).Inc()
			batch = append(batch, rec)
			if len(batch) >= r.spec.Consumer.MaxBatchSize {
				if err := cut(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// runHandle: modalità at-least-once. poll -> accumula -> (cut size/tempo) -> Handle -> commit dopo il ritorno.
func (r *runner) runHandle(ctx context.Context) error {
	gc, err := r.factory.NewGroupConsumer(r.spec, r.server)
	if err != nil {
		return err
	}
	defer gc.Close()
	return r.consume(ctx, gc, handleFlusher{r: r, gc: gc})
}

// handleFlusher rende definitivo il batch in modalità handle: Handle, eventuale DLQ col producer
// condiviso, poi commit degli offset.
type handleFlusher struct {
	r  *runner
	gc driver.GroupConsumer
}

func (h handleFlusher) flush(ctx context.Context, batch []*message.Record) error {
	r := h.r
	hErr := r.handler.Handle(ctx, batch)

	oc, pr, cause := classify(hErr, batch, r.spec.Consumer.OnError)
	poison := poisonRecords(pr)
	switch oc {
	case outcomeFail:
		return cause // niente commit → replay
	case outcomeDeadletter:
		if err := r.sendDeadletter(ctx, pr); err != nil {
			return err // DLQ non configurato/irraggiungibile → replay
		}
		deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(poison)))
	}
	// Una sola volta, fuori dallo switch: nel ramo commit `poison` è vuoto, quindi l'espressione era
	// la stessa scritta due volte — ed è la forma che transformFlusher ha già. Due contabilità
	// simmetriche scritte in modi diversi divergono senza che nessuno lo veda, se non su un grafico.
	processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch) - len(poison)))
	return h.gc.Commit(ctx)
}

// runTransform: modalità EOS Kafka->Kafka. poll -> accumula -> Begin -> Transform -> Produce -> Commit
// (atomico) o Abort. Un errore abortisce e forza il replay.
func (r *runner) runTransform(ctx context.Context) error {
	sess, err := r.factory.NewTransactSession(r.spec, r.server)
	if err != nil {
		return err
	}
	defer sess.Close()
	return r.consume(ctx, sess, transformFlusher{r: r, sess: sess})
}

// transformFlusher rende definitivo il batch in modalità transform: tutto dentro una transazione —
// record di output, eventuale DLQ e offset consumati sono atomici.
type transformFlusher struct {
	r    *runner
	sess driver.TransactSession
}

func (t transformFlusher) flush(ctx context.Context, batch []*message.Record) error {
	r := t.r
	// abort annulla la transazione in corso. L'esito è loggato e non ritornato: stiamo già gestendo
	// l'errore che ha reso necessario l'abort, ed è quello che deve risalire.
	abort := func() {
		if err := t.sess.Abort(ctx); err != nil {
			log.Warn().Err(err).Str("consumer", r.spec.Name).Msg("corekafka: abort della transazione fallito")
		}
	}

	if err := t.sess.Begin(ctx); err != nil {
		return err
	}
	out, tErr := r.transformer.Transform(ctx, batch)
	if err := r.resolveTopics(out); err != nil {
		// Difetto deterministico del transformer o della config: rigiocarlo darebbe lo stesso esito,
		// quindi si abortisce e l'errore risale (fail-fast) invece di provare a produrre.
		abort()
		return err
	}

	// Stesso modello a esiti della modalità handle (classify è condivisa), ma la consegna DLQ
	// avviene DENTRO la sessione transazionale (append agli output) così l'EOS resta intatto:
	// output "buoni" + record DLQ + commit offset sono atomici.
	oc, pr, cause := classify(tErr, batch, r.spec.Consumer.OnError)
	poison := poisonRecords(pr)
	switch oc {
	case outcomeFail:
		abort()
		return cause
	case outcomeDeadletter:
		dlq, derr := r.dlqRecords(pr)
		if derr != nil {
			abort()
			return derr
		}
		out = append(out, dlq...)
		deadletteredTotal.WithLabelValues(r.spec.Name).Add(float64(len(poison)))
	}

	if err := t.sess.Produce(ctx, out); err != nil {
		abort()
		return err
	}
	if err := t.sess.Commit(ctx); err != nil {
		abort()
		return err
	}
	producedTotal.WithLabelValues(r.spec.Name).Add(float64(len(out)))
	processedTotal.WithLabelValues(r.spec.Name).Add(float64(len(batch) - len(poison)))
	return nil
}

// dlqRecords converte i record poison in ProducerRecord verso il deadletter-topic dello spec (usati
// dalla modalità transform per instradare a DLQ dentro la stessa transazione EOS). Ritorna errore se
// il topic DLQ non è configurato, così una richiesta di DeadLetter senza DLQ non perde dati.
func (r *runner) dlqRecords(pr *processor.PoisonRecords) ([]*message.ProducerRecord, error) {
	if r.spec.Consumer.Deadletter() == "" {
		return nil, fmt.Errorf("corekafka: processor %q: DeadLetter richiesto ma deadletter-topic assente", r.spec.Name)
	}
	return r.toDLQ(pr), nil
}

// Header applicati ai record instradati al DLQ. Servono a diagnosticare (da dove veniva il record,
// perché è fallito, quando) e a riprocessare (il consumatore del DLQ sa il topic di origine e quante
// volte il record ci è già passato). Kafka-Delivery-Attempts riprende il nome usato da
// tpm-kafka-common, così gli strumenti di reprocessing esistenti lo riconoscono.
const (
	HeaderDLQSourceTopic     = "corekafka-dlq-source-topic"
	HeaderDLQSourcePartition = "corekafka-dlq-source-partition"
	HeaderDLQSourceOffset    = "corekafka-dlq-source-offset"
	HeaderDLQSourceTimestamp = "corekafka-dlq-source-timestamp"
	HeaderDLQProcessor       = "corekafka-dlq-processor"
	HeaderDLQError           = "corekafka-dlq-error"
	HeaderDLQErrorAt         = "corekafka-dlq-error-at"
	HeaderDeliveryAttempts   = "Kafka-Delivery-Attempts"
)

// toDLQ costruisce i ProducerRecord per il DLQ preservando payload e origine del record poison.
func (r *runner) toDLQ(pr *processor.PoisonRecords) []*message.ProducerRecord {
	at := time.Now().UTC().Format(time.RFC3339Nano)
	recs := pr.Records
	out := make([]*message.ProducerRecord, 0, len(recs))
	for i, rec := range recs {
		// Gli header di origine sono CLONATI: il record consumato non va mutato (l'handler può
		// averlo ancora in mano) e Set scrive in place. Il clone conserva le chiavi ripetute.
		h := rec.Headers.Clone()
		h.Set(HeaderDLQSourceTopic, rec.Topic)
		h.Set(HeaderDLQSourcePartition, strconv.Itoa(int(rec.Partition)))
		h.Set(HeaderDLQSourceOffset, strconv.FormatInt(rec.Offset, 10))
		h.Set(HeaderDLQProcessor, r.spec.Name)
		h.Set(HeaderDLQErrorAt, at)
		if !rec.Timestamp.IsZero() {
			h.Set(HeaderDLQSourceTimestamp, rec.Timestamp.UTC().Format(time.RFC3339Nano))
		}
		// La causa del SINGOLO record quando c'è (Convert la produce per ognuno), altrimenti quella
		// comune del gruppo: chi legge il DLQ vuole sapere perché è fallito QUESTO messaggio.
		if cause := pr.CauseFor(i); cause != nil {
			h.Set(HeaderDLQError, cause.Error())
		}
		// Un record già passato dal DLQ e reimmesso porta il contatore: incrementarlo permette a chi
		// riprocessa di fermarsi invece di girare all'infinito.
		h.Set(HeaderDeliveryAttempts, strconv.Itoa(attempts(rec.Headers)+1))
		out = append(out, &message.ProducerRecord{Topic: r.spec.Consumer.Deadletter(), Key: rec.Key, Value: rec.Value, Headers: h})
	}
	return out
}

// attempts legge il contatore dei tentativi da un record; 0 se assente o illeggibile.
func attempts(headers message.Headers) int {
	n, err := strconv.Atoi(headers.Get(HeaderDeliveryAttempts))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// resolveTopics assegna il DefaultOutputTopic ai record di output privi di Topic, e RIFIUTA quelli
// che restano senza destinazione.
//
// Il rifiuto è il punto. Prima un record senza Topic, in un processor senza `default-output-topic`,
// veniva prodotto verso il topic "": il fallimento arrivava dal client, sul primo batch, con un
// messaggio che non nominava né il processor né la chiave di config da correggere — mentre ogni altro
// difetto dello stesso spec ferma l'avvio.
//
// Non è invece un controllo di avvio: un transformer che instrada da sé ogni record (fan-out
// topic→topic, dove il Topic si calcola dal contenuto) è un uso legittimo e non ha alcun bisogno di un
// default. Pretenderlo al boot vieterebbe il caso d'uso; verificarlo qui coglie esattamente il record
// che non sa dove andare.
func (r *runner) resolveTopics(recs []*message.ProducerRecord) error {
	def := r.spec.DefaultOutputTopic
	for i, rec := range recs {
		if rec.Topic != "" {
			continue
		}
		if def == "" {
			return fmt.Errorf("corekafka: processor %q: il record di output #%d non ha Topic e `default-output-topic` non è configurato: impostarlo sullo spec, oppure assegnare il Topic nel Transformer", r.spec.Name, i)
		}
		rec.Topic = def
	}
	return nil
}
