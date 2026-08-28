package franzdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// transactSession implementa driver.TransactSession (modalità EOS Kafka→Kafka) sopra la
// GroupTransactSession di franz-go, che è esattamente questo seam: consuma in gruppo, produce e
// chiude la transazione committando gli offset consumati.
//
// Differenza rispetto al driver confluent: qui gli offset NON sono tracciati da noi. È End a
// committare gli offset dei record pollati dall'ultima chiusura (cl.UncommittedOffsets), dentro la
// transazione. Tracciarli in parallelo significherebbe tenere allineate due contabilità della stessa
// cosa, e la seconda non avrebbe modo di correggere la prima.
// txnClient è la sessione transazionale di franz-go vista dal driver: la implementa
// *kgo.GroupTransactSession. È un'interfaccia per la stessa ragione di groupClient — la classificazione
// dell'esito di End (in particolare committed=false senza errore) è logica nostra e va testata.
type txnClient interface {
	Begin() error
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	End(ctx context.Context, commit kgo.TransactionEndTry) (bool, error)
	Close()
}

type transactSession struct {
	session
	sess txnClient
	// txnOpen dice se c'è una transazione da chiudere. Abort/Discard sono chiamate anche su percorsi
	// in cui nessuna transazione è stata aperta (un errore risalito da Poll prima del primo Begin):
	// lì End darebbe un errore di stato invalido al posto di un no-op.
	txnOpen bool
}

func (t *transactSession) Poll(ctx context.Context, timeout time.Duration) (*message.Record, error) {
	r, err := t.pollRaw(ctx, timeout)
	if r == nil || err != nil {
		return nil, err
	}
	return toRecord(r), nil
}

// Begin apre la transazione. Non prende un timeout perché franz-go fa la InitProducerID da sé, con i
// propri retry, al primo Begin: non c'è una init separata da limitare (init-transactions-timeout è
// segnalato come non supportato al boot).
func (t *transactSession) Begin(context.Context) error {
	if err := wrap("begin-transaction", t.sess.Begin()); err != nil {
		return err
	}
	t.txnOpen = true
	return nil
}

// Produce invia i record di output nella transazione corrente e ATTENDE l'esito: ProduceSync ritorna
// quando tutti i delivery report sono arrivati, quindi un errore di produzione è rilevato prima del
// commit e la transazione può essere abortita. Il bound dell'attesa è il RecordDeliveryTimeout del
// client (producer.delivery-timeout, con il default della libreria) più il context del chiamante:
// nessuna attesa senza uscita, come nel driver confluent.
func (t *transactSession) Produce(ctx context.Context, recs []*message.ProducerRecord) error {
	if len(recs) == 0 {
		return nil
	}
	krecs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		krecs = append(krecs, toKgoRecord(r))
	}
	return wrap("produce", t.sess.ProduceSync(ctx, krecs...).FirstErr())
}

// Commit chiude la transazione committando output e offset consumati in modo atomico.
//
// Il caso committed=false con err=nil NON è un successo: End abortisce da sé se il gruppo ha
// rebalanciato da quando la transazione è iniziata (è il motivo per cui esiste GroupTransactSession).
// Ritornarlo come nil direbbe all'engine che il batch è andato a buon fine mentre è stato abortito;
// SeverityReset gli fa scartare il batch e riprendere, che è ciò che è realmente successo.
func (t *transactSession) Commit(ctx context.Context) error {
	t.txnOpen = false
	committed, err := t.sess.End(ctx, kgo.TryCommit)
	if err != nil {
		return endErr("commit-transaction", err)
	}
	if !committed {
		// L'abort riporta indietro il consumo agli ultimi offset committati: i record già fetchati e
		// non ancora consegnati verrebbero riletti, quindi vanno buttati e non consegnati due volte.
		t.dropAndRelease()
		return driver.NewError(driver.SeverityReset, "commit-transaction", errRebalanced)
	}
	t.release()
	return nil
}

// Abort annulla la transazione corrente: gli offset non vengono committati (replay). No-op se nessuna
// transazione è aperta.
func (t *transactSession) Abort(ctx context.Context) error {
	if !t.txnOpen {
		t.dropAndRelease()
		return nil
	}
	t.txnOpen = false
	_, err := t.sess.End(ctx, kgo.TryAbort)
	t.dropAndRelease()
	return endErr("abort-transaction", err)
}

// Discard scarta il batch in volo E abortisce la transazione aperta: in EOS le due cose sono la stessa
// operazione, perché sono gli offset dentro la transazione a dover restare non committati. L'esito
// dell'abort è loggato e non ritornato — vedi il contratto di driver.Session.Discard.
func (t *transactSession) Discard(ctx context.Context) {
	if err := t.Abort(ctx); err != nil {
		log.Warn().Err(err).Str("consumer", t.name).
			Msg("corekafka: abort della transazione fallito durante lo scarto del batch")
	}
}

// Close chiude la sessione, abortendo prima una transazione eventualmente aperta: chiuderla lasciandola
// in volo la tiene aperta fino al transaction.timeout.ms del broker, e nel frattempo i consumatori
// read_committed a valle restano bloccati su quelle partizioni.
//
// La Discard rilascia anche il rebalance (dropAndRelease), che è la precondizione della chiusura con
// BlockRebalanceOnPoll: il LeaveGroup dentro Close attende che i poller scendano a zero — vedi
// groupConsumer.Close per il dettaglio.
func (t *transactSession) Close() error {
	t.Discard(context.Background())
	t.sess.Close()
	return nil
}

// endErr classifica un errore di End. La documentazione di franz-go è esplicita: nessun errore
// ritornato da End è ritentabile — o l'id transazionale è entrato in stato di fallimento, o il client
// ha esaurito i suoi retry. L'unica risposta sensata è ricostruire la sessione (SeverityFatal), tranne
// quando il problema è di configurazione o autorizzazione: lì ricostruire non cambierebbe nulla e il
// processo deve uscire.
func endErr(op string, err error) error {
	if err == nil {
		return nil
	}
	classified := wrap(op, err)
	if driver.SeverityOf(classified) == driver.SeverityPermanent {
		return classified
	}
	return driver.NewError(driver.SeverityFatal, op, err)
}
