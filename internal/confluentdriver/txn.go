package confluentdriver

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// txn è la disciplina di una transazione Kafka lato producer: init lazy col suo timeout, apertura,
// esito, e il guard che rende l'abort un no-op quando nessuna transazione è aperta.
//
// È condivisa dai DUE transazionali del driver — la sessione EOS (transactSession, che alla chiusura
// aggiunge gli offset consumati) e il producer transazionale del processo (txProducer, che non ha
// offset perché non consuma). Sta in un tipo suo per la ragione di produceAndAwait: la stessa
// disciplina scritta due volte è la stessa correzione da applicare due volte, ed è così che i due
// percorsi transazionali sono già divergiti una volta.
type txn struct {
	p           *kafka.Producer
	initTimeout time.Duration
	reportWait  time.Duration
	inited      bool
	// open dice se c'è una transazione da chiudere. Serve perché abort è chiamato anche su percorsi
	// in cui nessuna transazione è stata aperta (un errore risalito da Poll prima del primo Begin):
	// abortire lì darebbe un errore di stato invalido al posto di un no-op, e su una sessione appena
	// creata un nil deref sul producer.
	open bool
}

// begin apre una transazione, facendo la InitTransactions al primo giro.
//
// La init è legata al context del chiamante E al proprio timeout: partendo da un context.Background()
// un arresto che cadesse durante la InitTransactions non potrebbe interromperla, e terrebbe il
// processo appeso fino a init-transactions-timeout.
func (t *txn) begin(ctx context.Context) error {
	if !t.inited {
		ictx, cancel := context.WithTimeout(ctx, t.initTimeout)
		defer cancel()
		if err := t.p.InitTransactions(ictx); err != nil {
			return wrap("init-transactions", err)
		}
		t.inited = true
	}
	if err := wrap("begin-transaction", t.p.BeginTransaction()); err != nil {
		return err
	}
	t.open = true
	return nil
}

// produce invia i record nella transazione corrente e verifica i delivery report PRIMA del commit,
// così un errore di produzione è rilevato mentre la transazione può ancora essere abortita.
func (t *txn) produce(ctx context.Context, recs []*message.ProducerRecord) error {
	return produceAndAwait(ctx, t.p, recs, t.reportWait)
}

// commit chiude la transazione. Su errore `open` resta true: la transazione è ancora da abortire, e
// azzerarlo qui la lascerebbe aperta fino al transaction.timeout.ms del broker.
func (t *txn) commit(ctx context.Context) error {
	if err := t.p.CommitTransaction(ctx); err != nil {
		return wrap("commit-transaction", err)
	}
	t.open = false
	return nil
}

// abort annulla la transazione corrente; è un no-op se non ce n'è una aperta.
func (t *txn) abort(ctx context.Context) error {
	if !t.open {
		return nil
	}
	t.open = false
	return wrap("abort-transaction", t.p.AbortTransaction(ctx))
}
