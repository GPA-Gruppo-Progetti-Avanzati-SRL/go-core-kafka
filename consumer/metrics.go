package consumer

import "github.com/prometheus/client_golang/prometheus"

var (
	consumedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "corekafka_consumed_records_total",
		Help: "Numero di record consumati per consumer.",
	}, []string{"consumer"})

	processedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "corekafka_processed_records_total",
		Help: "Numero di record elaborati con successo (sink/transform) per consumer.",
	}, []string{"consumer"})

	producedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "corekafka_produced_records_total",
		Help: "Numero di record prodotti in output (modalità transform) per consumer.",
	}, []string{"consumer"})

	deadletteredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "corekafka_deadlettered_records_total",
		Help: "Numero di record poison instradati al DLQ per consumer.",
	}, []string{"consumer"})

	batchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "corekafka_batch_duration_seconds",
		Help:    "Durata dell'elaborazione di un batch (Handle/Transform + flush/commit) per consumer.",
		Buckets: prometheus.DefBuckets,
	}, []string{"consumer"})

	// restartsTotal cresce quando il loop di un consumer viene ricostruito dopo un errore. Una
	// crescita continua senza record consumati è il segnale che il backoff sta mascherando un guasto
	// stabile che il processo, prima della supervisione, avrebbe reso evidente uscendo.
	restartsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "corekafka_consumer_restarts_total",
		Help: "Numero di riavvii in-process del loop di consumo, per consumer e severità dell'errore.",
	}, []string{"consumer", "severity"})

	// batchDiscardedTotal conta i record scartati senza commit (rebalance, abort transazionale):
	// verranno riletti, quindi è la misura dei duplicati introdotti dagli eventi di protocollo.
	batchDiscardedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "corekafka_batch_discarded_records_total",
		Help: "Numero di record scartati senza commit (batch in volo invalidato), per consumer e motivo.",
	}, []string{"consumer", "reason"})
)

func init() {
	prometheus.MustRegister(consumedTotal, processedTotal, producedTotal, deadletteredTotal,
		batchDuration, restartsTotal, batchDiscardedTotal)
}
