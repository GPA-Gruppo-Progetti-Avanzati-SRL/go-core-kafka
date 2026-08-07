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
)

func init() {
	prometheus.MustRegister(consumedTotal, processedTotal, producedTotal, deadletteredTotal, batchDuration)
}
