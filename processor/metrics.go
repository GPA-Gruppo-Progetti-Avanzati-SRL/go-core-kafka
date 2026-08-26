package processor

import "github.com/prometheus/client_golang/prometheus"

// Esiti della prima passata su un batch (label `outcome` di corekafka_convert_records_total).
const (
	ConvertValid     = "valid"     // conversione riuscita: il record ha prodotto almeno un item
	ConvertTombstone = "tombstone" // record con Value vuoto: la conversione non è stata chiamata
	ConvertSkipped   = "skipped"   // conversione riuscita ma senza nulla da elaborare
	ConvertPoison    = "poison"    // conversione fallita: il record va al DLQ
	ConvertCompacted = "compacted" // record valido superato da uno più recente con la stessa chiave
)

// convertTotal è la visibilità sulla prima passata, che prima non ne aveva nessuna: i record vuoti e
// quelli poison sparivano dentro la business logic dell'app. È complementare a
// corekafka_deadlettered_records_total, che conta l'instradamento EFFETTIVO al DLQ: un processor senza
// deadletter-topic (poison -> fail-fast) tiene quella a zero, mentre questa mostra comunque i poison
// rilevati. `compacted` dice quanto sta tagliando la compaction, cioè quanti duplicati per chiave
// arrivano nello stesso batch.
var convertTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "corekafka_convert_records_total",
	Help: "Numero di record classificati dalla prima passata (Convert), per consumer ed esito.",
}, []string{"consumer", "outcome"})

func init() {
	prometheus.MustRegister(convertTotal)
}
