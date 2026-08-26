package processor

import (
	"context"
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/rs/zerolog/log"
)

// Convert è la prima passata su un batch: il pezzo che ogni Handler/Transformer riscriveva a mano
// prima di arrivare alla propria business logic — saltare i record vuoti, decodificare il payload,
// separare i record "poison" (errore deterministico) da quelli buoni, opzionalmente compattare le
// versioni superate della stessa chiave, e loggare/contare ciò che è stato scartato.
//
// La sola cosa che resta all'app è la conversione di UN record: `func(*Record) ([]T, error)`.

// Compact / NoCompact rendono leggibile il flag di compaction al sito di chiamata:
//
//	res := corekafka.Convert(ctx, batch, corekafka.Compact, conv)
const (
	Compact   = true
	NoCompact = false
)

// PoisonRecord è un record scartato dalla conversione insieme alla SUA causa. Le cause restano
// per-record (non una sola per il gruppo) perché è la causa del singolo record a finire nell'header
// corekafka-dlq-error di quel record: chi legge il DLQ vede perché è fallito QUEL messaggio.
type PoisonRecord struct {
	Record *message.Record
	Cause  error
}

// Converted è l'esito della prima passata su un batch. Items è già compattato se richiesto; Poison è
// ciò che va instradato al DLQ; i tre contatori dicono cosa è stato scartato e perché (le stesse
// grandezze esposte da corekafka_convert_records_total).
type Converted[T any] struct {
	// Items sono i risultati della conversione, nell'ordine del batch.
	Items []T
	// Poison sono i record su cui la conversione ha fallito, ognuno con la sua causa.
	Poison []PoisonRecord
	// Tombstones conta i record con Value vuoto: la conversione non è stata nemmeno chiamata.
	Tombstones int
	// Skipped conta i record validi la cui conversione non ha prodotto nessun item (nil, nil).
	Skipped int
	// Compacted conta i record scartati perché superati da uno più recente con la stessa chiave.
	Compacted int
}

// DeadLetter è la coda di ogni processor che usa Convert: ritorna nil se non c'è nessun poison,
// altrimenti l'errore gestito da restituire da Handle/Transform (DLQ + commit del resto).
//
//	return res.DeadLetter()
func (c Converted[T]) DeadLetter() error {
	if len(c.Poison) == 0 {
		return nil
	}
	return DeadLetterEach(c.Poison)
}

// DeadLetterEach costruisce il *PoisonRecords conservando la causa di ogni singolo record. È
// l'equivalente per-record di DeadLetter(cause, recs...), che invece etichetta tutto il gruppo con
// un'unica causa. Cause resta valorizzata (con la prima causa, wrappata) perché è ciò che compare nel
// messaggio d'errore e nei log: unire tutte le cause renderebbe illeggibile un batch da 500 record.
func DeadLetterEach(poison []PoisonRecord) *PoisonRecords {
	recs := make([]*message.Record, 0, len(poison))
	causes := make([]error, 0, len(poison))
	for _, p := range poison {
		recs = append(recs, p.Record)
		causes = append(causes, p.Cause)
	}
	var cause error
	if len(causes) > 0 {
		cause = causes[0]
		if len(causes) > 1 && cause != nil {
			cause = fmt.Errorf("%w (+%d altri record poison)", cause, len(causes)-1)
		}
	}
	return &PoisonRecords{Records: recs, Causes: causes, Cause: cause}
}

// Convert applica conv a ogni record del batch e classifica l'esito record per record:
//
//	Value vuoto (tombstone) -> conv NON viene chiamata, Tombstones++
//	conv -> (items, nil)    -> items accodati a Items
//	conv -> (nil, nil)      -> Skipped++ (record valido che non produce nulla)
//	conv -> (_, err)        -> Poison, con err come causa di QUEL record (gli items sono ignorati)
//
// Gli errori della conversione sono per definizione deterministici (è lavoro in memoria su un payload
// fisso): rigiocarli non cambierebbe l'esito, quindi finiscono al DLQ. Gli errori TRANSIENTI — un sink
// irraggiungibile, una chiamata remota fallita — non passano di qui: restano nel corpo di Handle e si
// ritornano come error, che è ciò che impedisce il commit e provoca il replay del batch.
//
// Con compact=Compact sopravvive, fra i record convertiti con successo, solo l'ULTIMO per chiave Kafka
// (string(r.Key)): i suoi items prendono il posto di quelli dei precedenti, nella posizione di prima
// apparizione della chiave. I record con Key vuota non sono mai compattati — senza chiave non c'è
// identità, e collassarli insieme li perderebbe.
//
// La compaction va richiesta solo se la scrittura a valle sovrascrive per intero (upsert/ReplaceOne):
// se invece due record sulla stessa chiave si compongono (es. un update seguito da una cancellazione
// che scrive solo alcuni campi), tenere l'ultimo perde ciò che portava il precedente. Nel dubbio,
// NoCompact.
func Convert[T any](ctx context.Context, batch []*message.Record, compact bool, conv func(*message.Record) ([]T, error)) Converted[T] {
	name := spec.ConsumerNameFromContext(ctx)
	res := Converted[T]{}

	// Un gruppo per record sopravvissuto alla conversione: la compaction sostituisce gli items del
	// gruppo, così l'ordine è quello di PRIMA apparizione della chiave ma il contenuto è dell'ULTIMO
	// record — la stessa semantica del "last write wins" del log compattato di Kafka.
	type group struct{ items []T }
	groups := make([]group, 0, len(batch))
	var byKey map[string]int

	for _, r := range batch {
		if len(r.Value) == 0 {
			res.Tombstones++
			convertTotal.WithLabelValues(name, ConvertTombstone).Inc()
			log.Warn().Str("consumer", name).Str("topic", r.Topic).Int32("partition", r.Partition).
				Int64("offset", r.Offset).Msg("corekafka: record vuoto (tombstone), saltato")
			continue
		}

		items, err := conv(r)
		if err != nil {
			res.Poison = append(res.Poison, PoisonRecord{Record: r, Cause: err})
			convertTotal.WithLabelValues(name, ConvertPoison).Inc()
			log.Warn().Err(err).Str("consumer", name).Str("topic", r.Topic).Int32("partition", r.Partition).
				Int64("offset", r.Offset).Msg("corekafka: record poison, instradato al DLQ")
			continue
		}
		if len(items) == 0 {
			res.Skipped++
			convertTotal.WithLabelValues(name, ConvertSkipped).Inc()
			log.Debug().Str("consumer", name).Str("topic", r.Topic).Int32("partition", r.Partition).
				Int64("offset", r.Offset).Msg("corekafka: record senza nulla da elaborare, saltato")
			continue
		}
		convertTotal.WithLabelValues(name, ConvertValid).Inc()

		if !compact || len(r.Key) == 0 {
			groups = append(groups, group{items: items})
			continue
		}
		if byKey == nil {
			byKey = make(map[string]int, len(batch))
		}
		k := string(r.Key)
		if i, seen := byKey[k]; seen {
			res.Compacted++
			convertTotal.WithLabelValues(name, ConvertCompacted).Inc()
			groups[i].items = items
			continue
		}
		byKey[k] = len(groups)
		groups = append(groups, group{items: items})
	}

	n := 0
	for _, g := range groups {
		n += len(g.items)
	}
	res.Items = make([]T, 0, n)
	for _, g := range groups {
		res.Items = append(res.Items, g.items...)
	}
	return res
}
