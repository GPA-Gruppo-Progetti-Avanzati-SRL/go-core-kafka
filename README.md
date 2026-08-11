# go-core-kafka

Libreria Kafka del monorepo `go-core`, costruita su `go-core-app`. Fornisce **kafka-spooler** e
**router EOS** con la business logic definita come in `go-core-batch` (handler minimi + registrazione
via fx value group).

Package root: `corekafka` (`github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka`).

## ⚠️ CGo / librdkafka

Il client è **confluent-kafka-go/v2**, un binding CGo su librdkafka. Il modulo e le app che lo usano
richiedono `CGO_ENABLED=1` e una toolchain C; le immagini Docker devono includere le dipendenze
librdkafka (o usare i binari bundled del pacchetto, default sulle piattaforme supportate).

```bash
CGO_ENABLED=1 go build ./...
CGO_ENABLED=1 go test ./...
```

## Due seam di business logic (la modalità è DERIVATA dalla registrazione, non da config)

> Il tipo del consumer NON si dichiara in config: `RegisterHandler[T]("nome")` → modalità **handle**,
> `RegisterTransformer[T]("nome")` → modalità **transform**. L'engine lo deriva dal gruppo fx in cui è
> registrato il processore (nome in entrambi → errore; in nessuno → errore). Unica fonte di verità.
>
> **L'attivazione è comandata dalla lista `consumers` di config**: un processore registrato ma **non
> presente** in `consumers` non viene istanziato (solo un log info). Un consumer con **`disabled: true`**
> non viene attivato (per spegnerlo senza rimuoverlo dalla config).


- **`handle`** (default) — *at-least-once*. poll → `Handler.Handle(batch) error` → commit degli offset
  **dopo** il ritorno nil. **Business logic LIBERA**: l'handler fa ciò che vuole (Mongo, SQL, chiamate
  esterne…). Con scrittura idempotente (upsert) è effectively-once. La libreria NON fornisce un
  "sinker": è l'`Handler` a fare tutto.
- **`transform`** — *EOS Kafka→Kafka*. poll → `Transformer.Transform(batch) → []*ProducerRecord` →
  produce + commit degli offset consumati nella **stessa transazione** (`SendOffsetsToTransaction`).

Esito **uniforme** tra i due seam (via lo stesso errore gestito): `nil` → commit; `corekafka.DeadLetter(
cause, recs...)` → i record indicati vanno al **DLQ** (in handle via Producer; in transform prodotti nella
stessa transazione EOS) e il resto viene committato; `corekafka.ErrFailFast` → replay; qualsiasi altro
errore → policy `on-error` dello spec (`fail-fast` default | `deadletter`).

## Astrazione client → futuro franz-go

Il client concreto è confinato in `internal/confluentdriver`, dietro le interfacce di
`internal/driver`. L'API pubblica e l'engine non importano mai confluent-kafka-go. Aggiungere un
`internal/franzdriver` e cambiare `driversel.go` fa lo switch **senza toccare le app**.

## Esempio A — handle Kafka→Mongo (modalità handle, da RegisterHandler)

La business logic è libera nell'handler (qui persiste via un data layer `IData`); nessun "sinker" della libreria.

```go
func init() { corekafka.RegisterHandler[eventoHandler]("events") }

type eventoHandler struct {
    core.In
    Data data.IData   // data layer dell'app (scrive su Mongo)
}

func (h *eventoHandler) Handle(ctx context.Context, batch []*corekafka.Record) error {
    eventi, poison := convert(batch)            // business logic dell'app
    if err := h.Data.UpsertEventi(ctx, eventi); err != nil {
        return err                              // transiente → no commit → replay
    }
    if len(poison) > 0 {
        return corekafka.DeadLetter(errParse, poison...) // → DLQ + commit (serve deadletter-topic)
    }
    return nil                                  // → commit
}
```

## Esempio B — consume-transform-produce EOS (modalità transform, da RegisterTransformer), con mix output + deadletter

Il DLQ nel transform si fa con lo **stesso** `corekafka.DeadLetter(...)` dell'handle: si ritornano gli
output "buoni" PIÙ un `DeadLetter` per i record poison. L'engine produce output + DLQ (sul
`deadletter-topic` dello spec) nella **stessa transazione EOS**, poi committa.

```go
func init() { corekafka.RegisterTransformer[routingTransformer]("router") }

type routingTransformer struct{ core.In }
func (t *routingTransformer) Transform(ctx context.Context, batch []*corekafka.Record) ([]*corekafka.ProducerRecord, error) {
    out := make([]*corekafka.ProducerRecord, 0, len(batch))
    var poison []*corekafka.Record
    for _, r := range batch {
        if topic, ok := route(r); ok {
            out = append(out, &corekafka.ProducerRecord{Topic: topic, Key: r.Key, Value: r.Value})
        } else {
            poison = append(poison, r)   // instradato al DLQ dall'engine, nella stessa TX EOS
        }
    }
    if len(poison) > 0 {
        return out, corekafka.DeadLetter(fmt.Errorf("routing non valido"), poison...)
    }
    return out, nil
}
```

## Properties per-consumer + esito deciso dall'handler

Ogni `ConsumerSpec` può portare una mappa `properties` (stringa→stringa) che la business logic legge a
runtime — dal `ctx` (universale, vale sia per `Handler` sia per `Transformer`) o, per
precompute/validazione all'avvio, implementando `corekafka.Configurable`:

```go
// via context (dentro Handle/Transform):
func (h *eventoHandler) Handle(ctx context.Context, batch []*corekafka.Record) error {
    p := corekafka.PropertiesFromContext(ctx)
    coll := p.GetString("collection", "default")   // getter tipizzati: GetString/GetInt/GetBool/GetDuration
    ...
}

// oppure precompute + fail-fast all'avvio:
type condizioneHandler struct{ core.In; /* ... */ }
func (h *condizioneHandler) Configure(p corekafka.Properties) error {
    if !p.Has("collection") { return fmt.Errorf("property 'collection' obbligatoria") } // → l'app non parte
    return nil
}
```

L'handler può inoltre **decidere l'esito caso per caso**, oltre alla policy statica `on-error`:

```go
func (h *condizioneHandler) Handle(ctx context.Context, batch []*corekafka.Record) error {
    ...
    if isPoison(rec)      { return corekafka.DeadLetter(err, rec) } // questi record → DLQ, il resto committa
    if isUnrecoverable()  { return fmt.Errorf("stop: %w", corekafka.ErrFailFast) } // no commit → replay
    return nil                                                       // commit
}
```

Regole d'esito (uniformi handle/transform): `nil` → commit; `corekafka.DeadLetter(...)` → DLQ dei
record indicati + commit (richiede `deadletter-topic`); `corekafka.ErrFailFast` (anche wrappato) →
fail-fast; **qualsiasi altro errore** → policy di default dello spec (`on-error`: `fail-fast` default |
`deadletter`).

## Config YAML (esempio)

```yaml
kafka:
  bootstrap-servers: kafka:9092
  security-protocol: SASL_SSL
  sasl: { mechanisms: SCRAM-SHA-512, username: ${KAFKA_USER}, password: ${KAFKA_PASS} }
consumers:
  # nessun campo "mode": la modalità è derivata da RegisterHandler/RegisterTransformer sul nome
  - name: condizione            # RegisterHandler[...]("condizione") -> handle
    # disabled: true            # → non attiva questo consumer (senza rimuoverlo)
    topics: [businessEvents.condizioni]
    group-id: condizioni-spooler
    max-batch-size: 500
    cut-frequency: 1s
    on-error: deadletter
    deadletter-topic: businessEvents.condizioni.DLQ
    properties:
      collection: condizioni
  - name: router                # RegisterTransformer[...]("router") -> transform (EOS)
    topics: [in.topic]
    group-id: router
    transactional-id: router-tx-1
    default-output-topic: out.topic
    deadletter-topic: routing.DLQ        # dove finiscono i record da DeadLetter (in TX EOS)
```
