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

## Due modalità (per-consumer, campo `mode`)

- **`sink`** (default) — *at-least-once*. poll → `Handler.Handle(batch)` → commit degli offset **dopo**
  il sink. Con sink idempotente (upsert) è effectively-once. Sink pluggable via `WithSink`.
- **`transform`** — *EOS Kafka→Kafka*. poll → `Transformer.Transform(batch) → []*ProducerRecord` →
  produce + commit degli offset consumati nella **stessa transazione** (`SendOffsetsToTransaction`).

Record "poison" (errore business): `on-error: deadletter` (default, produce su `deadletter-topic` poi
committa) | `fail-fast` (non committa, esce → replay).

## Astrazione client → futuro franz-go

Il client concreto è confinato in `internal/confluentdriver`, dietro le interfacce di
`internal/driver`. L'API pubblica e l'engine non importano mai confluent-kafka-go. Aggiungere un
`internal/franzdriver` e cambiare `driversel.go` fa lo switch **senza toccare le app**.

## Esempio A — spooler Kafka→Mongo (mode: sink)

```go
func init() {
    core.ReadConfig(cfgYAML, "KAFKA_CONFIG", &cfg)

    corekafka.Module(&cfg.Kafka,
        corekafka.WithModes("spooler"),
        corekafka.WithSink(mongospooler.Module), // richiede una *mongo.Collection fornita dall'app
    )
    // l'app fornisce la collection di destinazione (da go-core-mongo)
    core.Provide(func(lks *mongolks.LinkedService) *mongo.Collection {
        return lks.GetCollection("condizioni", "")
    })
    // business logic = un Mapper record -> WriteModel + chiave di dedup
    corekafka.RegisterSink[mongo.WriteModel]("condizione", condizioneMapper)
}
```

## Esempio B — consume-transform-produce EOS (mode: transform)

```go
func init() {
    core.ReadConfig(cfgYAML, "KAFKA_CONFIG", &cfg)
    corekafka.Module(&cfg.Kafka, corekafka.WithModes("router"))
    corekafka.RegisterTransformer[routingTransformer]("router")
}

type routingTransformer struct{ core.In }
func (t *routingTransformer) Transform(ctx context.Context, batch []*corekafka.Record) ([]*corekafka.ProducerRecord, error) {
    out := make([]*corekafka.ProducerRecord, 0, len(batch))
    for _, r := range batch {
        out = append(out, &corekafka.ProducerRecord{Topic: "out.topic", Key: r.Key, Value: transform(r.Value)})
    }
    return out, nil
}
```

## Properties per-consumer + esito deciso dall'handler

Ogni `ConsumerSpec` può portare una mappa `properties` (stringa→stringa) che la business logic legge a
runtime — dal `ctx` (universale, vale anche per il `Mapper` di `batchsink`) o, per precompute/validazione
all'avvio, implementando `corekafka.Configurable`:

```go
// via context (Handler, Transformer o Mapper):
func condizioneMapper(ctx context.Context, r *corekafka.Record) (mongo.WriteModel, string, bool, error) {
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

Regole d'esito (modalità sink): `nil` → commit; `corekafka.DeadLetter(...)` → DLQ dei record indicati +
commit (richiede `deadletter-topic`); `corekafka.ErrFailFast` (anche wrappato) → fail-fast; **qualsiasi
altro errore** → policy di default dello spec (`on-error`: `fail-fast` default | `deadletter`).

## Config YAML (esempio)

```yaml
kafka:
  bootstrap-servers: kafka:9092
  security-protocol: SASL_SSL
  sasl: { mechanisms: SCRAM-SHA-512, username: ${KAFKA_USER}, password: ${KAFKA_PASS} }
consumers:
  - name: condizione
    topics: [businessEvents.condizioni]
    group-id: condizioni-spooler
    mode: sink
    max-batch-size: 500
    cut-frequency: 1s
    on-error: deadletter
    deadletter-topic: businessEvents.condizioni.DLQ
    properties:
      collection: condizioni
  - name: router
    topics: [in.topic]
    group-id: router
    mode: transform
    transactional-id: router-tx-1
    default-output-topic: out.topic
```
