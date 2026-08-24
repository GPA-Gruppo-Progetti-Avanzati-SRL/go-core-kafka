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

## Struttura dell'app consumer (convenzioni GPA)

Un'app consumer è strutturata come un microservizio GPA (vedi skill `gpa-microservice`): stessa
sequenza di boot, stesso data layer, stesse convenzioni di naming. Cambia solo il layer di ingresso:
al posto di `app/api/routes` c'è `app/consumer` (l'ingresso è Kafka, non HTTP).

```
gpa-consumer-app/
├── main.go                     # core.Invoke(...) + core.Run()
├── app-config.go               # composition root: init() con ReadConfig + TUTTO il wiring (core.Module("mongo", ...) + corekafka.Module)
├── config.yml                  # YAML embedded (sezioni services/app)
├── app/
│   ├── config.go               # AppName + app.Config (app-level)
│   ├── consumer/
│   │   ├── register.go         # ← Register(): TUTTE le RegisterHandler/RegisterTransformer + i nomi
│   │   ├── handler/            # ← business logic del consumer "handler" (modalità handle)
│   │   │   ├── consumer.go     #   Handler (IData con `inject:""`) — Handle(ctx, batch)
│   │   │   └── serializer.go   #   convertXxx(): Record Kafka -> model BSON (privato)
│   │   └── transformer/        # ← business logic del consumer "transformer" (modalità transform/EOS)
│   │       ├── routing.go      #   Transformer — Transform(ctx, batch)
│   │       └── serializer.go   #   transformRecord(): payload -> topic di destinazione (privato)
│   └── data/
│       ├── data.go             # IData + core.In Data + init() core.ProvideAs[IData]
│       ├── model/evento.go     # modello BSON
│       └── evento.go           # UpsertEventi: BulkWrite ReplaceOne upsert (go-core-mongo)
└── services/
    └── services.go             # SOLO Config (Kafka + Mongo) — nessun wiring, nessun import di business logic
```

`services` aggrega solo la **Config**, non il wiring: gli import di business logic (es. `app/consumer`
per il riferimento a `Register`) devono stare in `app-config.go`, non in `services` — altrimenti
un'app con più dipendenze rischia di far dipendere l'infra dalla business logic invece del contrario.

Corrispondenze con la skill `gpa-microservice`:

| Microservizio REST | App consumer | Nota |
|---|---|---|
| `app/api/routes/router.go` | `app/consumer/register.go` | punto unico di registrazione; il riferimento `Register` si passa a `Module` da `app-config.go` |
| `app/api/routes/<risorsa>/register.go` | `corekafka.RegisterHandler/RegisterTransformer` | un nome consumer per seam |
| `app/api/business/<risorsa>/` | `app/consumer/<nome>/` | un sub-package per consumer |
| `serializer.go` (convertXxx privati) | `serializer.go` | identico: le conversioni stanno qui, non nell'handler |
| `app/data` (`IData`, `core.In`) | `app/data` | identico: unico layer che parla col DB |

## Registrazione (`app/consumer/register.go`)

`Module` prende come secondo argomento il **riferimento** a una funzione `func()` dell'app (non il suo
valore di ritorno, non chiamata dal chiamante) e la invoca lui stesso, sincronamente, quando già
conosce l'insieme dei consumer attivi da config. `RegisterHandler[T]`/`RegisterTransformer[T]` vanno
chiamate **solo dall'interno** di quella funzione — panicano altrimenti.

La convenzione GPA raccoglie tutte le registrazioni dell'app in una funzione `Register()` centralizzata
(stesso idioma di `routes.NewRouter`/`register.go` dei microservizi REST), che tiene i nomi (devono
combaciare con `consumers[].name` in config) come costanti accanto alla registrazione:

```go
// app/consumer/register.go
package consumer

import (
    corekafka "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka"
    "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/gpa-consumer-app/app/consumer/handler"
    "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/gpa-consumer-app/app/consumer/transformer"
)

const HandlerConsumerName = "handler"         // deve combaciare con consumers[].name in config.yml
const TransformerConsumerName = "transformer"

func Register() {
    corekafka.RegisterHandler[handler.Handler](HandlerConsumerName)
    corekafka.RegisterTransformer[transformer.Transformer](TransformerConsumerName)
}
```

Il riferimento (`consumer.Register`, **senza parentesi**) si passa a `Module` esattamente dove `Module`
viene chiamato — nella **composition root** (`app-config.go`), l'unico punto che deve conoscere sia
l'infra sia la business logic (vedi sezione Wiring più sotto). `services` resta infra-only: non importa
mai `app/consumer`. Nessun registry globale persistente, nessun ordine `init()`/`main()` da rispettare:
`Register` gira sincronamente dentro `Module`, sempre nello stesso punto. `main.go` non chiama più
`Register`.

Per Handler/Transformer con costruzione non banale restano `corekafka.ProvideHandler(constructor)` /
`corekafka.ProvideTransformer(constructor)` (costruttore fx che ritorna la registrazione direttamente;
sempre EAGER, chiamabili anche fuori da `Register`/`Module`).

## Astrazione client → futuro franz-go

Il client concreto è confinato in `internal/confluentdriver`, dietro le interfacce di
`internal/driver`. L'API pubblica e l'engine non importano mai confluent-kafka-go. Aggiungere un
`internal/franzdriver` e cambiare `driversel.go` fa lo switch **senza toccare le app**.

## Esempio A — handle Kafka→Mongo (modalità handle, da RegisterHandler)

La business logic è libera nell'handler (qui persiste via il data layer `IData` dell'app); nessun
"sinker" della libreria. Le conversioni record→modello stanno nel `serializer.go` del package, come
nel business layer dei microservizi REST.

```go
// app/consumer/handler/consumer.go
package handler

type Handler struct {
    Data data.IData `inject:""`   // data layer dell'app (scrive su Mongo)
}

func (h *Handler) Handle(ctx context.Context, batch []*corekafka.Record) error {
    eventi := make([]*model.Evento, 0, len(batch))
    var poison []*corekafka.Record
    for _, r := range batch {
        e, err := convertEvento(r)   // serializer.go — privato al package
        if err != nil {
            poison = append(poison, r)
            continue
        }
        if e == nil {
            continue                 // tombstone/empty
        }
        eventi = append(eventi, e)
    }

    if appErr := h.Data.UpsertEventi(ctx, eventi); appErr != nil {
        return appErr                                    // transiente → no commit → replay
    }
    if len(poison) > 0 {
        return corekafka.DeadLetter(errParse, poison...) // → DLQ + commit (serve deadletter-topic)
    }
    return nil                                           // → commit
}
```

Il data layer resta l'unico a parlare col DB e propaga gli errori come negli altri progetti GPA
(`core.TechnicalErrorWithError`); l'upsert per `_id` rende l'at-least-once effectively-once.

## Esempio B — consume-transform-produce EOS (modalità transform, da RegisterTransformer), con mix output + deadletter

Il DLQ nel transform si fa con lo **stesso** `corekafka.DeadLetter(...)` dell'handle: si ritornano gli
output "buoni" PIÙ un `DeadLetter` per i record poison. L'engine produce output + DLQ (sul
`deadletter-topic` dello spec) nella **stessa transazione EOS**, poi committa.

```go
// app/consumer/transformer/routing.go
package transformer

type Transformer struct{}   // router puro Kafka→Kafka: nessun data layer né properties

func (t *Transformer) Transform(ctx context.Context, batch []*corekafka.Record) ([]*corekafka.ProducerRecord, error) {
    prefix := corekafka.PropertiesFromContext(ctx).GetString("topic-prefix", "gpa.")

    out := make([]*corekafka.ProducerRecord, 0, len(batch))
    var poison []*corekafka.Record
    for _, r := range batch {
        topic, ok := transformRecord(r, prefix)   // serializer.go — privato al package
        if !ok {
            poison = append(poison, r)            // instradato al DLQ dall'engine, nella stessa TX EOS
            continue
        }
        out = append(out, &corekafka.ProducerRecord{Topic: topic, Key: r.Key, Value: r.Value})
    }
    if len(poison) > 0 {
        return out, corekafka.DeadLetter(fmt.Errorf("routing non valido"), poison...)
    }
    return out, nil
}
```

## Wiring — composition root (`app-config.go`)

`Module` richiede il riferimento alla funzione di registrazione dell'app (business logic): per questo
il wiring del sottosistema Kafka NON va dentro `services.ProvideServices` (che deve restare infra-only,
senza importare mai business logic), ma nella **composition root** — l'unico punto dell'app che conosce
sia l'infra sia la business logic, esattamente come `main`/`app-config.go` è l'unico punto che importa
sia `services` sia `app/api/routes` nei microservizi REST.

```go
// services/services.go — SOLO Config, nessun wiring
type Config struct {
    Kafka corekafka.Config `mapstructure:",squash"`   // campi `kafka` + `consumers` sotto `services`
    Mongo coremongo.Config `yaml:"mongo" mapstructure:"mongo"`
}
```

```go
// app-config.go
import (
    corekafka "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka"
    coremongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
    "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/gpa-consumer-app/app/consumer"
    // ...
)

func init() {
    // ... ReadConfig ...

    coremongo.Module(&applicationConfig.ServicesConfig.Mongo)

    corekafka.Module(&applicationConfig.ServicesConfig.Kafka, consumer.Register) // NB: senza parentesi
}
```

`Module` ha firma `Module(cfg *Config, register func(), opts ...Option)`: `register` è il riferimento
alla funzione dell'app, chiamato da `Module` stesso — non un side-effect implicito da qualche `init()`.

Opzioni di `Module`: `WithModes(...)` (gate per `core.Mode`), `WithProducer()` (Producer pubblico
esplicito — è comunque auto-abilitato se almeno uno spec attivo ha `deadletter-topic`),
`WithModule(...)` (componenti extra come `ModuleFunc`, gate-ati sugli stessi modes).

### Solo i consumer attivi entrano nel grafo fx

`Module` calcola l'insieme dei consumer **attivi** (presenti in `consumers[]` e non `disabled`) e poi
chiama `register()`: dentro, `RegisterHandler`/`RegisterTransformer` forniscono a fx SOLO il processor
dei consumer attivi (`processor.Apply`). Le dipendenze di un consumer spento (es. un data layer Mongo)
non entrano quindi nel grafo e **non vengono mai connesse** — altrimenti fx costruirebbe eagerly tutti
i membri del value group, quindi anche i backend dei consumer disattivati. Un nome registrato ma
assente da `consumers[]` produce solo un log info ("costruzione saltata"), nessun errore.

## Properties per-consumer + esito deciso dall'handler

Ogni `ConsumerSpec` può portare un blocco `properties`. Il modo raccomandato per leggerle è
**mapparle sui campi della struct dell'Handler/Transformer** con il tag `prop:`: il mapping avviene al
wiring, quindi default e validazione sono applicati **prima dell'avvio** (config sbagliata → l'app non
parte, nessun client Kafka aperto).

```go
type Handler struct {
    Svc mypkg.IService `inject:""`                           // iniettato da fx

    Collection string        `prop:"collection" validate:"required"`
    BatchLimit int           `prop:"batch-limit" default:"100"`
    Timeout    time.Duration `prop:"timeout" default:"5s"`
    Tags       []string      `prop:"tags"`
}

func (h *Handler) Handle(ctx context.Context, batch []*corekafka.Record) error {
    _ = h.Collection   // tipizzato, con default, già validato al boot
    _ = h.BatchLimit
}
```

```yaml
properties:
  collection: events
  batch-limit: 200
  timeout: 5s
  tags: [a, b]        # liste e mappe annidate: i valori conservano il tipo YAML
```

Regole del mapping:

| Tag | A cosa serve |
|---|---|
| `inject:""` / `inject:"nome"` | dipendenza iniettata da fx (il nome diventa `name:` per dig); `from:"gruppo"` per un value group, `optional:"true"` per una dipendenza facoltativa |
| `prop:"chiave"` | marca il campo come property e ne indica la chiave (`prop:""` = nome del campo in minuscolo) |
| *nessun tag* | campo di lavorazione: né dipendenza né property, resta al valore zero |
| `default:"..."` | valore usato quando la chiave è assente; è una stringa e passa per la stessa conversione dei valori YAML (`"100"`, `"5s"`, `"true"`, `"a,b"`). |
| `validate:"..."` | vincolo [go-playground/validator](https://github.com/go-playground/validator) applicato al singolo campo dopo il decode. Attenzione: `required` su un `int` fallisce anche col valore `0` — di norma si abbina a un `default:`. |

Il meccanismo vive in **go-core-app** (`core.ProvideStruct` + `core.BindProps`) ed è lo stesso usato dai
task di go-core-batch. Il costruttore fornito a fx è **sintetizzato**: dig riceve un param object con le
sole dipendenze `inject:`/`from:`, quindi non vede mai i campi `prop:` e non prova a risolverli — e non
serve `optional:"true"` per nasconderglieli. Per lo stesso motivo `core.In` nella struct del processor
non serve più: se c'è, la struct torna alla semantica storica (ogni campo esportato è una dipendenza),
con un Warn di deprecazione.

Un valore presente ma non convertibile (`batch-limit: "abc"`) è un **errore al boot**, non un fallback
silenzioso al default. Una chiave non reclamata da nessun campo viene ignorata e loggata a Warn (rete di
sicurezza sui typo). I campi property vengono azzerati prima del decode, così un valore che dovesse
arrivare dal grafo fx non può mai spacciarsi per una property.

Per le properties dinamiche/non strutturate restano i getter tipizzati, dal `ctx` (universale, vale sia
per `Handler` sia per `Transformer`) o all'avvio implementando `corekafka.Configurable`:

```go
// via context (dentro Handle/Transform):
func (h *Handler) Handle(ctx context.Context, batch []*corekafka.Record) error {
    p := corekafka.PropertiesFromContext(ctx)
    coll := p.GetString("collection", "default")   // GetString/GetInt/GetBool/GetDuration
    ...
}

// oppure precompute/validazioni incrociate all'avvio (girano dopo il mapping):
func (h *Handler) Configure(p corekafka.Properties) error {
    if !p.Has("collection") { return fmt.Errorf("property 'collection' obbligatoria") } // → l'app non parte
    return nil
}
```

L'handler può inoltre **decidere l'esito caso per caso**, oltre alla policy statica `on-error`:

```go
func (h *Handler) Handle(ctx context.Context, batch []*corekafka.Record) error {
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

I nomi in `consumers[].name` sono la chiave di join con `Register()`: devono combaciare esattamente
con i nomi passati a `RegisterHandler`/`RegisterTransformer`.

```yaml
services:
  # connessione Kafka condivisa (spec.KafkaConfig)
  kafka:
    bootstrap-servers: kafka:9092
    security-protocol: SASL_SSL
    sasl: { mechanisms: SCRAM-SHA-512, username: ${KAFKA_USER}, password: ${KAFKA_PASS} }

  # lista consumer (una entry per spooler)
  consumers:
    # nessun campo "mode": la modalità è derivata da RegisterHandler/RegisterTransformer
    - name: handler               # RegisterHandler[handler.Handler]("handler") -> handle
      # disabled: true            # → non attiva questo consumer (senza rimuoverlo)
      topics: [gpa.events]
      group-id: gpa-consumer-app
      max-batch-size: 500
      cut-frequency: 1s
      auto-offset-reset: earliest
      on-error: fail-fast                 # errore generico -> replay (default)
      deadletter-topic: gpa.events.DLQ    # abilita il DLQ (l'handler ci instrada i payload non validi)
      flush-timeout: 60s
      properties:               # mappate sui campi `prop:` dell'handler (o lette dal ctx)
        collection: events
        batch-limit: 200

    - name: transformer           # RegisterTransformer[transformer.Transformer]("transformer") -> EOS
      topics: [gpa.events.in]
      group-id: gpa-transformer
      transactional-id: gpa-transformer-tx
      default-output-topic: gpa.output    # usato se il transformer lascia Topic vuoto
      deadletter-topic: gpa.routing.DLQ   # dove l'engine produce i record da DeadLetter (in TX EOS)
      max-batch-size: 200
      cut-frequency: 1s
      auto-offset-reset: earliest
      properties:
        topic-prefix: "gpa."
```

Default applicati da `ConsumerSpec.WithDefaults`: `max-batch-size: 500`, `cut-frequency: 1s`,
`auto-offset-reset: earliest`, `on-error: fail-fast`, `flush-timeout: 60s`.

## Metriche

Registrate su Prometheus dall'engine (esposte da `core.NewServerMetrics` su `:2112/metrics`):

| Metrica | Tipo |
|---|---|
| `corekafka_consumed_records_total` | counter |
| `corekafka_processed_records_total` | counter |
| `corekafka_produced_records_total` | counter |
| `corekafka_deadlettered_records_total` | counter |
| `corekafka_batch_duration_seconds` | histogram |

```bash
curl -s localhost:2112/metrics | grep corekafka_
```
