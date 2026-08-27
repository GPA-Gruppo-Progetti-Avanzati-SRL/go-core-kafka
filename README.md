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
> **L'attivazione è comandata dalla lista `processors` di config**: un processore registrato ma **non
> presente** in `processors` non viene istanziato (solo un log info). Un processor con **`disabled: true`**
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
├── main.go                     # composition root: core.Boot + TUTTO il wiring (coremongo.Module + corekafka.Module) + core.Run
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
per il riferimento a `Register`) devono stare in `main.go`, non in `services` — altrimenti
un'app con più dipendenze rischia di far dipendere l'infra dalla business logic invece del contrario.

Corrispondenze con la skill `gpa-microservice`:

| Microservizio REST | App consumer | Nota |
|---|---|---|
| `app/api/routes/router.go` | `app/consumer/register.go` | punto unico di registrazione; il riferimento `Register` si passa a `Module` da `main.go` |
| `app/api/routes/<risorsa>/register.go` | `corekafka.RegisterHandler/RegisterTransformer` | un nome consumer per seam |
| `app/api/business/<risorsa>/` | `app/consumer/<nome>/` | un sub-package per processor |
| `serializer.go` (convertXxx privati) | `serializer.go` | identico: le conversioni stanno qui, non nell'handler |
| `app/data` (`IData`, `core.In`) | `app/data` | identico: unico layer che parla col DB |

## Registrazione (`app/consumer/register.go`)

`Module` prende come secondo argomento il **riferimento** a una funzione `func()` dell'app (non il suo
valore di ritorno, non chiamata dal chiamante) e la invoca lui stesso, sincronamente, quando già
conosce l'insieme dei processor attivi da config. `RegisterHandler[T]`/`RegisterTransformer[T]` vanno
chiamate **solo dall'interno** di quella funzione — panicano altrimenti.

La convenzione GPA raccoglie tutte le registrazioni dell'app in una funzione `Register()` centralizzata
(stesso idioma di `routes.NewRouter`/`register.go` dei microservizi REST), che tiene i nomi (devono
combaciare con `processors[].name` in config) come costanti accanto alla registrazione:

```go
// app/consumer/register.go
package consumer

import (
    corekafka "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka"
    "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/gpa-consumer-app/app/consumer/handler"
    "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/gpa-consumer-app/app/consumer/transformer"
)

const HandlerConsumerName = "handler"         // deve combaciare con processors[].name in config.yml
const TransformerConsumerName = "transformer"

func Register() {
    corekafka.RegisterHandler[handler.Handler](HandlerConsumerName)
    corekafka.RegisterTransformer[transformer.Transformer](TransformerConsumerName)
}
```

Il riferimento (`consumer.Register`, **senza parentesi**) si passa a `Module` esattamente dove `Module`
viene chiamato — nella **composition root** (`main.go`), l'unico punto che deve conoscere sia
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
"sinker" della libreria. La conversione record→modello sta nel `serializer.go` del package, come nel
business layer dei microservizi REST — ma il **giro** attorno a quella conversione (record vuoti,
raccolta dei poison, compaction, coda `DeadLetter`) lo fa `corekafka.Convert`.

```go
// app/consumer/handler/consumer.go
package handler

type Handler struct {
    Data data.IData `inject:""`   // data layer dell'app (scrive su Mongo)
}

func (h *Handler) Handle(ctx context.Context, batch []*corekafka.Record) error {
    res := corekafka.Convert(ctx, batch, corekafka.Compact, convertEvento) // serializer.go

    if appErr := h.Data.UpsertEventi(ctx, res.Items); appErr != nil {
        return appErr           // transiente → no commit → replay
    }
    return res.DeadLetter()     // nil se non c'è poison → commit; altrimenti DLQ + commit del resto
}
```

```go
// app/consumer/handler/serializer.go — la conversione di UN record: ciò che resta all'app
func convertEvento(r *corekafka.Record) ([]*model.Evento, error) {
    var payload map[string]any
    if err := bson.UnmarshalExtJSON(r.Value, false, &payload); err != nil {
        return nil, err         // deterministico → poison, con QUESTA causa nell'header del DLQ
    }
    return []*model.Evento{{ID: string(r.Key), Payload: payload}}, nil
}
```

### `Convert` — la prima passata sul batch

`Convert(ctx, batch, compact, conv)` è il pezzo che ogni handler riscriveva a mano prima di arrivare
alla propria business logic. All'app resta la conversione di **un** record:

| La conv ritorna | Esito |
|---|---|
| `(items, nil)` | gli items finiscono in `res.Items` |
| `(nil, nil)` | niente da elaborare: `res.Skipped++` |
| `(_, err)` | `res.Poison`, con `err` come causa **di quel record** |

I record con `Value` vuoto (tombstone) **non arrivano** alla conv: li scarta `Convert`, contandoli in
`res.Tombstones`. Il `[]T` copre il fan-out 1:N — un record CDC che cambia chiave primaria produce sia
la cancellazione del vecchio id sia l'upsert del nuovo.

```go
type Converted[T any] struct {
    Items      []T            // già compattati, nell'ordine del batch
    Poison     []PoisonRecord // {Record, Cause}: la causa viaggia col record fino all'header DLQ
    Tombstones int
    Skipped    int
    Compacted  int
}
func (c Converted[T]) DeadLetter() error   // nil se non c'è nessun poison
```

**Compaction (`corekafka.Compact` / `corekafka.NoCompact`).** Con `Compact` sopravvive, per ogni chiave
Kafka, solo l'**ultimo** record del batch: i suoi items prendono il posto di quelli dei precedenti,
nella posizione di prima apparizione della chiave. I record senza chiave non sono mai compattati —
senza chiave non c'è identità, e collassarli insieme li perderebbe.

È un parametro e non un default perché la sua sicurezza dipende da **come scrive** il consumer: se la
scrittura sovrascrive per intero (upsert / `ReplaceOne`) scartare le versioni superate non perde nulla
ed è ciò che rende lecita una BulkWrite unordered; se invece due record sulla stessa chiave si
**compongono** (un update seguito da una cancellazione che tocca solo alcuni campi), tenere l'ultimo
perde ciò che portava il precedente. Nel dubbio, `NoCompact`.

**Errori deterministici, non transienti.** Quelli della conversione sono per definizione deterministici
(lavoro in memoria su un payload fisso): rigiocarli non cambierebbe l'esito, quindi vanno al DLQ. Gli
errori **transienti** — un sink irraggiungibile, una chiamata remota fallita — non passano da `Convert`:
restano nel corpo di `Handle` e si ritornano come `error`, che è ciò che impedisce il commit e provoca
il replay del batch.

**Le cause arrivano nel DLQ una per record.** `DeadLetter(cause, recs...)` etichetta tutto il gruppo con
un'unica causa; `res.DeadLetter()` (che passa da `DeadLetterEach`) conserva quella del singolo record,
ed è quella che `toDLQ` scrive nell'header `corekafka-dlq-error` di **quel** messaggio.

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

    // Stesso Convert dell'handle, con T = *ProducerRecord. NoCompact: un router non deduplica,
    // ogni record in ingresso deve produrre il suo output.
    res := corekafka.Convert(ctx, batch, corekafka.NoCompact, func(r *corekafka.Record) ([]*corekafka.ProducerRecord, error) {
        return routeRecord(r, prefix)   // serializer.go — privato al package
    })
    return res.Items, res.DeadLetter()  // i poison finiscono al DLQ nella stessa TX EOS
}
```

## Wiring — composition root (`main.go`)

`Module` richiede il riferimento alla funzione di registrazione dell'app (business logic): per questo
il wiring del sottosistema Kafka NON va nel package `services` (che deve restare infra-only, senza
importare mai business logic), ma nella **composition root** — l'unico punto dell'app che conosce sia
l'infra sia la business logic, esattamente come `main.go` è l'unico punto che importa sia `services`
sia `app/api/routes` nei microservizi REST.

```go
// services/config.go — SOLO Config, nessun wiring
type Config struct {
    Kafka corekafka.Config `yaml:"kafka" mapstructure:"kafka"`  // sotto: server / processors
    Mongo coremongo.Config `yaml:"mongo" mapstructure:"mongo"`
}
```

```go
// main.go
import (
    corekafka "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka"
    coremongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
    "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/gpa-consumer-app/app/consumer"
    // ...
)

func main() {
    svc := core.Boot[app.Config, services.Config](core.App{ /* ... */ })

    coremongo.Module(&svc.Mongo)
    corekafka.Module(&svc.Kafka, consumer.Register) // NB: senza parentesi

    core.Run(core.WithTracing())
}
```

`Module` ha firma `Module(cfg *Config, register func(), opts ...Option)`: `register` è il riferimento
alla funzione dell'app, chiamato da `Module` stesso — non un side-effect implicito da qualche `init()`.

Opzioni di `Module`: `WithModes(...)` (gate per `core.Mode`), `WithProducer()` (forza la costruzione
del Producer interno anche se nessuno spec attivo ha `deadletter-topic`, caso in cui è già
auto-abilitato per il DLQ), `WithModule(...)` (componenti extra come `ModuleFunc`, gate-ati sugli
stessi modes).

Le registrazioni stanno in un `core.ModuleClosed("kafka")`: Kafka è un **sottosistema chiuso** —
consuma i seam dell'app (`Handler`/`Transformer`) e non le espone nulla in cambio, quindi
`spec.KafkaServer`, `[]spec.ConsumerSpec`, `driver.Factory`, `*producer.Producer` e
`*consumer.Consumers` sono privati al modulo e non iniettabili dal grafo dell'app. Gli Handler
restano forniti a root: il value group li porta dentro il modulo, e le loro dipendenze applicative
sono risolte a root come sempre. Un consumer che deve produrre lo fa con un `Transformer` (EOS
Kafka→Kafka), che è il seam previsto.

### Solo i processor attivi entrano nel grafo fx

`Module` calcola l'insieme dei processor **attivi** (presenti in `processors[]` e non `disabled`) e poi
chiama `register()`: dentro, `RegisterHandler`/`RegisterTransformer` forniscono a fx SOLO il processor
dei processor attivi (`processor.Apply`). Le dipendenze di un processor spento (es. un data layer Mongo)
non entrano quindi nel grafo e **non vengono mai connesse** — altrimenti fx costruirebbe eagerly tutti
i membri del value group, quindi anche i backend dei consumer disattivati. Un nome registrato ma
assente da `processors[]` produce solo un log info ("costruzione saltata"), nessun errore.

## Properties per-processor + esito deciso dall'handler

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
**non va usato**: è un errore al wiring, perché il marker lo porta il param object sintetico e
accettarlo lascerebbe passare struct scritte per la vecchia semantica, con le dipendenze non taggate
silenziosamente a nil. Resta valido nei param object dei costruttori scritti a mano.

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

## Config YAML

I nomi in `processors[].name` sono la chiave di join con `Register()`: devono combaciare esattamente
con i nomi passati a `RegisterHandler`/`RegisterTransformer`.

La `corekafka.Config` ha **due sole sezioni**, e la forma è **simmetrica sui due livelli**:

- **`server`** — come parliamo con Kafka: la connessione, più tre blocchi globali `restart`,
  `consumer` e `producer`.
- **`processors`** — una voce per processor, con la sola **identità**, le properties applicative e i
  **blocchi omonimi** `consumer`/`producer`/`restart` che sovrascrivono i globali campo per campo.

```yaml
services:
  kafka:
    server:
      # --- connessione, condivisa da tutti i client del processo ---
      bootstrap-servers: kafka:9092
      client-id: gpa-consumer-app          # client.id: rende identificabile chi è connesso lato broker
      security-protocol: SASL_SSL
      sasl: { mechanisms: SCRAM-SHA-512, username: ${KAFKA_USER}, password: ${KAFKA_PASS} }
      # ssl:
      #   ca-location: /etc/ssl/kafka-ca.pem
      #   certificate-location: /etc/ssl/client.pem   # mTLS: il broker autentica anche il client
      #   key-location: /etc/ssl/client.key
      # socket-keepalive-enable: true       # vedi il preset Azure Event Hub più sotto
      # metadata-max-age-ms: 180000
      # debug: cgrp,fetch                   # contesti di debug librdkafka (verboso: per la diagnosi)

      # Supervisione del loop di consumo. Sta qui e NON dentro `consumer` perché non è tuning di un
      # client: non finisce in nessuna ConfigMap e non descrive come si consuma, ma cosa fa il
      # processo quando un loop muore. Vedi "Errori e restart".
      restart:
        max-attempts: 5                     # default; -1 = illimitati (con cautela), 0 = RIFIUTATO
        initial-backoff: 1s
        max-backoff: 30s

      # Default del client consumer: ogni processor li eredita.
      consumer:
        auto-offset-reset: earliest         # earliest | latest | none
        max-batch-size: 500
        cut-frequency: 1s
        session-timeout-ms: 10000
        heartbeat-interval-ms: 3000
        on-error: fail-fast                 # errore generico -> replay (default) | deadletter
        deadletter-topic: gpa.DLQ           # anche il DLQ è ereditabile: i record ci arrivano etichettati

      # Default del client producer.
      producer:
        acks: all                           # 0 | 1 | -1 | all
        compression-type: snappy            # none | gzip | snappy | lz4 | zstd
        linger-ms: 20                       # 0 esplicito = invia subito (è un *int, vedi sotto)
        flush-timeout: 60s                  # tempo concesso alla chiusura per svuotare la coda
        # transaction-timeout-ms: 100000     # oltre questa durata il broker fa fencing del producer
        # init-transactions-timeout: 60s

    processors:
      # nessun campo "mode": la modalità è derivata da RegisterHandler/RegisterTransformer
      - name: handler               # RegisterHandler[handler.Handler]("handler") -> handle
        # disabled: true            # → non attiva questo processor (senza rimuoverlo)
        topics: [gpa.events]
        group-id: gpa-consumer-app
        consumer:
          deadletter-topic: gpa.events.DLQ  # override del DLQ comune; il resto del tuning è ereditato
        properties:               # mappate sui campi `prop:` dell'handler (o lette dal ctx)
          collection: events
          batch-limit: 200

      - name: supporto              # un processor che deve reggere un'infra esterna intermittente
        topics: [cdc.condizioni]
        group-id: condizioni-supporto
        consumer:
          session-timeout-ms: 30000         # override del globale (10000)
        restart:
          initial-backoff: 2s               # override di UN campo: gli altri restano dal globale
          on-business-error: true
        properties:
          collection: servizi_bancari

      - name: transformer           # RegisterTransformer[transformer.Transformer]("transformer") -> EOS
        topics: [gpa.events.in]
        group-id: gpa-transformer
        transactional-id: gpa-transformer-tx
        default-output-topic: gpa.output    # usato se il transformer lascia Topic vuoto
        consumer:
          max-batch-size: 200               # le transazioni EOS restano corte
          deadletter-topic: gpa.routing.DLQ # dove l'engine produce i record da DeadLetter (in TX EOS)
        producer:
          linger-ms: 0                      # override del producer TRANSAZIONALE di questo processor
        properties:
          topic-prefix: "gpa."
```

### Eredità e override

**La precedenza è: valore nel blocco del processor → valore nel blocco omonimo di `server` → default
della libreria.** Un campo lasciato al suo zero significa "eredita"; un campo scritto sul processor
sovrascrive. I blocchi ai due livelli sono **gli stessi tipi Go**, quindi hanno per costruzione le
stesse chiavi: non c'è una lista di "campi sovrascrivibili" da tenere allineata a mano.

| Blocco | Cosa contiene | Ereditabile |
|---|---|---|
| `consumer` | tuning del client consumer (`auto-offset-reset`, `session-timeout-ms`, `heartbeat-interval-ms`, `fetch-*`, `max-partition-fetch-bytes`, `queued-max-messages-kbytes`, `max-poll-interval-ms`, `partition-assignment-strategy`, `isolation-level`), dell'engine (`max-batch-size`, `cut-frequency`, `poll-timeout`), policy (`on-error`, `deadletter-topic`) e `kafka-properties` | sì, campo per campo |
| `producer` | tuning del client producer (`acks`, `compression-type`, `linger-ms`, `batch-*`, `max-retries`, `max-in-flight`, `retry-backoff`, `delivery-timeout`, `request-timeout-ms`, `flush-timeout`, `enable-idempotence`), transazioni (`transaction-timeout-ms`, `init-transactions-timeout`) e `kafka-properties` | sì, campo per campo |
| `restart` | `disabled`, `max-attempts`, `initial-backoff`, `max-backoff`, `multiplier`, `reset-after`, `on-business-error` | sì, campo per campo |
| _identità_ | `name`, `topics`, `group-id`, `transactional-id`, `default-output-topic`, `properties` | **no**: è ciò che distingue un processor dall'altro |

**`processors[].producer` ha effetto solo in modalità transform.** Il producer transazionale è
l'unico che appartiene a un processor; in modalità handle il producer è quello **condiviso** del
processo (serve al DLQ) e un override non avrebbe destinatario — l'engine lo segnala con un warning
al boot. La modalità è derivata dalla registrazione, quindi chi scrive il YAML non ha modo di
accorgersene guardando solo la config: per questo è un warning e non un errore.

I blocchi si ereditano **campo per campo**: sovrascriverne uno non azzera gli altri. `restart.disabled`,
`restart.on-business-error`, `producer.enable-idempotence` e `producer.linger-ms` sono puntatori
proprio per questo — con un valore semplice, un `true` (o un `20`) globale non sarebbe più
sovrascrivibile con `false` (o `0`) da un singolo processor.

**Convenzione dei knob di tuning: un campo non valorizzato NON viene scritto nella ConfigMap**, così
resta il default di librdkafka. Scrivere lo zero al posto di omettere la chiave imporrebbe `0` a
proprietà dove zero ha un significato del tutto diverso dal default.

Default della libreria (l'ultimo anello della catena) — sono quelli che governano **l'engine**, non il
client: `consumer.max-batch-size: 500`, `consumer.cut-frequency: 1s`, `consumer.poll-timeout: 100ms`,
`consumer.auto-offset-reset: earliest`, `consumer.on-error: fail-fast`,
`producer.flush-timeout: 60s`, `producer.init-transactions-timeout: 60s`, e i default di `restart`
(`initial-backoff: 1s`, `max-backoff: 30s`, `multiplier: 2`, `reset-after: 2m`).

I valori enumerati sono validati al boot (`validate:"oneof=..."` su `on-error`, `auto-offset-reset`,
`isolation-level`, `partition-assignment-strategy`, `security-protocol`, `sasl.mechanisms`,
`producer.acks`, `producer.compression-type`) — ai **due** livelli, perché sono gli stessi tipi:
**un typo ferma l'avvio** invece di degradare in silenzio.

> Le chiavi di tuning vanno **dentro** i blocchi `consumer`/`producer`, non direttamente sulla voce
> del processor: una chiave piatta non viene mappata e resta senza effetto.

### Escape hatch: `kafka-properties`

Le proprietà librdkafka non coperte da un campo tipizzato si passano con le loro chiavi dotted. Sono
applicate **per ultime** — quindi vincono sui campi tipizzati, con un warning quando lo fanno — e
seguono la stessa eredità del blocco che le contiene, **fondendosi** chiave per chiave.

```yaml
server:
  kafka-properties:                         # comune a consumer e producer
    ssl.endpoint.identification.algorithm: none
  consumer:
    kafka-properties:
      fetch.error.backoff.ms: "1000"        # comune a tutti i processor
  producer:
    kafka-properties:
      queue.buffering.max.kbytes: "1048576"
processors:
  - name: handler
    consumer:
      kafka-properties:
        debug: "cgrp"                       # si FONDE con quelle di server.consumer
```

Da non confondere con `properties:`, che sono le proprietà **applicative** del processor (i campi
`prop:` di Handler/Transformer). Cinque chiavi sono **rifiutate al boot** perché non sono default
sovrascrivibili ma invarianti dell'engine: `bootstrap.servers`, `group.id`, `transactional.id`,
`enable.auto.commit` (il commit è manuale, sempre) e `isolation.level` (ha un campo tipizzato, e in
transform vale `read_committed`).

### Preset Azure Event Hub

Il gateway Kafka di Event Hub chiude le connessioni idle in modo aggressivo; questi tre valori
evitano i timeout spuri (preset ereditato da `tpm-kafka-common`):

```yaml
server:
  socket-keepalive-enable: true
  metadata-max-age-ms: 180000
  producer:
    request-timeout-ms: 60000
```

## Errori e restart

L'engine distingue **errori del client Kafka** — classificati dal driver, che è l'unico a vedere il
tipo concreto — da **errori della business logic**. Alla prima categoria è assegnata una *severità*,
che non è una scala di gravità ma un verbo: dice cosa deve fare l'engine.

| Severità | Origine tipica | Azione dell'engine |
|---|---|---|
| `permanent` | credenziali errate, SASL non supportato, config rifiutata | **esce**: nessun retry può aiutare, va corretta la config |
| `fatal` | fencing EOS, epoch invalido, errore `IsFatal()` | **ricostruisce** consumer/sessione dopo il backoff |
| `retriable` | transport, tutti i broker giù, leader non disponibile | **ricostruisce** consumer/sessione dopo il backoff |
| `abort` | il broker chiede l'abort della transazione (`TxnRequiresAbort`) | abortisce, **scarta il batch**, prosegue senza ricostruire |
| `reset` | rebalance in corso, partizioni revocate, generation superata | **scarta il batch**, prosegue senza ricostruire |
| `business` | errore risalito da `Handle`/`Transform` sotto `fail-fast` | **esce** (salvo `restart.on-business-error`) |

Il restart in-process esiste perché il client non sopravvive a tutto: un fencing EOS o un broker che
cade richiedono di **ricostruire** consumer e producer, non solo di riprovare la chiamata. Senza
supervisione l'unico recovery è la morte del processo, che su un rolling restart dei broker diventa un
CrashLoopBackOff. È per questo che `restart` sta in `server` e non in `consumer`: non è tuning di un
client, è una decisione di esercizio del processo.

```yaml
server:
  restart:
    disabled: false          # true = comportamento storico: qualunque errore fa uscire il processo
    max-attempts: 5          # default FINITO; -1 = illimitati (vedi sotto); 0 = errore di avvio
    initial-backoff: 1s
    max-backoff: 30s
    multiplier: 2            # >= 1 (1 = backoff costante)
    reset-after: 2m          # un run sano più lungo di così azzera il contatore dei tentativi
                             # deve essere > max-backoff, altrimenti il budget si ricarica sempre
    on-business-error: false # vedi sotto
processors:
  - name: supporto
    restart:
      initial-backoff: 2s    # override di UN campo: max-attempts, max-backoff… restano dal globale
      on-business-error: true
```

`on-business-error` resta **false** per default perché `on-error: fail-fast` documenta "non committa
ed esce": riprovare in-process un record poison sarebbe un loop senza uscita. Va messo a `true` quando
la causa attesa è un'infrastruttura applicativa transitoria (il DB irraggiungibile), non un payload
malformato.

#### Il budget dei tentativi è finito, e l'illimitato è esplicito

`max-attempts` assente vale **5** (≈31s di insistenza con i backoff di default: 1+2+4+8+16). Esaurito
il budget l'errore risale, il processo esce e il recovery passa all'orchestratore — che è il livello
che sa se ha senso continuare a insistere.

Prima il default era `0` = *illimitati*, cioè: l'opzione più pericolosa era quella che si otteneva
**non scrivendo nulla**, perché 0 è lo zero value del campo. Un riavvio senza limite maschera
indefinitamente un guasto stabile, che è esattamente il loop infinito che questo knob esiste per
evitare. Da qui tre conseguenze:

- **`max-attempts: -1`** = illimitati, scelta esplicita. All'avvio compare un `Warn`: il segnale da
  guardare è `corekafka_consumer_restarts_total`, e una sua crescita continua senza record consumati è
  il guasto stabile che la supervisione sta coprendo.
- **`max-attempts: 0`** = **errore di avvio**, con un messaggio che indica `-1` (illimitati) o
  `restart.disabled: true` (nessun riavvio). Chi l'aveva scritto intendeva "illimitati": meglio
  fermarlo che dargli in silenzio l'opposto.
- **`reset-after` deve essere maggiore di `max-backoff`**, altrimenti l'avvio fallisce. È l'altra
  strada verso lo stesso loop: un run che dura almeno `reset-after` azzera il contatore, quindi con
  `reset-after: 1s` un consumer che muore dopo due secondi ricarica il budget ogni volta e
  `max-attempts` finito diventa illimitato di fatto. Un run più breve di una singola attesa di backoff
  non può essere prova di salute.

Con `restart.disabled: true` nessuno di questi controlli si applica: la politica non ha effetto, e far
cadere un'app per un knob inerte sarebbe severità senza scopo.

### Offset e rebalance

Il driver registra un rebalance callback sulla sottoscrizione, per una ragione di **correttezza**: alla
revoca di una partizione gli offset già pollati ma non ancora elaborati vengono scartati e il batch in
volo invalidato (`reset`). Confermarli significherebbe dichiarare elaborati record che nessuno ha
elaborato, mentre il nuovo owner li sta rileggendo — cioè **perdere messaggi**. Scartandoli si ottiene
un replay dal nuovo owner: duplicati, mai buchi. In EOS il batch viene abortito prima del commit.

Il callback non chiama `Assign`/`Unassign`: se non riassegna, il client Kafka lo fa da sé scegliendo il
protocollo giusto (`incremental_assign` quando il gruppo è `cooperative-sticky`, `assign` altrimenti).

### Header dei record in DLQ

Oltre al payload e agli header originali (la correlazione di trace sopravvive), i record instradati al
DLQ portano:

| Header | Contenuto |
|---|---|
| `corekafka-dlq-source-topic` / `-source-partition` / `-source-offset` | coordinate esatte del record originale |
| `corekafka-dlq-source-timestamp` | timestamp del record (RFC3339Nano) |
| `corekafka-dlq-processor` | nome del processor che lo ha scartato |
| `corekafka-dlq-error` / `-error-at` | causa e istante dello scarto |
| `Kafka-Delivery-Attempts` | contatore incrementale (nome ereditato da `tpm-kafka-common`): permette a chi riprocessa il DLQ di fermarsi |

## Metriche

Registrate su Prometheus dall'engine (esposte da `core.NewServerMetrics` su `:2112/metrics`):

| Metrica | Tipo | Label |
|---|---|---|
| `corekafka_consumed_records_total` | counter | `consumer` |
| `corekafka_processed_records_total` | counter | `consumer` |
| `corekafka_produced_records_total` | counter | `consumer` |
| `corekafka_deadlettered_records_total` | counter | `consumer` |
| `corekafka_batch_duration_seconds` | histogram | `consumer` |
| `corekafka_consumer_restarts_total` | counter | `consumer`, `severity` |
| `corekafka_batch_discarded_records_total` | counter | `consumer`, `reason` |
| `corekafka_convert_records_total` | counter | `consumer`, `outcome` |

`corekafka_consumer_restarts_total` che cresce senza che cresca `consumed_records_total` è il segnale
che il backoff sta mascherando un guasto stabile — quello che, prima della supervisione, il processo
rendeva evidente uscendo. `batch_discarded_records_total` misura i duplicati introdotti dagli eventi di
protocollo (rebalance, abort).

`corekafka_convert_records_total` è la visibilità sulla prima passata, che prima non ne aveva nessuna:
`outcome` vale `valid`, `tombstone`, `skipped`, `poison` o `compacted`. È complementare a
`deadlettered_records_total`, che conta l'instradamento **effettivo** al DLQ: un processor senza
`deadletter-topic` (poison → fail-fast) tiene quella a zero, mentre `outcome="poison"` mostra comunque
quanti record sono stati rilevati. `outcome="compacted"` dice quanti duplicati per chiave arrivano
nello stesso batch.

```bash
curl -s localhost:2112/metrics | grep corekafka_
```
