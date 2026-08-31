# Codici di errore — go-core-kafka

`corekafka` ha **un solo `ApplicationError`**: tutto il resto della libreria ritorna `error`,
perché l'engine non risponde a un client HTTP ma decide se **committare, replayare, mandare al
DLQ, ricostruire il consumer o uscire**. Codificare quegli errori non servirebbe a nessuno: la
decisione la porta la `driver.Severity`, non un codice.

| Codice | HTTP | Costante | Origine | Significato |
|---|---|---|---|---|
| `KAFKA-PRODUCE` | 500 | `producer.CodeProduce` | `producer/producer.go` | `Produce` fallita: l'invio, l'attesa dei delivery report o — sul producer transazionale — il `Begin`/`Commit` della transazione (in quel caso la transazione è già stata abortita). `Ambit = go-core-kafka` (`producer.Ambit`) |

> **Da quale libreria viene l'errore.** Gli errori non-`ApplicationError` lo dicono nel
> messaggio, con un prefisso: `corekafka:` per engine, config e supervisione;
> `confluentdriver:` / `franzdriver:` per il client. Prima gli errori dell'engine iniziavano
> direttamente con `processor %q: …` e, arrivati a fx o a un log applicativo, non nominavano
> nessuna libreria.

## 1. Esito del batch → azione dell'engine

Ciò che l'`Handler`/`Transformer` ritorna è classificato da un'unica funzione `classify`
(condivisa da handle e transform):

| Ritorno | Azione |
|---|---|
| `nil` | **commit** degli offset |
| `corekafka.DeadLetter(cause, recs...)` | i record vanno sul **deadletter-topic** e gli offset sono committati. La `cause` etichetta l'intero gruppo |
| `corekafka.PoisonRecords` (da `Converted.DeadLetter()`) | come sopra, ma **la causa è per record**: quella del singolo record finisce nel *suo* header `corekafka-dlq-error` |
| `corekafka.ErrFailFast` (`processor/processor.go:78`) | **nessun commit**: il batch viene replayato. È la richiesta esplicita di replay |
| qualsiasi altro errore | decide la policy `consumer.on-error` dello spec: `fail-fast` (default, il processo esce) oppure `deadletter` |

**Regola per chi scrive un Handler:** nella `conv` di `corekafka.Convert` vanno solo gli errori
**deterministici** (payload malformato) → diventano poison → DLQ. Gli errori **transienti**
(sink irraggiungibile, chiamata remota fallita) vanno ritornati come `error` da `Handle`: è ciò
che impedisce il commit e provoca il replay. Invertirli significa buttare nel DLQ messaggi
validi, o riprocessare all'infinito messaggi irrecuperabili.

## 2. Header del deadletter

Sono l'unica "codifica" dell'errore che sopravvive al processo (`consumer/consumer.go:769`):

| Header | Contenuto |
|---|---|
| `corekafka-dlq-source-topic` | topic di origine |
| `corekafka-dlq-source-partition` | partizione di origine |
| `corekafka-dlq-source-offset` | offset di origine |
| `corekafka-dlq-source-timestamp` | timestamp del record originale |
| `corekafka-dlq-processor` | nome del processor che lo ha scartato |
| `corekafka-dlq-error` | **la causa**, per record |
| `corekafka-dlq-error-at` | istante dello scarto |

## 3. Severity: la tassonomia degli errori del client

`driver.Severity` (`internal/driver/errors.go`) **non è una scala di gravità, è un verbo**: dice
all'engine cosa fare. È anche la label della metrica `corekafka_consumer_restarts_total{consumer,severity}`.

| Severity | Esempi | Azione dell'engine |
|---|---|---|
| `permanent` | credenziali errate, SASL non supportato, config rifiutata | **esce**: nessun retry aiuta, va corretta la config |
| `fatal` | fencing EOS, epoch invalido, errore marcato fatal da librdkafka | **ricostruisce** consumer/sessione dopo backoff (un client nuovo può funzionare) |
| `retriable` | transport down, tutti i broker giù, leader non disponibile | **ricostruisce** dopo backoff |
| `abort` | transazione EOS da abortire, sessione ancora valida | abortisce, **scarta il batch** e continua senza ricostruire |
| `reset` | rebalance in corso, partizioni revocate, generation superata | **scarta il batch senza commit** e continua: committarlo dichiarerebbe elaborati record che il nuovo owner sta rileggendo (= perdita) |
| `business` | errore risalito da `Handle`/`Transform` | esce (semantica di `fail-fast`), salvo `restart.on-business-error`. È lo **zero value**: un errore non prodotto dal driver ricade qui |

`driver.Error` porta anche `Op` ("poll", "commit", "produce", "begin", …), così un errore non
richiede di risalire lo stack per capire dove è nato. La classificazione di `kafka.Error` è
confinata in `internal/confluentdriver/errors.go`.

Metriche correlate: `corekafka_consumer_restarts_total{consumer,severity}`,
`corekafka_batch_discarded_records_total{consumer,reason}`,
`corekafka_convert_records_total{consumer,outcome}`.

## 4. Errori di riavvio

| Messaggio | Origine | Causa |
|---|---|---|
| `corekafka: processor %q: esauriti i %d tentativi di riavvio: %w` | `consumer/consumer.go:345` | budget `restart.max-attempts` esaurito (default **5**, ~31s di insistenza). L'errore risale, il processo esce e il recovery passa all'orchestratore |

## 5. Errori di avvio (l'app non parte)

### Wiring

| Messaggio | Origine |
|---|---|
| `corekafka.Module: WithDriver è obbligatoria` | `module.go:79` — con i due import (`driver/confluent`, `driver/franz`) nel messaggio |
| `corekafka: RegisterHandler/RegisterTransformer chiamata fuori dalla funzione passata a Module` | `processor/processor.go:195` |

### Coerenza del processor (`consumer/consumer.go`, messaggi prefissati `corekafka: processor %q:`)

| Messaggio | Riga | Causa |
|---|---|---|
| `registrato sia come Handler sia come Transformer (ambiguo)` | 242 | la modalità è derivata dalla registrazione: due registrazioni = nessuna modalità deducibile |
| `nessun processor registrato` | 295 | voce in `processors:` senza `RegisterHandler`/`RegisterTransformer` corrispondente |
| `consumer.on-error=deadletter richiede consumer.deadletter-topic` | 258 | policy senza destinazione |
| `consumer.deadletter-topic impostato richiede il Producer` | 264 | manca `corekafka.WithProducer` |
| `(transform): transactional-id obbligatorio` | 281 | EOS senza identità transazionale (regime di default: `delivery` assente o `exactly-once`) |
| `(transform, delivery=at-least-once): transactional-id non ammesso` | 273 | l'id non ha destinatario, non c'è nessuna transazione. Errore e non avviso: chi l'ha scritto crede di avere l'EOS |
| `(transform): producer.transaction-timeout-ms <= consumer.cut-frequency` | 291 | la transazione scadrebbe prima della chiusura del batch: fencing a ogni giro. Non si applica in `at-least-once` |
| `Configure: %w` | 308 | la `Configurable` del processor ha rifiutato le properties |

### Configurazione (`spec/`)

| Messaggio | Origine | Causa |
|---|---|---|
| `corekafka: … restart.max-attempts=0 non è ammesso` | `spec/spec.go:546` | `0` è lo zero value: chi l'ha scritto intendeva "illimitati". Il messaggio nomina `-1` (illimitati, esplicito) e `restart.disabled` |
| `corekafka: … restart.reset-after <= restart.max-backoff` | `spec/spec.go:559` | un run più breve di una singola attesa azzererebbe il contatore, rendendo inefficace `max-attempts` |
| `kafka-properties contiene chiavi riservate` | `spec/kafkaprops.go:44` | invarianti dell'engine: `bootstrap.servers`, `group.id`, `transactional.id`, `enable.auto.commit`, `isolation.level` |
| valore fuori da `validate:"oneof=..."` | `spec/validate.go` | typo su `on-error`, `auto-offset-reset`, `acks`, `compression-type`, `delivery`, … Prima degradava in silenzio |

### Driver franz (`internal/franzdriver/kafkaprops.go`)

| Messaggio | Riga | Causa |
|---|---|---|
| `kafka-properties %s non traducibili nel driver franz-go` | 118 | chiave librdkafka senza opzione equivalente: **ferma l'avvio**, perché una property senza destinatario è indistinguibile da una mai scritta |
| `valore %q non valido (...)` | 56, 77, 138, 157, 167 | valore non parsabile per la chiave (ms, bool, byte, intero) |
| `mTLS incompleto` / `nessun certificato valido in ssl.ca-location` | `options.go:181,187` | TLS malconfigurato |

I **knob tipizzati** senza equivalente franz (`debug`, `socket-keepalive-enable`,
`queued-max-messages-kbytes`, `message-max-bytes`, `batch-num-messages`,
`metadata-max-idle-ms`, `init-transactions-timeout`) sono invece **ignorati con un Warn** che
li elenca: sono vocabolario della libreria, e un limite del driver si documenta.

## 6. Errori di produzione

| Messaggio | Origine | Severity |
|---|---|---|
| `delivery report non ricevuti entro %s: %d/%d` | `internal/confluentdriver/produce.go:75` | `retriable` — i record non sono né confermati né perduti: si replaya. Il bound viene da `producer.delivery-timeout` (default libreria 2m) |
| `attesa dei delivery report interrotta con %d/%d ricevuti` | `produce.go:72` | `retriable` — context cancellato |
| `flush incompleto alla chiusura: %d record ancora in coda` | `internal/confluentdriver/producer.go:31` | in shutdown |
| `il record di output #%d non ha Topic e default-output-topic non è configurato` | `consumer/consumer.go:832` | errore del Transformer |
| `DeadLetter richiesto ma deadletter-topic assente` | `consumer/consumer.go:754` | l'handler ha chiesto il DLQ su un processor che non ne ha uno |
