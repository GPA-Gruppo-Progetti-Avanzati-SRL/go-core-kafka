package franzdriver

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

// Questo file dà al client franz-go le due cose che un client Kafka in esercizio deve avere: una voce
// nei log dell'applicazione e delle metriche. È modellato su quanto già fatto per il producer
// franz-go di go-core-batch (internal/kafkaproducer/options.go); le due copie convergeranno quando
// quel producer passerà da questo driver.

// kgoLogger adatta il logging interno di franz-go a zerolog. Senza, gli errori di
// dial/TLS/SASL/transazione restano dentro il client e riemergono da noi come un generico "context
// deadline exceeded": è la classe di problemi per cui esiste la tassonomia Severity, e senza il log
// del client non si arriva alla causa.
//
// owner è il proprietario del client (il nome del processor, o `server.producer` per il producer
// condiviso): con più consumer nello stesso processo, una riga che non dice di chi è non è
// diagnosticabile.
type kgoLogger struct{ owner string }

// Level segue il livello globale di zerolog: il debug di franz è verboso e va acceso dall'app, non
// da una costante qui.
func (kgoLogger) Level() kgo.LogLevel {
	switch {
	case zerolog.GlobalLevel() <= zerolog.DebugLevel:
		return kgo.LogLevelDebug
	case zerolog.GlobalLevel() == zerolog.InfoLevel:
		return kgo.LogLevelInfo
	case zerolog.GlobalLevel() == zerolog.WarnLevel:
		return kgo.LogLevelWarn
	default:
		return kgo.LogLevelError
	}
}

func (l kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var zl zerolog.Level
	switch level {
	case kgo.LogLevelError:
		zl = zerolog.ErrorLevel
	case kgo.LogLevelWarn:
		zl = zerolog.WarnLevel
	case kgo.LogLevelInfo:
		zl = zerolog.InfoLevel
	default:
		zl = zerolog.DebugLevel
	}
	e := log.WithLevel(zl).Str("component", "kafka").Str("owner", l.owner)
	for i := 0; i+1 < len(keyvals); i += 2 {
		e = e.Interface(fmt.Sprint(keyvals[i]), keyvals[i+1])
	}
	e.Msg(msg)
}

// observabilityOpts sono il logger e gli hook OTel, applicati a TUTTI i client del driver (group
// consumer, sessione transazionale, producer del DLQ).
//
// kotel espone tracing e metriche client-side (connessioni, byte prodotti/consumati, latenze delle
// richieste) attraverso i provider OTel già configurati da go-core-app: nessuna registrazione
// Prometheus nuova, le serie compaiono su /metrics insieme alle altre. Le metriche corekafka_* non
// c'entrano e non cambiano — misurano il batch dell'engine, non il client.
func observabilityOpts(owner string) []kgo.Opt {
	k := kotel.NewKotel(
		kotel.WithTracer(kotel.NewTracer(kotel.TracerProvider(otel.GetTracerProvider()))),
		kotel.WithMeter(kotel.NewMeter(kotel.MeterProvider(otel.GetMeterProvider()))),
	)
	return []kgo.Opt{
		kgo.WithLogger(kgoLogger{owner: owner}),
		kgo.WithHooks(k.Hooks()...),
	}
}
