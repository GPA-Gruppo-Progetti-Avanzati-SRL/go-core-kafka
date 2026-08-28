package spec

import (
	"reflect"
	"testing"
	"time"
)

// L'eredità è delegata a core.Inherit, che applica a OGNI campo la stessa regola. Questo test è la
// controparte di quella scelta: enumera i campi per reflection e verifica che ognuno erediti e sia
// sovrascrivibile. È ciò che prima non esisteva — l'eredità era scritta campo per campo, e un campo
// aggiunto senza il suo ramo non ereditava senza che nulla lo segnalasse.
//
// Aggiungere un campo a uno dei tre blocchi non richiede di toccare questo test: entra da solo.

// setValue valorizza un campo con uno di due valori distinti e non-zero: `local` è quello che il
// processor scrive, l'altro è quello del blocco globale. Devono essere DIVERSI, altrimenti il test
// "l'override vince" passerebbe anche se vincesse il globale.
//
// Per un puntatore conta che non sia nil: il pointee può anche essere un valore "spegnente" (false, 0)
// — è esattamente il caso per cui quei campi sono puntatori.
func setValue(v reflect.Value, local bool) bool {
	switch v.Kind() {
	case reflect.String:
		v.SetString(map[bool]string{true: "locale", false: "globale"}[local])
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(map[bool]int64{true: 7, false: 99}[local])
	case reflect.Float32, reflect.Float64:
		v.SetFloat(map[bool]float64{true: 7, false: 99}[local])
	case reflect.Bool:
		// Il processor mette true, il globale false: così sul puntatore si verifica che uno "spegnente"
		// esplicito sopravviva a un globale valorizzato.
		v.SetBool(local)
	case reflect.Pointer:
		p := reflect.New(v.Type().Elem())
		if !setValue(p.Elem(), local) {
			return false
		}
		v.Set(p)
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		m.SetMapIndex(reflect.ValueOf(map[bool]string{true: "locale", false: "globale"}[local]), reflect.ValueOf("x"))
		v.Set(m)
	default:
		return false
	}
	return true
}

// setSentinel valorizza un campo col valore "del processor".
func setSentinel(v reflect.Value) bool { return setValue(v, true) }

// blockOf ritorna il blocco indicato di uno spec (o del server), indirizzabile.
func blockOf(v reflect.Value, block string) reflect.Value { return v.Elem().FieldByName(block) }

func TestResolve_OgniCampoEreditaEdESovrascrivibile(t *testing.T) {
	for _, block := range []string{"Consumer", "Producer", "Restart"} {
		t.Run(block, func(t *testing.T) {
			bt := blockOf(reflect.ValueOf(&ProcessorSpec{}), block).Type()

			for i := range bt.NumField() {
				f := bt.Field(i)
				if !f.IsExported() {
					continue
				}
				if f.Type.Kind() == reflect.Bool {
					// Un bool non eredita per contratto: false è indistinguibile da "non scritto",
					// e chi ha bisogno di ereditarlo usa *bool.
					continue
				}
				t.Run(f.Name, func(t *testing.T) {
					// 1) valorizzato SOLO sul globale ⇒ il processor lo eredita.
					server := &KafkaServer{}
					if !setSentinel(blockOf(reflect.ValueOf(server), block).Field(i)) {
						t.Fatalf("%s.%s: tipo %s non coperto da setValue — aggiungerlo, o l'eredità di questo campo resta non verificata", block, f.Name, f.Type)
					}
					want := blockOf(reflect.ValueOf(server), block).Field(i).Interface()

					local := validSpec()
					got := local.Resolve(*server)
					if g := blockOf(reflect.ValueOf(&got), block).Field(i).Interface(); !reflect.DeepEqual(g, want) {
						t.Errorf("%s.%s = %v, atteso %v ereditato dal globale", block, f.Name, g, want)
					}

					// 2) valorizzato su ENTRAMBI ⇒ vince il processor. Il globale porta un valore
					// diverso, così un'eredità sbagliata non passa per coincidenza.
					override := validSpec()
					if !setSentinel(blockOf(reflect.ValueOf(&override), block).Field(i)) {
						return
					}
					want2 := blockOf(reflect.ValueOf(&override), block).Field(i)
					server2 := &KafkaServer{}
					setValue(blockOf(reflect.ValueOf(server2), block).Field(i), false)
					got2 := blockOf(reflect.ValueOf(ptr(override.Resolve(*server2))), block).Field(i)

					if f.Type.Kind() == reflect.Map {
						// Le mappe si FONDONO, non si sostituiscono: quello che si verifica qui è che
						// la chiave del processor sopravviva (il merge completo è in
						// TestResolve_KafkaPropertiesSiFondono).
						for iter := want2.MapRange(); iter.Next(); {
							if g := got2.MapIndex(iter.Key()); !g.IsValid() || g.Interface() != iter.Value().Interface() {
								t.Errorf("%s.%s: la chiave %v del processor non è sopravvissuta al merge", block, f.Name, iter.Key())
							}
						}
						return
					}
					if !reflect.DeepEqual(got2.Interface(), want2.Interface()) {
						t.Errorf("%s.%s = %v, atteso %v del processor (il globale non deve vincere)", block, f.Name, got2, want2)
					}
				})
			}
		})
	}
}

func TestResolve_KafkaPropertiesSiFondono(t *testing.T) {
	// Il caso mappa passa dalla stessa via generica: aggiungere una proprietà a un processor non gli
	// deve far perdere quelle comuni.
	server := KafkaServer{Consumer: ConsumerTuning{KafkaProperties: map[string]string{
		"comune": "globale", "conteso": "globale",
	}}}
	s := validSpec()
	s.Consumer.KafkaProperties = map[string]string{"conteso": "locale", "solo-mio": "x"}

	got := s.Resolve(server).Consumer.KafkaProperties
	if got["comune"] != "globale" {
		t.Errorf("proprietà comune persa: %v", got)
	}
	if got["conteso"] != "locale" {
		t.Errorf("conteso = %q, atteso il valore del processor", got["conteso"])
	}
	if got["solo-mio"] != "x" {
		t.Errorf("proprietà locale persa: %v", got)
	}
	// Il blocco globale è condiviso: non deve aver preso le chiavi del processor.
	if _, leaked := server.Consumer.KafkaProperties["solo-mio"]; leaked {
		t.Error("l'eredità ha mutato la mappa globale")
	}
}

func TestIsZero_ProducerTuning(t *testing.T) {
	// IsZero distingue "il processor non ha un blocco producer" da "ne ha uno": è ciò che decide se
	// avvisare che un override in modalità handle non avrà effetto.
	if !(ProducerTuning{}).IsZero() {
		t.Error("un blocco producer vuoto deve essere zero")
	}
	// Ogni campo, uno per volta: è la lista che prima si teneva a mano e che dimenticandone uno
	// faceva sparire l'avviso.
	pt := reflect.TypeOf(ProducerTuning{})
	for i := range pt.NumField() {
		f := pt.Field(i)
		if !f.IsExported() {
			continue
		}
		p := &ProducerTuning{}
		if !setSentinel(reflect.ValueOf(p).Elem().Field(i)) {
			continue
		}
		if p.IsZero() {
			t.Errorf("producer.%s valorizzato ma IsZero() = true", f.Name)
		}
	}
}

func TestDefaults_SonoCoerenti(t *testing.T) {
	// I default della libreria devono superare le validazioni della libreria: un default che non le
	// passa è un'app che non parte senza che nessuno abbia configurato niente.
	// Le regole non sono più riscritte qui: sono i Validate della libreria, gli stessi che girano
	// all'avvio su ogni processor. Prima questo test ne teneva una copia a mano — utile finché i
	// Validate non esistevano, ma una seconda scrittura della stessa regola è una regola che diverge.
	got := validSpec().Resolve(KafkaServer{})
	for name, check := range map[string]func(string) error{
		"restart":  got.Restart.Validate,
		"consumer": got.Consumer.Validate,
		"producer": got.Producer.Validate,
	} {
		if err := check("default"); err != nil {
			t.Errorf("i default di %s non superano la propria validazione: %v", name, err)
		}
	}
}

func TestConsumerTuningValidate(t *testing.T) {
	base := func() ConsumerTuning { return ConsumerTuning{}.WithDefaults() }
	tests := []struct {
		name    string
		mutate  func(*ConsumerTuning)
		wantErr bool
	}{
		{"default coerenti", func(*ConsumerTuning) {}, false},
		// Un solo campo valorizzato non ha con cosa contraddirsi: lo zero significa "lascia il default
		// di librdkafka", e inventarsi il valore implicito per confrontarlo farebbe fallire l'avvio su
		// una configurazione che nessuno ha scritto.
		{"solo session-timeout", func(c *ConsumerTuning) { c.SessionTimeoutMs = 10000 }, false},
		{"solo heartbeat", func(c *ConsumerTuning) { c.HeartbeatIntervalMs = 3000 }, false},
		{"heartbeat sotto un terzo", func(c *ConsumerTuning) {
			c.SessionTimeoutMs, c.HeartbeatIntervalMs = 10000, 3000
		}, false},
		{"heartbeat esattamente un terzo", func(c *ConsumerTuning) {
			c.SessionTimeoutMs, c.HeartbeatIntervalMs = 9000, 3000
		}, true},
		{"heartbeat oltre un terzo", func(c *ConsumerTuning) {
			c.SessionTimeoutMs, c.HeartbeatIntervalMs = 10000, 8000
		}, true},
		{"max-poll sopra la sessione", func(c *ConsumerTuning) {
			c.SessionTimeoutMs, c.MaxPollIntervalMs = 10000, 300000
		}, false},
		{"max-poll sotto la sessione", func(c *ConsumerTuning) {
			c.SessionTimeoutMs, c.MaxPollIntervalMs = 45000, 10000
		}, true},
		{"poll-timeout oltre il taglio", func(c *ConsumerTuning) {
			c.PollTimeout, c.CutFrequency = 2*time.Second, time.Second
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(&c)
			if err := c.Validate("test"); (err != nil) != tc.wantErr {
				t.Errorf("Validate = %v, atteso errore = %v", err, tc.wantErr)
			}
		})
	}
}

// L'incoerenza che TestDefaults_SonoCoerenti aveva trovato nei default della libreria è ora una
// regola imposta anche a ciò che scrive l'utente.
func TestProducerTuningValidate(t *testing.T) {
	p := ProducerTuning{}.WithDefaults()
	if err := p.Validate("test"); err != nil {
		t.Fatalf("default = %v, atteso nil", err)
	}
	p.DeliveryTimeout = p.FlushTimeout + time.Second
	if err := p.Validate("test"); err == nil {
		t.Error("delivery-timeout > flush-timeout: atteso errore, i record in volo verrebbero abbandonati")
	}
}

func TestRestartValidate(t *testing.T) {
	base := func() RestartSpec {
		return RestartSpec{}.WithDefaults()
	}
	tests := []struct {
		name    string
		mutate  func(*RestartSpec)
		wantErr bool
	}{
		{"default coerenti", func(*RestartSpec) {}, false},
		{"tentativi finiti", func(r *RestartSpec) { r.MaxAttempts = ptr(3) }, false},
		{"illimitati espliciti", func(r *RestartSpec) { r.MaxAttempts = ptr(-1) }, false},
		// Prima 0 significava "illimitati" ED era il valore che si otteneva senza scrivere nulla.
		{"zero esplicito rifiutato", func(r *RestartSpec) { r.MaxAttempts = ptr(0) }, true},
		{"backoff costante ammesso", func(r *RestartSpec) { r.Multiplier = ptr(1.0) }, false},
		{"backoff decrescente rifiutato", func(r *RestartSpec) { r.Multiplier = ptr(0.5) }, true},
		{"initial > max rifiutato", func(r *RestartSpec) { r.InitialBackoff = time.Hour }, true},
		// reset-after piccolo = il budget si ricarica a ogni run breve, quindi max-attempts finito
		// diventa illimitato di fatto: è l'altra strada verso lo stesso loop.
		{"reset-after troppo piccolo rifiutato", func(r *RestartSpec) { r.ResetAfter = time.Second }, true},
		{"reset-after piccolo ma illimitati espliciti", func(r *RestartSpec) {
			r.ResetAfter = time.Second
			r.MaxAttempts = ptr(-1)
		}, false},
		// Con la supervisione spenta la politica non ha effetto: far cadere l'app per un knob inerte
		// sarebbe severità senza scopo.
		{"disabled salta i controlli", func(r *RestartSpec) {
			r.Disabled = ptr(true)
			r.MaxAttempts = ptr(0)
			r.ResetAfter = time.Second
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := base()
			tc.mutate(&r)
			err := r.Validate("processor test")
			if tc.wantErr && err == nil {
				t.Error("atteso errore, nessuno")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("errore inatteso: %v", err)
			}
		})
	}
}

func TestRestartAccessors(t *testing.T) {
	// Il budget di default è FINITO: è l'inversione che rende l'illimitato una scelta.
	var zero RestartSpec
	if zero.Attempts() != DefaultRestartMaxAttempts || zero.Unlimited() {
		t.Errorf("Attempts()=%d Unlimited()=%v su spec vuoto, atteso %d finito", zero.Attempts(), zero.Unlimited(), DefaultRestartMaxAttempts)
	}
	if zero.BackoffMultiplier() != DefaultRestartMultiplier {
		t.Errorf("BackoffMultiplier() = %v, atteso %v", zero.BackoffMultiplier(), DefaultRestartMultiplier)
	}
	if !(RestartSpec{MaxAttempts: ptr(-1)}).Unlimited() {
		t.Error("max-attempts negativo deve valere illimitati")
	}
	if (RestartSpec{MaxAttempts: ptr(1)}).Unlimited() {
		t.Error("max-attempts positivo non è illimitato")
	}
}

// TestTransactionalIDNonRaggiungeIlProducerEOS blocca l'invariante che rende sicuro tenere
// `transactional-id` dentro ProducerTuning: il campo è ereditato da `server.producer` come tutti gli
// altri, ma per il producer EOS di un processor il driver prende l'id da ProcessorSpec.TransactionalID
// — non dal tuning. Se un giorno un driver leggesse s.Producer.TransactionalID, due client
// (il producer del processo e quello EOS del processor) avrebbero lo STESSO id e si fencerebbero a
// vicenda: un guasto che si manifesta solo sotto carico, e mai in test.
func TestTransactionalIDNonRaggiungeIlProducerEOS(t *testing.T) {
	server := KafkaServer{
		BootstrapServers: "broker:9092",
		Producer:         ProducerTuning{TransactionalID: "notifiche-pod-0", Acks: "all"},
	}
	got := ProcessorSpec{Name: "ingest", Topics: []string{"t"}, GroupID: "g", TransactionalID: "eos-ingest"}.Resolve(server)

	// L'eredità c'è (è la regola uniforme di core.Inherit, senza eccezioni per campo)...
	if got.Producer.TransactionalID != "notifiche-pod-0" {
		t.Fatalf("Producer.TransactionalID = %q: atteso il valore ereditato da server.producer", got.Producer.TransactionalID)
	}
	// ...ma l'id che il processor usa davvero è il SUO, quello dello spec.
	if got.TransactionalID != "eos-ingest" {
		t.Fatalf("ProcessorSpec.TransactionalID = %q, atteso eos-ingest", got.TransactionalID)
	}
	if got.TransactionalID == got.Producer.TransactionalID {
		t.Fatal("i due id coincidono: il producer del processo e quello EOS si fencerebbero a vicenda")
	}
}
