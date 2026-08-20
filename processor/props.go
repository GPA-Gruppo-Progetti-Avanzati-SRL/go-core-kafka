package processor

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
	"github.com/go-viper/mapstructure/v2"
	"github.com/rs/zerolog/log"
)

// Tag riconosciuti sui campi di un Handler/Transformer.
const (
	// PropTag marca un campo come property del consumer; il valore è la chiave nel blocco `properties:`
	// dello spec (vuoto = nome del campo in minuscolo). SOLO i campi che lo portano sono toccati dal
	// mapping: le dipendenze iniettate da fx non sono nemmeno candidate.
	PropTag = "prop"
	// DefaultTag è il valore usato quando la chiave è assente dalle properties. È una stringa e passa
	// per la stessa conversione dei valori YAML (quindi "5s", "100", "true", "a,b" funzionano).
	DefaultTag = "default"
	// ValidateTag è il vincolo go-playground/validator applicato al singolo campo dopo il decode.
	ValidateTag = "validate"
)

// BindProps mappa le properties di un consumer sui campi di target (un puntatore a struct) taggati
// `prop:`. Chiamata dal wiring di RegisterHandler/RegisterTransformer, quindi un errore fa fallire la
// costruzione del grafo fx: l'app non parte (nessuna connessione Kafka aperta).
//
//	type myHandler struct {
//	    Svc mypkg.IService                                        // iniettato da fx
//	    Collection string        `prop:"collection" validate:"required"`
//	    BatchLimit int           `prop:"batch-limit" default:"100"`
//	    Timeout    time.Duration `prop:"timeout" default:"5s"`
//	}
//
// Nessun tag DI sui campi property: dig non li vede mai, perché il costruttore fornito a fx è
// sintetizzato da synthCtor con un param object che contiene le sole dipendenze. I campi `prop:`
// vengono comunque AZZERATI prima del decode, così nemmeno per vie indirette una property può
// ereditare un valore dal grafo fx.
//
// Le chiavi presenti nelle properties e non reclamate da nessun campo sono ignorate e loggate a Warn
// (rete di sicurezza sui typo, senza bloccare l'avvio).
func BindProps(target any, props spec.Properties) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("corekafka: BindProps richiede un puntatore a struct, ricevuto %T", target)
	}
	elem := rv.Elem()
	t := elem.Type()

	// Input del decode: SOLO le chiavi dei campi taggati `prop:`, così i campi non taggati (dipendenze
	// fx, core.In) non sono raggiungibili da mapstructure.
	in := make(map[string]any)
	claimed := make(map[string]bool)
	type propField struct {
		field reflect.StructField
		key   string
	}
	var fields []propField

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, ok := f.Tag.Lookup(PropTag)
		if !ok {
			continue
		}
		if f.PkgPath != "" {
			return fmt.Errorf("corekafka: campo %s.%s: il tag %q richiede un campo esportato", t.Name(), f.Name, PropTag)
		}
		key := tag
		if key == "" {
			key = strings.ToLower(f.Name)
		}
		claimed[key] = true
		fields = append(fields, propField{field: f, key: key})

		// Azzera: un valore iniettato da fx non deve sopravvivere come property.
		fv := elem.Field(i)
		fv.Set(reflect.Zero(f.Type))

		if v, present := props[key]; present {
			in[key] = v
		} else if def, hasDef := f.Tag.Lookup(DefaultTag); hasDef {
			in[key] = def
		}
	}

	// Nessun campo `prop:`: il consumer legge le properties dal context o da Configure — niente da
	// mappare e nessun warning da dare (le chiavi non sono "non reclamate", semplicemente non si usa
	// questo canale).
	if len(fields) == 0 {
		return nil
	}

	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           target,
		TagName:          PropTag,
		WeaklyTypedInput: true, // le sostituzioni ${ENV_VAR} e i default sono stringhe
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
	})
	if err != nil {
		return fmt.Errorf("corekafka: properties: %w", err)
	}
	if err := dec.Decode(in); err != nil {
		return fmt.Errorf("corekafka: properties: %w", err)
	}

	warnUnclaimed(t, props, claimed)

	// Validazione per campo (non sull'intera struct): evita di entrare nelle dipendenze iniettate, che
	// possono avere `validate:` propri, e permette di nominare la property nell'errore.
	for _, pf := range fields {
		rule, ok := pf.field.Tag.Lookup(ValidateTag)
		if !ok || rule == "" {
			continue
		}
		if err := core.Validator.Var(elem.FieldByIndex(pf.field.Index).Interface(), rule); err != nil {
			return fmt.Errorf("corekafka: property %q (campo %s): %w", pf.key, pf.field.Name, err)
		}
	}

	return nil
}

// warnUnclaimed logga le chiavi delle properties che nessun campo `prop:` ha reclamato: tipicamente un
// typo in config. Non è un errore (le chiavi extra restano lecite, es. properties lette dal context).
func warnUnclaimed(t reflect.Type, props spec.Properties, claimed map[string]bool) {
	var extra []string
	for k := range props {
		if !claimed[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) == 0 {
		return
	}
	sort.Strings(extra)
	log.Warn().Str("type", t.Name()).Strs("keys", extra).
		Msg("corekafka: properties non mappate su alcun campo `prop:` (typo in config?)")
}
