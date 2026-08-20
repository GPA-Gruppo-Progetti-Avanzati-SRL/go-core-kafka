package processor

import (
	"fmt"
	"reflect"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/spec"
)

var (
	inType    = reflect.TypeOf(core.In{})
	outType   = reflect.TypeOf(core.Out{})
	errorType = reflect.TypeOf((*error)(nil)).Elem()
)

// synthCtor sintetizza il costruttore fx per il tipo struct t di un Handler/Transformer:
//
//	func(<param object con le SOLE dipendenze di t>) (<result object nel value group>, error)
//
// Il punto è che dig non vede mai t: vede un param object sintetico che contiene solo i campi
// dipendenza (quelli senza tag `prop:`). I campi property sono quindi invisibili al grafo e NON
// richiedono `optional:"true"` nell'app; li riempie BindProps dopo l'iniezione.
//
// mk riceve il `*T` già popolato (dipendenze iniettate + properties mappate) e ritorna la
// HandlerRegistration/TransformerRegistration da mettere nel value group: è la chiusura generica del
// chiamante, l'unico punto che conosce staticamente T e PT.
//
// Il costruttore è creato con reflect.MakeFunc, quindi fx non ha una location sensata da mostrare
// (dig.LocationForPC non è esposto da fx) e riporterebbe `reflect.makeFuncStub`. Per non perdere il
// contesto sugli errori più frequenti, le dipendenze **nil-abili** (puntatori, interfacce, mappe,
// slice, chan, func) sono dichiarate `optional:"true"` nel param object sintetico e verificate qui:
// così una dipendenza mancante produce un errore che nomina consumer, campo e tipo. Le dipendenze non
// nil-abili (valori struct, param object annidati, interi) restano obbligatorie per dig e in quel caso
// il messaggio è quello di fx, col tipo mancante ma senza location utile.
func synthCtor(t, regType reflect.Type, group string, s spec.ConsumerSpec, mk func(ptr any) any) (ctor any, err error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("il tipo del processor deve essere una struct, ricevuto %s", t)
	}
	// reflect.StructOf panica su input che non sa rappresentare: trasformiamo il panic in errore per
	// dare un messaggio contestualizzato al wiring.
	defer func() {
		if r := recover(); r != nil {
			ctor, err = nil, fmt.Errorf("impossibile sintetizzare il costruttore per %s: %v", t, r)
		}
	}()

	// Param object sintetico: marker In + i soli campi dipendenza, con i loro tag originali (così
	// `optional:"true"`, `name:` e `group:"..."` su una VERA dipendenza continuano a funzionare).
	fields := []reflect.StructField{{Name: "In", Type: inType, Anonymous: true}}
	var depIdx []int   // indice, in t, del campo corrispondente a fields[k+1]
	var required []int // indici (in t) delle dipendenze nil-abili che verifichiamo noi
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Type == inType {
			continue // marker core.In di t: non è una dipendenza (ed è opzionale, ormai)
		}
		if _, isProp := f.Tag.Lookup(PropTag); isProp {
			continue // property: la riempie BindProps, dig non deve vederla
		}
		if f.PkgPath != "" {
			continue // campo non esportato: non iniettabile, resta a zero
		}
		// Anonymous=false anche per i campi embeddati: a dig serve solo il tipo (un param object
		// annidato è riconosciuto dal tipo, non dall'embedding), e StructOf panica sugli embedded con
		// metodi. Il valore lo ricopiamo per indice.
		tag := f.Tag
		if checkableDep(f) {
			// La rendiamo opzionale per dig e la verifichiamo noi, per poter dare un errore che nomina
			// consumer/campo/tipo invece del generico makeFuncStub.
			tag = reflect.StructTag(string(f.Tag) + ` optional:"true"`)
			required = append(required, i)
		}
		fields = append(fields, reflect.StructField{Name: f.Name, Type: f.Type, Tag: tag})
		depIdx = append(depIdx, i)
	}
	paramType := reflect.StructOf(fields)

	// Result object: marker Out + la registrazione taggata sul value group.
	resultType := reflect.StructOf([]reflect.StructField{
		{Name: "Out", Type: outType, Anonymous: true},
		{Name: "Registration", Type: regType, Tag: reflect.StructTag(`group:"` + group + `"`)},
	})

	fnType := reflect.FuncOf([]reflect.Type{paramType}, []reflect.Type{resultType, errorType}, false)
	fn := reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		zero := reflect.New(resultType).Elem()

		ptr := reflect.New(t) // *T
		for k, idx := range depIdx {
			ptr.Elem().Field(idx).Set(args[0].Field(k + 1))
		}

		for _, idx := range required {
			if ptr.Elem().Field(idx).IsZero() {
				f := t.Field(idx)
				err := fmt.Errorf("corekafka: consumer %q (%s): dipendenza mancante nel grafo fx: campo %s di tipo %s (manca un provider?)",
					s.Name, t, f.Name, f.Type)
				return []reflect.Value{zero, reflect.ValueOf(&err).Elem()}
			}
		}

		if err := BindProps(ptr.Interface(), s.Properties); err != nil {
			return []reflect.Value{zero, reflect.ValueOf(&err).Elem()}
		}

		res := reflect.New(resultType).Elem()
		res.Field(1).Set(reflect.ValueOf(mk(ptr.Interface())))
		return []reflect.Value{res, reflect.Zero(errorType)}
	})

	return fn.Interface(), nil
}

// checkableDep indica se la dipendenza può essere resa opzionale per dig e verificata da noi: serve un
// tipo nil-abile (per distinguere "assente" da "zero legittimo"), nessun `group:` (dig rifiuta i value
// group opzionali) e nessun `optional:"true"` messo dall'app (che vuole proprio poterla omettere).
func checkableDep(f reflect.StructField) bool {
	if f.Tag.Get("group") != "" || f.Tag.Get("optional") == "true" {
		return false
	}
	switch f.Type.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return true
	default:
		return false
	}
}
