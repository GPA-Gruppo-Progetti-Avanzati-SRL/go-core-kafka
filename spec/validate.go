package spec

import (
	"fmt"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// Questo file raccoglie le validazioni che la LIBRERIA garantisce, perché chiamate dai costruttori fx
// (consumer.NewConsumers, consumer.newRunner, producer.NewProducer): un difetto di configurazione fa
// fallire l'avvio invece di degradare in esercizio.
//
// La differenza rispetto a prima non è di severità ma di CHI esegue. I tag `validate:` sulle struct
// non si applicano da soli: li esegue core.ValidateStruct, e finché a chiamarla era soltanto l'app
// (dentro la propria core.ReadConfig) le regole dichiarate su questo spec valevano solo per chi
// passava di lì. Un `on-error: deadleter` non fermava nulla — `omitempty` lo lasciava vuoto, il
// default lo portava a fail-fast, e il refuso si manifestava mesi dopo come una policy deadletter
// che non entrava mai in funzione.

// ValidateServer verifica la sezione `server`: i tag `validate:` di tutto il sottoalbero
// (bootstrap-servers obbligatorio, gli enum di security/SASL e dei due blocchi di tuning) e le chiavi
// riservate nei tre blocchi `kafka-properties`.
//
// È una funzione e non tre chiamate al call site perché i due percorsi che devono farla — l'engine e
// il Producer condiviso, che è registrabile anche da solo — non devono tenere allineate due liste di
// etichette scritte a mano. L'ordine dei blocchi è FISSO: iterando una mappa, con due sezioni
// entrambe sbagliate quale errore comparisse cambiava da un avvio all'altro.
func ValidateServer(k KafkaServer) error {
	if err := core.ValidateStruct(k); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	for _, blk := range []struct {
		owner string
		props map[string]string
	}{
		{"server", k.KafkaProperties},
		{"server.consumer", k.Consumer.KafkaProperties},
		{"server.producer", k.Producer.KafkaProperties},
	} {
		if err := ValidateKafkaProperties(blk.owner, blk.props); err != nil {
			return err
		}
	}
	return nil
}

// ValidateProcessor verifica una voce di `processors` COSÌ COM'È SCRITTA: i tag `validate:`
// (name/topics/group-id obbligatori, e gli enum dei soli blocchi che il processor ha davvero scritto)
// e le chiavi riservate dei suoi due blocchi `kafka-properties`.
//
// Sullo spec GREZZO e non risolto, di proposito: un blocco ereditato è già stato validato al livello
// di `server`, e attribuire al processor un valore che non ha scritto manda a correggere il file
// sbagliato. Le relazioni FRA campi vanno invece verificate sullo spec risolto — vedi
// ConsumerTuning.Validate — perché lì il valore effettivo è quello che conta.
func ValidateProcessor(raw ProcessorSpec) error {
	if err := core.ValidateStruct(raw); err != nil {
		return fmt.Errorf("processor %q: %w", raw.Name, err)
	}
	for _, blk := range []struct {
		owner string
		props map[string]string
	}{
		{"processor " + raw.Name + " (consumer)", raw.Consumer.KafkaProperties},
		{"processor " + raw.Name + " (producer)", raw.Producer.KafkaProperties},
	} {
		if err := ValidateKafkaProperties(blk.owner, blk.props); err != nil {
			return err
		}
	}
	return nil
}
