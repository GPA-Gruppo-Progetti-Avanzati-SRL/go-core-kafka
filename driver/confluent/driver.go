// Package confluentdriver è il punto di selezione del driver Confluent (confluent-kafka-go/v2):
// l'unica cosa pubblica del driver è la sua registrazione, l'implementazione resta in
// internal/confluentdriver e non è nominabile dalle app.
//
// Importare questo package significa importare librdkafka via CGo, quindi l'applicazione richiede
// CGO_ENABLED=1. Un'app che non lo importa non se lo trascina né nel binario né nel proprio go.mod:
// è la ragione per cui la scelta del driver è un import dell'app e non una riga del package root.
package confluentdriver

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	impl "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/confluentdriver"
)

// Driver registra la driver.Factory Confluent nel grafo fx. È il valore da passare a
// corekafka.WithDriver:
//
//	corekafka.Module(&cfg.Kafka, Register, corekafka.WithDriver(confluentdriver.Driver))
//
// Si chiama Driver e non Module perché non wira un sottosistema: sceglie QUALE client Kafka usa il
// Module di corekafka.
//
// Non prende i modes: il driver è del Module che lo invoca e ne eredita il gating (corekafka.Module
// la chiama solo se core.IsMode passa). Un parametro qui permetterebbe di dichiarare un driver
// attivo in modi diversi da quelli dei consumer che lo usano — cioè un grafo che a runtime non si
// costruisce, per una divergenza che nessuno ha motivo di volere.
func Driver() { core.Provide(impl.New) }
