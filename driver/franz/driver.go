// Package franzdriver è il punto di selezione del driver franz-go (twmb/franz-go): l'unica cosa
// pubblica del driver è la sua registrazione, l'implementazione resta in internal/franzdriver e non è
// nominabile dalle app.
//
// È puro Go: un'applicazione che importa questo package (e non driver/confluent) builda con
// CGO_ENABLED=0 e non si porta dietro librdkafka né confluent-kafka-go, che nel suo go.mod non
// compaiono affatto.
package franzdriver

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	impl "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/franzdriver"
)

// Driver registra la driver.Factory franz-go nel grafo fx. È il valore da passare a
// corekafka.WithDriver:
//
//	corekafka.Module(&cfg.Kafka, Register, corekafka.WithDriver(franzdriver.Driver))
//
// Si chiama Driver e non Module perché non wira un sottosistema: sceglie QUALE client Kafka usa il
// Module di corekafka.
//
// Non prende i modes: il driver è del Module che lo invoca e ne eredita il gating (vedi
// corekafka.WithDriver).
func Driver() { core.Provide(impl.New) }
