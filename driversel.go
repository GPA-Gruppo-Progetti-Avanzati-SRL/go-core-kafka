package corekafka

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/internal/confluentdriver"
)

// provideDriver registra la driver.Factory attiva nel grafo fx. È l'UNICO punto legato al client
// Kafka concreto: per passare a franz-go in futuro basterà sostituire confluentdriver.New con
// franzdriver.New qui — engine, Producer e app restano invariati.
func provideDriver(modes ...string) {
	core.Provide(confluentdriver.New, modes...)
}
