package agent

import (
	"context"

	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/handlers"
	"github.com/markberger/yaks/internal/metastore"
	"gorm.io/gorm"
)

type Agent struct {
	db        *gorm.DB
	Metastore metastore.Metastore
	broker    *broker.Broker
}

func NewAgent(db *gorm.DB, host string, port int32) *Agent {
	metastore := metastore.NewGormMetastore(db)
	broker := broker.NewBroker(0, host, port)
	return &Agent{db, metastore, broker}
}

// TODO: agent should not apply migrations it should be done by a separate
// cmd tool before deployment
func (a *Agent) ApplyMigrations() error {
	return a.Metastore.ApplyMigrations()
}

func (a *Agent) AddHandlers() {
	a.broker.Add(handlers.NewMetadataRequestHandler(a.broker, a.Metastore))
	a.broker.Add(handlers.NewCreateTopicsRequestHandler("s3://test-bucket", a.Metastore))
	a.broker.Add(handlers.NewProduceRequestHandler(a.Metastore))
}

func (a *Agent) ListenAndServe(ctx context.Context) {
	a.broker.ListenAndServe(ctx)
}
