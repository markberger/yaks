package agent

import (
	"context"
	"fmt"

	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/handlers"
	"github.com/markberger/yaks/internal/metastore"
	"gorm.io/gorm"
)

type Agent struct {
	db        *gorm.DB
	metastore metastore.Metastore
	broker    *broker.Broker
}

func NewAgent(cfg metastore.Config, host string, port int32) (*Agent, error) {
	db, err := metastore.Connect(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to metastore: %v", err)
	}
	metastore := metastore.NewGormMetastore(db)
	broker := broker.NewBroker(0, host, port)
	return &Agent{db, metastore, broker}, nil
}

// TODO: agent should not apply migrations it should be done by a separate
// cmd tool before deployment
func (a *Agent) ApplyMigrations() error {
	return a.db.AutoMigrate(&metastore.Topic{}, &metastore.RecordBatch{})
}

func (a *Agent) AddHandlers() {
	a.broker.Add(handlers.NewMetadataRequestHandler(a.broker, a.metastore))
	a.broker.Add(handlers.NewCreateTopicsRequestHandler("s3://test-bucket", a.metastore))
}

func (a *Agent) ListenAndServe(ctx context.Context) {
	a.broker.ListenAndServe(ctx)
}
