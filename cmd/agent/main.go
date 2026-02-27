package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/config"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/metrics"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	db, err := metastore.Connect(cfg.DB)
	if err != nil {
		log.Fatalf("failed to connect to metastore: %v", err)
	}

	shutdownMetrics, err := metrics.Init(context.Background(), cfg.OTel)
	if err != nil {
		log.Fatalf("failed to init metrics: %v", err)
	}
	defer shutdownMetrics(context.Background())

	// TODO: move migrator to separate cmd
	agent := agent.NewAgent(db, cfg)
	if cfg.RunMigrations {
		err = agent.ApplyMigrations()
		if err != nil {
			log.Fatalf("failed to apply db migrations: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	agent.AddHandlers()
	agent.ListenAndServe(ctx)
}
