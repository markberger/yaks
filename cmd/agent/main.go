package main

import (
	"context"
	"log"
	"os"

	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/config"
	"github.com/markberger/yaks/internal/metastore"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	db, err := metastore.Connect(cfg.DB)
	if err != nil {
		log.Fatalf("failed to connect to metastore: %v", err)
		os.Exit(1)
	}

	// TODO: move migrator to separate cmd
	agent := agent.NewAgent(db, cfg)
	err = agent.ApplyMigrations()
	if err != nil {
		log.Fatalf("failed to apply db migrations: %v", err)
		os.Exit(1)
	}
	agent.AddHandlers()
	agent.ListenAndServe(context.Background())
}
