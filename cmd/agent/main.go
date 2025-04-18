package main

import (
	"context"
	"log"
	"os"

	"github.com/markberger/yaks/internal/agent"
	"github.com/markberger/yaks/internal/metastore"
)

func main() {
	config := metastore.Config{
		Host:     "localhost",
		Port:     5432,
		User:     "testuser",
		Password: "testpassword",
		DBName:   "testdb",
		SSLMode:  "disable",
	}
	db, err := metastore.Connect(config)
	if err != nil {
		log.Fatalf("failed to connect to metastore: %v", err)
		os.Exit(1)
	}

	// TODO: move migrator to separate cmd
	agent := agent.NewAgent(db, "localhost", 9092)
	err = agent.ApplyMigrations()
	if err != nil {
		log.Fatalf("failed to apply db migrations: %v", err)
		os.Exit(1)
	}
	agent.AddHandlers()
	agent.ListenAndServe(context.Background())
}
