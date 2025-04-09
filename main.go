package main

import (
	"context"

	"github.com/markberger/yaks/internal/broker"
)

func main() {
	server := broker.NewBroker(0, "localhost", 9092)
	server.ListenAndServe(context.TODO())
}
