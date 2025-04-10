package broker

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestApiVersionsHandler(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer func() {
		cancelFn()
		time.Sleep(time.Second)
	}()

	broker := NewBroker(0, "localhost", 9092)
	broker.Add(NewApiVersionsRequestHandler(broker.handlerRegistry))
	go broker.ListenAndServe(ctx)
	time.Sleep(1 * time.Second)

	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		t.Errorf("failed to create client")
	}

	request := kmsg.NewApiVersionsRequest()
	response, err := client.Request(ctx, &request)
	if err != nil {
		t.Errorf("Failed to make request: %v\n", err)
	}

	want := kmsg.NewApiVersionsResponse()
	want.Version = 3
	want.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{
			ApiKey:     int16(kmsg.ApiVersions),
			MinVersion: 0,
			MaxVersion: 4,
		},
	}
	if !reflect.DeepEqual(response, &want) {
		t.Errorf("got unexpected response: response=%v want=%v", response, want)
	}
}

func TestMetadataRequestHandler(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer func() {
		cancelFn()
		time.Sleep(time.Second)
	}()

	broker := NewBroker(0, "localhost", 9092)
	broker.Add(NewApiVersionsRequestHandler(broker.handlerRegistry))
	broker.Add(NewMetadataRequestHandler(broker))
	go broker.ListenAndServe(ctx)
	time.Sleep(1 * time.Second)

	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		t.Errorf("failed to create client")
	}

	request := kmsg.NewMetadataRequest()
	request.SetVersion(3)
	response, err := client.Request(ctx, &request)
	if err != nil {
		t.Errorf("Failed to make request: %v\n", err)
	}

	fmt.Println(response)
}

func TestConfluentMetadataRequest(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer func() {
		cancelFn()
		time.Sleep(time.Second)
	}()

	broker := NewBroker(0, "localhost", 9092)
	broker.Add(NewApiVersionsRequestHandler(broker.handlerRegistry))
	broker.Add(NewMetadataRequestHandler(broker))
	go broker.ListenAndServe(ctx)
	time.Sleep(1 * time.Second)

	config := &ckafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	adminClient, err := ckafka.NewAdminClient(config)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	fmt.Printf("Metadata: %v\n", metadata)
}
