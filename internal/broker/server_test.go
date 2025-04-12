package broker

import (
	"context"
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

func TestConfluentMetadataRequest(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer func() {
		cancelFn()
		time.Sleep(time.Second)
	}()

	broker := NewBroker(0, "localhost", 9092)
	broker.Add(NewApiVersionsRequestHandler(broker.handlerRegistry))
	broker.Add(NewMockMetadataRequestHandler(broker))
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

	want := &ckafka.Metadata{
		Brokers: []ckafka.BrokerMetadata{
			{
				ID:   0,
				Host: "localhost",
				Port: 9092,
			},
		},
		Topics: map[string]ckafka.TopicMetadata{
			"test-topic": {
				Topic: "test-topic",
				Partitions: []ckafka.PartitionMetadata{
					{
						ID: 0,
					},
				},
			},
		},
		OriginatingBroker: ckafka.BrokerMetadata{
			ID:   0,
			Host: "localhost:9092/0",
			Port: 0,
		},
	}

	// Compare broker metadata
	if !reflect.DeepEqual(metadata.Brokers, want.Brokers) || !reflect.DeepEqual(metadata.OriginatingBroker, want.OriginatingBroker) {
		t.Errorf("broker metadata mismatch:\ngot=%+v, %+v\nwant=%+v, %+v",
			metadata.Brokers, metadata.OriginatingBroker,
			want.Brokers, want.OriginatingBroker)
	}

	// Compare topic metadata
	gotTopic, exists := metadata.Topics["test-topic"]
	if !exists || gotTopic.Topic != "test-topic" || len(gotTopic.Partitions) != 1 || gotTopic.Partitions[0].ID != 0 {
		t.Errorf("topic metadata mismatch:\ngot=%+v\nwant=%+v", metadata.Topics, want.Topics)
	}
}

type MockMetadataRequestHandler struct {
	broker *Broker
}

func NewMockMetadataRequestHandler(broker *Broker) MockMetadataRequestHandler {
	return MockMetadataRequestHandler{broker}
}
func (h MockMetadataRequestHandler) Key() kmsg.Key     { return kmsg.Metadata }
func (h MockMetadataRequestHandler) MinVersion() int16 { return 0 }
func (h MockMetadataRequestHandler) MaxVersion() int16 { return 3 }
func (h MockMetadataRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.MetadataRequest)
	allTopics := len(request.Topics) == 0
	if !allTopics {
		panic("request specific topics not supported")
	}

	response := kmsg.NewMetadataResponse()
	response.SetVersion(request.Version)
	response.ThrottleMillis = 0
	response.Brokers = append(response.Brokers, kmsg.MetadataResponseBroker{
		NodeID: h.broker.NodeID,
		Host:   h.broker.Host,
		Port:   h.broker.Port,
	})
	response.ClusterID = kmsg.StringPtr("cluster id")
	response.ControllerID = 0

	topicName := "test-topic"
	response.Topics = append(response.Topics, kmsg.MetadataResponseTopic{
		ErrorCode:  0,
		Topic:      &topicName,
		IsInternal: false,
		Partitions: []kmsg.MetadataResponseTopicPartition{
			{
				ErrorCode:       0,
				Partition:       0,
				Leader:          0,
				LeaderEpoch:     0,
				Replicas:        []int32{},
				ISR:             []int32{},
				OfflineReplicas: []int32{},
			},
		},
	})
	return &response, nil
}
