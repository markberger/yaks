package broker

import (
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type ApiVersionsRequestHandler struct {
	handlerRegistry *handlerRegistry
}

func NewApiVersionsRequestHandler(handlerRegistry *handlerRegistry) ApiVersionsRequestHandler {
	return ApiVersionsRequestHandler{handlerRegistry}
}
func (h ApiVersionsRequestHandler) Key() kmsg.Key     { return kmsg.ApiVersions }
func (h ApiVersionsRequestHandler) MinVersion() int16 { return 0 }
func (h ApiVersionsRequestHandler) MaxVersion() int16 { return 4 }
func (h ApiVersionsRequestHandler) Handle(request kmsg.Request) (kmsg.Response, error) {
	response := kmsg.NewApiVersionsResponse()
	response.SetVersion(request.GetVersion())
	if response.GetVersion() > h.MaxVersion() {
		response.ErrorCode = 35
		return &response, nil
	}

	for key, versionToHandler := range h.handlerRegistry.handlerMap {
		// TODO: do not assume versions are contiguous
		versions := slices.Collect(maps.Keys(versionToHandler))
		apiKey := kmsg.NewApiVersionsResponseApiKey()
		apiKey.ApiKey = int16(key)
		apiKey.MinVersion = slices.Min(versions)
		apiKey.MaxVersion = slices.Max(versions)
		response.ApiKeys = append(response.ApiKeys, apiKey)
	}
	if response.GetVersion() >= 1 {
		response.SetThrottle(0)
	}

	return &response, nil
}

type MetadataRequestHandler struct {
	broker *Broker
}

func NewMetadataRequestHandler(broker *Broker) MetadataRequestHandler {
	return MetadataRequestHandler{broker}
}
func (h MetadataRequestHandler) Key() kmsg.Key     { return kmsg.Metadata }
func (h MetadataRequestHandler) MinVersion() int16 { return 0 }
func (h MetadataRequestHandler) MaxVersion() int16 { return 3 }
func (h MetadataRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.MetadataRequest)
	allTopics := len(request.Topics) == 0
	if !allTopics {
		panic("Requesting topic specific info is not yet supported")
	}

	response := kmsg.NewMetadataResponse()
	response.Default()
	response.SetVersion(request.Version)
	response.ThrottleMillis = 0
	response.Brokers = append(response.Brokers, kmsg.MetadataResponseBroker{
		NodeID: h.broker.nodeID,
		Host:   h.broker.host,
		Port:   h.broker.port,
	})
	clusterID := "cluster id"
	response.ClusterID = &clusterID
	response.ControllerID = 0

	topicName := "test-topic"
	response.Topics = append(response.Topics, kmsg.MetadataResponseTopic{
		ErrorCode:  0,
		Topic:      &topicName,
		IsInternal: false,
		Partitions: []kmsg.MetadataResponseTopicPartition{
			{
				ErrorCode: 0,
				Partition: 0,
				Leader:    0,
				Replicas:  []int32{},
				ISR:       []int32{},
			},
		},
	})
	return &response, nil
}
