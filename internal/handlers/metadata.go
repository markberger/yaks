package handlers

import (
	"github.com/markberger/yaks/internal/broker"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type MetadataRequestHandler struct {
	broker    *broker.Broker
	metastore metastore.Metastore
}

func NewMetadataRequestHandler(b *broker.Broker, m metastore.Metastore) *MetadataRequestHandler {
	return &MetadataRequestHandler{b, m}
}
func (h *MetadataRequestHandler) Key() kmsg.Key     { return kmsg.Metadata }
func (h *MetadataRequestHandler) MinVersion() int16 { return 0 }
func (h *MetadataRequestHandler) MaxVersion() int16 { return 7 }
func (h *MetadataRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.MetadataRequest)
	response := kmsg.NewMetadataResponse()
	response.SetVersion(request.GetVersion())
	response.ThrottleMillis = 0
	broker := kmsg.MetadataResponseBroker{
		NodeID: h.broker.NodeID,
		Host:   h.broker.Host,
		Port:   h.broker.Port,
		Rack:   nil,
	}
	response.Brokers = append(response.Brokers, broker)
	response.ClusterID = nil
	response.ControllerID = 0
	response.Topics = []kmsg.MetadataResponseTopic{}

	existingTopics, err := h.metastore.GetTopics()
	if err != nil {
		return nil, err
	}

	if request.AllowAutoTopicCreation && len(request.Topics) > 0 {
		for _, t := range request.Topics {
			if !topicExists(t, existingTopics) {
				h.metastore.CreateTopic(*t.Topic, 1)
			}
		}
		existingTopics, err = h.metastore.GetTopics()
		if err != nil {
			return nil, err
		}
	}

	for _, t := range existingTopics {
		topic := kmsg.MetadataResponseTopic{
			Topic:      &t.Name,
			IsInternal: false,
			Partitions: []kmsg.MetadataResponseTopicPartition{
				{
					ErrorCode:   0,
					Partition:   0,
					Leader:      h.broker.NodeID,
					LeaderEpoch: 0,
					Replicas:    []int32{h.broker.NodeID},
					ISR:         []int32{h.broker.NodeID},
				},
			},
		}
		response.Topics = append(response.Topics, topic)
	}
	return &response, nil
}

func topicExists(t kmsg.MetadataRequestTopic, existingTopics []metastore.Topic) bool {
	for _, existingTopic := range existingTopics {
		if *t.Topic == existingTopic.Name {
			return true
		}
	}
	return false
}
