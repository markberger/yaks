package handlers

import (
	"github.com/markberger/yaks/internal/metastore"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type OffsetFetchRequestHandler struct {
	metastore metastore.Metastore
}

func NewOffsetFetchRequestHandler(m metastore.Metastore) *OffsetFetchRequestHandler {
	return &OffsetFetchRequestHandler{m}
}

func (h *OffsetFetchRequestHandler) Key() kmsg.Key     { return kmsg.OffsetFetch }
func (h *OffsetFetchRequestHandler) MinVersion() int16 { return 1 }
func (h *OffsetFetchRequestHandler) MaxVersion() int16 { return 5 }
func (h *OffsetFetchRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.OffsetFetchRequest)

	response := kmsg.NewOffsetFetchResponse()
	response.SetVersion(request.GetVersion())
	if request.GetVersion() >= 3 {
		response.ThrottleMillis = 0
	}

	for _, t := range request.Topics {
		responseTopic := kmsg.NewOffsetFetchResponseTopic()
		responseTopic.Topic = t.Topic

		topic := h.metastore.GetTopicByName(t.Topic)

		for _, partition := range t.Partitions {
			responsePartition := kmsg.NewOffsetFetchResponseTopicPartition()
			responsePartition.Partition = partition

			if topic == nil {
				responsePartition.Offset = -1
				responsePartition.ErrorCode = kerr.UnknownTopicOrPartition.Code
			} else {
				offset, metadata, err := h.metastore.FetchOffset(request.Group, topic.ID, partition)
				if err != nil {
					responsePartition.Offset = -1
					responsePartition.ErrorCode = kerr.KafkaStorageError.Code
				} else {
					responsePartition.Offset = offset
					responsePartition.Metadata = &metadata
				}
			}

			responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
		}

		response.Topics = append(response.Topics, responseTopic)
	}

	return &response, nil
}
