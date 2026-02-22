package handlers

import (
	"github.com/markberger/yaks/internal/metastore"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type OffsetCommitRequestHandler struct {
	metastore metastore.Metastore
}

func NewOffsetCommitRequestHandler(m metastore.Metastore) *OffsetCommitRequestHandler {
	return &OffsetCommitRequestHandler{m}
}

func (h *OffsetCommitRequestHandler) Key() kmsg.Key     { return kmsg.OffsetCommit }
func (h *OffsetCommitRequestHandler) MinVersion() int16 { return 2 }
func (h *OffsetCommitRequestHandler) MaxVersion() int16 { return 7 }
func (h *OffsetCommitRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.OffsetCommitRequest)

	response := kmsg.NewOffsetCommitResponse()
	response.SetVersion(request.GetVersion())
	if request.GetVersion() >= 3 {
		response.ThrottleMillis = 0
	}

	for _, t := range request.Topics {
		responseTopic := kmsg.NewOffsetCommitResponseTopic()
		responseTopic.Topic = t.Topic

		topic := h.metastore.GetTopicByName(t.Topic)

		for _, p := range t.Partitions {
			responsePartition := kmsg.NewOffsetCommitResponseTopicPartition()
			responsePartition.Partition = p.Partition

			if topic == nil {
				responsePartition.ErrorCode = kerr.UnknownTopicOrPartition.Code
			} else {
				var metadata string
				if p.Metadata != nil {
					metadata = *p.Metadata
				}
				err := h.metastore.CommitOffset(request.Group, topic.ID, p.Partition, p.Offset, metadata)
				if err != nil {
					responsePartition.ErrorCode = kerr.KafkaStorageError.Code
				}
			}

			responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
		}

		response.Topics = append(response.Topics, responseTopic)
	}

	return &response, nil
}
