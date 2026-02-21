package handlers

import (
	"github.com/markberger/yaks/internal/metastore"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type ListOffsetsRequestHandler struct {
	metastore metastore.Metastore
}

func NewListOffsetsRequestHandler(m metastore.Metastore) *ListOffsetsRequestHandler {
	return &ListOffsetsRequestHandler{m}
}

func (h *ListOffsetsRequestHandler) Key() kmsg.Key     { return kmsg.ListOffsets }
func (h *ListOffsetsRequestHandler) MinVersion() int16  { return 1 }
func (h *ListOffsetsRequestHandler) MaxVersion() int16  { return 4 }
func (h *ListOffsetsRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.ListOffsetsRequest)

	response := kmsg.NewListOffsetsResponse()
	response.SetVersion(request.GetVersion())
	if request.GetVersion() >= 2 {
		response.ThrottleMillis = 0
	}

	for _, t := range request.Topics {
		responseTopic := kmsg.NewListOffsetsResponseTopic()
		responseTopic.Topic = t.Topic

		topicPartitions, err := h.metastore.GetTopicPartitions(t.Topic)
		if err != nil {
			return nil, err
		}

		tpMap := make(map[int32]metastore.TopicPartition, len(topicPartitions))
		for _, tp := range topicPartitions {
			tpMap[tp.Partition] = tp
		}

		for _, p := range t.Partitions {
			responsePartition := kmsg.NewListOffsetsResponseTopicPartition()
			responsePartition.Partition = p.Partition

			tp, found := tpMap[p.Partition]
			if !found || len(topicPartitions) == 0 {
				responsePartition.ErrorCode = kerr.UnknownTopicOrPartition.Code
			} else {
				switch p.Timestamp {
				case -2: // earliest
					responsePartition.Offset = tp.StartOffset
				case -1: // latest
					responsePartition.Offset = tp.EndOffset
				case -3: // max timestamp
					responsePartition.Offset = tp.EndOffset
				default: // timestamp lookup (best-effort)
					responsePartition.Offset = tp.EndOffset
				}
				responsePartition.Timestamp = -1
				if request.GetVersion() >= 4 {
					responsePartition.LeaderEpoch = -1
				}
			}

			responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
		}

		response.Topics = append(response.Topics, responseTopic)
	}

	return &response, nil
}
