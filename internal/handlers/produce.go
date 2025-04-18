package handlers

import (
	"errors"

	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type ProduceRequestHandler struct {
	metastore metastore.Metastore
}

func NewProduceRequestHandler(m metastore.Metastore) *ProduceRequestHandler {
	return &ProduceRequestHandler{m}
}
func (h *ProduceRequestHandler) Key() kmsg.Key     { return kmsg.Produce }
func (h *ProduceRequestHandler) MinVersion() int16 { return 3 }
func (h *ProduceRequestHandler) MaxVersion() int16 { return 3 }
func (h *ProduceRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.ProduceRequest)
	if request.TransactionID != nil {
		return nil, errors.New("ProduceRequestHandler does not support transactional producers")
	}

	response := kmsg.NewProduceResponse()
	response.SetVersion(request.GetVersion())
	response.ThrottleMillis = 0

	for _, topic := range request.Topics {
		var topicResponse kmsg.ProduceResponseTopic
		topicResponse.Topic = topic.Topic
		for n, partition := range topic.Partitions {
			partitionResponse := kmsg.NewProduceResponseTopicPartition()
			partitionResponse.Partition = partition.Partition

			var recordBatch kmsg.RecordBatch
			if err := recordBatch.ReadFrom(partition.Records); err != nil {
				partitionResponse.ErrorCode = kerr.CorruptMessage.Code
			} else {
				log.Infof("record batch has %d records", recordBatch.NumRecords)
				partitionResponse.ErrorCode = 0
				partitionResponse.BaseOffset = int64(n)
			}
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		response.Topics = append(response.Topics, topicResponse)
	}

	return &response, nil
}
