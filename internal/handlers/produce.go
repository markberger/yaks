package handlers

import (
	"errors"
	"fmt"

	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
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
		for _, partition := range topic.Partitions {
			var recordBatch kmsg.RecordBatch
			if err := recordBatch.ReadFrom(partition.Records); err != nil {
				return nil, fmt.Errorf("failed to read RecordBatch: %v", err)
			}

			log.Infof("record batch has %d records", recordBatch.NumRecords)
			// partitionResponse := kmsg.NewProduceResponseTopicPartition()
			// partitionResponse.Partition = partition.Partition
			// partitionResponse.BaseOffset = 0
			// topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
	}

	return &response, nil
}
