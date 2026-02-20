package handlers

import (
	"errors"

	"github.com/markberger/yaks/internal/buffer"
	"github.com/markberger/yaks/internal/metastore"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type ProduceRequestHandler struct {
	metastore metastore.Metastore
	buffer    *buffer.WriteBuffer
}

func NewProduceRequestHandler(m metastore.Metastore, buf *buffer.WriteBuffer) *ProduceRequestHandler {
	return &ProduceRequestHandler{
		metastore: m,
		buffer:    buf,
	}
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

	// Track distinct flush promises to wait on
	promiseSet := make(map[*buffer.FlushPromise]struct{})

	for _, topic := range request.Topics {
		var topicResponse kmsg.ProduceResponseTopic
		topicResponse.Topic = topic.Topic

		metaTopic := h.metastore.GetTopicByName(topic.Topic)

		for _, partition := range topic.Partitions {
			partitionResponse := kmsg.NewProduceResponseTopicPartition()
			partitionResponse.Partition = partition.Partition

			// Error response if (1) topic is missing or (2) partition is out of range
			if metaTopic == nil || partition.Partition < 0 || partition.Partition >= metaTopic.NPartitions {
				log.WithFields(log.Fields{"topic": topic.Topic, "partition": partition.Partition, "metaTopic": metaTopic}).Info("Invalid TopicPartition")
				partitionResponse.ErrorCode = kerr.UnknownTopicOrPartition.Code
				topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
				continue
			}

			// Parse RecordBatch
			var recordBatch kmsg.RecordBatch
			if err := recordBatch.ReadFrom(partition.Records); err != nil {
				partitionResponse.ErrorCode = kerr.CorruptMessage.Code
				topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
				continue
			}

			// Submit to write buffer
			key := buffer.PartitionKey{
				TopicID:   metaTopic.ID,
				TopicName: topic.Topic,
				Partition: partition.Partition,
			}
			promise := h.buffer.Submit(key, partition.Records, int64(recordBatch.NumRecords))
			promiseSet[promise] = struct{}{}

			// Offset is assigned later by the materializer
			partitionResponse.BaseOffset = -1
			partitionResponse.ErrorCode = 0
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		response.Topics = append(response.Topics, topicResponse)
	}

	// Wait for all flush promises
	for promise := range promiseSet {
		if err := promise.Wait(); err != nil {
			return nil, err
		}
	}

	return &response, nil
}
