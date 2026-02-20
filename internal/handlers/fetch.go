package handlers

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/markberger/yaks/internal/metastore"
	"github.com/markberger/yaks/internal/s3_client"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type FetchRequestHandler struct {
	metastore  metastore.Metastore
	s3Client   s3_client.S3Client
	bucketName string
}

func NewFetchRequestHandler(metastore metastore.Metastore, s3Client s3_client.S3Client, bucketName string) *FetchRequestHandler {
	return &FetchRequestHandler{metastore, s3Client, bucketName}
}

func (h *FetchRequestHandler) Key() kmsg.Key     { return kmsg.Fetch }
func (h *FetchRequestHandler) MinVersion() int16 { return 3 }
func (h *FetchRequestHandler) MaxVersion() int16 { return 3 }
func (h *FetchRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.FetchRequest)

	response := kmsg.NewFetchResponse()
	response.SetVersion(request.GetVersion())
	response.ThrottleMillis = 0

	// TODO: respect byte size cutoff when returning responses
	for _, t := range request.Topics {
		responseTopic := kmsg.NewFetchResponseTopic()
		responseTopic.Topic = t.Topic

		for _, p := range t.Partitions {
			batchesMetadata, err := h.metastore.GetRecordBatchesV2(t.Topic, p.Partition, p.FetchOffset)
			if err != nil {
				return nil, err
			}

			responsePartition := kmsg.NewFetchResponseTopicPartition()
			responsePartition.Partition = p.Partition
			responsePartition.ErrorCode = 0

			var recordBatches []byte
			for _, metadata := range batchesMetadata {
				data, err := h.downloadBatch(metadata.S3Key, metadata.ByteOffset, metadata.ByteLength)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch batch %s: %w", metadata.S3Key, err)
				}

				// Set RecordBatch.FirstOffset for consumer
				binary.BigEndian.PutUint64(data, uint64(metadata.StartOffset))
				recordBatches = append(recordBatches, data...)
			}
			responsePartition.RecordBatches = recordBatches
			responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
		}

		response.Topics = append(response.Topics, responseTopic)
		// TODO: support multiple topics
		break
	}
	return &response, nil
}

func (h *FetchRequestHandler) downloadBatch(key string, byteOffset, byteLength int64) ([]byte, error) {
	resp, err := h.s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &h.bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}
	return data[byteOffset : byteOffset+byteLength], nil
}
