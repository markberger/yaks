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

	// Identify all S3 files to download
	// TODO: respect byte size cuttoff when returning responses
	var batchesMetadata []metastore.RecordBatchV2
	for _, t := range request.Topics {
		for _, p := range t.Partitions {
			recordBatches, err := h.metastore.GetRecordBatchesV2(t.Topic, p.FetchOffset)
			if err != nil {
				return nil, err
			}

			batchesMetadata = append(batchesMetadata, recordBatches...)
		}

		// TODO: support multiple topics
		break
	}

	// TODO: support multiple topics
	responseTopic := kmsg.NewFetchResponseTopic()
	responseTopic.Topic = request.Topics[0].Topic
	responsePartition := kmsg.NewFetchResponseTopicPartition()
	responsePartition.Partition = 0
	responsePartition.ErrorCode = 0

	var recordBatches []byte
	for _, metadata := range batchesMetadata {
		data, err := h.downloadBatch(metadata.S3Key)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch batch %s: %w", metadata.S3Key, err)
		}

		// Set RecordBatch.FirstOffset for consumer
		binary.BigEndian.PutUint64(data, uint64(metadata.StartOffset))
		recordBatches = append(recordBatches, data...)
	}
	responsePartition.RecordBatches = recordBatches
	responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
	response.Topics = append(response.Topics, responseTopic)
	return &response, nil
}

func (h *FetchRequestHandler) downloadBatch(key string) ([]byte, error) {
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
	return data, nil
}
