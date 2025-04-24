package handlers

import (
	"github.com/markberger/yaks/internal/metastore"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type CreateTopicsRequestHandler struct {
	metastore metastore.Metastore
}

func NewCreateTopicsRequestHandler(m metastore.Metastore) *CreateTopicsRequestHandler {
	return &CreateTopicsRequestHandler{m}
}
func (h *CreateTopicsRequestHandler) Key() kmsg.Key     { return kmsg.CreateTopics }
func (h *CreateTopicsRequestHandler) MinVersion() int16 { return 2 }
func (h *CreateTopicsRequestHandler) MaxVersion() int16 { return 4 }
func (h *CreateTopicsRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	// TODO: respect ValidateOnly flag
	request := r.(*kmsg.CreateTopicsRequest)
	response := kmsg.NewCreateTopicsResponse()
	response.SetVersion(request.GetVersion())
	response.ThrottleMillis = 0

	for _, t := range request.Topics {
		err := h.metastore.CreateTopic(t.Topic)

		var errCode int16 = 0
		if err != nil {
			errCode = kerr.KafkaStorageError.Code
		}
		response.Topics = append(response.Topics, kmsg.CreateTopicsResponseTopic{
			Topic:             t.Topic,
			ErrorCode:         errCode,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}

	return &response, nil
}
