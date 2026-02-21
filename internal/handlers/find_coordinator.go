package handlers

import (
	"github.com/markberger/yaks/internal/broker"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type FindCoordinatorRequestHandler struct {
	broker *broker.Broker
}

func NewFindCoordinatorRequestHandler(b *broker.Broker) *FindCoordinatorRequestHandler {
	return &FindCoordinatorRequestHandler{b}
}

func (h *FindCoordinatorRequestHandler) Key() kmsg.Key     { return kmsg.FindCoordinator }
func (h *FindCoordinatorRequestHandler) MinVersion() int16  { return 0 }
func (h *FindCoordinatorRequestHandler) MaxVersion() int16  { return 2 }
func (h *FindCoordinatorRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.FindCoordinatorRequest)

	response := kmsg.NewFindCoordinatorResponse()
	response.SetVersion(request.GetVersion())
	if request.GetVersion() >= 1 {
		response.ThrottleMillis = 0
	}
	response.NodeID = h.broker.NodeID
	response.Host = h.broker.AdvertisedHost
	response.Port = h.broker.AdvertisedPort

	return &response, nil
}
