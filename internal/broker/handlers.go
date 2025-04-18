package broker

import (
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type ApiVersionsRequestHandler struct {
	handlerRegistry *handlerRegistry
}

func NewApiVersionsRequestHandler(handlerRegistry *handlerRegistry) ApiVersionsRequestHandler {
	return ApiVersionsRequestHandler{handlerRegistry}
}
func (h ApiVersionsRequestHandler) Key() kmsg.Key     { return kmsg.ApiVersions }
func (h ApiVersionsRequestHandler) MinVersion() int16 { return 0 }
func (h ApiVersionsRequestHandler) MaxVersion() int16 { return 4 }
func (h ApiVersionsRequestHandler) Handle(r kmsg.Request) (kmsg.Response, error) {
	request := r.(*kmsg.ApiVersionsRequest)
	response := kmsg.NewApiVersionsResponse()
	response.SetVersion(request.GetVersion())

	// If client version is ahead of us, respond with UNSUPPORTED_VERSION and
	// the versions we support.
	if response.GetVersion() > h.MaxVersion() {
		response.ErrorCode = kerr.UnsupportedVersion.Code
		apiKey := kmsg.NewApiVersionsResponseApiKey()
		apiKey.ApiKey = int16(kmsg.ApiVersions)
		apiKey.MinVersion = h.MinVersion()
		apiKey.MaxVersion = h.MaxVersion()
		response.ApiKeys = append(response.ApiKeys, apiKey)
		return &response, nil
	}

	for key, versionToHandler := range h.handlerRegistry.handlerMap {
		// TODO: do not assume versions are contiguous
		versions := slices.Collect(maps.Keys(versionToHandler))
		apiKey := kmsg.NewApiVersionsResponseApiKey()
		apiKey.ApiKey = int16(key)
		apiKey.MinVersion = slices.Min(versions)
		apiKey.MaxVersion = slices.Max(versions)
		response.ApiKeys = append(response.ApiKeys, apiKey)
	}
	if response.GetVersion() >= 1 {
		response.SetThrottle(0)
	}
	return &response, nil
}
