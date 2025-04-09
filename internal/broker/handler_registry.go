package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Defines the interface for implementing server handlers
type Handler interface {
	Key() kmsg.Key
	MinVersion() int16
	MaxVersion() int16
	Handle(kmsg.Request) (kmsg.Response, error)
}

// The HandleRegistry holds all state necessary so we can easily map from
// Request -> HandleFunc
type handlerRegistry struct {
	// TODO: change to arrays for faster accessing
	handlerMap map[kmsg.Key]map[int16]Handler
}

func NewHandlerRegistry() *handlerRegistry {
	return &handlerRegistry{make(map[kmsg.Key]map[int16]Handler)}
}

// Returns the appropriate Handler or nil if it does not exist
func (r *handlerRegistry) Get(request kmsg.Request) Handler {
	key := kmsg.Key(request.Key())
	version := request.GetVersion()
	if _, ok := r.handlerMap[key]; !ok {
		return nil
	}

	if h, ok := r.handlerMap[key][version]; !ok {
		return nil
	} else {
		return h
	}
}

// Register a Handler with the registry. Calls for the same (key, version)
// pair will only retain the latest entry.
func (r *handlerRegistry) Add(handler Handler) {
	key := handler.Key()
	if r.handlerMap[key] == nil {
		r.handlerMap[key] = make(map[int16]Handler)
	}

	minVersion := handler.MinVersion()
	maxVersion := handler.MaxVersion()
	for version := minVersion; version <= maxVersion; version++ {
		r.handlerMap[key][version] = handler
	}
}
