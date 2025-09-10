package api

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Represents a single request sent by the client. This is mostly
// a wrapper struct around kmsg.Request since franz-go does not have
// a reason to include this representation.
type ClientRequest struct {
	key           int16
	version       int16
	correlationID int32
	clientID      string
	tags          kmsg.Tags
	request       kmsg.Request
}

func (r *ClientRequest) Key() int16           { return r.key }
func (r *ClientRequest) KeyName() string      { return kmsg.NameForKey(r.key) }
func (r *ClientRequest) Version() int16       { return r.version }
func (r *ClientRequest) CorrelationID() int32 { return r.correlationID }
func (r *ClientRequest) ClientID() string     { return r.clientID }
func (r *ClientRequest) Body() kmsg.Request   { return r.request }

func (r *ClientRequest) ReadFrom(b *kbin.Reader) error {
	r.key = b.Int16()
	r.version = b.Int16()
	r.correlationID = b.Int32()
	s := b.NullableString()
	if s != nil {
		r.clientID = *s
	}

	// Determine whether request is "flexible" i.e. supports _tagged_fields
	// https://kafka.apache.org/protocol#protocol_messages
	request := kmsg.RequestForKey(r.key)
	if request == nil {
		return fmt.Errorf("api key has no associated request: %d", r.key)
	}
	request.SetVersion(r.version)
	if request.IsFlexible() {
		r.tags = kmsg.ReadTags(b)
	}

	if err := request.ReadFrom(b.Src); err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}
	r.request = request
	return nil
}
