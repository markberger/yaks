package broker

import (
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestApiVersionsRequestHandler_Handle(t *testing.T) {
	tests := []struct {
		name            string
		handlerRegistry *handlerRegistry
		want            kmsg.ApiVersionsResponse
		wantErr         bool
	}{
		{
			name:            "empty handler registry",
			handlerRegistry: NewHandlerRegistry(),
			want:            kmsg.NewApiVersionsResponse(),
			wantErr:         false,
		},
		{
			name: "non-empty handler registry",
			handlerRegistry: func() *handlerRegistry {
				r := NewHandlerRegistry()
				handler := mockHandler{
					key:        kmsg.Produce,
					minVersion: 2,
					maxVersion: 5,
				}
				r.Add(handler)
				return r
			}(),
			want: func() kmsg.ApiVersionsResponse {
				r := kmsg.NewApiVersionsResponse()
				r.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
					{
						ApiKey:     int16(kmsg.Produce),
						MinVersion: 2,
						MaxVersion: 5,
					},
				}
				return r
			}(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check handler properties
			h := NewApiVersionsRequestHandler(tt.handlerRegistry)
			if got, want := h.MinVersion(), int16(0); got != want {
				t.Errorf("unexpected MinVersion: got=%v, want=%v", got, want)
			}
			if got, want := h.MaxVersion(), int16(4); got != want {
				t.Errorf("unexpected MaxVersion: got=%v, want=%v", got, want)
			}
			if got, want := h.Key(), kmsg.ApiVersions; got != want {
				t.Errorf("unexpected MaxVersion: got=%v, want=%v", got, want)
			}

			// Check handler output
			request := &kmsg.ApiVersionsRequest{} // The request doesn't matter for this test
			got, err := h.Handle(request)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApiVersionsRequestHandler.Handle() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, &tt.want) {
				t.Errorf("ApiVersionsRequestHandler.Handle() = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockHandler struct {
	key        kmsg.Key
	minVersion int16
	maxVersion int16
}

func (m mockHandler) Key() kmsg.Key {
	return m.key
}

func (m mockHandler) MinVersion() int16 {
	return m.minVersion
}

func (m mockHandler) MaxVersion() int16 {
	return m.maxVersion
}

func (m mockHandler) Handle(kmsg.Request) (kmsg.Response, error) {
	return nil, nil
}
