package broker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/markberger/yaks/internal/api"
	"github.com/markberger/yaks/internal/metrics"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Broker struct {
	NodeID          int32
	Host            string
	Port            int32
	AdvertisedHost  string
	AdvertisedPort  int32
	handlerRegistry *handlerRegistry
}

func NewBroker(nodeID int32, host string, port int32, advertisedHost string, advertisedPort int32) *Broker {
	b := Broker{
		NodeID:          nodeID,
		Host:            host,
		Port:            port,
		AdvertisedHost:  advertisedHost,
		AdvertisedPort:  advertisedPort,
		handlerRegistry: NewHandlerRegistry(),
	}
	b.Add(NewApiVersionsRequestHandler(b.handlerRegistry))
	return &b
}

// A wrapper function that simply calls HandlerRegistry.Add
func (b *Broker) Add(handler Handler) {
	b.handlerRegistry.Add(handler)
}

func (b *Broker) ListenAndServe(ctx context.Context) {
	// TCP listener
	address := fmt.Sprintf("%s:%d", b.Host, b.Port)
	log.WithFields(log.Fields{"host": b.Host, "port": b.Port}).Info("Listening...")
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// Shutdown goroutine because Accept blocks
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	// Accept connections and spawn goroutines to handle requests
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Info("TCP listener closed due to ctx cancel")
				return
			default:
				log.WithError(err).Error("Accept error")
				continue
			}
		}

		log.WithFields(log.Fields{"client": conn.RemoteAddr()}).Info("Connection accepted")
		go func() {
			b.handleConn(ctx, conn)
			log.WithFields(log.Fields{"client": conn.RemoteAddr()}).Info("Connection closed")
		}()
	}
}

// TODO: should return error for easier testing?
func (b *Broker) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	requestCount, _ := metrics.Meter.Int64Counter("yaks.requests")
	requestDuration, _ := metrics.Meter.Float64Histogram("yaks.request.duration", metric.WithUnit("s"))
	unhandledCount, _ := metrics.Meter.Int64Counter("yaks.requests.unhandled")

	sizeBuf := make([]byte, 4)
	for {
		// Check context before proceeding
		select {
		case <-ctx.Done():
			log.WithField("client", conn.RemoteAddr()).Info("ctx signaled - closing connection")
			return
		default:
		}

		// Read from connection
		if _, err := io.ReadFull(reader, sizeBuf); err != nil {
			if err == io.EOF {
				log.WithField("client", conn.RemoteAddr()).Info("Received EOF - client closed connection")
				return
			}
			log.Error("failed to read msg size: ", err)
			return
		}
		size := binary.BigEndian.Uint32(sizeBuf)
		body := make([]byte, size)
		_, err := io.ReadFull(reader, body)
		if err != nil {
			log.Error("failed to read request body from client connection")
			return
		}

		// Parse the bytes into api.RequestHeader
		reader := kbin.Reader{Src: body}
		request := &api.ClientRequest{}
		err = request.ReadFrom(&reader)
		if err != nil {
			log.Error("failed to parse request header: ", err)
			return
		}
		log.WithFields(
			log.Fields{
				"client":        conn.RemoteAddr(),
				"clientID":      request.ClientID(),
				"key":           request.KeyName(),
				"version":       request.Version(),
				"correlationID": request.CorrelationID(),
			},
		).Info("Received request")

		// Run the appropriate Handler to generate a kmsg.Response
		handler := b.handlerRegistry.Get(request.Body())
		apiKey := kmsg.NameForKey(request.Body().Key())
		apiVersion := strconv.Itoa(int(request.Version()))
		if handler == nil {
			log.Error("failed to find appropriate handler")
			unhandledCount.Add(ctx, 1, metric.WithAttributes(
				attribute.String("api_key", apiKey),
			))
			return
		}

		start := time.Now()
		response, err := handler.Handle(request.Body())
		duration := time.Since(start)

		attrs := []attribute.KeyValue{
			attribute.String("api_key", apiKey),
			attribute.String("api_version", apiVersion),
		}
		requestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
		if err != nil {
			log.Errorf("handler returned an error: %v", err)
			requestCount.Add(ctx, 1, metric.WithAttributes(append(attrs, attribute.Bool("success", false))...))
			return
		}

		errorCode := extractErrorCode(response)
		successAttrs := append(attrs, attribute.Bool("success", true))
		if errorCode != 0 {
			successAttrs = append(successAttrs, attribute.String("error_code", strconv.Itoa(int(errorCode))))
		}
		requestCount.Add(ctx, 1, metric.WithAttributes(successAttrs...))

		// Serialize the response and send it to the client
		// TODO: check return value of conn.Write
		dst := make([]byte, 0)
		dst = AppendResponse(dst, response, request.CorrelationID())
		_, err = conn.Write(dst)
		if err != nil {
			log.Errorf("Error encountered when writing response: %v", err)
			return
		}
		log.WithFields(
			log.Fields{
				"client":        conn.RemoteAddr(),
				"key":           request.KeyName(),
				"version":       request.Version(),
				"correlationID": request.CorrelationID(),
			},
		).Info("Response sent")
	}
}

// TODO: check what kafka behavior is for these
// extractErrorCode returns the top-level ErrorCode from response types that
// have one. Produce and Fetch have per-partition errors only, so return 0.
func extractErrorCode(resp kmsg.Response) int16 {
	switch r := resp.(type) {
	case *kmsg.ApiVersionsResponse:
		return r.ErrorCode
	case *kmsg.FindCoordinatorResponse:
		return r.ErrorCode
	case *kmsg.CreateTopicsResponse:
		// CreateTopics has per-topic errors, no top-level code
		return 0
	default:
		return 0
	}
}
