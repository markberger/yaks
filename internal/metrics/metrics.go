package metrics

import (
	"context"
	"time"

	"github.com/markberger/yaks/internal/config"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Meter is the global OTel Meter used by all instrumented components.
// It defaults to a no-op meter and is initialized by Init.
var Meter metric.Meter = noop.NewMeterProvider().Meter("yaks")

// Init configures the global Meter from the given OTel config. When
// cfg.Enabled is false, Meter remains a no-op. Returns a shutdown function
// that flushes and closes the exporter.
func Init(ctx context.Context, cfg config.OTelConfig) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(cfg.Endpoint),
	)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(10*time.Second)),
		),
	)

	Meter = mp.Meter("yaks")

	shutdown := func(ctx context.Context) error {
		log.Info("Shutting down OTel MeterProvider...")
		return mp.Shutdown(ctx)
	}

	return shutdown, nil
}
