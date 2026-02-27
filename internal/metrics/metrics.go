package metrics

import (
	"net/http"

	"github.com/markberger/yaks/internal/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Meter is the global OTel Meter used by all instrumented components.
// It defaults to a no-op meter and is initialized by Init.
var Meter metric.Meter = noop.NewMeterProvider().Meter("yaks")

// Init configures the global Meter from the given OTel config. When
// cfg.Enabled is false, Meter remains a no-op and Handler returns nil.
// The returned http.Handler serves the Prometheus /metrics endpoint.
func Init(cfg config.OTelConfig) (http.Handler, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	exporter, err := prometheus.New()
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram, Unit: "s"},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{
						0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
					},
				},
			},
		)),
	)

	Meter = mp.Meter("yaks")

	return promhttp.Handler(), nil
}
