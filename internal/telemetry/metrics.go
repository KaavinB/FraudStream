package telemetry

import "github.com/prometheus/client_golang/prometheus"

var (
	Events   = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fraudstream_events_total", Help: "Events by stage and outcome"}, []string{"stage", "outcome"})
	Duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "fraudstream_stage_seconds", Help: "Measured wall clock duration by stage", Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30}}, []string{"stage"})
	Lag      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "fraudstream_consumer_lag", Help: "High watermark minus next committed offset (sampled while polling)"}, []string{"topic", "partition"})
)

func init() { prometheus.MustRegister(Events, Duration, Lag) }
