// Package metrics holds the Prometheus collectors shared by the API and worker roles.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsAccepted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingestor_events_accepted_total",
		Help: "Events accepted by the HTTP API.",
	}, []string{"mode"})

	EventsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingestor_events_rejected_total",
		Help: "Events rejected by validation or a failed write.",
	}, []string{"reason"})

	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ingestor_http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"route", "code"})

	EventsPersisted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_events_persisted_total",
		Help: "Events written to Postgres by the worker.",
	})

	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingestor_batch_size",
		Help:    "Number of events per Postgres flush.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})

	FlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingestor_flush_duration_seconds",
		Help:    "Time spent writing one batch to Postgres.",
		Buckets: prometheus.DefBuckets,
	})

	FlushErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_flush_errors_total",
		Help: "Failed Postgres flushes. Messages stay pending and are reclaimed later.",
	})

	Reclaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_messages_reclaimed_total",
		Help: "Stream messages reclaimed from crashed or slow consumers.",
	})

	DeadLettered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_dead_lettered_total",
		Help: "Messages moved to the dead-letter stream (undecodable, rejected by the database, or out of deliveries).",
	})

	BatchSplits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_batch_splits_total",
		Help: "Batches split in half to isolate a message the database rejects.",
	})

	Trimmed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_stream_trimmed_total",
		Help: "Acknowledged entries removed from the stream.",
	})

	BackpressureRejections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_backpressure_rejections_total",
		Help: "Requests rejected with 503 because the stream backlog was over the limit.",
	})

	StreamLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ingestor_stream_lag",
		Help: "Entries in the stream not yet delivered to the consumer group.",
	})

	StreamPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ingestor_stream_pending",
		Help: "Entries delivered to a worker but not yet acknowledged.",
	})

	StreamLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ingestor_stream_length",
		Help: "Entries kept in the stream (acknowledged entries are trimmed).",
	})
)
