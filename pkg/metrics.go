package pkg

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	registry   = prometheus.NewRegistry()
	registerer = promauto.With(registry)
	once       sync.Once
)

// Metrics exposes Prometheus metrics for Roar
type Metrics struct {
	// Kafka metrics
	kafkaMessagesTotal   *prometheus.CounterVec
	kafkaMessagesErrors  *prometheus.CounterVec
	kafkaMessagesLag     *prometheus.GaugeVec
	kafkaMessagesPending *prometheus.GaugeVec
	kafkaMessagesDropped *prometheus.CounterVec
	kafkaBytesReceived   *prometheus.CounterVec

	// Stream metrics
	recordBatchesCreated    *prometheus.CounterVec
	recordBatchesSent       *prometheus.CounterVec
	recordBatchesDropped    *prometheus.CounterVec
	activeStreams           prometheus.Gauge
	expiredStreams          prometheus.Counter
	streamMemoryBytes       *prometheus.GaugeVec
	streamMemoryPercent     *prometheus.GaugeVec
	streamBackPressure      *prometheus.GaugeVec
	streamBufferUtilization *prometheus.GaugeVec
	streamRecordsProcessed  *prometheus.CounterVec
	streamRecordsDropped    *prometheus.CounterVec
	processingLatency       *prometheus.HistogramVec

	// Flight metrics
	flightStreamRequests  *prometheus.CounterVec
	flightStreamingErrors *prometheus.CounterVec

	// DuckDB metrics
	duckdbInsertRows    *prometheus.CounterVec
	duckdbInsertErrors  *prometheus.CounterVec
	duckdbInsertLatency *prometheus.HistogramVec
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	m := &Metrics{
		// Kafka metrics
		kafkaMessagesTotal: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_kafka_messages_total",
				Help: "Total number of Kafka messages processed by topic",
			},
			[]string{"topic"},
		),
		kafkaMessagesErrors: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_kafka_messages_errors_total",
				Help: "Total number of Kafka message processing errors by topic",
			},
			[]string{"topic", "error_type"},
		),
		kafkaMessagesLag: registerer.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "roar_kafka_messages_lag_seconds",
				Help: "Current lag in seconds between message timestamp and processing time",
			},
			[]string{"topic"},
		),
		kafkaMessagesPending: registerer.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "roar_kafka_messages_pending",
				Help: "Number of messages waiting to be processed in the buffer",
			},
			[]string{"topic"},
		),
		kafkaMessagesDropped: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_kafka_messages_dropped_total",
				Help: "Total number of Kafka messages dropped due to back pressure",
			},
			[]string{"topic", "reason"},
		),
		kafkaBytesReceived: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_kafka_bytes_received_total",
				Help: "Total number of bytes received from Kafka",
			},
			[]string{"topic"},
		),

		// Stream metrics
		recordBatchesCreated: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_record_batches_created_total",
				Help: "Total number of Arrow RecordBatches created by topic",
			},
			[]string{"topic"},
		),
		recordBatchesSent: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_record_batches_sent_total",
				Help: "Total number of Arrow RecordBatches sent via Flight by topic",
			},
			[]string{"topic"},
		),
		recordBatchesDropped: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_record_batches_dropped_total",
				Help: "Total number of Arrow RecordBatches dropped due to TTL or memory limits",
			},
			[]string{"topic", "reason"},
		),
		activeStreams: registerer.NewGauge(
			prometheus.GaugeOpts{
				Name: "roar_active_streams",
				Help: "Current number of active streams",
			},
		),
		expiredStreams: registerer.NewCounter(
			prometheus.CounterOpts{
				Name: "roar_expired_streams_total",
				Help: "Total number of expired streams",
			},
		),
		streamMemoryBytes: registerer.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "roar_stream_memory_bytes",
				Help: "Current memory usage by stream in bytes",
			},
			[]string{"topic"},
		),
		streamMemoryPercent: registerer.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "roar_stream_memory_percent",
				Help: "Current memory usage by stream as percentage of limit",
			},
			[]string{"topic"},
		),
		streamBackPressure: registerer.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "roar_stream_back_pressure",
				Help: "Indicates if back pressure is being applied (1) or not (0)",
			},
			[]string{"topic"},
		),
		streamBufferUtilization: registerer.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "roar_stream_buffer_utilization_percent",
				Help: "Current buffer utilization as percentage",
			},
			[]string{"topic"},
		),
		streamRecordsProcessed: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_stream_records_processed_total",
				Help: "Total number of records processed by stream",
			},
			[]string{"topic"},
		),
		streamRecordsDropped: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_stream_records_dropped_total",
				Help: "Total number of records dropped by stream due to memory limits",
			},
			[]string{"topic"},
		),
		processingLatency: registerer.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "roar_processing_latency_seconds",
				Help:    "Latency of message processing from Kafka to Arrow RecordBatch",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
			},
			[]string{"topic"},
		),

		// Flight metrics
		flightStreamRequests: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_flight_stream_requests_total",
				Help: "Total number of Flight stream requests by topic",
			},
			[]string{"topic"},
		),
		flightStreamingErrors: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_flight_streaming_errors_total",
				Help: "Total number of Flight streaming errors by topic",
			},
			[]string{"topic", "error_type"},
		),

		// DuckDB metrics
		duckdbInsertRows: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_duckdb_insert_rows_total",
				Help: "Total number of rows inserted into DuckDB by topic",
			},
			[]string{"topic"},
		),
		duckdbInsertErrors: registerer.NewCounterVec(
			prometheus.CounterOpts{
				Name: "roar_duckdb_insert_errors_total",
				Help: "Total number of DuckDB insert errors by topic",
			},
			[]string{"topic", "error_type"},
		),
		duckdbInsertLatency: registerer.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "roar_duckdb_insert_latency_seconds",
				Help:    "Latency of inserting batches into DuckDB",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
			},
			[]string{"topic"},
		),
	}

	return m
}

// StartMetricsServer starts the HTTP server for Prometheus metrics
func StartMetricsServer(addr string) {
	once.Do(func() {
		// Create a new HTTP server
		http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

		go func() {
			log.Info().Str("addr", addr).Msg("Starting metrics server")
			if err := http.ListenAndServe(addr, nil); err != nil {
				log.Error().Err(err).Msg("Metrics server error")
			}
		}()
	})
}

// Kafka metrics
// IncKafkaMessages increments the Kafka messages counter
func (m *Metrics) IncKafkaMessages(topic string, count int) {
	m.kafkaMessagesTotal.WithLabelValues(topic).Add(float64(count))
}

// IncKafkaErrors increments the Kafka errors counter
func (m *Metrics) IncKafkaErrors(topic, errorType string) {
	m.kafkaMessagesErrors.WithLabelValues(topic, errorType).Inc()
}

// SetKafkaLag sets the lag between message timestamp and processing time
func (m *Metrics) SetKafkaLag(topic string, lag time.Duration) {
	m.kafkaMessagesLag.WithLabelValues(topic).Set(lag.Seconds())
}

// SetKafkaPending sets the number of messages pending processing
func (m *Metrics) SetKafkaPending(topic string, count int) {
	m.kafkaMessagesPending.WithLabelValues(topic).Set(float64(count))
}

// IncKafkaDropped increments the counter for dropped Kafka messages
func (m *Metrics) IncKafkaDropped(topic, reason string) {
	m.kafkaMessagesDropped.WithLabelValues(topic, reason).Inc()
}

// IncKafkaBytes increments the counter for bytes received from Kafka
func (m *Metrics) IncKafkaBytes(topic string, bytes int) {
	m.kafkaBytesReceived.WithLabelValues(topic).Add(float64(bytes))
}

// Stream metrics
// IncRecordBatchesCreated increments the record batches created counter
func (m *Metrics) IncRecordBatchesCreated(topic string) {
	m.recordBatchesCreated.WithLabelValues(topic).Inc()
}

// IncRecordBatchesSent increments the record batches sent counter
func (m *Metrics) IncRecordBatchesSent(topic string) {
	m.recordBatchesSent.WithLabelValues(topic).Inc()
}

// IncRecordBatchesDropped increments the record batches dropped counter
func (m *Metrics) IncRecordBatchesDropped(topic, reason string) {
	m.recordBatchesDropped.WithLabelValues(topic, reason).Inc()
}

// SetActiveStreams sets the active streams gauge
func (m *Metrics) SetActiveStreams(count int) {
	m.activeStreams.Set(float64(count))
}

// IncExpiredStreams increments the expired streams counter
func (m *Metrics) IncExpiredStreams() {
	m.expiredStreams.Inc()
}

// SetStreamMemory sets the stream memory gauge
func (m *Metrics) SetStreamMemory(topic string, bytes int64) {
	m.streamMemoryBytes.WithLabelValues(topic).Set(float64(bytes))
}

// SetStreamMemoryPercent sets the stream memory percentage gauge
func (m *Metrics) SetStreamMemoryPercent(topic string, percent float64) {
	m.streamMemoryPercent.WithLabelValues(topic).Set(percent)
}

// SetStreamBackPressure indicates if back pressure is being applied
func (m *Metrics) SetStreamBackPressure(topic string, active bool) {
	value := 0.0
	if active {
		value = 1.0
	}
	m.streamBackPressure.WithLabelValues(topic).Set(value)
}

// SetStreamBufferUtilization sets the buffer utilization percentage
func (m *Metrics) SetStreamBufferUtilization(topic string, percent float64) {
	m.streamBufferUtilization.WithLabelValues(topic).Set(percent)
}

// IncStreamRecordsProcessed increments the records processed counter
func (m *Metrics) IncStreamRecordsProcessed(topic string, count int64) {
	m.streamRecordsProcessed.WithLabelValues(topic).Add(float64(count))
}

// IncStreamRecordsDropped increments the records dropped counter
func (m *Metrics) IncStreamRecordsDropped(topic string, count int64) {
	m.streamRecordsDropped.WithLabelValues(topic).Add(float64(count))
}

// ObserveProcessingLatency records the processing latency
func (m *Metrics) ObserveProcessingLatency(topic string, duration time.Duration) {
	m.processingLatency.WithLabelValues(topic).Observe(duration.Seconds())
}

// Flight metrics
// IncFlightRequests increments the Flight requests counter
func (m *Metrics) IncFlightRequests(topic string) {
	m.flightStreamRequests.WithLabelValues(topic).Inc()
}

// IncFlightErrors increments the Flight errors counter
func (m *Metrics) IncFlightErrors(topic, errorType string) {
	m.flightStreamingErrors.WithLabelValues(topic, errorType).Inc()
}

// DuckDB metrics
// IncDuckDBRows increments the DuckDB insert rows counter
func (m *Metrics) IncDuckDBRows(topic string, count int64) {
	m.duckdbInsertRows.WithLabelValues(topic).Add(float64(count))
}

// IncDuckDBErrors increments the DuckDB errors counter
func (m *Metrics) IncDuckDBErrors(topic, errorType string) {
	m.duckdbInsertErrors.WithLabelValues(topic, errorType).Inc()
}

// ObserveDuckDBLatency records the DuckDB insert latency
func (m *Metrics) ObserveDuckDBLatency(topic string, duration time.Duration) {
	m.duckdbInsertLatency.WithLabelValues(topic).Observe(duration.Seconds())
}
