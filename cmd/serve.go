package cmd

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/TFMV/roar/duckdb"
	"github.com/TFMV/roar/flight"
	"github.com/TFMV/roar/kafka"
	"github.com/TFMV/roar/pkg"
	"github.com/TFMV/roar/stream"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	brokers        string
	topics         string
	schemaRegistry string
	batchSize      int
	ttl            string
	bufferLimit    string
	flightPort     int
	persist        bool
	dbPath         string
	metricsAddr    string
	pprofAddr      string
	enableMetrics  bool
	enablePprof    bool
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the Roar server",
	Long: `Start the Roar server that consumes from Kafka topics, 
converts messages to Arrow RecordBatches, and serves them via Arrow Flight.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ttlDuration, err := time.ParseDuration(ttl)
		if err != nil {
			return err
		}

		// Parse topics
		topicsList := strings.Split(topics, ",")
		for i, topic := range topicsList {
			topicsList[i] = strings.TrimSpace(topic)
		}

		// Initialize metrics if enabled
		metrics := pkg.NewMetrics()
		if enableMetrics {
			pkg.StartMetricsServer(metricsAddr)
			log.Info().Str("addr", metricsAddr).Msg("Metrics server enabled")
		}

		// Start profiling if enabled
		if enablePprof {
			pkg.StartProfiling(pprofAddr)
			log.Info().Str("addr", pprofAddr).Msg("Profiling server enabled")
		}

		// Initialize the stream manager
		streamMgr, err := stream.NewManager(ttlDuration, bufferLimit)
		if err != nil {
			return err
		}

		// Start the Flight server
		flightServer, err := flight.NewServer(streamMgr, flightPort)
		if err != nil {
			return err
		}

		// Start consuming from Kafka
		kafkaConsumer, err := kafka.NewConsumer(brokers, topicsList, schemaRegistry, batchSize, streamMgr)
		if err != nil {
			return err
		}

		// Initialize DuckDB sink if persistence is enabled
		var duckSink *duckdb.Sink
		if persist {
			duckSink, err = duckdb.NewSink(dbPath, streamMgr)
			if err != nil {
				return err
			}
			defer duckSink.Close()
		}

		// Create a cancellable context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Set up signal handling
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			<-sigChan
			log.Info().Msg("Received shutdown signal, gracefully stopping server...")
			cancel()
		}()

		// Start Kafka consumer
		if err := kafkaConsumer.Start(ctx); err != nil {
			return err
		}
		defer kafkaConsumer.Stop()

		// Start DuckDB sink if enabled
		if persist && duckSink != nil {
			if err := duckSink.Start(ctx); err != nil {
				return err
			}
		}

		// Start monitoring active streams for metrics
		if enableMetrics {
			go monitorStreams(ctx, streamMgr, metrics)
		}

		// Log server configuration
		log.Info().
			Str("brokers", brokers).
			Str("topics", topics).
			Int("batch_size", batchSize).
			Str("ttl", ttl).
			Str("buffer_limit", bufferLimit).
			Int("port", flightPort).
			Bool("persist", persist).
			Bool("metrics", enableMetrics).
			Bool("pprof", enablePprof).
			Msg("Starting Roar server")

		// Start the Flight server (blocking call)
		return flightServer.Serve(ctx)
	},
}

// monitorStreams periodically updates metrics about streams
func monitorStreams(ctx context.Context, streamMgr *stream.Manager, metrics *pkg.Metrics) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get overall metrics from stream manager
			mgrMetrics := streamMgr.GetMetrics()
			metrics.SetActiveStreams(int(mgrMetrics["active_streams"].(int32)))

			// Log overall stream metrics
			log.Debug().
				Interface("stream_manager_metrics", mgrMetrics).
				Msg("Stream manager metrics")

			// Get metrics for each stream
			topics := streamMgr.ListStreams()
			for _, topic := range topics {
				stream := streamMgr.GetStream(topic)
				streamMetrics := stream.GetStreamMetrics()

				// Memory metrics
				currentSize := streamMetrics["current_size"].(uint64)
				metrics.SetStreamMemory(topic, int64(currentSize))

				// Calculate memory percent if possible
				if maxBytes, ok := streamMetrics["max_bytes"].(uint64); ok && maxBytes > 0 {
					memPercent := float64(currentSize) / float64(maxBytes) * 100.0
					metrics.SetStreamMemoryPercent(topic, memPercent)
				}

				// Buffer utilization
				if currentItems, ok := streamMetrics["current_queue"].(uint64); ok {
					if maxQueue, ok := streamMetrics["max_queue_length"].(uint64); ok && maxQueue > 0 {
						bufferPercent := float64(currentItems) / float64(maxQueue) * 100.0
						metrics.SetStreamBufferUtilization(topic, bufferPercent)
					}
				}

				// Track dropped records
				if recordsDropped, ok := streamMetrics["records_dropped"].(uint64); ok {
					metrics.IncStreamRecordsDropped(topic, int64(recordsDropped))
				}

				// Track processed records
				if recordsProcessed, ok := streamMetrics["records_processed"].(uint64); ok {
					metrics.IncStreamRecordsProcessed(topic, int64(recordsProcessed))
				}

				// Log detailed stream metrics
				log.Debug().
					Str("topic", topic).
					Interface("stream_metrics", streamMetrics).
					Msg("Stream metrics")
			}
		}
	}
}

func init() {
	rootCmd.AddCommand(serveCmd)

	// Add flags
	serveCmd.Flags().StringVar(&brokers, "brokers", "localhost:9092", "Kafka bootstrap servers")
	serveCmd.Flags().StringVar(&topics, "topics", "", "Comma-separated list of topics")
	serveCmd.Flags().StringVar(&schemaRegistry, "schema-registry", "", "Avro/Protobuf schema registry URI")
	serveCmd.Flags().IntVar(&batchSize, "batch-size", 1024, "Max records per RecordBatch")
	serveCmd.Flags().StringVar(&ttl, "ttl", "60s", "Stream TTL (e.g. 60s, 5m)")
	serveCmd.Flags().StringVar(&bufferLimit, "buffer-limit", "100MB", "Max in-memory bytes per stream")
	serveCmd.Flags().IntVar(&flightPort, "port", 8080, "Arrow Flight server port")
	serveCmd.Flags().BoolVar(&persist, "persist", false, "Whether to write to DuckDB")
	serveCmd.Flags().StringVar(&dbPath, "db-path", "roar.duckdb", "Path to DuckDB database file")
	serveCmd.Flags().BoolVar(&enableMetrics, "metrics", false, "Enable Prometheus metrics")
	serveCmd.Flags().StringVar(&metricsAddr, "metrics-addr", ":9090", "Metrics server address")
	serveCmd.Flags().BoolVar(&enablePprof, "pprof", false, "Enable pprof profiling")
	serveCmd.Flags().StringVar(&pprofAddr, "pprof-addr", ":6060", "Pprof server address")

	// Mark topics as required
	serveCmd.MarkFlagRequired("topics")
}
