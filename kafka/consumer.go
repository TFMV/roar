package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TFMV/roar/stream"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

// ConsumerMetrics tracks various consumer metrics
type ConsumerMetrics struct {
	MessagesReceived      atomic.Uint64
	MessagesDropped       atomic.Uint64
	BytesReceived         atomic.Uint64
	BatchesCreated        atomic.Uint64
	MessageProcessingTime atomic.Uint64 // nanoseconds
	MessagesPending       atomic.Int64  // Messages waiting to be processed
	SchemaInferenceCount  atomic.Uint64
	LastBatchCreationTime atomic.Int64
	MessageLag            atomic.Int64 // Estimated lag in messages
	TopicPartitionCount   map[string]int
}

// Consumer is a Kafka consumer that reads messages and converts them to Arrow RecordBatches
type Consumer struct {
	readers           map[string]*kafka.Reader
	streamMgr         *stream.Manager
	batchSize         int
	allocator         memory.Allocator
	topics            []string
	brokers           []string
	schemaReg         string
	groupID           string
	wg                sync.WaitGroup
	cancel            context.CancelFunc
	mu                sync.Mutex
	messageChan       chan kafka.Message
	messageChanSize   int
	processingWorkers int
	metrics           *ConsumerMetrics
	converters        map[string]MessageConverter
}

// ConsumerOption provides functional options for Consumer
type ConsumerOption func(*Consumer)

// WithGroupID sets the Kafka consumer group ID
func WithGroupID(groupID string) ConsumerOption {
	return func(c *Consumer) {
		c.groupID = groupID
	}
}

// WithMessageBuffer sets the size of the message buffer channel
func WithMessageBuffer(size int) ConsumerOption {
	return func(c *Consumer) {
		c.messageChanSize = size
	}
}

// WithProcessingWorkers sets the number of workers processing messages
func WithProcessingWorkers(workers int) ConsumerOption {
	return func(c *Consumer) {
		c.processingWorkers = workers
	}
}

// WithConverter registers a custom message converter for a specific topic
func WithConverter(topic string, converter MessageConverter) ConsumerOption {
	return func(c *Consumer) {
		if c.converters == nil {
			c.converters = make(map[string]MessageConverter)
		}
		c.converters[topic] = converter
	}
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(brokers string, topics []string, schemaRegistry string, batchSize int, streamMgr *stream.Manager, opts ...ConsumerOption) (*Consumer, error) {
	// Parse brokers string into slice
	brokersList := strings.Split(brokers, ",")
	for i := range brokersList {
		brokersList[i] = strings.TrimSpace(brokersList[i])
	}

	consumer := &Consumer{
		readers:           make(map[string]*kafka.Reader),
		streamMgr:         streamMgr,
		batchSize:         batchSize,
		allocator:         memory.NewGoAllocator(),
		topics:            topics,
		brokers:           brokersList,
		schemaReg:         schemaRegistry,
		groupID:           "roar-consumer",
		messageChanSize:   100000, // Default to 100k message buffer
		processingWorkers: 10,     // Default to 10 workers
		metrics: &ConsumerMetrics{
			TopicPartitionCount: make(map[string]int),
		},
		converters: make(map[string]MessageConverter),
	}

	// Apply options
	for _, opt := range opts {
		opt(consumer)
	}

	return consumer, nil
}

// Start begins consuming messages from Kafka and processing them
func (c *Consumer) Start(ctx context.Context) error {
	// Create a cancellable context
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// Initialize the message channel
	c.messageChan = make(chan kafka.Message, c.messageChanSize)

	// Connect to Kafka for each topic
	for _, topic := range c.topics {
		err := c.connectTopic(topic)
		if err != nil {
			log.Error().
				Str("topic", topic).
				Err(err).
				Msg("Failed to connect to Kafka topic")
			return fmt.Errorf("failed to connect to topic %s: %w", topic, err)
		}
	}

	// Start the message consumption workers
	for i := 0; i < c.processingWorkers; i++ {
		c.wg.Add(1)
		go c.processMessages(ctx)
	}

	// Start a metrics collection goroutine
	go c.collectMetrics(ctx)

	return nil
}

// collectMetrics periodically logs metrics information
func (c *Consumer) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			channelPercent := float64(len(c.messageChan)) / float64(c.messageChanSize) * 100.0
			msgReceived := c.metrics.MessagesReceived.Load()

			log.Info().
				Uint64("messages_received", msgReceived).
				Uint64("batches_created", c.metrics.BatchesCreated.Load()).
				Uint64("bytes_received", c.metrics.BytesReceived.Load()).
				Int64("message_lag", c.metrics.MessageLag.Load()).
				Int64("messages_pending", c.metrics.MessagesPending.Load()).
				Float64("buffer_utilization_percent", channelPercent).
				Str("avg_processing_time", fmt.Sprintf("%.2fms", float64(c.metrics.MessageProcessingTime.Load())/float64(msgReceived)/1000000.0)).
				Msg("Kafka consumer metrics")
		}
	}
}

// Stop halts all Kafka consumers
func (c *Consumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}

	// Close all readers
	c.mu.Lock()
	for _, reader := range c.readers {
		reader.Close()
	}
	c.mu.Unlock()

	// Wait for all goroutines to finish
	c.wg.Wait()

	// Close the message channel
	close(c.messageChan)
}

// GetMetrics returns the current metrics
func (c *Consumer) GetMetrics() map[string]interface{} {
	avgProcTime := float64(0)
	if msgRecv := c.metrics.MessagesReceived.Load(); msgRecv > 0 {
		avgProcTime = float64(c.metrics.MessageProcessingTime.Load()) / float64(msgRecv) / 1000000.0
	}

	return map[string]interface{}{
		"messages_received":      c.metrics.MessagesReceived.Load(),
		"messages_dropped":       c.metrics.MessagesDropped.Load(),
		"bytes_received":         c.metrics.BytesReceived.Load(),
		"batches_created":        c.metrics.BatchesCreated.Load(),
		"avg_processing_time_ms": avgProcTime,
		"messages_pending":       c.metrics.MessagesPending.Load(),
		"message_lag":            c.metrics.MessageLag.Load(),
		"buffer_utilization":     len(c.messageChan),
		"buffer_capacity":        c.messageChanSize,
		"topics":                 c.topics,
		"partitions":             c.metrics.TopicPartitionCount,
		"last_batch_time":        time.Unix(0, c.metrics.LastBatchCreationTime.Load()).String(),
	}
}

// connectTopic establishes a connection to a Kafka topic
func (c *Consumer) connectTopic(topic string) error {
	config := kafka.ReaderConfig{
		Brokers:     c.brokers,
		GroupID:     c.groupID,
		Topic:       topic,
		MinBytes:    1e3,  // 1KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.LastOffset,
		// Use the dialer with TLS config if needed
	}

	reader := kafka.NewReader(config)

	// Query for partition information
	conn, err := kafka.DialLeader(context.Background(), "tcp", c.brokers[0], topic, 0)
	if err == nil {
		partitions, err := conn.ReadPartitions()
		if err == nil {
			c.metrics.TopicPartitionCount[topic] = len(partitions)
		}
		conn.Close()
	}

	c.mu.Lock()
	c.readers[topic] = reader
	c.mu.Unlock()

	// Start a goroutine to read messages
	c.wg.Add(1)
	go c.consumeTopic(topic, reader)

	log.Info().
		Str("topic", topic).
		Strs("brokers", c.brokers).
		Msg("Connected to Kafka topic")

	return nil
}

// consumeTopic continuously reads messages from a Kafka topic
func (c *Consumer) consumeTopic(topic string, reader *kafka.Reader) {
	defer c.wg.Done()

	for {
		ctx := context.Background()
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				log.Info().
					Str("topic", topic).
					Msg("Context cancelled, stopping Kafka consumption")
				return
			default:
				log.Error().
					Str("topic", topic).
					Err(err).
					Msg("Error reading Kafka message")
				time.Sleep(1 * time.Second)
				continue
			}
		}

		// Track metrics
		c.metrics.MessagesReceived.Add(1)
		c.metrics.BytesReceived.Add(uint64(len(msg.Value)))
		c.metrics.MessageLag.Store(msg.Time.UnixNano())

		// Send message to processing channel with back pressure
		select {
		case c.messageChan <- msg:
			c.metrics.MessagesPending.Add(1)
		default:
			// Channel full - implement back pressure
			log.Warn().
				Str("topic", topic).
				Msg("Message channel full, blocking producer until space available")
			c.messageChan <- msg // This will block until space is available
			c.metrics.MessagesPending.Add(1)
		}
	}
}

// processMessages reads from the message channel and converts messages to Arrow batches
func (c *Consumer) processMessages(ctx context.Context) {
	defer c.wg.Done()

	// Create message batches per topic
	batches := make(map[string][]kafka.Message)
	batchTimers := make(map[string]*time.Timer)
	lastFlush := make(map[string]time.Time)

	// Initialize timers
	for _, topic := range c.topics {
		batches[topic] = make([]kafka.Message, 0, c.batchSize)
		batchTimers[topic] = time.NewTimer(5 * time.Second)
		lastFlush[topic] = time.Now()
	}

	flushBatch := func(topic string) {
		if len(batches[topic]) == 0 {
			return
		}

		startTime := time.Now()

		// Get the stream for this topic
		stream := c.streamMgr.GetStream(topic)

		// Convert messages to Arrow record batch
		batch, err := c.createBatch(topic, batches[topic])
		if err != nil {
			log.Error().
				Str("topic", topic).
				Err(err).
				Msg("Failed to create Arrow batch")
			return
		}

		// Add batch to stream with back pressure
		stream.AddBatchAsync(ctx, c.streamMgr, batch)

		// Update metrics
		c.metrics.BatchesCreated.Add(1)
		c.metrics.MessageProcessingTime.Add(uint64(time.Since(startTime).Nanoseconds()))
		c.metrics.LastBatchCreationTime.Store(time.Now().UnixNano())
		c.metrics.MessagesPending.Add(-int64(len(batches[topic])))

		// Clear the batch
		batches[topic] = make([]kafka.Message, 0, c.batchSize)
		lastFlush[topic] = time.Now()

		// Reset the timer
		if !batchTimers[topic].Stop() {
			select {
			case <-batchTimers[topic].C:
			default:
			}
		}
		batchTimers[topic].Reset(5 * time.Second)
	}

	for {
		select {
		case <-ctx.Done():
			// Flush any remaining batches
			for topic := range batches {
				flushBatch(topic)
			}
			return

		case msg, ok := <-c.messageChan:
			if !ok {
				// Channel closed
				return
			}

			topic := msg.Topic
			batches[topic] = append(batches[topic], msg)

			// If batch is full, process it
			if len(batches[topic]) >= c.batchSize {
				flushBatch(topic)
			}

		default:
			// Check for timer expirations
			for topic, timer := range batchTimers {
				select {
				case <-timer.C:
					// Timer expired, flush the batch if it has messages and it's been at least 1 second
					if len(batches[topic]) > 0 && time.Since(lastFlush[topic]) >= time.Second {
						flushBatch(topic)
					} else {
						// Reset the timer
						timer.Reset(5 * time.Second)
					}
				default:
					// Timer hasn't expired
				}
			}

			// If no messages to process, sleep briefly to avoid busy-waiting
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// MessageConverter is an interface for custom message conversion logic
type MessageConverter interface {
	// ConvertMessage converts a Kafka message to Arrow arrays
	ConvertMessage(msg kafka.Message, schema *arrow.Schema) (map[string]arrow.Array, error)

	// InferSchema infers an Arrow schema from Kafka messages
	InferSchema(msgs []kafka.Message) (*arrow.Schema, error)
}

// createBatch converts a batch of Kafka messages to an Arrow record batch
func (c *Consumer) createBatch(topic string, messages []kafka.Message) (arrow.Record, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("empty message batch")
	}

	// Check if we have a registered converter for this topic
	converter, hasConverter := c.converters[topic]

	// Get the stream for schema information
	stream := c.streamMgr.GetStream(topic)
	schema := stream.GetSchema()

	if schema == nil {
		// Infer schema if not available
		c.metrics.SchemaInferenceCount.Add(1)

		var err error
		if hasConverter {
			// Use custom converter
			schema, err = converter.InferSchema(messages)
		} else {
			// Use default schema inference based on message content
			schema, err = c.inferSchema(messages)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to infer schema: %w", err)
		}

		// Update stream schema by creating a dummy batch
		dummyBatch := createDummyBatch(c.allocator, schema)
		// Adding and immediately retrieving to update the stream's schema
		if err := stream.AddBatch(context.Background(), dummyBatch); err != nil {
			return nil, fmt.Errorf("failed to update stream schema: %w", err)
		}
		dummyBatch.Release()
	}

	// Convert messages to columnar arrays
	if hasConverter {
		// Use custom converter for batch conversion
		return c.createBatchWithConverter(converter, schema, messages)
	}

	// Use default conversion logic based on inferred message format
	return c.createDefaultBatch(schema, messages)
}

// createBatchWithConverter uses a custom converter to create an Arrow record batch
func (c *Consumer) createBatchWithConverter(converter MessageConverter, schema *arrow.Schema, messages []kafka.Message) (arrow.Record, error) {
	// Create a builder for each field in the schema
	builders := make(map[string]array.Builder, schema.NumFields())
	for _, field := range schema.Fields() {
		builders[field.Name] = array.NewBuilder(c.allocator, field.Type)
	}

	// Process each message using the converter
	for _, msg := range messages {
		arrays, err := converter.ConvertMessage(msg, schema)
		if err != nil {
			log.Error().
				Str("topic", msg.Topic).
				Err(err).
				Msg("Error converting message with custom converter")
			continue
		}

		// Append values to each builder
		for fieldName, arr := range arrays {
			if builder, ok := builders[fieldName]; ok {
				// Append values from the array to the builder
				switch arr := arr.(type) {
				case *array.String:
					stringBuilder := builder.(*array.StringBuilder)
					for i := 0; i < arr.Len(); i++ {
						if arr.IsNull(i) {
							stringBuilder.AppendNull()
						} else {
							stringBuilder.Append(arr.Value(i))
						}
					}
				case *array.Int64:
					int64Builder := builder.(*array.Int64Builder)
					for i := 0; i < arr.Len(); i++ {
						if arr.IsNull(i) {
							int64Builder.AppendNull()
						} else {
							int64Builder.Append(arr.Value(i))
						}
					}
				case *array.Float64:
					float64Builder := builder.(*array.Float64Builder)
					for i := 0; i < arr.Len(); i++ {
						if arr.IsNull(i) {
							float64Builder.AppendNull()
						} else {
							float64Builder.Append(arr.Value(i))
						}
					}
				case *array.Boolean:
					boolBuilder := builder.(*array.BooleanBuilder)
					for i := 0; i < arr.Len(); i++ {
						if arr.IsNull(i) {
							boolBuilder.AppendNull()
						} else {
							boolBuilder.Append(arr.Value(i))
						}
					}
				case *array.Timestamp:
					tsBuilder := builder.(*array.TimestampBuilder)
					for i := 0; i < arr.Len(); i++ {
						if arr.IsNull(i) {
							tsBuilder.AppendNull()
						} else {
							tsBuilder.Append(arr.Value(i))
						}
					}
				case *array.Binary:
					binaryBuilder := builder.(*array.BinaryBuilder)
					for i := 0; i < arr.Len(); i++ {
						if arr.IsNull(i) {
							binaryBuilder.AppendNull()
						} else {
							binaryBuilder.Append(arr.Value(i))
						}
					}
				// Add more type cases as needed
				default:
					log.Warn().
						Str("field", fieldName).
						Str("type", arr.DataType().String()).
						Msg("Unsupported array type for custom converter, skipping field")
				}
			}
		}
	}

	// Build arrays from builders
	columns := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		if builder, ok := builders[field.Name]; ok {
			columns[i] = builder.NewArray()
			defer columns[i].Release()
			defer builder.Release()
		} else {
			// Create an empty array of the appropriate type for missing fields
			builder := array.NewBuilder(c.allocator, field.Type)
			for range messages {
				builder.AppendNull()
			}
			columns[i] = builder.NewArray()
			defer columns[i].Release()
			defer builder.Release()
		}
	}

	// Create the record batch
	return array.NewRecord(schema, columns, int64(len(messages))), nil
}

// createDefaultBatch creates a batch from messages using default conversion logic
func (c *Consumer) createDefaultBatch(schema *arrow.Schema, messages []kafka.Message) (arrow.Record, error) {
	// Create builders for each field in the schema
	builders := make([]array.Builder, schema.NumFields())
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(c.allocator, field.Type)
	}

	// Process each message
	for _, msg := range messages {
		// First, try to determine the message format (JSON, Avro, Protobuf, etc.)
		msgFormat := detectMessageFormat(msg.Value)

		// Extract values based on the format
		values, err := extractMessageValues(msg, msgFormat)
		if err != nil {
			log.Error().
				Str("topic", msg.Topic).
				Err(err).
				Msg("Error extracting values from message")
			// Append nulls for this message
			for _, builder := range builders {
				builder.AppendNull()
			}
			continue
		}

		// Append values to builders
		for i, field := range schema.Fields() {
			// Handle built-in fields
			if field.Name == "kafka_key" && i < len(builders) {
				appendValue(builders[i], string(msg.Key))
				continue
			}
			if field.Name == "kafka_timestamp" && i < len(builders) {
				appendTimestamp(builders[i], msg.Time)
				continue
			}
			if field.Name == "kafka_offset" && i < len(builders) {
				appendValue(builders[i], msg.Offset)
				continue
			}
			if field.Name == "kafka_partition" && i < len(builders) {
				appendValue(builders[i], msg.Partition)
				continue
			}

			// Handle values from message
			if val, ok := values[field.Name]; ok && i < len(builders) {
				appendValue(builders[i], val)
			} else if i < len(builders) {
				builders[i].AppendNull()
			}
		}
	}

	// Build arrays from builders
	columns := make([]arrow.Array, schema.NumFields())
	for i, builder := range builders {
		columns[i] = builder.NewArray()
		defer columns[i].Release()
		defer builder.Release()
	}

	// Create the record batch
	return array.NewRecord(schema, columns, int64(len(messages))), nil
}

// detectMessageFormat attempts to determine the format of a message
func detectMessageFormat(data []byte) string {
	// Check if it's JSON
	if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
		var js interface{}
		if err := json.Unmarshal(data, &js); err == nil {
			return "json"
		}
	}

	// Additional format detection could be added here
	// For Avro, Protobuf, etc.

	// Default to binary
	return "binary"
}

// extractMessageValues extracts values from a message based on its format
func extractMessageValues(msg kafka.Message, format string) (map[string]interface{}, error) {
	values := make(map[string]interface{})

	// Add Kafka metadata
	values["kafka_key"] = string(msg.Key)
	values["kafka_timestamp"] = msg.Time
	values["kafka_offset"] = msg.Offset
	values["kafka_partition"] = msg.Partition

	// Extract based on format
	switch format {
	case "json":
		var jsonData map[string]interface{}
		if err := json.Unmarshal(msg.Value, &jsonData); err != nil {
			return values, fmt.Errorf("failed to parse JSON: %w", err)
		}
		// Merge JSON values with existing values
		for k, v := range jsonData {
			values[k] = v
		}
	case "binary":
		// For binary, just store the raw value
		values["value"] = msg.Value
		// Add cases for other formats as needed
	}

	return values, nil
}

// appendValue appends a value to a builder based on the builder's type
func appendValue(builder array.Builder, value interface{}) {
	if value == nil {
		builder.AppendNull()
		return
	}

	switch builder := builder.(type) {
	case *array.StringBuilder:
		switch v := value.(type) {
		case string:
			builder.Append(v)
		default:
			builder.Append(fmt.Sprintf("%v", v))
		}
	case *array.Int8Builder:
		switch v := value.(type) {
		case int8:
			builder.Append(v)
		case int:
			builder.Append(int8(v))
		case float64:
			builder.Append(int8(v))
		default:
			builder.AppendNull()
		}
	case *array.Int16Builder:
		switch v := value.(type) {
		case int16:
			builder.Append(v)
		case int:
			builder.Append(int16(v))
		case float64:
			builder.Append(int16(v))
		default:
			builder.AppendNull()
		}
	case *array.Int32Builder:
		switch v := value.(type) {
		case int32:
			builder.Append(v)
		case int:
			builder.Append(int32(v))
		case float64:
			builder.Append(int32(v))
		default:
			builder.AppendNull()
		}
	case *array.Int64Builder:
		switch v := value.(type) {
		case int64:
			builder.Append(v)
		case int:
			builder.Append(int64(v))
		case float64:
			builder.Append(int64(v))
		default:
			builder.AppendNull()
		}
	case *array.Float32Builder:
		switch v := value.(type) {
		case float32:
			builder.Append(v)
		case float64:
			builder.Append(float32(v))
		default:
			builder.AppendNull()
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float64:
			builder.Append(v)
		case int:
			builder.Append(float64(v))
		default:
			builder.AppendNull()
		}
	case *array.BooleanBuilder:
		switch v := value.(type) {
		case bool:
			builder.Append(v)
		default:
			builder.AppendNull()
		}
	case *array.TimestampBuilder:
		appendTimestamp(builder, value)
	case *array.BinaryBuilder:
		switch v := value.(type) {
		case []byte:
			builder.Append(v)
		case string:
			builder.Append([]byte(v))
		default:
			builder.AppendNull()
		}
	default:
		log.Warn().
			Str("type", fmt.Sprintf("%T", builder)).
			Msg("Unsupported builder type, appending null")
		builder.AppendNull()
	}
}

// appendTimestamp appends a timestamp value to a timestamp builder
func appendTimestamp(builder array.Builder, value interface{}) {
	tsBuilder, ok := builder.(*array.TimestampBuilder)
	if !ok {
		log.Error().Msg("Builder is not a TimestampBuilder")
		builder.AppendNull()
		return
	}

	switch v := value.(type) {
	case time.Time:
		tsBuilder.Append(arrow.Timestamp(v.UnixNano()))
	case *time.Time:
		if v == nil {
			tsBuilder.AppendNull()
		} else {
			tsBuilder.Append(arrow.Timestamp(v.UnixNano()))
		}
	case int64:
		tsBuilder.Append(arrow.Timestamp(v))
	case float64:
		tsBuilder.Append(arrow.Timestamp(int64(v)))
	default:
		tsBuilder.AppendNull()
	}
}

// inferSchema creates a schema based on the content of Kafka messages
func (c *Consumer) inferSchema(messages []kafka.Message) (*arrow.Schema, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to infer schema from")
	}

	// Start with default fields for Kafka metadata
	fields := []arrow.Field{
		{Name: "kafka_key", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "kafka_timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
		{Name: "kafka_offset", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "kafka_partition", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}

	// Get a sample of messages to analyze
	sampleSize := min(len(messages), 10)
	sample := messages[:sampleSize]

	// Try to detect format and infer fields
	format := detectMessageFormat(sample[0].Value)

	if format == "json" {
		// For JSON, try to infer schema from all sample messages
		jsonFields, err := inferJsonSchema(sample)
		if err != nil {
			log.Warn().
				Err(err).
				Msg("Failed to infer JSON schema, using default schema")
		} else {
			fields = append(fields, jsonFields...)
		}
	} else {
		// For other formats or if JSON inference fails, add a binary value field
		fields = append(fields, arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true})
	}

	return arrow.NewSchema(fields, nil), nil
}

// inferJsonSchema analyzes JSON messages to infer field types
func inferJsonSchema(messages []kafka.Message) ([]arrow.Field, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to infer schema from")
	}

	// Map to track field types
	fieldTypes := make(map[string]arrow.DataType)
	fieldNullable := make(map[string]bool)

	// Process each message
	for _, msg := range messages {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			continue // Skip invalid JSON
		}

		// Analyze each field
		for key, value := range data {
			dataType, nullable := inferJsonFieldType(value)

			// If this is the first time seeing this field, record its type
			if existingType, ok := fieldTypes[key]; !ok {
				fieldTypes[key] = dataType
				fieldNullable[key] = nullable
			} else {
				// If we've seen this field before, update type if needed
				fieldTypes[key] = commonSuperType(existingType, dataType)
				fieldNullable[key] = fieldNullable[key] || nullable
			}
		}
	}

	// Convert to arrow.Field slice
	fields := make([]arrow.Field, 0, len(fieldTypes))
	for name, dataType := range fieldTypes {
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     dataType,
			Nullable: fieldNullable[name],
		})
	}

	return fields, nil
}

// inferJsonFieldType determines the Arrow data type for a JSON value
func inferJsonFieldType(value interface{}) (arrow.DataType, bool) {
	if value == nil {
		return arrow.BinaryTypes.String, true
	}

	switch v := value.(type) {
	case float64:
		// JSON numbers are float64 by default
		if v == float64(int64(v)) {
			// It's an integer
			return arrow.PrimitiveTypes.Int64, false
		}
		return arrow.PrimitiveTypes.Float64, false
	case string:
		// Try to parse as timestamp if it looks like one
		if _, err := time.Parse(time.RFC3339, v); err == nil {
			return arrow.FixedWidthTypes.Timestamp_ns, false
		}
		return arrow.BinaryTypes.String, false
	case bool:
		return arrow.FixedWidthTypes.Boolean, false
	case map[string]interface{}:
		// For nested objects, use string representation
		return arrow.BinaryTypes.String, false
	case []interface{}:
		// For arrays, use string representation
		return arrow.BinaryTypes.String, false
	default:
		return arrow.BinaryTypes.String, false
	}
}

// commonSuperType returns the common supertype of two Arrow data types
func commonSuperType(a, b arrow.DataType) arrow.DataType {
	// If types are the same, return either one
	if a.ID() == b.ID() {
		return a
	}

	// Integer type promotion
	if (a.ID() == arrow.INT8 || a.ID() == arrow.INT16 || a.ID() == arrow.INT32 || a.ID() == arrow.INT64) &&
		(b.ID() == arrow.INT8 || b.ID() == arrow.INT16 || b.ID() == arrow.INT32 || b.ID() == arrow.INT64) {
		if a.ID() > b.ID() {
			return a
		}
		return b
	}

	// Float type promotion
	if (a.ID() == arrow.FLOAT32 || a.ID() == arrow.FLOAT64) &&
		(b.ID() == arrow.FLOAT32 || b.ID() == arrow.FLOAT64) {
		if a.ID() > b.ID() {
			return a
		}
		return b
	}

	// Int to float promotion
	if (a.ID() == arrow.INT8 || a.ID() == arrow.INT16 || a.ID() == arrow.INT32 || a.ID() == arrow.INT64) &&
		(b.ID() == arrow.FLOAT32 || b.ID() == arrow.FLOAT64) {
		return b
	}
	if (b.ID() == arrow.INT8 || b.ID() == arrow.INT16 || b.ID() == arrow.INT32 || b.ID() == arrow.INT64) &&
		(a.ID() == arrow.FLOAT32 || a.ID() == arrow.FLOAT64) {
		return a
	}

	// Default to string for mixed types
	return arrow.BinaryTypes.String
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// createDummyBatch creates a minimal record batch with the given schema
func createDummyBatch(allocator memory.Allocator, schema *arrow.Schema) arrow.Record {
	// Create builders for each field
	builders := make([]array.Builder, schema.NumFields())
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(allocator, field.Type)
	}

	// Append null values to each builder
	for _, builder := range builders {
		builder.AppendNull()
	}

	// Build arrays
	arrays := make([]arrow.Array, schema.NumFields())
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
		defer builder.Release()
	}

	// Create record batch with a single row of nulls
	return array.NewRecord(schema, arrays, 1)
}
