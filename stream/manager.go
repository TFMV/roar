package stream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog/log"
)

// Stream represents an ephemeral stream of RecordBatches
type Stream struct {
	Topic       string
	Schema      *arrow.Schema
	Batches     []arrow.Record
	LastUpdated time.Time
	mu          sync.RWMutex
	maxBytes    uint64
	currentSize uint64
	// Metrics
	recordsProcessed  atomic.Uint64
	recordsDropped    atomic.Uint64
	bytesProcessed    atomic.Uint64
	maxQueueLength    atomic.Uint64
	currentQueueItems atomic.Uint64
}

// Manager manages the lifecycle of ephemeral streams
type Manager struct {
	streams       map[string]*Stream
	mu            sync.RWMutex
	ttl           time.Duration
	maxBufferSize uint64

	// Back pressure controls
	maxStreamConcurrency int
	streamSemaphore      chan struct{}
	batchQueueSize       int

	// Metrics
	activeStreams    atomic.Int32
	totalRecords     atomic.Uint64
	totalBytes       atomic.Uint64
	totalDropped     atomic.Uint64
	recordsPerSecond atomic.Uint64
	bytesPerSecond   atomic.Uint64

	// Configuration
	enableBackPressure bool
}

// ManagerOption defines functional options for the stream Manager
type ManagerOption func(*Manager)

// WithStreamConcurrency sets the maximum number of concurrent streams being processed
func WithStreamConcurrency(max int) ManagerOption {
	return func(m *Manager) {
		m.maxStreamConcurrency = max
		m.streamSemaphore = make(chan struct{}, max)
	}
}

// WithBatchQueueSize sets the buffer size for batch processing queues
func WithBatchQueueSize(size int) ManagerOption {
	return func(m *Manager) {
		m.batchQueueSize = size
	}
}

// WithBackPressure enables back pressure mechanisms
func WithBackPressure(enable bool) ManagerOption {
	return func(m *Manager) {
		m.enableBackPressure = enable
	}
}

// NewManager creates a new stream manager
func NewManager(ttl time.Duration, bufferLimit string, opts ...ManagerOption) (*Manager, error) {
	bufferSize, err := humanize.ParseBytes(bufferLimit)
	if err != nil {
		return nil, fmt.Errorf("invalid buffer limit: %w", err)
	}

	mgr := &Manager{
		streams:              make(map[string]*Stream),
		ttl:                  ttl,
		maxBufferSize:        bufferSize,
		maxStreamConcurrency: 100,  // Default to 100 concurrent streams
		batchQueueSize:       1000, // Default batch queue size
		enableBackPressure:   true, // Enable back pressure by default
	}

	// Apply options
	for _, opt := range opts {
		opt(mgr)
	}

	// Initialize semaphore if not done by options
	if mgr.streamSemaphore == nil {
		mgr.streamSemaphore = make(chan struct{}, mgr.maxStreamConcurrency)
	}

	// Start a goroutine to clean up expired streams
	go mgr.cleanupLoop()

	// Start a goroutine to collect metrics
	go mgr.collectMetrics()

	return mgr, nil
}

// cleanupLoop periodically checks for expired streams and removes them
func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(m.ttl / 2)
	defer ticker.Stop()

	for range ticker.C {
		m.cleanupExpiredStreams()
	}
}

// collectMetrics periodically updates aggregated metrics
func (m *Manager) collectMetrics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastTotalRecords, lastTotalBytes uint64

	for range ticker.C {
		currentTotalRecords := m.totalRecords.Load()
		currentTotalBytes := m.totalBytes.Load()

		// Calculate per-second rates
		recordsDelta := currentTotalRecords - lastTotalRecords
		bytesDelta := currentTotalBytes - lastTotalBytes
		m.recordsPerSecond.Store(recordsDelta)
		m.bytesPerSecond.Store(bytesDelta)

		// Update last values
		lastTotalRecords = currentTotalRecords
		lastTotalBytes = currentTotalBytes
	}
}

// cleanupExpiredStreams removes all streams that have exceeded their TTL
func (m *Manager) cleanupExpiredStreams() {
	expiredTopics := []string{}
	now := time.Now()

	m.mu.RLock()
	for topic, stream := range m.streams {
		stream.mu.RLock()
		if now.Sub(stream.LastUpdated) > m.ttl {
			expiredTopics = append(expiredTopics, topic)
		}
		stream.mu.RUnlock()
	}
	m.mu.RUnlock()

	if len(expiredTopics) > 0 {
		m.mu.Lock()
		for _, topic := range expiredTopics {
			stream, exists := m.streams[topic]
			if exists {
				// Release all record batches
				stream.mu.Lock()
				for _, batch := range stream.Batches {
					batch.Release()
				}
				stream.Batches = nil
				stream.mu.Unlock()

				delete(m.streams, topic)
				m.activeStreams.Add(-1)
				log.Info().Str("topic", topic).Msg("Removed expired stream")
			}
		}
		m.mu.Unlock()
	}
}

// AcquireStreamCapacity acquires capacity for a stream operation, blocking if at max concurrency
func (m *Manager) AcquireStreamCapacity(ctx context.Context) error {
	// If back pressure is disabled, return immediately
	if !m.enableBackPressure {
		return nil
	}

	select {
	case m.streamSemaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseStreamCapacity releases previously acquired stream capacity
func (m *Manager) ReleaseStreamCapacity() {
	// If back pressure is disabled, return immediately
	if !m.enableBackPressure {
		return
	}

	select {
	case <-m.streamSemaphore:
	default:
		// Should never happen, but just in case
		log.Warn().Msg("Attempted to release stream capacity without acquiring it first")
	}
}

// GetStream retrieves a stream by topic, creating it if it doesn't exist
func (m *Manager) GetStream(topic string) *Stream {
	m.mu.RLock()
	stream, exists := m.streams[topic]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		// Check again under write lock to avoid race condition
		stream, exists = m.streams[topic]
		if !exists {
			stream = &Stream{
				Topic:       topic,
				Batches:     []arrow.Record{},
				LastUpdated: time.Now(),
				maxBytes:    m.maxBufferSize,
			}
			m.streams[topic] = stream
			m.activeStreams.Add(1)
			log.Info().Str("topic", topic).Msg("Created new stream")
		}
		m.mu.Unlock()
	}

	return stream
}

// ListStreams returns a list of all active streams
func (m *Manager) ListStreams() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make([]string, 0, len(m.streams))
	for topic := range m.streams {
		topics = append(topics, topic)
	}
	return topics
}

// GetActiveStreamCount returns the current number of active streams
func (m *Manager) GetActiveStreamCount() int {
	return int(m.activeStreams.Load())
}

// GetMetrics returns current metrics about stream manager usage
func (m *Manager) GetMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"active_streams":     m.activeStreams.Load(),
		"total_records":      m.totalRecords.Load(),
		"total_bytes":        m.totalBytes.Load(),
		"total_dropped":      m.totalDropped.Load(),
		"records_per_second": m.recordsPerSecond.Load(),
		"bytes_per_second":   m.bytesPerSecond.Load(),
		"streams_concurrent": len(m.streamSemaphore),
		"max_concurrency":    m.maxStreamConcurrency,
	}

	return metrics
}

// AddBatch adds a record batch to the stream and updates its schema if needed
func (s *Stream) AddBatch(ctx context.Context, batch arrow.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update schema if needed
	if s.Schema == nil {
		s.Schema = batch.Schema()
	}

	// Check buffer limits
	batchSize := estimateBatchSize(batch)
	if s.currentSize+batchSize > s.maxBytes {
		// Remove oldest batches until we have space
		for len(s.Batches) > 0 && s.currentSize+batchSize > s.maxBytes {
			oldestBatch := s.Batches[0]
			oldestSize := estimateBatchSize(oldestBatch)
			s.currentSize -= oldestSize

			// Update dropped records count
			s.recordsDropped.Add(uint64(oldestBatch.NumRows()))

			// Release the batch to free memory
			oldestBatch.Release()

			// Remove from slice
			s.Batches = s.Batches[1:]

			log.Debug().
				Str("topic", s.Topic).
				Int64("removed_rows", oldestBatch.NumRows()).
				Uint64("removed_bytes", oldestSize).
				Msg("Removed oldest batch to make space")
		}
	}

	// Create a copy of the batch that we can retain
	newBatch := batch.NewSlice(0, batch.NumRows())
	s.Batches = append(s.Batches, newBatch)
	s.currentSize += batchSize
	s.LastUpdated = time.Now()

	// Update metrics
	s.recordsProcessed.Add(uint64(batch.NumRows()))
	s.bytesProcessed.Add(batchSize)
	s.currentQueueItems.Store(uint64(len(s.Batches)))

	// Update max queue length if needed
	for {
		currentMax := s.maxQueueLength.Load()
		newLength := uint64(len(s.Batches))
		if newLength <= currentMax {
			break
		}
		if s.maxQueueLength.CompareAndSwap(currentMax, newLength) {
			break
		}
	}

	log.Debug().
		Str("topic", s.Topic).
		Int("batches", len(s.Batches)).
		Int64("rows", newBatch.NumRows()).
		Uint64("buffer_used", s.currentSize).
		Msg("Added record batch to stream")

	return nil
}

// AddBatchAsync adds a record batch to the stream asynchronously with back pressure
func (s *Stream) AddBatchAsync(ctx context.Context, mgr *Manager, batch arrow.Record) {
	// Acquire capacity
	err := mgr.AcquireStreamCapacity(ctx)
	if err != nil {
		log.Warn().
			Str("topic", s.Topic).
			Err(err).
			Msg("Failed to acquire capacity for batch processing")
		return
	}

	// Process the batch
	go func() {
		defer mgr.ReleaseStreamCapacity()

		err := s.AddBatch(ctx, batch)
		if err != nil {
			log.Error().
				Str("topic", s.Topic).
				Err(err).
				Msg("Failed to add batch to stream")
		}

		// Update manager-level metrics
		mgr.totalRecords.Add(uint64(batch.NumRows()))
		mgr.totalBytes.Add(estimateBatchSize(batch))
	}()
}

// GetBatches returns all batches in the stream
func (s *Stream) GetBatches() []arrow.Record {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Update last accessed time
	s.LastUpdated = time.Now()

	result := make([]arrow.Record, len(s.Batches))
	copy(result, s.Batches)
	return result
}

// GetSchema returns the stream's schema
func (s *Stream) GetSchema() *arrow.Schema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Schema
}

// GetStreamMetrics returns metrics about this stream
func (s *Stream) GetStreamMetrics() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics := map[string]interface{}{
		"topic":             s.Topic,
		"current_batches":   len(s.Batches),
		"current_size":      s.currentSize,
		"records_processed": s.recordsProcessed.Load(),
		"records_dropped":   s.recordsDropped.Load(),
		"bytes_processed":   s.bytesProcessed.Load(),
		"max_queue_length":  s.maxQueueLength.Load(),
		"current_queue":     s.currentQueueItems.Load(),
		"last_updated":      s.LastUpdated.String(),
	}

	if s.Schema != nil {
		metrics["columns"] = len(s.Schema.Fields())
	}

	return metrics
}

// estimateBatchSize provides a rough estimate of the memory size of a record batch
func estimateBatchSize(batch arrow.Record) uint64 {
	numRows := batch.NumRows()
	numCols := int64(batch.NumCols())

	// This is a rough estimate that assumes 8 bytes per value on average
	// A more accurate calculation would consider the actual column types
	return uint64(numRows * numCols * 8)
}
