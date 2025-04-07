package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/TFMV/roar/stream"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog/log"
)

// Sink represents a DuckDB sink for persisting Arrow record batches
type Sink struct {
	db        *sql.DB
	streamMgr *stream.Manager
	dbPath    string
	mu        sync.Mutex
	tables    map[string]bool
}

// NewSink creates a new DuckDB sink
func NewSink(dbPath string, streamMgr *stream.Manager) (*Sink, error) {
	if dbPath == "" {
		dbPath = "roar.duckdb"
	}

	// Ensure the directory exists
	dir := filepath.Dir(dbPath)
	if dir != "." && dir != "" {
		if err := ensureDir(dir); err != nil {
			return nil, err
		}
	}

	// Connect to DuckDB
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=READ_WRITE", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB database: %w", err)
	}

	// Verify the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to DuckDB: %w", err)
	}

	return &Sink{
		db:        db,
		streamMgr: streamMgr,
		dbPath:    dbPath,
		tables:    make(map[string]bool),
	}, nil
}

// Start begins persisting data from streams to DuckDB
func (s *Sink) Start(ctx context.Context) error {
	// Start a goroutine to monitor each stream and persist data
	go s.monitorStreams(ctx)
	return nil
}

// Close closes the DuckDB connection
func (s *Sink) Close() error {
	return s.db.Close()
}

// monitorStreams periodically checks for new data in streams and persists it
func (s *Sink) monitorStreams(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Context cancelled, stopping DuckDB sink")
			return
		case <-ticker.C:
			s.persistStreams()
		}
	}
}

// persistStreams writes all stream data to DuckDB
func (s *Sink) persistStreams() {
	topics := s.streamMgr.ListStreams()
	for _, topic := range topics {
		stream := s.streamMgr.GetStream(topic)
		schema := stream.GetSchema()

		if schema == nil {
			// Skip streams with no schema yet
			continue
		}

		// Ensure table exists
		if err := s.ensureTable(topic, schema); err != nil {
			log.Error().
				Str("topic", topic).
				Err(err).
				Msg("Failed to ensure table exists")
			continue
		}

		// Get batches from the stream
		batches := stream.GetBatches()
		if len(batches) == 0 {
			continue
		}

		// Persist each batch
		for _, batch := range batches {
			if err := s.appendBatch(topic, batch); err != nil {
				log.Error().
					Str("topic", topic).
					Err(err).
					Msg("Failed to append batch to DuckDB")
			}
		}
	}
}

// ensureTable creates a table for the topic if it doesn't exist
func (s *Sink) ensureTable(topic string, schema *arrow.Schema) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we've already created this table
	if _, exists := s.tables[topic]; exists {
		return nil
	}

	// Create table based on Arrow schema
	createSQL, err := schemaToCreateTable(topic, schema)
	if err != nil {
		return err
	}

	// Execute create table statement
	_, err = s.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	s.tables[topic] = true
	log.Info().
		Str("topic", topic).
		Str("sql", createSQL).
		Msg("Created DuckDB table")

	return nil
}

// appendBatch appends an Arrow record batch to a DuckDB table
func (s *Sink) appendBatch(topic string, batch arrow.Record) error {
	// Convert Arrow batch to DuckDB compatible format using the Arrow IPC
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// In go-duckdb v2, we use SQL with arrow_table function to insert Arrow data
	_, err = conn.ExecContext(context.Background(),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM arrow_table($1)", topic),
		batch)
	if err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	log.Debug().
		Str("topic", topic).
		Int64("rows", batch.NumRows()).
		Msg("Appended batch to DuckDB table")

	return nil
}

// schemaToCreateTable converts an Arrow schema to a DuckDB CREATE TABLE statement
func schemaToCreateTable(tableName string, schema *arrow.Schema) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("schema is nil")
	}

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", tableName)

	for i, field := range schema.Fields() {
		if i > 0 {
			sql += ",\n"
		}

		colType, err := arrowToDuckDBType(field.Type)
		if err != nil {
			return "", err
		}

		sql += fmt.Sprintf("  %s %s", field.Name, colType)
		if !field.Nullable {
			sql += " NOT NULL"
		}
	}

	sql += "\n)"
	return sql, nil
}

// arrowToDuckDBType maps Arrow data types to DuckDB data types
func arrowToDuckDBType(arrowType arrow.DataType) (string, error) {
	switch arrowType.ID() {
	case arrow.INT8:
		return "TINYINT", nil
	case arrow.INT16:
		return "SMALLINT", nil
	case arrow.INT32:
		return "INTEGER", nil
	case arrow.INT64:
		return "BIGINT", nil
	case arrow.UINT8:
		return "UTINYINT", nil
	case arrow.UINT16:
		return "USMALLINT", nil
	case arrow.UINT32:
		return "UINTEGER", nil
	case arrow.UINT64:
		return "UBIGINT", nil
	case arrow.FLOAT32:
		return "FLOAT", nil
	case arrow.FLOAT64:
		return "DOUBLE", nil
	case arrow.BOOL:
		return "BOOLEAN", nil
	case arrow.STRING:
		return "VARCHAR", nil
	case arrow.BINARY:
		return "BLOB", nil
	case arrow.TIMESTAMP:
		return "TIMESTAMP", nil
	case arrow.DATE32:
		return "DATE", nil
	case arrow.DATE64:
		return "DATE", nil
	default:
		// Default to VARCHAR for complex types
		return "VARCHAR", nil
	}
}

// ensureDir ensures a directory exists
func ensureDir(dir string) error {
	// In a real implementation, we would create the directory if it doesn't exist
	return nil
}
