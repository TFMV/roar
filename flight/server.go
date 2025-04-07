package flight

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/TFMV/roar/stream"
	"github.com/apache/arrow-go/v18/arrow/flight"
	flightproto "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FlightServer implements the Arrow Flight service
type FlightServer struct {
	flightproto.UnimplementedFlightServiceServer
	server    *grpc.Server
	streamMgr *stream.Manager
	allocator memory.Allocator
	address   string
	port      int
}

// NewServer creates and initializes a new Flight server
func NewServer(streamMgr *stream.Manager, port int) (*FlightServer, error) {
	s := &FlightServer{
		streamMgr: streamMgr,
		allocator: memory.NewGoAllocator(),
		port:      port,
		address:   fmt.Sprintf("localhost:%d", port),
	}

	// Create the gRPC server
	server := grpc.NewServer()
	s.server = server

	// Register the service
	flightproto.RegisterFlightServiceServer(server, s)

	return s, nil
}

// Serve starts the Flight server
func (s *FlightServer) Serve(ctx context.Context) error {
	log.Info().
		Str("address", s.address).
		Msg("Starting Arrow Flight server")

	// Create listener
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	// Start server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.server.Serve(lis)
	}()

	// Wait for context cancelation or server error
	select {
	case <-ctx.Done():
		log.Info().Msg("Context canceled, shutting down Flight server")
		s.server.GracefulStop()
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("arrow flight server error: %w", err)
		}
		return nil
	}
}

// ListFlights lists available streams (topics) as FlightInfo objects
func (s *FlightServer) ListFlights(criteria *flightproto.Criteria, stream flightproto.FlightService_ListFlightsServer) error {
	topics := s.streamMgr.ListStreams()

	for _, topic := range topics {
		streamObj := s.streamMgr.GetStream(topic)
		schema := streamObj.GetSchema()

		if schema == nil {
			// Skip streams with no schema yet
			continue
		}

		// Create a flight descriptor for the topic
		descriptor := &flightproto.FlightDescriptor{
			Type: flightproto.FlightDescriptor_PATH,
			Path: []string{topic},
		}

		// Define endpoints
		endpoint := &flightproto.FlightEndpoint{
			Ticket: &flightproto.Ticket{
				Ticket: []byte(topic),
			},
			Location: []*flightproto.Location{
				{
					Uri: fmt.Sprintf("grpc://%s", s.address),
				},
			},
		}

		// Serialize the schema
		schemaBytes := flight.SerializeSchema(schema, s.allocator)

		// Create the flight info
		info := &flightproto.FlightInfo{
			Schema:           schemaBytes,
			FlightDescriptor: descriptor,
			Endpoint:         []*flightproto.FlightEndpoint{endpoint},
			TotalRecords:     -1, // Unknown total
			TotalBytes:       -1, // Unknown size
		}

		if err := stream.Send(info); err != nil {
			return status.Errorf(codes.Internal, "Failed to send flight info: %v", err)
		}
	}

	return nil
}

// GetFlightInfo returns information about a particular stream by topic
func (s *FlightServer) GetFlightInfo(ctx context.Context, descriptor *flightproto.FlightDescriptor) (*flightproto.FlightInfo, error) {
	if descriptor.Type != flightproto.FlightDescriptor_PATH || len(descriptor.Path) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid descriptor")
	}

	topic := descriptor.Path[0]
	streamObj := s.streamMgr.GetStream(topic)
	schema := streamObj.GetSchema()

	if schema == nil {
		return nil, status.Errorf(codes.NotFound, "Stream for topic %s has no schema yet", topic)
	}

	// Define endpoints
	endpoint := &flightproto.FlightEndpoint{
		Ticket: &flightproto.Ticket{
			Ticket: []byte(topic),
		},
		Location: []*flightproto.Location{
			{
				Uri: fmt.Sprintf("grpc://%s", s.address),
			},
		},
	}

	// Serialize the schema
	schemaBytes := flight.SerializeSchema(schema, s.allocator)

	return &flightproto.FlightInfo{
		Schema:           schemaBytes,
		FlightDescriptor: descriptor,
		Endpoint:         []*flightproto.FlightEndpoint{endpoint},
		TotalRecords:     -1, // Unknown
		TotalBytes:       -1, // Unknown
	}, nil
}

// DoGet streams record batches for a requested stream
func (s *FlightServer) DoGet(ticket *flightproto.Ticket, server flightproto.FlightService_DoGetServer) error {
	if ticket == nil || len(ticket.Ticket) == 0 {
		return status.Errorf(codes.InvalidArgument, "Invalid ticket")
	}

	topic := string(ticket.Ticket)
	streamObj := s.streamMgr.GetStream(topic)
	schema := streamObj.GetSchema()

	if schema == nil {
		return status.Errorf(codes.NotFound, "Stream for topic %s has no schema yet", topic)
	}

	// Get all batches
	batches := streamObj.GetBatches()
	if len(batches) == 0 {
		log.Debug().Str("topic", topic).Msg("No batches available for topic")
		return nil
	}

	// Create a writer for the stream
	writer := flight.NewRecordWriter(server, ipc.WithSchema(schema))
	defer writer.Close()

	// Write batches to the stream
	for _, batch := range batches {
		if err := writer.Write(batch); err != nil {
			return status.Errorf(codes.Internal, "Failed to write record batch: %v", err)
		}
	}

	log.Debug().
		Str("topic", topic).
		Int("batches", len(batches)).
		Msg("Successfully streamed batches")

	return nil
}

// GetSchema returns the schema for a stream
func (s *FlightServer) GetSchema(ctx context.Context, request *flightproto.FlightDescriptor) (*flightproto.SchemaResult, error) {
	if request.Type != flightproto.FlightDescriptor_PATH || len(request.Path) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid descriptor")
	}

	topic := request.Path[0]
	streamObj := s.streamMgr.GetStream(topic)
	schema := streamObj.GetSchema()

	if schema == nil {
		return nil, status.Errorf(codes.NotFound, "Stream for topic %s has no schema yet", topic)
	}

	// Serialize the schema
	schemaBytes := flight.SerializeSchema(schema, s.allocator)

	return &flightproto.SchemaResult{
		Schema: schemaBytes,
	}, nil
}

// DoAction implements custom actions
func (s *FlightServer) DoAction(action *flightproto.Action, server flightproto.FlightService_DoActionServer) error {
	switch action.Type {
	case "health":
		result := &flightproto.Result{Body: []byte("OK")}
		return server.Send(result)
	case "listTopics":
		topics := s.streamMgr.ListStreams()
		result := &flightproto.Result{Body: []byte(strings.Join(topics, ","))}
		return server.Send(result)
	default:
		return status.Errorf(codes.Unimplemented, "Action %s not implemented", action.Type)
	}
}
