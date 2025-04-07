package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr string
	topic      string
	output     string
	limit      int
)

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Connect to a Roar server and retrieve data",
	Long:  `Connect to a Roar server, list available topics, or retrieve data from a specific topic.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Parse server address
		_, err := url.Parse(serverAddr)
		if err != nil {
			return fmt.Errorf("invalid server address: %w", err)
		}

		// Connect to the Flight server
		client, err := connectToServer(serverAddr)
		if err != nil {
			return err
		}

		log.Info().Str("server", serverAddr).Msg("Connected to Flight server")

		// If no topic specified, list available topics
		if topic == "" {
			return listTopics(client)
		}

		// Fetch and display data for the specified topic
		return fetchTopic(client, topic, limit, output)
	},
}

// init initializes the client command
func init() {
	rootCmd.AddCommand(clientCmd)

	// Add flags
	clientCmd.Flags().StringVar(&serverAddr, "server", "grpc://localhost:8080", "Flight server address (e.g., grpc://localhost:8080)")
	clientCmd.Flags().StringVar(&topic, "topic", "", "Topic to fetch (if not specified, lists available topics)")
	clientCmd.Flags().StringVar(&output, "output", "table", "Output format (table or csv)")
	clientCmd.Flags().IntVar(&limit, "limit", 10, "Maximum number of rows to display")
}

// connectToServer establishes a connection to the Flight server
func connectToServer(serverAddr string) (flight.Client, error) {
	// Parse the server address
	u, err := url.Parse(serverAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid server address: %w", err)
	}

	// Remove the scheme
	host := u.Host
	if u.Port() == "" {
		host = fmt.Sprintf("%s:8080", host)
	}

	// Connect to the server
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	client, err := flight.NewClientWithMiddleware(host, nil, nil, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return client, nil
}

// listTopics lists all available topics on the server
func listTopics(client flight.Client) error {
	ctx := context.Background()

	// Get flight listings
	flightInfos, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		return fmt.Errorf("failed to list flights: %w", err)
	}

	fmt.Println("Available topics:")
	for {
		info, err := flightInfos.Recv()
		if err != nil {
			break // End of stream
		}

		if info.FlightDescriptor.Type == flight.DescriptorPATH && len(info.FlightDescriptor.Path) > 0 {
			fmt.Printf("- %s\n", info.FlightDescriptor.Path[0])
		}
	}

	return nil
}

// fetchTopic retrieves data from a topic and displays it
func fetchTopic(client flight.Client, topic string, rowLimit int, outputFormat string) error {
	ctx := context.Background()

	// Create a flight descriptor for the topic
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{topic},
	}

	// Get flight info to retrieve the ticket
	info, err := client.GetFlightInfo(ctx, descriptor)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %w", err)
	}

	if len(info.Endpoint) == 0 || info.Endpoint[0].Ticket == nil {
		return fmt.Errorf("no valid endpoints for topic: %s", topic)
	}

	// Get the ticket for data retrieval
	ticket := info.Endpoint[0].Ticket

	// Fetch the data
	stream, err := client.DoGet(ctx, ticket)
	if err != nil {
		return fmt.Errorf("failed to start data retrieval: %w", err)
	}

	// Create a reader
	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	// Get the schema
	schema := reader.Schema()
	if schema == nil {
		return fmt.Errorf("received nil schema for topic: %s", topic)
	}

	// Determine how to output the data
	switch strings.ToLower(outputFormat) {
	case "table":
		return outputAsTable(reader, schema, rowLimit)
	case "csv":
		return outputAsCSV(reader, schema, rowLimit)
	default:
		return fmt.Errorf("unsupported output format: %s", outputFormat)
	}
}

// outputAsTable formats and displays the data as a table
func outputAsTable(reader *flight.Reader, schema *arrow.Schema, rowLimit int) error {
	// Create a table writer
	table := tablewriter.NewWriter(os.Stdout)

	// Set up headers
	headers := make([]string, schema.NumFields())
	for i, field := range schema.Fields() {
		headers[i] = field.Name
	}
	table.SetHeader(headers)

	// Track the number of rows processed
	rowCount := 0

	// Process each batch
	for reader.Next() {
		record := reader.Record()

		// Process each row in the batch (up to the limit)
		for row := int64(0); row < record.NumRows() && rowCount < rowLimit; row++ {
			rowVals := make([]string, schema.NumFields())

			// Convert each column value to string
			for col := 0; col < int(record.NumCols()); col++ {
				column := record.Column(col)
				if column.IsNull(int(row)) {
					rowVals[col] = "NULL"
				} else {
					rowVals[col] = column.ValueStr(int(row))
				}
			}

			table.Append(rowVals)
			rowCount++
		}

		if rowCount >= rowLimit {
			break
		}
	}

	// Print the table
	if rowCount > 0 {
		table.Render()
		fmt.Printf("Displayed %d rows from topic %s\n", rowCount, topic)
	} else {
		fmt.Println("No data available")
	}

	return nil
}

// outputAsCSV formats and displays the data as CSV
func outputAsCSV(reader *flight.Reader, schema *arrow.Schema, rowLimit int) error {
	// Print the header row
	headers := make([]string, schema.NumFields())
	for i, field := range schema.Fields() {
		headers[i] = field.Name
	}
	fmt.Println(strings.Join(headers, ","))

	// Track the number of rows processed
	rowCount := 0

	// Process each batch
	for reader.Next() {
		record := reader.Record()

		// Process each row in the batch (up to the limit)
		for row := int64(0); row < record.NumRows() && rowCount < rowLimit; row++ {
			rowVals := make([]string, schema.NumFields())

			// Convert each column value to string
			for col := 0; col < int(record.NumCols()); col++ {
				column := record.Column(col)
				if column.IsNull(int(row)) {
					rowVals[col] = ""
				} else {
					// Quote string values that contain commas
					val := column.ValueStr(int(row))
					if strings.Contains(val, ",") {
						val = fmt.Sprintf("\"%s\"", val)
					}
					rowVals[col] = val
				}
			}

			fmt.Println(strings.Join(rowVals, ","))
			rowCount++

			if rowCount >= rowLimit {
				break
			}
		}
	}

	if rowCount == 0 {
		fmt.Println("No data available")
	} else {
		fmt.Printf("Displayed %d rows from topic %s\n", rowCount, topic)
	}

	return nil
}
