# Roar: Streaming Kafka to Analytics at Arrow Speed

## A High-Performance Gateway from Kafka to Arrow Flight

There's always been a weird disconnect in real-time data systems.

On one side, you've got Kafka—fast, resilient, purpose-built for moving messages at scale. On the other, you've got analytical engines like DuckDB and Polars that thrive on columnar formats, vectorized execution, and scan-friendly data layouts.

But trying to plug one into the other? That's where things get messy.
ETL jobs, serialization overhead, brittle schemas. Glue code galore.

So I built Roar—an experimental gateway that connects Kafka to analytical clients over Apache Arrow Flight.

## Why This Problem Matters

Kafka excels at streaming. Arrow excels at analytics. But the friction between them is very real:

- Kafka messages are row-based, schema-optional
- Analytical engines prefer columnar data with rich typing
- Moving between the two often requires brittle pipelines, custom connectors, or full-blown infrastructure

Roar skips the translation dance. It consumes from Kafka, batches messages into Arrow RecordBatches, and exposes them as ephemeral Flight streams. Clients like DuckDB or PyArrow can read the data directly—zero-copy, columnar, and fast.

## What Apache Arrow Flight Brings to the Table

If you haven't played with Arrow Flight, it's worth a look.

- Built for high-throughput columnar transport
- Way faster than JDBC/ODBC for analytical workloads
- Supports streaming semantics out of the box
- Plays well across languages (Python, Go, C++, Java)

For Roar, it's the missing piece that turns Kafka into something analytic clients can actually consume—without a middleman.

## How Roar Works

Here's the architecture in a nutshell:

1. **Kafka Ingest Layer**: Consumes messages in real-time
2. **Arrow Conversion Layer**: Batches and converts to RecordBatches
3. **Stream Manager**: TTLs, backpressure, memory tracking
4. **Flight Server**: Streams data to clients via Arrow Flight
5. **Optional DuckDB Sink**: Batches can also be persisted

The whole pipeline is in-memory, schema-aware, and built for low-latency access.

## Key Design Decisions

### ⚡ Ephemeral by Default

Roar doesn't try to be a database. It keeps recent data in memory and exposes it for a short time (TTL-controlled). That keeps:

- Memory usage predictable
- Clients focused on "what's happening now"
- The system dead simple to reason about

### 🧠 Backpressure Built In

Kafka can easily outpace memory if you're not careful. Roar uses:

- Stream-local buffer caps
- Message discarding when limits are hit
- Metrics to watch buffer usage and throttle consumption

### 🔍 Schema Inference That Actually Works

Kafka messages don't come with schemas. Roar handles this with:

- JSON auto-detection and schema inference
- Type promotion for mixed data
- Kafka metadata preservation (timestamps, offsets)
- Smart defaults when types are ambiguous

### 🔌 Client Integration That Doesn't Suck

The whole point is making Kafka data accessible. Here's how clients connect:

```python
# Python with PyArrow
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8080")
reader = client.do_get(flight.Ticket(b"my_topic"))
df = reader.read_all().to_pandas()
```

```sql
-- DuckDB with Arrow Flight extension
SELECT * FROM arrow_flight_scan('grpc://localhost:8080', 'my_topic');
```

## How It Performs

The performance numbers are promising:

- **Throughput**: Thousands of messages per second, even on modest hardware
- **Memory**: Configurable limits prevent surprise OOMs
- **Query Speed**: Arrow's columnar format enables blazing-fast analytics
- **Latency**: Sub-second from Kafka to query result

## Current Limitations

Roar is still experimental, with some limitations:

1. **Complex Types**: Nested structures and arrays need better handling
2. **Processing**: No transformations (yet)—it's currently read-only
3. **Scale**: Single-node design, though the components could be distributed
4. **Security**: Basic auth/encryption still on the roadmap

## Lessons From Building It

A few takeaways from building Roar:

1. **Arrow Flight Has Promise**: The protocol delivers on performance, though the ecosystem is still maturing.

2. **Memory Management Is Everything**: With streaming data, careful buffer management makes or breaks you.

3. **Schema Evolution Is Hard**: Handling changing message formats gracefully remains a challenge.

4. **Integration Points Matter**: The gaps between technologies are where the most value lives.

## Try It Yourself

The code is open-source and available for anyone interested in this approach. Contributions welcome!

It's experimental, but it demonstrates how we can build bridges between the streaming and analytical worlds using emerging protocols like Arrow Flight.

## Where This Could Go

Roar is just one approach to the streaming-to-analytics problem. Whether this specific implementation catches on or simply inspires better tools, I believe this pattern of columnar gateways has legs.

As real-time analytics becomes the norm rather than the exception, the tools we use to move data between systems will need to evolve beyond traditional ETL and batch processes.

---

*This article reflects my personal experiences building experimental data tools. Your mileage may vary, and I'd love to hear about alternative approaches!*
