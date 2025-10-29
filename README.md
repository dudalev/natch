# Chex

High-performance Elixir client for ClickHouse database, powered by the Rust [clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) library.

## Features

- **High Performance**: Built on top of the official Rust ClickHouse client
- **Connection Pooling**: Efficient HTTP connection management via hyper/tokio
- **Streaming**: Memory-efficient lazy evaluation of large result sets
- **Auto-batching Inserts**: Optimize write throughput with configurable batching
- **LZ4 Compression**: Reduce network bandwidth with transparent compression
- **Type Safety**: Comprehensive type mapping between Elixir and ClickHouse
- **Simple API**: Idiomatic Elixir interface with both low-level and high-level APIs

## Requirements

- Elixir 1.18+ / Erlang 27+
- Rust nightly (for NIF compilation)
- ClickHouse server 20.3+

## Installation

Add `chex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:chex, "~> 0.1.0"}
  ]
end
```

## Quick Start

```elixir
# Start a connection
{:ok, conn} = Chex.start_link(
  url: "http://localhost:8123",
  database: "default"
)

# Create a table
Chex.execute(conn, """
  CREATE TABLE users (
    id UInt32,
    name String,
    age UInt8
  ) ENGINE = MergeTree()
  ORDER BY id
""")

# Insert data
{:ok, insert} = Chex.insert(conn, "users")
Chex.write(insert, %{"id" => 1, "name" => "Alice", "age" => 30})
Chex.write(insert, %{"id" => 2, "name" => "Bob", "age" => 25})
Chex.end_insert(insert)

# Query data
{:ok, users} = Chex.query(conn, "SELECT * FROM users WHERE age > ?", [20])
IO.inspect(users)
# => [%{"id" => 1, "name" => "Alice", "age" => 30}, %{"id" => 2, "name" => "Bob", "age" => 25}]
```

## Usage

### Connection Management

Start a connection with configuration options:

```elixir
{:ok, conn} = Chex.start_link(
  url: "http://localhost:8123",
  database: "analytics",
  user: "readonly",
  password: "secret",
  compression: true
)
```

Connection options:
- `:url` - ClickHouse HTTP endpoint (default: `"http://localhost:8123"`)
- `:database` - Database name (default: `"default"`)
- `:user` - Username (optional)
- `:password` - Password (optional)
- `:compression` - Enable LZ4 compression (default: `true`)
- `:name` - Register the connection with a name (optional)

### Queries

Execute queries with parameter binding:

```elixir
# Simple query
{:ok, rows} = Chex.query(conn, "SELECT * FROM users")

# With parameters (? placeholders)
{:ok, rows} = Chex.query(conn, "SELECT * FROM users WHERE id = ?", [42])

# Multiple parameters
{:ok, rows} = Chex.query(conn,
  "SELECT * FROM users WHERE age BETWEEN ? AND ?",
  [18, 65]
)

# Bang version that raises on error
rows = Chex.query!(conn, "SELECT * FROM users")
```

### DDL Operations

Execute DDL/DML statements without expecting results:

```elixir
# Create table
:ok = Chex.execute(conn, """
  CREATE TABLE events (
    id UInt64,
    event_type String,
    timestamp DateTime
  ) ENGINE = MergeTree()
  ORDER BY timestamp
""")

# Drop table
:ok = Chex.execute(conn, "DROP TABLE events")

# Delete rows
:ok = Chex.execute(conn, "DELETE FROM users WHERE id = ?", [123])
```

### Streaming Results

Process large result sets efficiently with lazy streams:

```elixir
conn
|> Chex.stream("SELECT * FROM large_table")
|> Stream.filter(&(&1["status"] == "active"))
|> Stream.map(&(&1["name"]))
|> Stream.take(1000)
|> Enum.to_list()
```

### Single-Batch Inserts

For moderate data loads, use the insert API:

```elixir
{:ok, insert} = Chex.insert(conn, "users")

Enum.each(users, fn user ->
  Chex.write(insert, user)
end)

Chex.end_insert(insert)  # Must be called to finalize!
```

### Auto-batching Inserts

For high-throughput scenarios, use the inserter with automatic batching:

```elixir
# Batch by row count
{:ok, inserter} = Chex.inserter(conn, "events", max_rows: 10_000)

# Or by byte size
{:ok, inserter} = Chex.inserter(conn, "events", max_bytes: 1_048_576)

# Or by time period
{:ok, inserter} = Chex.inserter(conn, "events", period_ms: 5_000)

# Write data
Enum.each(events, fn event ->
  Chex.write_batch(inserter, event)
  Chex.commit(inserter)  # Check and commit batch if limits reached
end)

Chex.end_inserter(inserter)  # Finalize all pending batches
```

Inserter options:
- `:max_rows` - Maximum rows per batch (optional)
- `:max_bytes` - Maximum bytes per batch (optional)
- `:period_ms` - Time-based batching in milliseconds (optional)

## Development

### Running ClickHouse

Start ClickHouse with Docker Compose:

```bash
docker-compose up -d
```

Check it's running:

```bash
curl http://localhost:8123/ping
# => Ok.
```

### Compiling the NIF

The Rust NIF will be compiled automatically when you run:

```bash
mix compile
```

### Running Tests

Start ClickHouse first, then run the integration tests:

```bash
docker-compose up -d
mix test
```

### Running Examples

```bash
docker-compose up -d
mix run examples/basic_usage.exs
```

## Architecture

Chex uses a layered architecture:

1. **Rust NIF Layer** (`native/chex_nif/`)
   - Thin wrapper around clickhouse-rs
   - ResourceArc for safe resource management
   - Tokio runtime for async operations

2. **Elixir Low-Level Layer** (`lib/chex/native.ex`)
   - Direct NIF function declarations
   - Minimal abstraction over Rust API

3. **Elixir High-Level Layer** (`lib/chex.ex`, `lib/chex/connection.ex`)
   - GenServer-based connection management
   - Idiomatic Elixir API
   - Stream protocol implementation

## Type Mapping

| ClickHouse Type | Elixir Type |
|-----------------|-------------|
| UInt8-UInt64    | Integer     |
| Int8-Int64      | Integer     |
| Float32/64      | Float       |
| String          | String      |
| Date            | String*     |
| DateTime        | String*     |
| Bool            | Boolean     |
| Array(T)        | List        |
| Nullable(T)     | nil / value |

*Future versions may support native Elixir Date/DateTime types

## Performance Considerations

- **Compression**: Enabled by default, reduces network bandwidth by ~70% for typical workloads
- **Batching**: Use `inserter/3` for bulk inserts (10,000+ rows)
- **Streaming**: Use `stream/3` for large result sets to avoid memory issues
- **Connection Pooling**: HTTP connections are pooled internally by the Rust client

## Roadmap

- [ ] Native Elixir Date/DateTime support
- [ ] Ecto adapter
- [ ] TCP/Native protocol support
- [ ] Prepared statements
- [ ] Connection pooling at Elixir level
- [ ] Async query execution

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - See LICENSE file for details.

## Acknowledgments

Built on top of the excellent [clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) library by the ClickHouse team.
