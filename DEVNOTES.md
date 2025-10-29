# Chex Development Notes

## Project Overview

**Chex** is an Elixir client library for ClickHouse database that wraps the Rust `clickhouse-rs` library via Rustler NIFs (Native Implemented Functions).

### Goals

1. Provide a high-performance Elixir interface to ClickHouse
2. Leverage Rust's `clickhouse-rs` for performance and reliability
3. Support both simple query operations and high-throughput streaming inserts
4. Offer two API layers:
   - Low-level: Direct NIF calls for maximum control
   - High-level: Idiomatic Elixir API with GenServer-based connection management

### Design Philosophy

- **Dynamic typing**: Elixir uses runtime types (maps, lists) while Rust/ClickHouse are statically typed
- **JSON bridge**: Use JSON as the interchange format between Elixir and ClickHouse
- **Streaming**: Support lazy evaluation for large result sets
- **Auto-batching**: Optimize write throughput with configurable batching

---

## Architecture

### Layer Structure

```
┌─────────────────────────────────────────────────────┐
│  Elixir Application Layer                           │
│  - User-facing API                                  │
│  - Chex module (query/insert/stream functions)     │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  Elixir Management Layer                            │
│  - Chex.Connection GenServer                        │
│  - Connection lifecycle management                  │
│  - Client reference storage                         │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  Elixir NIF Declarations                            │
│  - Chex.Native module                               │
│  - Function stubs for NIF calls                     │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  Rust NIF Implementation (native/chex_nif/)         │
│  - Client resource (ResourceArc)                    │
│  - Query operations                                 │
│  - Insert/Inserter operations                       │
│  - Term ↔ JSON conversion                           │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  clickhouse-rs Library (v0.14.0)                    │
│  - HTTP client to ClickHouse                        │
│  - Query builder and execution                      │
│  - Compression (LZ4)                                │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  ClickHouse Server                                  │
│  - HTTP interface (port 8123)                       │
│  - SQL query processing                             │
│  - Data storage and retrieval                       │
└─────────────────────────────────────────────────────┘
```

### Data Flow

**Query Flow:**
```
Elixir map/params → term_to_json() → JSON Value → Query.bind()
                                                        ↓
ClickHouse ← HTTP ← fetch_bytes("JSONEachRow") ← clickhouse-rs
     ↓
JSON strings → parse → JSON Values → json_to_term() → Elixir maps
```

**Insert Flow (Attempted):**
```
Elixir maps → term_to_json() → JSON Values → Buffer
                                                  ↓
                                    JSONEachRow format string
                                                  ↓
                                    HTTP POST with body
                                                  ↓
                                            ClickHouse
```

---

## Current Implementation Status

### ✅ Fully Working Components

#### 1. Connection Management (`lib/chex/connection.ex`, `native/chex_nif/src/client.rs`)

**Status:** Complete and functional

**Features:**
- GenServer-based connection lifecycle
- Configuration options (URL, database, user, password, compression)
- Client stored as ResourceArc (thread-safe reference counting)
- Builder pattern from clickhouse-rs wrapped in NIFs

**Implementation:**
```rust
// Rust side
pub struct ClientResource {
    pub client: Client,
}

#[rustler::nif]
pub fn client_with_url(url: String) -> ChexResult<ResourceArc<ClientResource>>

#[rustler::nif]
pub fn client_with_database(client_res: ResourceArc<ClientResource>, database: String) -> ...
```

```elixir
# Elixir side
def start_link(opts \\ []) do
  GenServer.start_link(__MODULE__, opts)
end

def init(opts) do
  case build_client(opts) do
    {:ok, client} -> {:ok, %{client: client}}
    {:error, reason} -> {:stop, reason}
  end
end
```

#### 2. Query Operations (`native/chex_nif/src/query.rs`)

**Status:** Complete and functional

**Approach:** Use JSONEachRow format as the interchange format

**Implementation Details:**
```rust
pub fn query_execute(
    client_res: ResourceArc<ClientResource>,
    sql: String,
    params: Vec<Term>,
) -> ChexResult<String> {
    // 1. Build query from SQL
    let mut query = client_res.client.query(&sql);

    // 2. Bind parameters (converted from Elixir terms to JSON values)
    for param in params {
        let value = term_conv::term_to_json(param)?;
        query = bind_value(query, value)?;
    }

    // 3. Execute (no results expected)
    query.execute().await?;
}

pub fn query_fetch_all(
    env: Env,
    client_res: ResourceArc<ClientResource>,
    sql: String,
    params: Vec<Term>,
) -> ChexResult<Vec<Term>> {
    // 1. Build and bind query
    let mut query = client_res.client.query(&sql);
    for param in params {
        query = bind_value(query, term_conv::term_to_json(param)?)?;
    }

    // 2. Fetch as JSONEachRow format
    let mut cursor = query.fetch_bytes("JSONEachRow")?;

    // 3. Read all chunks
    let mut bytes_vec = Vec::new();
    while let Some(chunk) = cursor.next().await? {
        bytes_vec.extend_from_slice(&chunk);
    }

    // 4. Parse newline-delimited JSON
    let text = String::from_utf8(bytes_vec)?;
    for line in text.lines() {
        let json_value: Value = serde_json::from_str(line)?;
        rows.push(term_conv::json_to_term(env, &json_value));
    }
}
```

**Why this works:**
- ClickHouse's JSONEachRow format outputs one JSON object per line
- We can parse JSON directly without dealing with ClickHouse's typed Row system
- Full flexibility - any query result structure is supported

#### 3. Term ↔ JSON Conversion (`native/chex_nif/src/term_conv.rs`)

**Status:** Complete and functional

**Purpose:** Bridge between Elixir's dynamic typing and JSON

**Implementation:**
```rust
pub fn term_to_json<'a>(term: Term<'a>) -> ChexResult<Value> {
    // Convert Elixir term to serde_json::Value
    // Supports: integers, floats, strings, bools, atoms, lists, maps
}

pub fn json_to_term<'a>(env: Env<'a>, value: &Value) -> Term<'a> {
    // Convert serde_json::Value to Elixir term
    // Maps JSON types to Elixir equivalents
}
```

**Mappings:**
```
Elixir          JSON            ClickHouse
------          ----            ----------
integer    <->  number     <->  Int*, UInt*
float      <->  number     <->  Float*
string     <->  string     <->  String
boolean    <->  boolean    <->  Bool
nil        <->  null       <->  NULL
list       <->  array      <->  Array(T)
map        <->  object     <->  various types
```

#### 4. High-Level Elixir API (`lib/chex.ex`)

**Status:** Complete

**Features:**
```elixir
# Connection
{:ok, conn} = Chex.start_link(url: "http://localhost:8123")

# Queries
{:ok, rows} = Chex.query(conn, "SELECT * FROM users WHERE id = ?", [42])
rows = Chex.query!(conn, "SELECT * FROM users")

# DDL operations
:ok = Chex.execute(conn, "CREATE TABLE ...")

# Streaming (lazy evaluation)
conn
|> Chex.stream("SELECT * FROM large_table")
|> Stream.take(100)
|> Enum.to_list()
```

### ⚠️ Partially Implemented Components

#### 1. Insert Operations (`native/chex_nif/src/insert.rs`)

**Status:** Code written, not yet compiled

**Approach Attempted:** Buffer rows in memory, send as JSONEachRow on `end_insert`

**Current Implementation:**
```rust
pub struct InsertResource {
    table: String,
    rows: Mutex<Vec<Value>>,  // Buffered rows as JSON
}

pub fn insert_new() -> ChexResult<ResourceArc<InsertResource>> {
    // Just create a buffer, no ClickHouse interaction yet
}

pub fn insert_write(insert_res: ResourceArc<InsertResource>, row: Term) -> ChexResult<String> {
    // Convert term to JSON and add to buffer
    let value = term_conv::term_to_json(row)?;
    guard.push(value);
}

pub fn insert_end(
    client_res: ResourceArc<ClientResource>,
    insert_res: ResourceArc<InsertResource>,
) -> ChexResult<String> {
    // Build JSONEachRow format string
    let mut json_data = String::new();
    for row in guard.iter() {
        json_data.push_str(&serde_json::to_string(row)?);
        json_data.push('\n');
    }

    // Send to ClickHouse (THIS IS WHERE WE'RE STUCK)
    let sql = format!("INSERT INTO {} FORMAT JSONEachRow", insert_res.table);
    let query = client_res.client.query(&sql);
    query.with_body(json_data).execute().await?;  // ← with_body doesn't exist
}
```

**The Problem:**
- `clickhouse-rs` v0.14.0's Query type doesn't have a `with_body()` method
- The library's Insert API requires compile-time typed Row structs
- We can't dynamically insert JSON data using the provided Insert API

#### 2. Inserter Operations (`native/chex_nif/src/inserter.rs`)

**Status:** Same issue as Insert operations

**Attempted Approach:** Auto-batching based on max_rows/max_bytes with periodic commits

**Same fundamental problem:** No way to send raw JSON data to ClickHouse via clickhouse-rs Insert API

---

## Technical Challenges

### Challenge 1: Type System Mismatch

**The Core Issue:**

```rust
// clickhouse-rs expects this:
#[derive(Row, Serialize, Deserialize)]
struct User {
    id: u32,
    name: String,
    age: u8,
}

let mut insert = client.insert::<User>("users").await?;
insert.write(&User { id: 1, name: "Alice", age: 30 }).await?;
```

```elixir
# But we have this:
row = %{"id" => 1, "name" => "Alice", "age" => 30}
# Structure is dynamic, known only at runtime
```

**Why This Matters:**
- Rust requires compile-time knowledge of data structures
- Elixir's maps can have any shape
- No way to define Row structs at runtime

### Challenge 2: clickhouse-rs API Limitations

**What We Need:**
```rust
// Ideal API (doesn't exist)
let json_data = r#"{"id":1,"name":"Alice","age":30}"#;
client.execute_raw(
    "INSERT INTO users FORMAT JSONEachRow",
    json_data
).await?;
```

**What's Available:**
```rust
// Requires compile-time Row type
let mut insert = client.insert::<SomeRowType>("users").await?;
insert.write(&typed_row).await?;
insert.end().await?;
```

**Queries Work Because:**
- Output format can be specified: `fetch_bytes("JSONEachRow")`
- We parse the response ourselves
- No compile-time type constraints on results

**Inserts Don't Work Because:**
- Input must be a typed Row struct
- No way to specify "accept arbitrary JSON" as input format
- The library serializes data internally using its row binary format

### Challenge 3: Async Runtime in NIFs

**Current Approach:**
```rust
#[rustler::nif]
pub fn query_fetch_all(...) -> ChexResult<...> {
    // Create a new Tokio runtime for each NIF call
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        // Async clickhouse-rs operations here
    })
}
```

**Issues:**
- Creating a runtime per call has overhead
- Blocking the NIF scheduler
- Works for now but not optimal for high-throughput scenarios

**Better Approach (Future):**
- Use dirty schedulers for long-running operations
- Or maintain a global Tokio runtime
- Or use `rustler::spawn` for async operations

---

## Attempted Solutions

### Attempt 1: Use serde_rustler (Failed)

**Goal:** Automatic conversion between Elixir terms and Rust structs

**Problem:**
- `serde_rustler` 0.1.0 depends on `rustler` 0.21.x
- Our project uses `rustler` 0.35.x
- Incompatible versions, compilation fails

**Lesson:** Avoid transitive dependency version conflicts

### Attempt 2: Dynamic Row Type with serde_json::Value (Failed)

**Goal:** Use JSON Value as a Row type

```rust
impl clickhouse::Row for serde_json::Value {
    // ...
}
```

**Problem:**
- clickhouse::Row requires implementing Primitive trait
- serde_json::Value doesn't implement Primitive
- Can't add external trait to external type (orphan rule)

**Lesson:** Can't fake our way around the type system

### Attempt 3: Manual Term Conversion (Success for Queries)

**Goal:** Write custom term_to_json and json_to_term functions

**Result:** ✅ Works perfectly for query results

**Why it works:**
- Full control over conversion logic
- No dependency conflicts
- Handles all Elixir types (atoms, lists, maps, etc.)

### Attempt 4: Buffer and Batch JSON (Current, Incomplete)

**Goal:**
1. Buffer rows as JSON Values in memory
2. Serialize to JSONEachRow format on commit
3. Send raw HTTP POST to ClickHouse

**Status:**
- Steps 1-2: ✅ Complete
- Step 3: ❌ Blocked - no API to send raw body data

---

## Environment Setup

### Versions

```
Elixir: 1.18.4
Erlang: 27.2.2
Rust: nightly-1.93.0 (required for clickhouse-rs)
clickhouse-rs: v0.14.0 (git tag checked out locally)
```

### Local Dependencies

```toml
# native/chex_nif/Cargo.toml
clickhouse = { path = "/Users/brendon/work/clickhouse-rs", features = ["lz4"] }
```

**Why local path:**
- Development version of clickhouse-rs
- Can check out specific git tags
- Currently on v0.14.0 tag

### Key Rust Dependencies

```toml
rustler = "0.35.0"           # NIF framework
clickhouse = { ... }          # ClickHouse client
tokio = { version = "1", features = ["full"] }  # Async runtime
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"           # JSON serialization
```

### Key Elixir Dependencies

```elixir
{:rustler, "~> 0.35.0"}      # NIF framework
{:jason, "~> 1.4"}           # JSON (not actually used yet)
{:ex_doc, "~> 0.34", ...}    # Documentation
```

---

## File Structure

```
chex/
├── lib/
│   ├── chex.ex                    # High-level API
│   ├── chex/
│   │   ├── application.ex         # OTP application
│   │   ├── connection.ex          # GenServer for connection management
│   │   └── native.ex              # NIF function declarations
├── native/
│   └── chex_nif/
│       ├── Cargo.toml             # Rust dependencies
│       └── src/
│           ├── lib.rs             # Rustler init, module declarations
│           ├── client.rs          # Client resource and NIFs
│           ├── query.rs           # Query NIFs (WORKING)
│           ├── insert.rs          # Insert NIFs (INCOMPLETE)
│           ├── inserter.rs        # Inserter NIFs (INCOMPLETE)
│           ├── term_conv.rs       # Term ↔ JSON conversion
│           └── error.rs           # Error type and conversions
├── test/
│   ├── test_helper.exs
│   └── chex_test.exs              # Integration tests
├── examples/
│   └── basic_usage.exs            # Usage examples
├── docker-compose.yml             # ClickHouse for testing
├── .tool-versions                 # asdf version management
├── README.md                      # User-facing documentation
└── DEVNOTES.md                    # This file
```

---

## Compilation Status

### What Compiles

- ✅ All Elixir code
- ✅ Rust NIF infrastructure (lib.rs, client.rs, error.rs, term_conv.rs)
- ✅ Query operations (query.rs)

### What Doesn't Compile

- ❌ Insert operations (insert.rs) - API incompatibility
- ❌ Inserter operations (inserter.rs) - same issue

### Current Errors

```
error[E0599]: no method named `with_body` found for struct `clickhouse::query::Query`
  --> src/insert.rs:70:14
   |
70 |         query.with_body(json_data)
   |              ^^^^^^^^^ method not found in `Query`
```

**Root Cause:** The clickhouse-rs Query type doesn't support attaching arbitrary body data for INSERT operations.

---

## Possible Solutions

### Option 1: Raw HTTP Client (Recommended)

**Approach:** Bypass clickhouse-rs for insert operations, use HTTP directly

**Implementation:**
```rust
use hyper::{Client, Request, Body};

pub fn insert_end(...) -> ChexResult<String> {
    // Build JSONEachRow data
    let mut json_data = String::new();
    for row in guard.iter() {
        json_data.push_str(&serde_json::to_string(row)?);
        json_data.push('\n');
    }

    // Make raw HTTP POST
    let url = format!("{}/? query=INSERT%20INTO%20{}%20FORMAT%20JSONEachRow",
        clickhouse_url, table_name);

    let req = Request::builder()
        .method("POST")
        .uri(url)
        .header("Content-Type", "application/json")
        .body(Body::from(json_data))?;

    let resp = client.request(req).await?;
    // Check status, return result
}
```

**Pros:**
- Full control over HTTP requests
- Can send any format (JSONEachRow, CSV, etc.)
- No type system constraints
- Proven approach (ClickHouse docs recommend it)

**Cons:**
- Need to manage HTTP client separately from clickhouse-rs
- Lose some features (connection pooling, retries, etc.)
- More manual error handling

**Estimated Effort:** 2-3 hours

### Option 2: Simplify Insert API

**Approach:** Send each insert immediately, no batching

**Implementation:**
```rust
pub fn insert_write(client_res, table, row) -> ChexResult<String> {
    let json = serde_json::to_string(&term_conv::term_to_json(row)?)?;

    // Send immediately as single-row INSERT
    let url = format!("{}/? query=INSERT%20INTO%20{}%20FORMAT%20JSONEachRow",
        client_url, table);

    http_post(url, json).await?;
}

// No need for insert_end - each write is independent
```

**Pros:**
- Simpler API
- No buffering complexity
- Each operation is atomic

**Cons:**
- Many small HTTP requests
- Lower throughput for bulk inserts
- More network overhead

**Estimated Effort:** 1-2 hours

### Option 3: Use Different Rust Crate

**Approach:** Replace clickhouse-rs with a more flexible client

**Candidates:**
- Raw `reqwest` or `hyper` - HTTP clients
- Custom HTTP wrapper around ClickHouse HTTP interface

**Pros:**
- Complete control
- No type system constraints
- Can implement exactly what we need

**Cons:**
- Lose clickhouse-rs features (compression, connection pooling, retries)
- More code to maintain
- Need to reimplement query operations too

**Estimated Effort:** 1-2 days

### Option 4: Code Generation

**Approach:** Generate Rust Row structs from Elixir at compile time

**Implementation:**
```elixir
# At compile time
defmodule Chex.Schema do
  defmacro table(name, fields) do
    # Generate Rust code for Row struct
    # Compile into NIF
  end
end

# Usage
use Chex.Schema
table :users, [id: :integer, name: :string, age: :integer]
```

**Pros:**
- Type safety
- Uses clickhouse-rs properly
- Better performance (no JSON overhead)

**Cons:**
- Complex implementation
- Requires macros and compile-time code generation
- Less flexible for dynamic schemas
- Major architecture change

**Estimated Effort:** 1-2 weeks

---

## Recommended Next Steps

### Immediate (1-2 days)

1. **Implement Option 1** (Raw HTTP for inserts)
   - Add `hyper` or `reqwest` dependency
   - Implement HTTP POST for JSONEachRow inserts
   - Test with simple inserts
   - Verify auto-batching works

2. **Complete Test Suite**
   - Start ClickHouse via Docker Compose
   - Run integration tests
   - Fix any bugs discovered

3. **Update Documentation**
   - Update README with current status
   - Add notes about insert implementation
   - Document any limitations

### Short Term (1 week)

1. **Performance Testing**
   - Benchmark query performance
   - Test insert throughput
   - Profile NIF call overhead

2. **Error Handling**
   - Test error scenarios
   - Improve error messages
   - Add retry logic where appropriate

3. **Connection Pooling**
   - Evaluate if current approach is sufficient
   - Consider connection pool at Elixir level
   - Test under concurrent load

### Medium Term (2-4 weeks)

1. **Streaming Improvements**
   - Implement true streaming queries (not fetch_all)
   - Support backpressure
   - Add GenStage integration option

2. **Data Type Support**
   - Native DateTime conversion
   - UUID support
   - Array and nested types

3. **Advanced Features**
   - Prepared statements
   - Transactions (if supported by ClickHouse)
   - Cluster support

### Long Term (1-3 months)

1. **Ecto Adapter**
   - Implement Ecto.Adapter behavior
   - Schema definitions
   - Migration support

2. **Performance Optimization**
   - Reuse Tokio runtime
   - Dirty scheduler integration
   - Memory pooling

3. **Production Readiness**
   - Comprehensive logging
   - Metrics/telemetry
   - Connection health checks
   - Circuit breaker pattern

---

## Testing Strategy

### Current Test Coverage

```elixir
# test/chex_test.exs
describe "connection" do
  # GenServer lifecycle
end

describe "execute/3" do
  # DDL operations (CREATE, DROP)
end

describe "query/3" do
  # SELECT with parameters
  # Empty results
  # Multiple rows
end

describe "stream/3" do
  # Lazy evaluation
  # Stream operations
end

describe "insert/2" do
  # Single row
  # Multiple rows
  # with end_insert
end

describe "inserter/3" do
  # Auto-batching by row count
  # Auto-batching by byte size
end
```

### Test Setup

```yaml
# docker-compose.yml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"  # HTTP interface
```

```bash
# Run tests
docker-compose up -d
mix test
```

### Integration Test Pattern

```elixir
setup do
  {:ok, conn} = Chex.start_link(url: "http://localhost:8123")

  on_exit(fn ->
    Chex.execute(conn, "DROP TABLE IF EXISTS test_table")
    Chex.stop(conn)
  end)

  {:ok, conn: conn}
end
```

---

## Known Issues and Limitations

### Current Limitations

1. **Insert Operations Not Working**
   - Core functionality incomplete
   - Blocks production use

2. **No Streaming for Queries**
   - `stream/3` currently calls `query/3` under the hood
   - Loads all results into memory first
   - Not truly lazy

3. **Date/Time Types**
   - Returned as strings from ClickHouse
   - Need to parse manually in Elixir
   - No automatic DateTime conversion

4. **NIF Runtime Creation**
   - New Tokio runtime per NIF call
   - Performance overhead
   - Not optimal for high-frequency calls

5. **No Connection Pooling at Elixir Level**
   - Single connection per GenServer
   - Concurrent queries share connection via clickhouse-rs
   - May need explicit pool for very high concurrency

### Design Trade-offs

1. **JSON as Interchange Format**
   - Pro: Flexible, easy to work with
   - Con: Serialization overhead vs binary formats
   - Decision: Flexibility more important for Elixir use case

2. **GenServer per Connection**
   - Pro: Supervision, standard Elixir pattern
   - Con: One process overhead per connection
   - Decision: Standard pattern, overhead acceptable

3. **Blocking NIFs**
   - Pro: Simpler implementation
   - Con: Can block scheduler if operations are slow
   - Decision: OK for now, move to dirty schedulers later if needed

---

## Performance Considerations

### Query Performance

**Current Implementation:**
```
Elixir → NIF → Tokio runtime → clickhouse-rs → HTTP → ClickHouse
                                                          ↓
Elixir ← NIF ← Parse JSON ← JSONEachRow ← HTTP ← ClickHouse
```

**Overhead Sources:**
1. Tokio runtime creation (~few µs)
2. Term ↔ JSON conversion (~depends on data size)
3. HTTP round trip (~network latency)
4. JSON parsing (~depends on result size)

**Expected Performance:**
- Small queries (<1000 rows): 1-10ms
- Medium queries (1K-100K rows): 10-100ms
- Large queries (>100K rows): 100ms-seconds

### Insert Performance (When Implemented)

**Batching Strategy:**
```rust
// Option 1: Row count
max_rows: 10_000  // Commit every 10K rows

// Option 2: Byte size
max_bytes: 1_048_576  // Commit every 1MB

// Option 3: Time-based
period_ms: 5_000  // Commit every 5 seconds
```

**Expected Throughput:**
- Single inserts: 100-1000/sec
- Batched inserts: 10K-100K/sec
- Depends heavily on: network latency, ClickHouse hardware, data size

---

## Debugging Tips

### Enable Rust Logging

```rust
// Add to any function
println!("Debug: value = {:?}", value);
```

### Enable Elixir Logging

```elixir
require Logger
Logger.debug("Connection state: #{inspect(state)}")
```

### Check ClickHouse Logs

```bash
docker-compose logs -f clickhouse
```

### Inspect NIF Resources

```elixir
# Resources are opaque references
# Can check if they're valid but not inspect contents
is_reference(client)  # true
```

### Test Single Operation

```elixir
# Start IEx with project
iex -S mix

# Test connection
{:ok, conn} = Chex.start_link(url: "http://localhost:8123")

# Test query
Chex.execute(conn, "SELECT 1")

# Check errors
{:error, reason} = Chex.query(conn, "INVALID SQL")
IO.inspect(reason)
```

---

## Resources

### Documentation

- [ClickHouse HTTP Interface](https://clickhouse.com/docs/en/interfaces/http/)
- [clickhouse-rs Documentation](https://docs.rs/clickhouse/latest/clickhouse/)
- [Rustler Guide](https://github.com/rusterlium/rustler)
- [ClickHouse JSONEachRow Format](https://clickhouse.com/docs/en/interfaces/formats#jsoneachrow)

### Examples

- `/Users/brendon/work/clickhouse-rs/examples/` - clickhouse-rs examples
- `examples/basic_usage.exs` - Chex usage examples (once working)

### Similar Projects

- [Pillar](https://github.com/balance-platform/pillar) - Pure Elixir ClickHouse client
- [ClickHousex](https://github.com/clickhouse-elixir/clickhousex) - Another Elixir client
- [clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) - Official Rust client

### Community

- ClickHouse Slack: #rust channel
- Elixir Forum: database section
- GitHub Issues: both clickhouse-rs and Rustler

---

## Decision Log

### Why Rust/Rustler Instead of Pure Elixir?

**Considered:**
1. Pure Elixir HTTP client
2. Rust NIF wrapper
3. Port/external process

**Decision:** Rust NIF wrapper

**Reasoning:**
- Performance: Rust's speed for serialization/parsing
- Leverage: Use official clickhouse-rs library
- Types: Better type safety at serialization layer
- Ecosystem: Access to Rust crates for compression, etc.

### Why JSONEachRow Instead of RowBinary?

**Considered:**
1. RowBinary - clickhouse-rs native format
2. JSONEachRow - newline-delimited JSON
3. Native protocol - TCP-based

**Decision:** JSONEachRow

**Reasoning:**
- Flexibility: No compile-time type constraints
- Simplicity: Easy to parse and generate
- Debugging: Human-readable format
- Trade-off: Performance loss acceptable for flexibility

### Why GenServer for Connection Management?

**Considered:**
1. GenServer with state
2. Agent for simple state
3. No process, direct NIF calls

**Decision:** GenServer

**Reasoning:**
- Supervision: Fits OTP supervision tree
- Lifecycle: Clean connection setup/teardown
- Standard: Familiar pattern for Elixir developers
- Future: Room to add connection pooling, health checks

### Why Buffer Inserts Instead of Immediate Send?

**Considered:**
1. Send each row immediately
2. Buffer and batch
3. Stream directly to ClickHouse

**Decision:** Buffer and batch (attempted)

**Reasoning:**
- Performance: Fewer HTTP requests
- Throughput: Better utilization of network
- ClickHouse: Optimized for batch inserts
- Trade-off: Memory usage vs network efficiency

---

## Glossary

**NIF (Native Implemented Function):** Erlang/Elixir's way of calling native code (C, Rust, etc.) from Erlang VM

**Rustler:** Framework for writing safe Rust NIFs

**ResourceArc:** Rustler's wrapper around Arc (atomic reference counting) for sharing data between Rust and Erlang VM safely

**GenServer:** Generic server behavior in Elixir for stateful processes

**ClickHouse:** Column-oriented database management system

**JSONEachRow:** ClickHouse format where each row is a JSON object on its own line

**Tokio:** Async runtime for Rust

**clickhouse-rs:** Official Rust client library for ClickHouse

**Row trait:** Rust trait that defines how to serialize/deserialize ClickHouse rows

**Term:** Erlang's representation of any data value

**ResourceArc:** Safe wrapper around Arc that allows sharing Rust data with the BEAM

---

## Version History

**v0.1.0 (Current)** - 2024-10-28
- Initial project structure
- Connection management working
- Query operations working
- Insert operations incomplete
- Status: Development, not production-ready

---

## Contact / Maintainers

This is a development project. The implementation is based on:
- clickhouse-rs v0.14.0
- Rustler v0.35.0
- Elixir 1.18+

Last Updated: 2024-10-28
