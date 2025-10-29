# Basic usage example for Chex
#
# Start ClickHouse with:
# docker-compose up -d
#
# Run this example with:
# mix run examples/basic_usage.exs

# Start a connection
{:ok, conn} =
  Chex.start_link(
    url: "http://localhost:8123",
    database: "default",
    compression: true
  )

IO.puts("Connected to ClickHouse")

# Create a table
create_table_sql = """
CREATE TABLE IF NOT EXISTS users (
  id UInt32,
  name String,
  email String,
  age UInt8,
  created_at DateTime
) ENGINE = MergeTree()
ORDER BY id
"""

:ok = Chex.execute(conn, create_table_sql)
IO.puts("Created users table")

# Insert some data using the single-batch insert API
{:ok, insert} = Chex.insert(conn, "users")

users = [
  %{
    "id" => 1,
    "name" => "Alice",
    "email" => "alice@example.com",
    "age" => 30,
    "created_at" => "2024-01-15 10:30:00"
  },
  %{
    "id" => 2,
    "name" => "Bob",
    "email" => "bob@example.com",
    "age" => 25,
    "created_at" => "2024-01-16 14:20:00"
  },
  %{
    "id" => 3,
    "name" => "Charlie",
    "email" => "charlie@example.com",
    "age" => 35,
    "created_at" => "2024-01-17 09:15:00"
  }
]

Enum.each(users, fn user ->
  :ok = Chex.write(insert, user)
end)

:ok = Chex.end_insert(insert)
IO.puts("Inserted #{length(users)} users")

# Wait a moment for ClickHouse to process
Process.sleep(100)

# Query all users
{:ok, all_users} = Chex.query(conn, "SELECT * FROM users ORDER BY id")
IO.puts("\nAll users:")

Enum.each(all_users, fn user ->
  IO.puts("  #{user["id"]}: #{user["name"]} (#{user["email"]})")
end)

# Query with parameters
{:ok, filtered_users} = Chex.query(conn, "SELECT * FROM users WHERE age > ?", [28])
IO.puts("\nUsers older than 28:")

Enum.each(filtered_users, fn user ->
  IO.puts("  #{user["name"]} - age #{user["age"]}")
end)

# Use streaming for large result sets
IO.puts("\nStreaming users:")

conn
|> Chex.stream("SELECT * FROM users ORDER BY id")
|> Stream.map(fn user -> "#{user["name"]} <#{user["email"]}>" end)
|> Enum.each(&IO.puts("  #{&1}"))

# Create an events table for inserter example
create_events_sql = """
CREATE TABLE IF NOT EXISTS events (
  id UInt32,
  event_type String,
  user_id UInt32,
  timestamp DateTime,
  value Float32
) ENGINE = MergeTree()
ORDER BY (timestamp, id)
"""

:ok = Chex.execute(conn, create_events_sql)
IO.puts("\nCreated events table")

# Use auto-batching inserter for high-throughput scenarios
{:ok, inserter} = Chex.inserter(conn, "events", max_rows: 100, period_ms: 1000)

IO.puts("Inserting 500 events with auto-batching...")

for i <- 1..500 do
  event = %{
    "id" => i,
    "event_type" => Enum.random(["click", "view", "purchase"]),
    "user_id" => rem(i, 10) + 1,
    "timestamp" => "2024-01-20 #{rem(i, 24)}:#{rem(i, 60)}:00",
    "value" => :rand.uniform() * 100
  }

  :ok = Chex.write_batch(inserter, event)
  :ok = Chex.commit(inserter)
end

:ok = Chex.end_inserter(inserter)
IO.puts("Completed inserting events")

# Wait for processing
Process.sleep(200)

# Aggregate query
{:ok, stats} =
  Chex.query(conn, """
  SELECT
    event_type,
    count() as count,
    avg(value) as avg_value
  FROM events
  GROUP BY event_type
  ORDER BY count DESC
  """)

IO.puts("\nEvent statistics:")

Enum.each(stats, fn stat ->
  IO.puts(
    "  #{stat["event_type"]}: #{stat["count"]} events, avg value: #{Float.round(stat["avg_value"], 2)}"
  )
end)

# Clean up
:ok = Chex.execute(conn, "DROP TABLE users")
:ok = Chex.execute(conn, "DROP TABLE events")
IO.puts("\nCleaned up test tables")

# Stop the connection
:ok = Chex.stop(conn)
IO.puts("Disconnected")
