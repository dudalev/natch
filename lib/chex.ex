defmodule Chex do
  @moduledoc """
  Elixir client for ClickHouse database.

  Chex provides a high-level API for interacting with ClickHouse through
  the Rust clickhouse-rs library, offering both simple query operations
  and high-performance streaming inserts.

  ## Quick Start

      # Start a connection
      {:ok, conn} = Chex.start_link(
        url: "http://localhost:8123",
        database: "default"
      )

      # Execute a query and get all results
      {:ok, rows} = Chex.query(conn, "SELECT * FROM users WHERE id = ?", [42])

      # Stream results lazily
      conn
      |> Chex.stream("SELECT * FROM large_table")
      |> Stream.take(100)
      |> Enum.to_list()

      # Insert data
      {:ok, insert} = Chex.insert(conn, "users")
      :ok = Chex.write(insert, %{id: 1, name: "Alice"})
      :ok = Chex.end_insert(insert)

      # Auto-batching inserter for high throughput
      {:ok, inserter} = Chex.inserter(conn, "events", max_rows: 10_000)
      Enum.each(events, fn event ->
        Chex.write_batch(inserter, event)
        Chex.commit(inserter)
      end)
      Chex.end_inserter(inserter)
  """

  alias Chex.{Connection, Native}

  @type conn :: pid() | atom()
  @type insert :: {reference(), reference()}
  @type inserter :: {reference(), reference()}
  @type row :: map()

  # Connection Management

  @doc """
  Starts a new connection to ClickHouse.

  ## Options

  - `:url` - ClickHouse HTTP endpoint (default: "http://localhost:8123")
  - `:database` - Database name (default: "default")
  - `:user` - Username (optional)
  - `:password` - Password (optional)
  - `:compression` - Enable LZ4 compression (default: true)
  - `:name` - Process name for registration (optional)

  ## Examples

      {:ok, conn} = Chex.start_link(url: "http://localhost:8123")
      {:ok, conn} = Chex.start_link(database: "analytics", user: "readonly")
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    Connection.start_link(opts)
  end

  @doc """
  Stops a connection.
  """
  @spec stop(conn()) :: :ok
  def stop(conn) do
    GenServer.stop(conn)
  end

  # Query Operations

  @doc """
  Executes a query and returns all results as a list.

  ## Parameters

  - `conn` - Connection process
  - `sql` - SQL query string (supports `?` placeholders)
  - `params` - List of parameters to bind (default: [])

  ## Examples

      {:ok, rows} = Chex.query(conn, "SELECT * FROM users")
      {:ok, rows} = Chex.query(conn, "SELECT * FROM users WHERE id = ?", [42])
      {:ok, rows} = Chex.query(conn, "SELECT * FROM users WHERE status = ? AND age > ?", ["active", 18])
  """
  @spec query(conn(), String.t(), list()) :: {:ok, [row()]} | {:error, term()}
  def query(conn, sql, params \\ []) do
    with {:ok, client} <- Connection.get_client(conn) do
      Native.query_fetch_all(client, sql, params)
    end
  end

  @doc """
  Executes a query and returns all results, raising on error.
  """
  @spec query!(conn(), String.t(), list()) :: [row()]
  def query!(conn, sql, params \\ []) do
    case query(conn, sql, params) do
      {:ok, rows} -> rows
      {:error, reason} -> raise "Query failed: #{inspect(reason)}"
    end
  end

  @doc """
  Executes a DDL or DML statement without returning results.

  Useful for CREATE, DROP, ALTER, and DELETE statements.

  ## Examples

      :ok = Chex.execute(conn, "CREATE TABLE users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id")
      :ok = Chex.execute(conn, "DROP TABLE users")
      :ok = Chex.execute(conn, "DELETE FROM users WHERE id = ?", [42])
  """
  @spec execute(conn(), String.t(), list()) :: :ok | {:error, term()}
  def execute(conn, sql, params \\ []) do
    with {:ok, client} <- Connection.get_client(conn),
         {:ok, _} <- Native.query_execute(client, sql, params) do
      :ok
    end
  end

  @doc """
  Returns a lazy stream of query results.

  Results are fetched on-demand as the stream is consumed, allowing
  efficient processing of large result sets.

  ## Examples

      conn
      |> Chex.stream("SELECT * FROM large_table")
      |> Stream.filter(&(&1["status"] == "active"))
      |> Stream.map(&(&1["name"]))
      |> Enum.take(100)
  """
  @spec stream(conn(), String.t(), list()) :: Enumerable.t()
  def stream(conn, sql, params \\ []) do
    Stream.resource(
      fn -> {:ok, conn, sql, params} end,
      fn
        {:ok, conn, sql, params} ->
          case query(conn, sql, params) do
            {:ok, rows} -> {rows, :done}
            {:error, _} = error -> {[error], :done}
          end

        :done ->
          {:halt, :done}
      end,
      fn _ -> :ok end
    )
  end

  # Insert Operations

  @doc """
  Creates a new insert operation for a table.

  Returns an insert reference that can be used with `write/2` and `end_insert/1`.

  ## Examples

      {:ok, insert} = Chex.insert(conn, "users")
      :ok = Chex.write(insert, %{id: 1, name: "Alice"})
      :ok = Chex.write(insert, %{id: 2, name: "Bob"})
      :ok = Chex.end_insert(insert)
  """
  @spec insert(conn(), String.t()) :: {:ok, insert()} | {:error, term()}
  def insert(conn, table) do
    with {:ok, client} <- Connection.get_client(conn),
         {:ok, insert_ref} <- Native.insert_new(client, table) do
      {:ok, {client, insert_ref}}
    end
  end

  @doc """
  Writes a row to an insert operation.

  The row should be a map with keys matching the table columns.

  ## Examples

      :ok = Chex.write(insert, %{id: 1, name: "Alice", age: 30})
  """
  @spec write(insert(), row()) :: :ok | {:error, term()}
  def write({_client, insert_ref}, row) do
    case Native.insert_write(insert_ref, row) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Finalizes an insert operation.

  This must be called to ensure all buffered data is sent to ClickHouse.

  ## Examples

      :ok = Chex.end_insert(insert)
  """
  @spec end_insert(insert()) :: :ok | {:error, term()}
  def end_insert({client, insert_ref}) do
    case Native.insert_end(client, insert_ref) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  # Inserter Operations (Auto-batching)

  @doc """
  Creates a new auto-batching inserter for high-throughput scenarios.

  The inserter automatically creates multiple INSERT statements based on
  configured limits (rows, bytes, or time period).

  ## Options

  - `:max_rows` - Maximum rows per batch (optional)
  - `:max_bytes` - Maximum bytes per batch (optional)
  - `:period_ms` - Time-based batching in milliseconds (optional)

  ## Examples

      # Batch by row count
      {:ok, inserter} = Chex.inserter(conn, "events", max_rows: 10_000)

      # Batch by size and time
      {:ok, inserter} = Chex.inserter(conn, "events",
        max_bytes: 1_048_576,
        period_ms: 5_000
      )
  """
  @spec inserter(conn(), String.t(), keyword()) :: {:ok, inserter()} | {:error, term()}
  def inserter(conn, table, opts \\ []) do
    max_rows = Keyword.get(opts, :max_rows)
    max_bytes = Keyword.get(opts, :max_bytes)
    period_ms = Keyword.get(opts, :period_ms)

    with {:ok, client} <- Connection.get_client(conn),
         {:ok, inserter_ref} <- Native.inserter_new(client, table, max_rows, max_bytes, period_ms) do
      {:ok, {client, inserter_ref}}
    end
  end

  @doc """
  Writes a row to an auto-batching inserter.

  ## Examples

      :ok = Chex.write_batch(inserter, %{id: 1, value: 100})
  """
  @spec write_batch(inserter(), row()) :: :ok | {:error, term()}
  def write_batch({_client, inserter_ref}, row) do
    case Native.inserter_write(inserter_ref, row) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Commits the current batch if size or time limits are reached.

  Should be called periodically, typically after each `write_batch/2`.

  ## Examples

      :ok = Chex.commit(inserter)
  """
  @spec commit(inserter()) :: :ok | {:error, term()}
  def commit({client, inserter_ref}) do
    case Native.inserter_commit(client, inserter_ref) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Finalizes an inserter and ensures all pending batches are sent.

  ## Examples

      :ok = Chex.end_inserter(inserter)
  """
  @spec end_inserter(inserter()) :: :ok | {:error, term()}
  def end_inserter({client, inserter_ref}) do
    case Native.inserter_end(client, inserter_ref) do
      {:ok, _} -> :ok
      error -> error
    end
  end
end
