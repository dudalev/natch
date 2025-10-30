defmodule Chex.Insert do
  @moduledoc """
  Insert operations for ClickHouse via native TCP protocol.

  Provides high-level API for building blocks from columnar data and inserting
  into ClickHouse tables.

  ## Columnar Format

  Chex uses a columnar format that matches ClickHouse's native storage:

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        amount: [100.0, 200.0, 300.0]
      }

      schema = [id: :uint64, name: :string, amount: :float64]

      Chex.Insert.insert(conn, "users", columns, schema)

  This format is:
  - **10-1000x faster** than row-oriented (1 NIF call per column vs N×M calls)
  - **Matches ClickHouse native format** (no transposition needed)
  - **Natural for analytics** (operate on columns, not rows)

  If you have row-oriented data, use `Chex.Conversion.rows_to_columns/2`.
  """

  alias Chex.{Column, Conversion, Native}

  @doc """
  Inserts columnar data into a table.

  ## Parameters

  - `conn` - Connection GenServer
  - `table` - Table name
  - `columns` - Map of column_name => [values]
  - `schema` - Keyword list mapping column names to types

  ## Schema Types

  - `:uint64` - Unsigned 64-bit integer
  - `:int64` - Signed 64-bit integer
  - `:string` - String
  - `:float64` - Float
  - `:datetime` - DateTime (pass Elixir DateTime, converts to Unix timestamp)

  ## Examples

      # Columnar format (RECOMMENDED)
      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        amount: [100.5, 200.75, 300.25]
      }

      schema = [id: :uint64, name: :string, amount: :float64]

      Chex.Insert.insert(conn, "users", columns, schema)

      # Large batch (efficient!)
      columns = %{
        id: Enum.to_list(1..100_000),
        value: Enum.map(1..100_000, & &1 * 2)
      }

      schema = [id: :uint64, value: :uint64]

      Chex.Insert.insert(conn, "events", columns, schema)
  """
  @spec insert(GenServer.server(), String.t(), map(), keyword()) :: :ok | {:error, term()}
  def insert(conn, table, columns, schema)
      when is_map(columns) and is_list(schema) do
    GenServer.call(conn, {:insert, table, columns, schema}, :infinity)
  end

  @doc """
  Builds a Block from columnar data and schema.

  This is a lower-level function that creates a Block resource without
  inserting it. Useful for testing or custom insertion logic.

  ## Examples

      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      block = Chex.Insert.build_block(columns, schema)
  """
  @spec build_block(map(), keyword()) :: reference()
  def build_block(columns, schema) when is_map(columns) and is_list(schema) do
    # Validate columns
    case Conversion.validate_column_lengths(columns, schema) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, reason
    end

    case Conversion.validate_column_types(columns, schema) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, reason
    end

    # Create empty block
    block = Native.block_create()

    # Build columns using bulk operations
    column_refs = build_columns_bulk(columns, schema)

    # Append each column to the block
    for {name, column_ref} <- column_refs do
      Native.block_append_column(block, to_string(name), column_ref)
    end

    block
  end

  @doc """
  Builds columns from columnar data using bulk append operations.

  Returns a keyword list of {column_name, column_ref} pairs.

  This is the high-performance path: 1 NIF call per column instead of N calls per value.

  ## Examples

      columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      schema = [id: :uint64, name: :string]
      column_refs = Chex.Insert.build_columns_bulk(columns, schema)
  """
  @spec build_columns_bulk(map(), keyword()) :: keyword()
  def build_columns_bulk(columns, schema) when is_map(columns) and is_list(schema) do
    for {name, type} <- schema do
      # Get column values - support both atom and string keys
      values = Map.get(columns, name) || Map.get(columns, to_string(name))

      cond do
        values == nil ->
          raise ArgumentError,
                "Missing column #{inspect(name)} in columns #{inspect(Map.keys(columns))}"

        not is_list(values) ->
          raise ArgumentError,
                "Column #{inspect(name)} must be a list, got: #{inspect(values)}"

        true ->
          # Create column and bulk append all values (single NIF call!)
          column = Column.new(type)
          Column.append_bulk(column, values)
          {name, column.ref}
      end
    end
  end

  @doc """
  Streams columnar data in chunks for large datasets.

  Useful for inserts larger than memory. Yields control between chunks.

  ## Examples

      # Stream 1 million rows in 10k chunks
      big_columns = %{
        id: 1..1_000_000 |> Enum.to_list(),
        value: 1..1_000_000 |> Enum.map(& &1 * 2)
      }

      schema = [id: :uint64, value: :uint64]

      # Process in 10k row chunks
      Chex.Insert.insert_stream(conn, "events", big_columns, schema, chunk_size: 10_000)
  """
  @spec insert_stream(GenServer.server(), String.t(), map(), keyword(), keyword()) ::
          :ok | {:error, term()}
  def insert_stream(conn, table, columns, schema, opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, 10_000)

    # Get row count from first column
    first_column_name = schema |> hd() |> elem(0)
    row_count = length(Map.fetch!(columns, first_column_name))

    # Process in chunks
    0..(row_count - 1)
    |> Stream.chunk_every(chunk_size)
    |> Stream.each(fn row_indices ->
      # Extract chunk for each column
      chunk_columns =
        for {name, _type} <- schema, into: %{} do
          column_values = Map.fetch!(columns, name)
          chunk_values = Enum.map(row_indices, fn idx -> Enum.at(column_values, idx) end)
          {name, chunk_values}
        end

      # Insert chunk
      case insert(conn, table, chunk_columns, schema) do
        :ok -> :ok
        {:error, reason} -> throw({:error, reason})
      end
    end)
    |> Stream.run()

    :ok
  catch
    {:error, reason} -> {:error, reason}
  end
end
