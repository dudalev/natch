defmodule Chex.Stream do
  @moduledoc """
  Streaming insert operations for large datasets.

  Provides utilities for inserting datasets larger than memory by processing
  them in chunks. This is useful for bulk data loading scenarios where you
  need to insert millions of rows efficiently.
  """

  @doc """
  Streams columnar data in chunks for large datasets.

  Useful for inserts larger than memory. Yields control between chunks,
  allowing the VM to process other work and avoid memory exhaustion.

  ## Parameters

  - `conn` - Connection GenServer
  - `table` - Table name
  - `columns` - Map of column_name => [values] (full dataset)
  - `schema` - Keyword list mapping column names to types
  - `opts` - Options keyword list
    - `:chunk_size` - Number of rows per chunk (default: 10,000)

  ## Examples

      # Stream 1 million rows in 10k chunks
      big_columns = %{
        id: 1..1_000_000 |> Enum.to_list(),
        value: 1..1_000_000 |> Enum.map(& &1 * 2)
      }

      schema = [id: :uint64, value: :uint64]

      # Process in 10k row chunks
      Chex.Stream.insert_stream(conn, "events", big_columns, schema, chunk_size: 10_000)

  ## Performance

  Each chunk is inserted as a separate block. For 1 million rows with
  chunk_size: 10_000, this will result in 100 insert operations.

  Choose chunk_size based on:
  - Available memory (larger chunks = more memory)
  - Network latency (larger chunks = fewer round trips)
  - Typical value: 10,000 - 100,000 rows per chunk
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

      # Insert chunk using public API
      case Chex.insert(conn, table, chunk_columns, schema) do
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
