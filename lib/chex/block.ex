defmodule Chex.Block do
  @moduledoc """
  Pure functions for building ClickHouse blocks from columnar data.

  This module provides the core block-building logic without any GenServer
  coordination. Blocks are the fundamental unit for inserting data into
  ClickHouse via the native TCP protocol.

  ## Columnar Format

  Chex uses a columnar format that matches ClickHouse's native storage:

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        amount: [100.0, 200.0, 300.0]
      }

      schema = [id: :uint64, name: :string, amount: :float64]

      block = Chex.Block.build_block(columns, schema)

  This format is:
  - **10-1000x faster** than row-oriented (1 NIF call per column vs N×M calls)
  - **Matches ClickHouse native format** (no transposition needed)
  - **Natural for analytics** (operate on columns, not rows)

  If you have row-oriented data, use `Chex.Conversion.rows_to_columns/2`.
  """

  alias Chex.{Column, Conversion, Native}

  @doc """
  Builds a Block from columnar data and schema.

  This is a pure function that validates the input data and constructs a
  Block resource. The block can then be inserted via `Chex.insert/4` or
  used for testing.

  ## Parameters

  - `columns` - Map of column_name => [values]
  - `schema` - Keyword list mapping column names to types

  ## Schema Types

  - `:uint64` - Unsigned 64-bit integer
  - `:int64` - Signed 64-bit integer
  - `:string` - String
  - `:float64` - Float
  - `:datetime` - DateTime (pass Elixir DateTime, converts to Unix timestamp)

  ## Examples

      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      block = Chex.Block.build_block(columns, schema)

      # Block is a reference that can be passed to Native.client_insert
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
      column_refs = Chex.Block.build_columns_bulk(columns, schema)
      # => [id: #Reference<...>, name: #Reference<...>]
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
end
