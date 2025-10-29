defmodule Chex.Column do
  @moduledoc """
  Column builder for ClickHouse native types.

  Provides an Elixir API for creating and populating ClickHouse columns.
  """

  alias Chex.Native

  @type column :: %__MODULE__{
          ref: reference(),
          type: atom(),
          clickhouse_type: String.t()
        }

  defstruct [:ref, :type, :clickhouse_type]

  @doc """
  Creates a new column of the specified type.

  ## Supported Types

  - `:uint64` - UInt64
  - `:int64` - Int64
  - `:string` - String
  - `:float64` - Float64
  - `:datetime` - DateTime

  ## Examples

      iex> Chex.Column.new(:uint64)
      %Chex.Column{type: :uint64, clickhouse_type: "UInt64", ref: #Reference<...>}

      iex> Chex.Column.new(:string)
      %Chex.Column{type: :string, clickhouse_type: "String", ref: #Reference<...>}
  """
  @spec new(atom()) :: column()
  def new(type) when is_atom(type) do
    clickhouse_type = elixir_type_to_clickhouse(type)
    ref = Native.column_create(clickhouse_type)

    %__MODULE__{
      ref: ref,
      type: type,
      clickhouse_type: clickhouse_type
    }
  end

  @doc """
  Appends a value to the column.

  The value type must match the column type.

  ## Examples

      col = Chex.Column.new(:uint64)
      :ok = Chex.Column.append(col, 42)

      col = Chex.Column.new(:string)
      :ok = Chex.Column.append(col, "hello")
  """
  @spec append(column(), term()) :: :ok
  def append(%__MODULE__{type: :uint64, ref: ref}, value) when is_integer(value) and value >= 0 do
    Native.column_uint64_append(ref, value)
  end

  def append(%__MODULE__{type: :int64, ref: ref}, value) when is_integer(value) do
    Native.column_int64_append(ref, value)
  end

  def append(%__MODULE__{type: :string, ref: ref}, value) when is_binary(value) do
    Native.column_string_append(ref, value)
  end

  def append(%__MODULE__{type: :float64, ref: ref}, value)
      when is_float(value) or is_integer(value) do
    Native.column_float64_append(ref, value * 1.0)
  end

  def append(%__MODULE__{type: :datetime, ref: ref}, %DateTime{} = dt) do
    timestamp = DateTime.to_unix(dt)
    Native.column_datetime_append(ref, timestamp)
  end

  def append(%__MODULE__{type: :datetime, ref: ref}, timestamp) when is_integer(timestamp) do
    Native.column_datetime_append(ref, timestamp)
  end

  def append(%__MODULE__{type: type}, value) do
    raise ArgumentError,
          "Invalid value #{inspect(value)} for column type #{type}"
  end

  @doc """
  Returns the number of elements in the column.

  ## Examples

      col = Chex.Column.new(:uint64)
      Chex.Column.append(col, 1)
      Chex.Column.append(col, 2)
      Chex.Column.size(col)
      # => 2
  """
  @spec size(column()) :: non_neg_integer()
  def size(%__MODULE__{ref: ref}) do
    Native.column_size(ref)
  end

  # Private functions

  defp elixir_type_to_clickhouse(:uint64), do: "UInt64"
  defp elixir_type_to_clickhouse(:int64), do: "Int64"
  defp elixir_type_to_clickhouse(:string), do: "String"
  defp elixir_type_to_clickhouse(:float64), do: "Float64"
  defp elixir_type_to_clickhouse(:datetime), do: "DateTime"

  defp elixir_type_to_clickhouse(type) do
    raise ArgumentError, "Unsupported column type: #{inspect(type)}"
  end
end
