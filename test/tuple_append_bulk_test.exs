defmodule Natch.TupleAppendBulkTest do
  @moduledoc """
  Tests for Tuple column append_bulk functionality.

  This tests the ability to append a list of tuples directly to a Tuple column,
  as opposed to the columnar API (append_tuple_columns) which requires pre-separated
  column data.

  The append_bulk for tuples transposes the list of tuples into columnar format
  and delegates to append_tuple_columns.
  """

  use ExUnit.Case, async: true

  alias Natch.Column

  describe "Tuple append_bulk - basic types" do
    test "Tuple(Float64, Float64) - basic two-element tuple" do
      col = Column.new({:tuple, [:float64, :float64]})
      assert col.type == {:tuple, [:float64, :float64]}
      assert col.clickhouse_type == "Tuple(Float64, Float64)"

      tuples = [{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(Float32, Float32) - basic two-element tuple with Float32" do
      col = Column.new({:tuple, [:float32, :float32]})
      assert col.type == {:tuple, [:float32, :float32]}
      assert col.clickhouse_type == "Tuple(Float32, Float32)"

      tuples = [{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(Float32, Float64) - mixed float precision" do
      col = Column.new({:tuple, [:float32, :float64]})
      assert col.type == {:tuple, [:float32, :float64]}
      assert col.clickhouse_type == "Tuple(Float32, Float64)"

      tuples = [{1.5, 2.5}, {3.14, 4.0}, {5.0, 6.28}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(String, UInt64) - mixed types" do
      col = Column.new({:tuple, [:string, :uint64]})

      tuples = [{"Alice", 100}, {"Bob", 200}, {"Charlie", 300}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(Int64, Float64, String) - three elements" do
      col = Column.new({:tuple, [:int64, :float64, :string]})

      tuples = [{-1, 1.5, "first"}, {0, 2.5, "second"}, {1, 3.5, "third"}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(UInt64, UInt64, UInt64, UInt64) - four elements" do
      col = Column.new({:tuple, [:uint64, :uint64, :uint64, :uint64]})

      tuples = [{1, 2, 3, 4}, {5, 6, 7, 8}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 2
    end

    test "Tuple(Bool, String) - with booleans" do
      col = Column.new({:tuple, [:bool, :string]})

      tuples = [{true, "yes"}, {false, "no"}, {true, "maybe"}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(Date, String) - with dates" do
      col = Column.new({:tuple, [:date, :string]})

      tuples = [{~D[2024-01-01], "New Year"}, {~D[2024-12-25], "Christmas"}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 2
    end

    test "Tuple(DateTime, UInt64) - with datetime" do
      col = Column.new({:tuple, [:datetime, :uint64]})

      tuples = [{~U[2024-01-01 10:00:00Z], 100}, {~U[2024-12-31 23:59:59Z], 200}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 2
    end
  end

  describe "Tuple append_bulk - edge cases" do
    test "empty list of tuples" do
      col = Column.new({:tuple, [:float64, :float64]})

      assert :ok = Column.append_bulk(col, [])
      assert Column.size(col) == 0
    end

    test "single tuple" do
      col = Column.new({:tuple, [:string, :uint64]})

      tuples = [{"single", 42}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 1
    end

    test "large number of tuples" do
      col = Column.new({:tuple, [:uint64, :uint64]})

      tuples = for i <- 1..10_000, do: {i, i * 2}
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 10_000
    end

    test "multiple appends accumulate" do
      col = Column.new({:tuple, [:float64, :float64]})

      assert :ok = Column.append_bulk(col, [{1.0, 2.0}, {3.0, 4.0}])
      assert Column.size(col) == 2

      assert :ok = Column.append_bulk(col, [{5.0, 6.0}])
      assert Column.size(col) == 3

      assert :ok = Column.append_bulk(col, [{7.0, 8.0}, {9.0, 10.0}])
      assert Column.size(col) == 5
    end

    test "tuples with UTF-8 strings" do
      col = Column.new({:tuple, [:string, :string]})

      tuples = [{"Hello 世界", "Привет"}, {"مرحبا", "🌍🌎🌏"}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 2
    end

    test "tuples with empty strings" do
      col = Column.new({:tuple, [:string, :string]})

      tuples = [{"", ""}, {"a", ""}, {"", "b"}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "tuples with extreme float values" do
      col = Column.new({:tuple, [:float64, :float64]})

      tuples = [{1.0e308, -1.0e308}, {1.0e-308, 0.0}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 2
    end

    test "tuples with zero values" do
      col = Column.new({:tuple, [:int64, :float64, :uint64]})

      tuples = [{0, 0.0, 0}, {-1, 0.0, 1}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 2
    end
  end

  describe "Tuple append_bulk - validation and errors" do
    test "raises on wrong tuple size - too few elements" do
      col = Column.new({:tuple, [:float64, :float64, :float64]})

      assert_raise ArgumentError, ~r/must be tuples of size 3/, fn ->
        Column.append_bulk(col, [{1.0, 2.0}])
      end
    end

    test "raises on wrong tuple size - too many elements" do
      col = Column.new({:tuple, [:float64, :float64]})

      assert_raise ArgumentError, ~r/must be tuples of size 2/, fn ->
        Column.append_bulk(col, [{1.0, 2.0, 3.0}])
      end
    end

    test "raises on non-tuple value in list" do
      col = Column.new({:tuple, [:float64, :float64]})

      assert_raise ArgumentError, ~r/must be tuples of size 2/, fn ->
        Column.append_bulk(col, [[1.0, 2.0]])
      end
    end

    test "raises on mixed tuple and non-tuple values" do
      col = Column.new({:tuple, [:float64, :float64]})

      assert_raise ArgumentError, ~r/must be tuples of size 2/, fn ->
        Column.append_bulk(col, [{1.0, 2.0}, [3.0, 4.0], {5.0, 6.0}])
      end
    end

    test "raises on nil in tuple list" do
      col = Column.new({:tuple, [:float64, :float64]})

      assert_raise ArgumentError, ~r/must be tuples of size 2/, fn ->
        Column.append_bulk(col, [{1.0, 2.0}, nil, {5.0, 6.0}])
      end
    end

    test "raises on scalar value in tuple list" do
      col = Column.new({:tuple, [:float64, :float64]})

      assert_raise ArgumentError, ~r/must be tuples of size 2/, fn ->
        Column.append_bulk(col, [{1.0, 2.0}, 42, {5.0, 6.0}])
      end
    end
  end

  describe "Tuple append_bulk - used in Array context (orderbook use case)" do
    test "Array(Tuple(Float64, Float64)) - orderbook bid/ask levels" do
      # This is the primary use case: orderbook data with price/quantity pairs
      col = Column.new({:array, {:tuple, [:float64, :float64]}})
      assert col.clickhouse_type == "Array(Tuple(Float64, Float64))"

      # Each row is an array of (price, quantity) tuples
      arrays = [
        # First orderbook: 3 levels
        [{100.5, 10.0}, {100.6, 20.0}, {100.7, 30.0}],
        # Second orderbook: 2 levels
        [{250.0, 5.0}, {251.0, 15.0}],
        # Third orderbook: 4 levels
        [{50.0, 100.0}, {50.1, 200.0}, {50.2, 300.0}, {50.3, 400.0}]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Tuple(Float32, Float32)) - orderbook with Float32 precision" do
      col = Column.new({:array, {:tuple, [:float32, :float32]}})
      assert col.clickhouse_type == "Array(Tuple(Float32, Float32))"

      arrays = [
        [{100.5, 10.0}, {100.6, 20.0}, {100.7, 30.0}],
        [{250.0, 5.0}, {251.0, 15.0}],
        [{50.0, 100.0}, {50.1, 200.0}]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Tuple(Float32, Float32)) - empty arrays with Float32" do
      col = Column.new({:array, {:tuple, [:float32, :float32]}})

      arrays = [[], [{1.0, 2.0}], [], [{3.0, 4.0}, {5.0, 6.0}]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 4
    end

    test "Array(Tuple(Float32, Float32)) - large arrays with Float32" do
      col = Column.new({:array, {:tuple, [:float32, :float32]}})

      large_array = for i <- 1..100, do: {i * 1.0, i * 10.0}
      arrays = [large_array, large_array, large_array]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Tuple(Float64, Float64)) - empty arrays" do
      col = Column.new({:array, {:tuple, [:float64, :float64]}})

      arrays = [
        [],
        [{1.0, 2.0}],
        [],
        [{3.0, 4.0}, {5.0, 6.0}]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 4
    end

    test "Array(Tuple(Float64, Float64)) - single element arrays" do
      col = Column.new({:array, {:tuple, [:float64, :float64]}})

      arrays = [
        [{100.0, 1.0}],
        [{200.0, 2.0}],
        [{300.0, 3.0}]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Tuple(String, UInt64)) - named counters" do
      col = Column.new({:array, {:tuple, [:string, :uint64]}})

      arrays = [
        [{"errors", 5}, {"warnings", 10}],
        [{"success", 100}],
        [{"pending", 3}, {"failed", 2}, {"retrying", 1}]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Tuple(Float64, Float64)) - large arrays" do
      col = Column.new({:array, {:tuple, [:float64, :float64]}})

      # Simulate a deep orderbook with 100 levels
      large_array = for i <- 1..100, do: {i * 1.0, i * 10.0}
      arrays = [large_array, large_array, large_array]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end
  end

  describe "Tuple append_bulk - nested tuple types" do
    test "Tuple(Nullable(String), UInt64) - with nullable element" do
      col = Column.new({:tuple, [{:nullable, :string}, :uint64]})

      tuples = [{nil, 100}, {"test", 200}, {nil, 300}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(String, Nullable(UInt64)) - nullable second element" do
      col = Column.new({:tuple, [:string, {:nullable, :uint64}]})

      tuples = [{"a", 100}, {"b", nil}, {"c", 300}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 3
    end

    test "Tuple(LowCardinality(String), UInt64) - with low cardinality" do
      col = Column.new({:tuple, [{:low_cardinality, :string}, :uint64]})

      tuples = [{"active", 1}, {"inactive", 2}, {"active", 3}, {"pending", 4}]
      assert :ok = Column.append_bulk(col, tuples)
      assert Column.size(col) == 4
    end
  end

  describe "Tuple append_bulk - comparison with columnar API" do
    test "append_bulk produces same result as append_tuple_columns" do
      # Create two columns with the same type
      col_bulk = Column.new({:tuple, [:string, :uint64]})
      col_columnar = Column.new({:tuple, [:string, :uint64]})

      # Data
      names = ["Alice", "Bob", "Charlie"]
      scores = [100, 200, 300]
      tuples = [{"Alice", 100}, {"Bob", 200}, {"Charlie", 300}]

      # Append using both APIs
      Column.append_bulk(col_bulk, tuples)
      Column.append_tuple_columns(col_columnar, [names, scores])

      # Both should have same size
      assert Column.size(col_bulk) == Column.size(col_columnar)
      assert Column.size(col_bulk) == 3
    end
  end
end
