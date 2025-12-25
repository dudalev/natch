defmodule Natch.TupleIntegrationTest do
  @moduledoc """
  Integration tests for Tuple types with ClickHouse.

  Tests the full roundtrip: insert via native protocol and select back.
  Focuses on Array(Tuple(...)) use cases like orderbook data.

  These tests require a running ClickHouse instance on localhost:9000.
  Run with: mix test --include integration
  """

  use ExUnit.Case, async: true

  @moduletag :integration

  setup do
    # Generate unique table name for this test
    table = "test_tuple_#{System.unique_integer([:positive, :monotonic])}_#{:rand.uniform(999_999)}"

    # Connection options from environment or defaults
    host = System.get_env("CLICKHOUSE_HOST", "localhost")
    port = String.to_integer(System.get_env("CLICKHOUSE_PORT", "9000"))
    database = System.get_env("CLICKHOUSE_DATABASE", "default")
    user = System.get_env("CLICKHOUSE_USER", "default")
    password = System.get_env("CLICKHOUSE_PASSWORD", "")

    # Start connection
    {:ok, conn} = Natch.start_link(
      host: host,
      port: port,
      database: database,
      user: user,
      password: password
    )

    on_exit(fn ->
      # Clean up test table if it exists
      if Process.alive?(conn) do
        try do
          Natch.execute(conn, "DROP TABLE IF EXISTS #{table}")
        catch
          :exit, _ -> :ok
        end

        # Use Process.exit to avoid race conditions
        Process.exit(conn, :normal)
      end
    end)

    {:ok, conn: conn, table: table}
  end

  describe "Tuple(Float64, Float64) roundtrip" do
    test "basic tuple column", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        point Tuple(Float64, Float64)
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, point: {:tuple, [:float64, :float64]}]

      columns = %{
        id: [1, 2, 3],
        point: [{1.5, 2.5}, {3.0, 4.0}, {5.5, 6.5}]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      assert result == [
               %{id: 1, point: {1.5, 2.5}},
               %{id: 2, point: {3.0, 4.0}},
               %{id: 3, point: {5.5, 6.5}}
             ]
    end
  end

  describe "Array(Tuple(Float64, Float64)) roundtrip - orderbook use case" do
    test "orderbook with bid/ask levels", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        time DateTime64(3),
        instrument_id String,
        ask Array(Tuple(Float64, Float64)),
        bid Array(Tuple(Float64, Float64))
      ) ENGINE = MergeTree()
      ORDER BY (instrument_id, time)
      """)

      schema = [
        time: :datetime64,
        instrument_id: :string,
        ask: {:array, {:tuple, [:float64, :float64]}},
        bid: {:array, {:tuple, [:float64, :float64]}}
      ]

      now = DateTime.utc_now()

      columns = %{
        time: [now, now],
        instrument_id: ["SBER", "GAZP"],
        ask: [
          [{100.5, 10.0}, {100.6, 20.0}, {100.7, 30.0}],
          [{250.0, 5.0}, {251.0, 15.0}]
        ],
        bid: [
          [{100.4, 15.0}, {100.3, 25.0}],
          [{249.0, 10.0}, {248.0, 20.0}, {247.0, 30.0}]
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} =
               Natch.select_rows(conn, "SELECT instrument_id, ask, bid FROM #{table} ORDER BY instrument_id")

      assert length(result) == 2

      gazp = Enum.find(result, &(&1.instrument_id == "GAZP"))
      sber = Enum.find(result, &(&1.instrument_id == "SBER"))

      assert gazp.ask == [{250.0, 5.0}, {251.0, 15.0}]
      assert gazp.bid == [{249.0, 10.0}, {248.0, 20.0}, {247.0, 30.0}]

      assert sber.ask == [{100.5, 10.0}, {100.6, 20.0}, {100.7, 30.0}]
      assert sber.bid == [{100.4, 15.0}, {100.3, 25.0}]
    end

    test "orderbook with empty levels", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        levels Array(Tuple(Float64, Float64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, levels: {:array, {:tuple, [:float64, :float64]}}]

      columns = %{
        id: [1, 2, 3, 4],
        levels: [
          [],
          [{1.0, 10.0}],
          [],
          [{2.0, 20.0}, {3.0, 30.0}]
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 4

      assert result == [
               %{id: 1, levels: []},
               %{id: 2, levels: [{1.0, 10.0}]},
               %{id: 3, levels: []},
               %{id: 4, levels: [{2.0, 20.0}, {3.0, 30.0}]}
             ]
    end

    test "orderbook with many levels per row", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        levels Array(Tuple(Float64, Float64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, levels: {:array, {:tuple, [:float64, :float64]}}]

      # Generate 50 levels per row
      large_levels = for i <- 1..50, do: {i * 1.0, i * 100.0}

      columns = %{
        id: [1, 2],
        levels: [large_levels, large_levels]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert length(result |> hd() |> Map.get(:levels)) == 50
    end
  end

  describe "Array(Tuple(String, UInt64)) roundtrip" do
    test "named counters array", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        counters Array(Tuple(String, UInt64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, counters: {:array, {:tuple, [:string, :uint64]}}]

      columns = %{
        id: [1, 2, 3],
        counters: [
          [{"errors", 5}, {"warnings", 10}, {"info", 100}],
          [{"success", 42}],
          [{"pending", 3}, {"failed", 2}]
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      assert result == [
               %{id: 1, counters: [{"errors", 5}, {"warnings", 10}, {"info", 100}]},
               %{id: 2, counters: [{"success", 42}]},
               %{id: 3, counters: [{"pending", 3}, {"failed", 2}]}
             ]
    end
  end

  describe "Tuple(String, UInt64, Float64) roundtrip - three elements" do
    test "product data with three-element tuples", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        product Tuple(String, UInt64, Float64)
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, product: {:tuple, [:string, :uint64, :float64]}]

      columns = %{
        id: [1, 2, 3],
        product: [
          {"Widget", 100, 19.99},
          {"Gadget", 50, 49.99},
          {"Doohickey", 200, 9.99}
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      assert result == [
               %{id: 1, product: {"Widget", 100, 19.99}},
               %{id: 2, product: {"Gadget", 50, 49.99}},
               %{id: 3, product: {"Doohickey", 200, 9.99}}
             ]
    end
  end

  describe "Array(Tuple(Float32, Float32)) roundtrip" do
    test "orderbook with Float32 precision", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        levels Array(Tuple(Float32, Float32))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, levels: {:array, {:tuple, [:float32, :float32]}}]

      columns = %{
        id: [1, 2, 3],
        levels: [
          [{100.5, 10.0}, {100.6, 20.0}, {100.7, 30.0}],
          [{250.0, 5.0}, {251.0, 15.0}],
          []
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      # Float32 has less precision, so we check structure rather than exact values
      assert length(result |> Enum.at(0) |> Map.get(:levels)) == 3
      assert length(result |> Enum.at(1) |> Map.get(:levels)) == 2
      assert result |> Enum.at(2) |> Map.get(:levels) == []
    end

    test "multiple Float32 tuple columns", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        bids Array(Tuple(Float32, Float32)),
        asks Array(Tuple(Float32, Float32))
      ) ENGINE = Memory
      """)

      schema = [
        id: :uint64,
        bids: {:array, {:tuple, [:float32, :float32]}},
        asks: {:array, {:tuple, [:float32, :float32]}}
      ]

      columns = %{
        id: [1, 2],
        bids: [
          [{99.5, 100.0}, {98.5, 200.0}],
          [{199.5, 50.0}]
        ],
        asks: [
          [{101.5, 150.0}],
          [{201.5, 75.0}, {202.5, 100.0}]
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      row1 = Enum.at(result, 0)
      row2 = Enum.at(result, 1)

      assert length(row1.bids) == 2
      assert length(row1.asks) == 1
      assert length(row2.bids) == 1
      assert length(row2.asks) == 2
    end
  end

  describe "Tuple(Float32, Float32) roundtrip" do
    test "basic Float32 tuple column", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        point Tuple(Float32, Float32)
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, point: {:tuple, [:float32, :float32]}]

      columns = %{
        id: [1, 2, 3],
        point: [{1.5, 2.5}, {3.0, 4.0}, {5.5, 6.5}]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      # Check tuple structure (Float32 precision may differ slightly)
      for row <- result do
        {a, b} = row.point
        assert is_float(a)
        assert is_float(b)
      end
    end
  end

  describe "Array(Tuple(...)) with various element types" do
    test "Array(Tuple(Int64, Int64)) - signed integers", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        ranges Array(Tuple(Int64, Int64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, ranges: {:array, {:tuple, [:int64, :int64]}}]

      columns = %{
        id: [1, 2],
        ranges: [
          [{-100, 100}, {-50, 50}],
          [{0, 1000}, {-1000, 0}]
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, ranges: [{-100, 100}, {-50, 50}]},
               %{id: 2, ranges: [{0, 1000}, {-1000, 0}]}
             ]
    end

    test "Array(Tuple(String, String)) - string pairs", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        pairs Array(Tuple(String, String))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, pairs: {:array, {:tuple, [:string, :string]}}]

      columns = %{
        id: [1, 2],
        pairs: [
          [{"key1", "value1"}, {"key2", "value2"}],
          [{"hello", "world"}]
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, pairs: [{"key1", "value1"}, {"key2", "value2"}]},
               %{id: 2, pairs: [{"hello", "world"}]}
             ]
    end
  end

  describe "Multiple Array(Tuple(...)) columns" do
    test "bid and ask as separate columns", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        bids Array(Tuple(Float64, Float64)),
        asks Array(Tuple(Float64, Float64)),
        trades Array(Tuple(Float64, Float64))
      ) ENGINE = Memory
      """)

      schema = [
        id: :uint64,
        bids: {:array, {:tuple, [:float64, :float64]}},
        asks: {:array, {:tuple, [:float64, :float64]}},
        trades: {:array, {:tuple, [:float64, :float64]}}
      ]

      columns = %{
        id: [1, 2],
        bids: [
          [{99.0, 100.0}, {98.0, 200.0}],
          [{199.0, 50.0}]
        ],
        asks: [
          [{101.0, 150.0}],
          [{201.0, 75.0}, {202.0, 100.0}]
        ],
        trades: [
          [{100.0, 10.0}],
          []
        ]
      }

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{
                 id: 1,
                 bids: [{99.0, 100.0}, {98.0, 200.0}],
                 asks: [{101.0, 150.0}],
                 trades: [{100.0, 10.0}]
               },
               %{
                 id: 2,
                 bids: [{199.0, 50.0}],
                 asks: [{201.0, 75.0}, {202.0, 100.0}],
                 trades: []
               }
             ]
    end
  end

  describe "Large batch inserts" do
    test "1000 rows with Array(Tuple(Float64, Float64))", %{conn: conn, table: table} do
      Natch.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        data Array(Tuple(Float64, Float64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, data: {:array, {:tuple, [:float64, :float64]}}]

      # Generate 1000 rows, each with 5-10 tuples
      ids = Enum.to_list(1..1000)

      data =
        for _ <- 1..1000 do
          count = :rand.uniform(6) + 4
          for i <- 1..count, do: {i * 1.0, i * 10.0}
        end

      columns = %{id: ids, data: data}

      assert :ok = Natch.insert_cols(conn, table, columns, schema)

      assert {:ok, result} = Natch.select_rows(conn, "SELECT count() as cnt FROM #{table}")
      assert [%{cnt: 1000}] = result
    end
  end
end
