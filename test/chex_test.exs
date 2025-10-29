defmodule ChexTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  setup_all do
    # Start ClickHouse via docker-compose before running tests
    # docker-compose up -d clickhouse
    :ok
  end

  setup do
    {:ok, conn} = Chex.start_link(url: "http://localhost:8123", database: "default")

    on_exit(fn ->
      # Clean up any test tables
      Chex.execute(conn, "DROP TABLE IF EXISTS test_users")
      Chex.execute(conn, "DROP TABLE IF EXISTS test_events")
      Chex.stop(conn)
    end)

    {:ok, conn: conn}
  end

  describe "connection" do
    test "starts a connection successfully", %{conn: conn} do
      assert Process.alive?(conn)
    end

    test "stops a connection", %{conn: conn} do
      assert :ok = Chex.stop(conn)
      refute Process.alive?(conn)
    end
  end

  describe "execute/3" do
    test "creates a table", %{conn: conn} do
      sql = """
      CREATE TABLE IF NOT EXISTS test_users (
        id UInt32,
        name String,
        age UInt8
      ) ENGINE = MergeTree()
      ORDER BY id
      """

      assert :ok = Chex.execute(conn, sql)
    end

    test "drops a table", %{conn: conn} do
      Chex.execute(
        conn,
        "CREATE TABLE IF NOT EXISTS test_drop (id UInt32) ENGINE = MergeTree() ORDER BY id"
      )

      assert :ok = Chex.execute(conn, "DROP TABLE test_drop")
    end
  end

  describe "query/3" do
    setup %{conn: conn} do
      sql = """
      CREATE TABLE IF NOT EXISTS test_users (
        id UInt32,
        name String,
        age UInt8
      ) ENGINE = MergeTree()
      ORDER BY id
      """

      Chex.execute(conn, sql)

      # Insert test data
      {:ok, insert} = Chex.insert(conn, "test_users")
      Chex.write(insert, %{"id" => 1, "name" => "Alice", "age" => 30})
      Chex.write(insert, %{"id" => 2, "name" => "Bob", "age" => 25})
      Chex.write(insert, %{"id" => 3, "name" => "Charlie", "age" => 35})
      Chex.end_insert(insert)

      # Give ClickHouse a moment to process
      Process.sleep(100)

      :ok
    end

    test "fetches all rows", %{conn: conn} do
      {:ok, rows} = Chex.query(conn, "SELECT * FROM test_users ORDER BY id")
      assert length(rows) == 3
      assert List.first(rows)["name"] == "Alice"
    end

    test "fetches rows with parameter binding", %{conn: conn} do
      {:ok, rows} = Chex.query(conn, "SELECT * FROM test_users WHERE id = ?", [2])
      assert length(rows) == 1
      assert List.first(rows)["name"] == "Bob"
    end

    test "fetches rows with multiple parameters", %{conn: conn} do
      {:ok, rows} =
        Chex.query(conn, "SELECT * FROM test_users WHERE age > ? AND age < ?", [26, 36])

      assert length(rows) == 2
    end

    test "returns empty list for no results", %{conn: conn} do
      {:ok, rows} = Chex.query(conn, "SELECT * FROM test_users WHERE id = ?", [999])
      assert rows == []
    end
  end

  describe "query!/3" do
    setup %{conn: conn} do
      sql = """
      CREATE TABLE IF NOT EXISTS test_users (
        id UInt32,
        name String,
        age UInt8
      ) ENGINE = MergeTree()
      ORDER BY id
      """

      Chex.execute(conn, sql)
      :ok
    end

    test "returns rows on success", %{conn: conn} do
      {:ok, insert} = Chex.insert(conn, "test_users")
      Chex.write(insert, %{"id" => 1, "name" => "Alice", "age" => 30})
      Chex.end_insert(insert)
      Process.sleep(100)

      rows = Chex.query!(conn, "SELECT * FROM test_users")
      assert is_list(rows)
    end
  end

  describe "stream/3" do
    setup %{conn: conn} do
      sql = """
      CREATE TABLE IF NOT EXISTS test_users (
        id UInt32,
        name String,
        age UInt8
      ) ENGINE = MergeTree()
      ORDER BY id
      """

      Chex.execute(conn, sql)

      {:ok, insert} = Chex.insert(conn, "test_users")

      for i <- 1..10 do
        Chex.write(insert, %{"id" => i, "name" => "User#{i}", "age" => 20 + i})
      end

      Chex.end_insert(insert)
      Process.sleep(100)

      :ok
    end

    test "returns a stream", %{conn: conn} do
      stream = Chex.stream(conn, "SELECT * FROM test_users ORDER BY id")
      assert is_function(stream)
    end

    test "can be enumerated", %{conn: conn} do
      result =
        conn
        |> Chex.stream("SELECT * FROM test_users ORDER BY id")
        |> Enum.to_list()

      assert length(result) == 10
    end

    test "can be used with Stream functions", %{conn: conn} do
      result =
        conn
        |> Chex.stream("SELECT * FROM test_users ORDER BY id")
        |> Stream.take(5)
        |> Enum.to_list()

      assert length(result) == 5
    end
  end

  describe "insert/2" do
    setup %{conn: conn} do
      sql = """
      CREATE TABLE IF NOT EXISTS test_users (
        id UInt32,
        name String,
        age UInt8
      ) ENGINE = MergeTree()
      ORDER BY id
      """

      Chex.execute(conn, sql)
      :ok
    end

    test "inserts rows successfully", %{conn: conn} do
      {:ok, insert} = Chex.insert(conn, "test_users")
      assert :ok = Chex.write(insert, %{"id" => 1, "name" => "Alice", "age" => 30})
      assert :ok = Chex.write(insert, %{"id" => 2, "name" => "Bob", "age" => 25})
      assert :ok = Chex.end_insert(insert)

      Process.sleep(100)

      {:ok, rows} = Chex.query(conn, "SELECT count() as count FROM test_users")
      assert List.first(rows)["count"] == 2
    end
  end

  describe "inserter/3" do
    setup %{conn: conn} do
      sql = """
      CREATE TABLE IF NOT EXISTS test_events (
        id UInt32,
        event_type String,
        value UInt32
      ) ENGINE = MergeTree()
      ORDER BY id
      """

      Chex.execute(conn, sql)
      :ok
    end

    test "auto-batches inserts by row count", %{conn: conn} do
      {:ok, inserter} = Chex.inserter(conn, "test_events", max_rows: 5)

      for i <- 1..12 do
        assert :ok =
                 Chex.write_batch(inserter, %{
                   "id" => i,
                   "event_type" => "click",
                   "value" => i * 10
                 })

        assert :ok = Chex.commit(inserter)
      end

      assert :ok = Chex.end_inserter(inserter)

      Process.sleep(200)

      {:ok, rows} = Chex.query(conn, "SELECT count() as count FROM test_events")
      assert List.first(rows)["count"] == 12
    end

    test "auto-batches inserts by byte size", %{conn: conn} do
      {:ok, inserter} = Chex.inserter(conn, "test_events", max_bytes: 1024)

      for i <- 1..20 do
        assert :ok =
                 Chex.write_batch(inserter, %{"id" => i, "event_type" => "view", "value" => i})

        assert :ok = Chex.commit(inserter)
      end

      assert :ok = Chex.end_inserter(inserter)

      Process.sleep(200)

      {:ok, rows} = Chex.query(conn, "SELECT count() as count FROM test_events")
      assert List.first(rows)["count"] == 20
    end
  end
end
