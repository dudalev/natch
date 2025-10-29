defmodule Chex.MinimalPoCTest do
  use ExUnit.Case, async: false

  @moduletag :minimal_poc

  describe "Minimal Proof of Concept" do
    test "can load FINE NIFs" do
      # If we get here, the NIF loaded successfully
      assert function_exported?(Chex.Native, :create_client, 0)
      assert function_exported?(Chex.Native, :ping, 1)
    end

    @tag :integration
    test "can create client and ping ClickHouse" do
      # Requires ClickHouse running on localhost:9000
      # With homebrew: brew services start clickhouse
      client = Chex.Native.create_client()
      assert is_reference(client)

      result = Chex.Native.ping(client)
      assert result == "pong"
    end
  end
end
