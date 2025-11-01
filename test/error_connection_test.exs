defmodule Chex.ConnectionErrorTest do
  use ExUnit.Case, async: false

  alias Chex.ConnectionError

  describe "connection errors" do
    test "raises ConnectionError for invalid host" do
      Process.flag(:trap_exit, true)

      {:error, {%ConnectionError{} = error, _stacktrace}} =
        Chex.Connection.start_link(host: "invalid.nonexistent.host.example", port: 9999)

      assert error.reason == :connection_failed
      assert error.message =~ "nodename nor servname"
    end
  end

  describe "server errors" do
    setup do
      {:ok, conn} = Chex.Connection.start_link(host: "localhost", port: 9000)
      {:ok, conn: conn}
    end

    test "returns structured error for syntax errors", %{conn: conn} do
      # Invalid SQL syntax should return server error with code/name
      result = Chex.Connection.execute(conn, "INVALID SQL SYNTAX")

      assert {:error, error_message} = result
      # The error should contain structured JSON information
      assert is_binary(error_message)

      # Decode and verify the structure
      {:ok, error_json} = Jason.decode(error_message)
      assert error_json["type"] == "server"
      # SYNTAX_ERROR code
      assert error_json["code"] == 62
      assert error_json["name"] == "DB::Exception"
      assert error_json["message"] =~ "Syntax error"
      # Stack trace should be present for server errors
      assert is_binary(error_json["stack_trace"])
    end
  end
end
