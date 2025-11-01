defmodule Chex.ConnectionError do
  @moduledoc """
  Raised when a connection to ClickHouse cannot be established.

  This error is raised when the client fails to connect to the ClickHouse server,
  typically due to network issues, invalid host/port, or DNS resolution failures.

  ## Fields

  - `:message` - Human-readable error description
  - `:reason` - Underlying system error reason (e.g., `:econnrefused`, `:nxdomain`)
  """

  defexception [:message, :reason]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)
    reason = Keyword.get(opts, :reason)

    %__MODULE__{
      message: message,
      reason: reason
    }
  end
end
