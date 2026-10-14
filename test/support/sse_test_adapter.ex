defmodule Anubis.Test.SSEAdapter do
  @moduledoc false

  alias Plug.Adapters.Test.Conn

  defdelegate send_resp(payload, status, headers, body), to: Conn
  defdelegate send_file(payload, status, headers, path, offset, length), to: Conn
  defdelegate read_req_body(payload, opts), to: Conn
  defdelegate inform(payload, status, headers), to: Conn
  defdelegate upgrade(payload, protocol, opts), to: Conn
  defdelegate push(payload, path, headers), to: Conn
  defdelegate get_peer_data(payload), to: Conn
  defdelegate get_http_protocol(payload), to: Conn
  defdelegate get_sock_data(payload), to: Conn
  defdelegate get_ssl_data(payload), to: Conn

  @doc """
  Swaps a test conn's adapter for this one. `:disconnected` is the atomics
  whose first slot, once 1, fails every chunk write (default: a fresh one);
  `:on_open` runs after the chunked headers are sent.
  """
  def wrap(%Plug.Conn{adapter: {_adapter, payload}} = conn, opts \\ []) do
    disconnected = Keyword.get_lazy(opts, :disconnected, fn -> :atomics.new(1, []) end)
    payload = Map.merge(payload, %{disconnected: disconnected, on_open: Keyword.get(opts, :on_open)})
    %{conn | adapter: {__MODULE__, payload}}
  end

  def send_chunked(payload, status, headers) do
    {:ok, body, payload} = Conn.send_chunked(payload, status, headers)
    if on_open = Map.get(payload, :on_open), do: on_open.()
    send(payload.owner, {:sse_opened, self()})
    {:ok, body, payload}
  end

  def chunk(payload, body) do
    if :atomics.get(payload.disconnected, 1) == 1 do
      {:error, :closed}
    else
      send(payload.owner, {:sse_chunk, self(), IO.iodata_to_binary(body)})
      Conn.chunk(payload, body)
    end
  end
end
