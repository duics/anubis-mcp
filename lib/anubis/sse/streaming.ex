if Code.ensure_loaded?(Plug) do
  defmodule Anubis.SSE.Streaming do
    @moduledoc false

    use Anubis.Logging

    alias Anubis.SSE.Event

    @type conn :: Plug.Conn.t()
    @type transport :: GenServer.server()
    @type session_id :: String.t()

    @doc """
    Starts the SSE streaming loop for a connection.

    This function takes control of the connection and enters a receive loop,
    streaming messages to the client as they arrive.

    ## Parameters
      - `conn` - The Plug.Conn that has been prepared for chunked response
      - `transport` - The transport process
      - `session_id` - The session identifier
      - `opts` - Options including:
        - `:initial_event_id` - Starting event ID (default: 0)
        - `:on_close` - Function to call when connection closes

    ## Messages handled
      - `{:sse_message, binary}` - Message to send to client
      - `:close_sse` - Close the connection gracefully
    """
    @spec start(conn, transport, session_id, keyword()) :: conn
    def start(conn, transport, session_id, opts \\ []) do
      initial_event_id = Keyword.get(opts, :initial_event_id, 0)
      on_close = Keyword.get(opts, :on_close, fn -> :ok end)

      try do
        loop(conn, transport, session_id, initial_event_id)
      after
        on_close.()
      end
    end

    @doc """
    Prepares a connection for SSE streaming.

    Sets appropriate headers and starts chunked response.
    """
    @spec prepare_connection(conn) :: conn
    def prepare_connection(conn) do
      conn
      |> Plug.Conn.put_resp_content_type("text/event-stream")
      |> Plug.Conn.put_resp_header("cache-control", "no-cache")
      |> Plug.Conn.put_resp_header("x-accel-buffering", "no")
      |> Plug.Conn.send_chunked(200)
    end

    @doc """
    Streaming loop for a per-request POST stream (Streamable HTTP).

    Unlike `start/4` which keeps a long-lived GET stream open, `start_for_request/4`
    is a short-lived loop scoped to a single in-flight request. It terminates on:

      * `{:request_done, request_ref, {:ok, response_binary}}` — write one event
        with the response, halt cleanly.
      * `{:request_done, request_ref, {:error, encoded_error_envelope}}` — write
        the error event, halt.
      * `{:request_cancelled, request_ref}` — halt WITHOUT writing any final
        event (per `cancellation.mdx:39`: "Not send a response for the
        cancelled request").
      * `{:DOWN, monitor_ref, :process, _, reason}` — write `-32603 internal_error`
        and halt, UNLESS a `:request_cancelled` was already received (in which
        case the DOWN is the trailing process exit and we halt silently).

    In-flight `{:sse_message, binary}` notifications (from `send_progress` etc.)
    are written as events; a per-request keepalive prevents intermediary timeouts.
    """
    @spec start_for_request(conn, reference(), reference(), keyword()) :: conn
    def start_for_request(conn, request_ref, monitor_ref, opts \\ []) do
      keepalive_interval = Keyword.get(opts, :keepalive_interval, 5_000)
      session_id = Keyword.get(opts, :session_id)

      keepalive_timer =
        if keepalive_interval > 0,
          do: Process.send_after(self(), :sse_keepalive, keepalive_interval)

      try do
        request_loop(conn, request_ref, monitor_ref, %{
          event_counter: 0,
          session_id: session_id,
          keepalive_interval: keepalive_interval,
          keepalive_timer: keepalive_timer,
          cancelled: false
        })
      after
        if keepalive_timer, do: Process.cancel_timer(keepalive_timer)
      end
    end

    @doc """
    Sends a single SSE event.

    This is useful for sending events outside of the main loop.
    """
    @spec send_event(conn, binary(), non_neg_integer()) ::
            {:ok, conn} | {:error, term()}
    def send_event(conn, data, event_id) when is_binary(data) do
      event = %Event{
        id: to_string(event_id),
        event: "message",
        data: data
      }

      case Plug.Conn.chunk(conn, Event.encode(event)) do
        {:ok, conn} -> {:ok, conn}
        {:error, reason} -> {:error, reason}
      end
    end

    # Private functions

    defp loop(conn, transport, session_id, event_counter) do
      receive do
        :sse_keepalive ->
          case keep_alive(conn) do
            {:ok, conn} ->
              loop(conn, transport, session_id, event_counter + 1)

            {:error, reason} ->
              Logging.transport_event("sse_keepalive_failed", %{session_id: session_id, reason: reason}, level: :error)

              conn
          end

        {:sse_message, message} when is_binary(message) ->
          case send_event(conn, message, event_counter) do
            {:ok, conn} ->
              loop(conn, transport, session_id, event_counter + 1)

            {:error, reason} ->
              Logging.transport_event(
                "sse_send_failed",
                %{session_id: session_id, reason: reason},
                level: :warning
              )

              conn
          end

        :close_sse ->
          Logging.transport_event("sse_closing", %{session_id: session_id})
          Plug.Conn.halt(conn)

        {:plug_conn, :sent} ->
          # Ignore Plug internal messages
          loop(conn, transport, session_id, event_counter)

        msg ->
          Logging.transport_event(
            "sse_unknown_message",
            %{
              session_id: session_id,
              message: inspect(msg)
            },
            level: :warning
          )

          loop(conn, transport, session_id, event_counter)
      end
    end

    defp keep_alive(conn) do
      Plug.Conn.chunk(conn, ": keepalive\n\n")
    end

    defp request_loop(conn, request_ref, monitor_ref, ctx) do
      receive do
        {:request_cancelled, ^request_ref} ->
          Logging.transport_event("sse_request_cancelled", %{session_id: ctx.session_id})
          Plug.Conn.halt(conn)

        {:request_done, ^request_ref, {:ok, nil}} ->
          # No response payload — close cleanly without writing.
          Plug.Conn.halt(conn)

        {:request_done, ^request_ref, {:ok, response_binary}}
        when is_binary(response_binary) ->
          case send_event(conn, response_binary, ctx.event_counter) do
            {:ok, conn} -> Plug.Conn.halt(conn)
            {:error, _reason} -> Plug.Conn.halt(conn)
          end

        {:request_done, ^request_ref, {:error, encoded_envelope}}
        when is_binary(encoded_envelope) ->
          case send_event(conn, encoded_envelope, ctx.event_counter) do
            {:ok, conn} -> Plug.Conn.halt(conn)
            {:error, _reason} -> Plug.Conn.halt(conn)
          end

        {:DOWN, ^monitor_ref, :process, _pid, _reason} when ctx.cancelled ->
          Plug.Conn.halt(conn)

        {:DOWN, ^monitor_ref, :process, _pid, _reason} ->
          # Genuine crash — write -32603 once and halt.
          envelope = ~s({"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":null})
          _ = send_event(conn, envelope, ctx.event_counter)
          Plug.Conn.halt(conn)

        {:sse_message, message} when is_binary(message) ->
          case send_event(conn, message, ctx.event_counter) do
            {:ok, conn} ->
              request_loop(conn, request_ref, monitor_ref, %{ctx | event_counter: ctx.event_counter + 1})

            {:error, _reason} ->
              Plug.Conn.halt(conn)
          end

        :sse_keepalive ->
          case keep_alive(conn) do
            {:ok, conn} ->
              keepalive_timer =
                if ctx.keepalive_interval > 0,
                  do: Process.send_after(self(), :sse_keepalive, ctx.keepalive_interval)

              request_loop(conn, request_ref, monitor_ref, %{ctx | keepalive_timer: keepalive_timer})

            {:error, _reason} ->
              Plug.Conn.halt(conn)
          end

        {:plug_conn, :sent} ->
          request_loop(conn, request_ref, monitor_ref, ctx)

        msg ->
          Logging.transport_event(
            "sse_request_unknown_message",
            %{session_id: ctx.session_id, message: inspect(msg)},
            level: :warning
          )

          request_loop(conn, request_ref, monitor_ref, ctx)
      end
    end
  end
end
