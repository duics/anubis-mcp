if Code.ensure_loaded?(Plug) do
  defmodule Anubis.Server.Transport.StreamableHTTP.ResponseStream do
    @moduledoc """
    Answers a request whose handler may emit request-scoped notifications while
    it runs, such as progress.

    `Anubis.Server.Transport.StreamableHTTP.StatelessBinding` registers a
    stateless request as the SSE handler of its session, and
    `Anubis.Server.Transport.StreamableHTTP.Plug` registers a session-oriented
    request as a request stream of its session
    (`Anubis.Server.Transport.StreamableHTTP.register_request_stream/3`); each
    hands it here when the client accepts `text/event-stream`. The request is
    dispatched in a task. If the session emits a request-scoped notification
    before the reply, or the request outlives a keepalive interval of the
    transport, the response becomes an SSE stream carrying the notifications in
    order and ending with the JSON-RPC response; otherwise it stays a single
    JSON object, with the status codes the caller's JSON callback gives it. A
    handler completing without a reply closes an open stream without a final
    event, or answers through the JSON callback when the response has not
    started.

    Request-scoped means `notifications/progress`, and `notifications/message`
    when the request declared an `io.modelcontextprotocol/logLevel`. Anything
    else the session emits, such as a list change, belongs on a
    `subscriptions/listen` stream and is dropped here.

    A client that disconnects cancels the request, as the specification
    requires of Streamable HTTP. A disconnect is only visible to a write, so a
    long request writes a keepalive comment every interval: the write that
    fails stops the dispatch and calls `opts.on_cancel`, when given. A
    stateless session, owned by the request, stops with it and terminates the
    handler; a session-oriented caller cancels the request through
    `on_cancel`. With keepalive disabled on the transport, a disconnect goes
    unnoticed until the reply is written.

    `:close_sse`, the transport's shutdown signal, and a trapped `:shutdown`
    or `{:shutdown, reason}` exit also cancel the request. Both answer it with
    an internal error, as the final event of an open stream or through the JSON
    callback; the exit then exits the caller with its reason.
    """

    use Anubis.Logging

    alias Anubis.MCP.Error
    alias Anubis.Server.Transport.Session
    alias Anubis.SSE.Streaming

    @log_level_key "io.modelcontextprotocol/logLevel"

    @doc """
    Dispatches `message` to `session` and answers `conn`, as JSON through
    `json_reply` unless a request-scoped notification or keepalive arrives first.

    `opts.timeout` bounds dispatch; `opts.on_cancel`, when present, is a
    0-arity function called when the request is cancelled. A successful reply
    ends the stream with its JSON-RPC response; a successful no-reply
    completion closes it without an event. Dispatch failures become error events after streaming starts and
    retain the JSON callback's behavior beforehand. A dispatch exit before
    streaming starts exits the caller.
    """
    @spec serve(Plug.Conn.t(), pid(), map(), map(), map(), (Plug.Conn.t(), term() -> Plug.Conn.t())) :: Plug.Conn.t()
    def serve(conn, session, message, context, opts, json_reply) do
      caller = self()
      reply_ref = make_ref()

      {dispatcher, monitor} =
        spawn_monitor(fn -> send(caller, {reply_ref, dispatch(session, message, context, opts)}) end)

      await(%{
        conn: conn,
        streaming?: false,
        dispatcher: dispatcher,
        monitor: monitor,
        reply_ref: reply_ref,
        message: message,
        json_reply: json_reply,
        on_cancel: Map.get(opts, :on_cancel, fn -> :ok end),
        scoped: scoped_methods(message)
      })
    end

    defp dispatch(session, message, context, opts) do
      Session.dispatch_request(session, message, context, timeout: opts.timeout)
    catch
      :exit, reason -> {:exit, reason}
    end

    defp scoped_methods(message) do
      if get_in(message, ["params", "_meta", @log_level_key]),
        do: ["notifications/progress", "notifications/message"],
        else: ["notifications/progress"]
    end

    defp await(%{reply_ref: reply_ref, monitor: monitor} = state) do
      receive do
        {^reply_ref, result} ->
          Process.demonitor(monitor, [:flush])
          finish(state, result)

        {:DOWN, ^monitor, :process, _pid, reason} ->
          finish(state, {:exit, reason})

        {:sse_message, notification} ->
          forward(state, notification)

        {:sse_message, notification, {from, reply_ref}} ->
          send(from, {reply_ref, :ok})
          forward(state, notification)

        {:sse_message, notification, _event_id} ->
          forward(state, notification)

        :sse_keepalive ->
          state = open(state)
          write(state, fn conn -> Plug.Conn.chunk(conn, ": keepalive\n\n") end)

        :close_sse ->
          close(state)

        {:EXIT, _pid, :shutdown} ->
          close(state)
          exit(:shutdown)

        {:EXIT, _pid, {:shutdown, _} = reason} ->
          close(state)
          exit(reason)

        _other ->
          await(state)
      end
    end

    defp forward(state, notification) do
      if request_scoped?(notification, state.scoped) do
        state = open(state)
        write(state, fn conn -> Streaming.send_event(conn, String.trim_trailing(notification), nil) end)
      else
        await(state)
      end
    end

    defp request_scoped?(notification, scoped) do
      case JSON.decode(notification) do
        {:ok, %{"method" => method}} -> method in scoped
        _other -> false
      end
    end

    defp open(%{streaming?: true} = state), do: state
    defp open(state), do: %{state | conn: Streaming.prepare_connection(state.conn), streaming?: true}

    defp write(state, fun) do
      case fun.(state.conn) do
        {:ok, conn} ->
          await(%{state | conn: conn})

        {:error, reason} ->
          Logging.transport_event("response_stream_closed", %{reason: reason})
          cancel(state)
          state.conn
      end
    end

    defp cancel(state) do
      Process.exit(state.dispatcher, :kill)
      Process.demonitor(state.monitor, [:flush])
      state.on_cancel.()
    end

    defp close(state) do
      Logging.transport_event("response_stream_closing", %{id: state.message["id"]})
      cancel(state)
      finish(state, {:error, Error.protocol(:internal_error, %{message: "Server shutting down"})})
    end

    defp finish(%{streaming?: false}, {:exit, reason}), do: exit(reason)
    defp finish(%{streaming?: false} = state, result), do: state.json_reply.(state.conn, result)

    defp finish(state, {:ok, nil}), do: state.conn

    defp finish(state, {:ok, response}) when is_binary(response) do
      case Streaming.send_event(state.conn, String.trim_trailing(response), nil) do
        {:ok, conn} ->
          conn

        {:error, reason} ->
          Logging.transport_event("response_stream_closed", %{reason: reason})
          state.conn
      end
    end

    defp finish(state, other) do
      error =
        case other do
          {:error, reason} -> Error.wrap_reason(reason)
          _no_reply -> Error.protocol(:internal_error, %{message: "Server unavailable"})
        end

      {:ok, encoded} = Error.to_json_rpc(error, state.message["id"])
      finish(state, {:ok, encoded})
    end
  end
end
