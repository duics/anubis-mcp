defmodule Anubis.Server.Session.Scheduler do
  @moduledoc false

  # Internal request scheduling engine. State fields live in the Session's
  # state map; this module transforms them and performs side effects at the
  # edges on behalf of Anubis.Server.Session.
  use Anubis.Logging

  alias Anubis.MCP.Error
  alias Anubis.MCP.Message
  alias Anubis.Server.Frame
  alias Anubis.Server.Stateless
  alias Anubis.Telemetry

  @type in_flight :: %{
          ref: reference(),
          pid: pid(),
          request_id: String.t(),
          from: GenServer.from(),
          started_at: integer(),
          method: String.t(),
          stateless: Stateless.t() | nil,
          cache_hints: %{String.t() => term()} | nil,
          retry?: boolean(),
          caller_ref: reference(),
          disconnected?: boolean(),
          progress_token: String.t() | integer() | nil,
          telemetry: %{tool: String.t() | nil, retry: boolean(), input_responses: non_neg_integer()}
        }

  @type queued_request :: {map(), map(), GenServer.from()}
  @type deferred_callback :: {:cast | :info, term()}
  @type state :: map()
  @type frame_fn :: (state(), map() | nil -> Frame.t())

  @type apply_deferred_fn ::
          (deferred_callback(), state() ->
             {:noreply, state()} | {:noreply, state(), term()} | {:stop, term(), state()})

  @type callbacks :: %{frame: frame_fn(), apply_deferred: apply_deferred_fn()}

  @doc """
  Dispatches the request immediately when no request is in flight, otherwise
  appends it to the request queue.
  """
  @spec enqueue_or_dispatch(map(), map(), GenServer.from(), state(), callbacks()) :: {:noreply, state()}
  def enqueue_or_dispatch(request, ctx, from, %{in_flight: nil} = state, callbacks) do
    {:noreply, dispatch_request(request, ctx, from, state, callbacks.frame)}
  end

  def enqueue_or_dispatch(request, ctx, from, state, _callbacks) do
    {:noreply, %{state | request_queue: :queue.in({request, ctx, from}, state.request_queue)}}
  end

  @doc """
  Defers a cast or info message to be applied once the in-flight request
  completes.
  """
  @spec defer(state(), deferred_callback()) :: state()
  def defer(state, item) do
    %{state | deferred_callbacks: :queue.in(item, state.deferred_callbacks)}
  end

  @doc """
  Handles completion of the in-flight request task: decodes the handler
  result, replies to the caller, drains deferred callbacks, and dispatches
  the next queued request.
  """
  @spec handle_completion(reference(), term(), state(), callbacks()) ::
          {:noreply, state()} | {:stop, term(), state()}
  def handle_completion(ref, callback_result, %{in_flight: %{ref: ref} = inflight} = state, callbacks) do
    Process.demonitor(ref, [:flush])
    release_caller(inflight)
    {reply, state} = decode_task_result(callback_result, inflight, state)
    state = complete_request(%{state | in_flight: nil}, inflight.request_id)
    GenServer.reply(inflight.from, reply)

    finalize_after_task(state, callbacks)
  end

  @doc """
  Handles a crash of the in-flight request task, replying to the caller with
  an internal error before finalizing.
  """
  @spec handle_down(term(), state(), callbacks()) :: {:noreply, state()} | {:stop, term(), state()}
  def handle_down(reason, %{in_flight: inflight} = state, callbacks) do
    Logging.server_event(
      "request_task_crashed",
      %{request_id: inflight.request_id, method: inflight.method, tool: inflight.telemetry.tool, reason: inspect(reason)},
      level: :error
    )

    Telemetry.execute(
      Telemetry.event_server_error(),
      %{system_time: System.system_time()},
      request_event_metadata(inflight, %{error: reason})
    )

    release_caller(inflight)
    error = Error.protocol(:internal_error, %{message: "Tool execution crashed"})
    reply = {:ok, encode_reply(Error.build_json_rpc(error, inflight.request_id))}

    state = complete_request(%{state | in_flight: nil}, inflight.request_id)
    GenServer.reply(inflight.from, reply)

    finalize_after_task(state, callbacks)
  end

  @doc """
  Records that the in-flight request's caller exited. A caller that exits with
  `{:shutdown, :client_disconnected}` lost its client's response stream (see
  `Anubis.Server.Transport.StreamableHTTP.ResponseStream`), so the request is
  marked disconnected and keeps running.
  """
  @spec caller_down(state(), term()) :: state()
  def caller_down(%{in_flight: inflight} = state, reason) do
    %{state | in_flight: %{inflight | disconnected?: reason == {:shutdown, :client_disconnected}}}
  end

  @doc """
  Whether `request_id` is the in-flight request and its client disconnected
  from its response stream.
  """
  @spec disconnected?(state(), term()) :: boolean()
  def disconnected?(%{in_flight: %{request_id: request_id, disconnected?: disconnected?}}, request_id), do: disconnected?

  def disconnected?(_state, _request_id), do: false

  @doc """
  Whether `token` is the `progressToken` of the in-flight request and its
  client disconnected from its response stream. Progress a process spawned by
  the handler sends carries no request id, only the token.
  """
  @spec progress_disconnected?(state(), term()) :: boolean()
  def progress_disconnected?(%{in_flight: %{disconnected?: true, progress_token: token}}, token) when not is_nil(token),
    do: true

  def progress_disconnected?(_state, _token), do: false

  @doc """
  Handles a `notifications/cancelled` notification, cancelling either the
  in-flight request or any queued entries matching the request ID.
  """
  @spec cancel(map(), state(), callbacks()) :: {:noreply, state()} | {:stop, term(), state()}
  def cancel(notification, state, callbacks) do
    params = notification["params"] || %{}
    request_id = params["requestId"]
    reason = Map.get(params, "reason", "cancelled")

    cond do
      in_flight?(state, request_id) ->
        cancel_in_flight(state, request_id, reason, callbacks)

      queued?(state, request_id) ->
        cancel_queued(state, request_id, reason)

      true ->
        Logging.server_event("cancellation_for_unknown_request", %{
          session_id: state.session_id,
          request_id: request_id,
          reason: reason
        })

        {:noreply, state}
    end
  end

  @doc """
  Replies to the in-flight and queued callers with the given error during
  session termination, terminating the in-flight worker task.
  """
  @spec reply_pending(state(), Error.t()) :: :ok
  def reply_pending(%{in_flight: in_flight, request_queue: q, task_supervisor: task_supervisor}, error) do
    if in_flight do
      Task.Supervisor.terminate_child(task_supervisor, in_flight.pid)
      Process.demonitor(in_flight.ref, [:flush])
      flush_task_reply(in_flight.ref)

      reply = {:ok, encode_reply(Error.build_json_rpc(error, in_flight.request_id))}
      GenServer.reply(in_flight.from, reply)
    end

    Enum.each(:queue.to_list(q), fn {%{"id" => request_id}, _ctx, from} ->
      reply = {:ok, encode_reply(Error.build_json_rpc(error, request_id))}
      GenServer.reply(from, reply)
    end)
  end

  defp dispatch_request(%{"id" => request_id, "method" => method} = request, transport_context, from, state, frame_fn) do
    telemetry = Telemetry.request_metadata(request)

    Logging.server_event("handling_request", %{
      id: request_id,
      method: method,
      tool: telemetry.tool,
      session_id: state.session_id
    })

    state = track_request(state, request_id, method)

    Telemetry.execute(
      Telemetry.event_server_request(),
      %{system_time: System.system_time()},
      request_event_metadata(%{telemetry: telemetry, request_id: request_id, method: method}, %{})
    )

    frame = Frame.put_request_meta(frame_fn.(state, transport_context), request)
    module = state.server_module
    session = self()

    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        Anubis.Server.put_session(session, request_id)
        do_handle_request(module, request, frame, method)
      end)

    {caller, _tag} = from

    %{
      state
      | in_flight: %{
          ref: task.ref,
          pid: task.pid,
          request_id: request_id,
          from: from,
          started_at: System.monotonic_time(:millisecond),
          method: method,
          stateless: Stateless.context(transport_context),
          cache_hints: cache_hints(request, transport_context, state),
          retry?: retry?(request),
          caller_ref: Process.monitor(caller),
          disconnected?: false,
          progress_token: progress_token(request),
          telemetry: telemetry
        }
    }
  end

  defp cache_hints(%{"method" => method} = request, transport_context, state) do
    if Stateless.context(transport_context) do
      Stateless.cache_hints(state.server_module, method, retry?(request))
    end
  end

  defp progress_token(%{"params" => %{"_meta" => %{"progressToken" => token}}}), do: token
  defp progress_token(_request), do: nil

  defp retry?(request) do
    params = request["params"] || %{}
    Map.has_key?(params, "inputResponses") or Map.has_key?(params, "requestState")
  end

  defp do_handle_request(module, %{"method" => "tools/call"} = request, frame, _method) do
    Telemetry.span_tool_call(request, fn -> module.handle_request(request, frame) end)
  end

  defp do_handle_request(module, request, frame, _method) do
    module.handle_request(request, frame)
  end

  defp decode_task_result({:reply, response, %Frame{} = frame}, inflight, state) do
    response = shape_response(response, inflight, state)

    Telemetry.execute(
      Telemetry.event_server_response(),
      %{system_time: System.system_time()},
      request_event_metadata(inflight, %{status: :success, result_type: Telemetry.result_type(response)})
    )

    reply = {:ok, encode_reply(Message.build_response(response, inflight.request_id))}
    {reply, %{state | frame: frame}}
  end

  defp decode_task_result({:noreply, %Frame{} = frame}, inflight, state) do
    Telemetry.execute(
      Telemetry.event_server_response(),
      %{system_time: System.system_time()},
      request_event_metadata(inflight, %{status: :noreply})
    )

    {{:ok, nil}, %{state | frame: frame}}
  end

  defp decode_task_result({:error, %Error{} = error, %Frame{} = frame}, inflight, state) do
    Logging.server_event(
      "request_error",
      %{id: inflight.request_id, method: inflight.method, tool: inflight.telemetry.tool, error: error},
      level: :warning
    )

    Telemetry.execute(
      Telemetry.event_server_error(),
      %{system_time: System.system_time()},
      request_event_metadata(inflight, %{error: error})
    )

    reply = {:ok, encode_reply(Error.build_json_rpc(error, inflight.request_id))}
    {reply, %{state | frame: frame}}
  end

  defp decode_task_result(other, inflight, state) do
    Logging.server_event(
      "invalid_handle_request_return",
      %{id: inflight.request_id, method: inflight.method, tool: inflight.telemetry.tool, returned: inspect(other)},
      level: :error
    )

    Telemetry.execute(
      Telemetry.event_server_error(),
      %{system_time: System.system_time()},
      request_event_metadata(inflight, %{error: :invalid_return})
    )

    error = Error.protocol(:internal_error, %{message: "Invalid handler return value"})
    reply = {:ok, encode_reply(Error.build_json_rpc(error, inflight.request_id))}
    {reply, state}
  end

  # Only a response with a result has a result_type.
  defp request_event_metadata(%{telemetry: telemetry, request_id: id, method: method}, extra) do
    telemetry
    |> Map.merge(%{id: id, method: method, result_type: nil})
    |> Map.merge(extra)
  end

  defp shape_response(response, %{stateless: nil}, _state), do: response

  defp shape_response(response, inflight, state) when is_map(response) and not is_struct(response) do
    Stateless.shape_result(response, state.server_info, inflight.cache_hints, inflight.retry?)
  end

  defp shape_response(response, _inflight, _state), do: response

  defp drain_deferred_callbacks(%{deferred_callbacks: q} = state, apply_fn) do
    state = %{state | deferred_callbacks: :queue.new()}

    Enum.reduce_while(:queue.to_list(q), state, fn item, acc ->
      case apply_fn.(item, acc) do
        {:noreply, new_state} -> {:cont, new_state}
        {:noreply, new_state, _cont} -> {:cont, new_state}
        {:stop, _reason, _new_state} = stop -> {:halt, stop}
      end
    end)
  end

  defp finalize_after_task(state, callbacks) do
    case drain_deferred_callbacks(state, callbacks.apply_deferred) do
      {:stop, _reason, _new_state} = stop -> stop
      new_state -> new_state |> dispatch_next_queued(callbacks.frame) |> noreply()
    end
  end

  defp dispatch_next_queued(%{request_queue: q} = state, frame_fn) do
    case :queue.out(q) do
      {:empty, _} ->
        state

      {{:value, {request, ctx, from}}, rest} ->
        dispatch_request(request, ctx, from, %{state | request_queue: rest}, frame_fn)
    end
  end

  defp noreply(state), do: {:noreply, state}

  # The caller is monitored only while its request is in flight: an HTTP
  # connection process can call for many requests in turn.
  defp release_caller(%{caller_ref: ref}), do: Process.demonitor(ref, [:flush])

  defp in_flight?(%{in_flight: %{request_id: rid}}, rid), do: true
  defp in_flight?(_, _), do: false

  defp queued?(%{request_queue: q}, rid) do
    Enum.any?(:queue.to_list(q), fn {%{"id" => id}, _ctx, _from} -> id == rid end)
  end

  defp cancel_in_flight(%{in_flight: inflight} = state, request_id, reason, callbacks) do
    Task.Supervisor.terminate_child(state.task_supervisor, inflight.pid)
    Process.demonitor(inflight.ref, [:flush])
    release_caller(inflight)
    flush_task_reply(inflight.ref)

    Logging.server_event("request_cancelled", %{
      session_id: state.session_id,
      request_id: request_id,
      reason: reason,
      method: inflight.method,
      duration_ms: System.monotonic_time(:millisecond) - inflight.started_at
    })

    emit_cancellation_telemetry(state.session_id, request_id)

    error = Error.execution("Request cancelled", %{reason: reason})
    reply = {:ok, encode_reply(Error.build_json_rpc(error, request_id))}
    GenServer.reply(inflight.from, reply)

    state = complete_request(%{state | in_flight: nil}, request_id)
    finalize_after_task(state, callbacks)
  end

  defp cancel_queued(state, request_id, reason) do
    {cancelled, kept} =
      state.request_queue
      |> :queue.to_list()
      |> Enum.split_with(fn {%{"id" => id}, _ctx, _from} -> id == request_id end)

    error = Error.execution("Request cancelled", %{reason: reason})
    reply = {:ok, encode_reply(Error.build_json_rpc(error, request_id))}

    Enum.each(cancelled, fn {_request, _ctx, from} -> GenServer.reply(from, reply) end)

    Logging.server_event("queued_request_cancelled", %{
      session_id: state.session_id,
      request_id: request_id,
      reason: reason
    })

    emit_cancellation_telemetry(state.session_id, request_id)

    {:noreply, %{state | request_queue: :queue.from_list(kept)}}
  end

  defp emit_cancellation_telemetry(session_id, request_id) do
    Telemetry.execute(
      Telemetry.event_server_notification(),
      %{system_time: System.system_time()},
      %{method: "cancelled", session_id: session_id, request_id: request_id}
    )
  end

  defp track_request(state, request_id, method) do
    request_info = %{
      started_at: System.system_time(:millisecond),
      method: method
    }

    %{state | pending_requests: Map.put(state.pending_requests, request_id, request_info)}
  end

  defp complete_request(state, request_id) do
    %{state | pending_requests: Map.delete(state.pending_requests, request_id)}
  end

  defp flush_task_reply(ref) do
    receive do
      {^ref, _result} -> :ok
    after
      0 -> :ok
    end
  end

  defp encode_reply(message) when is_map(message) do
    JSON.encode!(message)
  end
end
