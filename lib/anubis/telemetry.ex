defmodule Anubis.Telemetry do
  @moduledoc """
  Telemetry events emitted by Anubis.

  Every event is namespaced under `[:anubis_mcp | event]`. The client emits
  them, the server emits them, and the transports emit their own.

  ## Attaching handlers

      :telemetry.attach_many(
        "anubis-metrics",
        [
          [:anubis_mcp, :server, :request],
          [:anubis_mcp, :server, :tool_call, :stop]
        ],
        &MyApp.Metrics.handle_event/4,
        %{}
      )

  ## Events

  Most events carry `%{system_time: System.system_time()}` as their only
  measurement; the interesting payload is in the metadata. The
  `:response` and `:error` events also carry `%{duration: elapsed_ms}`.

  ### Client

  | Event | Metadata |
  | --- | --- |
  | `[:anubis_mcp, :client, :init]` | `client_name`, `transport`, `protocol_version`, `capabilities` |
  | `[:anubis_mcp, :client, :request]` | `method`, `request_id` |
  | `[:anubis_mcp, :client, :response]` | `id`, `method`, `status`, `transport` |
  | `[:anubis_mcp, :client, :notification]` | `method`, `uri` |
  | `[:anubis_mcp, :client, :error]` | `id`, `method`, `error` |
  | `[:anubis_mcp, :client, :terminate]` | `client_name`, `reason`, `pending_requests` |
  | `[:anubis_mcp, :client, :roots]` | `action`, `request_id` |

  ### Server

  | Event | Metadata |
  | --- | --- |
  | `[:anubis_mcp, :server, :init]` | `session_id`, `capabilities`, `server_info`, `module` |
  | `[:anubis_mcp, :server, :request]` | `id`, `method`, plus the request metadata below |
  | `[:anubis_mcp, :server, :response]` | `id`, `method`, `status`, plus the request metadata below |
  | `[:anubis_mcp, :server, :notification]` | `method`, `server_module`, `started_at` |
  | `[:anubis_mcp, :server, :error]` | `error`; for a client request also `id`, `method` and the request metadata below |
  | `[:anubis_mcp, :server, :terminate]` | `reason` |

  ### Request metadata

  The `:request`, `:response` and `:error` events emitted for a client
  request, and the tool call span's `:stop` event, also carry:

    * `tool` - what the request names: the tool name for `tools/call`, the
      prompt name for `prompts/get`, the resource URI for `resources/*`, and
      `nil` for any other method.
    * `result_type` - `:complete` or `:input_required` (an
      `InputRequiredResult` asking the client for more input). It is `nil`
      where there is no result: on `:request`, on `:error`, on a
      `:response` whose handler did not reply, and on the span `:stop` of a
      call that returned an error.
    * `retry` - `true` when the request is a multi round-trip retry, i.e. its
      params carry `inputResponses` or `requestState`.
    * `input_responses` - how many `inputResponses` keys the request carries.

  An `:input_required` response still has `status: :success`, so a handler
  that counts outcomes should look at `result_type`.

  These events are emitted from the session process, while the tool itself
  runs in a task, so Logger metadata a tool sets is not on the session's own
  log lines. Correlate them through `id`, `method` and `tool` instead.

  ### Tool calls

  `[:anubis_mcp, :server, :tool_call]` is a `:telemetry.span/3`, so it emits
  `:start`, `:stop` and `:exception` events. See `span_tool_call/3`.

  ### Transport

  Transport events always carry `transport` (the layer, e.g. `:stdio` or
  `:streamable_http`). The rest varies by transport:

  | Event | Metadata |
  | --- | --- |
  | `[:anubis_mcp, :transport, :init]` | `transport`, `client`, `io_device` |
  | `[:anubis_mcp, :transport, :connect]` | `transport`, `client` |
  | `[:anubis_mcp, :transport, :send]` | `transport`, `client` |
  | `[:anubis_mcp, :transport, :receive]` | `transport`, `client`, `message_size` |
  | `[:anubis_mcp, :transport, :disconnect]` | `transport`, `client`, `reason` |
  | `[:anubis_mcp, :transport, :terminate]` | `transport`, `client`, `reason` |
  | `[:anubis_mcp, :transport, :error]` | `transport`, `client`, `error` |
  | `[:anubis_mcp, :transport, :sse_handler, :registered]` | `session_id`, `handler_pid`, `handler_count` (measurement) |

  Treat metadata as open: new keys may be added, so match on the ones you
  need and ignore the rest.
  """

  @default_capture_tool_payload false

  @doc """
  Execute a telemetry event with the Anubis MCP namespace.

  ## Parameters
  - `event_name` - List of atoms for the event name, excluding the :anubis_mcp prefix
  - `measurements` - Map of measurements for the event
  - `metadata` - Map of metadata for the event
  """
  @spec execute(list(atom()), map(), map()) :: :ok
  def execute(event_name, measurements, metadata) do
    :telemetry.execute([:anubis_mcp | event_name], measurements, metadata)
  end

  @doc """
  Wraps a tool call handler invocation in the `[:server, :tool_call]`
  telemetry span.

  Shared by the synchronous scheduler dispatch and the task-augmented
  `tools/call` worker path so both surface identical span data regardless
  of which route a given request took.

  The span's `:start` metadata always carries `tool`; the `:stop` metadata
  always carries `tool`, `is_error` and `result_type` (`nil` when the call
  returned an error). `span_tool_call/2` puts the request's `retry` and
  `input_responses` on both. When
  `:telemetry_capture_tool_payload` is enabled (defaults to `false`), the
  `:start` metadata also carries `arguments` and the `:stop` metadata also
  carries `result`. See `pages/testing.md` for the rationale behind the
  opt-in default.

  ## Examples

      iex> Anubis.Telemetry.span_tool_call("get_weather", %{"city" => "NYC"}, fn -> :ok end)
      :ok
  """
  @spec span_tool_call(String.t() | nil, map() | nil, (-> result)) :: result when result: var
  def span_tool_call(tool_name, arguments, fun), do: tool_call_span(tool_name, arguments, fun, %{})

  @doc """
  Wraps the handler of a decoded `tools/call` `request` in the tool call span,
  naming the tool and arguments its params carry, with its `retry` and
  `input_responses` (see "Request metadata") on both events.

  ## Examples

      iex> request = %{"method" => "tools/call", "params" => %{"name" => "get_weather"}}
      iex> Anubis.Telemetry.span_tool_call(request, fn -> :ok end)
      :ok
  """
  @spec span_tool_call(map(), (-> result)) :: result when result: var
  def span_tool_call(request, fun) when is_map(request) do
    params = if is_map(request["params"]), do: request["params"], else: %{}
    metadata = request |> request_metadata() |> Map.take([:retry, :input_responses])
    tool_call_span(params["name"], params["arguments"], fun, metadata)
  end

  defp tool_call_span(tool_name, arguments, fun, metadata) do
    capture_payload? =
      Application.get_env(:anubis_mcp, :telemetry_capture_tool_payload, @default_capture_tool_payload)

    base_metadata = Map.put(metadata, :tool, tool_name)

    start_metadata =
      if capture_payload? do
        Map.put(base_metadata, :arguments, arguments)
      else
        base_metadata
      end

    :telemetry.span(
      [:anubis_mcp | event_server_tool_call()],
      start_metadata,
      fn ->
        result = fun.()
        is_error = tool_call_error?(result)

        stop_metadata = Map.merge(base_metadata, %{is_error: is_error, result_type: tool_call_result_type(result)})
        stop_metadata = if capture_payload?, do: Map.put(stop_metadata, :result, result), else: stop_metadata

        {result, stop_metadata}
      end
    )
  end

  defp tool_call_error?({:error, _reason, _frame}), do: true
  defp tool_call_error?({:reply, %{"isError" => true}, _frame}), do: true
  defp tool_call_error?(_result), do: false

  defp tool_call_result_type({:reply, response, _frame}), do: result_type(response)
  defp tool_call_result_type(_result), do: nil

  @doc """
  Returns the request metadata (`tool`, `retry`, `input_responses`) for a
  decoded client request. See "Request metadata" above.

  ## Examples

      iex> Anubis.Telemetry.request_metadata(%{"method" => "tools/call", "params" => %{"name" => "greet"}})
      %{tool: "greet", retry: false, input_responses: 0}

      iex> Anubis.Telemetry.request_metadata(%{
      ...>   "method" => "resources/read",
      ...>   "params" => %{"uri" => "notes://draft", "inputResponses" => %{"unlock" => %{}}}
      ...> })
      %{tool: "notes://draft", retry: true, input_responses: 1}
  """
  @spec request_metadata(map()) :: %{tool: String.t() | nil, retry: boolean(), input_responses: non_neg_integer()}
  def request_metadata(request) do
    params = if is_map(request["params"]), do: request["params"], else: %{}

    %{
      tool: request_subject(request["method"], params),
      retry: Map.has_key?(params, "inputResponses") or Map.has_key?(params, "requestState"),
      input_responses: input_response_count(params["inputResponses"])
    }
  end

  @doc """
  Returns the `result_type` of a handler result: `:input_required` for an
  `InputRequiredResult`, `:complete` for any other.

  ## Examples

      iex> Anubis.Telemetry.result_type(%{"resultType" => "input_required"})
      :input_required

      iex> Anubis.Telemetry.result_type(%{"content" => []})
      :complete
  """
  @spec result_type(term()) :: :complete | :input_required
  def result_type(%{"resultType" => "input_required"}), do: :input_required
  def result_type(_result), do: :complete

  defp request_subject(method, params) when method in ["tools/call", "prompts/get"], do: string_or_nil(params["name"])
  defp request_subject("resources/" <> _rest, params), do: string_or_nil(params["uri"])
  defp request_subject(_method, _params), do: nil

  defp string_or_nil(value) when is_binary(value), do: value
  defp string_or_nil(_value), do: nil

  defp input_response_count(responses) when is_map(responses), do: map_size(responses)
  defp input_response_count(_responses), do: 0

  # Define event name constants to ensure consistency

  # Client events
  def event_client_init, do: [:client, :init]
  def event_client_request, do: [:client, :request]
  def event_client_response, do: [:client, :response]
  def event_client_terminate, do: [:client, :terminate]
  def event_client_error, do: [:client, :error]
  def event_client_notification, do: [:client, :notification]

  # Server events
  def event_server_init, do: [:server, :init]
  def event_server_request, do: [:server, :request]
  def event_server_response, do: [:server, :response]
  def event_server_notification, do: [:server, :notification]
  def event_server_error, do: [:server, :error]
  def event_server_terminate, do: [:server, :terminate]
  def event_server_tool_call, do: [:server, :tool_call]

  # Transport events
  def event_transport_init, do: [:transport, :init]
  def event_transport_connect, do: [:transport, :connect]
  def event_transport_send, do: [:transport, :send]
  def event_transport_receive, do: [:transport, :receive]
  def event_transport_disconnect, do: [:transport, :disconnect]
  def event_transport_error, do: [:transport, :error]
  def event_transport_terminate, do: [:transport, :terminate]
  def event_transport_sse_handler_registered, do: [:transport, :sse_handler, :registered]

  # Roots events
  def event_client_roots, do: [:client, :roots]
end
