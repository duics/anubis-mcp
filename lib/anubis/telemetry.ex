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
  | `[:anubis_mcp, :server, :send_failure]` | `kind`, `method`, `reason`, `error`, `session_id` |

  ### Request metadata

  The `:request`, `:response` and `:error` events emitted for a client
  request also carry the keys below. The tool call span's `:start` and
  `:stop` events carry `session_id`, `protocol_version`, `tool`, `retry` and
  `input_responses`, with `result_type` on `:stop` only, and no `prompt` or
  `resource_uri` key (see `span_tool_call/3`).

    * `session_id` - the id of the session serving the request.
    * `protocol_version` - the protocol version the request speaks.
    * `tool` - the tool name of a `tools/call`, `nil` for any other method.
    * `prompt` - the prompt name of a `prompts/get`, `nil` otherwise.
    * `resource_uri` - the URI of a `resources/read`, `resources/subscribe`
      or `resources/unsubscribe`, `nil` otherwise. A URI can have unbounded
      cardinality, so think twice before using it as a metric tag.
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
  log lines. Correlate them through `session_id` and `id` instead: a JSON-RPC
  id is only unique within its session.

  The names follow the OpenTelemetry semantic conventions for MCP:
  `gen_ai.tool.name`, `gen_ai.prompt.name`, `mcp.resource.uri`,
  `mcp.session.id` and `mcp.protocol.version`.

  ### Undelivered messages

  `[:anubis_mcp, :server, :send_failure]` fires when a session cannot hand a
  notification or a server-to-client request (sampling, roots, elicitation)
  to its transport, so it is never delivered; nothing is retried or buffered.
  `kind` is `:notification` or `:request`, `method` names the message, and
  `reason` is a bounded atom: the transport's own reason when it gave one
  (`:no_sse_handler` when the session has no GET stream open, locally or on
  another node through `:pg`), otherwise the error's (such as `:no_transport`
  or `:unsupported_protocol_version`), or `:other`. `error` is the full error
  term. The session logs a `:no_sse_handler` notification failure at `:debug`
  only, since a client that opens no GET stream has opted out of unrelated
  notifications; count this event to see the drop rate, for example to catch
  routing loss between nodes.

  ### Tool calls

  `[:anubis_mcp, :server, :tool_call]` is a `:telemetry.span/3`, so it emits
  `:start`, `:stop` and `:exception` events. See `span_tool_call/3`.

  ### Authorization

  `[:anubis_mcp, :server, :authorization]` is a `:telemetry.span/3` wrapping
  the Streamable HTTP bearer authorization flow, so it emits `:start`, `:stop`
  and `:exception` events. See `span_authorization/2`. The `:stop` metadata
  carries `outcome`, one of `:ok`, `:missing_token`, `:invalid_token`,
  `:invalid_issuer`, `:invalid_audience`, `:token_expired`, `:invalid_expiry`,
  `:validator_unavailable`, `:other`. Authorization metadata never includes
  tokens, claims, or raw validator error reasons.

  | Event | Metadata |
  | --- | --- |
  | `[:anubis_mcp, :server, :authorization, :start]` | `validator` |
  | `[:anubis_mcp, :server, :authorization, :stop]` | `validator`, `outcome` |
  | `[:anubis_mcp, :server, :authorization, :exception]` | `validator`, `kind`, `reason`, `stacktrace` |
  | `[:anubis_mcp, :server, :authorization, :validate]` (deprecated) | `validator` |

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

  @resource_uri_methods ["resources/read", "resources/subscribe", "resources/unsubscribe"]

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
  `input_responses` on both, and `span_tool_call/3` given a request also the
  `session_id` and `protocol_version` it is passed. When
  `:telemetry_capture_tool_payload` is enabled (defaults to `false`), the
  `:start` metadata also carries `arguments` and the `:stop` metadata also
  carries `result`. See `pages/testing.md` for the rationale behind the
  opt-in default.

  ## Examples

      iex> Anubis.Telemetry.span_tool_call("get_weather", %{"city" => "NYC"}, fn -> :ok end)
      :ok
  """
  @spec span_tool_call(String.t() | nil, map() | nil, (-> result)) :: result when result: var
  @spec span_tool_call(map(), %{optional(:session_id) => term(), optional(:protocol_version) => term()}, (-> result)) ::
          result
        when result: var
  def span_tool_call(request, session_metadata, fun) when is_map(request) do
    request_tool_call_span(request, Map.take(session_metadata, [:session_id, :protocol_version]), fun)
  end

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
  def span_tool_call(request, fun) when is_map(request), do: request_tool_call_span(request, %{}, fun)

  defp request_tool_call_span(request, metadata, fun) do
    params = if is_map(request["params"]), do: request["params"], else: %{}
    metadata = request |> request_metadata() |> Map.take([:retry, :input_responses]) |> Map.merge(metadata)
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
  Returns the request metadata a decoded client request carries itself
  (`tool`, `prompt`, `resource_uri`, `retry`, `input_responses`). See
  "Request metadata" above; the session adds `session_id` and
  `protocol_version`.

  ## Examples

      iex> Anubis.Telemetry.request_metadata(%{"method" => "tools/call", "params" => %{"name" => "greet"}})
      %{tool: "greet", prompt: nil, resource_uri: nil, retry: false, input_responses: 0}

      iex> Anubis.Telemetry.request_metadata(%{
      ...>   "method" => "resources/read",
      ...>   "params" => %{"uri" => "notes://draft", "inputResponses" => %{"unlock" => %{}}}
      ...> })
      %{tool: nil, prompt: nil, resource_uri: "notes://draft", retry: true, input_responses: 1}
  """
  @spec request_metadata(map()) :: %{
          tool: String.t() | nil,
          prompt: String.t() | nil,
          resource_uri: String.t() | nil,
          retry: boolean(),
          input_responses: non_neg_integer()
        }
  def request_metadata(request) do
    params = if is_map(request["params"]), do: request["params"], else: %{}
    method = request["method"]

    %{
      tool: if(method == "tools/call", do: string_or_nil(params["name"])),
      prompt: if(method == "prompts/get", do: string_or_nil(params["name"])),
      resource_uri: if(method in @resource_uri_methods, do: string_or_nil(params["uri"])),
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

  defp string_or_nil(value) when is_binary(value), do: value
  defp string_or_nil(_value), do: nil

  defp input_response_count(responses) when is_map(responses), do: map_size(responses)
  defp input_response_count(_responses), do: 0

  @doc """
  Wraps a bearer authorization flow in the `[:server, :authorization]`
  telemetry span.

  Emits `:start`, `:stop` and `:exception` events under
  `[:anubis_mcp, :server, :authorization]`. The `start_metadata` map (typically
  `%{validator: module}`) is used for the `:start` event and, with `outcome`
  added, for the `:stop` event. The `:stop` measurements carry `duration`.

  The fun returns `{result, outcome}` where `outcome` must be drawn from the
  bounded set in `Anubis.Server.Authorization.outcome/1`. Tokens, claims, and
  raw validator error reasons must not be added to the metadata.

  ## Examples

      Telemetry.span_authorization(%{validator: MyValidator}, fn ->
        {{:ok, conn, claims}, :ok}
      end)
  """
  @spec span_authorization(map(), (-> {result, Anubis.Server.Authorization.outcome()})) ::
          result
        when result: var
  def span_authorization(start_metadata, fun) do
    :telemetry.span(
      [:anubis_mcp | event_server_authorization()],
      start_metadata,
      fn ->
        {result, outcome} = fun.()
        {result, Map.put(start_metadata, :outcome, outcome)}
      end
    )
  end

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
  def event_server_authorization, do: [:server, :authorization]
  def event_server_send_failure, do: [:server, :send_failure]

  @doc """
  Deprecated single-shot event emitted before the validator runs.

  Superseded by the `[:server, :authorization]` span: the `:stop` event's
  bounded `outcome` reports whether validation succeeded and why it failed.
  Kept during a deprecation window; attach handlers to the span instead.
  """
  def event_server_authorization_validate, do: [:server, :authorization, :validate]

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
