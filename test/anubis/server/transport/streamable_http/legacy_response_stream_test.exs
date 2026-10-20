defmodule Anubis.Server.Transport.StreamableHTTP.LegacyResponseStreamTest do
  use Anubis.MCP.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Anubis.Server.Component
  alias Anubis.Server.Registry
  alias Anubis.Server.Supervisor, as: ServerSupervisor
  alias Anubis.Server.Transport.StreamableHTTP
  alias Anubis.Server.Transport.StreamableHTTP.Plug, as: StreamableHTTPPlug
  alias Anubis.Test.SSEAdapter

  @moduletag capture_log: true

  @version "2025-06-18"
  @accept_both "application/json, text/event-stream"

  defmodule SlowTool do
    @moduledoc false
    use Component, type: :tool

    alias Anubis.Server.Frame
    alias Anubis.Server.Response

    schema do
    end

    @impl true
    def execute(_params, frame) do
      token = Frame.progress_token(frame)
      if token, do: Anubis.Server.send_progress(token, 0, total: 100)
      send(LegacyResponseStreamTest, {:tool_running, self()})

      receive do
        :finish ->
          if token, do: Anubis.Server.send_progress(token, 100, total: 100)
          {:reply, Response.text(Response.tool(), "finished"), frame}
      end
    end
  end

  defmodule FastTool do
    @moduledoc false
    use Component, type: :tool

    alias Anubis.Server.Response

    schema do
    end

    @impl true
    def execute(_params, frame), do: {:reply, Response.text(Response.tool(), "fast"), frame}
  end

  defmodule AskTool do
    @moduledoc false
    use Component, type: :tool

    alias Anubis.Server.Response

    schema do
    end

    @impl true
    def execute(_params, frame) do
      Anubis.Server.send_elicitation_request("Which item?", %{
        "type" => "object",
        "properties" => %{"item" => %{"type" => "string"}}
      })

      Anubis.Server.send_log_message(:info, "asked")
      Anubis.Server.Notifier.progress(frame, "ask", 1)
      {:reply, Response.text(Response.tool(), "asked"), frame}
    end
  end

  defmodule LegacyServer do
    @moduledoc false
    use Anubis.Server,
      name: "legacy-streaming-server",
      version: "1.0.0",
      capabilities: [:tools, :logging],
      protocol_versions: ["2025-06-18"]

    component(SlowTool, name: "slow")
    component(FastTool, name: "fast")
    component(AskTool, name: "ask")

    @impl true
    def handle_elicitation(response, request_id, frame) do
      send(LegacyResponseStreamTest, {:elicitation_answered, request_id, response})
      {:noreply, frame}
    end
  end

  setup context do
    server = LegacyServer
    task_sup = Registry.task_supervisor_name(server)
    start_supervised!({Task.Supervisor, name: task_sup})

    registry_name = Registry.registry_name(server)
    start_supervised!({Registry.Local, name: registry_name})
    naming_registry = Registry.naming_registry_name(registry_name)
    start_supervised!({Elixir.Registry, keys: :unique, name: naming_registry})
    session_sup = Registry.session_supervisor_name(server)
    start_supervised!({DynamicSupervisor, name: session_sup, strategy: :one_for_one})

    http_transport = Registry.transport_name(server, :streamable_http)

    transport =
      start_supervised!(
        {StreamableHTTP,
         server: server,
         name: http_transport,
         task_supervisor: task_sup,
         keepalive: Map.get(context, :keepalive, false),
         keepalive_interval: 20}
      )

    :persistent_term.put({ServerSupervisor, server, :session_config}, %{
      server_module: server,
      registry_mod: Registry.Local,
      transport: [layer: StreamableHTTP, name: http_transport],
      session_idle_timeout: nil,
      timeout: 30_000,
      task_supervisor: task_sup
    })

    on_exit(fn -> :persistent_term.erase({ServerSupervisor, server, :session_config}) end)

    Process.register(self(), LegacyResponseStreamTest)
    opts = StreamableHTTPPlug.init(server: server)

    %{opts: opts, transport: transport, session_id: initialize(opts)}
  end

  describe "a POST that accepts SSE on a session" do
    @tag keepalive: true
    test "writes keepalive comments on its own stream while a slow tool runs", ctx do
      conn = sse_request(ctx.opts, ctx.session_id, %{}, "slow")
      request = Task.async(fn -> StreamableHTTPPlug.call(conn, ctx.opts) end)

      pid = request.pid

      assert_receive {:tool_running, tool}
      assert_receive {:sse_opened, ^pid}
      assert_receive {:sse_chunk, ^pid, ": keepalive\n\n"}, 1_000
      assert_receive {:sse_chunk, ^pid, ": keepalive\n\n"}, 1_000
      send(tool, :finish)
      conn = Task.await(request)

      assert [content_type | _] = get_resp_header(conn, "content-type")
      assert content_type =~ "text/event-stream"
      assert get_resp_header(conn, "mcp-session-id") == [ctx.session_id]
      assert [%{"id" => 1, "result" => %{"content" => [%{"text" => "finished"}]}}] = events(conn)
    end

    test "carries the request's progress ahead of the result, off the GET stream", ctx do
      :ok = StreamableHTTP.register_sse_handler(ctx.transport, ctx.session_id)

      request =
        Task.async(fn ->
          ctx.opts |> post(tool_call("slow", %{"progressToken" => "tok"}), @accept_both, ctx.session_id) |> call(ctx.opts)
        end)

      assert_receive {:tool_running, tool}
      send(tool, :finish)
      conn = Task.await(request)

      assert [content_type | _] = get_resp_header(conn, "content-type")
      assert content_type =~ "text/event-stream"

      assert [
               %{"method" => "notifications/progress", "params" => %{"progressToken" => "tok", "progress" => 0}},
               %{"method" => "notifications/progress", "params" => %{"progressToken" => "tok", "progress" => 100}},
               %{"id" => 1, "result" => %{"content" => [%{"text" => "finished"}]}}
             ] = events(conn)

      refute_received {:sse_message, _progress}

      later = progress("tok")
      :ok = send_progress(ctx.transport, ctx.session_id, later)
      assert_receive {:sse_message, ^later}
    end

    test "leaves another request's progress on the GET stream", ctx do
      :ok = StreamableHTTP.register_sse_handler(ctx.transport, ctx.session_id)

      request =
        Task.async(fn ->
          ctx.opts |> post(tool_call("slow", %{"progressToken" => "own"}), @accept_both, ctx.session_id) |> call(ctx.opts)
        end)

      assert_receive {:tool_running, tool}
      other = progress("other")
      :ok = send_progress(ctx.transport, ctx.session_id, other)
      assert_receive {:sse_message, ^other}

      send(tool, :finish)
      conn = Task.await(request)

      refute Enum.any?(events(conn), &(&1["params"]["progressToken"] == "other"))
    end

    test "answers a fast reply as JSON", ctx do
      conn = ctx.opts |> post(tool_call("fast"), @accept_both, ctx.session_id) |> call(ctx.opts)

      assert conn.status == 200
      assert [content_type | _] = get_resp_header(conn, "content-type")
      assert content_type =~ "application/json"
      assert %{"id" => 1, "result" => %{"content" => [%{"text" => "fast"}]}} = JSON.decode!(conn.resp_body)
    end

    test "carries what the handler sends ahead of the result, with no GET stream open", ctx do
      conn = ctx.opts |> post(tool_call("ask"), @accept_both, ctx.session_id) |> call(ctx.opts)

      assert [content_type | _] = get_resp_header(conn, "content-type")
      assert content_type =~ "text/event-stream"

      assert [
               %{"id" => elicitation_id, "method" => "elicitation/create", "params" => %{"message" => "Which item?"}},
               %{"method" => "notifications/message", "params" => %{"level" => "info", "data" => "asked"}},
               %{"method" => "notifications/progress", "params" => %{"progressToken" => "ask", "progress" => 1}},
               %{"id" => 1, "result" => %{"content" => [%{"text" => "asked"}]}}
             ] = events(conn)

      answer =
        JSON.encode!(%{
          "jsonrpc" => "2.0",
          "id" => elicitation_id,
          "result" => %{"action" => "accept", "content" => %{"item" => "first"}}
        })

      assert ctx.opts |> post(answer, "application/json", ctx.session_id) |> call(ctx.opts) |> Map.get(:status) == 202
      assert_receive {:elicitation_answered, ^elicitation_id, %{"action" => "accept", "content" => %{"item" => "first"}}}
    end

    test "falls back to JSON when the transport cannot register the stream", ctx do
      name = Registry.transport_name(LegacyServer, :streamable_http)
      Process.unregister(name)
      on_exit(fn -> if Process.alive?(ctx.transport), do: Process.register(ctx.transport, name) end)

      conn = ctx.opts |> post(tool_call("fast"), @accept_both, ctx.session_id) |> call(ctx.opts)

      assert conn.status == 200
      assert [content_type | _] = get_resp_header(conn, "content-type")
      assert content_type =~ "application/json"
      assert %{"id" => 1, "result" => %{"content" => [%{"text" => "fast"}]}} = JSON.decode!(conn.resp_body)
    end

    @tag keepalive: true
    test "drops the request's later progress once the client disconnects", ctx do
      :ok = StreamableHTTP.register_sse_handler(ctx.transport, ctx.session_id)
      disconnected = :atomics.new(1, [])
      :atomics.put(disconnected, 1, 1)

      conn = sse_request(ctx.opts, ctx.session_id, %{"progressToken" => "tok"}, "slow", disconnected)
      request = Task.async(fn -> StreamableHTTPPlug.call(conn, ctx.opts) end)

      assert_receive {:tool_running, tool}
      tool_ref = Process.monitor(tool)
      Task.await(request)

      send(tool, :finish)
      assert_receive {:DOWN, ^tool_ref, :process, ^tool, :normal}, 1_000
      refute_receive {:sse_message, _progress}, 100
    end

    @tag keepalive: true
    test "lets the tool finish when the client disconnects, and keeps the session", ctx do
      disconnected = :atomics.new(1, [])
      :atomics.put(disconnected, 1, 1)

      conn = sse_request(ctx.opts, ctx.session_id, %{}, "slow", disconnected)
      request = Task.async(fn -> StreamableHTTPPlug.call(conn, ctx.opts) end)

      assert_receive {:tool_running, tool}
      tool_ref = Process.monitor(tool)
      Task.await(request)

      refute_receive {:DOWN, ^tool_ref, :process, ^tool, _reason}, 100
      send(tool, :finish)
      assert_receive {:DOWN, ^tool_ref, :process, ^tool, :normal}, 1_000

      conn = ctx.opts |> post(tool_call("fast"), "application/json", ctx.session_id) |> call(ctx.opts)
      assert %{"result" => %{"content" => [%{"text" => "fast"}]}} = JSON.decode!(conn.resp_body)
    end

    test "answers with an error and cancels the tool on the transport's close signal", ctx do
      request = Task.async(fn -> ctx.opts |> post(tool_call("slow"), @accept_both, ctx.session_id) |> call(ctx.opts) end)

      assert_receive {:tool_running, tool}
      tool_ref = Process.monitor(tool)
      send(request.pid, :close_sse)
      conn = Task.await(request)

      assert_receive {:DOWN, ^tool_ref, :process, ^tool, _reason}, 1_000
      assert %{"id" => 1, "error" => _error} = JSON.decode!(conn.resp_body)
      assert conn.resp_body =~ "Server shutting down"
    end

    test "answers with an error and exits with the reason of a trapped shutdown", ctx do
      conn = post(ctx.opts, tool_call("slow"), @accept_both, ctx.session_id)

      {pid, ref} =
        spawn_monitor(fn ->
          Process.flag(:trap_exit, true)
          call(conn, ctx.opts)
        end)

      assert_receive {:tool_running, tool}
      tool_ref = Process.monitor(tool)
      Process.exit(pid, :shutdown)

      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, 1_000
      assert_receive {:DOWN, ^tool_ref, :process, ^tool, _reason}, 1_000
      assert {_status, _headers, body} = sent_resp(conn)
      assert %{"id" => 1, "error" => %{"data" => %{"message" => "Server shutting down"}}} = JSON.decode!(body)
    end
  end

  describe "a POST that accepts only JSON on a session" do
    test "leaves what the handler sends to the GET stream", ctx do
      :ok = StreamableHTTP.register_sse_handler(ctx.transport, ctx.session_id)
      conn = ctx.opts |> post(tool_call("ask"), "application/json", ctx.session_id) |> call(ctx.opts)

      assert %{"result" => %{"content" => [%{"text" => "asked"}]}} = JSON.decode!(conn.resp_body)
      assert_receive {:sse_message, elicitation}
      assert %{"method" => "elicitation/create"} = JSON.decode!(elicitation)
      assert_receive {:sse_message, log}
      assert %{"method" => "notifications/message"} = JSON.decode!(log)
      assert_receive {:sse_message, progress}
      assert %{"method" => "notifications/progress"} = JSON.decode!(progress)
    end

    test "keeps its progress on the GET stream", ctx do
      :ok = StreamableHTTP.register_sse_handler(ctx.transport, ctx.session_id)

      request =
        Task.async(fn ->
          ctx.opts
          |> post(tool_call("slow", %{"progressToken" => "tok"}), "application/json", ctx.session_id)
          |> call(ctx.opts)
        end)

      assert_receive {:tool_running, tool}
      assert_receive {:sse_message, progress}
      assert %{"params" => %{"progressToken" => "tok", "progress" => 0}} = JSON.decode!(progress)
      send(tool, :finish)
      conn = Task.await(request)

      assert %{"result" => %{"content" => [%{"text" => "finished"}]}} = JSON.decode!(conn.resp_body)
    end
  end

  describe "request stream registration" do
    @tag keepalive: true
    test "replies with the keepalive interval and leaves keepalives to the stream", %{transport: transport} do
      assert {:ok, 20} = StreamableHTTP.register_request_stream(transport, "s", nil, nil)

      refute_receive :sse_keepalive, 100
      assert StreamableHTTP.handler_count(transport) == 0
    end

    test "replies :infinity with keepalives disabled", %{transport: transport} do
      assert {:ok, :infinity} = StreamableHTTP.register_request_stream(transport, "s", nil, nil)
    end

    test "receives the messages related to its request id", %{transport: transport} do
      {:ok, _interval} = StreamableHTTP.register_request_stream(transport, "s", 7, nil)
      message = JSON.encode!(%{"jsonrpc" => "2.0", "method" => "notifications/message", "params" => %{}})

      :ok = StreamableHTTP.send_message(transport, message, session_id: "s", related_request_id: 7)
      assert_receive {:sse_message, ^message}

      assert {:error, :no_sse_handler} =
               StreamableHTTP.send_message(transport, message, session_id: "s", related_request_id: 8)
    end

    test "receives the transport's close signal on shutdown", %{transport: transport} do
      {:ok, _interval} = StreamableHTTP.register_request_stream(transport, "s", nil, "tok")
      StreamableHTTP.shutdown(transport)

      assert_receive :close_sse
    end

    test "is dropped when its process exits", %{transport: transport} do
      :ok = StreamableHTTP.register_sse_handler(transport, "s")

      stream =
        spawn(fn ->
          {:ok, _interval} = StreamableHTTP.register_request_stream(transport, "s", nil, "tok")
          send(LegacyResponseStreamTest, :registered)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :registered
      ref = Process.monitor(stream)
      send(stream, :stop)
      assert_receive {:DOWN, ^ref, :process, ^stream, _reason}

      message = progress("tok")
      :ok = send_progress(transport, "s", message)
      assert_receive {:sse_message, ^message}
    end

    test "a later registration of a token takes it over", %{transport: transport} do
      stream =
        spawn(fn ->
          {:ok, _interval} = StreamableHTTP.register_request_stream(transport, "s", nil, "tok")
          send(LegacyResponseStreamTest, :registered)
          Process.sleep(:infinity)
        end)

      assert_receive :registered
      {:ok, _interval} = StreamableHTTP.register_request_stream(transport, "s", nil, "tok")
      Process.exit(stream, :kill)

      message = progress("tok")
      :ok = send_progress(transport, "s", message)
      assert_receive {:sse_message, ^message}
    end
  end

  defp initialize(opts) do
    body =
      JSON.encode!(%{
        "jsonrpc" => "2.0",
        "id" => 0,
        "method" => "initialize",
        "params" => %{
          "protocolVersion" => @version,
          "capabilities" => %{"elicitation" => %{}},
          "clientInfo" => %{"name" => "Legacy", "version" => "1.0.0"}
        }
      })

    conn =
      :post
      |> conn("/", body)
      |> put_req_header("content-type", "application/json")
      |> put_req_header("accept", "application/json")
      |> call(opts)

    assert conn.status == 200
    [session_id] = get_resp_header(conn, "mcp-session-id")

    initialized = JSON.encode!(%{"jsonrpc" => "2.0", "method" => "notifications/initialized"})
    assert opts |> post(initialized, "application/json", session_id) |> call(opts) |> Map.get(:status) == 202
    session_id
  end

  defp tool_call(name, meta \\ %{}) do
    JSON.encode!(%{
      "jsonrpc" => "2.0",
      "id" => 1,
      "method" => "tools/call",
      "params" => %{"name" => name, "arguments" => %{}, "_meta" => meta}
    })
  end

  defp send_progress(transport, session_id, message) do
    %{"params" => %{"progressToken" => token}} = JSON.decode!(message)
    StreamableHTTP.send_message(transport, message, session_id: session_id, progress_token: token)
  end

  defp progress(token) do
    JSON.encode!(%{
      "jsonrpc" => "2.0",
      "method" => "notifications/progress",
      "params" => %{"progressToken" => token, "progress" => 50}
    })
  end

  defp post(_opts, body, accept, session_id) do
    :post
    |> conn("/", body)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("accept", accept)
    |> put_req_header("mcp-protocol-version", @version)
    |> put_req_header("mcp-session-id", session_id)
  end

  # Built in the test process, so the adapter reports chunks here as they are written.
  defp sse_request(opts, session_id, meta, tool, disconnected \\ :atomics.new(1, [])) do
    opts
    |> post(tool_call(tool, meta), @accept_both, session_id)
    |> SSEAdapter.wrap(disconnected: disconnected)
  end

  defp call(conn, opts), do: StreamableHTTPPlug.call(conn, opts)

  defp events(%Plug.Conn{adapter: {_adapter, %{chunks: chunks}}}) do
    for "data: " <> data <- String.split(chunks || "", "\n"), do: JSON.decode!(data)
  end
end
