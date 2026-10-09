defmodule Anubis.Server.ExtensionMethodsTest do
  use Anubis.MCP.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Anubis.MCP.Message
  alias Anubis.Protocol.V2025_11_25
  alias Anubis.Protocol.V2026_07_28
  alias Anubis.Server.Registry
  alias Anubis.Server.Session
  alias Anubis.Server.Supervisor, as: ServerSupervisor
  alias Anubis.Server.Transport.StreamableHTTP
  alias Anubis.Server.Transport.StreamableHTTP.Plug, as: StreamableHTTPPlug

  @moduletag capture_log: true

  @version "2026-07-28"

  defmodule EventsServer do
    @moduledoc false

    use Anubis.Server,
      name: "events-server",
      version: "1.0.0",
      capabilities: [:tools],
      protocol_versions: ["2026-07-28", "2025-11-25"],
      extension_methods: %{
        "events/list" => [eras: [:stateless], params: %{"cursor" => :string}],
        "events/subscribe" => [eras: [:stateless]],
        "events/unsubscribe" => [eras: [:stateless]],
        "acme/ping" => [eras: [:legacy]]
      }

    @impl true
    def handle_request(%{"method" => method} = request, frame)
        when method in ~w(events/list events/subscribe acme/ping) do
      {:reply,
       %{
         "method" => method,
         "params" => Map.delete(request["params"] || %{}, "_meta"),
         "protocolVersion" => frame.context.protocol_version,
         "protocolModule" => inspect(frame.context.protocol_module),
         "user" => frame.assigns[:user],
         "clientCapabilities" => frame.context.client_capabilities
       }, frame}
    end
  end

  describe "stateless requests over Streamable HTTP" do
    setup :start_http_server

    test "a declared method reaches handle_request/2 with the request's frame", %{opts: opts} do
      capabilities = %{"extensions" => %{"io.modelcontextprotocol/events" => %{}}}

      conn =
        post_stateless(opts, "events/list", %{"cursor" => "c1"},
          assigns: %{user: "sara"},
          client_capabilities: capabilities
        )

      assert conn.status == 200

      assert %{
               "method" => "events/list",
               "params" => %{"cursor" => "c1"},
               "protocolVersion" => @version,
               "protocolModule" => "Anubis.Protocol.V2026_07_28",
               "user" => "sara",
               "clientCapabilities" => ^capabilities,
               "resultType" => "complete"
             } = JSON.decode!(conn.resp_body)["result"]
    end

    test "a method declared with open params keeps every key", %{opts: opts} do
      params = %{"subscriptionId" => "s1", "delivery" => %{"url" => "https://example.test/hook"}}
      conn = post_stateless(opts, "events/subscribe", params)

      assert conn.status == 200
      assert JSON.decode!(conn.resp_body)["result"]["params"] == params
    end

    test "an undeclared method stays a 404 with -32601", %{opts: opts} do
      conn = post_stateless(opts, "events/poll", %{})

      assert conn.status == 404
      assert JSON.decode!(conn.resp_body)["error"]["code"] == -32_601
    end

    test "a declared method with invalid params is -32602", %{opts: opts} do
      conn = post_stateless(opts, "events/list", %{"cursor" => 42})

      assert conn.status == 400
      assert %{"code" => -32_602, "message" => "Invalid params"} = JSON.decode!(conn.resp_body)["error"]
    end

    test "a declared method the server leaves unhandled answers -32601", %{opts: opts} do
      conn = post_stateless(opts, "events/unsubscribe", %{})

      assert conn.status == 404
      assert JSON.decode!(conn.resp_body)["error"]["code"] == -32_601
    end

    test "a method declared for the legacy era only is -32601 here", %{opts: opts} do
      conn = post_stateless(opts, "acme/ping", %{})

      assert conn.status == 404
      assert JSON.decode!(conn.resp_body)["error"]["code"] == -32_601
    end
  end

  describe "legacy sessions" do
    test "a stateless-only method is -32601" do
      session = start_initialized_session(EventsServer)

      decoded = call_session(session, build_request("events/list", %{}, "req-1"))

      assert decoded["error"]["code"] == -32_601
    end

    test "a method declared for the legacy era reaches handle_request/2" do
      session = start_initialized_session(EventsServer)

      decoded = call_session(session, build_request("acme/ping", %{"n" => 1}, "req-1"), %{assigns: %{user: "kim"}})

      assert %{"method" => "acme/ping", "params" => %{"n" => 1}, "protocolVersion" => "2025-11-25", "user" => "kim"} =
               decoded["result"]
    end
  end

  describe "Message.validate_message/3" do
    @extensions %{
      "events/list" => %{eras: [:stateless], params: %{"cursor" => :string}},
      "acme/ping" => %{eras: [:legacy], params: :map}
    }

    test "admits a declared method for its era only" do
      stateless = stateless_request("events/list", %{"cursor" => "c1"})

      assert {:ok, %{"params" => %{"cursor" => "c1"}}} = Message.validate_message(stateless, V2026_07_28, @extensions)
      assert {:error, :method_not_found} = Message.validate_message(stateless, V2026_07_28)

      legacy = build_request("acme/ping", %{"anything" => true}, 1)
      assert {:ok, ^legacy} = Message.validate_message(legacy, V2025_11_25, @extensions)
      assert {:error, :method_not_found} = Message.validate_message(legacy, V2026_07_28, @extensions)
    end

    test "picks the version from the message when given nil" do
      assert {:ok, _} = Message.validate_message(stateless_request("events/list", %{}), nil, @extensions)
    end

    test "classifies a schema failure of a declared method as invalid params" do
      request = stateless_request("events/list", %{"cursor" => 1})
      assert {:error, :invalid_params} = Message.validate_message(request, V2026_07_28, @extensions)

      no_meta = build_request("events/list", %{}, 1)
      assert {:error, :invalid_params} = Message.validate_message(no_meta, V2026_07_28, @extensions)
    end

    test "never replaces a version's own method" do
      extensions = Map.put(@extensions, "tools/list", %{eras: [:stateless], params: :map})
      request = stateless_request("tools/list", %{"cursor" => 1})

      assert Message.validate_message(request, V2026_07_28, extensions) ==
               Message.validate_message(request, V2026_07_28)
    end

    test "decode/3 admits declared methods" do
      line = JSON.encode!(build_request("acme/ping", %{}, 1))

      assert {:ok, [%{"method" => "acme/ping"}]} = Message.decode(line, nil, @extensions)
      assert {:error, :method_not_found} = Message.decode(line)
    end
  end

  describe "the extension_methods option" do
    test "compiles to __extension_methods__/0 with defaults" do
      assert EventsServer.__extension_methods__()["events/subscribe"] == %{eras: [:stateless], params: :map}
      assert Anubis.Server.extension_methods(EventsServer)["events/list"].params == %{"cursor" => :string}
    end

    test "is empty for a server that declares none" do
      assert Anubis.Server.extension_methods(StubServer) == %{}

      assert Anubis.Server.normalize_extension_methods(StubServer, %{"x/y" => []}) == %{
               "x/y" => %{eras: [:legacy, :stateless], params: :map}
             }
    end

    test "refuses a protocol method, an unknown era and a non-map schema" do
      assert_raise ArgumentError, ~r/protocol method/, fn ->
        Anubis.Server.normalize_extension_methods(StubServer, %{"tools/call" => []})
      end

      assert_raise ArgumentError, ~r/:eras/, fn ->
        Anubis.Server.normalize_extension_methods(StubServer, %{"x/y" => [eras: [:future]]})
      end

      assert_raise ArgumentError, ~r/:params/, fn ->
        Anubis.Server.normalize_extension_methods(StubServer, %{"x/y" => [params: :string]})
      end
    end

    test "allows a method another era defines" do
      assert %{"tasks/get" => %{eras: [:stateless]}} =
               Anubis.Server.normalize_extension_methods(StubServer, %{"tasks/get" => [eras: [:stateless]]})
    end
  end

  # Helpers

  defp start_http_server(_ctx) do
    server = EventsServer

    task_sup = Registry.task_supervisor_name(server)
    start_supervised!({Task.Supervisor, name: task_sup})

    stub_transport = Registry.transport_name(server, StubTransport)
    start_supervised!({StubTransport, name: stub_transport})

    registry_name = Registry.registry_name(server)
    start_supervised!({Registry.Local, name: registry_name})
    start_supervised!({Elixir.Registry, keys: :unique, name: Registry.naming_registry_name(registry_name)})

    session_sup = Registry.session_supervisor_name(server)
    start_supervised!({DynamicSupervisor, name: session_sup, strategy: :one_for_one})

    :persistent_term.put({ServerSupervisor, server, :session_config}, %{
      server_module: server,
      registry_mod: Registry.Local,
      transport: [layer: StubTransport, name: stub_transport],
      session_idle_timeout: nil,
      timeout: 30_000,
      task_supervisor: task_sup
    })

    on_exit(fn -> :persistent_term.erase({ServerSupervisor, server, :session_config}) end)

    http_transport = Registry.transport_name(server, :streamable_http)
    start_supervised!({StreamableHTTP, server: server, name: http_transport, task_supervisor: task_sup})

    %{opts: StreamableHTTPPlug.init(server: server)}
  end

  defp post_stateless(opts, method, params, post_opts \\ []) do
    client_capabilities = Keyword.get(post_opts, :client_capabilities, %{})
    meta = stateless_meta(client_capabilities)
    body = JSON.encode!(%{"jsonrpc" => "2.0", "id" => 1, "method" => method, "params" => Map.put(params, "_meta", meta)})

    :post
    |> conn("/", body)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("accept", "application/json, text/event-stream")
    |> put_req_header("mcp-protocol-version", @version)
    |> put_req_header("mcp-method", method)
    |> merge_assigns(post_opts |> Keyword.get(:assigns, %{}) |> Map.to_list())
    |> StreamableHTTPPlug.call(opts)
  end

  defp stateless_request(method, params) do
    build_request(method, Map.put(params, "_meta", stateless_meta(%{})), 1)
  end

  defp stateless_meta(client_capabilities) do
    %{
      "io.modelcontextprotocol/protocolVersion" => @version,
      "io.modelcontextprotocol/clientCapabilities" => client_capabilities
    }
  end

  defp start_initialized_session(server_module) do
    session_id = "ext-#{System.unique_integer([:positive])}"
    transport_name = Registry.transport_name(server_module, StubTransport)
    start_supervised!({StubTransport, name: transport_name}, id: transport_name)

    task_sup = Registry.task_supervisor_name(server_module)
    start_supervised!({Task.Supervisor, name: task_sup}, id: task_sup)

    session_name = Registry.session_name(server_module, session_id)

    session =
      start_supervised!(
        {Session,
         session_id: session_id,
         server_module: server_module,
         name: session_name,
         transport: [layer: StubTransport, name: transport_name],
         task_supervisor: task_sup},
        id: session_name
      )

    request = init_request("2025-11-25", %{"name" => "TestClient", "version" => "1.0.0"})
    {:ok, _} = GenServer.call(session, {:mcp_request, request, %{}})
    :ok = GenServer.cast(session, {:mcp_notification, build_notification("notifications/initialized", %{}), %{}})

    session
  end

  defp call_session(session, request, transport_context \\ %{}) do
    {:ok, raw} = GenServer.call(session, {:mcp_request, request, transport_context})
    JSON.decode!(raw)
  end
end
