defmodule Anubis.Server.ServerCapabilitiesTest do
  use Anubis.MCP.Case, async: false

  alias Anubis.MCP.Message
  alias Anubis.Protocol.Registry, as: ProtocolRegistry
  alias Anubis.Protocol.V2025_11_25
  alias Anubis.Protocol.V2026_07_28
  alias Anubis.Server.Context
  alias Anubis.Server.Frame
  alias Anubis.Server.Handlers
  alias Anubis.Server.Handlers.Resources
  alias Anubis.Server.Handlers.Subscriptions
  alias Anubis.Server.Registry
  alias Anubis.Server.Session
  alias Anubis.Server.Stateless
  alias Anubis.Server.TaskStore.Local, as: TaskStoreLocal

  @moduletag capture_log: true

  @ui "io.modelcontextprotocol/ui"

  defmodule PlainServer do
    @moduledoc false

    use Anubis.Server,
      name: "plain-server",
      version: "1.0.0",
      capabilities: [:tools, :logging, {:resources, subscribe?: true}],
      instructions: "Static instructions",
      protocol_versions: ["2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26"]
  end

  defmodule PerCallerServer do
    @moduledoc false

    use Anubis.Server,
      name: "per-caller-server",
      version: "1.0.0",
      capabilities: [:tools],
      protocol_versions: ["2026-07-28", "2025-11-25"],
      capability_passthrough: [stateless: ["events"], legacy: ["extensions"]]

    @impl Anubis.Server
    def server_instructions(frame), do: "Instructions for #{frame.assigns[:audience] || "anyone"}"

    @impl Anubis.Server
    def server_capabilities(frame) do
      server_capabilities()
      |> then(&if frame.assigns[:agent_reports], do: Map.put(&1, "events", %{}), else: &1)
      |> then(
        &if Frame.client_supports_extension?(frame, "io.modelcontextprotocol/ui"),
          do: Map.put(&1, "extensions", %{"io.modelcontextprotocol/ui" => %{}}),
          else: &1
      )
    end
  end

  defmodule NoPassthroughServer do
    @moduledoc false

    use Anubis.Server,
      name: "no-passthrough-server",
      version: "1.0.0",
      capabilities: [:tools],
      protocol_versions: ["2026-07-28", "2025-11-25"]

    @impl Anubis.Server
    def server_capabilities(_frame), do: Map.put(server_capabilities(), "events", %{})
  end

  defmodule ListedTasksServer do
    @moduledoc false

    use Anubis.Server,
      name: "listed-tasks-server",
      version: "1.0.0",
      capabilities: [:tools, {:tasks, cancel?: true, requests: [tools: [:call]]}]

    @impl Anubis.Server
    def server_capabilities(frame) do
      if frame.assigns[:listed], do: server_capabilities(), else: Map.delete(server_capabilities(), "tasks")
    end

    component(TasksStubServer.MustBeTask, name: "must_be_task")
  end

  defmodule PerCallerSubscribeServer do
    @moduledoc false

    use Anubis.Server,
      name: "per-caller-subscribe-server",
      version: "1.0.0",
      capabilities: [{:resources, subscribe?: true}],
      protocol_versions: ["2026-07-28", "2025-11-25"]

    @impl Anubis.Server
    def server_capabilities(frame) do
      if frame.assigns[:subscriber], do: server_capabilities(), else: %{"resources" => %{}}
    end
  end

  defmodule LiveToolsServer do
    @moduledoc false

    use Anubis.Server,
      name: "live-tools-server",
      version: "1.0.0",
      capabilities: [:tools],
      protocol_versions: ["2026-07-28"]

    @impl Anubis.Server
    def server_capabilities(frame) do
      if frame.assigns[:live], do: %{"tools" => %{listChanged: true}}, else: server_capabilities()
    end
  end

  describe "defaults" do
    test "initialize answers exactly what upstream answers, for every legacy version" do
      for version <- ProtocolRegistry.legacy_versions() do
        session = start_session(PlainServer)
        {:ok, protocol_module} = ProtocolRegistry.get(version)

        request = init_request(version, %{"name" => "TestClient", "version" => "1.0.0"})
        {:ok, response} = GenServer.call(session, {:mcp_request, request, %{}})

        upstream = %{
          "protocolVersion" => version,
          "serverInfo" => PlainServer.server_info(),
          "capabilities" => protocol_module.server_capabilities(PlainServer.server_capabilities()),
          "instructions" => "Static instructions"
        }

        assert response == JSON.encode!(Message.build_response(upstream, request["id"]))
      end
    end

    test "server/discover answers exactly what upstream answers, with or without a frame" do
      upstream = %{
        "supportedVersions" => ["2026-07-28"],
        "capabilities" => V2026_07_28.server_capabilities(PlainServer.server_capabilities()),
        "instructions" => "Static instructions",
        "ttlMs" => 0,
        "cacheScope" => "private"
      }

      assert Stateless.discover_result(PlainServer, V2026_07_28) == upstream
      assert Stateless.discover_result(PlainServer, V2026_07_28, frame(%{user: "x"})) == upstream
    end

    test "a key the version does not model is dropped unless passed through" do
      capabilities = Stateless.discover_result(NoPassthroughServer, V2026_07_28, frame())["capabilities"]

      refute Map.has_key?(capabilities, "events")
      assert Map.has_key?(capabilities, "tools")
    end
  end

  describe "server/discover per caller" do
    test "carries the same instructions initialize gives the same assigns" do
      session = start_session(PerCallerServer)
      initialize_result = initialize(session, "2025-11-25", %{assigns: %{audience: "support"}})

      discover = Stateless.discover_result(PerCallerServer, V2026_07_28, frame(%{audience: "support"}))

      assert discover["instructions"] == "Instructions for support"
      assert discover["instructions"] == initialize_result["instructions"]
    end

    test "passes events through only for a caller whose capabilities declare it" do
      offered = Stateless.discover_result(PerCallerServer, V2026_07_28, frame(%{agent_reports: true}))
      assert offered["capabilities"]["events"] == %{}

      refute Map.has_key?(Stateless.discover_result(PerCallerServer, V2026_07_28, frame())["capabilities"], "events")
    end

    test "handles server/discover with the request's frame" do
      discover_frame = %{frame(%{agent_reports: true, audience: "ops"}) | context: %Context{protocol_module: V2026_07_28}}

      assert {:reply, result, _frame} =
               Handlers.handle(%{"method" => "server/discover"}, PerCallerServer, discover_frame)

      assert result["instructions"] == "Instructions for ops"
      assert result["capabilities"]["events"] == %{}
    end
  end

  describe "initialize per caller" do
    test "passes extensions through on 2025-11-25 when the server offers them" do
      session = start_session(PerCallerServer)
      result = initialize(session, "2025-11-25", %{}, %{"extensions" => %{@ui => %{}}})

      assert result["capabilities"]["extensions"] == %{@ui => %{}}
    end

    test "offers no extension to a client that did not declare it" do
      session = start_session(PerCallerServer)

      refute Map.has_key?(initialize(session, "2025-11-25")["capabilities"], "extensions")
    end

    test "keeps a stateless-only passthrough key out of the legacy handshake" do
      session = start_session(PerCallerServer)

      refute Map.has_key?(initialize(session, "2025-11-25", %{assigns: %{agent_reports: true}})["capabilities"], "events")
    end
  end

  describe "tasks declared per session" do
    test "a listed session offers tasks and accepts a task-augmented call" do
      session = start_session(ListedTasksServer, task_store: true)

      assert %{"tasks" => _} = initialize(session, "2025-11-25", %{assigns: %{listed: true}})["capabilities"]
      assert %{"result" => %{"task" => %{"taskId" => _}}} = task_call(session)
    end

    test "an unlisted session offers no tasks and refuses a task-augmented call" do
      session = start_session(ListedTasksServer, task_store: true)

      refute Map.has_key?(initialize(session, "2025-11-25")["capabilities"], "tasks")
      assert %{"error" => %{"code" => -32_601}} = task_call(session)
    end
  end

  describe "a session restored from a store" do
    test "resolves its capabilities from the first request's assigns" do
      session = start_session(ListedTasksServer, task_store: true, pre_initialized: true)

      assert %{"result" => %{"task" => %{"taskId" => _}}} = task_call(session, %{assigns: %{listed: true}})
    end

    test "keeps a capability the first request's caller is not offered out" do
      session = start_session(ListedTasksServer, task_store: true, pre_initialized: true)

      assert %{"error" => %{"code" => -32_601}} = task_call(session)
    end
  end

  describe "resources/subscribe" do
    test "follows the caller's resources.subscribe" do
      request = %{"params" => %{"uri" => "file:///a"}}

      assert {:reply, %{}, _} =
               Resources.handle_subscribe(request, frame(%{subscriber: true}), PerCallerSubscribeServer)

      assert {:error, %{code: -32_601}, _} = Resources.handle_subscribe(request, frame(), PerCallerSubscribeServer)
      assert {:error, %{code: -32_601}, _} = Resources.handle_unsubscribe(request, frame(), PerCallerSubscribeServer)
    end

    test "decides listen's resourceSubscriptions for the caller" do
      request = %{"params" => %{"notifications" => %{"resourceSubscriptions" => ["file:///a"]}}}

      assert {:reply, %{"notifications" => %{"resourceSubscriptions" => ["file:///a"]}}, _} =
               Subscriptions.handle_listen(request, frame(%{subscriber: true}), PerCallerSubscribeServer)

      assert {:reply, %{"notifications" => honored}, _} =
               Subscriptions.handle_listen(request, frame(), PerCallerSubscribeServer)

      assert honored == %{}
    end

    test "stays on for a server without server_capabilities/1" do
      request = %{"params" => %{"uri" => "file:///a"}}

      assert {:reply, %{}, _} = Resources.handle_subscribe(request, frame(), PlainServer)
    end
  end

  describe "subscriptions/listen" do
    test "honours the list-changed flags of the caller's capabilities" do
      request = %{"params" => %{"notifications" => %{"toolsListChanged" => true}}}

      assert {:reply, %{"notifications" => %{"toolsListChanged" => true}}, _} =
               Subscriptions.handle_listen(request, frame(%{live: true}), LiveToolsServer)

      assert {:reply, %{"notifications" => honored}, _} = Subscriptions.handle_listen(request, frame(), LiveToolsServer)
      assert honored == %{}
    end
  end

  describe "Frame.client_supports_extension?/2" do
    test "reads the extensions the client declared" do
      declared = %{frame() | context: %Context{client_capabilities: %{"extensions" => %{@ui => %{}}}}}

      assert Frame.client_supports_extension?(declared, @ui)
      refute Frame.client_supports_extension?(declared, "io.modelcontextprotocol/events")
      refute Frame.client_supports_extension?(frame(), @ui)
    end
  end

  test "capability_passthrough accepts a flat list for both eras" do
    assert Anubis.Server.normalize_capability_passthrough(PlainServer, ["events", :extensions]) ==
             %{legacy: ["events", "extensions"], stateless: ["events", "extensions"]}

    assert Anubis.Server.capability_passthrough(PlainServer) == %{legacy: [], stateless: []}

    assert_raise ArgumentError, ~r/unknown eras/, fn ->
      Anubis.Server.normalize_capability_passthrough(PlainServer, future: ["events"])
    end
  end

  test "the 2025-11-25 dialect alone still drops extensions" do
    refute Map.has_key?(V2025_11_25.server_capabilities(%{"extensions" => %{}}), "extensions")
  end

  # Helpers

  defp frame(assigns \\ %{}), do: Frame.new(assigns)

  defp start_session(server_module, opts \\ []) do
    session_id = "caps-#{System.unique_integer([:positive])}"
    transport_name = Registry.transport_name(server_module, StubTransport)
    task_sup = Registry.task_supervisor_name(server_module)

    if is_nil(Process.whereis(transport_name)) do
      start_supervised!({StubTransport, name: transport_name}, id: transport_name)
      start_supervised!({Task.Supervisor, name: task_sup}, id: task_sup)
    end

    session_name = Registry.session_name(server_module, session_id)

    session_opts = [
      session_id: session_id,
      server_module: server_module,
      name: session_name,
      transport: [layer: StubTransport, name: transport_name],
      task_supervisor: task_sup,
      pre_initialized: Keyword.get(opts, :pre_initialized, false)
    ]

    session_opts =
      if opts[:task_store] do
        task_store_name = Registry.task_store_name(server_module)
        start_supervised!({TaskStoreLocal, name: task_store_name})
        Keyword.put(session_opts, :task_store, adapter: TaskStoreLocal, name: task_store_name)
      else
        session_opts
      end

    start_supervised!({Session, session_opts}, id: session_name)
  end

  defp initialize(session, version, transport_context \\ %{}, client_capabilities \\ %{}) do
    request = init_request(version, %{"name" => "TestClient", "version" => "1.0.0"}, client_capabilities)
    {:ok, response} = GenServer.call(session, {:mcp_request, request, transport_context})
    :ok = GenServer.cast(session, {:mcp_notification, build_notification("notifications/initialized", %{}), %{}})

    JSON.decode!(response)["result"]
  end

  defp task_call(session, transport_context \\ %{}) do
    request = build_request("tools/call", %{"name" => "must_be_task", "arguments" => %{"msg" => "hi"}, "task" => %{}}, 7)
    {:ok, response} = GenServer.call(session, {:mcp_request, request, transport_context})
    JSON.decode!(response)
  end
end
