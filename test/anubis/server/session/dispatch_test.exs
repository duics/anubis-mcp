defmodule Anubis.Server.SessionDispatchTest do
  use Anubis.MCP.Case, async: false

  alias Anubis.Server.Registry
  alias Anubis.Server.Session

  @moduletag capture_log: true

  defmodule DispatchTestServer do
    @moduledoc false
    use Anubis.Server,
      name: "DispatchTestServer",
      version: "1.0.0",
      capabilities: [:tools]

    @impl true
    def init(_client_info, frame), do: {:ok, frame}

    @impl true
    def handle_request(%{"id" => _id, "method" => "tools/call", "params" => params}, frame) do
      sleep_ms = get_in(params, ["arguments", "sleep_ms"]) || 0
      label = get_in(params, ["arguments", "label"]) || "ok"
      crash? = get_in(params, ["arguments", "crash"]) || false

      if sleep_ms > 0, do: Process.sleep(sleep_ms)
      if crash?, do: raise("crash for test")

      response = %{
        "content" => [%{"type" => "text", "text" => label}],
        "isError" => false
      }

      {:reply, response, frame}
    end

    def handle_request(%{"id" => _, "method" => "tools/list"}, frame) do
      {:reply, %{"tools" => []}, frame}
    end

    def handle_request(_, frame), do: {:reply, %{}, frame}
  end

  setup do
    transport_name = Registry.transport_name(DispatchTestServer, StubTransport)
    start_supervised!({StubTransport, name: transport_name})

    task_sup = Registry.task_supervisor_name(DispatchTestServer)
    start_supervised!({Task.Supervisor, name: task_sup})

    session_id = "dispatch-#{System.unique_integer([:positive])}"
    session_name = Registry.session_name(DispatchTestServer, session_id)

    session =
      start_supervised!(
        {Session,
         session_id: session_id,
         server_module: DispatchTestServer,
         name: session_name,
         transport: [layer: StubTransport, name: transport_name],
         task_supervisor: task_sup,
         max_concurrent_requests: 4}
      )

    request = init_request("2025-03-26", %{"name" => "Test", "version" => "1.0.0"})
    assert {:ok, _} = GenServer.call(session, {:mcp_request, request, %{}})
    notification = build_notification("notifications/initialized", %{})
    :ok = GenServer.cast(session, {:mcp_notification, notification, %{}})
    Process.sleep(20)

    %{session: session, session_id: session_id}
  end

  defp tool_call(name, args \\ %{}) do
    build_request("tools/call", %{"name" => name, "arguments" => args})
  end

  describe "task-path dispatch" do
    test "tools/call returns {:ok, :dispatched, ref} synchronously and {:request_done, ref, _} asynchronously",
         %{session: session} do
      {:ok, :dispatched, request_ref} =
        GenServer.call(session, {:mcp_request, tool_call("t", %{"label" => "hello"}), %{}})

      assert_receive {:request_done, ^request_ref, {:ok, response_binary}}, 1_000
      assert is_binary(response_binary)
    end

    test "in_flight_tasks tracks the dispatched request and clears on completion", %{session: session} do
      pre_state = :sys.get_state(session)
      assert pre_state.in_flight_tasks == %{}

      {:ok, :dispatched, _ref} =
        GenServer.call(session, {:mcp_request, tool_call("t", %{"sleep_ms" => 50, "label" => "ok"}), %{}})

      Process.sleep(10)
      mid_state = :sys.get_state(session)
      assert map_size(mid_state.in_flight_tasks) == 1

      Process.sleep(80)
      post_state = :sys.get_state(session)
      assert post_state.in_flight_tasks == %{}
    end

    test "two parallel tool calls run concurrently — total wall time ≈ max, not sum",
         %{session: session} do
      slow = tool_call("t", %{"sleep_ms" => 200, "label" => "slow"})
      fast = tool_call("t", %{"sleep_ms" => 5, "label" => "fast"})

      start = System.monotonic_time(:millisecond)

      {:ok, :dispatched, slow_ref} = GenServer.call(session, {:mcp_request, slow, %{}})
      {:ok, :dispatched, fast_ref} = GenServer.call(session, {:mcp_request, fast, %{}})

      assert_receive {:request_done, ^fast_ref, {:ok, _}}, 500
      assert_receive {:request_done, ^slow_ref, {:ok, _}}, 1_000

      elapsed = System.monotonic_time(:millisecond) - start

      assert elapsed < 350,
             "expected parallel completion within ~max(200, 5) ms; got #{elapsed} ms — tools may be serialized"
    end

    test "per-session cap rejects above-cap dispatches with :overloaded", %{session: session} do
      slow_args = %{"sleep_ms" => 200, "label" => "blocking"}

      refs =
        for _ <- 1..4 do
          {:ok, :dispatched, ref} =
            GenServer.call(session, {:mcp_request, tool_call("t", slow_args), %{}})

          ref
        end

      Process.sleep(20)

      assert {:error, :overloaded, _request_id} =
               GenServer.call(session, {:mcp_request, tool_call("t", slow_args), %{}})

      for ref <- refs do
        assert_receive {:request_done, ^ref, {:ok, _}}, 1_000
      end
    end

    test "task crash forwards :error envelope to plug", %{session: session} do
      assert {:ok, :dispatched, ref} =
               GenServer.call(session, {:mcp_request, tool_call("t", %{"crash" => true}), %{}}, 5_000)

      assert_receive {:request_done, ^ref, {:error, encoded}}, 2_000
      assert is_binary(encoded)
    end
  end

  defmodule NotifyingServer do
    @moduledoc false
    use Anubis.Server,
      name: "NotifyingServer",
      version: "1.0.0",
      capabilities: [:tools, :logging]

    @impl true
    def init(_, frame), do: {:ok, frame}

    @impl true
    def handle_request(%{"id" => _, "method" => "tools/call", "params" => params}, frame) do
      session_pid = Anubis.Server.session_pid()
      label = get_in(params, ["arguments", "label"]) || "ok"

      if session_pid do
        Anubis.Server.send_log_message(:info, "tool start", %{"session_pid" => inspect(session_pid)})
        Anubis.Server.send_progress("token-1", 0.5, total: 1)
      end

      response = %{
        "content" => [%{"type" => "text", "text" => label}],
        "isError" => false
      }

      {:reply, response, frame}
    end

    def handle_request(_, frame), do: {:reply, %{}, frame}
  end

  describe "send_* helpers route via :anubis_session_pid (R6 / U4)" do
    test "send_progress + send_log_message from a Task land in Session and reach transport" do
      transport_name = Registry.transport_name(NotifyingServer, StubTransport)
      start_supervised!(Supervisor.child_spec({StubTransport, name: transport_name}, id: :n_trans))

      task_sup = Registry.task_supervisor_name(NotifyingServer)
      start_supervised!(Supervisor.child_spec({Task.Supervisor, name: task_sup}, id: :n_task_sup))

      session_id = "notifying-#{System.unique_integer([:positive])}"
      session_name = Registry.session_name(NotifyingServer, session_id)

      session =
        start_supervised!(
          Supervisor.child_spec(
            {Session,
             session_id: session_id,
             server_module: NotifyingServer,
             name: session_name,
             transport: [layer: StubTransport, name: transport_name],
             task_supervisor: task_sup},
            id: :n_session
          )
        )

      :ok = StubTransport.set_test_pid(transport_name, self())

      init = init_request("2025-03-26", %{"name" => "T", "version" => "1.0.0"})
      assert {:ok, _} = GenServer.call(session, {:mcp_request, init, %{}})
      :ok = GenServer.cast(session, {:mcp_notification, build_notification("notifications/initialized", %{}), %{}})
      Process.sleep(20)
      :ok = StubTransport.clear(transport_name)

      assert {:ok, :dispatched, ref} =
               GenServer.call(session, {:mcp_request, tool_call("t", %{"label" => "hello"}), %{}})

      assert_receive {:request_done, ^ref, {:ok, _}}, 1_000

      Process.sleep(50)
      messages = StubTransport.get_messages(transport_name)
      methods = Enum.map(messages, & &1["method"])

      assert "notifications/log/message" in methods
      assert "notifications/progress" in methods
    end

    test "session_pid/0 returns nil outside a request task" do
      assert Anubis.Server.session_pid() == nil
    end
  end

  describe "synchronous-allowlist (R11)" do
    test "ping replies inline, no task spawned", %{session: session} do
      ping = build_request("ping", %{})

      assert {:ok, response} = GenServer.call(session, {:mcp_request, ping, %{}})
      assert is_binary(response)

      state = :sys.get_state(session)
      assert state.in_flight_tasks == %{}
    end
  end

  describe "cancellation (R7)" do
    test "notifications/cancelled terminates running task; plug receives typed cancel signal",
         %{session: session} do
      {:ok, :dispatched, request_ref} =
        GenServer.call(
          session,
          {:mcp_request, tool_call("t", %{"sleep_ms" => 5_000, "label" => "long"}), %{}}
        )

      Process.sleep(20)
      state = :sys.get_state(session)
      [{request_id, entry}] = Map.to_list(state.in_flight_tasks)
      assert Process.alive?(entry.task_pid)

      cancel = build_notification("notifications/cancelled", %{"requestId" => request_id})
      :ok = GenServer.cast(session, {:mcp_notification, cancel, %{}})

      assert_receive {:request_cancelled, ^request_ref}, 2_000

      1..40
      |> Enum.reduce_while(false, fn _, _ ->
        if Process.alive?(entry.task_pid) do
          Process.sleep(50)
          {:cont, false}
        else
          {:halt, true}
        end
      end)
      |> assert("task should be terminated within 2 s of cancellation")

      refute_receive {:request_done, ^request_ref, _}, 100

      post_state = :sys.get_state(session)
      assert post_state.in_flight_tasks == %{}
    end
  end
end
