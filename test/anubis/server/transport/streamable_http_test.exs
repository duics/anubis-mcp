defmodule Anubis.Server.Transport.StreamableHTTPTest do
  use Anubis.MCP.Case, async: false

  import ExUnit.CaptureLog

  alias Anubis.Server.Registry
  alias Anubis.Server.Transport.StreamableHTTP

  @moduletag capture_log: true

  describe "start_link/1" do
    test "starts with valid options" do
      server = :"test_server_#{System.unique_integer([:positive])}"
      name = Registry.transport_name(server, :streamable_http)
      sup = Registry.task_supervisor_name(server)

      assert {:ok, pid} =
               StreamableHTTP.start_link(server: server, name: name, task_supervisor: sup)

      assert Process.alive?(pid)
    end

    test "requires server option" do
      assert_raise Peri.InvalidSchema, fn ->
        StreamableHTTP.start_link(name: :test)
      end
    end
  end

  describe "with running transport" do
    setup do
      name = Registry.transport_name(StubServer, :streamable_http)
      sup = Registry.task_supervisor_name(StubServer)
      start_supervised!({Task.Supervisor, name: sup})

      {:ok, transport} =
        start_supervised({StreamableHTTP, server: StubServer, name: name, task_supervisor: sup})

      %{transport: transport, server: StubServer}
    end

    test "registers and unregisters SSE handlers", %{transport: transport} do
      session_id = "test-session-123"
      handler_pid = self()

      assert :ok = StreamableHTTP.register_sse_handler(transport, session_id)
      assert ^handler_pid = StreamableHTTP.get_sse_handler(transport, session_id)
      assert :ok = StreamableHTTP.unregister_sse_handler(transport, session_id)
      refute StreamableHTTP.get_sse_handler(transport, session_id)
    end

    test "stale unregister cannot remove a newer handler", %{transport: transport} do
      session_id = "test-session-race"
      test_pid = self()

      old_handler =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, session_id)
          send(test_pid, {:registered, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:registered, ^old_handler}

      new_handler =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, session_id)
          send(test_pid, {:registered, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:registered, ^new_handler}
      assert ^new_handler = StreamableHTTP.get_sse_handler(transport, session_id)

      # Simulate delayed close from old SSE connection.
      assert :ok = StreamableHTTP.unregister_sse_handler(transport, session_id, old_handler)
      assert ^new_handler = StreamableHTTP.get_sse_handler(transport, session_id)

      assert :ok = StreamableHTTP.unregister_sse_handler(transport, session_id, new_handler)
      refute StreamableHTTP.get_sse_handler(transport, session_id)

      send(old_handler, :stop)
      send(new_handler, :stop)
    end

    test "routes messages to sessions", %{transport: transport} do
      session_id = "test-session-789"

      assert :ok = StreamableHTTP.register_sse_handler(transport, session_id)

      message = "test message"
      assert :ok = StreamableHTTP.route_to_session(transport, session_id, message)

      assert_receive {:sse_message, ^message}

      capture_log(fn ->
        StreamableHTTP.unregister_sse_handler(transport, session_id)
        Process.sleep(10)
      end)
    end

    test "cleans up handlers when they crash", %{transport: transport} do
      session_id = "test-session-crash"
      test_pid = self()

      capture_log(fn ->
        handler_pid =
          spawn(fn ->
            StreamableHTTP.register_sse_handler(transport, session_id)
            send(test_pid, :registered)

            receive do
              :crash -> exit(:boom)
            end
          end)

        assert_receive :registered, 1000

        handler = StreamableHTTP.get_sse_handler(transport, session_id)
        assert is_pid(handler)

        send(handler_pid, :crash)
        Process.sleep(100)

        refute StreamableHTTP.get_sse_handler(transport, session_id)
      end)
    end

    test "send_message/3 routes per-session when session has a GET stream", %{transport: transport} do
      session_id = "test-session-send"
      :ok = StreamableHTTP.register_sse_handler(transport, session_id)

      message = "test message"

      assert :ok =
               StreamableHTTP.send_message(transport, message,
                 timeout: 5000,
                 session_id: session_id
               )

      assert_receive {:sse_message, ^message}

      capture_log(fn ->
        StreamableHTTP.unregister_sse_handler(transport, session_id)
        Process.sleep(10)
      end)
    end

    test "send_message/3 returns :no_get_stream when session has no GET stream", %{transport: transport} do
      assert {:error, :no_get_stream} =
               StreamableHTTP.send_message(transport, "msg", session_id: "no-stream-session")
    end

    test "send_message/3 errors without session_id (no broadcast)", %{transport: transport} do
      capture_log(fn ->
        assert {:error, :missing_session_id} =
                 StreamableHTTP.send_message(transport, "msg", timeout: 5000)
      end)
    end

    test "send_unsolicited/3 routes notification to that session only — no cross-session leak",
         %{transport: transport} do
      session_a = "session-a"
      session_b = "session-b"

      target = self()

      handler_b =
        spawn(fn ->
          StreamableHTTP.register_sse_handler(transport, session_b)
          send(target, :b_registered)

          receive do
            {:sse_message, msg} -> send(target, {:b_received, msg})
            :stop -> :ok
          after
            500 -> send(target, :b_timeout)
          end
        end)

      assert_receive :b_registered, 500

      :ok = StreamableHTTP.register_sse_handler(transport, session_a)
      notification = ~s({"jsonrpc":"2.0","method":"notifications/progress","params":{}})
      assert :ok = StreamableHTTP.send_unsolicited(transport, session_a, notification)

      assert_receive {:sse_message, ^notification}
      refute_receive {:b_received, ^notification}, 100

      send(handler_b, :stop)

      capture_log(fn ->
        StreamableHTTP.unregister_sse_handler(transport, session_a)
        Process.sleep(10)
      end)
    end

    test "send_unsolicited/3 raises when given a JSON-RPC response envelope",
         %{transport: transport} do
      :ok = StreamableHTTP.register_sse_handler(transport, "guard-session")
      response = ~s({"jsonrpc":"2.0","result":{},"id":1})

      assert_raise ArgumentError, ~r/only accepts JSON-RPC notification envelopes/, fn ->
        StreamableHTTP.send_unsolicited(transport, "guard-session", response)
      end

      capture_log(fn ->
        StreamableHTTP.unregister_sse_handler(transport, "guard-session")
        Process.sleep(10)
      end)
    end

    test "send_unsolicited/3 raises when given a JSON-RPC request envelope",
         %{transport: transport} do
      :ok = StreamableHTTP.register_sse_handler(transport, "guard-session-2")
      request = ~s({"jsonrpc":"2.0","method":"sampling/createMessage","params":{},"id":7})

      assert_raise ArgumentError, ~r/only accepts JSON-RPC notification envelopes/, fn ->
        StreamableHTTP.send_unsolicited(transport, "guard-session-2", request)
      end

      capture_log(fn ->
        StreamableHTTP.unregister_sse_handler(transport, "guard-session-2")
        Process.sleep(10)
      end)
    end

    test "send_unsolicited/3 returns :no_get_stream for unknown sessions",
         %{transport: transport} do
      notification = ~s({"jsonrpc":"2.0","method":"notifications/progress","params":{}})

      assert {:error, :no_get_stream} =
               StreamableHTTP.send_unsolicited(transport, "unknown-session", notification)
    end

    test "shutdown/1 gracefully shuts down", %{transport: transport} do
      assert Process.alive?(transport)
      assert :ok = StreamableHTTP.shutdown(transport)
      Process.sleep(100)
      refute Process.alive?(transport)
    end
  end

  describe "supported_protocol_versions/0" do
    test "returns supported versions" do
      versions = StreamableHTTP.supported_protocol_versions()
      assert is_list(versions)
      assert "2025-03-26" in versions
    end
  end
end
