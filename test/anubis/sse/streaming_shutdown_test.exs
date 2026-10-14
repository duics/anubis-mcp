defmodule Anubis.SSE.StreamingShutdownTest do
  use ExUnit.Case, async: true

  alias Anubis.SSE.Streaming

  @moduletag capture_log: true

  defmodule Connection do
    @moduledoc false
    use GenServer, restart: :temporary

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner) do
      Process.flag(:trap_exit, true)
      {:ok, owner, {:continue, :stream}}
    end

    @impl true
    def handle_continue(:stream, owner) do
      conn = Streaming.prepare_connection(Plug.Test.conn(:get, "/mcp"))
      send(owner, {:streaming, self()})

      conn =
        Streaming.start(conn, nil, "shutdown-test", on_close: fn -> send(owner, {:closed, self()}) end)

      send(owner, {:returned, self(), conn.halted})
      {:stop, :normal, owner}
    end
  end

  test "supervisor shutdown closes the stream and runs cleanup before force-kill" do
    pid = start_supervised!({Connection, self()}, shutdown: 250)
    ref = Process.monitor(pid)
    assert_receive {:streaming, ^pid}, 1_000

    stop_supervised!(Connection)

    assert_receive {:closed, ^pid}
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    refute_received {:returned, ^pid, _}
  end

  test "a shutdown exit with a reason closes the stream and runs cleanup" do
    pid = start_supervised!({Connection, self()}, shutdown: 250)
    ref = Process.monitor(pid)
    assert_receive {:streaming, ^pid}, 1_000

    Process.exit(pid, {:shutdown, :draining})

    assert_receive {:closed, ^pid}
    assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :draining}}
    refute_received {:returned, ^pid, _}
  end

  test "explicit stream close still runs cleanup" do
    pid = start_supervised!({Connection, self()}, shutdown: 250)
    ref = Process.monitor(pid)
    assert_receive {:streaming, ^pid}, 1_000

    send(pid, :close_sse)

    assert_receive {:closed, ^pid}
    assert_receive {:returned, ^pid, true}
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
  end

  describe "reraise_shutdown/1" do
    test "re-raises a shutdown exit" do
      assert catch_exit(Streaming.reraise_shutdown(:shutdown)) == :shutdown
      assert catch_exit(Streaming.reraise_shutdown({:shutdown, :draining})) == {:shutdown, :draining}
    end

    test "returns for a session that stopped mid-call and for other reasons" do
      assert Streaming.reraise_shutdown({:shutdown, {GenServer, :call, []}}) == :ok
      assert Streaming.reraise_shutdown(:noproc) == :ok
      assert Streaming.reraise_shutdown({:shutdown, :a, :b}) == :ok
    end
  end
end
