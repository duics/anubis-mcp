defmodule Anubis.Server.SupervisorTest do
  use ExUnit.Case, async: false

  alias Anubis.Server.Session
  alias Anubis.Server.Supervisor, as: ServerSupervisor

  defmodule DebouncedServer do
    @moduledoc false

    use Anubis.Server,
      name: "debounced-server",
      version: "1.0.0",
      capabilities: [{:tools, list_changed?: true}]
  end

  setup do
    on_exit(fn ->
      :persistent_term.erase({ServerSupervisor, DebouncedServer, :session_config})
      :persistent_term.erase({ServerSupervisor, DebouncedServer, :session_supervisor_mod})
    end)
  end

  describe "list_changed_debounce" do
    test "reaches the stdio session" do
      assert {:ok, {_flags, children}} =
               ServerSupervisor.init(module: DebouncedServer, transport: :stdio, list_changed_debounce: 250)

      for %{start: {module, _fun, _args}} = child <- children, module in [Task.Supervisor, Session] do
        start_supervised!(child)
      end

      [%{start: {Session, :start_link, [session_opts]}}] = Enum.filter(children, &match?(%{start: {Session, _, _}}, &1))
      session = Keyword.fetch!(session_opts, :name)

      assert %{list_changed_debounce: 250} = :sys.get_state(session)
    end

    test "is refused at boot when it is not a positive integer" do
      for window <- [0, -1, "500", 1.5] do
        assert_raise ArgumentError, ~r/:list_changed_debounce/, fn ->
          ServerSupervisor.init(module: DebouncedServer, transport: :stdio, list_changed_debounce: window)
        end
      end
    end
  end
end
