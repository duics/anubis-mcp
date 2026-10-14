defmodule Anubis.Server.SessionNotificationContextTest do
  use Anubis.MCP.Case, async: false

  alias Anubis.Server.Frame
  alias Anubis.Server.Registry
  alias Anubis.Server.Session

  @moduletag capture_log: true

  defmodule ContextServer do
    @moduledoc false
    use Anubis.Server, name: "notification-context-server", version: "1.0.0", capabilities: [:tools]

    @impl true
    def init(_client_info, frame), do: {:ok, Frame.assign(frame, :init_context, seen(frame))}

    @impl true
    def handle_notification(_notification, frame), do: {:noreply, Frame.assign(frame, :notification_context, seen(frame))}

    defp seen(frame), do: Map.take(frame.context, [:headers, :remote_ip, :auth])
  end

  @transport_context %{
    assigns: %{},
    type: :http,
    req_headers: [{"authorization", "Bearer abc"}],
    remote_ip: {127, 0, 0, 1},
    auth: %{"sub" => "user-7"}
  }

  @seen %{headers: %{"authorization" => "Bearer abc"}, remote_ip: {127, 0, 0, 1}, auth: %{"sub" => "user-7"}}

  setup do
    transport = Registry.transport_name(ContextServer, StubTransport)
    start_supervised!({StubTransport, name: transport})

    task_sup = Registry.task_supervisor_name(ContextServer)
    start_supervised!({Task.Supervisor, name: task_sup})

    session =
      start_supervised!(
        {Session,
         session_id: "notification-context",
         server_module: ContextServer,
         name: Registry.session_name(ContextServer, "notification-context"),
         transport: [layer: StubTransport, name: transport],
         task_supervisor: task_sup}
      )

    {:ok, _} =
      GenServer.call(session, {:mcp_request, init_request("2025-06-18", %{"name" => "t", "version" => "1"}), %{}})

    %{session: session}
  end

  test "init/2 sees the headers, remote IP and auth of notifications/initialized", %{session: session} do
    GenServer.cast(session, {:mcp_notification, build_notification("notifications/initialized"), @transport_context})

    assert :sys.get_state(session).frame.assigns.init_context == @seen
  end

  test "handle_notification/2 sees the headers, remote IP and auth of its notification", %{session: session} do
    GenServer.cast(session, {:mcp_notification, build_notification("notifications/initialized"), %{}})

    GenServer.cast(
      session,
      {:mcp_notification, build_notification("notifications/roots/list_changed"), @transport_context}
    )

    assert :sys.get_state(session).frame.assigns.notification_context == @seen
  end
end
