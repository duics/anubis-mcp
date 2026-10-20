defmodule Anubis.TelemetryTest do
  use ExUnit.Case, async: true

  alias Anubis.Telemetry

  doctest Telemetry

  describe "request_metadata/1" do
    test "names nothing for a method without a subject" do
      assert %{tool: nil, prompt: nil, resource_uri: nil, retry: false, input_responses: 0} =
               Telemetry.request_metadata(%{"method" => "tools/list"})
    end

    test "tolerates params that are not an object" do
      assert %{tool: nil, retry: false, input_responses: 0} =
               Telemetry.request_metadata(%{"method" => "tools/call", "params" => "oops"})
    end

    test "counts a requestState without responses as a retry" do
      request = %{"method" => "prompts/get", "params" => %{"name" => "draft", "requestState" => "token"}}
      assert %{tool: nil, prompt: "draft", retry: true, input_responses: 0} = Telemetry.request_metadata(request)
    end

    test "keeps the tool, prompt and resource URI apart" do
      assert %{tool: "greet", prompt: nil, resource_uri: nil} =
               Telemetry.request_metadata(%{"method" => "tools/call", "params" => %{"name" => "greet"}})

      for method <- ["resources/read", "resources/subscribe", "resources/unsubscribe"] do
        assert %{tool: nil, prompt: nil, resource_uri: "notes://a"} =
                 Telemetry.request_metadata(%{"method" => method, "params" => %{"uri" => "notes://a"}})
      end

      assert %{tool: nil, prompt: nil, resource_uri: nil} =
               Telemetry.request_metadata(%{"method" => "resources/templates/list", "params" => %{"uri" => "x"}})
    end
  end

  describe "span_tool_call/3 with a request" do
    test "puts the session id and protocol version on the start and stop events" do
      test_pid = self()
      handler_id = "span-tool-call-#{System.unique_integer([:positive])}"

      :telemetry.attach_many(
        handler_id,
        [[:anubis_mcp, :server, :tool_call, :start], [:anubis_mcp, :server, :tool_call, :stop]],
        fn event, _m, meta, _c -> send(test_pid, {event, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      request = %{"method" => "tools/call", "params" => %{"name" => "greet", "requestState" => "s"}}
      session = %{session_id: "s-1", protocol_version: "2025-11-25", ignored: true}
      assert :ok = Telemetry.span_tool_call(request, session, fn -> :ok end)

      for phase <- [:start, :stop] do
        assert_receive {[:anubis_mcp, :server, :tool_call, ^phase], meta}
        assert %{tool: "greet", session_id: "s-1", protocol_version: "2025-11-25", retry: true} = meta
        refute Map.has_key?(meta, :ignored)
      end
    end
  end
end
