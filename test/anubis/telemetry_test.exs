defmodule Anubis.TelemetryTest do
  use ExUnit.Case, async: true

  alias Anubis.Telemetry

  doctest Telemetry

  describe "request_metadata/1" do
    test "names nothing for a method without a subject" do
      assert %{tool: nil, retry: false, input_responses: 0} = Telemetry.request_metadata(%{"method" => "tools/list"})
    end

    test "tolerates params that are not an object" do
      assert %{tool: nil, retry: false, input_responses: 0} =
               Telemetry.request_metadata(%{"method" => "tools/call", "params" => "oops"})
    end

    test "counts a requestState without responses as a retry" do
      request = %{"method" => "prompts/get", "params" => %{"name" => "draft", "requestState" => "token"}}
      assert %{tool: "draft", retry: true, input_responses: 0} = Telemetry.request_metadata(request)
    end
  end
end
