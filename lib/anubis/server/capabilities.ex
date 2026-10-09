defmodule Anubis.Server.Capabilities do
  @moduledoc false

  alias Anubis.Server.Frame

  @doc """
  Resolves the capabilities a caller is offered: `c:Anubis.Server.server_capabilities/1`
  when the server defines it, `fallback` otherwise.
  """
  @spec resolve(module(), Frame.t(), map()) :: map()
  def resolve(server, %Frame{} = frame, fallback) when is_map(fallback) do
    if Anubis.exported?(server, :server_capabilities, 1),
      do: server.server_capabilities(frame),
      else: fallback
  end

  @doc """
  Shapes capabilities for the wire of `protocol_module`: the version's own
  filter, then the keys the server passes through for that version's era.
  """
  @spec for_protocol(module(), module(), map()) :: map()
  def for_protocol(server, protocol_module, capabilities) when is_map(capabilities) do
    passthrough =
      server
      |> Anubis.Server.capability_passthrough()
      |> Map.get(protocol_module.era(), [])

    capabilities
    |> protocol_module.server_capabilities()
    |> Map.merge(Map.take(capabilities, passthrough))
  end
end
