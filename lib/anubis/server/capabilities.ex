defmodule Anubis.Server.Capabilities do
  @moduledoc false

  alias Anubis.Server.Frame

  @doc """
  Resolves the capabilities a caller is offered: `c:Anubis.Server.server_capabilities/1`
  when the server defines it, `c:Anubis.Server.server_capabilities/0` otherwise.
  """
  @spec resolve(module(), Frame.t()) :: map()
  def resolve(server, %Frame{} = frame) do
    if Anubis.exported?(server, :server_capabilities, 1),
      do: server.server_capabilities(frame),
      else: server.server_capabilities()
  end

  @doc """
  The capabilities offered to the caller of `frame`: those a session kept from
  its handshake, or, for a stateless request, those resolved for the request.
  """
  @spec offered(module(), Frame.t()) :: map()
  def offered(server, %Frame{context: %{server_capabilities: nil}} = frame), do: resolve(server, frame)
  def offered(_server, %Frame{context: %{server_capabilities: capabilities}}), do: capabilities

  @doc """
  Resolves the instructions a caller is offered: `c:Anubis.Server.server_instructions/1`
  when the server defines it, `c:Anubis.Server.server_instructions/0` otherwise.
  """
  @spec instructions(module(), Frame.t()) :: String.t() | nil
  def instructions(server, %Frame{} = frame) do
    cond do
      Anubis.exported?(server, :server_instructions, 1) -> server.server_instructions(frame)
      Anubis.exported?(server, :server_instructions, 0) -> server.server_instructions()
      true -> nil
    end
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
