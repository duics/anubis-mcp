defmodule Anubis.Server.Handlers.InputRequests do
  @moduledoc false

  alias Anubis.MCP.Error
  alias Anubis.Server.Frame
  alias Anubis.Server.InputRequired
  alias Anubis.Server.RequestState
  alias Anubis.Server.Stateless

  @type binding :: RequestState.binding()

  @doc """
  Puts a retry's `inputResponses` and verified `requestState` on the frame
  before its handler runs.

  A response that is not an object is dropped rather than rejected, so the
  handler sees it as unanswered and asks again. A state that fails
  verification is refused with `-32602`.

  The state is bound to a SHA-256 of the params. This release accepts a state
  bound to either the params' canonical JSON or their `term_to_binary`, but
  still mints the `term_to_binary` digest, so a release that knows only that
  digest, running beside this one during a deploy, accepts what this one
  mints. A later release flips minting to canonical JSON once every deployed
  release verifies it.
  """
  @spec admit(map(), Frame.t(), binding()) :: {:ok, Frame.t()} | {:error, Error.t(), Frame.t()}
  def admit(request, %Frame{} = frame, binding) do
    params = request["params"] || %{}
    responses = answered(params["inputResponses"])
    frame = with_digest(frame, legacy_digest(params))

    case verify_state(params["requestState"], params, frame, binding) do
      {:ok, state} ->
        {:ok, %{frame | context: %{frame.context | input_responses: responses, request_state: state}}}

      {:error, reason} ->
        {:error, Error.protocol(:invalid_params, %{message: "Invalid requestState", reason: reason}), frame}
    end
  end

  @doc """
  Turns a handler's `Anubis.Server.InputRequired` into the result sent to the
  client, or the error that stands in for it.
  """
  @spec respond(InputRequired.t(), Frame.t(), binding()) :: {:reply, map(), Frame.t()} | {:error, Error.t(), Frame.t()}
  def respond(%InputRequired{} = input, %Frame{} = frame, binding) do
    capabilities = frame.context.client_capabilities || %{}

    missing =
      Map.reject(InputRequired.required_capabilities(input), fn {capability, features} ->
        supports?(capability, Map.get(capabilities, capability), features)
      end)

    cond do
      Stateless.era(frame.context.protocol_module) != :stateless ->
        internal_error(frame, "InputRequiredResult needs a request of the stateless protocol era")

      input.input_requests == %{} and is_nil(input.request_state) ->
        internal_error(frame, "InputRequiredResult needs at least one input request or a state")

      missing != %{} ->
        {:error, Error.missing_required_client_capability(missing), frame}

      true ->
        {:reply, result(input, frame, binding), frame}
    end
  end

  defp supports?("elicitation", declared, required) when declared == %{},
    do: supports?("elicitation", %{"form" => %{}}, required)

  defp supports?(_capability, declared, required) when is_map(declared) do
    Enum.all?(required, fn {feature, _settings} -> is_map(Map.get(declared, feature)) end)
  end

  defp supports?(_capability, _declared, _required), do: false

  defp result(input, frame, binding) do
    %{"resultType" => "input_required"}
    |> put_unless_empty("inputRequests", input.input_requests)
    |> put_state(input.request_state, frame, binding)
  end

  defp put_unless_empty(result, _key, value) when value == %{}, do: result
  defp put_unless_empty(result, key, value), do: Map.put(result, key, value)

  defp put_state(result, nil, _frame, _binding), do: result

  defp put_state(result, {:set, term}, frame, binding),
    do: Map.put(result, "requestState", RequestState.sign(term, frame, binding))

  # Canonical JSON, not term_to_binary: its external term format may change
  # between OTP releases. Verified only, until every deployed release verifies
  # it; then minting flips to it.
  defp digest(params), do: :crypto.hash(:sha256, canonical_json(salient(params)))

  # Still minted, so a release that verifies only this digest accepts our states.
  defp legacy_digest(params), do: :crypto.hash(:sha256, :erlang.term_to_binary(salient(params), [:deterministic]))

  defp with_digest(frame, digest), do: %{frame | context: %{frame.context | request_digest: digest}}

  defp salient(params), do: Map.drop(params, ["_meta", "inputResponses", "requestState"])

  defp canonical_json(map) when is_map(map) and not is_struct(map) do
    members =
      map
      |> Enum.map(fn {key, value} -> {to_string(key), value} end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {key, value} -> [JSON.encode!(key), ?:, canonical_json(value)] end)

    [?{, Enum.intersperse(members, ?,), ?}]
  end

  defp canonical_json(list) when is_list(list), do: [?[, Enum.intersperse(Enum.map(list, &canonical_json/1), ?,), ?]]
  defp canonical_json(value), do: JSON.encode!(value)

  defp verify_state(nil, _params, _frame, _binding), do: {:ok, nil}

  defp verify_state(token, params, frame, binding) when is_binary(token) do
    with {:error, :mismatch} <- RequestState.verify(token, frame, binding) do
      RequestState.verify(token, with_digest(frame, digest(params)), binding)
    end
  end

  defp verify_state(_other, _params, _frame, _binding), do: {:error, :invalid}

  defp answered(responses) when is_map(responses),
    do: Map.filter(responses, fn {key, value} -> is_binary(key) and is_map(value) end)

  defp answered(_other), do: %{}

  defp internal_error(frame, message), do: {:error, Error.protocol(:internal_error, %{message: message}), frame}
end
