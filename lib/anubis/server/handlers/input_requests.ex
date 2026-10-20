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
  verification is refused with `-32602` "Invalid params", whose `data` says so
  in fields to match on rather than the message text: `kind:
  :invalid_request_state`, and `reason`, why it failed (`:invalid`,
  `:mismatch` or `:expired`, from `RequestState.verify/3`).

  The state is bound to a SHA-256 of the params (less `_meta`,
  `inputResponses` and `requestState`). Two digests exist: the SHA-256 of
  `:erlang.term_to_binary(params, [:deterministic])`, whose output OTP does
  not promise to keep across major releases, and the SHA-256 of the params
  encoded as JSON with the keys of every object sorted, by Elixir's `JSON`
  (a deterministic encoding of our own, not RFC 8785 canonical JSON). The
  rollout runs in three phases so a retry that crosses releases during a
  deploy keeps verifying:

  1. (this release) verify either digest, mint `term_to_binary`, so a
     release that knows only that digest accepts what this one mints;
  2. mint the sorted-key JSON digest and keep verifying both. The
     `term_to_binary` path must stay until at least `:request_state_ttl`
     after the last phase-1 node has drained, or a state that node minted
     is refused on its retry;
  3. drop the `term_to_binary` digest.
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
        error =
          Error.protocol(:invalid_params, %{message: "Invalid requestState", kind: :invalid_request_state, reason: reason})

        {:error, error, frame}
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

  # Sorted-key JSON, not term_to_binary: its external term format may change
  # between OTP releases. Verified only, until every deployed release verifies
  # it; then minting flips to it (phase 2 in `admit/3`).
  defp digest(params), do: :crypto.hash(:sha256, sorted_json(salient(params)))

  # Still minted, so a release that verifies only this digest accepts our states.
  defp legacy_digest(params), do: :crypto.hash(:sha256, :erlang.term_to_binary(salient(params), [:deterministic]))

  defp with_digest(frame, digest), do: %{frame | context: %{frame.context | request_digest: digest}}

  defp salient(params), do: Map.drop(params, ["_meta", "inputResponses", "requestState"])

  defp sorted_json(map) when is_map(map) and not is_struct(map) do
    members =
      map
      |> Enum.map(fn {key, value} -> {to_string(key), value} end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {key, value} -> [JSON.encode!(key), ?:, sorted_json(value)] end)

    [?{, Enum.intersperse(members, ?,), ?}]
  end

  defp sorted_json(list) when is_list(list), do: [?[, Enum.intersperse(Enum.map(list, &sorted_json/1), ?,), ?]]
  defp sorted_json(value), do: JSON.encode!(value)

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
