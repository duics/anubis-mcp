defmodule Anubis.Server.Frame do
  @moduledoc """
  The Anubis Frame — pure user state + read-only context.

  ## User fields

    * `assigns` - shared user data as a map. For HTTP transports, this inherits
      from `Plug.Conn.assigns`.

  ## Component maps

  Runtime-registered components are stored in typed maps keyed by name/URI:

    * `tools` - `%{name => %Tool{}}`
    * `resources` - `%{uri => %Resource{}}`
    * `prompts` - `%{name => %Prompt{}}`
    * `resource_templates` - `%{name => %Resource{uri_template: ...}}`

  ## Pagination

    * `pagination_limit` - optional limit for listing operations

  ## Context

    * `context` - read-only `%Context{}`, refreshed by Session before each callback
  """

  alias Anubis.Server.Component
  alias Anubis.Server.Component.Prompt
  alias Anubis.Server.Component.Resource
  alias Anubis.Server.Component.Schema
  alias Anubis.Server.Component.Tool
  alias Anubis.Server.Context

  @type server_component_t :: Tool.t() | Resource.t() | Prompt.t()

  @type t :: %__MODULE__{
          assigns: map(),
          tools: %{optional(String.t()) => Tool.t()},
          resources: %{optional(String.t()) => Resource.t()},
          prompts: %{optional(String.t()) => Prompt.t()},
          resource_templates: %{optional(String.t()) => Resource.t()},
          pagination_limit: non_neg_integer() | nil,
          context: Context.t()
        }

  defstruct assigns: %{},
            tools: %{},
            resources: %{},
            prompts: %{},
            resource_templates: %{},
            pagination_limit: nil,
            context: %Context{}

  @doc """
  Creates a new frame with optional initial assigns.

  ## Examples

      iex> Frame.new()
      %Frame{assigns: %{}}

      iex> Frame.new(%{user: "alice"})
      %Frame{assigns: %{user: "alice"}}
  """
  @spec new :: t()
  @spec new(assigns :: map()) :: t()
  def new(assigns \\ %{}), do: struct(__MODULE__, assigns: assigns)

  @doc """
  Assigns a value or multiple values to the frame.

  ## Examples

      frame = Frame.assign(frame, :status, :active)
      frame = Frame.assign(frame, %{status: :active, count: 5})
      frame = Frame.assign(frame, status: :active, count: 5)
  """
  @spec assign(t(), Enumerable.t()) :: t()
  @spec assign(t(), key :: atom(), value :: any()) :: t()
  def assign(%__MODULE__{} = frame, assigns) when is_map(assigns) or is_list(assigns) do
    Enum.reduce(assigns, frame, fn {key, value}, frame ->
      assign(frame, key, value)
    end)
  end

  def assign(%__MODULE__{} = frame, key, value) when is_atom(key) do
    %{frame | assigns: Map.put(frame.assigns, key, value)}
  end

  @doc """
  Assigns a value to the frame only if the key doesn't already exist.

  The value is computed lazily using the provided function.

  ## Examples

      frame = Frame.assign_new(frame, :timestamp, fn -> DateTime.utc_now() end)
  """
  @spec assign_new(t(), key :: atom(), value_fun :: (-> term())) :: t()
  def assign_new(%__MODULE__{} = frame, key, fun) when is_atom(key) and is_function(fun, 0) do
    case frame.assigns do
      %{^key => _} -> frame
      _ -> assign(frame, key, fun.())
    end
  end

  @doc """
  Sets the pagination limit for listing operations.

  ## Examples

      frame = Frame.put_pagination_limit(frame, 10)
      frame.pagination_limit
      # => 10
  """
  @spec put_pagination_limit(t(), non_neg_integer()) :: t()
  def put_pagination_limit(%__MODULE__{} = frame, limit) when limit > 0 do
    %{frame | pagination_limit: limit}
  end

  @doc """
  Registers a tool definition at runtime.
  """
  @spec register_tool(t(), String.t(), list(tool_opt)) :: t()
        when tool_opt:
               {:description, String.t() | nil}
               | {:input_schema, map() | nil}
               | {:output_schema, map() | nil}
               | {:title, String.t() | nil}
               | {:annotations, map() | nil}
  def register_tool(%__MODULE__{} = frame, name, opts) when is_binary(name) do
    input_schema = Schema.normalize(opts[:input_schema] || %{})
    raw_schema = Component.__clean_schema_for_peri__(input_schema)
    validate_input = fn params -> Peri.validate(raw_schema, params) end

    output_schema = if s = opts[:output_schema], do: Schema.normalize(s)

    validate_output =
      if output_schema do
        raw_output = Component.__clean_schema_for_peri__(output_schema)
        fn params -> Peri.validate(raw_output, params) end
      end

    annotations = opts[:annotations]
    title = annotations[:title] || annotations["title"] || opts[:title] || name

    tool = %Tool{
      name: name,
      description: opts[:description],
      input_schema: Schema.to_json_schema(input_schema),
      output_schema: if(output_schema, do: Schema.to_json_schema(output_schema)),
      annotations: annotations,
      meta: opts[:meta],
      title: title,
      validate_input: validate_input,
      validate_output: validate_output
    }

    %{frame | tools: Map.put(frame.tools, name, tool)}
  end

  @doc """
  Registers a prompt definition at runtime.
  """
  @spec register_prompt(t(), String.t(), list(prompt_opt)) :: t()
        when prompt_opt: {:description, String.t() | nil} | {:arguments, map() | nil} | {:title, String.t() | nil}
  def register_prompt(%__MODULE__{} = frame, name, opts) when is_binary(name) do
    arguments = Schema.normalize(opts[:arguments] || %{})
    raw_schema = Component.__clean_schema_for_peri__(arguments)
    validate_input = fn params -> Peri.validate(raw_schema, params) end
    title = opts[:title] || name

    prompt = %Prompt{
      name: name,
      title: title,
      description: opts[:description],
      arguments: Schema.to_prompt_arguments(arguments),
      validate_input: validate_input
    }

    %{frame | prompts: Map.put(frame.prompts, name, prompt)}
  end

  @doc """
  Registers a resource definition with a fixed URI.

  For parameterized resources, use `register_resource_template/3` instead.
  """
  @spec register_resource(t(), String.t(), list(resource_opt)) :: t()
        when resource_opt:
               {:title, String.t() | nil}
               | {:name, String.t() | nil}
               | {:description, String.t() | nil}
               | {:mime_type, String.t() | nil}
  def register_resource(%__MODULE__{} = frame, uri, opts) when is_binary(uri) do
    name = opts[:name] || Path.basename(uri)

    resource = %Resource{
      uri: uri,
      title: opts[:title] || name,
      name: name,
      description: opts[:description],
      mime_type: opts[:mime_type] || "text/plain"
    }

    %{frame | resources: Map.put(frame.resources, uri, resource)}
  end

  @doc """
  Registers a resource template definition using a URI template (RFC 6570).

  ## Examples

      frame = Frame.register_resource_template(frame, "file:///{path}",
        name: "project_files",
        title: "Project Files",
        description: "Access files in the project directory"
      )
  """
  @spec register_resource_template(t(), String.t(), list(resource_template_opt)) :: t()
        when resource_template_opt:
               {:title, String.t() | nil}
               | {:name, String.t()}
               | {:description, String.t() | nil}
               | {:mime_type, String.t() | nil}
  def register_resource_template(%__MODULE__{} = frame, uri_template, opts) when is_binary(uri_template) do
    name = Keyword.fetch!(opts, :name)

    resource = %Resource{
      uri_template: uri_template,
      title: opts[:title] || name,
      name: name,
      description: opts[:description],
      mime_type: opts[:mime_type] || "text/plain"
    }

    %{frame | resource_templates: Map.put(frame.resource_templates, name, resource)}
  end

  @doc "Clears all runtime-registered components"
  @spec clear_components(t()) :: t()
  def clear_components(%__MODULE__{} = frame) do
    %{frame | tools: %{}, resources: %{}, prompts: %{}, resource_templates: %{}}
  end

  @doc "Retrieves all runtime-registered components as a flat list"
  @spec get_components(t()) :: list(server_component_t())
  def get_components(%__MODULE__{} = frame) do
    Map.values(frame.tools) ++
      Map.values(frame.resources) ++
      Map.values(frame.prompts) ++
      Map.values(frame.resource_templates)
  end

  @doc false
  @spec get_tools(t()) :: list(Tool.t())
  def get_tools(%__MODULE__{} = frame), do: Map.values(frame.tools)

  @doc false
  @spec get_prompts(t()) :: list(Prompt.t())
  def get_prompts(%__MODULE__{} = frame), do: Map.values(frame.prompts)

  @doc false
  @spec get_resources(t()) :: list(Resource.t())
  def get_resources(%__MODULE__{} = frame) do
    Map.values(frame.resources) ++ Map.values(frame.resource_templates)
  end

  @doc false
  @spec get_component(t(), name :: String.t()) :: server_component_t() | nil
  def get_component(%__MODULE__{} = frame, name) do
    frame.tools[name] ||
      frame.prompts[name] ||
      frame.resource_templates[name] ||
      Enum.find(Map.values(frame.resources), &(&1.name == name))
  end

  # Per-field merge — concurrent task result reconciliation
  #
  # Tasks dispatched by the Session capture a frame snapshot, run user code,
  # and return a `frame_diff`. The Session merges those diffs back into its
  # current frame using per-field policy:
  #
  #   * `assigns` — per-key LWW, with conflict detection vs snapshot
  #   * `tools` / `resources` / `prompts` / `resource_templates` —
  #     per-key union; conflicts on identical key emit telemetry
  #   * `pagination_limit` — scalar LWW; conflict if state moved since snapshot
  #   * `context` — never merged (Session re-derives per dispatch)

  @typedoc """
  A diff between a frame snapshot and the post-callback frame produced by a task.
  """
  @type assigns_diff :: %{added_or_changed: map(), deleted: [term()]}
  @type component_diff :: %{added_or_changed: map(), deleted: [String.t()]}
  @type pagination_diff :: :unchanged | {:set, non_neg_integer() | nil}

  @type frame_diff :: %{
          assigns: assigns_diff(),
          tools: component_diff(),
          resources: component_diff(),
          prompts: component_diff(),
          resource_templates: component_diff(),
          pagination_limit: pagination_diff()
        }

  @typedoc """
  A conflict event emitted by `merge_diff/3` when a per-key write would clobber
  state that has moved since the diffing task captured its snapshot.

  Shape: `{field, key | nil, snapshot_value, current_value, new_value}`. For scalar
  fields like `pagination_limit`, `key` is `nil`.
  """
  @type conflict_event ::
          {field :: atom(), key :: term() | nil, snapshot_value :: term(), current_value :: term(), new_value :: term()}

  @doc """
  Computes a structured diff from a `snapshot` frame to a `post` frame.

  The diff captures only what changed, scoped per field. The `:context` field
  is intentionally excluded — Session re-derives it per-dispatch.
  """
  @spec diff(t(), t()) :: frame_diff()
  def diff(%__MODULE__{} = snapshot, %__MODULE__{} = post) do
    %{
      assigns: map_diff(snapshot.assigns, post.assigns),
      tools: map_diff(snapshot.tools, post.tools),
      resources: map_diff(snapshot.resources, post.resources),
      prompts: map_diff(snapshot.prompts, post.prompts),
      resource_templates: map_diff(snapshot.resource_templates, post.resource_templates),
      pagination_limit: scalar_diff(snapshot.pagination_limit, post.pagination_limit)
    }
  end

  @doc """
  Merges a `diff` into `state_frame`, using `snapshot_frame` to detect per-key
  conflicts (cases where state has moved since the task captured its snapshot).

  Returns `{merged_frame, conflicts}` where `conflicts` is a list of
  `t:conflict_event/0` tuples — one per key whose pre-merge value in state
  differs from the value the task observed in its snapshot. The merge is
  applied unconditionally (last-write-wins); conflicts are surfaced for
  telemetry, not blocked.
  """
  @spec merge_diff(t(), frame_diff(), t()) :: {t(), [conflict_event()]}
  def merge_diff(%__MODULE__{} = state_frame, %{} = diff, %__MODULE__{} = snapshot_frame) do
    {assigns, c1} = merge_map(state_frame.assigns, diff.assigns, snapshot_frame.assigns, :assigns)
    {tools, c2} = merge_map(state_frame.tools, diff.tools, snapshot_frame.tools, :tools)

    {resources, c3} =
      merge_map(state_frame.resources, diff.resources, snapshot_frame.resources, :resources)

    {prompts, c4} = merge_map(state_frame.prompts, diff.prompts, snapshot_frame.prompts, :prompts)

    {resource_templates, c5} =
      merge_map(
        state_frame.resource_templates,
        diff.resource_templates,
        snapshot_frame.resource_templates,
        :resource_templates
      )

    {pagination_limit, c6} =
      merge_scalar(
        state_frame.pagination_limit,
        diff.pagination_limit,
        snapshot_frame.pagination_limit,
        :pagination_limit
      )

    merged = %{
      state_frame
      | assigns: assigns,
        tools: tools,
        resources: resources,
        prompts: prompts,
        resource_templates: resource_templates,
        pagination_limit: pagination_limit
    }

    {merged, c1 ++ c2 ++ c3 ++ c4 ++ c5 ++ c6}
  end

  defp map_diff(snapshot, post) when is_map(snapshot) and is_map(post) do
    added_or_changed =
      for {k, v} <- post, Map.get(snapshot, k) != v, into: %{}, do: {k, v}

    deleted =
      for k <- Map.keys(snapshot), not Map.has_key?(post, k), do: k

    %{added_or_changed: added_or_changed, deleted: deleted}
  end

  defp scalar_diff(snapshot_v, post_v) when snapshot_v == post_v, do: :unchanged
  defp scalar_diff(_snapshot_v, post_v), do: {:set, post_v}

  defp merge_map(state_map, %{added_or_changed: changes, deleted: deletes}, snapshot_map, field) do
    {merged, conflicts} =
      Enum.reduce(changes, {state_map, []}, fn {k, new_v}, {acc, conflicts} ->
        snapshot_v = Map.get(snapshot_map, k)
        current_v = Map.get(acc, k)

        conflicts =
          if Map.has_key?(acc, k) and current_v != snapshot_v and current_v != new_v do
            [{field, k, snapshot_v, current_v, new_v} | conflicts]
          else
            conflicts
          end

        {Map.put(acc, k, new_v), conflicts}
      end)

    merged_after_deletes =
      Enum.reduce(deletes, merged, fn k, acc -> Map.delete(acc, k) end)

    {merged_after_deletes, Enum.reverse(conflicts)}
  end

  defp merge_scalar(state_v, :unchanged, _snapshot_v, _field), do: {state_v, []}

  defp merge_scalar(state_v, {:set, new_v}, snapshot_v, field) do
    if state_v != snapshot_v and state_v != new_v do
      {new_v, [{field, nil, snapshot_v, state_v, new_v}]}
    else
      {new_v, []}
    end
  end

  @doc """
  Serializes Frame for persistent storage.

  Only `assigns` and `pagination_limit` are persisted. The following fields are
  **runtime-only** and excluded from serialization:

    * `tools` — runtime-registered tool definitions (includes validator functions)
    * `resources` — runtime-registered resource definitions
    * `prompts` — runtime-registered prompt definitions
    * `resource_templates` — runtime-registered resource template definitions
    * `context` — rebuilt by Session before each callback invocation

  Compile-time components (registered via the `component` macro) are always
  available from the server module and do not need persistence.
  """
  @spec to_saved(t()) :: map()
  def to_saved(%__MODULE__{} = frame) do
    %{
      "assigns" => frame.assigns,
      "pagination_limit" => frame.pagination_limit
    }
  end

  @doc """
  Reconstructs Frame from a previously saved map.

  Only `assigns` and `pagination_limit` are restored. Runtime-only fields (`tools`,
  `resources`, `prompts`, `resource_templates`) are initialized empty — their validator
  functions are not serializable. `context` is left as the default struct and will be
  set by Session before each callback invocation.
  """
  @spec from_saved(map()) :: t()
  def from_saved(map) when is_map(map) do
    %__MODULE__{
      assigns: Map.get(map, "assigns", %{}),
      pagination_limit: Map.get(map, "pagination_limit")
    }
  end

  def from_saved(_), do: %__MODULE__{}
end

defimpl Inspect, for: Anubis.Server.Frame do
  import Inspect.Algebra

  def inspect(frame, opts) do
    info = [
      assigns: frame.assigns,
      tools: map_size(frame.tools),
      resources: map_size(frame.resources),
      prompts: map_size(frame.prompts),
      resource_templates: map_size(frame.resource_templates)
    ]

    info =
      if session_id = frame.context.session_id,
        do: [{:session_id, session_id} | info],
        else: info

    concat(["#Frame<", to_doc(info, opts), ">"])
  end
end
