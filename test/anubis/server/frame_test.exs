defmodule Anubis.Server.FrameTest do
  use ExUnit.Case, async: true

  alias Anubis.Server.Component.Resource
  alias Anubis.Server.Context
  alias Anubis.Server.Frame

  describe "assign/2 preserves context" do
    test "assigning values does not modify context" do
      original_context = %Context{
        session_id: "session-123",
        client_info: %{"name" => "test"},
        headers: %{"authorization" => "Bearer token"},
        remote_ip: {127, 0, 0, 1}
      }

      frame = %Frame{context: original_context, assigns: %{existing: true}}
      updated_frame = Frame.assign(frame, %{new_key: "value", another: 42})

      assert updated_frame.context == original_context
      assert updated_frame.assigns[:new_key] == "value"
      assert updated_frame.assigns[:another] == 42
      assert updated_frame.assigns[:existing] == true
    end

    test "assigning does not allow overwriting context struct fields" do
      context = %Context{session_id: "original"}
      frame = %Frame{context: context}

      updated_frame = Frame.assign(frame, %{context: "malicious"})

      assert updated_frame.context == context
      assert updated_frame.assigns[:context] == "malicious"
    end
  end

  describe "diff/2 + merge_diff/3 (per-field merge for concurrent task results)" do
    setup do
      snapshot = %Frame{assigns: %{user: "alice", count: 1}, pagination_limit: 10}
      %{snapshot: snapshot}
    end

    test "diff captures added, changed, and deleted assigns", %{snapshot: snapshot} do
      post = snapshot |> Frame.assign(%{count: 2, new_key: :v}) |> then(&%{&1 | assigns: Map.delete(&1.assigns, :user)})

      diff = Frame.diff(snapshot, post)

      assert diff.assigns.added_or_changed == %{count: 2, new_key: :v}
      assert :user in diff.assigns.deleted
    end

    test "diff for unchanged assigns is empty", %{snapshot: snapshot} do
      diff = Frame.diff(snapshot, snapshot)
      assert diff.assigns.added_or_changed == %{}
      assert diff.assigns.deleted == []
      assert diff.tools.added_or_changed == %{}
      assert diff.tools.deleted == []
      assert diff.pagination_limit == :unchanged
    end

    test "merge_diff applies non-conflicting per-key changes from two tasks", %{snapshot: snapshot} do
      task_a = Frame.assign(snapshot, %{role: :admin})
      task_b = Frame.assign(snapshot, %{theme: :dark})

      diff_a = Frame.diff(snapshot, task_a)
      diff_b = Frame.diff(snapshot, task_b)

      {state_after_a, conflicts_a} = Frame.merge_diff(snapshot, diff_a, snapshot)
      assert conflicts_a == []
      assert state_after_a.assigns[:role] == :admin

      {state_after_b, conflicts_b} = Frame.merge_diff(state_after_a, diff_b, snapshot)
      assert conflicts_b == []
      assert state_after_b.assigns[:role] == :admin
      assert state_after_b.assigns[:theme] == :dark
      assert state_after_b.assigns[:user] == "alice"
    end

    test "merge_diff detects per-key conflict when same key changes between snapshot and merge",
         %{snapshot: snapshot} do
      task_a = Frame.assign(snapshot, %{count: 99})
      task_b = Frame.assign(snapshot, %{count: 100})

      diff_a = Frame.diff(snapshot, task_a)
      diff_b = Frame.diff(snapshot, task_b)

      {state_after_a, _} = Frame.merge_diff(snapshot, diff_a, snapshot)
      {state_after_b, conflicts_b} = Frame.merge_diff(state_after_a, diff_b, snapshot)

      assert state_after_b.assigns[:count] == 100
      assert [{:assigns, :count, 1, 99, 100}] = conflicts_b
    end

    test "merge_diff with deleted assign drops the key", %{snapshot: snapshot} do
      post = %{snapshot | assigns: Map.delete(snapshot.assigns, :count)}
      diff = Frame.diff(snapshot, post)

      {merged, _} = Frame.merge_diff(snapshot, diff, snapshot)
      refute Map.has_key?(merged.assigns, :count)
    end

    test "tools/resources/prompts diff and union-merge cleanly when keys differ", %{snapshot: snapshot} do
      task_a = Frame.register_tool(snapshot, "tool_a", description: "A")
      task_b = Frame.register_tool(snapshot, "tool_b", description: "B")

      diff_a = Frame.diff(snapshot, task_a)
      diff_b = Frame.diff(snapshot, task_b)

      {state_after_a, []} = Frame.merge_diff(snapshot, diff_a, snapshot)
      {state_after_b, []} = Frame.merge_diff(state_after_a, diff_b, snapshot)

      assert Map.has_key?(state_after_b.tools, "tool_a")
      assert Map.has_key?(state_after_b.tools, "tool_b")
    end

    test "pagination_limit is scalar LWW with conflict telemetry", %{snapshot: snapshot} do
      task_a = Frame.put_pagination_limit(snapshot, 50)
      task_b = Frame.put_pagination_limit(snapshot, 100)

      diff_a = Frame.diff(snapshot, task_a)
      diff_b = Frame.diff(snapshot, task_b)

      assert diff_a.pagination_limit == {:set, 50}

      {state_after_a, []} = Frame.merge_diff(snapshot, diff_a, snapshot)
      {state_after_b, conflicts_b} = Frame.merge_diff(state_after_a, diff_b, snapshot)

      assert state_after_b.pagination_limit == 100
      assert [{:pagination_limit, nil, 10, 50, 100}] = conflicts_b
    end

    test "context is never carried in diff (re-derived per-dispatch by Session)", %{snapshot: snapshot} do
      post = %{snapshot | context: %Context{session_id: "different"}}
      diff = Frame.diff(snapshot, post)

      refute Map.has_key?(diff, :context)
    end
  end

  describe "register_resource_template/3" do
    test "registers a resource template at runtime" do
      frame = Frame.new()

      frame =
        Frame.register_resource_template(frame, "dynamic:///{type}/{id}",
          name: "dynamic_template",
          description: "Dynamically registered template",
          mime_type: "application/json"
        )

      resources = Frame.get_resources(frame)

      assert [%Resource{} = template] = resources
      assert template.uri_template == "dynamic:///{type}/{id}"
      assert template.name == "dynamic_template"
      assert template.title == "dynamic_template"
      assert template.description == "Dynamically registered template"
      assert template.mime_type == "application/json"
      assert is_nil(template.uri)
      assert is_nil(template.handler)
    end

    test "requires name option" do
      frame = Frame.new()

      assert_raise KeyError, fn ->
        Frame.register_resource_template(frame, "dynamic:///{id}", [])
      end
    end

    test "uses custom title when provided" do
      frame = Frame.new()

      frame =
        Frame.register_resource_template(frame, "custom:///{id}",
          name: "custom_template",
          title: "Custom Template Title"
        )

      [resource] = Frame.get_resources(frame)
      assert resource.title == "Custom Template Title"
    end

    test "defaults to text/plain mime type when not specified" do
      frame = Frame.new()

      frame =
        Frame.register_resource_template(frame, "default:///{id}", name: "default_template")

      [resource] = Frame.get_resources(frame)
      assert resource.mime_type == "text/plain"
    end

    test "allows multiple templates to be registered" do
      frame = Frame.new()

      frame =
        frame
        |> Frame.register_resource_template("first:///{id}", name: "first")
        |> Frame.register_resource_template("second:///{id}", name: "second")

      resources = Frame.get_resources(frame)
      assert length(resources) == 2
      assert Enum.any?(resources, &(&1.name == "first"))
      assert Enum.any?(resources, &(&1.name == "second"))
    end
  end
end
