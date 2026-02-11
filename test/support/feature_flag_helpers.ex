defmodule RabbitMQMessageDeduplication.FeatureFlagHelpers do
  @moduledoc """
  Helper functions for mocking RabbitMQ feature flags in tests.

  Uses :meck to mock :rabbit_feature_flags and :rabbit_khepri modules,
  allowing tests to run without a running RabbitMQ broker.
  """

  @khepri_db :khepri_db

  @doc """
  Setup mocks for Mnesia backend testing.

  Mocks:
  - :rabbit_feature_flags.is_enabled/2 to return false
  - :rabbit_khepri.handle_fallback/1 to call the :mnesia function

  Call this in test setup for Mnesia backend tests.
  """
  @spec setup_mnesia_mocks() :: :ok
  def setup_mnesia_mocks() do
    # Mock rabbit_feature_flags to return false (Mnesia mode)
    :meck.new(:rabbit_feature_flags, [:non_strict, :passthrough])
    :meck.expect(:rabbit_feature_flags, :is_enabled, fn @khepri_db, _blocking -> false end)

    # Mock rabbit_khepri.handle_fallback to call mnesia function
    :meck.new(:rabbit_khepri, [:non_strict, :passthrough])
    :meck.expect(:rabbit_khepri, :handle_fallback, fn %{mnesia: mnesia_fn, khepri: _khepri_fn} ->
      mnesia_fn.()
    end)

    :ok
  end

  @doc """
  Setup mocks for Khepri backend testing.

  Mocks:
  - :rabbit_feature_flags.is_enabled/2 to return true
  - :rabbit_khepri.handle_fallback/1 to call the :khepri function
  - :rabbit_khepri.put/2 for Khepri storage operations
  - :rabbit_khepri.get/1 for Khepri retrieval operations
  - :rabbit_khepri.delete/1 for Khepri deletion operations
  - :rabbit_khepri.get_many/1 for Khepri bulk operations
  - :rabbit_khepri.delete_many/1 for Khepri bulk deletion

  Call this in test setup for Khepri backend tests.
  """
  @spec setup_khepri_mocks() :: :ok
  def setup_khepri_mocks() do
    # Mock rabbit_feature_flags to return true (Khepri mode)
    :meck.new(:rabbit_feature_flags, [:non_strict, :passthrough])
    :meck.expect(:rabbit_feature_flags, :is_enabled, fn @khepri_db, _blocking -> true end)

    # Mock rabbit_khepri.handle_fallback to call khepri function
    :meck.new(:rabbit_khepri, [:non_strict, :passthrough])
    :meck.expect(:rabbit_khepri, :handle_fallback, fn %{mnesia: _mnesia_fn, khepri: khepri_fn} ->
      khepri_fn.()
    end)

    # Setup in-memory storage for Khepri operations
    Agent.start_link(fn -> %{} end, name: :khepri_storage)

    # Mock basic Khepri operations to use in-memory storage
    :meck.expect(:rabbit_khepri, :put, fn path, value ->
      Agent.update(:khepri_storage, &Map.put(&1, path, value))
      :ok
    end)

    :meck.expect(:rabbit_khepri, :get, fn path ->
      case Agent.get(:khepri_storage, &Map.get(&1, path)) do
        nil -> {:error, {:khepri, :no_data, %{}}}
        value -> {:ok, value}
      end
    end)

    :meck.expect(:rabbit_khepri, :adv_get, fn path ->
      case Agent.get(:khepri_storage, &Map.get(&1, path)) do
        nil -> {:error, {:khepri, :no_data, %{}}}
        value -> {:ok, %{data: value, payload_version: 1}}
      end
    end)

    :meck.expect(:rabbit_khepri, :delete, fn path ->
      Agent.update(:khepri_storage, &Map.delete(&1, path))
      :ok
    end)

    :meck.expect(:rabbit_khepri, :get_many, fn path ->
      # Extract pattern from path (last element might be wildcard)
      prefix = if List.last(path) == :khepri_wildcard_star do
        Enum.slice(path, 0..-2//1)
      else
        path
      end

      # Find all keys matching the prefix
      matching = Agent.get(:khepri_storage, fn storage ->
        storage
        |> Enum.filter(fn {key, _value} ->
          List.starts_with?(key, prefix)
        end)
        |> Map.new()
      end)

      if map_size(matching) == 0 do
        {:error, {:khepri, :no_data, %{}}}
      else
        {:ok, matching}
      end
    end)

    :meck.expect(:rabbit_khepri, :delete_many, fn path ->
      # Extract pattern from path
      prefix = if List.last(path) == :khepri_wildcard_star do
        Enum.slice(path, 0..-2//1)
      else
        path
      end

      # Delete all keys matching the prefix
      Agent.update(:khepri_storage, fn storage ->
        storage
        |> Enum.reject(fn {key, _value} ->
          List.starts_with?(key, prefix)
        end)
        |> Map.new()
      end)

      :ok
    end)

    :ok
  end

  @doc """
  Clean up mocks after tests.

  Unloads all mocked modules and cleans up resources.
  Call this in test cleanup/on_exit callbacks.
  Safe to call even if mocks aren't set up.
  """
  @spec cleanup_mocks() :: :ok
  def cleanup_mocks() do
    try do
      :meck.unload(:rabbit_feature_flags)
    rescue
      _ -> :ok
    end

    try do
      :meck.unload(:rabbit_khepri)
    rescue
      _ -> :ok
    end

    # Stop the Khepri storage agent if it exists
    if Process.whereis(:khepri_storage) do
      Agent.stop(:khepri_storage)
    end

    :ok
  end

  @doc """
  Check if the khepri_db feature flag is mocked as enabled.
  """
  @spec khepri_enabled?() :: boolean()
  def khepri_enabled?() do
    try do
      :rabbit_feature_flags.is_enabled(@khepri_db, :non_blocking) == true
    rescue
      _ -> false
    end
  end

  @doc """
  Ensure Mnesia backend mocks are set up for tests.

  Call this in test setup to ensure tests run against mocked Mnesia backend.
  """
  @spec ensure_mnesia_backend() :: :ok
  def ensure_mnesia_backend() do
    setup_mnesia_mocks()
    :ok
  end

  @doc """
  Ensure Khepri backend mocks are set up for tests.

  Call this in test setup to ensure tests run against mocked Khepri backend.
  """
  @spec ensure_khepri_backend() :: :ok
  def ensure_khepri_backend() do
    setup_khepri_mocks()
    :ok
  end

  @doc """
  Wait for feature flag state to stabilize.

  In mocked environment, this is a no-op that immediately returns :ok.
  """
  @spec wait_for_stable_state(keyword()) :: :ok
  def wait_for_stable_state(_opts \\ []) do
    :ok
  end
end
