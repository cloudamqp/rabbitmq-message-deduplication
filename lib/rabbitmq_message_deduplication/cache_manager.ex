# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager do
  @moduledoc """
  The Cache Manager takes care of creating, maintaining and destroying caches.
  """

  use GenServer

  require RabbitMQMessageDeduplication.Cache

  alias :timer, as: Timer
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.Common, as: Common

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @rabbit_boot_step {
    __MODULE__,
    [description: "message deduplication plugin cache maintenance process",
     mfa: {:rabbit_sup, :start_child, [__MODULE__]},
     cleanup: {:rabbit_sup, :stop_child, [__MODULE__]},
     requires: :kernel_ready,
     enables: :recovery]}

  def start_link() do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Create the cache and register it within the maintenance process.
  """
  @spec create(atom, boolean, list) :: :ok | { :error, any }
  def create(cache, distributed, options) do
    require Logger

    try do
      timeout = Common.cache_wait_time() + Timer.seconds(5)
      GenServer.call(__MODULE__, {:create, cache, distributed, options}, timeout)
    catch
      :exit, {:noproc, info} ->
        Logger.error("CacheManager GenServer not found! Module: #{inspect(__MODULE__)}, Info: #{inspect(info)}")
        {:error, :noproc}
    end
  end

  @doc """
  Destroy the cache and remove it from the maintenance process.
  """
  @spec destroy(atom) :: :ok | { :error, any }
  def destroy(cache) do
    try do
      GenServer.call(__MODULE__, {:destroy, cache})
    catch
      :exit, {:noproc, _} -> {:error, :noproc}
    end
  end

  @doc """
  Disable the cache and terminate the manager process.
  """
  def disable() do
    :ok = Supervisor.terminate_child(:rabbit_sup, __MODULE__)
    :ok = Supervisor.delete_child(:rabbit_sup, __MODULE__)
  end

  ## Server Callbacks

  # Start the cleanup routine. Registry path will be created lazily on first use.
  def init(_state) do
    require Logger
    Process.send_after(self(), :cleanup, Common.cleanup_period())
    {:ok, %{registry_ensured: false}}
  end

  # Create the cache and add it to the registry
  def handle_call({:create, cache, distributed, options}, _from, state) do
    # Ensure the registry base path exists on first use
    state = ensure_registry_path(state)

    registry_path = cache_registry_path(cache)

    case Cache.create(cache, distributed, options) do
      :ok ->
        case khepri_put(registry_path, %{}) do
          {:ok, _} -> {:reply, :ok, state}
          :ok -> {:reply, :ok, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
      error ->
        {:reply, error, state}
    end
  end

  # Drop the cache and remove it from the registry
  def handle_call({:destroy, cache}, _from, state) do
    registry_path = cache_registry_path(cache)

    case Cache.drop(cache) do
      :ok ->
        case khepri_delete(registry_path) do
          :ok -> {:reply, :ok, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
      error ->
        {:reply, error, state}
    end
  end

  # The maintenance process deletes expired cache entries.
  def handle_info(:cleanup, state) do
    case khepri_get_all_caches() do
      {:ok, caches} ->
        Enum.each(caches, fn cache -> Cache.delete_expired_entries(cache) end)
      _ ->
        :ok
    end

    Process.send_after(self(), :cleanup, Common.cleanup_period())
    {:noreply, state}
  end

  def handle_info(_event, state) do
    {:noreply, state}
  end

  ## Utility functions

  # Ensure the registry base path exists (lazy initialization)
  defp ensure_registry_path(%{registry_ensured: true} = state), do: state
  defp ensure_registry_path(%{registry_ensured: false} = state) do
    registry_path = registry_base_path()
    case khepri_ensure_path(registry_path) do
      {:ok, _} -> %{state | registry_ensured: true}
      :ok -> %{state | registry_ensured: true}
      {:error, _} -> state  # Keep trying on next call
    end
  end

  # Khepri path helpers
  defp registry_base_path() do
    [:rabbitmq_message_deduplication, :cache_registry]
  end

  defp cache_registry_path(cache) do
    [:rabbitmq_message_deduplication, :cache_registry, cache]
  end

  # Khepri wrapper functions
  defp khepri_ensure_path(path) do
    store_id = get_store_id()
    case :khepri.put(store_id, path, %{}) do
      :ok -> {:ok, :ok}
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp khepri_put(path, data) do
    store_id = get_store_id()
    case :khepri.put(store_id, path, data) do
      :ok -> {:ok, :ok}
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp khepri_delete(path) do
    store_id = get_store_id()
    case :khepri.delete(store_id, path) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp khepri_get_all_caches() do
    store_id = get_store_id()
    registry_path = registry_base_path()

    case :khepri.get_many(store_id, registry_path ++ [{:if_name_matches, :any, :undefined}]) do
      {:ok, entries} ->
        # Extract cache names from the paths
        cache_names = entries
          |> Map.keys()
          |> Enum.map(fn path -> List.last(path) end)
        {:ok, cache_names}
      {:error, {:node_not_found, _}} ->
        {:ok, []}
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Get the Khepri store ID
  # In production, use rabbit's store. In tests, use a test store.
  defp get_store_id() do
    case Application.get_env(:rabbitmq_message_deduplication, :khepri_store_id) do
      nil -> :rabbitmq_metadata
      store_id -> store_id
    end
  end
end
