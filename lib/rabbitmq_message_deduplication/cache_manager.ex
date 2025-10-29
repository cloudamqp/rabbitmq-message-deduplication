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

  # Start the cleanup routine
  def init(_state) do
    require Logger
    Process.send_after(self(), :cleanup, Common.cleanup_period())
    {:ok, %{}}
  end

  # Create the cache
  def handle_call({:create, cache, distributed, options}, _from, state) do
    case Cache.create(cache, distributed, options) do
      :ok ->
        {:reply, :ok, state}
      error ->
        {:reply, error, state}
    end
  end

  # Drop the cache
  def handle_call({:destroy, cache}, _from, state) do
    case Cache.drop(cache) do
      :ok ->
        {:reply, :ok, state}
      error ->
        {:reply, error, state}
    end
  end

  # The maintenance process deletes expired cache entries.
  # Note: We don't have a registry anymore, so we can't enumerate caches.
  # Cleanup will be triggered per-cache by the individual decorators.
  def handle_info(:cleanup, state) do
    Process.send_after(self(), :cleanup, Common.cleanup_period())
    {:noreply, state}
  end

  def handle_info(_event, state) do
    {:noreply, state}
  end

end
