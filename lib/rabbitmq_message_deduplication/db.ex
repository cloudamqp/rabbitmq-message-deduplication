# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.DB do
  @moduledoc """
  Database abstraction layer for message deduplication caches.

  Provides a unified interface for both Mnesia and Khepri storage backends.
  Uses `:rabbit_khepri.handle_fallback/1` to automatically switch between
  backends based on the khepri_db feature flag.
  """

  alias :os, as: Os
  alias :erlang, as: Erlang
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Common, as: Common

  @options [:size, :ttl, :distributed, :limit, :default_ttl]

  ## Public API

  @doc """
  Create a new cache with the given name and options.
  """
  @spec create_cache(atom, boolean, map) :: :ok | {:error, any}
  def create_cache(cache, distributed, options) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> create_cache_mnesia(cache, distributed, options) end,
      khepri: fn -> create_cache_khepri(cache, distributed, options) end
    })
  end

  defp create_cache_mnesia(cache, distributed, options) do
    persistence =
      case Map.get(options, :persistence) do
        :disk -> :disc_copies
        :memory -> :ram_copies
        _ -> :ram_copies
      end

    replicas = if distributed, do: cache_replicas(), else: [Node.self()]

    mnesia_options = [
      {:attributes, [:entry, :expiration]},
      {persistence, replicas},
      {:index, [:expiration]},
      {:user_properties,
       [
         {:distributed, distributed},
         {:size, Map.get(options, :size)},
         {:ttl, Map.get(options, :ttl)}
       ]}
    ]

    case Mnesia.create_table(cache, mnesia_options) do
      {:atomic, :ok} ->
        wait_for_cache(cache)

      {:aborted, reason} when elem(reason, 0) == :already_exists ->
        maybe_reconfigure(cache, distributed)

      error ->
        error
    end
  end

  defp create_cache_khepri(cache, distributed, options) do
    metadata = %{
      distributed: distributed,
      size: Map.get(options, :size),
      ttl: Map.get(options, :ttl),
      persistence: Map.get(options, :persistence, :memory)
    }

    path = khepri_cache_metadata_path(cache)

    case :rabbit_khepri.put(path, metadata) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Insert an entry into the cache if it doesn't exist.
  """
  @spec insert(atom, any, integer | nil) :: {:ok, :inserted | :exists} | {:error, any}
  def insert(cache, entry, ttl \\ nil) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> insert_mnesia(cache, entry, ttl) end,
      khepri: fn -> insert_khepri(cache, entry, ttl) end
    })
  end

  defp insert_mnesia(cache, entry, ttl) do
    function = fn ->
      if cache_member?(cache, entry) do
        :exists
      else
        if cache_full?(cache) do
          cache_delete_first(cache)
        end

        Mnesia.write({cache, entry, entry_expiration(cache, ttl)})

        :inserted
      end
    end

    case Mnesia.transaction(function) do
      {:atomic, result} -> {:ok, result}
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp insert_khepri(cache, entry, ttl) do
    insert_khepri_with_retry(cache, entry, ttl, 10)
  end

  defp insert_khepri_with_retry(cache, entry, ttl, retries) when retries > 0 do
    entry_path = khepri_cache_entry_path(cache, entry)

    # Check if entry already exists
    case :rabbit_khepri.get(entry_path) do
      {:ok, %{expiration: exp}} when is_integer(exp) ->
        if exp > Os.system_time(:millisecond) do
          {:ok, :exists}
        else
          # Expired, proceed to insert
          do_insert_khepri(cache, entry, ttl, retries)
        end

      {:ok, %{expiration: nil}} ->
        {:ok, :exists}

      _ ->
        # Doesn't exist, proceed to insert
        do_insert_khepri(cache, entry, ttl, retries)
    end
  end

  defp insert_khepri_with_retry(_cache, _entry, _ttl, 0) do
    {:error, :too_many_retries}
  end

  defp do_insert_khepri(cache, entry, ttl, retries) do
    # Check if cache is full
    entries_path = khepri_cache_entries_path(cache)

    case :rabbit_khepri.get_many(entries_path ++ [:khepri_wildcard_star]) do
      {:ok, entries_map} ->
        metadata_path = khepri_cache_metadata_path(cache)

        case :rabbit_khepri.get(metadata_path) do
          {:ok, metadata} ->
            size_limit = Map.get(metadata, :size)
            current_size = map_size(entries_map)

            # If full, delete a random entry
            if size_limit != nil and current_size >= size_limit do
              delete_random_entry_khepri(entries_map)
            end

            # Insert the new entry
            expiration = calculate_expiration_khepri(metadata, ttl)
            entry_value = %{expiration: expiration, data: entry}
            entry_path = khepri_cache_entry_path(cache, entry)

            case :rabbit_khepri.put(entry_path, entry_value) do
              :ok ->
                {:ok, :inserted}

              {:error, {:khepri, :mismatching_node, _}} ->
                insert_khepri_with_retry(cache, entry, ttl, retries - 1)

              {:error, _} = error ->
                error
            end

          {:error, _} = error ->
            error
        end

      _ ->
        # No entries yet, just insert
        metadata_path = khepri_cache_metadata_path(cache)

        case :rabbit_khepri.get(metadata_path) do
          {:ok, metadata} ->
            expiration = calculate_expiration_khepri(metadata, ttl)
            entry_value = %{expiration: expiration, data: entry}
            entry_path = khepri_cache_entry_path(cache, entry)
            :rabbit_khepri.put(entry_path, entry_value)
            {:ok, :inserted}

          {:error, _} = error ->
            error
        end
    end
  end

  @doc """
  Delete an entry from the cache.
  """
  @spec delete(atom, any) :: :ok | {:error, any}
  def delete(cache, entry) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> delete_mnesia(cache, entry) end,
      khepri: fn -> delete_khepri(cache, entry) end
    })
  end

  defp delete_mnesia(cache, entry) do
    case Mnesia.transaction(fn -> Mnesia.delete({cache, entry}) end) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp delete_khepri(cache, entry) do
    path = khepri_cache_entry_path(cache, entry)

    case :rabbit_khepri.delete(path) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Check whether an entry exists in the cache.
  """
  @spec exists?(atom, any) :: {:ok, boolean} | {:error, any}
  def exists?(cache, entry) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> exists_mnesia?(cache, entry) end,
      khepri: fn -> exists_khepri?(cache, entry) end
    })
  end

  defp exists_mnesia?(cache, entry) do
    case Mnesia.transaction(fn -> cache_member?(cache, entry) end) do
      {:atomic, result} -> {:ok, result}
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp exists_khepri?(cache, entry) do
    path = khepri_cache_entry_path(cache, entry)

    case :rabbit_khepri.get(path) do
      {:ok, %{expiration: exp}} when is_integer(exp) ->
        if exp > Os.system_time(:millisecond) do
          {:ok, true}
        else
          # Expired, delete it and return false
          :rabbit_khepri.delete(path)
          {:ok, false}
        end

      {:ok, %{expiration: nil}} ->
        {:ok, true}

      _ ->
        {:ok, false}
    end
  end

  @doc """
  Flush all entries from the cache.
  """
  @spec flush(atom) :: :ok | {:error, any}
  def flush(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> flush_mnesia(cache) end,
      khepri: fn -> flush_khepri(cache) end
    })
  end

  defp flush_mnesia(cache) do
    case Mnesia.clear_table(cache) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp flush_khepri(cache) do
    # Delete all entries but keep metadata
    path = khepri_cache_entries_path(cache) ++ [:khepri_wildcard_star]

    case :rabbit_khepri.delete_many(path) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Drop the cache entirely.
  """
  @spec drop(atom) :: :ok | {:error, any}
  def drop(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> drop_mnesia(cache) end,
      khepri: fn -> drop_khepri(cache) end
    })
  end

  defp drop_mnesia(cache) do
    case Mnesia.delete_table(cache) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp drop_khepri(cache) do
    # Delete entire cache (metadata + entries)
    path = [:rabbitmq, :message_deduplication, :cache, cache]

    case :rabbit_khepri.delete(path) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Delete all expired entries from the cache.
  """
  @spec delete_expired_entries(atom) :: :ok | {:error, any}
  def delete_expired_entries(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> delete_expired_entries_mnesia(cache) end,
      khepri: fn -> delete_expired_entries_khepri(cache) end
    })
  end

  defp delete_expired_entries_mnesia(cache) do
    select = fn ->
      Mnesia.select(cache, [
        {{cache, :"$1", :"$2"}, [{:>, Os.system_time(:millisecond), :"$2"}], [:"$1"]}
      ])
    end

    delete = fn x -> Enum.each(x, fn e -> Mnesia.delete({cache, e}) end) end

    case Mnesia.transaction(select) do
      {:atomic, expired} ->
        case Mnesia.transaction(delete, [expired], 1) do
          {:atomic, :ok} -> :ok
          {:aborted, reason} -> {:error, reason}
        end

      {:aborted, {:no_exists, _}} ->
        {:error, :no_cache}
    end
  end

  defp delete_expired_entries_khepri(cache) do
    entries_path = khepri_cache_entries_path(cache) ++ [:khepri_wildcard_star]
    now = Os.system_time(:millisecond)

    case :rabbit_khepri.get_many(entries_path) do
      {:ok, entries_map} ->
        expired_paths =
          entries_map
          |> Enum.filter(fn {_path, entry_value} ->
            case entry_value do
              %{expiration: exp} when is_integer(exp) -> exp < now
              _ -> false
            end
          end)
          |> Enum.map(fn {path, _} -> path end)

        # Delete all expired entries
        Enum.each(expired_paths, fn path ->
          :rabbit_khepri.delete(path)
        end)

        :ok

      {:error, {:khepri, :no_data, _}} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Get information about the cache.
  """
  @spec info(atom) :: list
  def info(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> info_mnesia(cache) end,
      khepri: fn -> info_khepri(cache) end
    })
  end

  defp info_mnesia(cache) do
    with entries when is_integer(entries) <- Mnesia.table_info(cache, :size),
         words when is_integer(words) <- Mnesia.table_info(cache, :memory) do
      {_, nodes} = cache_layout(cache)
      bytes = words * Erlang.system_info(:wordsize)

      case cache_property(cache, :size) do
        nil -> [entries: entries, bytes: bytes, nodes: nodes]
        size -> [entries: entries, bytes: bytes, nodes: nodes, size: size]
      end
    else
      :undefined -> []
    end
  end

  defp info_khepri(cache) do
    metadata_path = khepri_cache_metadata_path(cache)
    entries_path = khepri_cache_entries_path(cache) ++ [:khepri_wildcard_star]

    case :rabbit_khepri.get(metadata_path) do
      {:ok, metadata} ->
        # Get entries, handling the case where there are no entries
        entries_count = case :rabbit_khepri.get_many(entries_path) do
          {:ok, entries_map} -> map_size(entries_map)
          {:error, {:khepri, :no_data, _}} -> 0
          _ -> 0
        end

        size_limit = Map.get(metadata, :size)

        # Estimate bytes (rough approximation for compatibility)
        # Each entry value is approximately 100-200 bytes
        estimated_bytes = entries_count * 150

        info = [entries: entries_count, bytes: estimated_bytes, nodes: [Node.self()]]

        if size_limit != nil do
          info ++ [size: size_limit]
        else
          info
        end

      _ ->
        []
    end
  end

  @doc """
  Rebalance cache replicas across cluster nodes.
  """
  @spec rebalance_replicas(atom) :: any
  def rebalance_replicas(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> rebalance_replicas_mnesia(cache) end,
      khepri: fn -> rebalance_replicas_khepri(cache) end
    })
  end

  defp rebalance_replicas_mnesia(cache) do
    if cache_property(cache, :distributed) do
      cache_rebalance(cache)
    end
  end

  defp rebalance_replicas_khepri(_cache) do
    # Khepri handles replication automatically via Raft
    :ok
  end

  @doc """
  Change a cache option.
  """
  @spec change_option(atom, atom, any) :: :ok | {:error, any}
  def change_option(cache, option, value) when option in @options do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> change_option_mnesia(cache, option, value) end,
      khepri: fn -> change_option_khepri(cache, option, value) end
    })
  end

  def change_option(_cache, option, _value), do: {:error, {:invalid, option}}

  defp change_option_mnesia(cache, option, value) when option in @options do
    :ok = cache_property(cache, option, value)
  end

  defp change_option_khepri(cache, option, value) when option in @options do
    metadata_path = khepri_cache_metadata_path(cache)

    case :rabbit_khepri.get(metadata_path) do
      {:ok, metadata} ->
        updated_metadata = Map.put(metadata, option, value)
        :rabbit_khepri.put(metadata_path, updated_metadata)

      {:error, _} = error ->
        error
    end
  end

  ## Shared Helper Functions

  # Mnesia helpers
  defp wait_for_cache(cache) do
    case Mnesia.wait_for_tables([cache], Common.cache_wait_time()) do
      {:timeout, [^cache]} -> Mnesia.force_load_table(cache)
      result -> result
    end
  end

  defp cache_member?(cache, entry) do
    case cache |> Mnesia.read(entry) |> List.keyfind(entry, 1) do
      {_, _, expiration} ->
        if expiration <= Os.system_time(:millisecond) do
          Mnesia.delete({cache, entry})
          false
        else
          true
        end

      nil ->
        false
    end
  end

  defp cache_delete_first(cache) do
    Mnesia.delete({cache, Mnesia.first(cache)})
  end

  defp cache_full?(cache) do
    Mnesia.table_info(cache, :size) >= cache_property(cache, :size)
  end

  defp entry_expiration(cache, ttl) do
    default = cache_property(cache, :ttl)

    cond do
      ttl != nil -> Os.system_time(:millisecond) + ttl
      default != nil -> Os.system_time(:millisecond) + default
      true -> nil
    end
  end

  defp cache_property(cache, property) do
    cache |> Mnesia.table_info(:user_properties) |> Keyword.get(property)
  end

  defp cache_property(cache, property, value) when property in @options do
    case Mnesia.write_table_property(cache, {property, value}) do
      {:atomic, :ok} -> :ok
      {:aborted, error} -> {:error, error}
    end
  end

  defp cache_rebalance(cache) do
    {storage_type, cache_nodes} = cache_layout(cache)

    for node <- cache_replicas(cache_nodes) do
      case Mnesia.add_table_copy(cache, node, storage_type) do
        {:atomic, :ok} ->
          wait_for_cache(cache)

        {:aborted, reason} when elem(reason, 0) == :already_exists ->
          maybe_reconfigure(cache, true)
      end
    end
  end

  defp cache_replicas(cache_nodes \\ []) do
    cluster_nodes = Mnesia.system_info(:running_db_nodes)
    replica_number = floor(length(cluster_nodes) * 2 / 3)

    Enum.take(cache_nodes ++ (cluster_nodes -- cache_nodes), replica_number)
  end

  defp cache_layout(cache) do
    case Mnesia.table_info(cache, :ram_copies) do
      [] -> {:disc_copies, Mnesia.table_info(cache, :disc_copies)}
      nodes -> {:ram_copies, nodes}
    end
  end

  defp maybe_reconfigure(cache, distributed) do
    if cache_property(cache, :distributed) == nil do
      cache_property(cache, :distributed, distributed)
      cache_property(cache, :size, cache_property(cache, :limit))
      cache_property(cache, :ttl, cache_property(cache, :default_ttl))

      Mnesia.delete_table_property(cache, :limit)
      Mnesia.delete_table_property(cache, :default_ttl)
    end

    wait_for_cache(cache)
  end

  # Khepri helpers
  defp khepri_cache_metadata_path(cache) do
    [:rabbitmq, :message_deduplication, :cache, cache, :metadata]
  end

  defp khepri_cache_entries_path(cache) do
    [:rabbitmq, :message_deduplication, :cache, cache, :entries]
  end

  defp khepri_cache_entry_path(cache, entry) do
    [:rabbitmq, :message_deduplication, :cache, cache, :entries, entry]
  end

  defp calculate_expiration_khepri(metadata, ttl) do
    default = Map.get(metadata, :ttl)

    cond do
      ttl != nil -> Os.system_time(:millisecond) + ttl
      default != nil -> Os.system_time(:millisecond) + default
      true -> nil
    end
  end

  defp delete_random_entry_khepri(entries_map) do
    if map_size(entries_map) > 0 do
      # Get a random entry key
      keys = Map.keys(entries_map)
      random_key = Enum.random(keys)

      # Extract cache name and entry from the path
      case random_key do
        [:rabbitmq, :message_deduplication, :cache, cache, :entries, entry] ->
          :rabbit_khepri.delete(khepri_cache_entry_path(cache, entry))

        _ ->
          :ok
      end
    end
  end
end
