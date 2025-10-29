# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Cache do
  @moduledoc """
  Simple cache implemented on top of Khepri.

  Entries can be stored within the cache with a given TTL.
  After the TTL expires the entrys will be transparently removed.

  When the cache is full, a random element is removed to make space to a new one.
  A FIFO approach would be preferrable but impractical by now.

  """
  alias :os, as: Os

  @options [:size, :ttl, :distributed, :limit, :default_ttl]

  # Khepri path structure:
  # [rabbitmq_message_deduplication, caches, cache_name, entries, entry_key] => %{expiration: timestamp}
  # [rabbitmq_message_deduplication, caches, cache_name, metadata] => %{size: N, ttl: ms, distributed: bool, ...}

  @doc """
  Create a new cache with the given name and options.

  A distributed cache is replicated across multiple nodes.

  """
  @spec create(atom, boolean, list) :: :ok | {:error, any}
  def create(cache, distributed, options) do
    store_id = get_store_id()
    metadata_path = [:rabbitmq_message_deduplication, :caches, cache, :metadata]

    metadata = %{
      distributed: distributed,
      size: Keyword.get(options, :size),
      ttl: Keyword.get(options, :ttl),
      persistence: Keyword.get(options, :persistence, :memory)
    }

    case :khepri.put(store_id, metadata_path, metadata) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Insert the given entry into the cache if it doesn't exist.
  The TTL controls the lifetime in milliseconds of the entry.

  If the cache is full, an entry will be removed to make space.
  """
  @spec insert(atom, any, integer | nil) ::
    {:ok, :inserted | :exists} | {:error, any}
  def insert(cache, entry, ttl \\ nil) do
    store_id = get_store_id()
    # Calculate current_time OUTSIDE transaction (Horus doesn't allow :os.system_time inside)
    current_time = :os.system_time(:millisecond)
    valid_entry = valid_khepri_entry(entry)

    # Transaction function must use only basic Erlang operations for Horus
    tx_fun = fn ->
      entries_path = [:rabbitmq_message_deduplication, :caches, cache, :entries]
      entry_path = [:rabbitmq_message_deduplication, :caches, cache, :entries, valid_entry]
      metadata_path = [:rabbitmq_message_deduplication, :caches, cache, :metadata]

      # Check if entry exists
      case :khepri_tx.get(entry_path) do
        {:ok, %{expiration: expiration}} when is_integer(expiration) ->
          # Entry exists, check if expired
          if expiration <= current_time do
            # Expired, delete and insert new
            :khepri_tx.delete(entry_path)

            # Check if cache is full before inserting
            case :khepri_tx.get(metadata_path) do
              {:ok, metadata} when is_map(metadata) ->
                max_size = :maps.get(:size, metadata, nil)

                if is_integer(max_size) do
                  case :khepri_tx.get_many(entries_path ++ [{:if_name_matches, :any, :undefined}]) do
                    {:ok, entry_map} when is_map(entry_map) ->
                      current_size = :maps.size(entry_map)

                      if current_size >= max_size do
                        # Delete first entry using :maps.keys
                        keys = :maps.keys(entry_map)
                        case keys do
                          [first_key | _] ->
                            :khepri_tx.delete(first_key)
                          [] -> :ok
                        end
                      end
                    _ -> :ok
                  end
                end
              _ -> :ok
            end

            # Calculate expiration and insert
            expiration_time = case :khepri_tx.get(metadata_path) do
              {:ok, metadata} when is_map(metadata) ->
                default_ttl = :maps.get(:ttl, metadata, nil)

                cond do
                  is_integer(ttl) -> current_time + ttl
                  is_integer(default_ttl) -> current_time + default_ttl
                  true -> nil
                end
              _ ->
                if is_integer(ttl), do: current_time + ttl, else: nil
            end

            :khepri_tx.put(entry_path, %{expiration: expiration_time})
            :inserted
          else
            # Not expired, entry exists
            :exists
          end

        {:ok, %{expiration: nil}} ->
          # Entry exists with no expiration
          :exists

        _ ->
          # Entry doesn't exist, insert it

          # Check if cache is full
          case :khepri_tx.get(metadata_path) do
            {:ok, metadata} when is_map(metadata) ->
              max_size = :maps.get(:size, metadata, nil)

              if is_integer(max_size) do
                case :khepri_tx.get_many(entries_path ++ [{:if_name_matches, :any, :undefined}]) do
                  {:ok, entry_map} when is_map(entry_map) ->
                    current_size = :maps.size(entry_map)

                    if current_size >= max_size do
                      # Delete first entry using :maps.keys
                      keys = :maps.keys(entry_map)
                      case keys do
                        [first_key | _] ->
                          :khepri_tx.delete(first_key)
                        [] -> :ok
                      end
                    end
                  _ -> :ok
                end
              end
            _ -> :ok
          end

          # Calculate expiration
          expiration_time = case :khepri_tx.get(metadata_path) do
            {:ok, metadata} when is_map(metadata) ->
              default_ttl = :maps.get(:ttl, metadata, nil)

              cond do
                is_integer(ttl) -> current_time + ttl
                is_integer(default_ttl) -> current_time + default_ttl
                true -> nil
              end
            _ ->
              if is_integer(ttl), do: current_time + ttl, else: nil
          end

          # Insert the entry
          :khepri_tx.put(entry_path, %{expiration: expiration_time})
          :inserted
      end
    end

    case :khepri.transaction(store_id, tx_fun, :rw) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Delete the given entry from the cache.
  """
  @spec delete(atom, any) :: :ok | {:error, any}
  def delete(cache, entry) do
    store_id = get_store_id()
    valid_entry = valid_khepri_entry(entry)
    entry_path = [:rabbitmq_message_deduplication, :caches, cache, :entries, valid_entry]

    case :khepri.delete(store_id, entry_path) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Check whether the entry exists within the cache.
  """
  @spec exists?(atom, any) :: {:ok, boolean} | {:error, any}
  def exists?(cache, entry) do
    store_id = get_store_id()
    valid_entry = valid_khepri_entry(entry)
    entry_path = [:rabbitmq_message_deduplication, :caches, cache, :entries, valid_entry]

    case :khepri.get(store_id, entry_path) do
      {:ok, %{expiration: expiration}} when is_integer(expiration) ->
        current_time = Os.system_time(:millisecond)
        if expiration <= current_time do
          # Expired, delete asynchronously
          :khepri.delete(store_id, entry_path)
          {:ok, false}
        else
          {:ok, true}
        end
      {:ok, %{expiration: nil}} ->
        {:ok, true}
      {:error, {:node_not_found, _}} ->
        {:ok, false}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Flush the cache content.
  """
  @spec flush(atom) :: :ok | {:error, any}
  def flush(cache) do
    store_id = get_store_id()
    entries_path = [:rabbitmq_message_deduplication, :caches, cache, :entries]

    case :khepri.delete_many(store_id, entries_path ++ [{:if_name_matches, :any, :undefined}]) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Drop the cache with all its content.
  """
  @spec drop(atom) :: :ok | {:error, any}
  def drop(cache) do
    store_id = get_store_id()
    cache_path = [:rabbitmq_message_deduplication, :caches, cache]

    case :khepri.delete(store_id, cache_path) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Remove all entries which TTL has expired.
  """
  @spec delete_expired_entries(atom) :: :ok | {:error, any}
  def delete_expired_entries(cache) do
    store_id = get_store_id()
    entries_path = [:rabbitmq_message_deduplication, :caches, cache, :entries]
    now = Os.system_time(:millisecond)

    case :khepri.get_many(store_id, entries_path ++ [{:if_name_matches, :any, :undefined}]) do
      {:ok, entries} when is_map(entries) ->
        # Filter and delete expired entries
        :maps.fold(
          fn path, value, acc ->
            case value do
              %{expiration: exp} when is_integer(exp) and exp <= now ->
                :khepri.delete(store_id, path)
              _ ->
                :ok
            end
            acc
          end,
          :ok,
          entries
        )
        :ok
      {:error, {:node_not_found, _}} ->
        {:error, :no_cache}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Return information related to the given cache.
  """
  @spec info(atom) :: list
  def info(cache) do
    store_id = get_store_id()
    metadata_path = [:rabbitmq_message_deduplication, :caches, cache, :metadata]

    # First check if cache exists by checking metadata
    case :khepri.get(store_id, metadata_path) do
      {:ok, metadata} when is_map(metadata) ->
        # Cache exists, now get entries count
        entries_path = [:rabbitmq_message_deduplication, :caches, cache, :entries]

        case :khepri.get_many(store_id, entries_path ++ [{:if_name_matches, :any, :undefined}]) do
          {:ok, entries} when is_map(entries) ->
            entry_count = :maps.size(entries)
            memory_bytes = entry_count * 100  # Rough estimate
            nodes = [Node.self()]

            case :maps.get(:size, metadata, nil) do
              size when is_integer(size) ->
                [entries: entry_count, bytes: memory_bytes, nodes: nodes, size: size]
              _ ->
                [entries: entry_count, bytes: memory_bytes, nodes: nodes]
            end
          _ ->
            # Metadata exists but no entries
            [entries: 0, bytes: 0, nodes: [Node.self()]]
        end
      {:error, {:node_not_found, _}} ->
        # Cache doesn't exist
        []
      _ ->
        []
    end
  end

  @doc """
  Rebalance cache replicas.
  """
  @spec rebalance_replicas(atom) :: any
  def rebalance_replicas(_cache) do
    # Khepri handles replication automatically
    :ok
  end

  @doc """
  Change cache options.
  """
  @spec change_option(atom, atom, any) :: :ok | {:error, any}
  def change_option(cache, option, value) when option in @options do
    store_id = get_store_id()
    metadata_path = [:rabbitmq_message_deduplication, :caches, cache, :metadata]

    tx_fun = fn ->
      case :khepri_tx.get(metadata_path) do
        {:ok, metadata} when is_map(metadata) ->
          updated = :maps.put(option, value, metadata)
          :khepri_tx.put(metadata_path, updated)
          :ok
        _ ->
          {:error, :no_cache}
      end
    end

    case :khepri.transaction(store_id, tx_fun, :rw) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end
  def change_option(_, option, _), do: {:error, {:invalid, option}}

  # Get the Khepri store ID
  defp get_store_id() do
    Application.get_env(:rabbitmq_message_deduplication, :khepri_store_id, :rabbitmq_metadata)
  end

  # Normalize entry key to be Khepri-compatible
  # Khepri path components must be atoms or binaries, not integers
  defp valid_khepri_entry(entry) when is_integer(entry), do: Integer.to_string(entry)
  defp valid_khepri_entry(entry) when is_binary(entry), do: entry
  defp valid_khepri_entry(entry) when is_atom(entry), do: entry
  defp valid_khepri_entry(entry), do: :erlang.term_to_binary(entry)
end
