# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Migration do
  @moduledoc """
  Migration utilities for moving message deduplication caches from Mnesia to Khepri.

  This module provides functions to migrate existing Mnesia-based caches to Khepri
  when the khepri_db feature flag is enabled.

  ## Usage

      # Migrate a specific cache
      Migration.migrate_cache(:my_cache)

      # Check if a cache needs migration
      Migration.needs_migration?(:my_cache)

  ## Migration Process

  1. Check if cache exists in Mnesia
  2. Read cache metadata (size, ttl, distributed, persistence)
  3. Read all cache entries with their expiration timestamps
  4. Create cache in Khepri with metadata
  5. Copy all non-expired entries to Khepri
  6. Optionally drop Mnesia cache after successful migration

  Note: Migration happens automatically when a cache is accessed after khepri_db
  feature flag is enabled, so manual migration is usually not necessary.
  """

  alias :mnesia, as: Mnesia
  alias :os, as: Os
  alias RabbitMQMessageDeduplication.DB, as: DB

  @doc """
  Check if a cache needs migration from Mnesia to Khepri.

  Returns `true` if:
  - khepri_db feature flag is enabled
  - Cache exists in Mnesia
  - Cache does not exist in Khepri

  Returns `false` otherwise.
  """
  @spec needs_migration?(atom) :: boolean
  def needs_migration?(cache) do
    khepri_enabled?() and mnesia_cache_exists?(cache) and not khepri_cache_exists?(cache)
  end

  @doc """
  Migrate a cache from Mnesia to Khepri.

  Options:
  - `:drop_mnesia` - Drop the Mnesia table after successful migration (default: false)

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  @spec migrate_cache(atom, keyword) :: :ok | {:error, any}
  def migrate_cache(cache, opts \\ []) do
    drop_mnesia = Keyword.get(opts, :drop_mnesia, false)

    cond do
      not khepri_enabled?() ->
        {:error, :khepri_not_enabled}

      not mnesia_cache_exists?(cache) ->
        {:error, :cache_not_found_in_mnesia}

      khepri_cache_exists?(cache) ->
        {:error, :cache_already_exists_in_khepri}

      true ->
        do_migrate_cache(cache, drop_mnesia)
    end
  end

  @doc """
  Migrate all message deduplication caches from Mnesia to Khepri.

  This function scans for all Mnesia tables that appear to be message deduplication
  caches and migrates them to Khepri.

  Options:
  - `:drop_mnesia` - Drop Mnesia tables after successful migration (default: false)

  Returns `{:ok, migrated_count, failed}` where:
  - `migrated_count` is the number of successfully migrated caches
  - `failed` is a list of `{cache_name, error_reason}` tuples for failed migrations
  """
  @spec migrate_all_caches(keyword) :: {:ok, integer, list}
  def migrate_all_caches(opts \\ []) do
    if khepri_enabled?() do
      caches = find_message_deduplication_caches()

      {migrated, failed} =
        Enum.reduce(caches, {0, []}, fn cache, {count, errors} ->
          case migrate_cache(cache, opts) do
            :ok ->
              {count + 1, errors}

            {:error, :cache_already_exists_in_khepri} ->
              # Already migrated, count as success
              {count + 1, errors}

            {:error, reason} ->
              {count, [{cache, reason} | errors]}
          end
        end)

      {:ok, migrated, Enum.reverse(failed)}
    else
      {:error, :khepri_not_enabled}
    end
  end

  ## Private Functions

  defp khepri_enabled?() do
    try do
      case :rabbit_feature_flags.is_enabled(:khepri_db, :non_blocking) do
        true -> true
        _ -> false
      end
    rescue
      _ -> false
    end
  end

  defp mnesia_cache_exists?(cache) do
    cache in Mnesia.system_info(:tables)
  end

  defp khepri_cache_exists?(cache) do
    metadata_path = [:rabbitmq, :message_deduplication, :cache, cache, :metadata]

    case :rabbit_khepri.get(metadata_path) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp find_message_deduplication_caches() do
    # Get all Mnesia tables
    tables = Mnesia.system_info(:tables)

    # Filter to find message deduplication caches
    # We identify them by checking if they have the expected structure
    Enum.filter(tables, fn table ->
      try do
        case Mnesia.table_info(table, :attributes) do
          [:entry, :expiration] ->
            # This looks like a message deduplication cache
            true

          _ ->
            false
        end
      rescue
        _ -> false
      end
    end)
  end

  defp do_migrate_cache(cache, drop_mnesia) do
    with {:ok, metadata} <- read_mnesia_metadata(cache),
         {:ok, entries} <- read_mnesia_entries(cache),
         :ok <- create_khepri_cache(cache, metadata),
         :ok <- copy_entries_to_khepri(cache, entries) do
      if drop_mnesia do
        case Mnesia.delete_table(cache) do
          {:atomic, :ok} -> :ok
          {:aborted, reason} -> {:error, {:failed_to_drop_mnesia, reason}}
        end
      else
        :ok
      end
    end
  end

  defp read_mnesia_metadata(cache) do
    try do
      distributed = cache |> Mnesia.table_info(:user_properties) |> Keyword.get(:distributed)
      size = cache |> Mnesia.table_info(:user_properties) |> Keyword.get(:size)
      ttl = cache |> Mnesia.table_info(:user_properties) |> Keyword.get(:ttl)

      # Determine persistence from storage type
      persistence =
        case Mnesia.table_info(cache, :ram_copies) do
          [] -> :disk
          _ -> :memory
        end

      metadata = %{
        distributed: distributed,
        size: size,
        ttl: ttl,
        persistence: persistence
      }

      {:ok, metadata}
    rescue
      error -> {:error, {:failed_to_read_metadata, error}}
    end
  end

  defp read_mnesia_entries(cache) do
    transaction = fn ->
      # Read all entries from the cache
      Mnesia.foldl(
        fn {^cache, entry, expiration}, acc ->
          # Only include non-expired entries
          if expiration == nil or expiration > Os.system_time(:millisecond) do
            [{entry, expiration} | acc]
          else
            acc
          end
        end,
        [],
        cache
      )
    end

    case Mnesia.transaction(transaction) do
      {:atomic, entries} -> {:ok, entries}
      {:aborted, reason} -> {:error, {:failed_to_read_entries, reason}}
    end
  end

  defp create_khepri_cache(cache, metadata) do
    distributed = Map.get(metadata, :distributed, false)
    DB.create_cache(cache, distributed, metadata)
  end

  defp copy_entries_to_khepri(cache, entries) do
    # Calculate TTL for each entry based on expiration
    results =
      Enum.map(entries, fn {entry, expiration} ->
        # For entries without expiration, use nil TTL
        # For entries with expiration, calculate remaining TTL
        ttl =
          if expiration == nil do
            nil
          else
            remaining = expiration - Os.system_time(:millisecond)
            max(remaining, 0)
          end

        # Insert directly into Khepri (bypass the exists check)
        insert_entry_directly(cache, entry, expiration)
      end)

    # Check if all inserts succeeded
    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      failed = Enum.filter(results, &(&1 != :ok))
      {:error, {:failed_to_copy_entries, failed}}
    end
  end

  defp insert_entry_directly(cache, entry, expiration) do
    # Directly insert into Khepri without going through the normal insert flow
    # This avoids the exists check and size limit enforcement during migration
    entry_path = [:rabbitmq, :message_deduplication, :cache, cache, :entries, entry]
    entry_value = %{expiration: expiration, data: entry}

    case :rabbit_khepri.put(entry_path, entry_value) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
