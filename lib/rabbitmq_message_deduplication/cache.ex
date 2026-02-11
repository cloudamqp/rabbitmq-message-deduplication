# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Cache do
  @moduledoc """
  Simple cache with dual backend support (Mnesia and Khepri).

  Entries can be stored within the cache with a given TTL.
  After the TTL expires the entries will be transparently removed.

  When the cache is full, a random element is removed to make space to a new one.
  A FIFO approach would be preferable but impractical by now due to backend limitations.

  This module delegates all operations to the DB abstraction layer which
  automatically switches between Mnesia and Khepri based on the khepri_db
  feature flag.
  """
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.DB, as: DB

  @doc """
  Create a new cache with the given name and options.

  A distributed cache is replicated across multiple nodes.

  """
  @spec create(atom, boolean, list) :: :ok | {:error, any}
  def create(cache, distributed, options) do
    Mnesia.start()

    DB.create_cache(cache, distributed, format_options(options))
  end

  @doc """
  Insert the given entry into the cache if it doesn't exist.
  The TTL controls the lifetime in milliseconds of the entry.

  If the cache is full, an entry will be removed to make space.

  """
  @spec insert(atom, any, integer | nil) :: {:ok, :inserted | :exists} | {:error, any}
  def insert(cache, entry, ttl \\ nil) do
    DB.insert(cache, entry, ttl)
  end

  @doc """
  Delete the given entry from the cache.
  """
  @spec delete(atom, any) :: :ok | {:error, any}
  def delete(cache, entry) do
    DB.delete(cache, entry)
  end

  @doc """
  Check whether the entry exists within the cache.
  """
  @spec exists?(atom, any) :: {:ok, boolean} | {:error, any}
  def exists?(cache, entry) do
    DB.exists?(cache, entry)
  end

  @doc """
  Flush the cache content.
  """
  @spec flush(atom) :: :ok | {:error, any}
  def flush(cache) do
    DB.flush(cache)
  end

  @doc """
  Drop the cache with all its content.
  """
  @spec drop(atom) :: :ok | {:error, any}
  def drop(cache) do
    DB.drop(cache)
  end

  @doc """
  Remove all entries which TTL has expired.
  """
  @spec delete_expired_entries(atom) :: :ok | {:error, any}
  def delete_expired_entries(cache) do
    DB.delete_expired_entries(cache)
  end

  @doc """
  Return information related to the given cache.
  """
  @spec info(atom) :: list
  def info(cache) do
    DB.info(cache)
  end

  @doc """
  Rebalance cache replicas.
  """
  @spec rebalance_replicas(atom) :: any
  def rebalance_replicas(cache) do
    DB.rebalance_replicas(cache)
  end

  @doc """
  Change cache options.
  """
  @spec change_option(atom, atom, any) :: :ok | {:error, any}
  def change_option(cache, option, value) do
    DB.change_option(cache, option, value)
  end

  ## Private Helpers

  # Convert keyword list options to map for DB module
  defp format_options(options) do
    %{
      size: Keyword.get(options, :size),
      ttl: Keyword.get(options, :ttl),
      persistence: Keyword.get(options, :persistence, :memory)
    }
  end
end
