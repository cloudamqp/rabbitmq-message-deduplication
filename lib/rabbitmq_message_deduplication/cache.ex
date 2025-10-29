# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Cache do
  @moduledoc """
  Simple cache implemented on top of Mnesia or Khepri.

  Entries can be stored within the cache with a given TTL.
  After the TTL expires the entrys will be transparently removed.

  When the cache is full, a random element is removed to make space to a new one.
  A FIFO approach would be preferrable but impractical by now.

  This module delegates to either CacheMnesia or CacheKhepri implementation
  based on whether the node is using Mnesia or Khepri for metadata storage.

  """

  alias RabbitMQMessageDeduplication.CacheMnesia
  alias RabbitMQMessageDeduplication.CacheKhepri

  @options [:size, :ttl, :distributed, :limit, :default_ttl]

  @doc """
  Create a new cache with the given name and options.

  A distributed cache is replicated across multiple nodes.

  """
  @spec create(atom, boolean, list) :: :ok | {:error, any}
  def create(cache, distributed, options) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.create(cache, distributed, options) end,
      khepri: fn -> CacheKhepri.create(cache, distributed, options) end
    })
  end

  @doc """
  Insert the given entry into the cache if it doesn't exist.
  The TTL controls the lifetime in milliseconds of the entry.

  If the cache is full, an entry will be removed to make space.

  """
  @spec insert(atom, any, integer | nil) ::
    {:ok, :inserted | :exists} | {:error, any}
  def insert(cache, entry, ttl \\ nil) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.insert(cache, entry, ttl) end,
      khepri: fn -> CacheKhepri.insert(cache, entry, ttl) end
    })
  end

  @doc """
  Delete the given entry from the cache.
  """
  @spec delete(atom, any) :: :ok | {:error, any}
  def delete(cache, entry) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.delete(cache, entry) end,
      khepri: fn -> CacheKhepri.delete(cache, entry) end
    })
  end

  @doc """
  Check whether the entry exists within the cache.
  """
  @spec exists?(atom, any) :: {:ok, boolean} | {:error, any}
  def exists?(cache, entry) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.exists?(cache, entry) end,
      khepri: fn -> CacheKhepri.exists?(cache, entry) end
    })
  end

  @doc """
  Flush the cache content.
  """
  @spec flush(atom) :: :ok | {:error, any}
  def flush(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.flush(cache) end,
      khepri: fn -> CacheKhepri.flush(cache) end
    })
  end

  @doc """
  Drop the cache with all its content.
  """
  @spec drop(atom) :: :ok | {:error, any}
  def drop(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.drop(cache) end,
      khepri: fn -> CacheKhepri.drop(cache) end
    })
  end

  @doc """
  Remove all entries which TTL has expired.
  """
  @spec delete_expired_entries(atom) :: :ok | {:error, any}
  def delete_expired_entries(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.delete_expired_entries(cache) end,
      khepri: fn -> CacheKhepri.delete_expired_entries(cache) end
    })
  end

  @doc """
  Return information related to the given cache.
  """
  @spec info(atom) :: list
  def info(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.info(cache) end,
      khepri: fn -> CacheKhepri.info(cache) end
    })
  end

  @doc """
  Rebalance cache replicas.
  """
  @spec rebalance_replicas(atom) :: any
  def rebalance_replicas(cache) do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.rebalance_replicas(cache) end,
      khepri: fn -> CacheKhepri.rebalance_replicas(cache) end
    })
  end

  @doc """
  Change cache options.
  """
  @spec change_option(atom, atom, any) :: :ok | {:error, any}
  def change_option(cache, option, value) when option in @options do
    :rabbit_khepri.handle_fallback(%{
      mnesia: fn -> CacheMnesia.change_option(cache, option, value) end,
      khepri: fn -> CacheKhepri.change_option(cache, option, value) end
    })
  end
  def change_option(_, option, _), do: {:error, {:invalid, option}}
end
