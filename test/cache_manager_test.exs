# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager.Test do
  use ExUnit.Case

  alias :timer, as: Timer
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.CacheManager, as: CacheManager

  setup do
    start_supervised!(%{id: :cache_manager,
                        start: {CacheManager,
                                :start_link,
                                []}})

    %{}
  end

  test "cache creation", %{} do
    options = [persistence: :memory]

    CacheManager.create(:cache, true, options)

    # Verify cache exists by checking info
    info = Cache.info(:cache)
    assert Keyword.has_key?(info, :entries)

    CacheManager.destroy(:cache)
  end

  test "cache deletion", %{} do
    options = [persistence: :memory]

    :ok = CacheManager.create(:cache, false, options)

    # Verify cache exists
    info = Cache.info(:cache)
    assert Keyword.has_key?(info, :entries)

    :ok = CacheManager.destroy(:cache)

    # Verify cache is deleted
    assert Cache.info(:cache) == []
  end

  test "cache cleanup routine", %{} do
    options = [persistence: :memory]

    :ok = CacheManager.create(:cache, true, options)

    {:ok, :inserted} = Cache.insert(:cache, "foo", 1000)

    Timer.sleep(3200)

    # Verify cache is empty after cleanup
    [entries: 0, bytes: _, nodes: _] = Cache.info(:cache)
    {:ok, :inserted} = Cache.insert(:cache, "foo")

    :ok = CacheManager.destroy(:cache)
  end

  def caches(), do: :message_deduplication_caches
end
