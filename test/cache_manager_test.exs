# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager.Test do
  use ExUnit.Case

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.CacheManager, as: CacheManager
  import RabbitMQMessageDeduplication.FeatureFlagHelpers

  # Shared setup for cache manager
  defp setup_cache_manager do
    start_supervised!(%{id: :cache_manager,
                        start: {CacheManager,
                                :start_link,
                                []}})

    on_exit fn ->
      cleanup_mocks()
    end

    %{}
  end

  def caches(), do: :message_deduplication_caches

  # Tests with Mnesia backend (khepri_db disabled)
  describe "with Mnesia backend" do
    setup do
      case ensure_mnesia_backend() do
        :ok -> setup_cache_manager()
        skip -> skip
      end
    end

    test "cache creation", %{} do
    options = [persistence: :memory]

    CacheManager.create(:cache, true, options)
    {:atomic, [:cache]} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
    CacheManager.destroy(:cache)
  end

  test "cache deletion", %{} do
    options = [persistence: :memory]

    :ok = CacheManager.create(:cache, false, options)
    {:atomic, [:cache]} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
    :ok = CacheManager.destroy(:cache)
    {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
  end

    test "cache cleanup routine", %{} do
      options = [persistence: :memory]

      :ok = CacheManager.create(:cache, true, options)

      {:ok, :inserted} = Cache.insert(:cache, "foo", 1000)

      Timer.sleep(3200)

      {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(:cache) end)
      {:ok, :inserted} = Cache.insert(:cache, "foo")

      :ok = CacheManager.destroy(:cache)
    end
  end

  # Tests with Khepri backend (khepri_db enabled)
  describe "with Khepri backend" do
    setup do
      case ensure_khepri_backend() do
        :ok ->
          :ok = wait_for_stable_state()
          setup_cache_manager()

        {:error, reason} ->
          {:skip, "Failed to enable khepri_db: #{inspect(reason)}"}
      end
    end

    test "cache creation", %{} do
      options = [persistence: :memory]

      CacheManager.create(:cache, true, options)
      # Note: Direct Mnesia check - will need backend-agnostic verification in Phase 3
      {:atomic, [:cache]} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
      CacheManager.destroy(:cache)
    end

    test "cache deletion", %{} do
      options = [persistence: :memory]

      :ok = CacheManager.create(:cache, false, options)
      # Note: Direct Mnesia checks - will need backend-agnostic verification in Phase 3
      {:atomic, [:cache]} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
      :ok = CacheManager.destroy(:cache)
      {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
    end

    test "cache cleanup routine", %{} do
      options = [persistence: :memory]

      :ok = CacheManager.create(:cache, true, options)

      {:ok, :inserted} = Cache.insert(:cache, "foo", 1000)

      Timer.sleep(3200)

      # Backend-agnostic check: verify cache is empty after cleanup
      info = Cache.info(:cache)
      assert Keyword.get(info, :entries) == 0
      {:ok, :inserted} = Cache.insert(:cache, "foo")

      :ok = CacheManager.destroy(:cache)
    end
  end
end
