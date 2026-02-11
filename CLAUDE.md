# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a RabbitMQ plugin written in Elixir that provides message deduplication functionality. It supports both exchange-level and queue-level deduplication using a custom Mnesia-based cache system.

**Important:** This is a RabbitMQ plugin, NOT a standard Elixir application. It uses a hybrid build system combining Erlang's `erlang.mk` (via Make) and Elixir's Mix.

## Build System

The project uses Make as the primary build interface, which internally delegates to Mix:

- **Build the plugin:** `make dist` - Compiles and creates `.ez` archive files in the `plugins/` directory
- **Compile code:** `make app` - Runs `mix make_app` (deps.get, deps.compile, compile)
- **Run tests:** `make tests` - Runs `mix make_tests` (deps.get, test)
- **Clean build artifacts:** `make clean` - Removes `_build/` directory

### Mix Commands (for development)

- `mix compile` - Compile the project
- `mix test` - Run all tests (automatically runs with `--no-start` flag)
- `mix test test/cache_test.exs` - Run a single test file
- `MIX_ENV=dev mix compile` - Compile in dev environment (default)
- `MIX_ENV=prod mix compile` - Compile for production (enforces broker version requirements)

## Architecture

### Core Components

1. **Cache System** (`lib/rabbitmq_message_deduplication/cache.ex`)
   - Dual-backend cache system supporting both Mnesia and Khepri storage
   - Automatically switches between backends based on `khepri_db` feature flag
   - Handles distributed caching across RabbitMQ cluster nodes
   - Supports both disk and memory persistence
   - Automatically evicts expired entries and manages cache size limits
   - Delegates all operations to the DB abstraction layer

2. **CacheManager** (`lib/rabbitmq_message_deduplication/cache_manager.ex`)
   - GenServer that manages cache lifecycle
   - Runs periodic cleanup of expired cache entries (default: every 3 seconds)
   - Handles cache rebalancing when nodes join the cluster
   - Registered as a RabbitMQ boot step dependency

3. **Exchange Implementation** (`lib/rabbitmq_message_deduplication/rabbit_message_deduplication_exchange.ex`)
   - Implements `rabbit_exchange_type` behavior
   - Exchange type: `x-message-deduplication`
   - Acts like a fanout exchange (no routing key matching)
   - Creates distributed caches (replicated across 2/3 of cluster nodes)
   - Supports dynamic policy changes via `policy_changed/2`

4. **Queue Implementation** (`lib/rabbitmq_message_deduplication/rabbit_message_deduplication_queue.ex`)
   - Implements `rabbit_backing_queue` behavior
   - Wraps the actual backing queue module using passthrough macros
   - Creates non-distributed caches (local to queue)
   - Uses custom `dqack` records to track deduplication headers during message acknowledgment
   - Handles both immediate delivery and queued message scenarios

5. **Common Utilities** (`lib/rabbitmq_message_deduplication/common.ex`)
   - Shared functions for parsing RabbitMQ arguments and message headers
   - Cache name sanitization (converts resource names to valid atom names)
   - Configuration helpers for timeouts and cleanup periods

6. **Database Abstraction Layer** (`lib/rabbitmq_message_deduplication/db.ex`)
   - Unified interface for both Mnesia and Khepri storage backends
   - Uses `:rabbit_khepri.handle_fallback/1` to automatically switch between backends
   - All cache operations implemented for both backends
   - Khepri support enables better consistency and cluster management

7. **Migration Utilities** (`lib/rabbitmq_message_deduplication/migration.ex`)
   - Helper functions for migrating caches from Mnesia to Khepri
   - Auto-discovery of message deduplication caches
   - Preserves cache metadata and non-expired entries during migration
   - Optional Mnesia cleanup after successful migration

### Storage Backend Architecture

The plugin now supports dual storage backends:

**Mnesia Backend (default for RabbitMQ < 4.0):**
- Traditional Erlang distributed database
- One Mnesia table per cache with attributes: `[:entry, :expiration]`
- Cache metadata stored in table `user_properties`
- Manual replica management for distributed caches (2/3 of cluster nodes)
- Secondary index on `:expiration` for efficient cleanup

**Khepri Backend (when `khepri_db` feature flag enabled):**
- Raft-based distributed key-value store
- Hierarchical path structure:
  ```
  [:rabbitmq, :message_deduplication, :cache, cache_name, :metadata] → cache options
  [:rabbitmq, :message_deduplication, :cache, cache_name, :entries, entry] → entry data
  ```
- Entry format: `%{expiration: timestamp | nil, data: entry}`
- Metadata format: `%{size: int, ttl: int, distributed: bool, persistence: atom}`
- Automatic replication via Raft consensus (no manual replica management)
- Optimistic locking with retry for concurrent inserts
- Check-on-read TTL enforcement (expired entries deleted automatically)

**Backend Selection:**
- Determined by RabbitMQ's `khepri_db` feature flag at runtime
- Uses `:rabbit_khepri.handle_fallback/1` for automatic dispatch
- Zero API changes - backends are transparent to callers
- Migration utilities provided for existing deployments

### Key Design Patterns

- **Passthrough Macros:** Queue implementation uses macros (`passthrough`, `passthrough1`, `passthrough2`, `passthrough3`) to delegate to the underlying backing queue while maintaining deduplication state
- **Elixir Records:** Uses Erlang-style records via `defrecord` for interop with RabbitMQ's Erlang codebase
- **Boot Steps:** Components register as RabbitMQ boot steps via module attributes (`@rabbit_boot_step`)
- **Version Compatibility:** Multiple function clauses handle differences between RabbitMQ 3.13.x and 4.x APIs

### Deduplication Flow

**Exchange-level:**
1. Message arrives with `x-deduplication-header`
2. Header value checked against distributed cache
3. If not found: message routed, header cached with optional TTL
4. If found: message dropped (not routed)

**Queue-level:**
1. Message arrives with `x-deduplication-header`
2. Header value checked against local cache
3. If not found: message enqueued, header cached
4. On message acknowledgment/drop: header removed from cache
5. On queue purge: entire cache flushed

## Testing

Tests are located in `test/` directory:

**ExUnit Tests (run standalone with mocks):**
- `cache_test.exs` - Tests for the Cache module (runs against both Mnesia and Khepri backends using mocked feature flags)
- `cache_manager_test.exs` - Tests for the CacheManager GenServer (dual-backend support with mocks)
- `policies_test.exs` - Tests for policy handling
- `test_helper.exs` - ExUnit setup
- `test/support/feature_flag_helpers.ex` - Mocking infrastructure for `:rabbit_feature_flags` and `:rabbit_khepri` modules

**Common Test Suites (require running RabbitMQ broker):**
- `exchange_SUITE.erl` - Integration tests for exchange-level deduplication
- `queue_SUITE.erl` - Integration tests for queue-level deduplication
- `migration_SUITE.erl` - Tests for Mnesia to Khepri migration with actual feature flag transitions

**Running Tests:**
- ExUnit tests (standalone): `MIX_ENV=test mix test test/cache_test.exs`
- Common Test (in RabbitMQ context): `make tests` (requires RabbitMQ server repo)

The ExUnit suite does NOT start the application (configured via `test: "test --no-start"` alias) and uses `:meck` for mocking RabbitMQ modules. Common Test suites run in a full RabbitMQ broker environment via the `rabbit_ct_helpers` framework.

## Version Support

- Current version (0.7.3) requires RabbitMQ 3.13.0+
- Broker version requirements enforced only in `MIX_ENV=prod`
- Multiple function clauses handle API changes between RabbitMQ versions (search for `# v3.13.x` comments)

## Development Notes

- **MIX_ENV:** Use `dev` for development (default), `prod` for releases
- **Git branch:** Current work is on `khepri_support` branch (main branch not set)
- **Dependencies:** The plugin depends on `rabbit` application and various RabbitMQ libraries loaded via `erlang.mk`
- **Cache reconfiguration:** Both Exchange and Queue modules include `maybe_reconfigure_caches/0` functions to handle upgrades from versions prior to 0.6.0
- **Storage systems:**
  - Mnesia backend: Caches stored as Mnesia tables with custom user properties (`:size`, `:ttl`, `:distributed`)
  - Khepri backend: Caches stored in hierarchical Khepri paths under `[:rabbitmq, :message_deduplication, :cache]`
  - Backend selection automatic via `khepri_db` feature flag
- **Testing with backends:** Tests in `test/cache_test.exs` and `test/cache_manager_test.exs` run against both Mnesia and Khepri backends using ExUnit `describe` blocks
- **Migration:** Use `Migration.migrate_cache/1` or `Migration.migrate_all_caches/0` to move existing Mnesia caches to Khepri

## Common Gotchas

1. **Atom generation:** Cache names are dynamically created atoms from resource names - see `Common.cache_name/1` for sanitization
2. **TTL handling:** Supports both exchange/queue-level default TTL and per-message TTL via `x-cache-ttl` header
3. **Message expiration:** Queue-level deduplication respects message TTL from `MC.ttl(message)` for cache entry expiration
4. **Distributed vs local caches:** Exchange caches are distributed, queue caches are local
5. **Backing queue wrapping:** Queue implementation must handle both old and new RabbitMQ APIs with version-specific function clauses
6. **Backend differences:**
   - Mnesia: Manual replica management, secondary indexes, immediate consistency
   - Khepri: Automatic Raft replication, hierarchical paths, eventual consistency
   - Both backends behave identically from the API perspective
7. **Migration:** Migrating from Mnesia to Khepri preserves all non-expired entries and cache metadata. Consider running migration during low-traffic periods.
