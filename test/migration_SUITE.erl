% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2025, Matteo Cafasso.
% All rights reserved.

-module(migration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
     {group, mnesia_to_khepri_migration}
    ].

groups() ->
    [
     {mnesia_to_khepri_migration, [], [
                                       migration_preserves_cache_data,
                                       migration_handles_expired_entries,
                                       migration_preserves_cache_options,
                                       operations_work_after_migration
                                      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),

    % Start broker without khepri_db enabled (Mnesia mode)
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
                                                rabbit_ct_broker_helpers:setup_steps() ++
                                                rabbit_ct_client_helpers:setup_steps()),

    % Verify khepri_db is NOT enabled
    false = rabbit_ct_broker_helpers:is_feature_flag_enabled(Config2, 0, khepri_db),

    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config, rabbit_ct_client_helpers:teardown_steps() ++
          rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    % Clean up test exchanges/queues
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    catch amqp_channel:call(Channel, #'exchange.delete'{exchange = <<"migration_test_exchange">>}),
    catch amqp_channel:call(Channel, #'queue.delete'{queue = <<"migration_test_queue">>}),

    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Helper Functions
%% -------------------------------------------------------------------

% Create exchange with deduplication cache
create_dedup_exchange(Channel, Name, CacheSize, CacheTTL) ->
    Declare = #'exchange.declare'{
        exchange = Name,
        type = <<"x-message-deduplication">>,
        auto_delete = false,
        arguments = [
            {<<"x-cache-size">>, long, CacheSize},
            {<<"x-cache-ttl">>, long, CacheTTL},
            {<<"x-cache-persistence">>, longstr, <<"memory">>}
        ]
    },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare).

% Create queue with deduplication
create_dedup_queue(Channel, Name, CacheSize) ->
    Declare = #'queue.declare'{
        queue = Name,
        auto_delete = false,
        arguments = [
            {<<"x-message-deduplication">>, bool, true},
            {<<"x-cache-size">>, long, CacheSize},
            {<<"x-cache-persistence">>, longstr, <<"memory">>}
        ]
    },
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare).

% Publish message with deduplication header
publish_message(Channel, Exchange, RoutingKey, DedupHeader, Payload) ->
    Props = #'P_basic'{
        headers = [{<<"x-deduplication-header">>, longstr, DedupHeader}]
    },
    Publish = #'basic.publish'{
        exchange = Exchange,
        routing_key = RoutingKey
    },
    Msg = #amqp_msg{props = Props, payload = Payload},
    amqp_channel:cast(Channel, Publish, Msg).

% Get cache info via RabbitMQ management
get_cache_info(Config, CacheName) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    CacheAtom = binary_to_atom(CacheName, utf8),
    rpc:call(Node, 'Elixir.RabbitMQMessageDeduplication.Cache', info, [CacheAtom]).

% Enable khepri_db feature flag
enable_khepri_db(Config) ->
    rabbit_ct_broker_helpers:enable_feature_flag(Config, 0, khepri_db).

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

migration_preserves_cache_data(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),
    ExchangeName = <<"migration_test_exchange">>,

    % Phase 1: Create exchange with Mnesia backend
    create_dedup_exchange(Channel, ExchangeName, 100, 60000),

    % Bind a queue to receive messages
    QueueName = <<"migration_test_queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(
        Channel,
        #'queue.declare'{queue = QueueName, auto_delete = false}
    ),
    #'queue.bind_ok'{} = amqp_channel:call(
        Channel,
        #'queue.bind'{queue = QueueName, exchange = ExchangeName}
    ),

    % Publish messages with deduplication headers
    publish_message(Channel, ExchangeName, <<>>, <<"msg1">>, <<"Message 1">>),
    publish_message(Channel, ExchangeName, <<>>, <<"msg2">>, <<"Message 2">>),
    publish_message(Channel, ExchangeName, <<>>, <<"msg3">>, <<"Message 3">>),

    % Duplicate should be ignored (still in Mnesia)
    publish_message(Channel, ExchangeName, <<>>, <<"msg1">>, <<"Duplicate">>),

    timer:sleep(100), % Allow messages to be processed

    % Verify 3 messages in queue (4th was duplicate)
    #'queue.declare_ok'{message_count = 3} = amqp_channel:call(
        Channel,
        #'queue.declare'{queue = QueueName, passive = true}
    ),

    % Get cache name and verify entries in Mnesia
    CacheName = <<"x_message_deduplication_x2F_migration_test_exchange">>,
    InfoBefore = get_cache_info(Config, CacheName),
    ct:pal("Cache info before migration: ~p", [InfoBefore]),
    3 = proplists:get_value(entries, InfoBefore),

    % Phase 2: Enable khepri_db (triggers migration)
    ok = enable_khepri_db(Config),

    % Wait for migration to stabilize
    timer:sleep(2000),

    % Phase 3: Verify data in Khepri
    InfoAfter = get_cache_info(Config, CacheName),
    ct:pal("Cache info after migration: ~p", [InfoAfter]),
    3 = proplists:get_value(entries, InfoAfter),

    % Verify deduplication still works (using Khepri now)
    publish_message(Channel, ExchangeName, <<>>, <<"msg1">>, <<"Still duplicate">>),
    publish_message(Channel, ExchangeName, <<>>, <<"msg4">>, <<"New message">>),

    timer:sleep(100),

    % Should have 4 messages total (msg4 added, duplicate ignored)
    #'queue.declare_ok'{message_count = 4} = amqp_channel:call(
        Channel,
        #'queue.declare'{queue = QueueName, passive = true}
    ),

    % Cache should have 4 entries
    InfoFinal = get_cache_info(Config, CacheName),
    ct:pal("Cache info after new messages: ~p", [InfoFinal]),
    4 = proplists:get_value(entries, InfoFinal),

    ok.

migration_handles_expired_entries(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),
    ExchangeName = <<"migration_test_exchange">>,

    % Create exchange with 2 second TTL
    create_dedup_exchange(Channel, ExchangeName, 100, 2000),

    QueueName = <<"migration_test_queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(
        Channel,
        #'queue.declare'{queue = QueueName, auto_delete = false}
    ),
    #'queue.bind_ok'{} = amqp_channel:call(
        Channel,
        #'queue.bind'{queue = QueueName, exchange = ExchangeName}
    ),

    % Publish messages
    publish_message(Channel, ExchangeName, <<>>, <<"msg1">>, <<"Message 1">>),
    publish_message(Channel, ExchangeName, <<>>, <<"msg2">>, <<"Message 2">>),

    timer:sleep(100),

    % Verify 2 entries before expiration
    CacheName = <<"x_message_deduplication_x2F_migration_test_exchange">>,
    InfoBefore = get_cache_info(Config, CacheName),
    2 = proplists:get_value(entries, InfoBefore),

    % Wait for entries to expire
    timer:sleep(2500),

    % Trigger cleanup (both Mnesia and Khepri support this)
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    CacheAtom = binary_to_atom(CacheName, utf8),
    ok = rpc:call(Node, 'Elixir.RabbitMQMessageDeduplication.Cache', delete_expired_entries, [CacheAtom]),

    % Entries should be gone
    InfoAfterCleanup = get_cache_info(Config, CacheName),
    0 = proplists:get_value(entries, InfoAfterCleanup),

    % Enable Khepri - expired entries shouldn't be migrated
    ok = enable_khepri_db(Config),
    timer:sleep(2000),

    % Cache should still be empty
    InfoAfterMigration = get_cache_info(Config, CacheName),
    0 = proplists:get_value(entries, InfoAfterMigration),

    % New messages should work
    publish_message(Channel, ExchangeName, <<>>, <<"msg1">>, <<"New msg1">>),
    timer:sleep(100),

    InfoFinal = get_cache_info(Config, CacheName),
    1 = proplists:get_value(entries, InfoFinal),

    ok.

migration_preserves_cache_options(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),
    ExchangeName = <<"migration_test_exchange">>,

    % Create exchange with specific options
    CacheSize = 50,
    CacheTTL = 30000,
    create_dedup_exchange(Channel, ExchangeName, CacheSize, CacheTTL),

    % Get cache info with Mnesia
    CacheName = <<"x_message_deduplication_x2F_migration_test_exchange">>,
    InfoBefore = get_cache_info(Config, CacheName),
    ct:pal("Cache options before migration: ~p", [InfoBefore]),

    % Verify size is preserved
    CacheSize = proplists:get_value(size, InfoBefore),

    % Enable Khepri
    ok = enable_khepri_db(Config),
    timer:sleep(2000),

    % Verify options preserved
    InfoAfter = get_cache_info(Config, CacheName),
    ct:pal("Cache options after migration: ~p", [InfoAfter]),
    CacheSize = proplists:get_value(size, InfoAfter),

    ok.

operations_work_after_migration(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),
    ExchangeName = <<"migration_test_exchange">>,

    % Create exchange and publish some data with Mnesia
    create_dedup_exchange(Channel, ExchangeName, 100, 60000),

    QueueName = <<"migration_test_queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(
        Channel,
        #'queue.declare'{queue = QueueName, auto_delete = false}
    ),
    #'queue.bind_ok'{} = amqp_channel:call(
        Channel,
        #'queue.bind'{queue = QueueName, exchange = ExchangeName}
    ),

    publish_message(Channel, ExchangeName, <<>>, <<"pre_migration">>, <<"Before">>),
    timer:sleep(100),

    % Enable Khepri
    ok = enable_khepri_db(Config),
    timer:sleep(2000),

    % Test all operations work with Khepri
    CacheName = <<"x_message_deduplication_x2F_migration_test_exchange">>,
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    CacheAtom = binary_to_atom(CacheName, utf8),

    % Insert new entry
    publish_message(Channel, ExchangeName, <<>>, <<"post_migration">>, <<"After">>),
    timer:sleep(100),

    Info1 = get_cache_info(Config, CacheName),
    2 = proplists:get_value(entries, Info1),

    % Delete entry
    {ok, exists} = rpc:call(Node, 'Elixir.RabbitMQMessageDeduplication.Cache', exists?,
                            [CacheAtom, <<"pre_migration">>]),
    ok = rpc:call(Node, 'Elixir.RabbitMQMessageDeduplication.Cache', delete,
                  [CacheAtom, <<"pre_migration">>]),
    {ok, false} = rpc:call(Node, 'Elixir.RabbitMQMessageDeduplication.Cache', exists?,
                           [CacheAtom, <<"pre_migration">>]),

    Info2 = get_cache_info(Config, CacheName),
    1 = proplists:get_value(entries, Info2),

    % Flush cache
    ok = rpc:call(Node, 'Elixir.RabbitMQMessageDeduplication.Cache', flush, [CacheAtom]),

    Info3 = get_cache_info(Config, CacheName),
    0 = proplists:get_value(entries, Info3),

    % Verify new inserts work after flush
    publish_message(Channel, ExchangeName, <<>>, <<"after_flush">>, <<"New">>),
    timer:sleep(100),

    Info4 = get_cache_info(Config, CacheName),
    1 = proplists:get_value(entries, Info4),

    ok.
