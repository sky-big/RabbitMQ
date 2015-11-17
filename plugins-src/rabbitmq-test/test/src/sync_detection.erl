%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%
-module(sync_detection).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_test_util, [stop_app/1, start_app/1]).
-import(rabbit_misc, [pget/2]).

-define(LOOP_RECURSION_DELAY, 100).

slave_synchronization_with() -> [cluster_ab, ha_policy_two_pos].
slave_synchronization([Master, Slave]) ->
    Channel = pget(channel, Master),
    Queue = <<"ha.two.test">>,
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = Queue,
                                                    auto_delete = false}),

    %% The comments on the right are the queue length and the pending acks on
    %% the master.
    stop_app(Slave),

    %% We get and ack one message when the slave is down, and check that when we
    %% start the slave it's not marked as synced until ack the message.  We also
    %% publish another message when the slave is up.
    send_dummy_message(Channel, Queue),                                 % 1 - 0
    {#'basic.get_ok'{delivery_tag = Tag1}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 0 - 1

    start_app(Slave),

    slave_unsynced(Master, Queue),
    send_dummy_message(Channel, Queue),                                 % 1 - 1
    slave_unsynced(Master, Queue),

    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag1}),      % 1 - 0

    slave_synced(Master, Queue),

    %% We restart the slave and we send a message, so that the slave will only
    %% have one of the messages.
    stop_app(Slave),
    start_app(Slave),

    send_dummy_message(Channel, Queue),                                 % 2 - 0

    slave_unsynced(Master, Queue),

    %% We reject the message that the slave doesn't have, and verify that it's
    %% still unsynced
    {#'basic.get_ok'{delivery_tag = Tag2}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 1 - 1
    slave_unsynced(Master, Queue),
    amqp_channel:cast(Channel, #'basic.reject'{ delivery_tag = Tag2,
                                                requeue      = true }), % 2 - 0
    slave_unsynced(Master, Queue),
    {#'basic.get_ok'{delivery_tag = Tag3}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 1 - 1
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag3}),      % 1 - 0
    slave_synced(Master, Queue),
    {#'basic.get_ok'{delivery_tag = Tag4}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 0 - 1
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag4}),      % 0 - 0
    slave_synced(Master, Queue).

slave_synchronization_ttl_with() -> [cluster_abc, ha_policy_two_pos].
slave_synchronization_ttl([Master, Slave, DLX]) ->
    Channel = pget(channel, Master),
    DLXChannel = pget(channel, DLX),

    %% We declare a DLX queue to wait for messages to be TTL'ed
    DLXQueue = <<"dlx-queue">>,
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = DLXQueue,
                                                    auto_delete = false}),

    TestMsgTTL = 5000,
    Queue = <<"ha.two.test">>,
    %% Sadly we need fairly high numbers for the TTL because starting/stopping
    %% nodes takes a fair amount of time.
    Args = [{<<"x-message-ttl">>, long, TestMsgTTL},
            {<<"x-dead-letter-exchange">>, longstr, <<>>},
            {<<"x-dead-letter-routing-key">>, longstr, DLXQueue}],
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = Queue,
                                                    auto_delete = false,
                                                    arguments   = Args}),

    slave_synced(Master, Queue),

    %% All unknown
    stop_app(Slave),
    send_dummy_message(Channel, Queue),
    send_dummy_message(Channel, Queue),
    start_app(Slave),
    slave_unsynced(Master, Queue),
    wait_for_messages(DLXQueue, DLXChannel, 2),
    slave_synced(Master, Queue),

    %% 1 unknown, 1 known
    stop_app(Slave),
    send_dummy_message(Channel, Queue),
    start_app(Slave),
    slave_unsynced(Master, Queue),
    send_dummy_message(Channel, Queue),
    slave_unsynced(Master, Queue),
    wait_for_messages(DLXQueue, DLXChannel, 2),
    slave_synced(Master, Queue),

    %% %% both known
    send_dummy_message(Channel, Queue),
    send_dummy_message(Channel, Queue),
    slave_synced(Master, Queue),
    wait_for_messages(DLXQueue, DLXChannel, 2),
    slave_synced(Master, Queue),

    ok.

send_dummy_message(Channel, Queue) ->
    Payload = <<"foo">>,
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Queue},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}).

slave_pids(Node, Queue) ->
    {ok, Q} = rpc:call(Node, rabbit_amqqueue, lookup,
                       [rabbit_misc:r(<<"/">>, queue, Queue)]),
    SSP = synchronised_slave_pids,
    [{SSP, Pids}] = rpc:call(Node, rabbit_amqqueue, info, [Q, [SSP]]),
    case Pids of
        '' -> [];
        _  -> Pids
    end.

%% The mnesia syncronization takes a while, but we don't want to wait for the
%% test to fail, since the timetrap is quite high.
wait_for_sync_status(Status, Cfg, Queue) ->
    Max = 10000 / ?LOOP_RECURSION_DELAY,
    wait_for_sync_status(0, Max, Status, pget(node, Cfg), Queue).

wait_for_sync_status(N, Max, Status, Node, Queue) when N >= Max ->
    error({sync_status_max_tries_failed,
          [{queue, Queue},
           {node, Node},
           {expected_status, Status},
           {max_tried, Max}]});
wait_for_sync_status(N, Max, Status, Node, Queue) ->
    Synced = length(slave_pids(Node, Queue)) =:= 1,
    case Synced =:= Status of
        true  -> ok;
        false -> timer:sleep(?LOOP_RECURSION_DELAY),
                 wait_for_sync_status(N + 1, Max, Status, Node, Queue)
    end.

slave_synced(Cfg, Queue) ->
    wait_for_sync_status(true, Cfg, Queue).

slave_unsynced(Cfg, Queue) ->
    wait_for_sync_status(false, Cfg, Queue).

wait_for_messages(Queue, Channel, N) ->
    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = CTag} = amqp_channel:call(Channel, Sub),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    lists:foreach(
      fun (_) -> receive
                     {#'basic.deliver'{delivery_tag = Tag}, _Content} ->
                         amqp_channel:cast(Channel,
                                           #'basic.ack'{delivery_tag = Tag})
                 end
      end, lists:seq(1, N)),
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = CTag}).
