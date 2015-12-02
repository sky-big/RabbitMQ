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
-module(eager_sync).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-define(QNAME, <<"ha.two.test">>).
-define(QNAME_AUTO, <<"ha.auto.test">>).
-define(MESSAGE_COUNT, 2000).

-import(rabbit_test_util, [a2b/1, publish/3, consume/3, fetch/3]).
-import(rabbit_misc, [pget/2]).

-define(CONFIG, [cluster_abc, ha_policy_two_pos]).

eager_sync_with() -> ?CONFIG.
eager_sync([A, B, C]) ->
    %% Queue is on AB but not C.
    ACh = pget(channel, A),
    Ch = pget(channel, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),

    %% Don't sync, lose messages
    publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(A),
    restart(B),
    consume(Ch, ?QNAME, 0),

    %% Sync, keep messages
    publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(A),
    ok = sync(C, ?QNAME),
    restart(B),
    consume(Ch, ?QNAME, ?MESSAGE_COUNT),

    %% Check the no-need-to-sync path
    publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    ok = sync(C, ?QNAME),
    consume(Ch, ?QNAME, ?MESSAGE_COUNT),

    %% keep unacknowledged messages
    publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    fetch(Ch, ?QNAME, 2),
    restart(A),
    fetch(Ch, ?QNAME, 3),
    sync(C, ?QNAME),
    restart(B),
    consume(Ch, ?QNAME, ?MESSAGE_COUNT),

    ok.

eager_sync_cancel_with() -> ?CONFIG.
eager_sync_cancel([A, B, C]) ->
    %% Queue is on AB but not C.
    ACh = pget(channel, A),
    Ch = pget(channel, C),

    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),
    {ok, not_syncing} = sync_cancel(C, ?QNAME), %% Idempotence
    eager_sync_cancel_test2(A, B, C, Ch).

eager_sync_cancel_test2(A, B, C, Ch) ->
    %% Sync then cancel
    publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(A),
    spawn_link(fun() -> ok = sync_nowait(C, ?QNAME) end),
    case wait_for_syncing(C, ?QNAME, 1) of
        ok ->
            case sync_cancel(C, ?QNAME) of
                ok ->
                    wait_for_running(C, ?QNAME),
                    restart(B),
                    consume(Ch, ?QNAME, 0),

                    {ok, not_syncing} = sync_cancel(C, ?QNAME), %% Idempotence
                    ok;
                {ok, not_syncing} ->
                    %% Damn. Syncing finished between wait_for_syncing/3 and
                    %% sync_cancel/2 above. Start again.
                    amqp_channel:call(Ch, #'queue.purge'{queue = ?QNAME}),
                    eager_sync_cancel_test2(A, B, C, Ch)
            end;
        synced_already ->
            %% Damn. Syncing finished before wait_for_syncing/3. Start again.
            amqp_channel:call(Ch, #'queue.purge'{queue = ?QNAME}),
            eager_sync_cancel_test2(A, B, C, Ch)
    end.

eager_sync_auto_with() -> ?CONFIG.
eager_sync_auto([A, B, C]) ->
    ACh = pget(channel, A),
    Ch = pget(channel, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME_AUTO,
                                            durable = true}),

    %% Sync automatically, don't lose messages
    publish(Ch, ?QNAME_AUTO, ?MESSAGE_COUNT),
    restart(A),
    wait_for_sync(C, ?QNAME_AUTO),
    restart(B),
    wait_for_sync(C, ?QNAME_AUTO),
    consume(Ch, ?QNAME_AUTO, ?MESSAGE_COUNT),

    ok.

eager_sync_auto_on_policy_change_with() -> ?CONFIG.
eager_sync_auto_on_policy_change([A, B, C]) ->
    ACh = pget(channel, A),
    Ch = pget(channel, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),

    %% Sync automatically once the policy is changed to tell us to.
    publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    restart(A),
    Params = [a2b(pget(node, Cfg)) || Cfg <- [A, B]],
    rabbit_test_util:set_ha_policy(
      A, <<"^ha.two.">>, {<<"nodes">>, Params},
      [{<<"ha-sync-mode">>, <<"automatic">>}]),
    wait_for_sync(C, ?QNAME),

    ok.

eager_sync_requeue_with() -> ?CONFIG.
eager_sync_requeue([A, B, C]) ->
    %% Queue is on AB but not C.
    ACh = pget(channel, A),
    Ch = pget(channel, C),
    amqp_channel:call(ACh, #'queue.declare'{queue   = ?QNAME,
                                            durable = true}),

    publish(Ch, ?QNAME, 2),
    {#'basic.get_ok'{delivery_tag = TagA}, _} =
        amqp_channel:call(Ch, #'basic.get'{queue = ?QNAME}),
    {#'basic.get_ok'{delivery_tag = TagB}, _} =
        amqp_channel:call(Ch, #'basic.get'{queue = ?QNAME}),
    amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = TagA, requeue = true}),
    restart(B),
    ok = sync(C, ?QNAME),
    amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = TagB, requeue = true}),
    consume(Ch, ?QNAME, 2),

    ok.

restart(Cfg) -> rabbit_test_util:restart_app(Cfg).

sync(Cfg, QName) ->
    case sync_nowait(Cfg, QName) of
        ok -> wait_for_sync(Cfg, QName),
              ok;
        R  -> R
    end.

sync_nowait(Cfg, QName) -> action(Cfg, sync_queue, QName).
sync_cancel(Cfg, QName) -> action(Cfg, cancel_sync_queue, QName).

wait_for_sync(Cfg, QName) ->
    sync_detection:wait_for_sync_status(true, Cfg, QName).

action(Cfg, Action, QName) ->
    rabbit_test_util:control_action(
      Action, Cfg, [binary_to_list(QName)], [{"-p", "/"}]).

queue(Cfg, QName) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, QName),
    {ok, Q} = rpc:call(pget(node, Cfg), rabbit_amqqueue, lookup, [QNameRes]),
    Q.

wait_for_syncing(Cfg, QName, Target) ->
    case state(Cfg, QName) of
        {{syncing, _}, _} -> ok;
        {running, Target} -> synced_already;
        _                 -> timer:sleep(100),
                             wait_for_syncing(Cfg, QName, Target)
    end.

wait_for_running(Cfg, QName) ->
    case state(Cfg, QName) of
        {running, _} -> ok;
        _            -> timer:sleep(100),
                        wait_for_running(Cfg, QName)
    end.

state(Cfg, QName) ->
    [{state, State}, {synchronised_slave_pids, Pids}] =
        rpc:call(pget(node, Cfg), rabbit_amqqueue, info,
                 [queue(Cfg, QName), [state, synchronised_slave_pids]]),
    {State, length(Pids)}.
