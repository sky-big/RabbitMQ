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
-module(crashing_queues).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-import(rabbit_test_util, [set_ha_policy/3, a2b/1]).
-import(rabbit_misc, [pget/2]).

crashing_unmirrored_with() -> [cluster_ab].
crashing_unmirrored([CfgA, CfgB]) ->
    A = pget(node, CfgA),
    ChA = pget(channel, CfgA),
    ConnB = pget(connection, CfgB),
    amqp_channel:call(ChA, #'confirm.select'{}),
    test_queue_failure(A, ChA, ConnB, 1, 0,
                       #'queue.declare'{queue = <<"test">>, durable = true}),
    test_queue_failure(A, ChA, ConnB, 0, 0,
                       #'queue.declare'{queue = <<"test">>, durable = false}),
    ok.

crashing_mirrored_with() -> [cluster_ab, ha_policy_all].
crashing_mirrored([CfgA, CfgB]) ->
    A = pget(node, CfgA),
    ChA = pget(channel, CfgA),
    ConnB = pget(connection, CfgB),
    amqp_channel:call(ChA, #'confirm.select'{}),
    test_queue_failure(A, ChA, ConnB, 2, 1,
                       #'queue.declare'{queue = <<"test">>, durable = true}),
    test_queue_failure(A, ChA, ConnB, 2, 1,
                       #'queue.declare'{queue = <<"test">>, durable = false}),
    ok.

test_queue_failure(Node, Ch, RaceConn, MsgCount, SlaveCount, Decl) ->
    #'queue.declare_ok'{queue = QName} = amqp_channel:call(Ch, Decl),
    publish(Ch, QName, transient),
    publish(Ch, QName, durable),
    Racer = spawn_declare_racer(RaceConn, Decl),
    kill_queue(Node, QName),
    assert_message_count(MsgCount, Ch, QName),
    assert_slave_count(SlaveCount, Node, QName),
    stop_declare_racer(Racer),
    amqp_channel:call(Ch, #'queue.delete'{queue = QName}).

give_up_after_repeated_crashes_with() -> [cluster_ab].
give_up_after_repeated_crashes([CfgA, CfgB]) ->
    A = pget(node, CfgA),
    ChA = pget(channel, CfgA),
    ChB = pget(channel, CfgB),
    QName = <<"test">>,
    amqp_channel:call(ChA, #'confirm.select'{}),
    amqp_channel:call(ChA, #'queue.declare'{queue   = QName,
                                            durable = true}),
    await_state(A, QName, running),
    publish(ChA, QName, durable),
    kill_queue_hard(A, QName),
    {'EXIT', _} = (catch amqp_channel:call(
                           ChA, #'queue.declare'{queue   = QName,
                                                 durable = true})),
    await_state(A, QName, crashed),
    amqp_channel:call(ChB, #'queue.delete'{queue = QName}),
    amqp_channel:call(ChB, #'queue.declare'{queue   = QName,
                                            durable = true}),
    await_state(A, QName, running),

    %% Since it's convenient, also test absent queue status here.
    rabbit_test_configs:stop_node(CfgB),
    await_state(A, QName, down),
    ok.


publish(Ch, QName, DelMode) ->
    Publish = #'basic.publish'{exchange = <<>>, routing_key = QName},
    Msg = #amqp_msg{props = #'P_basic'{delivery_mode = del_mode(DelMode)}},
    amqp_channel:cast(Ch, Publish, Msg),
    amqp_channel:wait_for_confirms(Ch).

del_mode(transient) -> 1;
del_mode(durable)   -> 2.

spawn_declare_racer(Conn, Decl) ->
    Self = self(),
    spawn_link(fun() -> declare_racer_loop(Self, Conn, Decl) end).

stop_declare_racer(Pid) ->
    Pid ! stop,
    MRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    end.

declare_racer_loop(Parent, Conn, Decl) ->
    receive
        stop -> unlink(Parent)
    after 0 ->
            %% Catch here because we might happen to catch the queue
            %% while it is in the middle of recovering and thus
            %% explode with NOT_FOUND because crashed. Doesn't matter,
            %% we are only in this loop to try to fool the recovery
            %% code anyway.
            try
                case amqp_connection:open_channel(Conn) of
                    {ok, Ch} -> amqp_channel:call(Ch, Decl);
                    closing  -> ok
                end
            catch
                exit:_ ->
                    ok
            end,
            declare_racer_loop(Parent, Conn, Decl)
    end.

await_state(Node, QName, State) ->
    await_state(Node, QName, State, 30000).

await_state(Node, QName, State, Time) ->
    case state(Node, QName) of
        State ->
            ok;
        Other ->
            case Time of
                0 -> exit({timeout_awaiting_state, State, Other});
                _ -> timer:sleep(100),
                     await_state(Node, QName, State, Time - 100)
            end
    end.

state(Node, QName) ->
    V = <<"/">>,
    Res = rabbit_misc:r(V, queue, QName),
    [[{name,  Res},
      {state, State}]] =
        rpc:call(Node, rabbit_amqqueue, info_all, [V, [name, state]]),
    State.

kill_queue_hard(Node, QName) ->
    case kill_queue(Node, QName) of
        crashed -> ok;
        _NewPid -> timer:sleep(100),
                   kill_queue_hard(Node, QName)
    end.

kill_queue(Node, QName) ->
    Pid1 = queue_pid(Node, QName),
    exit(Pid1, boom),
    await_new_pid(Node, QName, Pid1).

queue_pid(Node, QName) ->
    #amqqueue{pid   = QPid,
              state = State} = lookup(Node, QName),
    case State of
        crashed -> case sup_child(Node, rabbit_amqqueue_sup_sup) of
                       {ok, _}           -> QPid;   %% restarting
                       {error, no_child} -> crashed %% given up
                   end;
        _       -> QPid
    end.

sup_child(Node, Sup) ->
    case rpc:call(Node, supervisor2, which_children, [Sup]) of
        [{_, Child, _, _}]              -> {ok, Child};
        []                              -> {error, no_child};
        {badrpc, {'EXIT', {noproc, _}}} -> {error, no_sup}
    end.

lookup(Node, QName) ->
    {ok, Q} = rpc:call(Node, rabbit_amqqueue, lookup,
                       [rabbit_misc:r(<<"/">>, queue, QName)]),
    Q.

await_new_pid(Node, QName, OldPid) ->
    case queue_pid(Node, QName) of
        OldPid -> timer:sleep(10),
                  await_new_pid(Node, QName, OldPid);
        New    -> New
    end.

assert_message_count(Count, Ch, QName) ->
    #'queue.declare_ok'{message_count = Count} =
        amqp_channel:call(Ch, #'queue.declare'{queue   = QName,
                                               passive = true}).

assert_slave_count(Count, Node, QName) ->
    Q = lookup(Node, QName),
    [{_, Pids}] = rpc:call(Node, rabbit_amqqueue, info, [Q, [slave_pids]]),
    RealCount = case Pids of
                    '' -> 0;
                    _  -> length(Pids)
                end,
    case RealCount of
        Count ->
            ok;
        _ when RealCount < Count ->
            timer:sleep(10),
            assert_slave_count(Count, Node, QName);
        _ ->
            exit({too_many_slaves, Count, RealCount})
    end.
