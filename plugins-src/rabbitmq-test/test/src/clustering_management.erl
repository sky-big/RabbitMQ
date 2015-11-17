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
-module(clustering_management).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2]).

-define(LOOP_RECURSION_DELAY, 100).

join_and_part_cluster_with() -> start_abc.
join_and_part_cluster(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Rabbit, Bunny),
    assert_clustered([Rabbit, Bunny]),

    stop_join_start(Hare, Bunny, true),
    assert_cluster_status(
      {[Bunny, Hare, Rabbit], [Bunny, Rabbit], [Bunny, Hare, Rabbit]},
      [Rabbit, Hare, Bunny]),

    %% Allow clustering with already clustered node
    ok = stop_app(Rabbit),
    {ok, already_member} = join_cluster(Rabbit, Hare),
    ok = start_app(Rabbit),

    stop_reset_start(Rabbit),
    assert_not_clustered(Rabbit),
    assert_cluster_status({[Bunny, Hare], [Bunny], [Bunny, Hare]},
                          [Hare, Bunny]),

    stop_reset_start(Hare),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny).

join_cluster_bad_operations_with() -> start_abc.
join_cluster_bad_operations(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    %% Non-existant node
    ok = stop_app(Rabbit),
    assert_failure(fun () -> join_cluster(Rabbit, non@existant) end),
    ok = start_app(Rabbit),
    assert_not_clustered(Rabbit),

    %% Trying to cluster with mnesia running
    assert_failure(fun () -> join_cluster(Rabbit, Bunny) end),
    assert_not_clustered(Rabbit),

    %% Trying to cluster the node with itself
    ok = stop_app(Rabbit),
    assert_failure(fun () -> join_cluster(Rabbit, Rabbit) end),
    ok = start_app(Rabbit),
    assert_not_clustered(Rabbit),

    %% Do not let the node leave the cluster or reset if it's the only
    %% ram node
    stop_join_start(Hare, Rabbit, true),
    assert_cluster_status({[Rabbit, Hare], [Rabbit], [Rabbit, Hare]},
                          [Rabbit, Hare]),
    ok = stop_app(Hare),
    assert_failure(fun () -> join_cluster(Rabbit, Bunny) end),
    assert_failure(fun () -> reset(Rabbit) end),
    ok = start_app(Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit], [Rabbit, Hare]},
                          [Rabbit, Hare]),

    %% Cannot start RAM-only node first
    ok = stop_app(Rabbit),
    ok = stop_app(Hare),
    assert_failure(fun () -> start_app(Hare) end),
    ok = start_app(Rabbit),
    ok = start_app(Hare),
    ok.

%% This tests that the nodes in the cluster are notified immediately of a node
%% join, and not just after the app is started.
join_to_start_interval_with() -> start_abc.
join_to_start_interval(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    ok = stop_app(Rabbit),
    ok = join_cluster(Rabbit, Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit, Hare]),
    ok = start_app(Rabbit),
    assert_clustered([Rabbit, Hare]).

forget_cluster_node_with() -> start_abc.
forget_cluster_node([_, HareCfg, _] = Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    %% Trying to remove a node not in the cluster should fail
    assert_failure(fun () -> forget_cluster_node(Hare, Rabbit) end),

    stop_join_start(Rabbit, Hare),
    assert_clustered([Rabbit, Hare]),

    %% Trying to remove an online node should fail
    assert_failure(fun () -> forget_cluster_node(Hare, Rabbit) end),

    ok = stop_app(Rabbit),
    %% We're passing the --offline flag, but Hare is online
    assert_failure(fun () -> forget_cluster_node(Hare, Rabbit, true) end),
    %% Removing some non-existant node will fail
    assert_failure(fun () -> forget_cluster_node(Hare, non@existant) end),
    ok = forget_cluster_node(Hare, Rabbit),
    assert_not_clustered(Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit]),

    %% Now we can't start Rabbit since it thinks that it's still in the cluster
    %% with Hare, while Hare disagrees.
    assert_failure(fun () -> start_app(Rabbit) end),

    ok = reset(Rabbit),
    ok = start_app(Rabbit),
    assert_not_clustered(Rabbit),

    %% Now we remove Rabbit from an offline node.
    stop_join_start(Bunny, Hare),
    stop_join_start(Rabbit, Hare),
    assert_clustered([Rabbit, Hare, Bunny]),
    ok = stop_app(Hare),
    ok = stop_app(Rabbit),
    ok = stop_app(Bunny),
    %% This is fine but we need the flag
    assert_failure(fun () -> forget_cluster_node(Hare, Bunny) end),
    %% Also fails because hare node is still running
    assert_failure(fun () -> forget_cluster_node(Hare, Bunny, true) end),
    %% But this works
    HareCfg2 = rabbit_test_configs:stop_node(HareCfg),
    rabbit_test_configs:rabbitmqctl(
      HareCfg2, {"forget_cluster_node --offline ~s", [Bunny]}),
    _HareCfg3 = rabbit_test_configs:start_node(HareCfg2),
    ok = start_app(Rabbit),
    %% Bunny still thinks its clustered with Rabbit and Hare
    assert_failure(fun () -> start_app(Bunny) end),
    ok = reset(Bunny),
    ok = start_app(Bunny),
    assert_not_clustered(Bunny),
    assert_clustered([Rabbit, Hare]).

forget_removes_things_with() -> cluster_ab.
forget_removes_things(Cfg) ->
    test_removes_things(Cfg, fun (R, H) -> ok = forget_cluster_node(H, R) end).

reset_removes_things_with() -> cluster_ab.
reset_removes_things(Cfg) ->
    test_removes_things(Cfg, fun (R, _H) -> ok = reset(R) end).

test_removes_things([RabbitCfg, HareCfg] = Config, LoseRabbit) ->
    Unmirrored = <<"unmirrored-queue">>,
    [Rabbit, Hare] = cluster_members(Config),
    RCh = pget(channel, RabbitCfg),
    declare(RCh, Unmirrored),
    ok = stop_app(Rabbit),

    {_HConn, HCh} = rabbit_test_util:connect(HareCfg),
    {'EXIT',{{shutdown,{server_initiated_close,404,_}}, _}} =
        (catch declare(HCh, Unmirrored)),

    ok = LoseRabbit(Rabbit, Hare),
    {_HConn2, HCh2} = rabbit_test_util:connect(HareCfg),
    declare(HCh2, Unmirrored),
    ok.

forget_offline_removes_things_with() -> cluster_ab.
forget_offline_removes_things([Rabbit, Hare]) ->
    Unmirrored = <<"unmirrored-queue">>,
    X = <<"X">>,
    RCh = pget(channel, Rabbit),
    declare(RCh, Unmirrored),

    amqp_channel:call(RCh, #'exchange.declare'{durable     = true,
                                               exchange    = X,
                                               auto_delete = true}),
    amqp_channel:call(RCh, #'queue.bind'{queue    = Unmirrored,
                                         exchange = X}),
    ok = stop_app(pget(node, Rabbit)),

    {_HConn, HCh} = rabbit_test_util:connect(Hare),
    {'EXIT',{{shutdown,{server_initiated_close,404,_}}, _}} =
        (catch declare(HCh, Unmirrored)),

    Hare2 = rabbit_test_configs:stop_node(Hare),
    _Rabbit2 = rabbit_test_configs:stop_node(Rabbit),
    rabbit_test_configs:rabbitmqctl(
      Hare2, {"forget_cluster_node --offline ~s", [pget(node, Rabbit)]}),
    Hare3 = rabbit_test_configs:start_node(Hare2),

    {_HConn2, HCh2} = rabbit_test_util:connect(Hare3),
    declare(HCh2, Unmirrored),
    {'EXIT',{{shutdown,{server_initiated_close,404,_}}, _}} =
        (catch amqp_channel:call(HCh2,#'exchange.declare'{durable     = true,
                                                          exchange    = X,
                                                          auto_delete = true,
                                                          passive     = true})),
    ok.

forget_promotes_offline_slave_with() ->
    fun (Cfgs) ->
            rabbit_test_configs:cluster(Cfgs, [a, b, c, d])
    end.

forget_promotes_offline_slave([A, B, C, D]) ->
    ACh = pget(channel, A),
    ANode = pget(node, A),
    Q = <<"mirrored-queue">>,
    declare(ACh, Q),
    set_ha_policy(Q, A, [B, C]),
    set_ha_policy(Q, A, [C, D]), %% Test add and remove from recoverable_slaves

    %% Publish and confirm
    amqp_channel:call(ACh, #'confirm.select'{}),
    amqp_channel:cast(ACh, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props = #'P_basic'{delivery_mode = 2}}),
    amqp_channel:wait_for_confirms(ACh),

    %% We kill nodes rather than stop them in order to make sure
    %% that we aren't dependent on anything that happens as they shut
    %% down (see bug 26467).
    D2 = rabbit_test_configs:kill_node(D),
    C2 = rabbit_test_configs:kill_node(C),
    _B2 = rabbit_test_configs:kill_node(B),
    _A2 = rabbit_test_configs:kill_node(A),

    rabbit_test_configs:rabbitmqctl(C2, "force_boot"),

    C3 = rabbit_test_configs:start_node(C2),

    %% We should now have the following dramatis personae:
    %% A - down, master
    %% B - down, used to be slave, no longer is, never had the message
    %% C - running, should be slave, but has wiped the message on restart
    %% D - down, recoverable slave, contains message
    %%
    %% So forgetting A should offline-promote the queue to D, keeping
    %% the message.

    rabbit_test_configs:rabbitmqctl(C3, {"forget_cluster_node ~s", [ANode]}),

    D3 = rabbit_test_configs:start_node(D2),
    {_DConn2, DCh2} = rabbit_test_util:connect(D3),
    #'queue.declare_ok'{message_count = 1} = declare(DCh2, Q),
    ok.

set_ha_policy(Q, MasterCfg, SlaveCfgs) ->
    Nodes = [list_to_binary(atom_to_list(pget(node, N))) ||
                N <- [MasterCfg | SlaveCfgs]],
    rabbit_test_util:set_ha_policy(MasterCfg, Q, {<<"nodes">>, Nodes}),
    await_slaves(Q, pget(node, MasterCfg), [pget(node, C) || C <- SlaveCfgs]).

await_slaves(Q, MNode, SNodes) ->
    {ok, #amqqueue{pid        = MPid,
                   slave_pids = SPids}} =
        rpc:call(MNode, rabbit_amqqueue, lookup,
                 [rabbit_misc:r(<<"/">>, queue, Q)]),
    ActMNode = node(MPid),
    ActSNodes = lists:usort([node(P) || P <- SPids]),
    case {MNode, lists:usort(SNodes)} of
        {ActMNode, ActSNodes} -> ok;
        _                     -> timer:sleep(100),
                                 await_slaves(Q, MNode, SNodes)
    end.

force_boot_with() -> cluster_ab.
force_boot([Rabbit, Hare]) ->
    rabbit_test_configs:rabbitmqctl_fail(Rabbit, force_boot),
    Rabbit2 = rabbit_test_configs:stop_node(Rabbit),
    _Hare2 = rabbit_test_configs:stop_node(Hare),
    rabbit_test_configs:start_node_fail(Rabbit2),
    rabbit_test_configs:rabbitmqctl(Rabbit2, force_boot),
    _Rabbit3 = rabbit_test_configs:start_node(Rabbit2),
    ok.

change_cluster_node_type_with() -> start_abc.
change_cluster_node_type(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    %% Trying to change the ram node when not clustered should always fail
    ok = stop_app(Rabbit),
    assert_failure(fun () -> change_cluster_node_type(Rabbit, ram) end),
    assert_failure(fun () -> change_cluster_node_type(Rabbit, disc) end),
    ok = start_app(Rabbit),

    ok = stop_app(Rabbit),
    join_cluster(Rabbit, Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit, Hare]),
    change_cluster_node_type(Rabbit, ram),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Hare]},
                          [Rabbit, Hare]),
    change_cluster_node_type(Rabbit, disc),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit, Hare]),
    change_cluster_node_type(Rabbit, ram),
    ok = start_app(Rabbit),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Hare, Rabbit]},
                          [Rabbit, Hare]),

    %% Changing to ram when you're the only ram node should fail
    ok = stop_app(Hare),
    assert_failure(fun () -> change_cluster_node_type(Hare, ram) end),
    ok = start_app(Hare).

change_cluster_when_node_offline_with() -> start_abc.
change_cluster_when_node_offline(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    %% Cluster the three notes
    stop_join_start(Rabbit, Hare),
    assert_clustered([Rabbit, Hare]),

    stop_join_start(Bunny, Hare),
    assert_clustered([Rabbit, Hare, Bunny]),

    %% Bring down Rabbit, and remove Bunny from the cluster while
    %% Rabbit is offline
    ok = stop_app(Rabbit),
    ok = stop_app(Bunny),
    ok = reset(Bunny),
    assert_cluster_status({[Bunny], [Bunny], []}, [Bunny]),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]}, [Hare]),
    assert_cluster_status(
      {[Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny], [Hare, Bunny]}, [Rabbit]),

    %% Bring Rabbit back up
    ok = start_app(Rabbit),
    assert_clustered([Rabbit, Hare]),
    ok = start_app(Bunny),
    assert_not_clustered(Bunny),

    %% Now the same, but Rabbit is a RAM node, and we bring up Bunny
    %% before
    ok = stop_app(Rabbit),
    ok = change_cluster_node_type(Rabbit, ram),
    ok = start_app(Rabbit),
    stop_join_start(Bunny, Hare),
    assert_cluster_status(
      {[Rabbit, Hare, Bunny], [Hare, Bunny], [Rabbit, Hare, Bunny]},
      [Rabbit, Hare, Bunny]),
    ok = stop_app(Rabbit),
    ok = stop_app(Bunny),
    ok = reset(Bunny),
    ok = start_app(Bunny),
    assert_not_clustered(Bunny),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Hare]}, [Hare]),
    assert_cluster_status(
      {[Rabbit, Hare, Bunny], [Hare, Bunny], [Hare, Bunny]},
      [Rabbit]),
    ok = start_app(Rabbit),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]),
    assert_not_clustered(Bunny).

update_cluster_nodes_with() -> start_abc.
update_cluster_nodes(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    %% Mnesia is running...
    assert_failure(fun () -> update_cluster_nodes(Rabbit, Hare) end),

    ok = stop_app(Rabbit),
    ok = join_cluster(Rabbit, Hare),
    ok = stop_app(Bunny),
    ok = join_cluster(Bunny, Hare),
    ok = start_app(Bunny),
    stop_reset_start(Hare),
    assert_failure(fun () -> start_app(Rabbit) end),
    %% Bogus node
    assert_failure(fun () -> update_cluster_nodes(Rabbit, non@existant) end),
    %% Inconsisent node
    assert_failure(fun () -> update_cluster_nodes(Rabbit, Hare) end),
    ok = update_cluster_nodes(Rabbit, Bunny),
    ok = start_app(Rabbit),
    assert_not_clustered(Hare),
    assert_clustered([Rabbit, Bunny]).

erlang_config_with() -> start_ab.
erlang_config(Config) ->
    [Rabbit, Hare] = cluster_members(Config),

    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {[Rabbit], disc}]),
    ok = start_app(Hare),
    assert_clustered([Rabbit, Hare]),

    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {[Rabbit], ram}]),
    ok = start_app(Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit], [Rabbit, Hare]},
                          [Rabbit, Hare]),

    %% Check having a stop_app'ed node around doesn't break completely.
    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = stop_app(Rabbit),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {[Rabbit], disc}]),
    ok = start_app(Hare),
    ok = start_app(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Rabbit),

    %% We get a warning but we start anyway
    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {[non@existent], disc}]),
    ok = start_app(Hare),
    assert_not_clustered(Hare),
    assert_not_clustered(Rabbit),

    %% If we use a legacy config file, the node fails to start.
    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, [Rabbit]]),
    assert_failure(fun () -> start_app(Hare) end),
    assert_not_clustered(Rabbit),

    %% If we use an invalid node name, the node fails to start.
    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {["Mike's computer"], disc}]),
    assert_failure(fun () -> start_app(Hare) end),
    assert_not_clustered(Rabbit),

    %% If we use an invalid node type, the node fails to start.
    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {[Rabbit], blue}]),
    assert_failure(fun () -> start_app(Hare) end),
    assert_not_clustered(Rabbit),

    %% If we use an invalid cluster_nodes conf, the node fails to start.
    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, true]),
    assert_failure(fun () -> start_app(Hare) end),
    assert_not_clustered(Rabbit),

    ok = stop_app(Hare),
    ok = reset(Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, "Yes, please"]),
    assert_failure(fun () -> start_app(Hare) end),
    assert_not_clustered(Rabbit).

force_reset_node_with() -> start_abc.
force_reset_node(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    stop_join_start(Rabbit, Hare),
    stop_app(Rabbit),
    force_reset(Rabbit),
    %% Hare thinks that Rabbit is still clustered
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Hare]),
    %% %% ...but it isn't
    assert_cluster_status({[Rabbit], [Rabbit], []}, [Rabbit]),
    %% We can rejoin Rabbit and Hare
    update_cluster_nodes(Rabbit, Hare),
    start_app(Rabbit),
    assert_clustered([Rabbit, Hare]).

%% ----------------------------------------------------------------------------
%% Internal utils

cluster_members(Nodes) -> [pget(node,Cfg) || Cfg <- Nodes].

assert_cluster_status(Status0, Nodes) ->
    Status = {AllNodes, _, _} = sort_cluster_status(Status0),
    wait_for_cluster_status(Status, AllNodes, Nodes).

wait_for_cluster_status(Status, AllNodes, Nodes) ->
    Max = 10000 / ?LOOP_RECURSION_DELAY,
    wait_for_cluster_status(0, Max, Status, AllNodes, Nodes).

wait_for_cluster_status(N, Max, Status, _AllNodes, Nodes) when N >= Max ->
    error({cluster_status_max_tries_failed,
           [{nodes, Nodes},
            {expected_status, Status},
            {max_tried, Max}]});
wait_for_cluster_status(N, Max, Status, AllNodes, Nodes) ->
    case lists:all(fun (Node) ->
                            verify_status_equal(Node, Status, AllNodes)
                   end, Nodes) of
        true  -> ok;
        false -> timer:sleep(?LOOP_RECURSION_DELAY),
                 wait_for_cluster_status(N + 1, Max, Status, AllNodes, Nodes)
    end.

verify_status_equal(Node, Status, AllNodes) ->
    NodeStatus = sort_cluster_status(cluster_status(Node)),
    (AllNodes =/= [Node]) =:= rpc:call(Node, rabbit_mnesia, is_clustered, [])
        andalso NodeStatus =:= Status.

cluster_status(Node) ->
    {rpc:call(Node, rabbit_mnesia, cluster_nodes, [all]),
     rpc:call(Node, rabbit_mnesia, cluster_nodes, [disc]),
     rpc:call(Node, rabbit_mnesia, cluster_nodes, [running])}.

sort_cluster_status({All, Disc, Running}) ->
    {lists:sort(All), lists:sort(Disc), lists:sort(Running)}.

assert_clustered(Nodes) ->
    assert_cluster_status({Nodes, Nodes, Nodes}, Nodes).

assert_not_clustered(Node) ->
    assert_cluster_status({[Node], [Node], [Node]}, [Node]).

assert_failure(Fun) ->
    case catch Fun() of
        {error, Reason}                -> Reason;
        {badrpc, {'EXIT', Reason}}     -> Reason;
        {badrpc_multi, Reason, _Nodes} -> Reason;
        Other                          -> exit({expected_failure, Other})
    end.

stop_app(Node) ->
    control_action(stop_app, Node).

start_app(Node) ->
    control_action(start_app, Node).

join_cluster(Node, To) ->
    join_cluster(Node, To, false).

join_cluster(Node, To, Ram) ->
    control_action(join_cluster, Node, [atom_to_list(To)], [{"--ram", Ram}]).

reset(Node) ->
    control_action(reset, Node).

force_reset(Node) ->
    control_action(force_reset, Node).

forget_cluster_node(Node, Removee, RemoveWhenOffline) ->
    control_action(forget_cluster_node, Node, [atom_to_list(Removee)],
                   [{"--offline", RemoveWhenOffline}]).

forget_cluster_node(Node, Removee) ->
    forget_cluster_node(Node, Removee, false).

change_cluster_node_type(Node, Type) ->
    control_action(change_cluster_node_type, Node, [atom_to_list(Type)]).

update_cluster_nodes(Node, DiscoveryNode) ->
    control_action(update_cluster_nodes, Node, [atom_to_list(DiscoveryNode)]).

stop_join_start(Node, ClusterTo, Ram) ->
    ok = stop_app(Node),
    ok = join_cluster(Node, ClusterTo, Ram),
    ok = start_app(Node).

stop_join_start(Node, ClusterTo) ->
    stop_join_start(Node, ClusterTo, false).

stop_reset_start(Node) ->
    ok = stop_app(Node),
    ok = reset(Node),
    ok = start_app(Node).

control_action(Command, Node) ->
    control_action(Command, Node, [], []).

control_action(Command, Node, Args) ->
    control_action(Command, Node, Args, []).

control_action(Command, Node, Args, Opts) ->
    rpc:call(Node, rabbit_control_main, action,
             [Command, Node, Args, Opts,
              fun io:format/2]).

declare(Ch, Name) ->
    Res = amqp_channel:call(Ch, #'queue.declare'{durable = true,
                                                 queue   = Name}),
    amqp_channel:call(Ch, #'queue.bind'{queue    = Name,
                                        exchange = <<"amq.fanout">>}),
    Res.
