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
-module(cluster_rename).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-import(rabbit_misc, [pget/2]).

-define(CLUSTER2,
        fun(C) -> rabbit_test_configs:cluster(C, [bugs, bigwig]) end).

-define(CLUSTER3,
        fun(C) -> rabbit_test_configs:cluster(C, [bugs, bigwig, peter]) end).

%% Rolling rename of a cluster, each node should do a secondary rename.
rename_cluster_one_by_one_with() -> ?CLUSTER3.
rename_cluster_one_by_one([Bugs, Bigwig, Peter]) ->
    publish_all([{Bugs, <<"1">>}, {Bigwig, <<"2">>}, {Peter, <<"3">>}]),

    Jessica = stop_rename_start(Bugs,   jessica, [bugs, jessica]),
    Hazel   = stop_rename_start(Bigwig, hazel,   [bigwig, hazel]),
    Flopsy  = stop_rename_start(Peter,  flopsy,  [peter, flopsy]),

    consume_all([{Jessica, <<"1">>}, {Hazel, <<"2">>}, {Flopsy, <<"3">>}]),
    stop_all([Jessica, Hazel, Flopsy]),
    ok.

%% Big bang rename of a cluster, bugs should do a primary rename.
rename_cluster_big_bang_with() -> ?CLUSTER3.
rename_cluster_big_bang([Bugs, Bigwig, Peter]) ->
    publish_all([{Bugs, <<"1">>}, {Bigwig, <<"2">>}, {Peter, <<"3">>}]),

    Peter1  = rabbit_test_configs:stop_node(Peter),
    Bigwig1 = rabbit_test_configs:stop_node(Bigwig),
    Bugs1   = rabbit_test_configs:stop_node(Bugs),

    Map = [bugs, jessica, bigwig, hazel, peter, flopsy],
    Jessica0 = rename_node(Bugs1,   jessica, Map),
    Hazel0   = rename_node(Bigwig1, hazel,   Map),
    Flopsy0  = rename_node(Peter1,  flopsy,  Map),

    Jessica = rabbit_test_configs:start_node(Jessica0),
    Hazel   = rabbit_test_configs:start_node(Hazel0),
    Flopsy  = rabbit_test_configs:start_node(Flopsy0),

    consume_all([{Jessica, <<"1">>}, {Hazel, <<"2">>}, {Flopsy, <<"3">>}]),
    stop_all([Jessica, Hazel, Flopsy]),
    ok.

%% Here we test that bugs copes with things being renamed around it.
partial_one_by_one_with() -> ?CLUSTER3.
partial_one_by_one([Bugs, Bigwig, Peter]) ->
    publish_all([{Bugs, <<"1">>}, {Bigwig, <<"2">>}, {Peter, <<"3">>}]),

    Jessica = stop_rename_start(Bugs,   jessica, [bugs, jessica]),
    Hazel   = stop_rename_start(Bigwig, hazel,   [bigwig, hazel]),

    consume_all([{Jessica, <<"1">>}, {Hazel, <<"2">>}, {Peter, <<"3">>}]),
    stop_all([Jessica, Hazel, Peter]),
    ok.

%% Here we test that bugs copes with things being renamed around it.
partial_big_bang_with() -> ?CLUSTER3.
partial_big_bang([Bugs, Bigwig, Peter]) ->
    publish_all([{Bugs, <<"1">>}, {Bigwig, <<"2">>}, {Peter, <<"3">>}]),

    Peter1  = rabbit_test_configs:stop_node(Peter),
    Bigwig1 = rabbit_test_configs:stop_node(Bigwig),
    Bugs1   = rabbit_test_configs:stop_node(Bugs),

    Map = [bigwig, hazel, peter, flopsy],
    Hazel0   = rename_node(Bigwig1, hazel,   Map),
    Flopsy0  = rename_node(Peter1,  flopsy,  Map),

    Bugs2  = rabbit_test_configs:start_node(Bugs1),
    Hazel  = rabbit_test_configs:start_node(Hazel0),
    Flopsy = rabbit_test_configs:start_node(Flopsy0),

    consume_all([{Bugs2, <<"1">>}, {Hazel, <<"2">>}, {Flopsy, <<"3">>}]),
    stop_all([Bugs2, Hazel, Flopsy]),
    ok.

%% We should be able to specify the -n parameter on ctl with either
%% the before or after name for the local node (since in real cases
%% one might want to invoke the command before or after the hostname
%% has changed) - usually we test before so here we test after.
post_change_nodename_with() -> ?CLUSTER2.
post_change_nodename([Bugs, _Bigwig]) ->
    publish(Bugs, <<"bugs">>),

    Bugs1    = rabbit_test_configs:stop_node(Bugs),
    Bugs2    = [{nodename, jessica} | proplists:delete(nodename, Bugs1)],
    Jessica0 = rename_node(Bugs2, jessica, [bugs, jessica]),
    Jessica  = rabbit_test_configs:start_node(Jessica0),

    consume(Jessica, <<"bugs">>),
    stop_all([Jessica]),
    ok.

%% If we invoke rename but the node name does not actually change, we
%% should roll back.
abortive_rename_with() -> ?CLUSTER2.
abortive_rename([Bugs, _Bigwig]) ->
    publish(Bugs,  <<"bugs">>),

    Bugs1   = rabbit_test_configs:stop_node(Bugs),
    _Jessica = rename_node(Bugs1, jessica, [bugs, jessica]),
    Bugs2 = rabbit_test_configs:start_node(Bugs1),

    consume(Bugs2, <<"bugs">>),
    ok.

%% And test some ways the command can fail.
rename_fail_with() -> ?CLUSTER2.
rename_fail([Bugs, _Bigwig]) ->
    Bugs1 = rabbit_test_configs:stop_node(Bugs),
    %% Rename from a node that does not exist
    rename_node_fail(Bugs1, [bugzilla, jessica]),
    %% Rename to a node which does
    rename_node_fail(Bugs1, [bugs, bigwig]),
    %% Rename two nodes to the same thing
    rename_node_fail(Bugs1, [bugs, jessica, bigwig, jessica]),
    %% Rename while impersonating a node not in the cluster
    rename_node_fail(set_node(rabbit, Bugs1), [bugs, jessica]),
    ok.

rename_twice_fail_with() -> ?CLUSTER2.
rename_twice_fail([Bugs, _Bigwig]) ->
    Bugs1 = rabbit_test_configs:stop_node(Bugs),
    Indecisive = rename_node(Bugs1, indecisive, [bugs, indecisive]),
    rename_node_fail(Indecisive, [indecisive, jessica]),
    ok.

%% ----------------------------------------------------------------------------

%% Normal post-test stop does not work since names have changed...
stop_all(Cfgs) ->
     [rabbit_test_configs:stop_node(Cfg) || Cfg <- Cfgs].

stop_rename_start(Cfg, Nodename, Map) ->
    rabbit_test_configs:start_node(
      rename_node(rabbit_test_configs:stop_node(Cfg), Nodename, Map)).

rename_node(Cfg, Nodename, Map) ->
    rename_node(Cfg, Nodename, Map, fun rabbit_test_configs:rabbitmqctl/2).

rename_node_fail(Cfg, Map) ->
    rename_node(Cfg, ignored, Map, fun rabbit_test_configs:rabbitmqctl_fail/2).

rename_node(Cfg, Nodename, Map, Ctl) ->
    MapS = string:join(
             [atom_to_list(rabbit_nodes:make(N)) || N <- Map], " "),
    Ctl(Cfg, {"rename_cluster_node ~s", [MapS]}),
    set_node(Nodename, Cfg).

publish(Cfg, Q) ->
    Ch = pget(channel, Cfg),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:call(Ch, #'queue.declare'{queue = Q, durable = true}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                payload = Q}),
    amqp_channel:wait_for_confirms(Ch).

consume(Cfg, Q) ->
    {_Conn, Ch} = rabbit_test_util:connect(Cfg),
    amqp_channel:call(Ch, #'queue.declare'{queue = Q, durable = true}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Q}} =
        amqp_channel:call(Ch, #'basic.get'{queue = Q}).


publish_all(CfgsKeys) ->
    [publish(Cfg, Key) || {Cfg, Key} <- CfgsKeys].

consume_all(CfgsKeys) ->
    [consume(Cfg, Key) || {Cfg, Key} <- CfgsKeys].

set_node(Nodename, Cfg) ->
    [{nodename, Nodename} | proplists:delete(nodename, Cfg)].
