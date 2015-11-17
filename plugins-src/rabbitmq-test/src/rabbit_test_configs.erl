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
-module(rabbit_test_configs).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([enable_plugins/1]).
-export([cluster/2, cluster_ab/1, cluster_abc/1, start_ab/1, start_abc/1]).
-export([start_connections/1, build_cluster/1]).
-export([ha_policy_all/1, ha_policy_two_pos/1]).
-export([start_nodes/2, start_nodes/3, add_to_cluster/2,
         rabbitmqctl/2, rabbitmqctl_fail/2]).
-export([stop_nodes/1, start_node/1, stop_node/1, kill_node/1, restart_node/1,
         start_node_fail/1, execute/1]).
-export([cover_work_factor/2]).

-import(rabbit_test_util, [set_ha_policy/3, set_ha_policy/4, a2b/1]).
-import(rabbit_misc, [pget/2, pget/3]).

-define(INITIAL_KEYS, [cover, base, server, plugins]).
-define(NON_RUNNING_KEYS, ?INITIAL_KEYS ++ [nodename, port, mnesia_dir]).

cluster_ab(InitialCfg)  -> cluster(InitialCfg, [a, b]).
cluster_abc(InitialCfg) -> cluster(InitialCfg, [a, b, c]).
start_ab(InitialCfg)    -> start_nodes(InitialCfg, [a, b]).
start_abc(InitialCfg)   -> start_nodes(InitialCfg, [a, b, c]).

cluster(InitialCfg, NodeNames) ->
    start_connections(build_cluster(start_nodes(InitialCfg, NodeNames))).

start_nodes(InitialCfg, NodeNames) ->
    start_nodes(InitialCfg, NodeNames, 5672).

start_nodes(InitialCfg0, NodeNames, FirstPort) ->
    {ok, Already0} = net_adm:names(),
    Already = [list_to_atom(N) || {N, _P} <- Already0],
    [check_node_not_running(Node, Already) || Node <- NodeNames],
    Ports = lists:seq(FirstPort, length(NodeNames) + FirstPort - 1),
    InitialCfgs = case InitialCfg0 of
                      [{_, _}|_] -> [InitialCfg0 || _ <- NodeNames];
                      _          -> InitialCfg0
                  end,
    Nodes = [[{nodename, N}, {port, P},
              {mnesia_dir, rabbit_misc:format("rabbitmq-~s-mnesia", [N])} |
              strip_non_initial(Cfg)]
             || {N, P, Cfg} <- lists:zip3(NodeNames, Ports, InitialCfgs)],
    [start_node(Node) || Node <- Nodes].

check_node_not_running(Node, Already) ->
    case lists:member(Node, Already) of
        true  -> exit({node_already_running, Node});
        false -> ok
    end.

strip_non_initial(Cfg) ->
    [{K, V} || {K, V} <- Cfg, lists:member(K, ?INITIAL_KEYS)].

strip_running(Cfg) ->
    [{K, V} || {K, V} <- Cfg, lists:member(K, ?NON_RUNNING_KEYS)].

enable_plugins(Cfg) ->
    enable_plugins(pget(plugins, Cfg), pget(server, Cfg), Cfg).

enable_plugins(none, _Server, _Cfg) -> ok;
enable_plugins(_Dir, Server, Cfg) ->
    R = execute(Cfg, Server ++ "/scripts/rabbitmq-plugins list -m"),
    Plugins = string:join(string:tokens(R, "\n"), " "),
    execute(Cfg, {Server ++ "/scripts/rabbitmq-plugins set --offline ~s",
                  [Plugins]}),
    ok.

start_node(Cfg0) ->
    Node = rabbit_nodes:make(pget(nodename, Cfg0)),
    Cfg = [{node, Node} | Cfg0],
    Server = pget(server, Cfg),
    Linked = execute_bg(Cfg, Server ++ "/scripts/rabbitmq-server"),
    rabbitmqctl(Cfg, {"wait ~s", [pid_file(Cfg)]}),
    OSPid = rpc:call(Node, os, getpid, []),
    %% The cover system thinks all nodes with the same name are the
    %% same node and will automaticaly re-establish cover as soon as
    %% we see them, so we only want to start cover once per node name
    %% for the entire test run.
    case {pget(cover, Cfg), lists:member(Node, cover:which_nodes())} of
        {true, false} -> cover:start([Node]);
        _             -> ok
    end,
    [{os_pid,     OSPid},
     {linked_pid, Linked} | Cfg].

start_node_fail(Cfg0) ->
    Node = rabbit_nodes:make(pget(nodename, Cfg0)),
    Cfg = [{node, Node}, {acceptable_exit_codes, lists:seq(1, 255)} | Cfg0],
    Server = pget(server, Cfg),
    execute(Cfg, Server ++ "/scripts/rabbitmq-server"),
    ok.

build_cluster([First | Rest]) ->
    add_to_cluster([First], Rest).

add_to_cluster([First | _] = Existing, New) ->
    [cluster_with(First, Node) || Node <- New],
    Existing ++ New.

cluster_with(Cfg, NewCfg) ->
    Node = pget(node, Cfg),
    rabbitmqctl(NewCfg, stop_app),
    rabbitmqctl(NewCfg, {"join_cluster ~s", [Node]}),
    rabbitmqctl(NewCfg, start_app).

rabbitmqctl(Cfg, Str) ->
    Node = pget(node, Cfg),
    Server = pget(server, Cfg),
    Cmd = case Node of
              undefined -> {"~s", [fmt(Str)]};
              _         -> {"-n ~s ~s", [Node, fmt(Str)]}
          end,
    execute(Cfg, {Server ++ "/scripts/rabbitmqctl ~s", [fmt(Cmd)]}).

rabbitmqctl_fail(Cfg, Str) ->
    rabbitmqctl([{acceptable_exit_codes, lists:seq(1, 255)} | Cfg], Str).

ha_policy_all([Cfg | _] = Cfgs) ->
    set_ha_policy(Cfg, <<".*">>, <<"all">>),
    Cfgs.

ha_policy_two_pos([Cfg | _] = Cfgs) ->
    Members = [a2b(pget(node, C)) || C <- Cfgs],
    TwoNodes = [M || M <- lists:sublist(Members, 2)],
    set_ha_policy(Cfg, <<"^ha.two.">>, {<<"nodes">>, TwoNodes},
                  [{<<"ha-promote-on-shutdown">>, <<"always">>}]),
    set_ha_policy(Cfg, <<"^ha.auto.">>, {<<"nodes">>, TwoNodes},
                  [{<<"ha-sync-mode">>,           <<"automatic">>},
                   {<<"ha-promote-on-shutdown">>, <<"always">>}]),
    Cfgs.

start_connections(Nodes) -> [start_connection(Node) || Node <- Nodes].

start_connection(Cfg) ->
    Port = pget(port, Cfg),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{port = Port}),
    {ok, Ch} =  amqp_connection:open_channel(Conn),
    [{connection, Conn}, {channel, Ch} | Cfg].

stop_nodes(Nodes) -> [stop_node(Node) || Node <- Nodes].

stop_node(Cfg) ->
    maybe_flush_cover(Cfg),
    catch rabbitmqctl(Cfg, {"stop ~s", [pid_file(Cfg)]}),
    strip_running(Cfg).

kill_node(Cfg) ->
    maybe_flush_cover(Cfg),
    OSPid = pget(os_pid, Cfg),
    catch execute(Cfg, {"kill -9 ~s", [OSPid]}),
    await_os_pid_death(OSPid),
    strip_running(Cfg).

await_os_pid_death(OSPid) ->
    case rabbit_misc:is_os_process_alive(OSPid) of
        true  -> timer:sleep(100),
                 await_os_pid_death(OSPid);
        false -> ok
    end.

restart_node(Cfg) ->
    start_node(stop_node(Cfg)).

maybe_flush_cover(Cfg) ->
    case pget(cover, Cfg) of
        true  -> cover:flush(pget(node, Cfg));
        false -> ok
    end.

%% Cover slows things down enough that if we are sending messages in
%% bulk, we want to send fewer or we'll be here all day...
cover_work_factor(Without, Cfg) ->
    case pget(cover, Cfg) of
        true  -> trunc(Without * 0.1);
        false -> Without
    end.

%%----------------------------------------------------------------------------

execute(Cmd) ->
    execute([], Cmd, [0]).

execute(Cfg, Cmd) ->
    %% code 137 -> killed with SIGKILL which we do in some tests
    execute(environment(Cfg), Cmd, pget(acceptable_exit_codes, Cfg, [0, 137])).

execute(Env0, Cmd0, AcceptableExitCodes) ->
    Env = [{"RABBITMQ_" ++ K, fmt(V)} || {K, V} <- Env0],
    Cmd = fmt(Cmd0),
    error_logger:info_msg("Invoking '~s'~n", [Cmd]),
    Port = erlang:open_port(
             {spawn, "/usr/bin/env sh -c \"" ++ Cmd ++ "\""},
             [{env, Env}, exit_status,
              stderr_to_stdout, use_stdio]),
    port_receive_loop(Port, "", AcceptableExitCodes).

environment(Cfg) ->
    Nodename = pget(nodename, Cfg),
    Plugins = pget(plugins, Cfg),
    case Nodename of
        undefined ->
            plugins_env(Plugins);
        _         ->
            Port = pget(port, Cfg),
            Base = pget(base, Cfg),
            Server = pget(server, Cfg),
            [{"MNESIA_DIR",         {"~s/~s", [Base, pget(mnesia_dir, Cfg)]}},
             {"PLUGINS_EXPAND_DIR", {"~s/~s-plugins-expand", [Base, Nodename]}},
             {"LOG_BASE",           {"~s", [Base]}},
             {"NODENAME",           {"~s", [Nodename]}},
             {"NODE_PORT",          {"~B", [Port]}},
             {"PID_FILE",           pid_file(Cfg)},
             {"CONFIG_FILE",        "/some/path/which/does/not/exist"},
             {"ALLOW_INPUT",        "1"}, %% Needed to make it close on exit
             %% Bit of a hack - only needed for mgmt tests.
             {"SERVER_START_ARGS",
              {"-rabbitmq_management listener [{port,1~B}]", [Port]}},
             {"SERVER_ERL_ARGS",
              %% Next two lines are defaults
              {"+K true +A30 +P 1048576 "
               "-kernel inet_default_connect_options [{nodelay,true}] "
               %% Some tests need to be able to make distribution unhappy
               "-pa ~s/../rabbitmq-test/ebin "
               "-proto_dist inet_proxy", [Server]}}
             | plugins_env(Plugins)]
    end.

plugins_env(none) ->
    [{"ENABLED_PLUGINS_FILE", "/does-not-exist"}];
plugins_env(Dir) ->
    [{"PLUGINS_DIR",          {"~s/plugins", [Dir]}},
     {"PLUGINS_EXPAND_DIR",   {"~s/expand", [Dir]}},
     {"ENABLED_PLUGINS_FILE", {"~s/enabled_plugins", [Dir]}}].

pid_file(Cfg) ->
    rabbit_misc:format("~s/~s.pid", [pget(base, Cfg), pget(nodename, Cfg)]).

port_receive_loop(Port, Stdout, AcceptableExitCodes) ->
    receive
        {Port, {exit_status, X}} ->
            Fmt = "Command exited with code ~p~nStdout: ~s~n",
            Args = [X, Stdout],
            case lists:member(X, AcceptableExitCodes) of
                true  -> error_logger:info_msg(Fmt, Args),
                         Stdout;
                false -> error_logger:error_msg(Fmt, Args),
                         exit({exit_status, X, AcceptableExitCodes, Stdout})
            end;
        {Port, {data, Out}} ->
            port_receive_loop(Port, Stdout ++ Out, AcceptableExitCodes)
    end.

execute_bg(Cfg, Cmd) ->
    spawn_link(fun () ->
                       execute(Cfg, Cmd),
                       {links, Links} = process_info(self(), links),
                       [unlink(L) || L <- Links]
               end).

fmt({Fmt, Args}) -> rabbit_misc:format(Fmt, Args);
fmt(Str)         -> Str.

