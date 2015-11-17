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
-module(rabbit_test_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-import(rabbit_misc, [pget/2]).

-compile(export_all).

set_ha_policy(Cfg, Pattern, Policy) ->
    set_ha_policy(Cfg, Pattern, Policy, []).

set_ha_policy(Cfg, Pattern, Policy, Extra) ->
    set_policy(Cfg, Pattern, Pattern, <<"queues">>, ha_policy(Policy) ++ Extra).

ha_policy(<<"all">>)      -> [{<<"ha-mode">>,   <<"all">>}];
ha_policy({Mode, Params}) -> [{<<"ha-mode">>,   Mode},
                              {<<"ha-params">>, Params}].

set_policy(Cfg, Name, Pattern, ApplyTo, Definition) ->
    ok = rpc:call(pget(node, Cfg), rabbit_policy, set,
                  [<<"/">>, Name, Pattern, Definition, 0, ApplyTo]).

clear_policy(Cfg, Name) ->
    ok = rpc:call(pget(node, Cfg), rabbit_policy, delete, [<<"/">>, Name]).

set_param(Cfg, Component, Name, Value) ->
    ok = rpc:call(pget(node, Cfg), rabbit_runtime_parameters, set,
                  [<<"/">>, Component, Name, Value, none]).

clear_param(Cfg, Component, Name) ->
    ok = rpc:call(pget(node, Cfg), rabbit_runtime_parameters, clear,
                 [<<"/">>, Component, Name]).

enable_plugin(Cfg, Plugin) ->
    plugins_action(enable, Cfg, [Plugin], []).

disable_plugin(Cfg, Plugin) ->
    plugins_action(disable, Cfg, [Plugin], []).

control_action(Command, Cfg) ->
    control_action(Command, Cfg, [], []).

control_action(Command, Cfg, Args) ->
    control_action(Command, Cfg, Args, []).

control_action(Command, Cfg, Args, Opts) ->
    Node = pget(node, Cfg),
    rpc:call(Node, rabbit_control_main, action,
             [Command, Node, Args, Opts,
              fun (F, A) ->
                      error_logger:info_msg(F ++ "~n", A)
              end]).

plugins_action(Command, Cfg, Args, Opts) ->
    PluginsFile = os:getenv("RABBITMQ_ENABLED_PLUGINS_FILE"),
    PluginsDir = os:getenv("RABBITMQ_PLUGINS_DIR"),
    Node = pget(node, Cfg),
    rpc:call(Node, rabbit_plugins_main, action,
             [Command, Node, Args, Opts, PluginsFile, PluginsDir]).

restart_app(Cfg) ->
    stop_app(Cfg),
    start_app(Cfg).

stop_app(Cfg) ->
    control_action(stop_app, Cfg).

start_app(Cfg) ->
    control_action(start_app, Cfg).

connect(Cfg) ->
    Port = pget(port, Cfg),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{port = Port}),
    {ok, Ch} =  amqp_connection:open_channel(Conn),
    {Conn, Ch}.

%%----------------------------------------------------------------------------

kill_after(Time, Cfg, Method) ->
    timer:sleep(Time),
    kill(Cfg, Method).

kill(Cfg, Method) ->
    kill0(Cfg, Method),
    wait_down(pget(node, Cfg)).

kill0(Cfg, stop)    -> rabbit_test_configs:stop_node(Cfg);
kill0(Cfg, sigkill) -> rabbit_test_configs:kill_node(Cfg).

wait_down(Node) ->
    case net_adm:ping(Node) of
        pong -> timer:sleep(25),
                wait_down(Node);
        pang -> ok
    end.

a2b(A) -> list_to_binary(atom_to_list(A)).

%%----------------------------------------------------------------------------

publish(Ch, QName, Count) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:call(Ch,
                       #'basic.publish'{routing_key = QName},
                       #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                 payload = list_to_binary(integer_to_list(I))})
     || I <- lists:seq(1, Count)],
    amqp_channel:wait_for_confirms(Ch).

consume(Ch, QName, Count) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName, no_ack = true},
                           self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = C} -> C end,
    [begin
         Exp = list_to_binary(integer_to_list(I)),
         receive {#'basic.deliver'{consumer_tag = CTag},
                  #amqp_msg{payload = Exp}} ->
                 ok
         after 500 ->
                 exit(timeout)
         end
     end|| I <- lists:seq(1, Count)],
    #'queue.declare_ok'{message_count = 0}
        = amqp_channel:call(Ch, #'queue.declare'{queue   = QName,
                                                 durable = true}),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    ok.

fetch(Ch, QName, Count) ->
    [{#'basic.get_ok'{}, _} =
         amqp_channel:call(Ch, #'basic.get'{queue = QName}) ||
        _ <- lists:seq(1, Count)],
    ok.
