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
-module(simple_ha).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_test_util, [set_ha_policy/3, a2b/1]).
-import(rabbit_misc, [pget/2]).

-define(CONFIG, [cluster_abc, ha_policy_all]).

rapid_redeclare_with() -> [cluster_ab, ha_policy_all].
rapid_redeclare([CfgA | _]) ->
    Ch = pget(channel, CfgA),
    Queue = <<"test">>,
    [begin
         amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                                durable = true}),
         amqp_channel:call(Ch, #'queue.delete'{queue  = Queue})
     end || _I <- lists:seq(1, 20)],
    ok.

%% Check that by the time we get a declare-ok back, the slaves are up
%% and in Mnesia.
declare_synchrony_with() -> [cluster_ab, ha_policy_all].
declare_synchrony([Rabbit, Hare]) ->
    RabbitCh = pget(channel, Rabbit),
    HareCh = pget(channel, Hare),
    Q = <<"mirrored-queue">>,
    declare(RabbitCh, Q),
    amqp_channel:call(RabbitCh, #'confirm.select'{}),
    amqp_channel:cast(RabbitCh, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props = #'P_basic'{delivery_mode = 2}}),
    amqp_channel:wait_for_confirms(RabbitCh),
    _Rabbit2 = rabbit_test_configs:kill_node(Rabbit),

    #'queue.declare_ok'{message_count = 1} = declare(HareCh, Q),
    ok.

declare(Ch, Name) ->
    amqp_channel:call(Ch, #'queue.declare'{durable = true, queue = Name}).

consume_survives_stop_with()     -> ?CONFIG.
consume_survives_sigkill_with()  -> ?CONFIG.
consume_survives_policy_with()   -> ?CONFIG.
auto_resume_with()               -> ?CONFIG.
auto_resume_no_ccn_client_with() -> ?CONFIG.

consume_survives_stop(Cf)     -> consume_survives(Cf, fun stop/2,    true).
consume_survives_sigkill(Cf)  -> consume_survives(Cf, fun sigkill/2, true).
consume_survives_policy(Cf)   -> consume_survives(Cf, fun policy/2,  true).
auto_resume(Cf)               -> consume_survives(Cf, fun sigkill/2, false).
auto_resume_no_ccn_client(Cf) -> consume_survives(Cf, fun sigkill/2, false,
                                                  false).

confirms_survive_stop_with()    -> ?CONFIG.
confirms_survive_sigkill_with() -> ?CONFIG.
confirms_survive_policy_with()  -> ?CONFIG.

confirms_survive_stop(Cf)    -> confirms_survive(Cf, fun stop/2).
confirms_survive_sigkill(Cf) -> confirms_survive(Cf, fun sigkill/2).
confirms_survive_policy(Cf)  -> confirms_survive(Cf, fun policy/2).

%%----------------------------------------------------------------------------

consume_survives(Nodes, DeathFun, CancelOnFailover) ->
    consume_survives(Nodes, DeathFun, CancelOnFailover, true).

consume_survives([CfgA, CfgB, CfgC] = Nodes,
                 DeathFun, CancelOnFailover, CCNSupported) ->
    Msgs = rabbit_test_configs:cover_work_factor(20000, CfgA),
    Channel1 = pget(channel, CfgA),
    Channel2 = pget(channel, CfgB),
    Channel3 = pget(channel, CfgC),

    %% declare the queue on the master, mirrored to the two slaves
    Queue = <<"test">>,
    amqp_channel:call(Channel1, #'queue.declare'{queue       = Queue,
                                                 auto_delete = false}),

    %% start up a consumer
    ConsCh = case CCNSupported of
                 true  -> Channel2;
                 false -> open_incapable_channel(pget(port, CfgB))
             end,
    ConsumerPid = rabbit_ha_test_consumer:create(
                    ConsCh, Queue, self(), CancelOnFailover, Msgs),

    %% send a bunch of messages from the producer
    ProducerPid = rabbit_ha_test_producer:create(Channel3, Queue,
                                                 self(), false, Msgs),
    DeathFun(CfgA, Nodes),
    %% verify that the consumer got all msgs, or die - the await_response
    %% calls throw an exception if anything goes wrong....
    rabbit_ha_test_consumer:await_response(ConsumerPid),
    rabbit_ha_test_producer:await_response(ProducerPid),
    ok.

confirms_survive([CfgA, CfgB, _CfgC] = Nodes, DeathFun) ->
    Msgs = rabbit_test_configs:cover_work_factor(20000, CfgA),
    Node1Channel = pget(channel, CfgA),
    Node2Channel = pget(channel, CfgB),

    %% declare the queue on the master, mirrored to the two slaves
    Queue = <<"test">>,
    amqp_channel:call(Node1Channel,#'queue.declare'{queue       = Queue,
                                                    auto_delete = false,
                                                    durable     = true}),

    %% send a bunch of messages from the producer
    ProducerPid = rabbit_ha_test_producer:create(Node2Channel, Queue,
                                                 self(), true, Msgs),
    DeathFun(CfgA, Nodes),
    rabbit_ha_test_producer:await_response(ProducerPid),
    ok.

stop(Cfg, _Cfgs)    -> rabbit_test_util:kill_after(50, Cfg, stop).
sigkill(Cfg, _Cfgs) -> rabbit_test_util:kill_after(50, Cfg, sigkill).
policy(Cfg, [_|T])  -> Nodes = [a2b(pget(node, C)) || C <- T],
                       set_ha_policy(Cfg, <<".*">>, {<<"nodes">>, Nodes}).

open_incapable_channel(NodePort) ->
    Props = [{<<"capabilities">>, table, []}],
    {ok, ConsConn} =
        amqp_connection:start(#amqp_params_network{port              = NodePort,
                                                   client_properties = Props}),
    {ok, Ch} = amqp_connection:open_channel(ConsConn),
    Ch.
