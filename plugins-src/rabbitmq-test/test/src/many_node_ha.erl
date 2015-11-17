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
-module(many_node_ha).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_test_util, [a2b/1]).
-import(rabbit_misc, [pget/2]).

kill_intermediate_with() ->
    fun (Cfg) -> rabbit_test_configs:ha_policy_all(
                   rabbit_test_configs:cluster(Cfg, [a,b,c,d,e,f]))
    end.
kill_intermediate([CfgA, CfgB, CfgC, CfgD, CfgE, CfgF]) ->
    Msgs            = rabbit_test_configs:cover_work_factor(20000, CfgA),
    MasterChannel   = pget(channel, CfgA),
    ConsumerChannel = pget(channel, CfgE),
    ProducerChannel = pget(channel, CfgF),
    Queue = <<"test">>,
    amqp_channel:call(MasterChannel, #'queue.declare'{queue       = Queue,
                                                      auto_delete = false}),

    %% TODO: this seems *highly* timing dependant - the assumption being
    %% that the kill will work quickly enough that there will still be
    %% some messages in-flight that we *must* receive despite the intervening
    %% node deaths. It would be nice if we could find a means to do this
    %% in a way that is not actually timing dependent.

    %% Worse still, it assumes that killing the master will cause a
    %% failover to Slave1, and so on. Nope.

    ConsumerPid = rabbit_ha_test_consumer:create(ConsumerChannel,
                                                 Queue, self(), false, Msgs),

    ProducerPid = rabbit_ha_test_producer:create(ProducerChannel,
                                                 Queue, self(), false, Msgs),

    %% create a killer for the master and the first 3 slaves
    [rabbit_test_util:kill_after(Time, Cfg, sigkill) ||
        {Cfg, Time} <- [{CfgA, 50},
                        {CfgB, 50},
                        {CfgC, 100},
                        {CfgD, 100}]],

    %% verify that the consumer got all msgs, or die, or time out
    rabbit_ha_test_producer:await_response(ProducerPid),
    rabbit_ha_test_consumer:await_response(ConsumerPid),
    ok.

