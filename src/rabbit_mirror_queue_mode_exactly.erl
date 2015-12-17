%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_mode_exactly).

%% 将队列在RabbitMQ集群中指定个数的节点上进行镜像，节点的个数由ha-params指定

-include("rabbit.hrl").

-behaviour(rabbit_mirror_queue_mode).

-export([description/0, suggested_queue_nodes/5, validate_policy/1]).

-rabbit_boot_step({?MODULE,
				   [{description, "mirror mode exactly"},
					{mfa,         {rabbit_registry, register,
								   [ha_mode, <<"exactly">>, ?MODULE]}},
					{requires,    rabbit_registry},
					{enables,     kernel_ready}]}).

description() ->
	[{description, <<"Mirror queue to a specified number of nodes">>}].

%% When we need to add nodes, we randomise our candidate list as a
%% crude form of load-balancing. TODO it would also be nice to
%% randomise the list of ones to remove when we have too many - we
%% would have to take account of synchronisation though.
%% 获取队列的镜像队列所在的节点列表(rabbit_mirror_queue_mode_exactly模式表示在指定数量的节点上启动该队列的镜像队列)
suggested_queue_nodes(Count, MNode, SNodes, _SSNodes, Poss) ->
	%% 出去主节点上的镜像队列数量后，SCount表示要在这么多节点上启动镜像队列
	SCount = Count - 1,
	{MNode, case SCount > length(SNodes) of
				true  -> Cand = shuffle((Poss -- [MNode]) -- SNodes),
						 %% 将还没有启动镜像的节点加入的老的镜像队列节点列表中
						 SNodes ++ lists:sublist(Cand, SCount - length(SNodes));
				%% 截取前SCount个节点
				false -> lists:sublist(SNodes, SCount)
			end}.


%% 对还没有启动镜像队列的节点进行均匀洗牌
shuffle(L) ->
	{A1, A2, A3} = now(),
	random:seed(A1, A2, A3),
	{_, L1} = lists:unzip(lists:keysort(1, [{random:uniform(), N} || N <- L])),
	L1.


validate_policy(N) when is_integer(N) andalso N > 0 ->
	ok;
validate_policy(Params) ->
	{error, "ha-mode=\"exactly\" takes an integer, ~p given", [Params]}.
