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

-module(rabbit_mirror_queue_mode_nodes).

%% 将队列在RabbitMQ集群中指定的节点上进行镜像，节点名称通过ha-params指定

-include("rabbit.hrl").

-behaviour(rabbit_mirror_queue_mode).

-export([description/0, suggested_queue_nodes/5, validate_policy/1]).

-rabbit_boot_step({?MODULE,
				   [{description, "mirror mode nodes"},
					{mfa,         {rabbit_registry, register,
								   [ha_mode, <<"nodes">>, ?MODULE]}},
					{requires,    rabbit_registry},
					{enables,     kernel_ready}]}).


description() ->
	[{description, <<"Mirror queue to specified nodes">>}].


%% 获取队列的镜像队列所在的节点列表(rabbit_mirror_queue_mode_nodes类型表示在指定的节点列表中启动队列的镜像队列)
suggested_queue_nodes(Nodes0, MNode, _SNodes, SSNodes, Poss) ->
	%% 将传入的节点列表格式化
	Nodes1 = [list_to_atom(binary_to_list(Node)) || Node <- Nodes0],
	%% If the current master is not in the nodes specified, then what we want
	%% to do depends on whether there are any synchronised slaves. If there
	%% are then we can just kill the current master - the admin has asked for
	%% a migration and we should give it to them. If there are not however
	%% then we must keep the master around so as not to lose messages.
	Nodes = case SSNodes of
				[] -> lists:usort([MNode | Nodes1]);
				_  -> Nodes1
			end,
	%% 得到需要启动的节点，但是该节点没有在运行中的节点列表
	Unavailable = Nodes -- Poss,
	%% 获得需要启动镜像队列且该节点正在运行中
	Available = Nodes -- Unavailable,
	case Available of
		[] -> %% We have never heard of anything? Not much we can do but
			%% keep the master alive.
			{MNode, []};
		_  -> case lists:member(MNode, Available) of
				  true  -> {MNode, Available -- [MNode]};
				  false -> %% Make sure the new master is synced! In order to
					  %% get here SSNodes must not be empty.
					  [NewMNode | _] = SSNodes,
					  %% 获取新的主镜像节点和要启动的副镜像节点列表
					  {NewMNode, Available -- [NewMNode]}
			  end
	end.


validate_policy([]) ->
	{error, "ha-mode=\"nodes\" list must be non-empty", []};
validate_policy(Nodes) when is_list(Nodes) ->
	case [I || I <- Nodes, not is_binary(I)] of
		[]      -> ok;
		Invalid -> {error, "ha-mode=\"nodes\" takes a list of strings, "
						"~p was not a string", [Invalid]}
	end;
validate_policy(Params) ->
	{error, "ha-mode=\"nodes\" takes a list, ~p given", [Params]}.
