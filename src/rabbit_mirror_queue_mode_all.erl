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

-module(rabbit_mirror_queue_mode_all).

%% 将队列在RabbitMQ集群中的所有节点上做镜像队列

-include("rabbit.hrl").

-behaviour(rabbit_mirror_queue_mode).

-export([description/0, suggested_queue_nodes/5, validate_policy/1]).

-rabbit_boot_step({?MODULE,
				   [{description, "mirror mode all"},
					{mfa,         {rabbit_registry, register,
								   [ha_mode, <<"all">>, ?MODULE]}},
					{requires,    rabbit_registry},
					{enables,     kernel_ready}]}).


description() ->
	[{description, <<"Mirror queue to all nodes">>}].


%% 获取队列的镜像队列所在的节点列表(rabbit_mirror_queue_mode_all模式表示队列的镜像队列存在于RabbitMQ集群中的每一个节点)
suggested_queue_nodes(_Params, MNode, _SNodes, _SSNodes, Poss) ->
	%% 集群中的所有节点将主队列的节点排除，得到副镜像队列启动的节点列表
	{MNode, Poss -- [MNode]}.


validate_policy(none) ->
	ok;
validate_policy(_Params) ->
	{error, "ha-mode=\"all\" does not take parameters", []}.
