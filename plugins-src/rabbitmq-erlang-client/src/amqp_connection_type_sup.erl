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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

%% @private
-module(amqp_connection_type_sup).

-include("amqp_client_internal.hrl").

-behaviour(supervisor2).

-export([start_link/0, start_infrastructure_fun/3, type_module/1]).

-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------
%% amqp客户端连接类型的监督进程启动入口
start_link() ->
	supervisor2:start_link(?MODULE, []).



type_module(#amqp_params_direct{})  -> {direct, amqp_direct_connection};

type_module(#amqp_params_network{}) -> {network, amqp_network_connection}.

%%---------------------------------------------------------------------------
%% 启动频道管理相关进程
start_channels_manager(Sup, Conn, ConnName, Type) ->
	%% 启动频道监督进程的监督进程
	{ok, ChSupSup} = supervisor2:start_child(
					   Sup,
					   {channel_sup_sup, {amqp_channel_sup_sup, start_link,
										  [Type, Conn, ConnName]},
						intrinsic, infinity, supervisor,
						[amqp_channel_sup_sup]}),
	%% 启动频道管理进程
	{ok, _} = supervisor2:start_child(
				Sup,
				{channels_manager, {amqp_channels_manager, start_link,
									[Conn, ConnName, ChSupSup]},
				 transient, ?MAX_WAIT, worker, [amqp_channels_manager]}).


%% 得到基础设施函数
start_infrastructure_fun(Sup, Conn, network) ->
	fun (Sock, ConnName) ->
			 %% 启动频道管理相关进程
			 {ok, ChMgr} = start_channels_manager(Sup, Conn, ConnName, network),
			 {ok, AState} = rabbit_command_assembler:init(?PROTOCOL),
			 %% 启动写进程，该进程主要是用来发消息给RabbitMQ服务器
			 {ok, Writer} =
				 supervisor2:start_child(
				   Sup,
				   {writer,
					{rabbit_writer, start_link,
					 [Sock, 0, ?FRAME_MIN_SIZE, ?PROTOCOL, Conn, ConnName]},
					transient, ?MAX_WAIT, worker, [rabbit_writer]}),
			 %% 启动读进程，该进程主要用来从Socket读取服务端发送过来的数据，然后进行解析操作
			 {ok, _Reader} =
				 supervisor2:start_child(
				   Sup,
				   {main_reader, {amqp_main_reader, start_link,
								  [Sock, Conn, ChMgr, AState, ConnName]},
					transient, ?MAX_WAIT, worker, [amqp_main_reader]}),
			 {ok, ChMgr, Writer}
	end;

start_infrastructure_fun(Sup, Conn, direct) ->
	fun (ConnName) ->
			 {ok, ChMgr} = start_channels_manager(Sup, Conn, ConnName, direct),
			 {ok, Collector} =
				 supervisor2:start_child(
				   Sup,
				   {collector, {rabbit_queue_collector, start_link, [ConnName]},
					transient, ?MAX_WAIT, worker, [rabbit_queue_collector]}),
			 {ok, ChMgr, Collector}
	end.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
	{ok, {{one_for_all, 0, 1}, []}}.
