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
-module(amqp_main_reader).

-include("amqp_client_internal.hrl").

-behaviour(gen_server).

-export([start_link/5]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {
				sock,													%% 当前连接的Socket
				connection,												%% 当前连接进程的Pid
				channels_manager,										%% channel的管理进程的Pid
				astate,													%% 组装消息协议解析编码处理文件
				message = none %% none | {Type, Channel, Length}
			   }).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------
%% amqp_main_reader进程的启动入口函数
start_link(Sock, Connection, ChMgr, AState, ConnName) ->
	gen_server:start_link(
	  ?MODULE, [Sock, Connection, ConnName, ChMgr, AState], []).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------
%% amqp_main_reader进程的启动回调初始化函数
init([Sock, Connection, ConnName, ChMgr, AState]) ->
	?store_proc_name(ConnName),
	State = #state{sock             = Sock,
				   connection       = Connection,
				   channels_manager = ChMgr,
				   astate           = AState,
				   message          = none},
	%% amqp连接进程进行异步监听socket数据的到来(0表示一次性取得Socket里面的全部数据)
	case rabbit_net:async_recv(Sock, 0, infinity) of
		{ok, _}         -> {ok, State};
		{error, Reason} -> {stop, Reason, _} = handle_error(Reason, State),
						   {stop, Reason}
	end.


terminate(_Reason, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


handle_call(Call, From, State) ->
	{stop, {unexpected_call, Call, From}, State}.


handle_cast(Cast, State) ->
	{stop, {unexpected_cast, Cast}, State}.


%% Socket中的消息到来的处理
handle_info({inet_async, Sock, _, {ok, Data}},
			State = #state {sock = Sock}) ->
	%% Latency hiding: Request next packet first, then process data
	case rabbit_net:async_recv(Sock, 0, infinity) of
		{ok, _}         -> handle_data(Data, State);
		{error, Reason} -> handle_error(Reason, State)
	end;

handle_info({inet_async, Sock, _, {error, Reason}},
			State = #state{sock = Sock}) ->
	handle_error(Reason, State).


%% 处理从Socket中取得的Frame数据，此处还一次性取得了个完整的Frame消息数据
handle_data(<<Type:8, Channel:16, Length:32, Payload:Length/binary, ?FRAME_END,
			  More/binary>>,
			#state{message = none} = State) when
  Type =:= ?FRAME_METHOD; Type =:= ?FRAME_HEADER;
  Type =:= ?FRAME_BODY;   Type =:= ?FRAME_HEARTBEAT ->
	%% Optimisation(优化) for the direct match
	handle_data(
	  More, process_frame(Type, Channel, Payload, State#state{message = none}));

%% 处理从Socket中取得的Frame数据，但是此处没有一次性的取得一个完整的Frame数据消息，因此将先得到的数据储存起来等待后续数据的到来
handle_data(<<Type:8, Channel:16, Length:32, Data/binary>>,
			#state{message = none} = State) when
  Type =:= ?FRAME_METHOD; Type =:= ?FRAME_HEADER;
  Type =:= ?FRAME_BODY;   Type =:= ?FRAME_HEARTBEAT ->
	{noreply, State#state{message = {Type, Channel, Length, Data}}};

%% 此处是收到客户端发送的handshake的消息
handle_data(<<"AMQP", A, B, C>>, #state{sock = Sock, message = none} = State) ->
	{ok, <<D>>} = rabbit_net:sync_recv(Sock, 1),
	handle_error({refused, {A, B, C, D}}, State);

handle_data(<<Malformed:7/binary, _Rest/binary>>,
			#state{message = none} = State) ->
	handle_error({malformed_header, Malformed}, State);

handle_data(<<Data/binary>>, #state{message = none} = State) ->
	{noreply, State#state{message = {expecting_header, Data}}};

%% 此处是前面没有收到完整的消息，此处后续的数据到达，拼接后成为完整的Frame数据
handle_data(Data, #state{message = {Type, Channel, L, OldData}} = State) ->
	case <<OldData/binary, Data/binary>> of
		<<Payload:L/binary, ?FRAME_END, More/binary>> ->
			handle_data(More,
						process_frame(Type, Channel, Payload,
									  State#state{message = none}));
		NotEnough ->
			%% Read in more data from the socket
			{noreply, State#state{message = {Type, Channel, L, NotEnough}}}
	end;

handle_data(Data,
			#state{message = {expecting_header, Old}} = State) ->
	handle_data(<<Old/binary, Data/binary>>, State#state{message = none});


%% 已经将从RabbitMQ系统得到的数据处理完毕
handle_data(<<>>, State) ->
	{noreply, State}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------
%% 处理RabbitMQ系统发送过来的消息
process_frame(Type, ChNumber, Payload,
			  State = #state{connection       = Connection,
							 channels_manager = ChMgr,
							 astate           = AState}) ->
	case rabbit_command_assembler:analyze_frame(Type, Payload, ?PROTOCOL) of
		heartbeat when ChNumber /= 0 ->
			amqp_gen_connection:server_misbehaved(
			  Connection,
			  #amqp_error{name        = command_invalid,
						  explanation = "heartbeat on non-zero channel"}),
			State;
		%% Match heartbeats but don't do anything with them
		heartbeat ->
			State;
		AnalyzedFrame when ChNumber /= 0 ->
			%% channel不为0的情况则通过amqp_channel_manager进程发送到对应的amqp_channel进程进行处理
			amqp_channels_manager:pass_frame(ChMgr, ChNumber, AnalyzedFrame),
			State;
		AnalyzedFrame ->
			%% 如果channel为0则默认发送给amqp_gen_connection进程进行处理
			State#state{astate = amqp_channels_manager:process_channel_frame(
								   AnalyzedFrame, 0, Connection, AState)}
	end.


%% 处理异常数据的接口
handle_error(closed, State = #state{connection = Conn}) ->
	Conn ! socket_closed,
	{noreply, State};


handle_error({refused, Version},  State = #state{connection = Conn}) ->
	Conn ! {refused, Version},
	{noreply, State};


handle_error({malformed_header, Version},  State = #state{connection = Conn}) ->
	Conn ! {malformed_header, Version},
	{noreply, State};


handle_error(Reason, State = #state{connection = Conn}) ->
	Conn ! {socket_error, Reason},
	{stop, {socket_error, Reason}, State}.
