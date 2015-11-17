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

-module(rabbit_reader).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/1, info_keys/0, info/1, info/2, force_event_refresh/2,
         shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/2, mainloop/4, recvloop/4]).

-export([conserve_resources/3, server_properties/1]).

-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 30).										%% rabbit_reader进程向自己发送终止消息的最大秒数
-define(CHANNEL_TERMINATION_TIMEOUT, 3).							%% 给每个channel进程shutdown操作的秒数
-define(SILENT_CLOSE_DELAY, 3).										%% 延迟关闭rabbit_reader进程的秒数
-define(CHANNEL_MIN, 1).

%%--------------------------------------------------------------------------
%% 当前rabbit_reader进程中的基本信息
-record(v1, {
			 parent,												%% Parent为rabbit_connection_sup监督进程Pid
			 sock,													%% 客户端连接过来的socket
			 connection,											%% 连接信息，就是下面connection数据结构
			 callback,												%% 用来区分当前rabbit_reader进程的处理流程的字段
			 recv_len,												%% 当前操作需要从客户端接收的数据大小
			 pending_recv,											%% 当前是否在等待接收客户端发送数据的状态
			 connection_state,										%% 同客户端连接过程中的状态
			 helper_sup,											%% rabbit_connection_helper_sup监督进程的Pid
			 queue_collector,										%% rabbit_queue_collector进程Pid(拥有者为当前rabbit_reader进程的队列进程需要向rabbit_queue_collector进程注册)
			 heartbeater,											%% 检查发送心跳消息的进程和检查接收心跳的进程的两个Pid
			 stats_timer,											%% 根据配置信息向rabbit_event发送信息的数据结构
			 channel_sup_sup_pid,									%% rabbit_channel_sup_sup监督进程的Pid
			 channel_count,											%% 当前连接下实际的rabbit_channel进程的个数
			 throttle												%% 存储控制rabbit_reader进程阻塞的数据结构
			}).

%% 当前rabbit_reader进程中连接的详细信息
-record(connection, {
					 name,											%% 当前连接进程的名字(通过客户端的端口信息和RabbitMQ系统的端口信息组装)
					 host,											%% RabbitMQ系统自己监听的Host
					 peer_host,										%% 客户端的Host
					 port,											%% RabbitMQ系统自己监听的端口
					 peer_port,										%% 客户端连接过来的端口
					 protocol,										%% AMQP协议使用的模块(rabbit_framing_amqp_0_9_1，该模块的代码是工具生成)
					 user,											%% 用户名字
					 timeout_sec,									%% 超时的时间(单位秒)
					 frame_max,										%% RabbitMQ系统自定义的数据包的最大数量
					 channel_max,									%% 当前rabbit_reader连接进程中channel的最大值，0表示最大为65535
					 vhost,											%% RabbitMQ系统中的Vhost
					 client_properties,								%% 客户端的连接属性
					 capabilities,									%% capabilities:能力，客户端通过connection.start_ok消息发送上来的客户端支持的功能
					 auth_mechanism,								%% 验证名字和对应的模块，例如：{'PLAIN', rabbit_auth_mechanism_plain}, {'AMQPLAIN', rabbit_auth_mechanism_amqplain}
					 auth_state,									%% 存储用户登陆验证信息的字段
					 connected_at									%% 客户端连接过来的时间
					}).

%% rabbit_reader进程阻塞的数据结构(阻塞包括系统资源耗尽或者消息流处理不过来导致系统阻塞)
-record(throttle, {
				   alarmed_by,										%% 当前集群中所有的报警信息
				   last_blocked_by,									%% 当前rabbit_reader进程最后一次被阻塞的原因(resource表示资源阻塞，flow表示消息流阻塞)
				   last_blocked_at									%% 当前rabbit_reader进程最后一次被祖苏的时间
				  }).

%% 统计当前rabbit_reader进程关键信息的key列表(该关键信息用来将当前rabbit_reader进程的关键信息发送到rabbit_event事件中心)
-define(STATISTICS_KEYS, [pid, recv_oct, recv_cnt, send_oct, send_cnt,
                          send_pend, state, channels]).

%% 当前rabbit_reader进程创建的时候将信息计入日志中的key列表
-define(CREATION_EVENT_KEYS,
        [pid, name, port, peer_port, host,
        peer_host, ssl, peer_cert_subject, peer_cert_issuer,
        peer_cert_validity, auth_mechanism, ssl_protocol,
        ssl_key_exchange, ssl_cipher, ssl_hash, protocol, user, vhost,
        timeout, frame_max, channel_max, client_properties, connected_at]).

%% rabbit_reader进程关键的所有信息key列表
-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

%% rabbit_reader进程中用户验证的时候需要将rabbit_reader进程中的关键信息发布到rabbit_event事件中心的关键key列表
-define(AUTH_NOTIFICATION_INFO_KEYS,
        [host, vhost, name, peer_host, peer_port, protocol, auth_mechanism,
         ssl, ssl_protocol, ssl_cipher, peer_cert_issuer, peer_cert_subject,
         peer_cert_validity]).

%% 判断当前rabbit_reader进程是否处于运行中的宏定义
-define(IS_RUNNING(State),
        (State#v1.connection_state =:= running orelse
         State#v1.connection_state =:= blocking orelse
         State#v1.connection_state =:= blocked)).

%% 判断当前rabbit_reader进程是否将要关闭的状态
-define(IS_STOPPING(State),
        (State#v1.connection_state =:= closing orelse
         State#v1.connection_state =:= closed)).

%%--------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (pid()) -> rabbit_types:ok(pid())).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(info/2 :: (pid(), rabbit_types:info_keys()) -> rabbit_types:infos()).
-spec(force_event_refresh/2 :: (pid(), reference()) -> 'ok').
-spec(shutdown/2 :: (pid(), string()) -> 'ok').
-spec(conserve_resources/3 :: (pid(), atom(), boolean()) -> 'ok').
-spec(server_properties/1 :: (rabbit_types:protocol()) ->
                                  rabbit_framing:amqp_table()).

%% These specs only exists to add no_return() to keep dialyzer happy
-spec(init/2 :: (pid(), pid()) -> no_return()).
-spec(start_connection/5 ::
        (pid(), pid(), any(), rabbit_net:socket(),
         fun ((rabbit_net:socket()) ->
                     rabbit_types:ok_or_error2(
                       rabbit_net:socket(), any()))) -> no_return()).

-spec(mainloop/4 :: (_,[binary()], non_neg_integer(), #v1{}) -> any()).
-spec(system_code_change/4 :: (_,_,_,_) -> {'ok',_}).
-spec(system_continue/3 :: (_,_,{[binary()], non_neg_integer(), #v1{}}) ->
                                any()).
-spec(system_terminate/4 :: (_,_,_,_) -> none()).

-endif.

%%--------------------------------------------------------------------------
%% rabbit_reader进程的启动入口函数
%% intrinsic如果child非正常退出(normal, {shutdown, _}({shutdown,restart})这些都是正常退出，其他的都是异常退出，异常退出intrinsic会重启该子进程，否则会将该监督进程下的其他子进程删除掉)
%% supervisor也会退出并删除其他所有children。
%% rabbit_reader进程在rabbit_connection_sup监督进程下的重启类型是intrinsic，因此如果rabbit_reader进程正常终止，会将rabbit_connection_sup监督进程以及监督进程下的其他子进程删除
start_link(HelperSup) ->
	{ok, proc_lib:spawn_link(?MODULE, init, [self(), HelperSup])}.


%% 关闭pid为Pid的rabbit_reader进程
shutdown(Pid, Explanation) ->
	gen_server:call(Pid, {shutdown, Explanation}, infinity).


%% rabbit_reader进程的初始化回调函数(Parent为rabbit_connection_sup监督进程的Pid)
init(Parent, HelperSup) ->
	Deb = sys:debug_options([]),
	%% 阻塞等待go消息的到来
	receive
		{go, Sock, SockTransform} ->
			%% 启动一个连接
			start_connection(Parent, HelperSup, Deb, Sock, SockTransform)
	end.


system_continue(Parent, Deb, {Buf, BufLen, State}) ->
	mainloop(Deb, Buf, BufLen, State#v1{parent = Parent}).


system_terminate(Reason, _Parent, _Deb, _State) ->
	exit(Reason).


system_code_change(Misc, _Module, _OldVsn, _Extra) ->
	{ok, Misc}.


%% 拿到rabbit_reader进程中关键信息的key列表
info_keys() -> ?INFO_KEYS.


%% 列出当前rabbit_reader进程的详细信息
info(Pid) ->
	gen_server:call(Pid, info, infinity).


%% 列出当前rabbit_reader进程Items中key对应的信息
info(Pid, Items) ->
	case gen_server:call(Pid, {info, Items}, infinity) of
		{ok, Res}      -> Res;
		{error, Error} -> throw(Error)
	end.


%% 处理强制将当前rabbit_reader进程的创建信息重新发布到rabbit_event事件中心去
force_event_refresh(Pid, Ref) ->
	gen_server:cast(Pid, {force_event_refresh, Ref}).


%% conserve：保存
%% 通知当前rabbit_reader进程有系统资源被限制或者有系统资源被取消限制，这样可以用来让rabbit_reader进程进入阻塞或者取消阻塞
conserve_resources(Pid, Source, Conserve) ->
	Pid ! {conserve_resources, Source, Conserve},
	ok.


%% 组装RabbitMQ系统服务端特性，准备发送给客户端
server_properties(Protocol) ->
	%% id指的是RabbitMQ(系统产品名字)
	{ok, Product} = application:get_key(rabbit, id),
	%% 得到RabbitMQ系统当前的版本号
	{ok, Version} = application:get_key(rabbit, vsn),
	
	%% Get any configuration-specified server properties
	%% 从RabbitMQ系统配置文件中拿到配置的特性
	{ok, RawConfigServerProps} = application:get_env(rabbit,
													 server_properties),
	
	%% Normalize the simplifed (2-tuple) and unsimplified (3-tuple) forms
	%% from the config and merge them with the generated built-in properties
	NormalizedConfigServerProps =
		[{<<"capabilities">>, table, server_capabilities(Protocol)} |
			 [case X of
				  {KeyAtom, Value} -> {list_to_binary(atom_to_list(KeyAtom)),
									   longstr,
									   %% 将list转化为binary
									   maybe_list_to_binary(Value)};
				  {BinKey, Type, Value} -> {BinKey, Type, Value}
			  end || X <- RawConfigServerProps ++
						 [{product,      Product},
						  {version,      Version},
						  {cluster_name, rabbit_nodes:cluster_name()},
						  {platform,     "Erlang/OTP"},
						  {copyright,    ?COPYRIGHT_MESSAGE},
						  {information,  ?INFORMATION_MESSAGE}]]],
	
	%% Filter duplicated properties in favour of config file provided values
	%% 排序
	lists:usort(fun ({K1, _, _}, {K2, _, _}) -> K1 =< K2 end,
				NormalizedConfigServerProps).


%% 将list转化为binary
maybe_list_to_binary(V) when is_binary(V) -> V;

maybe_list_to_binary(V) when is_list(V)   -> list_to_binary(V).


%% 服务器能力列表
server_capabilities(rabbit_framing_amqp_0_9_1) ->
	[{<<"publisher_confirms">>,           bool, true},
	 {<<"exchange_exchange_bindings">>,   bool, true},
	 {<<"basic.nack">>,                   bool, true},
	 {<<"consumer_cancel_notify">>,       bool, true},
	 {<<"connection.blocked">>,           bool, true},
	 {<<"consumer_priorities">>,          bool, true},
	 {<<"authentication_failure_close">>, bool, true},
	 {<<"per_consumer_qos">>,             bool, true}];

server_capabilities(_) ->
	[].

%%--------------------------------------------------------------------------
%% 打印日志(根据配置文件中的connection日志配置等级进行打印日志)
log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).


%% 打印Socket错误的函数
socket_error(Reason) when is_atom(Reason) ->
	log(error, "Error on AMQP connection ~p: ~s~n",
		[self(), rabbit_misc:format_inet_error(Reason)]);

socket_error(Reason) ->
	log(error, "Error on AMQP connection ~p:~n~p~n", [self(), Reason]).


inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).


%% 对Socket执行一次Fun函数，返回对应的值
socket_op(Sock, Fun) ->
	case Fun(Sock) of
		{ok, Res}       -> Res;
		{error, Reason} -> socket_error(Reason),
						   %% NB: this is tcp socket, even in case of ssl
						   rabbit_net:fast_close(Sock),
						   exit(normal)
	end.


%% 启动一个连接
start_connection(Parent, HelperSup, Deb, Sock, SockTransform) ->
	%% 能够接收EXIT信号作为一般的进程间通信的信号进行接受并处理
	process_flag(trap_exit, true),
	%% 得到当前Socket连接工作进程的名字
	Name = case rabbit_net:connection_string(Sock, inbound) of
			   {ok, Str}         -> Str;
			   {error, enotconn} -> rabbit_net:fast_close(Sock),
									exit(normal);
			   {error, Reason}   -> socket_error(Reason),
									rabbit_net:fast_close(Sock),
									exit(normal)
		   end,
	%% 从配置文件中得到握手超时的时间
	{ok, HandshakeTimeout} = application:get_env(rabbit, handshake_timeout),
	ClientSock = socket_op(Sock, SockTransform),
	%% 启动握手超时的定时器
	erlang:send_after(HandshakeTimeout, self(), handshake_timeout),
	%% 拿到Socket连接的客户端的地址，端口和本地的地址和端口
	{PeerHost, PeerPort, Host, Port} =
		socket_op(Sock, fun (S) -> rabbit_net:socket_ends(S, inbound) end),
	%% 将当前Socket连接工作进程的名字写入进程字典process_name
	?store_proc_name(list_to_binary(Name)),
	%% 自己加的打印代码
	io:format("RabbitMQ Accept One Client Request:~p~n", [{Name, PeerHost, PeerPort, Host, Port}]),
	State = #v1{parent              = Parent,																	%% Parent为rabbit_connection_sup监督进程
				sock                = ClientSock,																%% 客户端连接过来的socket
				connection          = #connection{
												  name               = list_to_binary(Name),					%% 当前连接进程的名字
												  host               = Host,									%% RabbitMQ系统自己的Host
												  peer_host          = PeerHost,								%% 客户端的Host
												  port               = Port,									%% RabbitMQ系统自己监听的端口
												  peer_port          = PeerPort,								%% 客户端连接过来的端口
												  protocol           = none,									%% AMQP协议使用的模块
												  user               = none,									%% 用户名字
												  timeout_sec        = (HandshakeTimeout / 1000),				%% 连接进程握手超时的时间(单位秒)
												  frame_max          = ?FRAME_MIN_SIZE,							%% RabbitMQ系统自定义的数据包的最大数量
												  vhost              = none,									%% RabbitMQ系统中的Vhost
												  client_properties  = none,									%% 客户端的连接属性
												  capabilities       = [],										%% capabilities:能力
												  auth_mechanism     = none,
												  auth_state         = none,
												  connected_at       = rabbit_misc:now_to_ms(os:timestamp())	%% 客户端连接过来的时间
												 },
				callback            = uninitialized_callback,
				recv_len            = 0,																		%% 当前操作需要从客户端接收的数据大小
				pending_recv        = false,																	%% 当前是否在等待接收客户端发送数据的状态
				connection_state    = pre_init,																	%% 同客户端连接过程中的状态
				queue_collector     = undefined,  %% started on tune-ok
				helper_sup          = HelperSup,																%% rabbit_connection_helper_sup监督进程的Pid
				heartbeater         = none,
				channel_sup_sup_pid = none,
				channel_count       = 0,
				throttle            = #throttle{
												alarmed_by      = [],
												last_blocked_by = none,
												last_blocked_at = never}},
	try
		%% 进入主循环(先跟客户端进行握手操作)
		run({?MODULE, recvloop,
			 %% 首先进行握手handshake操作，首先要接收8个字节的数据
			 [Deb, [], 0, switch_callback(rabbit_event:init_stats_timer(
											State, #v1.stats_timer),
										  handshake, 8)]}),
		log(info, "closing AMQP connection ~p (~s)~n", [self(), Name])
	catch
		%% 当前rabbit_reader进程出现异常，则将异常信息根据connection日志配置将异常信息写入本地日志
		Ex -> log(case Ex of
				connection_closed_with_no_data_received -> debug;
				%% Socket突然中断的情况
				connection_closed_abruptly              -> warning;
				_                                       -> error
			end, "closing AMQP connection ~p (~s):~n~p~n",
			[self(), Name, Ex])
	after
		%% We don't call gen_tcp:close/1 here since it waits for
		%% pending output to be sent, which results in unnecessary
		%% delays. We could just terminate - the reader is the
		%% controlling process and hence its termination will close
		%% the socket. However, to keep the file_handle_cache
		%% accounting as accurate as possible we ought to close the
		%% socket w/o delay before termination.
		%% 将Socket立刻停止掉
		rabbit_net:fast_close(ClientSock),
		%% 通知rabbit_networking当前rabbit_reader进程取消注册
		rabbit_networking:unregister_connection(self()),
		%% 向rabbit_event事件中心发布connection关闭的事件
		rabbit_event:notify(connection_closed, [{pid, self()}])
	end,
	done.


%% 执行对应模块的对应函数
run({M, F, A}) ->
    try apply(M, F, A)
    catch {become, MFA} -> run(MFA)
    end.


%% 接收数据的主循环
%% 如果当前等待接收数据字段状态是true，则继续从socket接收数据
recvloop(Deb, Buf, BufLen, State = #v1{pending_recv = true}) ->
	mainloop(Deb, Buf, BufLen, State);

%% 当前连接被blocked时则不设置{active,once}，这个接收进程就阻塞在receive方法上，等待其他进程发送过来的消息，但是不接受Socket发送的数据
recvloop(Deb, Buf, BufLen, State = #v1{connection_state = blocked}) ->
	mainloop(Deb, Buf, BufLen, State);

recvloop(Deb, Buf, BufLen, State = #v1{connection_state = {become, F}}) ->
	throw({become, F(Deb, Buf, BufLen, State)});

%% 当缓冲区中的数据小于要接收到的数据则继续从socket接收数据
recvloop(Deb, Buf, BufLen, State = #v1{sock = Sock, recv_len = RecvLen})
  when BufLen < RecvLen ->
	%% 设置Socket主动接收一次数据
	case rabbit_net:setopts(Sock, [{active, once}]) of
		ok              -> mainloop(Deb, Buf, BufLen,
									State#v1{pending_recv = true});
		{error, Reason} -> stop(Reason, State)
	end;

%% 当前需要从socket接收的数据已经全部拿到，然后根据callback配置的参数进行相关的处理(第一次处理handshake消息的时候会直接入该接口),
%% 第一种情况，因为刚开始的时候，B=handshake消息，Buf的列表只有该一个元素，因此处理handshake消息
%% 第二种情况是，一次性从Socket取得了一个Frame结构，则通过下面的函数截取走Frame的前七个字节，此时Buf = [B]，B为Frame结构的除去前七个字节的后半部分数据，然后将B数据交由handle_input函数处理
recvloop(Deb, [B], _BufLen, State) ->
	{Rest, State1} = handle_input(State#v1.callback, B, State),
	recvloop(Deb, [Rest], size(Rest), State1);

%% 后续的任何消息Buf字段都是[接收的客户端发送过来的消息 | <<>>]的结构，因此会匹配到此处,先解析7个字节，该7个字节第一个字节为消息类型，第二三个字节为channel，再有四个字节表示后续数据的长度
%% 如果一次性只从Socket取得一个Frame结构，则在此处将前七个字节取走处理后，会调用到上面的函数，处理除去前七个字节的Frame数据
%% 如果一次性从Socket取得多个Frame结构，则还会走到此处，取得除去前七个字节的Frame数据进行处理
recvloop(Deb, Buf, BufLen, State = #v1{recv_len = RecvLen}) ->
	%% DataLRev是Frame结构定义好的前七个字节
	%% 此处有可能一次性从Socket中取得了多个Frame结构，
	%% 则第一次binlist_split函数得到最早到达的Frame结构的前RecvLen(7)个字节，同时RestLRev列表中顺序是按照时间顺序排列的，首先第一个Frame结构前七个字节已经被截取走到DataLRev
	%% 第二次执行到此处binlist_split函数截取到的DataLRev数据是最早到达的Frame结构出去前七个字节的后半部分，然后通过handle_input处理Frame结构实际的数据部分
	{DataLRev, RestLRev} = binlist_split(BufLen - RecvLen, Buf, []),
	Data = list_to_binary(lists:reverse(DataLRev)),
	{<<>>, State1} = handle_input(State#v1.callback, Data, State),
	recvloop(Deb, lists:reverse(RestLRev), BufLen - RecvLen, State1).


%% 将二进制数据分割成两份
binlist_split(0, L, Acc) ->
	{L, Acc};

binlist_split(Len, L, [Acc0 | Acc]) when Len < 0 ->
	{H, T} = split_binary(Acc0, -Len),
	{[H | L], [T | Acc]};

binlist_split(Len, [H | T], Acc) ->
	binlist_split(Len - size(H), T, [H | Acc]).


%% 主循环，接收客户端发送的数据
mainloop(Deb, Buf, BufLen, State = #v1{sock = Sock,
									   connection_state = CS,
									   connection = #connection{
																name = ConnName}}) ->
	%% 阻塞接收Socket或者其他进程发送过来的数据
	Recv = rabbit_net:recv(Sock),
	case CS of
		pre_init when Buf =:= [] ->
			%% We only log incoming connections when either the
			%% first byte was received or there was an error (eg. a
			%% timeout).
			%%
			%% The goal is to not log TCP healthchecks (a connection
			%% with no data received) unless specified otherwise.
			%% 当第一次接收到客户端数据或者有错误发生才会打印日志
			log(case Recv of
					closed -> debug;
					_      -> info
				end, "accepting AMQP connection ~p (~s)~n",
				[self(), ConnName]);
		_ ->
			ok
	end,
	case Recv of
		%% 次数是处理收到的Socket的数据
		{data, Data} ->
			%% 收到数据后同时将等待接收数据的状态重置为false(如果接受数据失败，则下次recvloop循环会立刻进入该函数继续接受Socket数据)
			recvloop(Deb, [Data | Buf], BufLen + size(Data),
					 State#v1{pending_recv = false});
		closed when State#v1.connection_state =:= closed ->
			ok;
		closed ->
			stop(closed, State);
		%% 处理错误消息
		{error, Reason} ->
			stop(Reason, State);
		%% 处理系统消息
		{other, {system, From, Request}} ->
			sys:handle_system_msg(Request, From, State#v1.parent,
								  ?MODULE, Deb, {Buf, BufLen, State});
		%% 此处是处理其他进程向rabbit_reader进程发送的消息
		{other, Other}  ->
			%% 处理rabbit_reader进程接收到的其他消息
			case handle_other(Other, State) of
				%% reader进程直接stop掉，不在执行后续操作
				stop     -> ok;
				NewState -> recvloop(Deb, Buf, BufLen, NewState)
			end
	end.


%% 以下是Socket出错导致的rabbit_reader进程的终止
%% Socket关闭的时候，当前rabbit_reader进程仍然处于pre_init状态
stop(closed, #v1{connection_state = pre_init} = State) ->
	%% The connection was closed before any packet was received. It's
	%% probably a load-balancer healthcheck: don't consider this a
	%% failure.
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	throw(connection_closed_with_no_data_received);

%% abruptly：突然
%% 处理socket突然中断的逻辑
stop(closed, State) ->
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 扔出connection_closed_abruptly信息
	throw(connection_closed_abruptly);

%% 处理其他出错的原因
stop(Reason, State) ->
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 抛出Reason异常信息
	throw({inet_error, Reason}).


%% 处理rabbit_reader进程接收到的其他消息(Conserve为true，表示增加报警信息Source，如果Conserve为false，则表示删除报警信息Source)
handle_other({conserve_resources, Source, Conserve},
			 State = #v1{throttle = Throttle = #throttle{alarmed_by = CR}}) ->
	CR1 = case Conserve of
			  true  -> lists:usort([Source | CR]);
			  false -> CR -- [Source]
		  end,
	%% 根据当前集群的资源报警信息和当前连接进程rabbit_reader消息流的阻塞信息，用来阻塞或者让当前进程运行
	State1 = control_throttle(
			   State#v1{throttle = Throttle#throttle{alarmed_by = CR1}}),
	%% 根据报警信息进行阻塞，如果有报警信息则当前rabbit_reader进程处于锁住状态，否则非锁住状态
	case {blocked_by_alarm(State), blocked_by_alarm(State1)} of
		%% 当前rabbit_reader进程进入阻塞状态
		{false, true} -> ok = send_blocked(State1);
		%% 当前rabbit_reader进程解除解锁状态
		{true, false} -> ok = send_unblocked(State1);
		{_,        _} -> ok
	end,
	State1;

%% 处理channel进程关闭的消息
handle_other({channel_closing, ChPid}, State) ->
	%% 通知rabbit_channel进程准备关闭
	ok = rabbit_channel:ready_for_close(ChPid),
	%% 将关闭的ChPid进程在当前rabbit_reader进程中的信息清除掉
	{_, State1} = channel_cleanup(ChPid, State),
	%% 根据当前集群的资源报警信息和当前连接进程rabbit_reader消息流的阻塞信息，用来阻塞或者让当前进程运行
	maybe_close(control_throttle(State1));

%% 接收rabbit_connection_sup监督进程DOWN掉的消息
handle_other({'EXIT', Parent, Reason}, State = #v1{parent = Parent}) ->
	%% 中断当前连接rabbit_reader，如果客户端有配置，则将异常信息发送给客户端(主动将当前连接上的所有rabbit_channel全部关闭掉)
	terminate(io_lib:format("broker forced connection closure "
								"with reason '~w'", [Reason]), State),
	%% this is what we are expected to do according to
	%% http://www.erlang.org/doc/man/sys.html
	%%
	%% If we wanted to be *really* nice we should wait for a while for
	%% clients to close the socket at their end, just as we do in the
	%% ordinary error case. However, since this termination is
	%% initiated by our parent it is probably more important to exit
	%% quickly.
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 立刻将rabbit_reader进程退出
	exit(Reason);

%% 处理rabbit_writer进程发送数据失败的消息(挂掉的原因是rabbit_writer进程发送数据失败)
handle_other({channel_exit, _Channel, E = {writer, send_failed, _E}}, State) ->
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 然后抛出异常信息，将当前连接关掉
	throw(E);

%% 处理rabbit_channel出现异常的消息
handle_other({channel_exit, Channel, Reason}, State) ->
	%% 发送异常信息，将当前连接关闭掉
	handle_exception(State, Channel, Reason);

%% 处理rabbit_channel进程DOWN掉的消息
handle_other({'DOWN', _MRef, process, ChPid, Reason}, State) ->
	%% 处理rabbit_channel进程ChPid DOWN掉的消息
	handle_dependent_exit(ChPid, Reason, State);

%% 处理让rabbit_reader进程终止的terminate_connection消息
handle_other(terminate_connection, State) ->
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 然后将当前rabbit_reader进程终止掉
	stop;

%% 处理握手超时的消息(如果当前连接已经在运行中或者关闭中，则什么都不做)
handle_other(handshake_timeout, State)
  when ?IS_RUNNING(State) orelse ?IS_STOPPING(State) ->
	State;

%% 处理握手超时的消息(当前连接仍然处于等待握手状态)
handle_other(handshake_timeout, State) ->
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 当前连接rabbit_reader则抛出异常，然后关闭当前连接
	throw({handshake_timeout, State#v1.callback});

%% 处理心跳超时的消息，如果当前连接处于closed状态，则什么都不做
handle_other(heartbeat_timeout, State = #v1{connection_state = closed}) ->
	State;

%% 处理心跳超时的消息，则发布详细信息，然后抛出心跳超时异常，然后关闭掉连接
handle_other(heartbeat_timeout, State = #v1{connection_state = S}) ->
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 抛出心跳超时的异常消息heartbeat_timeout
	throw({heartbeat_timeout, S});

%% 处理同步将连接关闭的消息
handle_other({'$gen_call', From, {shutdown, Explanation}}, State) ->
	%% 中断当前连接rabbit_reader，如果客户端有配置，则将异常信息发送给客户端
	{ForceTermination, NewState} = terminate(Explanation, State),
	%% 回复给调用者
	gen_server:reply(From, ok),
	case ForceTermination of
		force  -> stop;
		normal -> NewState
	end;

%% 列出当前rabbit_reader进程的信息
handle_other({'$gen_call', From, info}, State) ->
	gen_server:reply(From, infos(?INFO_KEYS, State)),
	State;

%% 列出当前rabbit_reader进程Items中key对应的信息
handle_other({'$gen_call', From, {info, Items}}, State) ->
    gen_server:reply(From, try {ok, infos(Items, State)}
                           catch Error -> {error, Error}
                           end),
    State;

%% 处理强制将当前rabbit_reader进程的创建信息重新发布到rabbit_event事件中心去的消息
handle_other({'$gen_cast', {force_event_refresh, Ref}}, State)
  when ?IS_RUNNING(State) ->
	%% 将当前rabbit_reader进程的创建信息重新发布到rabbit_event事件中心去
	rabbit_event:notify(
	  connection_created,
	  [{type, network} | infos(?CREATION_EVENT_KEYS, State)], Ref),
	%% 重新初始化向rabbit_event事件中心发布事件的数据结构
	rabbit_event:init_stats_timer(State, #v1.stats_timer);

%% 异步处理force_event_refresh消息，则什么都不做
handle_other({'$gen_cast', {force_event_refresh, _Ref}}, State) ->
	%% Ignore, we will emit a created event once we start running.
	State;

%% 确保向rabbit_event事件中心发布消息的定时器启动
handle_other(ensure_stats, State) ->
	%% 确保向rabbit_event事件中心发布消息的定时器启动
	ensure_stats_timer(State);

%% 向rabbit_event事件中心发布自己当前的所有详细信息
handle_other(emit_stats, State) ->
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	emit_stats(State);

%% 处理接收到增加能够向下游进程rabbit_channel进程发送消息的数量
handle_other({bump_credit, Msg}, State) ->
	%% 处理接收到增加能够向下游进程rabbit_channel进程发送消息的数量
	credit_flow:handle_bump_msg(Msg),
	%% 处理判断当前rabbit_reader进程是否进入阻塞状态或者退出阻塞状态
	control_throttle(State);

%% 处理其他信息
handle_other(Other, State) ->
	%% internal error -> something worth dying for
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State),
	%% 将当前连接关闭掉
	exit({unexpected_message, Other}).


%% 交换callback，同时改变需要接收的客户端数据长度
switch_callback(State, Callback, Length) ->
	State#v1{callback = Callback, recv_len = Length}.


%% 中断当前连接rabbit_reader，如果客户端有配置，则将异常信息发送给客户端(主动将当前连接上的所有rabbit_channel全部关闭掉)
terminate(Explanation, State) when ?IS_RUNNING(State) ->
	{normal, handle_exception(State, 0,
							  rabbit_misc:amqp_error(
								connection_forced, Explanation, [], none))};

terminate(_Explanation, State) ->
	{force, State}.


%% 根据当前集群的资源报警信息和当前连接进程rabbit_reader消息流的阻塞信息，用来阻塞或者让当前进程运行
control_throttle(State = #v1{connection_state = CS, throttle = Throttle}) ->
	%% credit_flow:blocked()判断当前进程是否是阻塞状态(Throttled:节流)
	IsThrottled = ((Throttle#throttle.alarmed_by =/= []) orelse
					   credit_flow:blocked()),
	case {CS, IsThrottled} of
		%% 老状态在运行状态，新状态处于阻塞状态，则将当前rabbit_reader进程的状态置为blocking
		{running,   true} -> State#v1{connection_state = blocking};
		%% 老状态是blocking，新的状态处于非阻塞状态，将当前rabbit_reader进程的状态更新为running运行状态
		{blocking, false} -> State#v1{connection_state = running};
		%% 老状态是阻塞状态，新的状态则非阻塞的情况，取消心跳进程的暂停，将当前rabbit_reader进程的状态更新为running运行状态
		{blocked,  false} -> %% 将当前rabbit_reader进程的心跳进程取消暂停
							 ok = rabbit_heartbeat:resume_monitor(
									State#v1.heartbeater),
							 %% 将当前rabbit_reader进程的状态更新为running运行状态
							 State#v1{connection_state = running};
		%% 老状态是阻塞状态，新的状态仍然处于阻塞状态，则此次只是更新最新的阻塞原因
		{blocked,   true} -> State#v1{throttle = update_last_blocked_by(
												   Throttle)};
		{_,            _} -> State
	end.


%% 判断当前rabbit_reader进程是否进入阻塞状态
maybe_block(State = #v1{connection_state = blocking,
						throttle         = Throttle}) ->
	%% 将当前rabbit_reader连接进程对应的两个心跳进程暂停
	ok = rabbit_heartbeat:pause_monitor(State#v1.heartbeater),
	%% 将当前连接进程rabbit_reader进程的状态设置为blocked阻塞状态，同时更新最新的阻塞原因和阻塞时间
	State1 = State#v1{connection_state = blocked,
					  throttle = update_last_blocked_by(
								   Throttle#throttle{
													 %% 记录当前rabbit_reader进程阻塞的时间
													 last_blocked_at = erlang:now()})},
	case {blocked_by_alarm(State), blocked_by_alarm(State1)} of
		%% 如果当前rabbit_reader进程进入阻塞状态，则将阻塞的消息发送给客户端
		{false, true} -> ok = send_blocked(State1);
		{_,        _} -> ok
	end,
	State1;

maybe_block(State) ->
	State.


%% 根据报警信息进行阻塞，如果有报警信息则当前rabbit_reader进程处于锁住状态，否则非锁住状态
blocked_by_alarm(#v1{connection_state = blocked,
					 throttle         = #throttle{alarmed_by = CR}})
  when CR =/= [] ->
	true;

blocked_by_alarm(#v1{}) ->
	false.


%% 通知客户端当前RabbitMQ系统已经阻塞
send_blocked(#v1{throttle   = #throttle{alarmed_by = CR},
				 connection = #connection{protocol     = Protocol,
										  capabilities = Capabilities},
				 sock       = Sock}) ->
	%% 如果客户端的特性中设置有向客户端发送阻塞消息的特性connection.blocked，则向客户端发送当前连接已经处于阻塞状态
	case rabbit_misc:table_lookup(Capabilities, <<"connection.blocked">>) of
		{bool, true} ->
			RStr = string:join([atom_to_list(A) || A <- CR], " & "),
			Reason = list_to_binary(rabbit_misc:format("low on ~s", [RStr])),
			%% 通过Socket通知客户端当前RabbitMQ系统已经阻塞掉(将该消息发送到channel为0的通道)
			ok = send_on_channel0(Sock, #'connection.blocked'{reason = Reason},
								  Protocol);
		_ ->
			ok
	end.

%% 通知客户端当前RabbitMQ系统已经解除阻塞
send_unblocked(#v1{connection = #connection{protocol     = Protocol,
											capabilities = Capabilities},
				   sock       = Sock}) ->
	%% 如果客户端的特性中设置有向客户端发送阻塞消息的特性
	case rabbit_misc:table_lookup(Capabilities, <<"connection.blocked">>) of
		{bool, true} ->
			%% (将该消息发送到channel为0的通道)
			ok = send_on_channel0(Sock, #'connection.unblocked'{}, Protocol);
		_ ->
			ok
	end.


%% 更新当前阻塞的原因
update_last_blocked_by(Throttle = #throttle{alarmed_by = []}) ->
	%% 当前阻塞原因为消息流阻塞
	Throttle#throttle{last_blocked_by = flow};

update_last_blocked_by(Throttle) ->
	%% 当前阻塞原因为资源阻塞
	Throttle#throttle{last_blocked_by = resource}.

%%--------------------------------------------------------------------------
%% error handling / termination
%% 关闭connection操作，先将所有以当前rabbit_reader进程作为拥有者的消息队列停止掉，然后向自己定时发送终止的消息
close_connection(State = #v1{queue_collector = Collector,
							 connection = #connection{
													  timeout_sec = TimeoutSec}}) ->
	%% The spec says "Exclusive queues may only be accessed by the
	%% current connection, and are deleted when that connection
	%% closes."  This does not strictly imply synchrony, but in
	%% practice it seems to be what people assume.
	%% 将所有以当前rabbit_reader进程作为拥有者的消息队列停止掉
	rabbit_queue_collector:delete_all(Collector),
	%% We terminate the connection after the specified interval, but
	%% no later than ?CLOSING_TIMEOUT seconds.
	%% 向自己发送terminate_connection的消息
	erlang:send_after((if TimeoutSec > 0 andalso
							  TimeoutSec < ?CLOSING_TIMEOUT -> TimeoutSec;
						  true                          -> ?CLOSING_TIMEOUT
					   end) * 1000, self(), terminate_connection),
	%% 将rabbit_reader进程的状态设置为closed
	State#v1{connection_state = closed}.


%% 处理rabbit_channel进程ChPid DOWN掉的消息
handle_dependent_exit(ChPid, Reason, State) ->
	%% 将关闭的ChPid进程在当前rabbit_reader进程中的信息清除掉
	{Channel, State1} = channel_cleanup(ChPid, State),
	case {Channel, termination_kind(Reason)} of
		{undefined,   controlled} -> State1;
		{undefined, uncontrolled} -> exit({abnormal_dependent_exit,
										   ChPid, Reason});
		%% 处理rabbit_channel进程异常终止的消息
		{_,           controlled} -> maybe_close(control_throttle(State1));
		%% 处理rabbit_channel进程异常终止的消息
		{_,         uncontrolled} -> State2 = handle_exception(
												State1, Channel, Reason),
									 maybe_close(control_throttle(State2))
	end.


%% 将当前连接上启动的所有channel终止掉
terminate_channels(#v1{channel_count = 0} = State) ->
    State;

terminate_channels(#v1{channel_count = ChannelCount} = State) ->
	%% 对当前连接上启动的所有channel进程执行shutdown操作
    lists:foreach(fun rabbit_channel:shutdown/1, all_channels()),
	%% 获得所有channel进程shutdown操作的超时总时间
    Timeout = 1000 * ?CHANNEL_TERMINATION_TIMEOUT * ChannelCount,
	%% 启动定时器
    TimerRef = erlang:send_after(Timeout, self(), cancel_wait),
    wait_for_channel_termination(ChannelCount, TimerRef, State).


%% 等待channel进程执行shutdown操作的完成，如果完成，则将定时器停止
wait_for_channel_termination(0, TimerRef, State) ->
	case erlang:cancel_timer(TimerRef) of
		false -> receive
					 cancel_wait -> State
				 end;
		_     -> State
	end;

%% 等待channel进程执行shutdown操作的完成
wait_for_channel_termination(N, TimerRef,
							 State = #v1{connection_state = CS,
										 connection = #connection{
																  name  = ConnName,
																  user  = User,
																  vhost = VHost}}) ->
	receive
		{'DOWN', _MRef, process, ChPid, Reason} ->
			%% 将关闭的ChPid进程在当前rabbit_reader进程中的信息清除掉
			{Channel, State1} = channel_cleanup(ChPid, State),
			case {Channel, termination_kind(Reason)} of
				{undefined,    _} ->
					exit({abnormal_dependent_exit, ChPid, Reason});
				%% 此处是channel进程正常终止掉的处理逻辑
				{_,   controlled} ->
					%% 然后继续等待后续的channel进程的终止消息
					wait_for_channel_termination(N - 1, TimerRef, State1);
				%% 此处是channel进程异常终止的处理逻辑
				{_, uncontrolled} ->
					%% 则立刻打印本地日志
					log(error, "Error on AMQP connection ~p (~s, vhost: '~s',"
							" user: '~s', state: ~p), channel ~p:"
							"error while terminating:~n~p~n",
						[self(), ConnName, VHost, User#user.username,
						 CS, Channel, Reason]),
					%% 然后继续等待后续的channel进程的终止消息
					wait_for_channel_termination(N - 1, TimerRef, State1)
			end;
		%% 如果收到的是超时的消息
		cancel_wait ->
			%% 则将rabbit_reader进程使用channel_termination_timeout原因立刻终止
			exit(channel_termination_timeout)
	end.


%% 如果当前连接状态为closing，则将当前rabbit_reader进程关闭掉，同时发送connection.close_ok消息通知客户端
maybe_close(State = #v1{connection_state = closing,
						channel_count    = 0,
						connection       = #connection{protocol = Protocol},
						sock             = Sock}) ->
	NewState = close_connection(State),
	%% 发送connection.close_ok消息通知客户端
	ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
	NewState;

maybe_close(State) ->
	State.


termination_kind(normal) -> controlled;
termination_kind(_)      -> uncontrolled.


%% 根据配置文件中connection的日志配置打印RabbitMQ服务器本地日志
log_hard_error(#v1{connection_state = CS,
				   connection = #connection{
											name  = ConnName,
											user  = User,
											vhost = VHost}}, Channel, Reason) ->
	%% 打印日志(根据配置文件中的connection日志配置等级进行打印日志)
	log(error,
		"Error on AMQP connection ~p (~s, vhost: '~s',"
			" user: '~s', state: ~p), channel ~p:~n~p~n",
		[self(), ConnName, VHost, User#user.username, CS, Channel, Reason]).


%% 处理异常信息，如果当前rabbit_reader进程已经处于closed状态，则只进行本地日志的写入
handle_exception(State = #v1{connection_state = closed}, Channel, Reason) ->
	%% 根据配置文件中connection的日志配置打印RabbitMQ服务器本地日志
	log_hard_error(State, Channel, Reason),
	State;

%% 处理异常信息，如果当前rabbit_reader进程处于正在运行状态或者closing状态，则先写本地日志，然后将rabbit_reader进程终止，然后将异常信息发送给客户端
handle_exception(State = #v1{connection = #connection{protocol = Protocol},
							 connection_state = CS},
				 Channel, Reason)
  when ?IS_RUNNING(State) orelse CS =:= closing ->
	%% 根据配置文件中connection的日志配置打印RabbitMQ服务器本地日志
	log_hard_error(State, Channel, Reason),
	%% 获取将要向客户端发送的错误消息Method
	{0, CloseMethod} =
		rabbit_binary_generator:map_exception(Channel, Reason, Protocol),
	%% 先将当前rabbit_reader进程上启动的所有channel进程终止掉，然后关闭掉connection，先将所有以当前rabbit_reader进程作为拥有者的消息队列停止掉，然后向自己定时发送终止的消息
	State1 = close_connection(terminate_channels(State)),
	%% 将数据发送到channel为0上
	ok = send_on_channel0(State1#v1.sock, CloseMethod, Protocol),
	State1;

handle_exception(State, Channel, Reason) ->
	%% We don't trust the client at this point - force them to wait
	%% for a bit so they can't DOS us with repeated failed logins etc.
	timer:sleep(?SILENT_CLOSE_DELAY * 1000),
	throw({handshake_error, State#v1.connection_state, Channel, Reason}).

%% we've "lost sync" with the client and hence must not accept any
%% more input
%% fatal：致命的
fatal_frame_error(Error, Type, Channel, Payload, State) ->
	%% 处理Frame出错
	frame_error(Error, Type, Channel, Payload, State),
	%% grace period to allow transmission(传输) of error
	%% 让当前进程睡眠一段时间，好让错误消息发送到客户端
	timer:sleep(?SILENT_CLOSE_DELAY * 1000),
	%% 扔出错误信息
	throw(fatal_frame_error).


%% 处理frame出错的接口
frame_error(Error, Type, Channel, Payload, State) ->
	{Str, Bin} = payload_snippet(Payload),
	handle_exception(State, Channel,
					 rabbit_misc:amqp_error(frame_error,
											"type ~p, ~s octets = ~p: ~p",
											[Type, Str, Bin, Error], none)).


unexpected_frame(Type, Channel, Payload, State) ->
	{Str, Bin} = payload_snippet(Payload),
	handle_exception(State, Channel,
					 rabbit_misc:amqp_error(unexpected_frame,
											"type ~p, ~s octets = ~p",
											[Type, Str, Bin], none)).


payload_snippet(Payload) when size(Payload) =< 16 ->
	{"all", Payload};

payload_snippet(<<Snippet:16/binary, _/binary>>) ->
	{"first 16", Snippet}.

%%--------------------------------------------------------------------------
%% 创建频道(channel_max的值为0，则表示当前能够创建的channel为65535个,因为在RabbitMQ的消息协议里channel是两个字符表现)
%% 如果设置了chanel_max，该值不能超过65535，且设置后，创建的channel不能超过该值
create_channel(_Channel,
			   #v1{channel_count = ChannelCount,
				   connection    = #connection{channel_max = ChannelMax}})
  when ChannelMax /= 0 andalso ChannelCount >= ChannelMax ->
	{error, rabbit_misc:amqp_error(
	   not_allowed, "number of channels opened (~w) has reached the "
		   "negotiated channel_max (~w)",
	   [ChannelCount, ChannelMax], 'none')};

create_channel(Channel,
			   #v1{sock                = Sock,
				   queue_collector     = Collector,
				   channel_sup_sup_pid = ChanSupSup,
				   channel_count       = ChannelCount,
				   connection =
					   #connection{name         = Name,
								   protocol     = Protocol,
								   frame_max    = FrameMax,
								   user         = User,
								   vhost        = VHost,
								   capabilities = Capabilities}} = State) ->
	%% 启动rabbit_channel_sup监督进程以及它下面的子进程集
	{ok, _ChSupPid, {ChPid, AState}} =
		rabbit_channel_sup_sup:start_channel(
		  ChanSupSup, {tcp, Sock, Channel, FrameMax, self(), Name,
					   Protocol, User, VHost, Capabilities, Collector}),
	MRef = erlang:monitor(process, ChPid),
	%% 将rabbit_channel进程的PID存储在进程字典
	put({ch_pid, ChPid}, {Channel, MRef}),
	put({channel, Channel}, {ChPid, AState}),
	{ok, {ChPid, AState}, State#v1{channel_count = ChannelCount + 1}}.


%% 将关闭的ChPid进程在当前rabbit_reader进程中的信息清除掉
channel_cleanup(ChPid, State = #v1{channel_count = ChannelCount}) ->
	case get({ch_pid, ChPid}) of
		undefined       -> {undefined, State};
		{Channel, MRef} -> %% 清除流量控制相关的信息
						   credit_flow:peer_down(ChPid),
						   %% 清除Channel编号对应的ChPid接收消息相关
						   erase({channel, Channel}),
						   %% 清除当前进程对ChPid监视的标识
						   erase({ch_pid, ChPid}),
						   %% 解除对ChPid这个rabbit_channel进程的监视
						   erlang:demonitor(MRef, [flush]),
						   %% 将当前rabbit_reader进程的rabbit_channel进程数量减一
						   {Channel, State#v1{channel_count = ChannelCount - 1}}
	end.


%% 拿到当前连接上启动的所有channel进程的Pid列表
all_channels() -> [ChPid || {{ch_pid, ChPid}, _ChannelMRef} <- get()].

%%--------------------------------------------------------------------------
%% 处理默认频道是0的消息，且当前连接的状态是停止状态
handle_frame(Type, 0, Payload,
			 State = #v1{connection = #connection{protocol = Protocol}})
  when ?IS_STOPPING(State) ->
	case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
		{method, MethodName, FieldsBin} ->
			handle_method0(MethodName, FieldsBin, State);
		_Other -> State
	end;

%% 处理默认频道是0的消息
handle_frame(Type, 0, Payload,
			 State = #v1{connection = #connection{protocol = Protocol}}) ->
	%% 此处根据消息解析文件得到该消息的名字
	case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
		error     -> frame_error(unknown_frame, Type, 0, Payload, State);
		heartbeat -> State;
		{method, MethodName, FieldsBin} ->
			handle_method0(MethodName, FieldsBin, State);
		_Other    -> unexpected_frame(Type, 0, Payload, State)
	end;

%% 处理频道不是0的消息，且当前连接是正在运行中的状态
handle_frame(Type, Channel, Payload,
			 State = #v1{connection = #connection{protocol = Protocol}})
  when ?IS_RUNNING(State) ->
	case rabbit_command_assembler:analyze_frame(Type, Payload, Protocol) of
		error     -> frame_error(unknown_frame, Type, Channel, Payload, State);
		heartbeat -> unexpected_frame(Type, Channel, Payload, State);
		Frame     -> process_frame(Frame, Channel, State)
	end;

handle_frame(_Type, _Channel, _Payload, State) when ?IS_STOPPING(State) ->
	State;

handle_frame(Type, Channel, Payload, State) ->
	unexpected_frame(Type, Channel, Payload, State).


%% 处理框架消息(如果当前频道没有创建，则创建一个频道，该频道包括一颗进程树)
process_frame(Frame, Channel, State) ->
	%% 得到该Channel对应的rabbit_channel进程的Pid(如果没有则创建一个进程)
	ChKey = {channel, Channel},
	case (case get(ChKey) of
			  undefined -> create_channel(Channel, State);
			  Other     -> {ok, Other, State}
		  end) of
		{error, Error} ->
			handle_exception(State, Channel, Error);
		{ok, {ChPid, AState}, State1} ->
			%% 处理消息的流程是：先处理basic.publish消息，然后再处理content_header，然后再处理实际的消息内容msg_content
			case rabbit_command_assembler:process(Frame, AState) of
				{ok, NewAState} ->
					put(ChKey, {ChPid, NewAState}),
					post_process_frame(Frame, ChPid, State1);
				%% 处理没有带内容的消息
				{ok, Method, NewAState} ->
					%% 自己添加的打印客户端发送过来的消息
					rabbit_misc:print_client_send_msg([{Channel, Method}]),
					%% 没有消息内容的消息不进行流量控制
					rabbit_channel:do(ChPid, Method),
					put(ChKey, {ChPid, NewAState}),
					post_process_frame(Frame, ChPid, State1);
				%% 处理带有内容的消息
				{ok, Method, Content, NewAState} ->
					%% 自己添加的打印客户端发送过来的消息
					rabbit_misc:print_client_send_msg([{Channel, Method, Content}]),
					%% 带有进程间消息流量限制的发送消息和消息内容到rabbit_channnel进程
					rabbit_channel:do_flow(ChPid, Method, Content),
					put(ChKey, {ChPid, NewAState}),
					%% 先根据当前集群的资源报警信息和当前连接进程rabbit_reader消息流的阻塞信息，用来阻塞或者让当前进程运行，然后看看当前进程是否已经进入阻塞状态
					post_process_frame(Frame, ChPid, control_throttle(State1));
				{error, Reason} ->
					handle_exception(State1, Channel, Reason)
			end
	end.


post_process_frame({method, 'channel.close_ok', _}, ChPid, State) ->
	{_, State1} = channel_cleanup(ChPid, State),
	%% This is not strictly necessary, but more obviously
	%% correct. Also note that we do not need to call maybe_close/1
	%% since we cannot possibly be in the 'closing' state.
	control_throttle(State1);

%% 当有消息内容content_header的时候才判断自己rabbit_reader进程是否进入阻塞状态
post_process_frame({content_header, _, _, _, _}, _ChPid, State) ->
	maybe_block(State);

%% 当有消息内容content_bodyr的时候才判断自己rabbit_reader进程是否进入阻塞状态
post_process_frame({content_body, _}, _ChPid, State) ->
	maybe_block(State);

post_process_frame(_Frame, _ChPid, State) ->
	State.

%%--------------------------------------------------------------------------

%% We allow clients to exceed the frame size a little bit since quite
%% a few get it wrong - off-by 1 or 8 (empty frame size) are typical.
-define(FRAME_SIZE_FUDGE, ?EMPTY_FRAME_SIZE).

%% 消息内容的大小PayloadSize大于FrameMax，则通知客户端内容太大
handle_input(frame_header, <<Type:8, Channel:16, PayloadSize:32, _/binary>>,
			 State = #v1{connection = #connection{frame_max = FrameMax}})
  when FrameMax /= 0 andalso
		   PayloadSize > FrameMax - ?EMPTY_FRAME_SIZE + ?FRAME_SIZE_FUDGE ->
	%% 通知客户端消息的内容大小超过设置的上限，同时根据服务器connection日志配置进行本地日志打印
	fatal_frame_error(
	  {frame_too_large, PayloadSize, FrameMax - ?EMPTY_FRAME_SIZE},
	  Type, Channel, <<>>, State);

%% 处理普通的AMQP消息，消息直接以FRAME_END结尾(现在的逻辑不会走到这里)
handle_input(frame_header, <<Type:8, Channel:16, PayloadSize:32,
							 Payload:PayloadSize/binary, ?FRAME_END,
							 Rest/binary>>,
			 State) ->
	{Rest, ensure_stats_timer(handle_frame(Type, Channel, Payload, State))};

%% 处理消息头的步骤，经过该步骤，会立刻进入处理消息主体内容的步骤(此处先处理AMQP消息协议的前七个字节，然后再处理后面的数据)
handle_input(frame_header, <<Type:8, Channel:16, PayloadSize:32, Rest/binary>>,
			 State) ->
	{Rest, ensure_stats_timer(
	   %% 交换回调操作为frame_payload，即下次是立刻操作该Frame结构的后续内容
	   switch_callback(State,
					   {frame_payload, Type, Channel, PayloadSize},
					   %% 在此处加一是因为能包括消息尾部加入的FRAME_END这个结束符，处理除去Frame结构前七个字节的数据
					   PayloadSize + 1))};

%% 处理消息主体内容的步骤
handle_input({frame_payload, Type, Channel, PayloadSize}, Data, State) ->
	<<Payload:PayloadSize/binary, EndMarker, Rest/binary>> = Data,
	%% 如果结尾是FRAME_END则消息二进制的结构是正确的
	case EndMarker of
		?FRAME_END -> State1 = handle_frame(Type, Channel, Payload, State),
					  %% 继续处理后续的Frame结构，每次处理新的Frame消息，都是先取得前七个字节分析
					  {Rest, switch_callback(State1, frame_header, 7)};
		%% Frame结尾的数据不正确，则Frame结构出错，则执行fatal_frame_error操作，先写本地日志，然后当前rabbit_reader进程处在正在运行中或者closing则向客户端发送错误信息
		_          -> fatal_frame_error({invalid_frame_end_marker, EndMarker},
										Type, Channel, Payload, State)
	end;

%% 处理AMQP协议的握手(客户端连接到RabbitMQ系统会第一时间发送它对应的头)
handle_input(handshake, <<"AMQP", A, B, C, D, Rest/binary>>, State) ->
	{Rest, handshake({A, B, C, D}, State)};

%% 客户端和RabbitMQ服务器第一次握手handshake操作的时候，收到了客户端发送的不是自己期望的信息，则立刻向客户端通知第一次需要向RabbitMQ服务器发送的信息
handle_input(handshake, <<Other:8/binary, _/binary>>, #v1{sock = Sock}) ->
	%% 拒绝接受Exception信息， 则立刻向客户端通知第一次需要向RabbitMQ服务器发送的信息
	refuse_connection(Sock, {bad_header, Other});

%% 其他的Callback则都是异常，则抛出异常信息
handle_input(Callback, Data, _State) ->
	throw({bad_input, Callback, Data}).

%% The two rules pertaining(有关) to version negotiation(交涉):
%%
%% * If the server cannot support the protocol specified in the
%% protocol header, it MUST respond with a valid protocol header and
%% then close the socket connection.
%%
%% * The server MUST provide a protocol version that is lower than or
%% equal to that requested by the client in the protocol header.
%% 以下握手先根据版本号拿到对应的协议解析编码模块,然后向客户端发送RabbitMQ系统这边的一些信息
handshake({0, 0, 9, 1}, State) ->
	%% 向客户端发送connection.start消息，将RabbitMQ服务器的特性和验证信息等相关信息发送给客户端
	start_connection({0, 9, 1}, rabbit_framing_amqp_0_9_1, State);

%% This is the protocol header for 0-9, which we can safely treat as
%% though it were 0-9-1.
handshake({1, 1, 0, 9}, State) ->
	start_connection({0, 9, 0}, rabbit_framing_amqp_0_9_1, State);

%% This is what most clients send for 0-8.  The 0-8 spec, confusingly,
%% defines the version as 8-0.
handshake({1, 1, 8, 0}, State) ->
	start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);

%% The 0-8 spec as on the AMQP web site actually has this as the
%% protocol header; some libraries e.g., py-amqplib, send it when they
%% want 0-8.
handshake({1, 1, 9, 1}, State) ->
	start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);


%% ... and finally, the 1.0 spec is crystal clear!
handshake({Id, 1, 0, 0}, State) ->
	become_1_0(Id, State);


handshake(Vsn, #v1{sock = Sock}) ->
	refuse_connection(Sock, {bad_version, Vsn}).

%% Offer a protocol(协议) version to the client.  Connection.start only
%% includes a major and minor(较小的) version number, Luckily 0-9 and 0-9-1
%% are similar enough that clients will be happy with either.
%% 向客户端发送connection.start消息，将RabbitMQ服务器的特性和验证信息等相关信息发送给客户端
start_connection({ProtocolMajor, ProtocolMinor, _ProtocolRevision},
				 Protocol,
				 State = #v1{sock = Sock, connection = Connection}) ->
	%% 注册一下conection(该进程启动在kernel应用中kernel_safe_sup监督进程下)
	rabbit_networking:register_connection(self()),
	%% 组装connection.start消息准备发送给客户端
	Start = #'connection.start'{
								version_major = ProtocolMajor,
								version_minor = ProtocolMinor,
								server_properties = server_properties(Protocol),				%% 得到服务器的特性
								mechanisms = auth_mechanisms_binary(Sock),
								locales = <<"en_US">> },
	%% 向客户端发送connection.start的信息
	ok = send_on_channel0(Sock, Start, Protocol),
	%% 接下来处理AMQP协议的头(交换callback参数为frame_header)(将消息解析编码的工具文件名字存储起来)(将connection_state设置为starting)
	switch_callback(State#v1{connection = Connection#connection{
																timeout_sec = ?NORMAL_TIMEOUT,
																%% 更新AMQP协议处理模块Protocol
																protocol = Protocol},
							 %% 将当前状态更新为连接正在进行中starting
							 connection_state = starting},
							frame_header, 7).


%% 拒绝接受Exception信息， 则立刻向客户端通知第一次需要向RabbitMQ服务器发送的信息
refuse_connection(Sock, Exception, {A, B, C, D}) ->
	ok = inet_op(fun () -> rabbit_net:send(Sock, <<"AMQP", A, B, C, D>>) end),
	throw(Exception).

-ifdef(use_specs).
-spec(refuse_connection/2 :: (rabbit_net:socket(), any()) -> no_return()).
-endif.
%% 拒绝接受Exception信息
refuse_connection(Sock, Exception) ->
	refuse_connection(Sock, Exception, {0, 0, 9, 1}).


%% 确保向rabbit_event事件中心发布消息的定时器启动
ensure_stats_timer(State = #v1{connection_state = running}) ->
	rabbit_event:ensure_stats_timer(State, #v1.stats_timer, emit_stats);

ensure_stats_timer(State) ->
	State.

%%--------------------------------------------------------------------------
%% 处理channel 为0的消息
handle_method0(MethodName, FieldsBin,
               State = #v1{connection = #connection{protocol = Protocol}}) ->
    try
		%% 新加的代码，打印客户端向channel为0发送的消息
		rabbit_misc:print_client_send_msg([{0, Protocol:decode_method_fields(MethodName, FieldsBin)}]),
		%% 处理客户端发送过来的channel为0的消息
        handle_method0(Protocol:decode_method_fields(MethodName, FieldsBin),
                       State)
    catch throw:{inet_error, E} when E =:= closed; E =:= enotconn ->
            maybe_emit_stats(State),
            throw(connection_closed_abruptly);
          exit:#amqp_error{method = none} = Reason ->
            handle_exception(State, 0, Reason#amqp_error{method = MethodName});
          Type:Reason ->
            Stack = erlang:get_stacktrace(),
            handle_exception(State, 0, {Type, Reason, MethodName, Stack})
    end.


%% 接收到客户端发送过来的connection.start_ok消息
handle_method0(#'connection.start_ok'{mechanism = Mechanism,
									  response = Response,
									  client_properties = ClientProperties},
			   State0 = #v1{connection_state = starting,						%% starting状态是第一次接收到客户端发送的<<"AMQP", 0, 0, 9, 1>>时候设置的
							connection       = Connection,
							sock             = Sock}) ->
	%% 根据验证类型得到对应的验证模块名字
	AuthMechanism = auth_mechanism_to_module(Mechanism, Sock),
	%% 得到客户端的特性
	Capabilities =
		case rabbit_misc:table_lookup(ClientProperties, <<"capabilities">>) of
			{table, Capabilities1} -> Capabilities1;
			_                      -> []
		end,
	%% 将得到的特性存储起来(客户端的特性client_properties，AuthMechanism是得到的验证模块)
	State = State0#v1{connection_state = securing,								%% 将connection_state状态设置为securing
					  connection       =
						  Connection#connection{
												client_properties = ClientProperties,
												capabilities      = Capabilities,
												auth_mechanism    = {Mechanism, AuthMechanism},
												auth_state        = AuthMechanism:init(Sock)}},
	%% 进入验证阶段
	auth_phase(Response, State);

%% 处理客户端发送来的connection.secure_ok消息
handle_method0(#'connection.secure_ok'{response = Response},
			   State = #v1{connection_state = securing}) ->
	auth_phase(Response, State);

%% 处理connection.tune_ok消息
%% frame_max：和客户端通信时所允许的最大的frame size.默认值为131072，增大这个值有助于提高吞吐，降低这个值有利于降低时延
%% channel_max：最大链接数
handle_method0(#'connection.tune_ok'{frame_max   = FrameMax,
									 channel_max = ChannelMax,
									 heartbeat   = ClientHeartbeat},
			   State = #v1{connection_state = tuning,						%% 在向客户端发送connection.tune消息的时候将该状态字段置为tuning
						   connection = Connection,
						   helper_sup = SupPid,
						   sock = Sock}) ->
	%% 验证协商客户端发过来的整数值和服务器设置的frame_max整数值
	ok = validate_negotiated_integer_value(
		   frame_max,   ?FRAME_MIN_SIZE, FrameMax),
	%% 验证协商客户端发过来的整数值和服务器设置的channel_max整数值
	ok = validate_negotiated_integer_value(
		   channel_max, ?CHANNEL_MIN,    ChannelMax),
	%% 在rabbit_connection_helper_sup监督进程下启动queue_collector进程
	{ok, Collector} = rabbit_connection_helper_sup:start_queue_collector(
						SupPid, Connection#connection.name),
	%% 创建心跳包消息Frame结构
	Frame = rabbit_binary_generator:build_heartbeat_frame(),
	%% 创建发送心跳包消息的函数
	SendFun = fun() -> catch rabbit_net:send(Sock, Frame) end,
	Parent = self(),
	%% 创建向自己发送心跳超时的消息的函数
	ReceiveFun = fun() -> Parent ! heartbeat_timeout end,
	%% 在rabbit_connection_helper_sup监督进程下启动两个心跳进程
	%% 一个在ClientHeartbeat除以2后检测RabbitMQ向客户端发送数据的心跳检测进程
	%% 一个是在ClientHeartbeat时间内检测RabbitMQ的当前rabbit_reader进程的socket接收数据的心跳检测进程
	Heartbeater = rabbit_heartbeat:start(
					SupPid, Sock, Connection#connection.name,
					ClientHeartbeat, SendFun, ClientHeartbeat, ReceiveFun),
	State#v1{connection_state = opening,									%% 接收connection.tune_ok消息将connection_state状态置为opening
			 connection = Connection#connection{
												frame_max   = FrameMax,
												channel_max = ChannelMax,
												timeout_sec = ClientHeartbeat},
			 queue_collector = Collector,
			 heartbeater = Heartbeater};

%% 处理客户端发送过来的connection.open消息
handle_method0(#'connection.open'{virtual_host = VHostPath},
			   State = #v1{connection_state = opening,						%% 接收connection.tune_ok消息将connection_state状态置为opening
						   connection       = Connection = #connection{
																	   user = User,
																	   protocol = Protocol},
						   helper_sup       = SupPid,
						   sock             = Sock,
						   throttle         = Throttle}) ->
	%% 检查当前账号的客户端能否使用该VirtualHost
	ok = rabbit_access_control:check_vhost_access(User, VHostPath, Sock),
	%% 得到当前客户端使用的VirtualHostPath
	NewConnection = Connection#connection{vhost = VHostPath},
	%% 向客户端发送connection.open_ok的消息
	ok = send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),
	%% 向rabbit_alarm进程注册，同时返回当前集群的所有报警信息
	Conserve = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
	Throttle1 = Throttle#throttle{alarmed_by = Conserve},
	%% 在rabbit_connection_helper_sup监督进程下启动channel_sup_sup监督进程
	{ok, ChannelSupSupPid} =
		rabbit_connection_helper_sup:start_channel_sup_sup(SupPid),
	%% 根据当前集群的资源报警信息和当前连接进程rabbit_reader消息流的阻塞信息，用来阻塞或者让当前进程运行
	State1 = control_throttle(
			   State#v1{connection_state    = running,
						connection          = NewConnection,
						channel_sup_sup_pid = ChannelSupSupPid,
						throttle            = Throttle1}),
	%% 向rabbit_event事件中心发布connection产生的事件
	rabbit_event:notify(connection_created,
						[{type, network} |
							 infos(?CREATION_EVENT_KEYS, State1)]),
	%% 向rabbit_event事件中心发布自己当前的所有详细信息
	maybe_emit_stats(State1),
	State1;

%% 当前rabbit_reader进程处于运行中，则关闭当前连接下的所有channel进程，如果当前连接状态为closing，则将当前rabbit_reader进程关闭掉，同时发送connection.close_ok消息通知客户端
handle_method0(#'connection.close'{}, State) when ?IS_RUNNING(State) ->
	%% 关闭当前连接下的所有channel进程
	lists:foreach(fun rabbit_channel:shutdown/1, all_channels()),
	%% 如果当前连接状态为closing，则将当前rabbit_reader进程关闭掉，同时发送connection.close_ok消息通知客户端
	maybe_close(State#v1{connection_state = closing});

%% 如果当前rabbit_reader进程处于停止中，则只需要向客户端发送connection.close_ok消息
handle_method0(#'connection.close'{},
			   State = #v1{connection = #connection{protocol = Protocol},
						   sock = Sock})
  when ?IS_STOPPING(State) ->
	%% We're already closed or closing, so we don't need to cleanup
	%% anything.
	ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
	State;

%% 处理客户端发送过来的connection.close_ok，则立刻停止当前rabbit_reader进程
handle_method0(#'connection.close_ok'{},
			   State = #v1{connection_state = closed}) ->
	self() ! terminate_connection,
	State;

%% 如果当前rabbit_reader进程处于正在关闭中，则不能处理客户端发送过来channel为0的消息
handle_method0(_Method, State) when ?IS_STOPPING(State) ->
	State;

%% 如果当前rabbit_reader进程正在运行中，则不能处理的channel为0的消息，同时当前rabbit_reader进程立刻终止
handle_method0(_Method, #v1{connection_state = S}) ->
	rabbit_misc:protocol_error(
	  channel_error, "unexpected method in connection state ~w", [S]).


%% negotiated：协商
%% 验证协商客户端发过来的整数值和服务器设置的整数值
validate_negotiated_integer_value(Field, Min, ClientValue) ->
	%% 从rabbit应用拿到Field对应的配置数据
	ServerValue = get_env(Field),
	if ClientValue /= 0 andalso ClientValue < Min ->
		   %% 验证的客户端值比服务器的最小值小，则将当前rabbit_reader进程终止
		   fail_negotiation(Field, min, ServerValue, ClientValue);
	   ServerValue /= 0 andalso (ClientValue =:= 0 orelse
									 ClientValue > ServerValue) ->
		   %% 验证 的客户端值大于服务器设置的最大值，则将当前rabbit_reader进程终止
		   fail_negotiation(Field, max, ServerValue, ClientValue);
	   true ->
		   ok
	end.

%% keep dialyzer happy
-spec fail_negotiation(atom(), 'min' | 'max', integer(), integer()) ->
                              no_return().
%% 验证整数值出错，将当前rabbit_reader进程终止
fail_negotiation(Field, MinOrMax, ServerValue, ClientValue) ->
	{S1, S2} = case MinOrMax of
				   min -> {lower,  minimum};
				   max -> {higher, maximum}
			   end,
	rabbit_misc:protocol_error(
	  not_allowed, "negotiated ~w = ~w is ~w than the ~w allowed value (~w)",
	  [Field, ClientValue, S1, S2, ServerValue], 'connection.tune').


%% 从rabbit应用配置文件中拿到Key对应的数据
get_env(Key) ->
	{ok, Value} = application:get_env(rabbit, Key),
	Value.


%% 将数据发送到channel为0上
send_on_channel0(Sock, Method, Protocol) ->
	ok = rabbit_writer:internal_send_command(Sock, 0, Method, Protocol).


%% 根据验证类型得到对应的验证模块名字
auth_mechanism_to_module(TypeBin, Sock) ->
	case rabbit_registry:binary_to_type(TypeBin) of
		{error, not_found} ->
			rabbit_misc:protocol_error(
			  command_invalid, "unknown authentication mechanism '~s'",
			  [TypeBin]);
		T ->
			case {lists:member(T, auth_mechanisms(Sock)),
				  rabbit_registry:lookup_module(auth_mechanism, T)} of
				{true, {ok, Module}} ->
					Module;
				_ ->
					rabbit_misc:protocol_error(
					  command_invalid,
					  "invalid authentication mechanism '~s'", [T])
			end
	end.


%% 验证机制(mechanisms:验证)
auth_mechanisms(Sock) ->
	{ok, Configured} = application:get_env(auth_mechanisms),
	[Name || {Name, Module} <- rabbit_registry:lookup_all(auth_mechanism),
			 Module:should_offer(Sock), lists:member(Name, Configured)].


%% 验证机制的二进制数据
%% mechanisms：验证
auth_mechanisms_binary(Sock) ->
	list_to_binary(
	  string:join([atom_to_list(A) || A <- auth_mechanisms(Sock)], " ")).


%% 验证阶段(去系统查找该用户及其对应的密码是否合法正确)
auth_phase(Response,
		   State = #v1{connection = Connection =
										#connection{protocol       = Protocol,
													auth_mechanism = {Name, AuthMechanism},
													auth_state     = AuthState},
					   sock = Sock}) ->
	%% 通过验证模块得到对应的用户名字和密码
	case AuthMechanism:handle_response(Response, AuthState) of
		{refused, Username, Msg, Args} ->
			%% 验证拒绝，则中断当前rabbit_reader进程
			auth_fail(Username, Msg, Args, Name, State);
		{protocol_error, Msg, Args} ->
			%% 向rabbit_event发布用户验证结果
			notify_auth_result(none, user_authentication_failure,
							   [{error, rabbit_misc:format(Msg, Args)}],
							   State),
			rabbit_misc:protocol_error(syntax_error, Msg, Args);
		%% 此处是客户端使用rabbit_auth_mechanism_cr_demo这个作为验证模块，则客户端第一次只发送用户名，然后服务器通过connection.secure消息通知客户端通过connection.secure_ok告诉密码
		{challenge, Challenge, AuthState1} ->
			Secure = #'connection.secure'{challenge = Challenge},
			%% 将connection.secure消息发送给客户端
			ok = send_on_channel0(Sock, Secure, Protocol),
			State#v1{connection = Connection#connection{
														auth_state = AuthState1}};
		{ok, User = #user{username = Username}} ->
			%% 在rabbit应用配置中loopback_users字段配置用户名，这些配置的用户名只能本地地址才能使用这些用户名字
			case rabbit_access_control:check_user_loopback(Username, Sock) of
				ok ->
					%% 向rabbit_event发布用户验证结果
					notify_auth_result(Username, user_authentication_success,
									   [], State);
				not_allowed ->
					%% 验证失败，这个用户只能是内网才能使用
					auth_fail(Username, "user '~s' can only connect via "
								  "localhost", [Username], Name, State)
			end,
			%% 组装connection.tune消息
			Tune = #'connection.tune'{frame_max   = get_env(frame_max),
									  channel_max = get_env(channel_max),
									  heartbeat   = get_env(heartbeat)},
			%% 将connection.tune消息发送给客户端
			ok = send_on_channel0(Sock, Tune, Protocol),
			%% 将得到的用户存储起来
			State#v1{connection_state = tuning,									%% 将connection_state状态字段置为tuning，等待connection.tune_ok消息的返回
					 connection = Connection#connection{user       = User,
														auth_state = none}}
	end.

-ifdef(use_specs).
-spec(auth_fail/5 ::
        (rabbit_types:username() | none, string(), [any()], binary(), #v1{}) ->
           no_return()).
-endif.
%% 验证失败，Msg，Args组装成失败的原因，AuthName为用户名字
auth_fail(Username, Msg, Args, AuthName,
		  State = #v1{connection = #connection{protocol     = Protocol,
											   capabilities = Capabilities}}) ->
	%% 向rabbit_event发布用户验证结果
	notify_auth_result(Username, user_authentication_failure,
					   [{error, rabbit_misc:format(Msg, Args)}], State),
	%% 组装amqp_error结构，该结构用来发送给客户端
	AmqpError = rabbit_misc:amqp_error(
				  access_refused, "~s login refused: ~s",
				  [AuthName, io_lib:format(Msg, Args)], none),
	case rabbit_misc:table_lookup(Capabilities,
								  <<"authentication_failure_close">>) of
		%% 如果客户端特性中authentication_failure_close字段为true，则向客户端发送验证失败的原因
		{bool, true} ->
			SafeMsg = io_lib:format(
						"Login was refused using authentication "
							"mechanism ~s. For details see the broker "
								"logfile.", [AuthName]),
			%% 得到新的amqp_error结构
			AmqpError1 = AmqpError#amqp_error{explanation = SafeMsg},
			%% 得到要向客户端发送的Frame
			{0, CloseMethod} = rabbit_binary_generator:map_exception(
								 0, AmqpError1, Protocol),
			%% 将错误信息发送给客户端
			ok = send_on_channel0(State#v1.sock, CloseMethod, Protocol);
		_ -> ok
	end,
	%% 当前rabbit_reader进程立刻中断停止
	rabbit_misc:protocol_error(AmqpError).


%% 向rabbit_event发布用户验证结果
notify_auth_result(Username, AuthResult, ExtraProps, State) ->
	EventProps = [{connection_type, network},
				  {name, case Username of none -> ''; _ -> Username end}] ++
					 [case Item of
						  name -> {connection_name, i(name, State)};
						  _    -> {Item, i(Item, State)}
					  end || Item <- ?AUTH_NOTIFICATION_INFO_KEYS] ++
					 ExtraProps,
	rabbit_event:notify(AuthResult, [P || {_, V} = P <- EventProps, V =/= '']).

%%--------------------------------------------------------------------------
%% 列出当前rabbit_reader进程Items中key对应的信息
infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].


%% 列出当前rabbit_reader进程的Pid
i(pid,                #v1{}) -> self();

%% 得到当前rabbit_reader进程中的Socket信息
i(SockStat,           S) when SockStat =:= recv_oct;
							  SockStat =:= recv_cnt;
							  SockStat =:= send_oct;
							  SockStat =:= send_cnt;
							  SockStat =:= send_pend ->
	socket_info(fun (Sock) -> rabbit_net:getstat(Sock, [SockStat]) end,
				fun ([{_, I}]) -> I end, S);

%% 判断当前rabbit_reader进程中的Socket是否是ssl类型
i(ssl,                #v1{sock = Sock}) -> rabbit_net:is_ssl(Sock);

%% 得到当前rabbit_reader进程Socket中ssl_protocol对应的信息
i(ssl_protocol,       S) -> ssl_info(fun ({P,         _}) -> P end, S);

%% 得到当前rabbit_reader进程Socket中ssl_key_exchange对应的信息
i(ssl_key_exchange,   S) -> ssl_info(fun ({_, {K, _, _}}) -> K end, S);

%% 得到当前rabbit_reader进程Socket中ssl_cipher对应的信息
i(ssl_cipher,         S) -> ssl_info(fun ({_, {_, C, _}}) -> C end, S);

%% 得到当前rabbit_reader进程Socket中ssl_hash对应的信息
i(ssl_hash,           S) -> ssl_info(fun ({_, {_, _, H}}) -> H end, S);

%% 得到当前rabbit_reader进程Socket中peer_cert_issuer对应的信息
i(peer_cert_issuer,   S) -> cert_info(fun rabbit_ssl:peer_cert_issuer/1,   S);

%% 得到当前rabbit_reader进程Socket中peer_cert_subject对应的信息
i(peer_cert_subject,  S) -> cert_info(fun rabbit_ssl:peer_cert_subject/1,  S);

%% 得到当前rabbit_reader进程Socket中peer_cert_validity对应的信息
i(peer_cert_validity, S) -> cert_info(fun rabbit_ssl:peer_cert_validity/1, S);

%% 得到当前rabbit_reader进程中channel的个数
i(channels,           #v1{channel_count = ChannelCount}) -> ChannelCount;

%% 得到当前rabbit_reader进程的是否处于flow消息流阻塞状态，如果没有则返回connection_state字段值
i(state, #v1{connection_state = ConnectionState,
			 throttle         = #throttle{alarmed_by      = Alarms,
										  last_blocked_by = WasBlockedBy,
										  last_blocked_at = T}}) ->
	case Alarms =:= [] andalso %% not throttled by resource alarms(没有被资源耗尽阻塞)
			 (credit_flow:blocked() %% throttled by flow now(当前已经被消息流阻塞)
				  orelse                %% throttled by flow recently(在小于5秒内有阻塞)
				  (WasBlockedBy =:= flow andalso T =/= never andalso
					   timer:now_diff(erlang:now(), T) < 5000000)) of
		true  -> flow;
		false -> ConnectionState
	end;

%% 得到rabbit_reader其他信息
i(Item,               #v1{connection = Conn}) -> ic(Item, Conn).


%% 得到当前rabbit_reader连接进程的名字
ic(name,              #connection{name        = Name})     -> Name;

%% 得到当前rabbit_reader连接进程的Host(当前RabbitMQ服务器的host)
ic(host,              #connection{host        = Host})     -> Host;

%% 得到当前rabbit_reader连接进程的peer_host(即客户端的Host)
ic(peer_host,         #connection{peer_host   = PeerHost}) -> PeerHost;

%% 得到当前rabbit_reader连接进程的Port(当前RabbitMQ服务器的Port)
ic(port,              #connection{port        = Port})     -> Port;

%% 得到当前rabbit_reader连接进程的peer_port(即得到客户端的Port)
ic(peer_port,         #connection{peer_port   = PeerPort}) -> PeerPort;

%% 得到当前rabbit_reader连接进程的protocol
ic(protocol,          #connection{protocol    = none})     -> none;

%% 得到当前rabbit_reader连接进程的protocol
ic(protocol,          #connection{protocol    = P})        -> P:version();

%% 得到当前rabbit_reader连接进程的玩家信息
ic(user,              #connection{user        = none})     -> '';

%% 得到当前rabbit_reader连接进程的玩家信息
ic(user,              #connection{user        = U})        -> U#user.username;

%% 得到当前rabbit_reader连接进程的VHost
ic(vhost,             #connection{vhost       = VHost})    -> VHost;

%% 得到当前rabbit_reader连接进程的timeout
ic(timeout,           #connection{timeout_sec = Timeout})  -> Timeout;

%% 得到当前rabbit_reader连接进程的frame_max
ic(frame_max,         #connection{frame_max   = FrameMax}) -> FrameMax;

%% 得到当前rabbit_reader连接进程的channel_max
ic(channel_max,       #connection{channel_max = ChMax})    -> ChMax;

%% 得到当前rabbit_reader连接进程的client_properties(即客户端的支持的特性)
ic(client_properties, #connection{client_properties = CP}) -> CP;

%% 得到当前rabbit_reader连接进程的auth_mechanism(即用户验证的模块名)
ic(auth_mechanism,    #connection{auth_mechanism = none})  -> none;

%% 得到当前rabbit_reader连接进程的auth_mechanism(即用户验证的模块名)
ic(auth_mechanism,    #connection{auth_mechanism = {Name, _Mod}}) -> Name;

%% 得到当前rabbit_reader连接进程的connected_at(即客户端连接的时间)
ic(connected_at,      #connection{connected_at = T}) -> T;

%% 不是别的Key
ic(Item,              #connection{}) -> throw({bad_argument, Item}).


socket_info(Get, Select, #v1{sock = Sock}) ->
	case Get(Sock) of
		{ok,    T} -> Select(T);
		{error, _} -> ''
	end.


ssl_info(F, #v1{sock = Sock}) ->
	%% The first ok form is R14
	%% The second is R13 - the extra term is exportability (by inspection,
	%% the docs are wrong)
	case rabbit_net:ssl_info(Sock) of
		nossl                   -> '';
		{error, _}              -> '';
		{ok, {P, {K, C, H}}}    -> F({P, {K, C, H}});
		{ok, {P, {K, C, H, _}}} -> F({P, {K, C, H}})
	end.


cert_info(F, #v1{sock = Sock}) ->
	case rabbit_net:peercert(Sock) of
		nossl      -> '';
		{error, _} -> '';
		{ok, Cert} -> list_to_binary(F(Cert))
	end.


%% 向rabbit_event事件中心发布自己当前的所有详细信息
maybe_emit_stats(State) ->
	rabbit_event:if_enabled(State, #v1.stats_timer,
							fun() -> emit_stats(State) end).


%% 向rabbit_event事件中心发布自己当前的所有详细信息
emit_stats(State) ->
	%% 先拿到STATISTICS_KEYS中key对应的信息
	Infos = infos(?STATISTICS_KEYS, State),
	%% 向rabbit_event事件中心发布拿到的rabbit_reader进程信息
	rabbit_event:notify(connection_stats, Infos),
	%% 重置当前rabbit_reader进程中向rabbit_event事件发送信息的数据结构
	State1 = rabbit_event:reset_stats_timer(State, #v1.stats_timer),
	%% If we emit an event which looks like we are in flow control, it's not a
	%% good idea for it to be our last even if we go idle. Keep emitting
	%% events, either we stay busy or we drop out of flow control.
	case proplists:get_value(state, Infos) of
		flow -> ensure_stats_timer(State1);
		_    -> State1
	end.

%% 1.0 stub
-ifdef(use_specs).
-spec(become_1_0/2 :: (non_neg_integer(), #v1{}) -> no_return()).
-endif.
become_1_0(Id, State = #v1{sock = Sock}) ->
	case code:is_loaded(rabbit_amqp1_0_reader) of
		false -> refuse_connection(Sock, amqp1_0_plugin_not_enabled);
		_     -> Mode = case Id of
							0 -> amqp;
							3 -> sasl;
							_ -> refuse_connection(
								   Sock, {unsupported_amqp1_0_protocol_id, Id},
								   {3, 1, 0, 0})
						end,
				 F = fun (_Deb, Buf, BufLen, S) ->
							  {rabbit_amqp1_0_reader, init,
							   [Mode, pack_for_1_0(Buf, BufLen, S)]}
					 end,
				 State#v1{connection_state = {become, F}}
	end.


pack_for_1_0(Buf, BufLen, #v1{parent       = Parent,
							  sock         = Sock,
							  recv_len     = RecvLen,
							  pending_recv = PendingRecv,
							  helper_sup   = SupPid}) ->
	{Parent, Sock, RecvLen, PendingRecv, SupPid, Buf, BufLen}.
