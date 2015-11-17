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

-module(rabbit_writer).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([start/6, start_link/6, start/7, start_link/7]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([send_command/2, send_command/3,
         send_command_sync/2, send_command_sync/3,
         send_command_and_notify/4, send_command_and_notify/5,
         send_command_flow/2, send_command_flow/3,
         flush/1]).
-export([internal_send_command/4, internal_send_command/6]).

%% internal
-export([enter_mainloop/2, mainloop/2, mainloop1/2]).

-record(wstate, {
				 sock,						%% 连接中的Socket
				 channel,					%% channel的数字
				 frame_max,					%% Frame的最大数量
				 protocol,					%% AMQP协议的模板处理模块名字
				 reader,					%% 从Socket读取数据的进程Pid
				 stats_timer,				%% 状态定时器
				 pending					%% 等待队列
				}).

-define(HIBERNATE_AFTER, 5000).

%%---------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/6 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name())
        -> rabbit_types:ok(pid())).
-spec(start_link/6 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name())
        -> rabbit_types:ok(pid())).
-spec(start/7 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean())
        -> rabbit_types:ok(pid())).
-spec(start_link/7 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean())
        -> rabbit_types:ok(pid())).

-spec(system_code_change/4 :: (_,_,_,_) -> {'ok',_}).
-spec(system_continue/3 :: (_,_,#wstate{}) -> any()).
-spec(system_terminate/4 :: (_,_,_,_) -> none()).

-spec(send_command/2 ::
        (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(send_command/3 ::
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content())
        -> 'ok').
-spec(send_command_sync/2 ::
        (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(send_command_sync/3 ::
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content())
        -> 'ok').
-spec(send_command_and_notify/4 ::
        (pid(), pid(), pid(), rabbit_framing:amqp_method_record())
        -> 'ok').
-spec(send_command_and_notify/5 ::
        (pid(), pid(), pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:content())
        -> 'ok').
-spec(send_command_flow/2 ::
        (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(send_command_flow/3 ::
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content())
        -> 'ok').
-spec(flush/1 :: (pid()) -> 'ok').
-spec(internal_send_command/4 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         rabbit_framing:amqp_method_record(), rabbit_types:protocol())
        -> 'ok').
-spec(internal_send_command/6 ::
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         rabbit_framing:amqp_method_record(), rabbit_types:content(),
         non_neg_integer(), rabbit_types:protocol())
        -> 'ok').

-endif.

%%---------------------------------------------------------------------------
%% rabbit_writer进程的启动入口函数
start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity) ->
	start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity, false).


%% rabbit_writer进程的启动入口函数
start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity) ->
	start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity, false).


start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
	  ReaderWantsStats) ->
	State = initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid,
						  ReaderWantsStats),
	{ok, proc_lib:spawn(?MODULE, enter_mainloop, [Identity, State])}.


start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
		   ReaderWantsStats) ->
	State = initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid,
						  ReaderWantsStats),
	{ok, proc_lib:spawn_link(?MODULE, enter_mainloop, [Identity, State])}.


%% 初始化写进程的状态,该进程主要是用来发消息给服务器
initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid, ReaderWantsStats) ->
	(case ReaderWantsStats of
		 %% 表示根据配置文件中的配置来初始化是否能够向rabbit_event事件中心发布事件
		 true  -> fun rabbit_event:init_stats_timer/2;
		 %% 表示不能够向rabbit_event事件中心发布事件
		 false -> fun rabbit_event:init_disabled_stats_timer/2
	 end)(#wstate{sock      = Sock,
				  channel   = Channel,
				  frame_max = FrameMax,
				  protocol  = Protocol,
				  reader    = ReaderPid,
				  pending   = []},
		  #wstate.stats_timer).


system_continue(Parent, Deb, State) ->
	mainloop(Deb, State#wstate{reader = Parent}).


system_terminate(Reason, _Parent, _Deb, _State) ->
	exit(Reason).


system_code_change(Misc, _Module, _OldVsn, _Extra) ->
	{ok, Misc}.


%% rabbit_writer进程的主循环函数enter_mainloop
enter_mainloop(Identity, State) ->
	Deb = sys:debug_options([]),
	?store_proc_name(Identity),
	mainloop(Deb, State).


%% rabbit_writer进程的主循环函数mainloop
mainloop(Deb, State) ->
	try
		mainloop1(Deb, State)
	catch
		exit:Error -> #wstate{reader = ReaderPid, channel = Channel} = State,
					  ReaderPid ! {channel_exit, Channel, Error}
	end,
	done.


%% rabbit_writer进程的主循环函数
mainloop1(Deb, State = #wstate{pending = []}) ->
	receive
		Message -> {Deb1, State1} = handle_message(Deb, Message, State),
				   ?MODULE:mainloop1(Deb1, State1)
		%% 如果超过5s钟没有消息到来，则直接将自己rabbit_writer进程进入休眠状态
		after ?HIBERNATE_AFTER ->
			erlang:hibernate(?MODULE, mainloop, [Deb, State])
	end;

mainloop1(Deb, State) ->
	receive
		Message -> {Deb1, State1} = handle_message(Deb, Message, State),
				   ?MODULE:mainloop1(Deb1, State1)
		after 0 ->
			%% 当pending等待发送数据的队列里有数据，则立刻将数据发送到RabbitMQ服务器的客户端
			?MODULE:mainloop1(Deb, internal_flush(State))
	end.


%% 处理系统消息
handle_message(Deb, {system, From, Req}, State = #wstate{reader = Parent}) ->
	sys:handle_system_msg(Req, From, Parent, ?MODULE, Deb, State);

%% 处理普通消息
handle_message(Deb, Message, State) ->
	{Deb, handle_message(Message, State)}.


%% 异步发送不带消息内容的Frame消息到Socket，但是不带消息流限制
handle_message({send_command, MethodRecord}, State) ->
	internal_send_command_async(MethodRecord, State);

%% 异步发送消息并带有内容到Socket，但是不带消息流限制
handle_message({send_command, MethodRecord, Content}, State) ->
	internal_send_command_async(MethodRecord, Content, State);

%% 异步发送不带消息内容的Frame消息到Socket，带有消息流限制
handle_message({send_command_flow, MethodRecord, Sender}, State) ->
	credit_flow:ack(Sender),
	internal_send_command_async(MethodRecord, State);

%% 异步发送带有消息内容的Frame消息到Socket，带有消息流限制
handle_message({send_command_flow, MethodRecord, Content, Sender}, State) ->
	credit_flow:ack(Sender),
	internal_send_command_async(MethodRecord, Content, State);

%% 同步处理发送不带消息内容的Frame消息
handle_message({'$gen_call', From, {send_command_sync, MethodRecord}}, State) ->
	State1 = internal_flush(
			   internal_send_command_async(MethodRecord, State)),
	gen_server:reply(From, ok),
	State1;

%% 同步处理发送带消息内容的Frame消息
handle_message({'$gen_call', From, {send_command_sync, MethodRecord, Content}},
			   State) ->
	State1 = internal_flush(
			   internal_send_command_async(MethodRecord, Content, State)),
	gen_server:reply(From, ok),
	State1;

%% 处理同步刷新的消息
handle_message({'$gen_call', From, flush}, State) ->
	State1 = internal_flush(State),
	gen_server:reply(From, ok),
	State1;

%% 异步发送不带消息内容的Frame消息的接口，同时回调通知发送成功
handle_message({send_command_and_notify, QPid, ChPid, MethodRecord}, State) ->
	State1 = internal_send_command_async(MethodRecord, State),
	rabbit_amqqueue:notify_sent(QPid, ChPid),
	State1;

%% 异步发送带消息内容的Frame消息的接口，同时回调通知发送成功
handle_message({send_command_and_notify, QPid, ChPid, MethodRecord, Content},
			   State) ->
	State1 = internal_send_command_async(MethodRecord, Content, State),
	rabbit_amqqueue:notify_sent(QPid, ChPid),
	State1;

%% 处理进程挂掉的消息
handle_message({'DOWN', _MRef, process, QPid, _Reason}, State) ->
	rabbit_amqqueue:notify_sent_queue_down(QPid),
	State;

handle_message({inet_reply, _, ok}, State) ->
	rabbit_event:ensure_stats_timer(State, #wstate.stats_timer, emit_stats);

handle_message({inet_reply, _, Status}, _State) ->
	exit({writer, send_failed, Status});

handle_message(emit_stats, State = #wstate{reader = ReaderPid}) ->
	ReaderPid ! ensure_stats,
	rabbit_event:reset_stats_timer(State, #wstate.stats_timer);

%% 不能识别的消息，直接让rabbit_writer进程终止掉
handle_message(Message, _State) ->
	exit({writer, message_not_understood, Message}).

%%---------------------------------------------------------------------------
%% 暴露出来的异步发送Frame消息的接口，不带消息流限制
send_command(W, MethodRecord) ->
	W ! {send_command, MethodRecord},
	ok.


%% 暴露出来的异步发送带有消息内容的Frame消息的接口，不带消息流限制
send_command(W, MethodRecord, Content) ->
	W ! {send_command, MethodRecord, Content},
	ok.


%% 暴露出来的异步发送Frame消息的接口，但是带有消息流限制
send_command_flow(W, MethodRecord) ->
	credit_flow:send(W),
	W ! {send_command_flow, MethodRecord, self()},
	ok.


%% 暴露出来的异步发送带有消息内容的Frame消息的接口，但是带有消息流限制
send_command_flow(W, MethodRecord, Content) ->
	credit_flow:send(W),
	W ! {send_command_flow, MethodRecord, Content, self()},
	ok.


%% 暴露出来的同步发送不带消息内容的Frame消息的接口
send_command_sync(W, MethodRecord) ->
	call(W, {send_command_sync, MethodRecord}).


%% 暴露出来的同步发送带消息内容的Frame消息的接口
send_command_sync(W, MethodRecord, Content) ->
	call(W, {send_command_sync, MethodRecord, Content}).


%% 异步发送不带消息内容的Frame消息的接口，同时回调通知发送成功
send_command_and_notify(W, Q, ChPid, MethodRecord) ->
	W ! {send_command_and_notify, Q, ChPid, MethodRecord},
	ok.


%% 异步发送带消息内容的Frame消息的接口，同时回调通知发送成功
send_command_and_notify(W, Q, ChPid, MethodRecord, Content) ->
	W ! {send_command_and_notify, Q, ChPid, MethodRecord, Content},
	ok.


%% 暴露给外面的刷新函数
flush(W) -> call(W, flush).

%%---------------------------------------------------------------------------
%% 同步向rabbit_writer进程发送消息的接口
call(Pid, Msg) ->
	{ok, Res} = gen:call(Pid, '$gen_call', Msg, infinity),
	Res.

%%---------------------------------------------------------------------------
%% 将要发送的消息数据组合成二进制
assemble_frame(Channel, MethodRecord, Protocol) ->
	rabbit_binary_generator:build_simple_method_frame(
	  Channel, MethodRecord, Protocol).


%% 组装消息 + 消息附带的内容为二进制
assemble_frames(Channel, MethodRecord, Content, FrameMax, Protocol) ->
	MethodName = rabbit_misc:method_record_type(MethodRecord),
	true = Protocol:method_has_content(MethodName), % assertion
	%% 根据不同的MethodRecord和Protocol组装数据(先组装Method结构)
	MethodFrame = rabbit_binary_generator:build_simple_method_frame(
					Channel, MethodRecord, Protocol),
	%% 得到内容的打包的二进制数据
	ContentFrames = rabbit_binary_generator:build_simple_content_frames(
					  Channel, Content, FrameMax, Protocol),
	%% 将内容的二进制数据直接放在Method消息二进制数据之后
	[MethodFrame | ContentFrames].


%% 直接将数据发送到客户端
tcp_send(Sock, Data) ->
	rabbit_misc:throw_on_error(inet_error,
							   fun () -> rabbit_net:send(Sock, Data) end).


%% 内部同步(即立刻将数据发送到客户端)向客户端发送数据，没有消息内容
internal_send_command(Sock, Channel, MethodRecord, Protocol) ->
	%% 增加消息发送的打印(自己加上去的代码)
	rabbit_misc:print_client_recieve_msg([{Channel, MethodRecord}]),
	ok = tcp_send(Sock, assemble_frame(Channel, MethodRecord, Protocol)).


%% 内部同步(即立刻将数据发送到客户端)向客户端发送数据，同时带有消息内容
internal_send_command(Sock, Channel, MethodRecord, Content, FrameMax,
					  Protocol) ->
	%% 增加消息发送的打印(自己加上去的代码)
	rabbit_misc:print_client_recieve_msg([{Channel, MethodRecord, Content}]),
	ok = lists:foldl(fun (Frame,     ok) -> tcp_send(Sock, Frame);
						(_Frame, Other) -> Other
					 end, ok, assemble_frames(Channel, MethodRecord,
											  Content, FrameMax, Protocol)).


%% 内部异步(即不会立刻发送到客户端，等待队列中的数据量超过FLUSH_THRESHOLD或者收到下一个消息的到来会刷新将数据发送到客户端)发送数据到Socket
internal_send_command_async(MethodRecord,
							State = #wstate{channel   = Channel,
											protocol  = Protocol,
											pending   = Pending}) ->
	%% 增加消息发送的打印(自己加上去的代码)
	rabbit_misc:print_client_recieve_msg([{Channel, MethodRecord}]),
	Frame = assemble_frame(Channel, MethodRecord, Protocol),
	maybe_flush(State#wstate{pending = [Frame | Pending]}).


%% 内部异步(即不会立刻发送到客户端，等待队列中的数据量超过FLUSH_THRESHOLD或者收到下一个消息的到来会刷新将数据发送到客户端)发送数据到Socket，同时带有消息内容
internal_send_command_async(MethodRecord, Content,
							State = #wstate{channel   = Channel,
											frame_max = FrameMax,
											protocol  = Protocol,
											pending   = Pending}) ->
	%% 增加消息发送的打印(自己加上去的代码)
	rabbit_misc:print_client_recieve_msg([{Channel, MethodRecord, Content}]),
	Frames = assemble_frames(Channel, MethodRecord, Content, FrameMax,
							 Protocol),
	%% 在调用该函数的进程里，当操作的数据超过1000000的时候则当前进程进行一次垃圾回收
	rabbit_basic:maybe_gc_large_msg(Content),
	maybe_flush(State#wstate{pending = [Frames | Pending]}).

%% This magic number is the tcp-over-ethernet MSS (1460) minus the
%% minimum size of a AMQP basic.deliver method frame (24) plus basic
%% content header (22). The idea is that we want to flush just before
%% exceeding the MSS.
-define(FLUSH_THRESHOLD, 1414).

%% 当rabbit_writer进程中缓存的数据操作1414，则将数据发送到Socket中
maybe_flush(State = #wstate{pending = Pending}) ->
	case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
		true  -> internal_flush(State);
		false -> State
	end.


internal_flush(State = #wstate{pending = []}) ->
	State;

internal_flush(State = #wstate{sock = Sock, pending = Pending}) ->
	%% 将数据放入Socket的接口
	ok = port_cmd(Sock, lists:reverse(Pending)),
	%% 最后将等待队列清空
	State#wstate{pending = []}.

%% gen_tcp:send/2 does a selective receive of {inet_reply, Sock,
%% Status} to obtain the result. That is bad when it is called from
%% the writer since it requires scanning of the writers possibly quite
%% large message queue.
%%
%% So instead we lift the code from prim_inet:send/2, which is what
%% gen_tcp:send/2 calls, do the first half here and then just process
%% the result code in handle_message/2 as and when it arrives.
%%
%% This means we may end up happily sending data down a closed/broken
%% socket, but that's ok since a) data in the buffers will be lost in
%% any case (so qualitatively we are no worse off than if we used
%% gen_tcp:send/2), and b) we do detect the changed socket status
%% eventually, i.e. when we get round to handling the result code.
%%
%% Also note that the port has bounded buffers and port_command blocks
%% when these are full. So the fact that we process the result
%% asynchronously does not impact flow control.
%% 将数据放入Socket的接口
port_cmd(Sock, Data) ->
	true = try rabbit_net:port_command(Sock, Data)
		   catch error:Error -> exit({writer, send_failed, Error})
		   end,
	ok.
