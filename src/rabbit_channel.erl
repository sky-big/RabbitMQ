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

-module(rabbit_channel).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/11, do/2, do/3, do_flow/3, flush/1, shutdown/1]).
-export([send_command/2, deliver/4, deliver_reply/2,
         send_credit_reply/2, send_drained/2]).
-export([list/0, info_keys/0, info/1, info/2, info_all/0, info_all/1]).
-export([refresh_config_local/0, ready_for_close/1]).
-export([force_event_refresh/1]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).
%% Internal
-export([list_local/0, deliver_reply_local/3]).

-record(ch, {
			 state,											%% 当前channel进程的状态
			 protocol,										%% AMQP协议使用的模块(rabbit_framing_amqp_0_9_1，该模块的代码是工具生成)
			 channel,										%% 当前channel的数字编号
			 reader_pid,									%% 读取Socket数据的进程Pid(rabbit_reader进程的Pid)
			 writer_pid,									%% 向Socket发送数据的进程Pid
			 conn_pid,										%% 读取Socket数据的进程Pid(rabbit_reader进程的Pid)
			 conn_name,										%% 读取Socket数据进程的名字
			 limiter,										%% limiter进程的Pid
			 tx,											%% rabbit_channel进程事务操作存储数据的字段(默认是不开启的)
			 next_tag,										%% 给等待消费者ack操作的消息的索引
			 unacked_message_q,								%% 消费者还没有确认的消息队列，使用OTP提供的queue数据结构
			 user,											%% 该channel对应的用户信息
			 virtual_host,									%% Vhost
			 most_recently_declared_queue,					%% 上次最新的操作的队列名字
			 queue_names,									%% 跟当前rabbit_channel进程有关系的队列进程Pid对应的队列名字字典结构
			 queue_monitors,								%% 监视所有跟当前rabbit_channel进程有关联的队列进程Pid结构
			 consumer_mapping,								%% 当前rabbit_channel进程上所有的消费者相关信息的dict数据结构
			 queue_consumers,								%% 队列中消费者列表，是一个字典，以队列Pid做key，value为gb_sets结构，结构中存储的都是消费者的标识
			 delivering_queues,								%% 正在传输的队列进程Pid集合(即需要ack的消息队列)
			 queue_collector_pid,							%% 当前rabbit_channel进程对应的rabbit_queue_collector进程的Pid
			 stats_timer,									%% 当前rabbit_channel进程向rabbit_event发布信息的数据结构
			 confirm_enabled,								%% 是否启动confirm机制的标志，客户端通过发送confirm.select消息进行设置开启，默认是confirm机制是关闭的
			 publish_seqno,									%% 下一个消息confirm生成的确认ID
			 unconfirmed,									%% 生产者生产消息还没有confirm的dtree数据结构
			 confirmed,										%% 当前rabbit_channel进程已经得到confirm的publish_seqno列表
			 mandatory,										%% mandatory：强制性
			 capabilities,									%% capabilities:能力，客户端通过connection.start_ok消息发送上来的客户端支持的功能
			 trace_state,
			 consumer_prefetch,								%% prefetch_count=1的basic_qos方法可告知RabbitMQ只有在consumer处理并确认了上一个message后才分配新的message给他，
															%% 否则分给另一个空闲的consumer
			 reply_consumer,								%% 快速回复消费者信息的字段
			 %% flow | noflow, see rabbitmq-server#114
			 delivery_flow									%% 消息传递的时候是否要进行流量控制的标志
			}).

%% 用户权限访问缓存数量
-define(MAX_PERMISSION_CACHE_SIZE, 12).

%% 统计的关键词
-define(STATISTICS_KEYS,
		[
		 pid,
		 transactional,
		 confirm,
		 consumer_count,
		 messages_unacknowledged,
		 messages_unconfirmed,
		 messages_uncommitted,
		 acks_uncommitted,
		 prefetch_count,
		 global_prefetch_count,
		 state
		]).

%% rabbit_channel进程创建后，发布到rabbit_event中的信息关键词
-define(CREATION_EVENT_KEYS,
		[
		 pid,
		 name,
		 connection,
		 number,
		 user,
		 vhost
		]).

%% 将统计关键词和rabbit_channel创建后发布的关键词减去pid的关键词列表
-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

%% 如果当前rabbit_channel进程中stats_timer字段是fine，则进行加一操作
-define(INCR_STATS(Incs, Measure, State),
        case rabbit_event:stats_level(State, #ch.stats_timer) of
            fine -> incr_stats(Incs, Measure);
            _    -> ok
        end).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([channel_number/0]).

-type(channel_number() :: non_neg_integer()).

-spec(start_link/11 ::
        (channel_number(), pid(), pid(), pid(), string(),
         rabbit_types:protocol(), rabbit_types:user(), rabbit_types:vhost(),
         rabbit_framing:amqp_table(), pid(), pid()) ->
                            rabbit_types:ok_pid_or_error()).
-spec(do/2 :: (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(do/3 :: (pid(), rabbit_framing:amqp_method_record(),
               rabbit_types:maybe(rabbit_types:content())) -> 'ok').
-spec(do_flow/3 :: (pid(), rabbit_framing:amqp_method_record(),
                    rabbit_types:maybe(rabbit_types:content())) -> 'ok').
-spec(flush/1 :: (pid()) -> 'ok').
-spec(shutdown/1 :: (pid()) -> 'ok').
-spec(send_command/2 :: (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(deliver/4 ::
        (pid(), rabbit_types:ctag(), boolean(), rabbit_amqqueue:qmsg())
        -> 'ok').
-spec(deliver_reply/2 :: (binary(), rabbit_types:delivery()) -> 'ok').
-spec(deliver_reply_local/3 ::
        (pid(), binary(), rabbit_types:delivery()) -> 'ok').
-spec(send_credit_reply/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(send_drained/2 :: (pid(), [{rabbit_types:ctag(), non_neg_integer()}])
                        -> 'ok').
-spec(list/0 :: () -> [pid()]).
-spec(list_local/0 :: () -> [pid()]).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(info/2 :: (pid(), rabbit_types:info_keys()) -> rabbit_types:infos()).
-spec(info_all/0 :: () -> [rabbit_types:infos()]).
-spec(info_all/1 :: (rabbit_types:info_keys()) -> [rabbit_types:infos()]).
-spec(refresh_config_local/0 :: () -> 'ok').
-spec(ready_for_close/1 :: (pid()) -> 'ok').
-spec(force_event_refresh/1 :: (reference()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% rabbit_channel进程的启动入口API函数
start_link(Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User,
		   VHost, Capabilities, CollectorPid, Limiter) ->
	gen_server2:start_link(
	  ?MODULE, [Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol,
				User, VHost, Capabilities, CollectorPid, Limiter], []).


%% 处理客户端发送过来的Method消息，没有进行流量控制
do(Pid, Method) ->
	do(Pid, Method, none).


%% 处理客户端发送过来的Method消息,并带有内容，没有进行流量控制
do(Pid, Method, Content) ->
	gen_server2:cast(Pid, {method, Method, Content, noflow}).


%% Pid为rabbit_channel进程的Pid，有流量控制的发送消息到rabbit_channel进程
do_flow(Pid, Method, Content) ->
	credit_flow:send(Pid),
	gen_server2:cast(Pid, {method, Method, Content, flow}).


%% 刷新接口，目前rabbit_channel进程收到该消息后没有进行任何操作
flush(Pid) ->
	gen_server2:call(Pid, flush, infinity).


%% 让rabbit_channel进程中断停止，然后将rabbit_writer进程中要发送给客户端的消息立刻刷新，全部立刻发送给客户端
shutdown(Pid) ->
	gen_server2:cast(Pid, terminate).


%% 消息队列通过rabbit_channel进程将Msg消息发送对应的客户端
send_command(Pid, Msg) ->
	gen_server2:cast(Pid,  {command, Msg}).


%% 消息队列通知rabbit_channel进程中的向消费者发送消息
deliver(Pid, ConsumerTag, AckRequired, Msg) ->
	gen_server2:cast(Pid, {deliver, ConsumerTag, AckRequired, Msg}).


%% 直接将消息发送给指定消费者标识的rabbit_channel进程，由rabbit_channel进程发送给客户端
deliver_reply(<<"amq.rabbitmq.reply-to.", Rest/binary>>, Delivery) ->
	%% 解析出要直接将消息发送到的rabbit_channel进程Pid和消费者标识
	case decode_fast_reply_to(Rest) of
		{ok, Pid, Key} ->
			delegate:invoke_no_result(
			  Pid, {?MODULE, deliver_reply_local, [Key, Delivery]});
		error ->
			ok
	end.

%% We want to ensure people can't use this mechanism to send a message
%% to an arbitrary(任意的) process and kill it!
%% 处理将消息直接发送给客户端的接口
deliver_reply_local(Pid, Key, Delivery) ->
	case pg_local:in_group(rabbit_channels, Pid) of
		true  -> gen_server2:cast(Pid, {deliver_reply, Key, Delivery});
		false -> ok
	end.


%% 判断快速回复的设置是否存在
declare_fast_reply_to(<<"amq.rabbitmq.reply-to">>) ->
	exists;

declare_fast_reply_to(<<"amq.rabbitmq.reply-to.", Rest/binary>>) ->
	case decode_fast_reply_to(Rest) of
		{ok, Pid, Key} ->
			Msg = {declare_fast_reply_to, Key},
			rabbit_misc:with_exit_handler(
			  rabbit_misc:const(not_found),
			  fun() -> gen_server2:call(Pid, Msg, infinity) end);
		error ->
			not_found
	end;

declare_fast_reply_to(_) ->
	not_found.


%% 解析出要直接将消息发送到的rabbit_channel进程Pid和消费者标识
decode_fast_reply_to(Rest) ->
	case string:tokens(binary_to_list(Rest), ".") of
		[PidEnc, Key] -> Pid = binary_to_term(base64:decode(PidEnc)),
						 {ok, Pid, Key};
		_             -> error
	end.


send_credit_reply(Pid, Len) ->
	gen_server2:cast(Pid, {send_credit_reply, Len}).


send_drained(Pid, CTagCredit) ->
	gen_server2:cast(Pid, {send_drained, CTagCredit}).


%% 列出集群节点中所有的rabbit_channel进程
list() ->
	rabbit_misc:append_rpc_all_nodes(rabbit_mnesia:cluster_nodes(running),
									 rabbit_channel, list_local, []).


%% 得到当前节点启动的所有rabbit_channel
list_local() ->
	pg_local:get_members(rabbit_channels).


%% 拿到收集信息的关键词key
info_keys() -> ?INFO_KEYS.


%% 拿到Pid对应的rabbit_channel进程中的关键词对应的所有信息
info(Pid) ->
	gen_server2:call(Pid, info, infinity).


%% 拿到Pid对应的rabbit_channel进程中的Items关键词列表中对应的信息
info(Pid, Items) ->
	case gen_server2:call(Pid, {info, Items}, infinity) of
		{ok, Res}      -> Res;
		{error, Error} -> throw(Error)
	end.


%% 返回当前RabbitMQ集群中所有的rabbit_channel进程中的相关信息
info_all() ->
	rabbit_misc:filter_exit_map(fun (C) -> info(C) end, list()).


%% 返回当前RabbitMQ集群中所有的rabbit_channel进程中Items对应的关键信息
info_all(Items) ->
	rabbit_misc:filter_exit_map(fun (C) -> info(C, Items) end, list()).


%% rabbit_trace性能跟踪的开启关闭的VHost有变化，则通知rabbit_channel进程进行重新初始化
refresh_config_local() ->
	rabbit_misc:upmap(
	  fun (C) -> gen_server2:call(C, refresh_config, infinity) end,
	  %% 得到当前节点启动的所有rabbit_channel
	  list_local()),
	ok.


%% 通知rabbit_channel进程准备关闭，则rabbit_channel进程将rabbit_writer进程中剩余的消息立刻发送给客户端
%% 收到客户端发送过来的channel.close消息，则立刻通知所有队列和rabbit_reader，rabbit_reader进程清除Pid这个rabbit_channel进程的信息后，调用此接口让rabbit_channel进程
%% 向客户端发送channel.close_ok成功消息，在此之前会将rabbit_writer进程中剩余的消息发送给客户端
ready_for_close(Pid) ->
	gen_server2:cast(Pid, ready_for_close).


%% 将当前RabbitMQ集群中所有的rabbit_channel进程强制刷新事件，所有的rabbit_channel进程立刻发布一次进程创建对应的关键词对应的信息，同时重新初始化stats_timer字段
force_event_refresh(Ref) ->
	[gen_server2:cast(C, {force_event_refresh, Ref}) || C <- list()],
	ok.

%%---------------------------------------------------------------------------
%% rabbit_channel进程的初始化回调函数
init([Channel, ReaderPid, WriterPid, ConnPid, ConnName, Protocol, User, VHost,
	  Capabilities, CollectorPid, LimiterPid]) ->
	process_flag(trap_exit, true),
	%% 存储当前rabbit_channel进程的名字
	?store_proc_name({ConnName, Channel}),
	%% 到当前节点上的pg_local进程注册当前的rabbit_channel进程，方便用来拿到当前节点上的所有rabbit_channel进程
	ok = pg_local:join(rabbit_channels, self()),
	%% 得到配置文件中的是否进行流量控制的配置参数(默认的为true，表示开启)
	Flow = case rabbit_misc:get_env(rabbit, mirroring_flow_control, true) of
			   true   -> flow;
			   false  -> noflow
		   end,
	State = #ch{state                   = starting,							%% 当前channel进程的状态
				protocol                = Protocol,							%% AMQP协议使用的模块(rabbit_framing_amqp_0_9_1，该模块的代码是工具生成)
				channel                 = Channel,							%% 当前channel的数字编号
				reader_pid              = ReaderPid,						%% 读取Socket数据的进程Pid(rabbit_reader进程的Pid)
				writer_pid              = WriterPid,						%% 向Socket发送数据的进程Pid
				conn_pid                = ConnPid,							%% 读取Socket数据的进程Pid(rabbit_reader进程的Pid)
				conn_name               = ConnName,							%% 读取Socket数据进程的名字
				limiter                 = rabbit_limiter:new(LimiterPid),	%% limiter进程的Pid
				tx                      = none,								%% rabbit_channel进程事务操作存储数据的字段(默认是不开启的)
				next_tag                = 1,								%% 给等待消费者ack操作的消息的索引
				unacked_message_q       = queue:new(),						%% 消费者还没有确认的消息队列，使用OTP提供的queue数据结构
				user                    = User,								%% 该channel对应的用户信息
				virtual_host            = VHost,							%% Vhost
				most_recently_declared_queue = <<>>,						%% 上次最新的操作的队列名字
				queue_names             = dict:new(),						%% 跟当前rabbit_channel进程有关系的队列进程Pid对应的队列名字字典结构
				queue_monitors          = pmon:new(),						%% 监视所有跟当前rabbit_channel进程有关联的队列进程Pid结构
				consumer_mapping        = dict:new(),						%% 当前rabbit_channel进程上所有的消费者相关信息的dict数据结构
				queue_consumers         = dict:new(),						%% 队列中消费者列表，是一个字典，以队列Pid做key，value为gb_sets结构，结构中存储的都是消费者的标识
				delivering_queues       = sets:new(),						%% 正在传输的队列进程Pid集合(即需要ack的消息队列)
				queue_collector_pid     = CollectorPid,						%% 当前rabbit_channel进程对应的rabbit_collector_queue进程的Pid
				confirm_enabled         = false,							%% 是否启动confirm机制的标志，客户端通过发送confirm.select消息进行设置开启，默认是confirm机制是关闭的
				publish_seqno           = 1,								%% 下一个消息confirm生成的确认ID
				unconfirmed             = dtree:empty(),					%% 生产者生产消息还没有confirm的dtree数据结构
				confirmed               = [],								%% 当前rabbit_channel进程已经得到confirm的publish_seqno列表
				mandatory               = dtree:empty(),					%% mandatory：强制性
				capabilities            = Capabilities,						%% capabilities:能力，客户端通过connection.start_ok消息发送上来的客户端支持的功能
				trace_state             = rabbit_trace:init(VHost),
				consumer_prefetch       = 0,								%% prefetch_count=1的basic_qos方法可告知RabbitMQ只有在consumer处理并确认了上一个message后才分配新的message给他，
																			%% 否则分给另一个空闲的consumer
				reply_consumer          = none,
				delivery_flow           = Flow								%% 消息传递的时候是否要进行流量控制的标志
			   },
	%% 初始化当前rabbit_channel进程向rabbit_event发布信息状态数据结构
	State1 = rabbit_event:init_stats_timer(State, #ch.stats_timer),
	%% 通过rabbit_event发布channel创建的事件
	rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State1)),
	%% 如果RabbitMQ系统中的配置参数collect_statistics配置的不是none，则会执行收集信息的函数
	rabbit_event:if_enabled(State1, #ch.stats_timer,
							fun() -> emit_stats(State1) end),
	{ok, State1, hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


%% 返回call消息的优先级
prioritise_call(Msg, _From, _Len, _State) ->
	case Msg of
		info           -> 9;
		{info, _Items} -> 9;
		_              -> 0
	end.


%% 返回cast消息的优先级
prioritise_cast(Msg, _Len, _State) ->
	case Msg of
		{confirm,            _MsgSeqNos, _QPid} -> 5;
		{mandatory_received, _MsgSeqNo,  _QPid} -> 5;
		_                                       -> 0
	end.


%% 返回info消息的优先级
prioritise_info(Msg, _Len, _State) ->
	case Msg of
		emit_stats                   -> 7;
		_                            -> 0
	end.


%% flush的消息当前rabbit_channel进程直接返回OK，不做任何操作
handle_call(flush, _From, State) ->
	reply(ok, State);

%% 处理同步获取当前channel进程的默认关键信息的消息
handle_call(info, _From, State) ->
	reply(infos(?INFO_KEYS, State), State);

%% 处理同步获取当前channel进程指定的Items中的信息的消息
handle_call({info, Items}, _From, State) ->
    try
        reply({ok, infos(Items, State)}, State)
    catch Error -> reply({error, Error}, State)
    end;

%% rabbit_trace性能跟踪的开启关闭的VHost有变化，则通知rabbit_channel进程进行重新初始化
handle_call(refresh_config, _From, State = #ch{virtual_host = VHost}) ->
	reply(ok, State#ch{trace_state = rabbit_trace:init(VHost)});

%% 处理判断快速消费者是否设置的消息
handle_call({declare_fast_reply_to, Key}, _From,
			State = #ch{reply_consumer = Consumer}) ->
	reply(case Consumer of
			  {_, _, Key} -> exists;
			  _           -> not_found
		  end, State);

handle_call(_Request, _From, State) ->
	noreply(State).


%% 处理客户端发送的Method消息
handle_cast({method, Method, Content, Flow},
			State = #ch{reader_pid   = Reader,
						virtual_host = VHost}) ->
	case Flow of
		%% 接收到上游进程发送的消息，进入消息流模块进行处理(当接收上流进程发送的进程数减少为0的时候则需要通知上游进程增加向下游进程发送消息的数量)
		flow   -> credit_flow:ack(Reader);
		noflow -> ok
	end,
	%% 先尝试处理客户端发送过来的方法
	try handle_method(rabbit_channel_interceptor:intercept_method(
						%% 如果客户端没有发送队列的名字，则默认使用上次最后使用过的队列名字
						expand_shortcuts(Method, State), VHost),
					  Content, State) of
		{reply, Reply, NewState} ->
			ok = send(Reply, NewState),
			noreply(NewState);
		{noreply, NewState} ->
			noreply(NewState);
		stop ->
			{stop, normal, State}
	catch
		%% 如果出现异常退出，且退出原因是amqp_error这种RabbitMQ定义好的错误结构，则进行相关的处理
		exit:Reason = #amqp_error{} ->
	
			MethodName = rabbit_misc:method_record_type(Method),
			%% 处理rabbit_channel进程异常退出的事件，通知当前rabbit_channel上的所有队列进程，如果错误信息需要关闭rabbit_channel进程，则立刻通知rabbit_reader进程
			handle_exception(Reason#amqp_error{method = MethodName}, State);
		_:Reason ->
			{stop, {Reason, erlang:get_stacktrace()}, State}
	end;

%% 通知rabbit_channel进程准备关闭，则rabbit_channel进程将rabbit_writer进程中剩余的消息立刻发送给客户端
%% 收到客户端发送过来的channel.close消息，则立刻通知所有队列和rabbit_reader，rabbit_reader进程清除Pid这个rabbit_channel进程的信息后，调用此接口让rabbit_channel进程
%% 向客户端发送channel.close_ok成功消息，在此之前会将rabbit_writer进程中剩余的消息发送给客户端
handle_cast(ready_for_close, State = #ch{state      = closing,
										 writer_pid = WriterPid}) ->
	%% 向客户端发送channel.close_ok关闭成功消息
	ok = rabbit_writer:send_command_sync(WriterPid, #'channel.close_ok'{}),
	{stop, normal, State};

%% 让rabbit_channel进程中断停止，然后将rabbit_writer进程中要发送给客户端的消息立刻刷新，全部立刻发送给客户端
handle_cast(terminate, State = #ch{writer_pid = WriterPid}) ->
	%% 将rabbit_writer进程中要发送给客户端的消息立刻刷新，全部立刻发送给客户端
	ok = rabbit_writer:flush(WriterPid),
	{stop, normal, State};

%% 处理将basic.consume_ok消息发送给客户端的消息，即消费者已经设置成功，由消息队列通知对应的rabbit_channel进程
handle_cast({command, #'basic.consume_ok'{consumer_tag = CTag} = Msg}, State) ->
	ok = send(Msg, State),
	%% 监视该消费者对应的消息队列进程
	noreply(consumer_monitor(CTag, State));

%% 处理发消息到客户端，当前rabbit_channel进程收到消息通过rabbit_writer进程发送给客户端
handle_cast({command, Msg}, State) ->
	ok = send(Msg, State),
	noreply(State);

%% 处理向当前队列中指定的消费者ConsumerTag发送消息的消息(如果当前rabbit_channel进程处于closing状态，则什么都不做)
handle_cast({deliver, _CTag, _AckReq, _Msg}, State = #ch{state = closing}) ->
	noreply(State);

%% 处理向当前队列中指定的消费者ConsumerTag发送消息的消息
handle_cast({deliver, ConsumerTag, AckRequired,
			 Msg = {_QName, QPid, _MsgId, Redelivered,
					#basic_message{exchange_name = ExchangeName,
								   routing_keys  = [RoutingKey | _CcRoutes],
								   content       = Content}}},
			State = #ch{writer_pid = WriterPid,
						next_tag   = DeliveryTag}) ->
	%% 直接将消息发送到rabbit_channel进程对应的rabbit_writer进程
	ok = rabbit_writer:send_command_and_notify(
		   WriterPid, QPid, self(),
		   #'basic.deliver'{consumer_tag = ConsumerTag,
							delivery_tag = DeliveryTag,
							redelivered  = Redelivered,
							exchange     = ExchangeName#resource.name,
							routing_key  = RoutingKey},
		   Content),
	%% 在调用该函数的进程里，当操作的数据超过1000000的时候则当前进程进行一次垃圾回收
	rabbit_basic:maybe_gc_large_msg(Content),
	%% 记录消息的发送，如果被发送的消息需要等待ack，则需要将该消息存入等待ack的队列里
	noreply(record_sent(ConsumerTag, AckRequired, Msg, State));

%% 当前rabbit_channel进程状态closing，则不能够将消息直接发送给指定的消费者
handle_cast({deliver_reply, _K, _Del}, State = #ch{state = closing}) ->
	noreply(State);

%% 如果没有指定直接发送的消费者，则不能够将消息直接发送给指定的消费者
handle_cast({deliver_reply, _K, _Del}, State = #ch{reply_consumer = none}) ->
	noreply(State);

%% 将消息直接发送到指定好的消费者
handle_cast({deliver_reply, Key, #delivery{message =
											   #basic_message{exchange_name = ExchangeName,
															  routing_keys  = [RoutingKey | _CcRoutes],
															  content       = Content}}},
			State = #ch{writer_pid     = WriterPid,
						next_tag       = DeliveryTag,
						reply_consumer = {ConsumerTag, _Suffix, Key}}) ->
	%% 将消息直接发送到指定好的消费者
	ok = rabbit_writer:send_command(
		   WriterPid,
		   #'basic.deliver'{consumer_tag = ConsumerTag,
							delivery_tag = DeliveryTag,
							redelivered  = false,
							exchange     = ExchangeName#resource.name,
							routing_key  = RoutingKey},
		   Content),
	noreply(State);

%% 如果关键字不一样，则不能将消息发送到指定的消费者
handle_cast({deliver_reply, _K1, _}, State=#ch{reply_consumer = {_, _, _K2}}) ->
	noreply(State);

%% 处理send_credit_reply消息，该消息是basic.credit消息触发，然后消息队列返回给rabbit_channel进程，通过basic.credit_ok消息告诉客户端当前消息队列的消息数量
handle_cast({send_credit_reply, Len}, State = #ch{writer_pid = WriterPid}) ->
	ok = rabbit_writer:send_command(
		   WriterPid, #'basic.credit_ok'{available = Len}),
	noreply(State);

handle_cast({send_drained, CTagCredit}, State = #ch{writer_pid = WriterPid}) ->
	[ok = rabbit_writer:send_command(
			WriterPid, #'basic.credit_drained'{consumer_tag   = ConsumerTag,
											   credit_drained = CreditDrained})
			|| {ConsumerTag, CreditDrained} <- CTagCredit],
	noreply(State);

%% 强制事件刷新
handle_cast({force_event_refresh, Ref}, State) ->
	%% 将rabbit_channel进程创建的关键字信息重新发布到rabbit_event
	rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State),
						Ref),
	%% 同时重新初始化stats_timer字段
	noreply(rabbit_event:init_stats_timer(State, #ch.stats_timer));

handle_cast({mandatory_received, MsgSeqNo}, State = #ch{mandatory = Mand}) ->
	%% NB: don't call noreply/1 since we don't want to send confirms.
	noreply_coalesce(State#ch{mandatory = dtree:drop(MsgSeqNo, Mand)});

%% 队列进程发送过来的confirm消息
handle_cast({confirm, MsgSeqNos, QPid}, State = #ch{unconfirmed = UC}) ->
	%% 从unconfirmed字段中根据MsgSeqNos confirm ID和队列进程QPid拿到对应的交换机exchange信息
	{MXs, UC1} = dtree:take(MsgSeqNos, QPid, UC),
	%% NB: don't call noreply/1 since we don't want to send confirms.
	%% 将已经confirm的消息放入confirmed字段中
	noreply_coalesce(record_confirms(MXs, State#ch{unconfirmed = UC1})).

%% 处理rabbit_channel进程下游进程即队列进程通知rabbit_channel进程增加向下流进程发送消息的数量的消息
handle_info({bump_credit, Msg}, State) ->
	credit_flow:handle_bump_msg(Msg),
	noreply(State);

handle_info(timeout, State) ->
	noreply(State);

%% 处理定时器消息，发布rabbit_channel进程的相关信息到rabbit_event事件中心
handle_info(emit_stats, State) ->
	%% 发布rabbit_channel进程的相关信息到rabbit_event
	emit_stats(State),
	%% 重置向rabbit_event事件中心发布信息的数据结构
	State1 = rabbit_event:reset_stats_timer(State, #ch.stats_timer),
	%% NB: don't call noreply/1 since we don't want to kick off the
	%% stats timer.
	%% 将已经confirm的消息发送给客户端
	{noreply, send_confirms(State1), hibernate};

%% 处理队列进程有down掉的情况
handle_info({'DOWN', _MRef, process, QPid, Reason}, State) ->
	%% 队列进程中断后，处理还没有confirm的消息和mandatory相关的消息
	State1 = handle_publishing_queue_down(QPid, Reason, State),
	%% 消息队列中断后，将消息队列的所有消费者删除掉
	State3 = handle_consuming_queue_down(QPid, State1),
	%% 队列中断，处理delivering_queues字段中数据，将队列进程Pid从delivering_queues字段中删除掉
	State4 = handle_delivering_queue_down(QPid, State3),
	%% 从流量控制中将队列进程的信息清除掉
	credit_flow:peer_down(QPid),
	#ch{queue_names = QNames, queue_monitors = QMons} = State4,
	case dict:find(QPid, QNames) of
		%% 擦除rabbit_channel进程保存在进程字典中的队列信息
		{ok, QName} -> erase_queue_stats(QName);
		error       -> ok
	end,
	%% 更新最新的queue_names字段和queue_monitors字段
	noreply(State4#ch{queue_names    = dict:erase(QPid, QNames),
					  %% 删除对QPid队列进程的监视
					  queue_monitors = pmon:erase(QPid, QMons)});

handle_info({'EXIT', _Pid, Reason}, State) ->
	{stop, Reason, State}.


handle_pre_hibernate(State) ->
	ok = clear_permission_cache(),
	rabbit_event:if_enabled(
	  State, #ch.stats_timer,
	  fun () -> emit_stats(State, [{idle_since, now()}]) end),
	{hibernate, rabbit_event:stop_stats_timer(State, #ch.stats_timer)}.


%% rabbit_channel进程停止运行
terminate(Reason, State) ->
	%% 通知该channel进程下的所有队列rabbit_channel进程停止运行
	{Res, _State1} = notify_queues(State),
	case Reason of
		normal            -> ok = Res;
		shutdown          -> ok = Res;
		{shutdown, _Term} -> ok = Res;
		_                 -> ok
	end,
	%% 通知pg_local当前rabbit_channel进程已经删除
	pg_local:leave(rabbit_channels, self()),
	%% 将当前rabbit_channel进程中的信息发布到rabbit_event中
	rabbit_event:if_enabled(State, #ch.stats_timer,
							fun() -> emit_stats(State) end),
	%% 发布channel进程终止的事件
	rabbit_event:notify(channel_closed, [{pid, self()}]).


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%---------------------------------------------------------------------------

log(Level, Fmt, Args) -> rabbit_log:log(channel, Level, Fmt, Args).


%% 处理消息后，向发送者回复消息，同时进行next_state操作
reply(Reply, NewState) -> {reply, Reply, next_state(NewState), hibernate}.


%% 处理消息后，不行发送者回复消息，但是要进行next_state操作
noreply(NewState) -> {noreply, next_state(NewState), hibernate}.


%% 每一次处理完一个消息后，会立刻将已经得到confirm的消息通知客户端，同时确保向rabbit_event事件中心发布事件的定时器的启动
next_state(State) -> ensure_stats_timer(send_confirms(State)).


%% 确保向rabbit_event事件中心发布事件的定时器的启动，但是不将已经得到confirm的消息发送给客户端
noreply_coalesce(State = #ch{confirmed = C}) ->
	Timeout = case C of [] -> hibernate; _ -> 0 end,
	{noreply, ensure_stats_timer(State), Timeout}.


%% 判断是否需要启动状态定时器
ensure_stats_timer(State) ->
	rabbit_event:ensure_stats_timer(State, #ch.stats_timer, emit_stats).


%% 根据NoWait字段返回给客户端消息
return_ok(State, true, _Msg)  -> {noreply, State};

%% 如果NoWait为false，则需要将返回消息返回给客户端
return_ok(State, false, Msg)  -> {reply, Msg, State}.


ok_msg(true, _Msg) -> undefined;

ok_msg(false, Msg) -> Msg.


%% 将Command数据通过当前rabbit_channel进程对应的rabbit_writer进程发送给客户端
send(_Command, #ch{state = closing}) ->
	ok;

send(Command, #ch{writer_pid = WriterPid}) ->
	ok = rabbit_writer:send_command(WriterPid, Command).


%% 处理rabbit_channel进程异常退出的事件，通知当前rabbit_channel上的所有队列进程，如果错误信息需要关闭rabbit_channel进程，则立刻通知rabbit_reader进程
handle_exception(Reason, State = #ch{protocol     = Protocol,
									 channel      = Channel,
									 writer_pid   = WriterPid,
									 reader_pid   = ReaderPid,
									 conn_pid     = ConnPid,
									 conn_name    = ConnName,
									 virtual_host = VHost,
									 user         = User}) ->
	%% something bad's happened: notify_queues may not be 'ok'
	%% 通知该channel进程下的所有队列rabbit_channel进程停止运行
	{_Result, State1} = notify_queues(State),
	case rabbit_binary_generator:map_exception(Channel, Reason, Protocol) of
		{Channel, CloseMethod} ->
			%% 根据配置文件中配置的channel日志等级进行写日志
			log(error, "Channel error on connection ~p (~s, vhost: '~s',"
					" user: '~s'), channel ~p:~n~p~n",
				[ConnPid, ConnName, VHost, User#user.username,
				 Channel, Reason]),
			%% 将错误信息发送给客户端
			ok = rabbit_writer:send_command(WriterPid, CloseMethod),
			{noreply, State1};
		%% 如果返回的channel为0则将当前rabbit_channel进程停止，同时立刻通知rabbit_reader进程
		{0, _} ->
			ReaderPid ! {channel_exit, Channel, Reason},
			{stop, normal, State1}
	end.

-ifdef(use_specs).
-spec(precondition_failed/1 :: (string()) -> no_return()).
-endif.
precondition_failed(Format) -> precondition_failed(Format, []).

-ifdef(use_specs).
-spec(precondition_failed/2 :: (string(), [any()]) -> no_return()).
-endif.
precondition_failed(Format, Params) ->
	rabbit_misc:protocol_error(precondition_failed, Format, Params).


%% 向客户端返回创建队列成功的消息
return_queue_declare_ok(#resource{name = ActualName},
						NoWait, MessageCount, ConsumerCount, State) ->
	return_ok(State#ch{most_recently_declared_queue = ActualName}, NoWait,
					  #'queue.declare_ok'{queue          = ActualName,
										  message_count  = MessageCount,
										  consumer_count = ConsumerCount}).


%% 检查资源可操作访问的合法性(Perm为检查的类型)
check_resource_access(User, Resource, Perm) ->
	V = {Resource, Perm},
	Cache = case get(permission_cache) of
				undefined -> [];
				Other     -> Other
			end,
	case lists:member(V, Cache) of
		true  -> ok;
		false -> ok = rabbit_access_control:check_resource_access(
						User, Resource, Perm),
				 %% 删除多余的用户权限缓存数量
				 CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
				 %% 将最新的用户权限放入进程字典中，淘汰时间较早的用户权限
				 put(permission_cache, [V | CacheTail])
	end.


clear_permission_cache() -> erase(permission_cache),
							ok.


%% 检查User用户对资源Resource是否能够进行配置
check_configure_permitted(Resource, #ch{user = User}) ->
	check_resource_access(User, Resource, configure).


%% 检查User用户对资源Resource是否能够进行写操作
check_write_permitted(Resource, #ch{user = User}) ->
	check_resource_access(User, Resource, write).


%% 检查User用户对资源Resource能否进行读操作
check_read_permitted(Resource, #ch{user = User}) ->
	check_resource_access(User, Resource, read).


%% 检查用户名是否正确，如果undefined表示正确
check_user_id_header(#'P_basic'{user_id = undefined}, _) ->
	ok;

%% 如果发送消息里面带的Username等于rabbit_channel进程中的Username则表示正确
check_user_id_header(#'P_basic'{user_id = Username},
					 #ch{user = #user{username = Username}}) ->
	ok;

check_user_id_header(
  #'P_basic'{}, #ch{user = #user{authz_backends =
									 [{rabbit_auth_backend_dummy, _}]}}) ->
	ok;

%% impersonator：冒领
check_user_id_header(#'P_basic'{user_id = Claimed},
					 #ch{user = #user{username = Actual,
									  tags     = Tags}}) ->
	case lists:member(impersonator, Tags) of
		true  -> ok;
		false -> precondition_failed(
				   "user_id property set to '~s' but authenticated user was "
					   "'~s'", [Claimed, Actual])
	end.


%% 检查消息特性中的过期时间是否设置正确
check_expiration_header(Props) ->
	case rabbit_basic:parse_expiration(Props) of
		{ok, _}    -> ok;
		{error, E} -> precondition_failed("invalid expiration '~s': ~p",
										  [Props#'P_basic'.expiration, E])
	end.


%% 检查exchange结构中的internal不能为true
check_internal_exchange(#exchange{name = Name, internal = true}) ->
	rabbit_misc:protocol_error(access_refused,
							   "cannot publish to internal ~s",
							   [rabbit_misc:rs(Name)]);

check_internal_exchange(_) ->
	ok.


%% 检查消息内容的大小
check_msg_size(Content) ->
	%% rabbit_channel进程接收大的消息块，因此添加了进程字典，记录消息的累计大小，如果超过一定上限则进行一次垃圾回收
	Size = rabbit_basic:maybe_gc_large_msg(Content),
	%% 消息的大小不能超过2147383648
	case Size > ?MAX_MSG_SIZE of
		true  -> precondition_failed("message size ~B larger than max size ~B",
									 [Size, ?MAX_MSG_SIZE]);
		false -> ok
	end.


%% 组装队列的资源结构
qbin_to_resource(QueueNameBin, State) ->
	name_to_resource(queue, QueueNameBin, State).


%% 组装资源数据结构
name_to_resource(Type, NameBin, #ch{virtual_host = VHostPath}) ->
	rabbit_misc:r(VHostPath, Type, NameBin).


%% 如果客户端没有发送队列的名字，则默认使用上次最后使用过的队列名字
expand_queue_name_shortcut(<<>>, #ch{most_recently_declared_queue = <<>>}) ->
	rabbit_misc:protocol_error(not_found, "no previously declared queue", []);

expand_queue_name_shortcut(<<>>, #ch{most_recently_declared_queue = MRDQ}) ->
	MRDQ;

expand_queue_name_shortcut(QueueNameBin, _) ->
	QueueNameBin.


%% 如果客户端没有传入路由key，则默认使用上次最后使用过的队列名字
expand_routing_key_shortcut(<<>>, <<>>,
							#ch{most_recently_declared_queue = <<>>}) ->
	rabbit_misc:protocol_error(not_found, "no previously declared queue", []);

expand_routing_key_shortcut(<<>>, <<>>,
							#ch{most_recently_declared_queue = MRDQ}) ->
	MRDQ;

expand_routing_key_shortcut(_QueueNameBin, RoutingKey, _State) ->
	RoutingKey.


%% 如果客户端没有发送队列的名字，则默认使用上次最后使用过的队列名字
expand_shortcuts(#'basic.get'    {queue = Q} = M, State) ->
	M#'basic.get'    {queue = expand_queue_name_shortcut(Q, State)};

expand_shortcuts(#'basic.consume'{queue = Q} = M, State) ->
	M#'basic.consume'{queue = expand_queue_name_shortcut(Q, State)};

expand_shortcuts(#'queue.delete' {queue = Q} = M, State) ->
	M#'queue.delete' {queue = expand_queue_name_shortcut(Q, State)};

expand_shortcuts(#'queue.purge'  {queue = Q} = M, State) ->
	M#'queue.purge'  {queue = expand_queue_name_shortcut(Q, State)};

expand_shortcuts(#'queue.bind'   {queue = Q, routing_key = K} = M, State) ->
	M#'queue.bind'   {queue       = expand_queue_name_shortcut(Q, State),
					  %% 如果队列名字和路由信息都为空则使用上次最后使用过的队列名字作为路由信息
					  routing_key = expand_routing_key_shortcut(Q, K, State)};

expand_shortcuts(#'queue.unbind' {queue = Q, routing_key = K} = M, State) ->
	M#'queue.unbind' {queue       = expand_queue_name_shortcut(Q, State),
					  %% 如果队列名字和路由信息都为空则使用上次最后使用过的队列名字作为路由信息
					  routing_key = expand_routing_key_shortcut(Q, K, State)};

expand_shortcuts(M, _State) ->
	M.


%% 检查当前的exchange名字是否合法，交换机的名字不能为空
check_not_default_exchange(#resource{kind = exchange, name = <<"">>}) ->
	rabbit_misc:protocol_error(
	  access_refused, "operation not permitted on the default exchange", []);

check_not_default_exchange(_) ->
	ok.


check_exchange_deletion(XName = #resource{name = <<"amq.", _/binary>>,
										  kind = exchange}) ->
	rabbit_misc:protocol_error(
	  access_refused, "deletion of system ~s not allowed",
	  [rabbit_misc:rs(XName)]);

check_exchange_deletion(_) ->
	ok.

%% check that an exchange/queue name does not contain the reserved
%% "amq."  prefix.
%%
%% As per the AMQP 0-9-1 spec, the exclusion of "amq." prefixed names
%% only applies on actual creation, and not in the cases where the
%% entity already exists or passive=true.
%%
%% NB: We deliberately do not enforce the other constraints on names
%% required by the spec.
%% 名字不能以amq.开头，这个开头的队列名字是临时队列
check_name(Kind, NameBin = <<"amq.", _/binary>>) ->
	rabbit_misc:protocol_error(
	  access_refused,
	  "~s name '~s' contains reserved prefix 'amq.*'",[Kind, NameBin]);

check_name(_Kind, NameBin) ->
	NameBin.


%% 设置快速发送的消息特性
maybe_set_fast_reply_to(
  C = #content{properties = P = #'P_basic'{reply_to =
											   <<"amq.rabbitmq.reply-to">>}},
  #ch{reply_consumer = ReplyConsumer}) ->
	case ReplyConsumer of
		none         -> rabbit_misc:protocol_error(
						  precondition_failed,
						  "fast reply consumer does not exist", []);
		{_, Suf, _K} -> Rep = <<"amq.rabbitmq.reply-to.", Suf/binary>>,
						rabbit_binary_generator:clear_encoded_content(
						  C#content{properties = P#'P_basic'{reply_to = Rep}})
	end;

maybe_set_fast_reply_to(C, _State) ->
	C.


%% 将已经confirm的消息放入confirmed字段中
record_confirms([], State) ->
	State;

record_confirms(MXs, State = #ch{confirmed = C}) ->
	State#ch{confirmed = [MXs | C]}.


%% 以下都是处理客户端发送过来的消息
%% 处理channel.open消息，将当前rabbit_channel进程中的信息发布到rabbit_event，同时将当前进程的状态置为running
handle_method(#'channel.open'{}, _, State = #ch{state = starting}) ->
	%% Don't leave "starting" as the state for 5s. TODO is this TRTTD?
	%% 将当前rabbit_channel进程状态置为running，则当前进程正式进入工作状态
	State1 = State#ch{state = running},
	%% 将当前rabbit_channel进程中的信息发布到rabbit_event
	rabbit_event:if_enabled(State1, #ch.stats_timer,
							fun() -> emit_stats(State1) end),
	{reply, #'channel.open_ok'{}, State1};

%% 如果当前rabbit_channel进程不是starting状态，再次接受到channel.open消息，则通知客户端消息重复发送错误消息
handle_method(#'channel.open'{}, _, _State) ->
	rabbit_misc:protocol_error(
	  command_invalid, "second 'channel.open' seen", []);


%% 如果当前rabbit_channel进程还处于starting，则受到其他要处理的消息，则通知客户端没有受到channel.open消息，就进行其他操作是错误的行为
handle_method(_Method, _, #ch{state = starting}) ->
	rabbit_misc:protocol_error(channel_error, "expected 'channel.open'", []);

%% 处理channel.close_ok消息，则将当前rabbit_channel进程状态置为closing，同时将当前进程停止掉
handle_method(#'channel.close_ok'{}, _, #ch{state = closing}) ->
	stop;

%% 当前rabbit_channel进程收到channel.close消息且当前状态为closing，则发送channel.close_ok消息通知客户端，然后客户端再发channel.close_ok通知RabbitMQ服务端，将rabbit_channel进程终止掉
handle_method(#'channel.close'{}, _, State = #ch{writer_pid = WriterPid,
												 state      = closing}) ->
	ok = rabbit_writer:send_command(WriterPid, #'channel.close_ok'{}),
	{noreply, State};

%% 如果当前rabbit_channel进程处于closing状态，则不处理任何客户端消息
handle_method(_Method, _, State = #ch{state = closing}) ->
	{noreply, State};

%% 当前rabbit_channel进程是非closing，同时收到了channel.close消息，则通知rabbit_reader进程，当前进程关闭
handle_method(#'channel.close'{}, _, State = #ch{reader_pid = ReaderPid}) ->
	%% 通知该channel进程下的所有队列rabbit_channel进程停止运行
	{ok, State1} = notify_queues(State),
	%% We issue the channel.close_ok response after a handshake with
	%% the reader, the other half of which is ready_for_close. That
	%% way the reader forgets about the channel before we send the
	%% response (and this channel process terminates). If we didn't do
	%% that, a channel.open for the same channel number, which a
	%% client is entitled to send as soon as it has received the
	%% close_ok, might be received by the reader before it has seen
	%% the termination and hence be sent to the old, now dead/dying
	%% channel process, instead of a new process, and thus lost.
	%% 通知rabbit_reader进程当前rabbit_channel进程终止
	ReaderPid ! {channel_closing, self()},
	{noreply, State1};

%% Even though the spec prohibits the client from sending commands
%% while waiting for the reply to a synchronous command, we generally
%% do allow this...except in the case of a pending tx.commit, where
%% it could wreak havoc.
%% 当当前rabbit_channel进程正在进行事务提交操作或者事务失败，则直接通知客户端当前channel正在进行事务提交操作
handle_method(_Method, _, #ch{tx = Tx})
  when Tx =:= committing orelse Tx =:= failed ->
	rabbit_misc:protocol_error(
	  channel_error, "unexpected command while processing 'tx.commit'", []);

handle_method(#'access.request'{},_, State) ->
	{reply, #'access.request_ok'{ticket = 1}, State};

%% immediate：立即，basic.publish消息中的这个参数现在版本已经不再支持
%% *************************************************************************************************************************
%% immediate:当immediate标志位设置为true时，如果exchange在将消息route到queue(s)时发现对应的queue上没有消费者，
%% 那么这条消息不会放入队列中。当与消息routeKey关联的所有queue(一个或多个)都没有消费者时，该消息会通过basic.return方法返还给生产者
%% *************************************************************************************************************************
handle_method(#'basic.publish'{immediate = true}, _Content, _State) ->
	%% implemented：实现
	rabbit_misc:protocol_error(not_implemented, "immediate=true", []);

%% 客户端向RabbitMQ发送的发布消息的消息(mandatory:强制性)
%% *************************************************************************************************************************
%% mandatory:当mandatory标志位设置为true时，如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，
%% 那么会调用basic.return方法将消息返还给生产者；当mandatory设为false时，出现上述情形broker会直接将消息扔掉
%% *************************************************************************************************************************
handle_method(#'basic.publish'{exchange    = ExchangeNameBin,
							   routing_key = RoutingKey,
							   mandatory   = Mandatory},
			  Content, State = #ch{virtual_host    = VHostPath,
								   tx              = Tx,
								   channel         = ChannelNum,
								   confirm_enabled = ConfirmEnabled,
								   trace_state     = TraceState,
								   user            = #user{username = Username},
								   conn_name       = ConnName,
								   delivery_flow   = Flow}) ->
	%% 检查消息内容的大小
	check_msg_size(Content),
	%% 组装exchange的资源结构
	ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
	%% 检查该用户在VHostPath虚拟机下对该exchange写的权限
	check_write_permitted(ExchangeName, State),
	%% 查找对应的exchange是否存在(如果存在则从mnesia数据库取出该数据)
	Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
	%% 检查exchange结构中的internal不能为true(不能够向RabbitMQ系统内部的exchange发送消息)
	check_internal_exchange(Exchange),
	%% We decode the content's properties here because we're almost
	%% certain to want to look at delivery-mode and priority.
	%% 在此处解析消息特性主要是想要得到当前消息的delivery-mode传递模式和优先级
	DecodedContent = #content{properties = Props} =
								 %% 设置快速发送的消息特性
								 maybe_set_fast_reply_to(
								   %% 确保消息内容的解析完全，如果properties尚未解析则通过Protocol进行解析
								   rabbit_binary_parser:ensure_content_decoded(Content), State),
	%% 检查用户名是否正确
	check_user_id_header(Props, State),
	%% 检查消息特性中的过期时间是否设置正确
	check_expiration_header(Props),
	%% 看当前是否处在事务模式或者confirm模式下
	DoConfirm = Tx =/= none orelse ConfirmEnabled,
	{MsgSeqNo, State1} =
		case DoConfirm orelse Mandatory of
			false -> {undefined, State};
			true  -> SeqNo = State#ch.publish_seqno,
					 %% 得到消息同生产者之间进行confirm的编号ID
					 {SeqNo, State#ch{publish_seqno = SeqNo + 1}}
		end,
	%% 组装消息结构(组装basic_message机构)
	case rabbit_basic:message(ExchangeName, RoutingKey, DecodedContent) of
		{ok, Message} ->
			%% 组装delivery的数据结构
			Delivery = rabbit_basic:delivery(
						 Mandatory, DoConfirm, Message, MsgSeqNo),
			%% 根据交换机和Delivery结构得到路由到的队列名字的列表
			QNames = rabbit_exchange:route(Exchange, Delivery),
			rabbit_trace:tap_in(Message, QNames, ConnName, ChannelNum,
								Username, TraceState),
			%% 将当前rabbit_channel中的流量控制标志放到delivery结构中
			DQ = {Delivery#delivery{flow = Flow}, QNames},
			{noreply, case Tx of
						  %% 直接将消息传送到路由到的消息队列里
						  none         -> deliver_to_queues(DQ, State1);
						  {Msgs, Acks} -> %% 将要发送到队列的消息保存到tx事务字段中
							  			  Msgs1 = queue:in(DQ, Msgs),
										  %% 将要发送的消息和队列名字组成的结构作为元素放入到事务字段tuple第一个字段中的队列中
										  State1#ch{tx = {Msgs1, Acks}}
					  end};
		{error, Reason} ->
			precondition_failed("invalid message: ~p", [Reason])
	end;

%% requeue参数:表示被拒绝的消息是否可以被重新分配
%% 该消息是客户端拒绝ack消息，因此如果requeue为true，则需要将消息重新放入到队列中，否则丢弃该消息(multiple为true表示小于DeliveryTag的消息都要进程拒绝操作)
handle_method(#'basic.nack'{delivery_tag = DeliveryTag,
							multiple     = Multiple,
							requeue      = Requeue}, _, State) ->
	reject(DeliveryTag, Requeue, Multiple, State);

%% 消费者对消息进行ack操作(multiple为true表示小于DeliveryTag的消息都要进行ack操作)
handle_method(#'basic.ack'{delivery_tag = DeliveryTag,
						   multiple     = Multiple},
			  _, State = #ch{unacked_message_q = UAMQ, tx = Tx}) ->
	%% 根据客户端传来的DeliveryTag和Multiple得到能够ack的消息列表
	{Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
	%% 将剩下的等待消费者ack操作的消息列表更新到unacked_message_q字段
	State1 = State#ch{unacked_message_q = Remaining},
	{noreply, case Tx of
				  %% 当前rabbit_channel进程不在事务状态中，则直接进行ack操作
				  none         -> ack(Acked, State1),
								  State1;
				  %% 事务中先记录已经ack的消息
				  {Msgs, Acks} -> Acks1 = ack_cons(ack, Acked, Acks),
								  State1#ch{tx = {Msgs, Acks1}}
			  end};

%% 客户端消费者根据队列名字来拿消息
handle_method(#'basic.get'{queue = QueueNameBin, no_ack = NoAck},
			  _, State = #ch{writer_pid = WriterPid,
							 conn_pid   = ConnPid,
							 limiter    = Limiter,
							 next_tag   = DeliveryTag}) ->
	%% 组装队列的资源结构
	QueueName = qbin_to_resource(QueueNameBin, State),
	%% 检查资源对该用户读的合法性
	check_read_permitted(QueueName, State),
	case rabbit_amqqueue:with_exclusive_access_or_die(
		   QueueName, ConnPid,
		   %% 实际从队列进程去取消息的函数
		   fun (Q) -> rabbit_amqqueue:basic_get(
						Q, self(), NoAck, rabbit_limiter:pid(Limiter))
		   end) of
		{ok, MessageCount,
		 Msg = {QName, QPid, _MsgId, Redelivered,
				#basic_message{exchange_name = ExchangeName,
							   routing_keys  = [RoutingKey | _CcRoutes],
							   content       = Content}}} ->
			%% 返回给客户端取得的消息
			ok = rabbit_writer:send_command(
				   WriterPid,
				   %% 组装basic.get_ok消息，通过当前rabbit_channel进程对应的rabbit_writer进程发送出去
				   #'basic.get_ok'{delivery_tag  = DeliveryTag,
								   redelivered   = Redelivered,
								   exchange      = ExchangeName#resource.name,
								   routing_key   = RoutingKey,
								   message_count = MessageCount},
				   Content),
			%% 监视正在传递消息的队列进程
			State1 = monitor_delivering_queue(NoAck, QPid, QName, State),
			%% 记录消息的发送，如果被发送的消息需要等待ack，则需要将该消息存入等待ack的队列里
			{noreply, record_sent(none, not(NoAck), Msg, State1)};
		empty ->
			{reply, #'basic.get_empty'{}, State}
	end;

%% 处理设置快速消费者的消息
handle_method(#'basic.consume'{queue        = <<"amq.rabbitmq.reply-to">>,
							   consumer_tag = CTag0,
							   no_ack       = NoAck,
							   nowait       = NoWait},
			  _, State = #ch{reply_consumer   = ReplyConsumer,
							 consumer_mapping = ConsumerMapping}) ->
	%% 判断消费者标识是否已经被使用过
	case dict:find(CTag0, ConsumerMapping) of
		error ->
			case {ReplyConsumer, NoAck} of
				{none, true} ->
					%% 如果消费的标志为空，则通过rabbit_guid:gen_secure生成一个新的独一无二的标识
					CTag = case CTag0 of
							   <<>>  -> rabbit_guid:binary(
										  rabbit_guid:gen_secure(), "amq.ctag");
							   Other -> Other
						   end,
					%% Precalculate both suffix and key; base64 encoding is
					%% expensive
					%% 将消费者关键字进行编码
					Key = base64:encode(rabbit_guid:gen_secure()),
					%% 将当前rabbit_channel进程的Pid进行编码
					PidEnc = base64:encode(term_to_binary(self())),
					Suffix = <<PidEnc/binary, ".", Key/binary>>,
					Consumer = {CTag, Suffix, binary_to_list(Key)},
					%% 更新reply_consumer字段
					State1 = State#ch{reply_consumer = Consumer},
					case NoWait of
						true  -> {noreply, State1};
						%% 通过basic.consume_ok消息通知客户端成为消费者成功
						false -> Rep = #'basic.consume_ok'{consumer_tag = CTag},
								 {reply, Rep, State1}
					end;
				{_, false} ->
					rabbit_misc:protocol_error(
					  precondition_failed,
					  "reply consumer cannot acknowledge", []);
				_ ->
					rabbit_misc:protocol_error(
					  precondition_failed, "reply consumer already set", [])
			end;
		{ok, _} ->
			%% 传入的消费者标识已经被人使用
			%% Attempted reuse of consumer tag.
			rabbit_misc:protocol_error(
			  not_allowed, "attempt to reuse consumer tag '~s'", [CTag0])
	end;

%% 处理快速消费者的删除消息
handle_method(#'basic.cancel'{consumer_tag = ConsumerTag, nowait = NoWait},
			  _, State = #ch{reply_consumer = {ConsumerTag, _, _}}) ->
	State1 = State#ch{reply_consumer = none},
	case NoWait of
		true  -> {noreply, State1};
		%% 如果客户端要等待设置成功，则立刻将basic.cancel_ok消息发送给客户端
		false -> Rep = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
				 {reply, Rep, State1}
	end;

%% 处理增加消费者的消息(prefetch:预取)
handle_method(#'basic.consume'{queue        = QueueNameBin,
							   consumer_tag = ConsumerTag,
							   no_local     = _, % FIXME: implement
							   no_ack       = NoAck,
							   exclusive    = ExclusiveConsume,
							   nowait       = NoWait,
							   arguments    = Args},
			  _, State = #ch{consumer_prefetch = ConsumerPrefetch,
							 consumer_mapping  = ConsumerMapping}) ->
	case dict:find(ConsumerTag, ConsumerMapping) of
		error ->
			%% 组装队列的资源数据结构
			QueueName = qbin_to_resource(QueueNameBin, State),
			%% 检查用户对该队列读的权限
			check_read_permitted(QueueName, State),
			%% 如果消费的标志为空，则通过rabbit_guid:gen_secure生成一个新的独一无二的标识
			ActualConsumerTag =
				case ConsumerTag of
					<<>>  -> rabbit_guid:binary(rabbit_guid:gen_secure(),
												"amq.ctag");
					Other -> Other
				end,
			%% 实际处理队列中增加消费者操作
			case basic_consume(
				   QueueName, NoAck, ConsumerPrefetch, ActualConsumerTag,
				   ExclusiveConsume, Args, NoWait, State) of
				{ok, State1} ->
					{noreply, State1};
				{error, exclusive_consume_unavailable} ->
					rabbit_misc:protocol_error(
					  access_refused, "~s in exclusive use",
					  [rabbit_misc:rs(QueueName)])
			end;
		{ok, _} ->
			%% reuse：重用
			%% Attempted reuse of consumer tag.
			%% 通知客户端使用了重复的消费者标识ConsumerTag
			rabbit_misc:protocol_error(
			  not_allowed, "attempt to reuse consumer tag '~s'", [ConsumerTag])
	end;

%% 处理删除消费者消息
handle_method(#'basic.cancel'{consumer_tag = ConsumerTag, nowait = NoWait},
			  _, State = #ch{consumer_mapping = ConsumerMapping,
							 queue_consumers  = QCons}) ->
	%% 准备返回给客户端的取消成功的消息
	OkMsg = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
	%% 从消费者对应的字典里查询
	case dict:find(ConsumerTag, ConsumerMapping) of
		error ->
			%% Spec requires we ignore this situation.
			return_ok(State, NoWait, OkMsg);
		{ok, {Q = #amqqueue{pid = QPid}, _CParams}} ->
			%% 将消费者字典中的信息删除掉
			ConsumerMapping1 = dict:erase(ConsumerTag, ConsumerMapping),
			%% 将队列进程Pid对应的消费者名字删除掉
			QCons1 =
				case dict:find(QPid, QCons) of
					error       -> QCons;
					{ok, CTags} -> CTags1 = gb_sets:delete(ConsumerTag, CTags),
								   case gb_sets:is_empty(CTags1) of
									   true  -> dict:erase(QPid, QCons);
									   false -> dict:store(QPid, CTags1, QCons)
								   end
				end,
			%% 更新最新的消费者字典和队列进程对应的消费者名字字典
			NewState = State#ch{consumer_mapping = ConsumerMapping1,
								queue_consumers  = QCons1},
			%% In order to ensure that no more messages are sent to
			%% the consumer after the cancel_ok has been sent, we get
			%% the queue process to send the cancel_ok on our
			%% behalf. If we were sending the cancel_ok ourselves it
			%% might overtake a message sent previously by the queue.
			case rabbit_misc:with_exit_handler(
				   fun () -> {error, not_found} end,
				   %% call到消息队列直接删除掉消费者
				   fun () ->
							rabbit_amqqueue:basic_cancel(
							  Q, self(), ConsumerTag, ok_msg(NoWait, OkMsg))
				   end) of
				ok ->
					{noreply, NewState};
				{error, not_found} ->
					%% Spec requires we ignore this situation.
					return_ok(NewState, NoWait, OkMsg)
			end
	end;


%% prefetch_size:限制预取的消息大小参数暂时没有实现(如果prefetch_size字段不是默认值0，则通知客户端出错)
handle_method(#'basic.qos'{prefetch_size = Size}, _, _State) when Size /= 0 ->
	%% 通知客户端RabbitMQ系统没有实现该参数的功能
	rabbit_misc:protocol_error(not_implemented,
							   "prefetch_size!=0 (~w)", [Size]);

%% basic.qos为false，则设置当前rabbit_channel进程中消息预取的数量
%% global为false，则表示当前设置的消息预取数量只对以后增加的消费者有作用
handle_method(#'basic.qos'{global         = false,
						   prefetch_count = PrefetchCount}, _, State) ->
	{reply, #'basic.qos_ok'{}, State#ch{consumer_prefetch = PrefetchCount}};

%% basic.qos消息中global字段为true，prefetch_count=0，表示取消当前rabbit_channel进程取消消息预取的限制
%% global：glotal=true时表示在当前channel上所有的consumer都生效，否则只对设置了之后新建的consumer生效
handle_method(#'basic.qos'{global         = true,
						   prefetch_count = 0},
			  _, State = #ch{limiter = Limiter}) ->
	%% call到rabbit_limiter进程取消掉消息预取的限制
	Limiter1 = rabbit_limiter:unlimit_prefetch(Limiter),
	{reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter1}};

%% basic.qos消息中global字段为true，prefetch_count/=0，表示同时限制消息预取的数量，已经ack操作的上限
%% global：glotal=true时表示在当前channel上所有的consumer都生效，否则只对设置了之后新建的consumer生效
handle_method(#'basic.qos'{global         = true,
						   prefetch_count = PrefetchCount},
			  _, State = #ch{limiter = Limiter, unacked_message_q = UAMQ}) ->
	%% TODO queue:len(UAMQ) is not strictly right since that counts
	%% unacked messages from basic.get too. Pretty obscure though.
	%% 同时限制消息预取的数量，已经ack操作的上限
	Limiter1 = rabbit_limiter:limit_prefetch(Limiter,
											 PrefetchCount, queue:len(UAMQ)),
	case ((not rabbit_limiter:is_active(Limiter)) andalso
			  rabbit_limiter:is_active(Limiter1)) of
		%% 如果以前没有限制消息预取的数量，现在限制了消息的预取数量，则需要通知消费者对应的所有消息队列进程进行激活
		true  -> rabbit_amqqueue:activate_limit_all(
				   %% 收集所有的消费者对应的消息队列的Pid列表
				   consumer_queues(State#ch.consumer_mapping), self());
		false -> ok
	end,
	{reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter1}};

%% basic.recover_async消息中的字段requeue不能设置为true，将当前rabbit_channel进程中还没有ack的消息全部重新放回消息队列
handle_method(#'basic.recover_async'{requeue = true},
			  _, State = #ch{unacked_message_q = UAMQ, limiter = Limiter}) ->
	OkFun = fun () -> ok end,
	UAMQL = queue:to_list(UAMQ),
	%% 将当前rabbit_channel进程中还没有ack的消息全部重新放回消息队列
	foreach_per_queue(
	  fun (QPid, MsgIds) ->
			   rabbit_misc:with_exit_handler(
				 OkFun,
				 fun () -> rabbit_amqqueue:requeue(QPid, MsgIds, self()) end)
	  end, lists:reverse(UAMQL)),
	%% 如果全局的预取消息开启，则通知rabbit_limiter进程最新的已经ack消息数量
	ok = notify_limiter(Limiter, UAMQL),
	%% No answer required - basic.recover is the newer, synchronous
	%% variant of this method
	%% 给
	{noreply, State#ch{unacked_message_q = queue:new()}};

%% basic.recover_async消息中的字段requeue不能设置为false，当前系统不支持
handle_method(#'basic.recover_async'{requeue = false}, _, _State) ->
	rabbit_misc:protocol_error(not_implemented, "requeue=false", []);

%% 同步的将当前rabbit_channel进程的还未ack的消息回复，如果requeue为true，则将消息重新放入到消息队列中，否则将还未ack的消息丢弃
handle_method(#'basic.recover'{requeue = Requeue}, Content, State) ->
	{noreply, State1} = handle_method(#'basic.recover_async'{requeue = Requeue},
									  Content, State),
	{reply, #'basic.recover_ok'{}, State1};

%% 处理basic.reject消息，将DeliveryTag对应的单个还未ack的消息重新放入到队列中(requeue为true表示重新将为ack的消息重新放入到队列)
handle_method(#'basic.reject'{delivery_tag = DeliveryTag, requeue = Requeue},
			  _, State) ->
	%% 消费者拒绝接受消息的处理
	reject(DeliveryTag, Requeue, false, State);

%% 处理exchange.declare消息(声明一个新的exchange)
handle_method(#'exchange.declare'{exchange    = ExchangeNameBin,
								  type        = TypeNameBin,
								  passive     = false,
								  durable     = Durable,
								  auto_delete = AutoDelete,
								  internal    = Internal,
								  nowait      = NoWait,
								  arguments   = Args},
			  _, State = #ch{virtual_host = VHostPath}) ->
	%% 不用的exchange类型用不同的模块来进行相关处理
	%% direct类型 -> rabbit_exchange_type_direct模块(消息与一个特定的路由键完全匹配)
	%% fanout类型 -> rabbit_exchange_type_fanout模块(一个发送到交换机的消息都会被转发到与该交换机绑定的所有队列上)
	%% topic类型  -> rabbit_exchange_type_topic模块(将路由键和某模式进行匹配)
	%% 检查客户端传入的Exchange类型是否合法，如果合法则将二进制类型转化为atom类型
	CheckedType = rabbit_exchange:check_type(TypeNameBin),
	%% 从工具文件中组装一个exchange资源数据结构
	ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
	%% 检查当前的exchange名字是否合法，交换机的名字不能为空
	check_not_default_exchange(ExchangeName),
	%% 检查当前用户能否使用该VirtualHost(configure:配置)
	check_configure_permitted(ExchangeName, State),
	%% 从rabbit_exchange数据库表中查询
	X = case rabbit_exchange:lookup(ExchangeName) of
			{ok, FoundX} -> FoundX;
			{error, not_found} ->
				%% exchange名字检查，exchange的名字不能以amq.开头，因为amq.是保留字
				check_name('exchange', ExchangeNameBin),
				AeKey = <<"alternate-exchange">>,
				%% 默认Args为[]
				%% alternate：备用
				case rabbit_misc:r_arg(VHostPath, exchange, Args, AeKey) of
					undefined -> ok;
					{error, {invalid_type, Type}} ->
						precondition_failed(
						  "invalid type '~s' for arg '~s' in ~s",
						  [Type, AeKey, rabbit_misc:rs(ExchangeName)]);
					%% exchange声明参数里面配置有备用的名字
					AName     -> %% 检查当前用户对ExchangeName读取的权限
								 check_read_permitted(ExchangeName, State),
								 %% 检查当前用户对备用名字AName的写权限
								 check_write_permitted(AName, State),
								 ok
				end,
				%% 重新声明个交换机exchange
				rabbit_exchange:declare(ExchangeName,
										CheckedType,
										Durable,
										AutoDelete,
										Internal,
										Args)
		end,
	%% 断言是否相等(判读最新得到的exchange数据结构和客户端发送过来的是一样的判断)
	ok = rabbit_exchange:assert_equivalence(X, CheckedType, Durable,
											AutoDelete, Internal, Args),
	%% 如果NoWait为false则向客户端发送exchange.declare_ok，通知客户端exchange创建成功
	return_ok(State, NoWait, #'exchange.declare_ok'{});

%% 如果将passive设置为true，则是查看交换机ExchangeNameBin是否存在，如果存在则返回true，否者返回Error
handle_method(#'exchange.declare'{exchange = ExchangeNameBin,
								  passive  = true,
								  nowait   = NoWait},
			  _, State = #ch{virtual_host = VHostPath}) ->
	%% 组装exchange的资源结构信息
	ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
	%% 检查当前的exchange名字是否合法，交换机的名字不能为空
	check_not_default_exchange(ExchangeName),
	%% 如果从mnesia数据库没有发现该交换机exchange的信息，则直接导致异常，使该rabbit_channel进程exit掉，同时发送给客户端rabbit_channel进程异常的原因
	_ = rabbit_exchange:lookup_or_die(ExchangeName),
	%% 如果查找到该交换机信息，则返回给客户端exchange.declare_ok的消息
	return_ok(State, NoWait, #'exchange.declare_ok'{});

%% 交换机exchange的删除(if_unused为true表示无条件的删除，同时将绑定信息删除，如果为false，则如果该exchange还有绑定信息，则删除失败)
handle_method(#'exchange.delete'{exchange  = ExchangeNameBin,
								 if_unused = IfUnused,
								 nowait    = NoWait},
			  _, State = #ch{virtual_host = VHostPath}) ->
	%% 组装exchange的资源结构信息
	ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
	%% 检查当前的exchange名字是否合法，交换机的名字不能为空
	check_not_default_exchange(ExchangeName),
	%% 以amq.开头的exchange交换机不能够删除
	check_exchange_deletion(ExchangeName),
	%% 检查当前用户能否使用该VirtualHost(configure:配置)
	check_configure_permitted(ExchangeName, State),
	%% 删除数据库中的表数据
	case rabbit_exchange:delete(ExchangeName, IfUnused) of
		{error, not_found} ->
			%% 根本没有该名字的exchange交换机
			return_ok(State, NoWait,  #'exchange.delete_ok'{});
		{error, in_use} ->
			%% 当前要删除的exchange还有绑定信息，删除失败
			precondition_failed("~s in use", [rabbit_misc:rs(ExchangeName)]);
		ok ->
			%% 删除成功
			return_ok(State, NoWait,  #'exchange.delete_ok'{})
	end;

%% exchange.bind消息是将交换机和交换机进行绑定
handle_method(#'exchange.bind'{destination = DestinationNameBin,
							   source      = SourceNameBin,
							   routing_key = RoutingKey,
							   nowait      = NoWait,
							   arguments   = Arguments}, _, State) ->
	binding_action(fun rabbit_binding:add/2,
				   SourceNameBin, exchange, DestinationNameBin, RoutingKey,
				   Arguments, #'exchange.bind_ok'{}, NoWait, State);

%% exchange.unbind消息表示解除交换机和交换机的绑定
handle_method(#'exchange.unbind'{destination = DestinationNameBin,
								 source      = SourceNameBin,
								 routing_key = RoutingKey,
								 nowait      = NoWait,
								 arguments   = Arguments}, _, State) ->
	binding_action(fun rabbit_binding:remove/2,
				   SourceNameBin, exchange, DestinationNameBin, RoutingKey,
				   Arguments, #'exchange.unbind_ok'{}, NoWait, State);

%% Note that all declares to these are effectively passive. If it
%% exists it by definition has one consumer.
%% 快速队列amq.rabbitmq.reply-to的创建
handle_method(#'queue.declare'{queue   = <<"amq.rabbitmq.reply-to",
										   _/binary>> = QueueNameBin,
							   nowait  = NoWait}, _,
			  State = #ch{virtual_host = VHost}) ->
	%% 创建队列资源结构
	QueueName = rabbit_misc:r(VHost, queue, QueueNameBin),
	%% 判断快速回复的设置是否存在
	case declare_fast_reply_to(QueueNameBin) of
		%% 如果存在，则向客户端发送快速队列声明成功的消息
		exists    -> return_queue_declare_ok(QueueName, NoWait, 0, 1, State);
		not_found -> rabbit_misc:not_found(QueueName)
	end;

%% 队列的创建
%% exclusive    : 仅创建者可以使用的私有队列，断开后自动删除(当前连接不在时，队列是否自动删除)
%% auto_delete  : 当所有消费客户端连接断开后，是否自动删除队列(没有consumer时，队列是否自动删除)
%% arguments    : 用于拓展参数，比如x-ha-policy用于mirrored queue
handle_method(#'queue.declare'{queue       = QueueNameBin,
							   passive     = false,
							   durable     = DurableDeclare,
							   exclusive   = ExclusiveDeclare,
							   auto_delete = AutoDelete,
							   nowait      = NoWait,
							   arguments   = Args} = Declare,
			  _, State = #ch{virtual_host        = VHostPath,
							 conn_pid            = ConnPid,
							 queue_collector_pid = CollectorPid}) ->
	%% 如果ExclusiveDeclare为true，则该队列的拥有者为rabbit_reader进程
	Owner = case ExclusiveDeclare of
				true  -> ConnPid;
				false -> none
			end,
	%% 得到队列是否是持久化队列(只有ExclusiveDeclare为false，同时DurableDeclare为true，队列才是持久化队列)
	Durable = DurableDeclare andalso not ExclusiveDeclare,
	%% 得到队列的名字
	ActualNameBin = case QueueNameBin of
						%% 如果没有传入队列名字，则生成一个amq.gen开头的临时队列
						<<>>  -> rabbit_guid:binary(rabbit_guid:gen_secure(),
													"amq.gen");
						%% 名字不能以amq.开头，这个开头的队列名字是临时队列
						Other -> check_name('queue', Other)
					end,
	%% 组装成队列的resource的数据结构
	QueueName = rabbit_misc:r(VHostPath, queue, ActualNameBin),
	%% 查看使用者是否能够使用VirtualHost
	check_configure_permitted(QueueName, State),
	%% 从mnesia数据库查找是否已经有该队列
	case rabbit_amqqueue:with(
		   QueueName,
		   fun (Q) -> ok = rabbit_amqqueue:assert_equivalence(
							 Q, Durable, AutoDelete, Args, Owner),
					  %% 拿到当前队列中消息的数量以及消费者的数量
					  maybe_stat(NoWait, Q)
		   end) of
		%% 当前队列存在，则将得到的队列中的消息数量和消费者的个数通过queue.declare_ok消息返回给客户端
		{ok, MessageCount, ConsumerCount} ->
			%% 向客户端返回创建队列成功的消息
			return_queue_declare_ok(QueueName, NoWait, MessageCount,
									ConsumerCount, State);
		%% 没有找到该队列
		{error, not_found} ->
			%% x-dead-letter-exchange参数将死的信息重新发布到该exchange交换机上(死信息包括：1.消息被拒绝（basic.reject or basic.nack）；2.消息TTL过期；3.队列达到最大长度)
			DlxKey = <<"x-dead-letter-exchange">>,
			case rabbit_misc:r_arg(VHostPath, exchange, Args, DlxKey) of
				undefined ->
					ok;
				{error, {invalid_type, Type}} ->
					precondition_failed(
					  "invalid type '~s' for arg '~s' in ~s",
					  [Type, DlxKey, rabbit_misc:rs(QueueName)]);
				%% 如果参数中配置有该参数
				DLX ->
					%% 检查该用户对队列名字读的权限
					check_read_permitted(QueueName, State),
					%% 检查用户对交换机资源写的权限
					check_write_permitted(DLX, State),
					ok
			end,
			%% 创建一个新的队列，该操作是call创建队列，等待队列返回的{new, Q}同步消息
			case rabbit_amqqueue:declare(QueueName, Durable, AutoDelete,
										 Args, Owner) of
				%% 在rabbit_amqqueue_sup_sup监督树下创建队列成功(先创建rabbit_amqqueue_sup监督进程，然后在该监督树下启动rabbit_amqqueue进程)
				{new, #amqqueue{pid = QPid}} ->
					%% We need to notify the reader within the channel
					%% process so that we can be sure there are no
					%% outstanding exclusive queues being declared as
					%% the connection shuts down.
					ok = case Owner of
							 none -> ok;
							 _    -> rabbit_queue_collector:register(
									   CollectorPid, QPid)
						 end,
					%% 向客户端返回创建队列成功的消息
					return_queue_declare_ok(QueueName, NoWait, 0, 0, State);
				{existing, _Q} ->
					%% must have been created between the stat and the
					%% declare. Loop around again.
					handle_method(Declare, none, State);
				{absent, Q, Reason} ->
					rabbit_misc:absent(Q, Reason);
				{owner_died, _Q} ->
					%% Presumably our own days are numbered since the
					%% connection has died. Pretend the queue exists though,
					%% just so nothing fails.
					return_queue_declare_ok(QueueName, NoWait, 0, 0, State)
			end;
		{error, {absent, Q, Reason}} ->
			rabbit_misc:absent(Q, Reason)
	end;

%% 客户端将passive设置为true，表示来请求RabbitMQ系统，该队列是否存在
handle_method(#'queue.declare'{queue   = QueueNameBin,
							   passive = true,
							   nowait  = NoWait},
			  _, State = #ch{virtual_host = VHostPath,
							 conn_pid     = ConnPid}) ->
	%% 组装队列的资源结构
	QueueName = rabbit_misc:r(VHostPath, queue, QueueNameBin),
	%% 如果Name的队列不存在，则会让当前进程终止掉，同时将异常消息发送给客户端
	{{ok, MessageCount, ConsumerCount}, #amqqueue{} = Q} =
		rabbit_amqqueue:with_or_die(
		  QueueName, fun (Q) -> {maybe_stat(NoWait, Q), Q} end),
	%% 执行到此处已经查找到该队列信息，检查exclusive_owner字段的正确性
	ok = rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
	%% 向客户端返回创建队列成功的消息
	return_queue_declare_ok(QueueName, NoWait, MessageCount, ConsumerCount,
							State);

%% 处理队列删除的消息(IfEmpty为true表示只有队列为空的时候才能够进行删除队列的操作；IfUnused为true表示队列中消费者为0的时候才能够进行删除队列的操作)
handle_method(#'queue.delete'{queue     = QueueNameBin,
							  if_unused = IfUnused,
							  if_empty  = IfEmpty,
							  nowait    = NoWait},
			  _, State = #ch{conn_pid = ConnPid}) ->
	%% 组装队列的资源结构
	QueueName = qbin_to_resource(QueueNameBin, State),
	%% 检查资源配置的权限
	check_configure_permitted(QueueName, State),
	case rabbit_amqqueue:with(
		   QueueName,
		   fun (Q) ->
					%% 检查exclusive_owner字段的正确性
					rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
					%% 最终的执行删除队列的操作
					rabbit_amqqueue:delete(Q, IfUnused, IfEmpty)
		   end,
		   fun (not_found)            -> {ok, 0};
			  %% 如果查找的队列进程已经崩溃，则将崩溃的消息队列删除掉
			  ({absent, Q, crashed}) -> rabbit_amqqueue:delete_crashed(Q),
										{ok, 0};
			  ({absent, Q, Reason})  -> rabbit_misc:absent(Q, Reason)
		   end) of
		%% 当前队列上面有消费者正在使用中，不能删除掉
		{error, in_use} ->
			precondition_failed("~s in use", [rabbit_misc:rs(QueueName)]);
		%% 当前队列中还有消息存在，不能删除
		{error, not_empty} ->
			precondition_failed("~s not empty", [rabbit_misc:rs(QueueName)]);
		{ok, PurgedMessageCount} ->
			return_ok(State, NoWait,
					  #'queue.delete_ok'{message_count = PurgedMessageCount})
	end;

%% 处理将交换机exchange和队列进行绑定的消息
handle_method(#'queue.bind'{queue       = QueueNameBin,
							exchange    = ExchangeNameBin,
							routing_key = RoutingKey,
							nowait      = NoWait,
							arguments   = Arguments}, _, State) ->
	binding_action(fun rabbit_binding:add/2,
				   ExchangeNameBin, queue, QueueNameBin, RoutingKey, Arguments,
				   #'queue.bind_ok'{}, NoWait, State);

%% 处理将交换机exchange和队列解除绑定的消息
handle_method(#'queue.unbind'{queue       = QueueNameBin,
							  exchange    = ExchangeNameBin,
							  routing_key = RoutingKey,
							  arguments   = Arguments}, _, State) ->
	binding_action(fun rabbit_binding:remove/2,
				   ExchangeNameBin, queue, QueueNameBin, RoutingKey, Arguments,
				   #'queue.unbind_ok'{}, false, State);

%% 处理清除队列中消息的消息
handle_method(#'queue.purge'{queue = QueueNameBin, nowait = NoWait},
			  _, State = #ch{conn_pid = ConnPid}) ->
	%% 根据队列名字得到队列资源结构
	QueueName = qbin_to_resource(QueueNameBin, State),
	%% 检查资源对该用户读的合法性
	check_read_permitted(QueueName, State),
	{ok, PurgedMessageCount} = rabbit_amqqueue:with_exclusive_access_or_die(
								 QueueName, ConnPid,
								 %% 实际清除队列中的消息操作
								 fun (Q) -> rabbit_amqqueue:purge(Q) end),
	%% 向客户端发送清除成功的消息queue.purge_ok，将已经清除的队列中的消息数量返回
	return_ok(State, NoWait,
			  #'queue.purge_ok'{message_count = PurgedMessageCount});

%% 无法从confirm模式调整到tx事务模式
handle_method(#'tx.select'{}, _, #ch{confirm_enabled = true}) ->
	precondition_failed("cannot switch from confirm to tx mode");

%% 如果事务模式字段为none，则给事务字段tx初始化状态
handle_method(#'tx.select'{}, _, State = #ch{tx = none}) ->
	{reply, #'tx.select_ok'{}, State#ch{tx = new_tx()}};

%% 当前rabbit_channel进程已经处在事务模式下，则通知客户端设置成功
handle_method(#'tx.select'{}, _, State) ->
	{reply, #'tx.select_ok'{}, State};

%% 如果当前事务字段为none，则提交事务报错
handle_method(#'tx.commit'{}, _, #ch{tx = none}) ->
	%% 通知客户端当前rabbit_channel进程没有处在事务下
	precondition_failed("channel is not transactional");

%% 处理客户端发送过来的tx.commit事务提交的消息
handle_method(#'tx.commit'{}, _, State = #ch{tx      = {Msgs, Acks},
											 limiter = Limiter}) ->
	%% 将tx字段中的消息队列中的要发送给各个队列的操作，进行一次性操作，将要发送的消息发送到各个队列中去
	State1 = rabbit_misc:queue_fold(fun deliver_to_queues/2, State, Msgs),
	Rev = fun (X) -> lists:reverse(lists:sort(X)) end,
	%% 将已经ack的消息进行ack操作，对于拒绝的消息进行拒绝操作
	lists:foreach(fun ({ack,     A}) -> ack(Rev(A), State1);
					 ({Requeue, A}) -> reject(Requeue, Rev(A), Limiter)
				  end, lists:reverse(Acks)),
	%% 将当前rabbit_channel进程tx事务字段置为committing，表示正在进行事务提交操作，则现在rabbit_channel进程不能够处理其他消息，
	%% 如果有其他消息到来，会通知客户端自己正在事务提交状态(判断事务提交完成的条件是unconfirmed字段为空，即所有的消息已经得到confirm)
	{noreply, maybe_complete_tx(State1#ch{tx = committing})};

%% 如果当前事务字段为none，则收到客户端tx.rollback事务回滚的消息报错
handle_method(#'tx.rollback'{}, _, #ch{tx = none}) ->
	%% 通知客户端当前rabbit_channel进程没有处在事务下
	precondition_failed("channel is not transactional");

%% 处理客户端发送过来的事务回滚消息tx.rollback，将要发送的消息直接丢弃掉，然后将等待ack的消息直接从事务结构中拿到等待ack的队列unacked_message_q中
handle_method(#'tx.rollback'{}, _, State = #ch{unacked_message_q = UAMQ,
											   tx = {_Msgs, Acks}}) ->
	AcksL = lists:append(lists:reverse([lists:reverse(L) || {_, L} <- Acks])),
	UAMQ1 = queue:from_list(lists:usort(AcksL ++ queue:to_list(UAMQ))),
	%% 将当前事务中的已经ack的消息放入到没有ack的队列中
	{reply, #'tx.rollback_ok'{}, State#ch{unacked_message_q = UAMQ1,
										  tx                = new_tx()}};

%% 处在事务模式下，不能将模式转化为confirm模式
handle_method(#'confirm.select'{}, _, #ch{tx = {_, _}}) ->
	precondition_failed("cannot switch from tx to confirm mode");

%% 当前没有处在事务模式下，则收到客户端confirm.select，则将当前rabbit_channel进程设置为confirm模式
handle_method(#'confirm.select'{nowait = NoWait}, _, State) ->
	return_ok(State#ch{confirm_enabled = true},
					  NoWait, #'confirm.select_ok'{});

%% 当前版本对channel.flow消息不支持
handle_method(#'channel.flow'{active = true}, _, State) ->
	{reply, #'channel.flow_ok'{active = true}, State};

%% 当前版本对channel.flow消息不支持
handle_method(#'channel.flow'{active = false}, _, _State) ->
	rabbit_misc:protocol_error(not_implemented, "active=false", []);

handle_method(#'basic.credit'{consumer_tag = CTag,
							  credit       = Credit,
							  drain        = Drain},
			  _, State = #ch{consumer_mapping = Consumers}) ->
	case dict:find(CTag, Consumers) of
		{ok, {Q, _CParams}} -> ok = rabbit_amqqueue:credit(
									  Q, self(), CTag, Credit, Drain),
							   {noreply, State};
		error               -> precondition_failed(
								 "unknown consumer tag '~s'", [CTag])
	end;

%% 如果是其他Method，则通知客户端是还没有完成的Method
handle_method(_MethodRecord, _Content, _State) ->
	rabbit_misc:protocol_error(
	  command_invalid, "unimplemented method", []).

%%----------------------------------------------------------------------------

%% We get the queue process to send the consume_ok on our behalf. This
%% is for symmetry with basic.cancel - see the comment in that method
%% for why.
%% 实际处理队列中增加消费者操作
basic_consume(QueueName, NoAck, ConsumerPrefetch, ActualConsumerTag,
			  ExclusiveConsume, Args, NoWait,
			  State = #ch{conn_pid          = ConnPid,
						  limiter           = Limiter,
						  consumer_mapping  = ConsumerMapping}) ->
	%% 找到对应的消息队列，将消费者添加到队列中
	case rabbit_amqqueue:with_exclusive_access_or_die(
		   QueueName, ConnPid,
		   fun (Q) ->
					%% call到队列进程添加消费者
					{rabbit_amqqueue:basic_consume(
					   Q, NoAck, self(),
					   %% rabbit_channel进程中得到rabbit_limiter进程的Pid接口
					   rabbit_limiter:pid(Limiter),
					   rabbit_limiter:is_active(Limiter),
					   ConsumerPrefetch, ActualConsumerTag,
					   ExclusiveConsume, Args,
					   ok_msg(NoWait, #'basic.consume_ok'{
														  consumer_tag = ActualConsumerTag})),
					 Q}
		   end) of
		{ok, Q = #amqqueue{pid = QPid, name = QName}} ->
			%% 将消费者的标识同它对应的队列已经其它信息放入到字典consumer_mapping字段中
			CM1 = dict:store(
					ActualConsumerTag,
					{Q, {NoAck, ConsumerPrefetch, ExclusiveConsume, Args}},
					ConsumerMapping),
			%% 监视正在传递消息的队列进程
			State1 = monitor_delivering_queue(
					   NoAck, QPid, QName,
					   State#ch{consumer_mapping = CM1}),
			{ok, case NoWait of
					 %% NoWait字段为true，则立刻监听消费者对应的消息队列，立刻返回给客户端basic.consume_ok消息，表示成功成为指定队列进程的消费者
					 true  -> %% 监视消费者对应的队列进程，同时更新queue_consumers字段
						 	  consumer_monitor(ActualConsumerTag, State1);
					 %% NoWait字段为false，则表示要队列进程做完相关的处理后，才向客户端发送basic.consume_ok消息
					 false -> State1
				 end};
		{{error, exclusive_consume_unavailable} = E, _Q} ->
			E
	end.


%% 如果客户端等待消息的返回，则将当前队列的状态返回给客户端(拿到当前队列中消息的数量以及消费者的数量)
maybe_stat(false, Q) -> rabbit_amqqueue:stat(Q);

%% 如果客户端不等待消息的返回，则不用拿到队列的状态
maybe_stat(true, _Q) -> {ok, 0, 0}.


%% 监视消费者对应的队列进程，同时更新queue_consumers字段
consumer_monitor(ConsumerTag,
				 State = #ch{consumer_mapping = ConsumerMapping,
							 queue_monitors   = QMons,
							 queue_consumers  = QCons}) ->
	%% 根据消费者标识得到对应的队列进程Pid
	{#amqqueue{pid = QPid}, _CParams} =
		dict:fetch(ConsumerTag, ConsumerMapping),
	%% 更新队列进程中对应的消费者标识(一个队列进程对应多个消费者标识，因此采用字典dict结构，字典中的key-value中，key是队列进程Pid，value是gb_sets结构的消费者的标识列表)
	QCons1 = dict:update(QPid, fun (CTags) ->
										gb_sets:insert(ConsumerTag, CTags)
						 end,
						 gb_sets:singleton(ConsumerTag), QCons),
	%% 更新监控进程的字段queue_monitors，以及queue_consumers字段
	State#ch{queue_monitors  = pmon:monitor(QPid, QMons),
			 queue_consumers = QCons1}.


%% 监视正在传递消息的队列进程
monitor_delivering_queue(NoAck, QPid, QName,
						 State = #ch{queue_names       = QNames,
									 queue_monitors    = QMons,
									 delivering_queues = DQ}) ->
	State#ch{queue_names       = dict:store(QPid, QName, QNames),
			 queue_monitors    = pmon:monitor(QPid, QMons),
			 delivering_queues = case NoAck of
									 true  -> DQ;
									 %% 如果消息需要消费者的ack操作，则将该队列进程存储在delivering_queues字段中
									 false -> sets:add_element(QPid, DQ)
								 end}.


%% 队列进程中断后，处理还没有confirm的消息和mandatory相关的消息
handle_publishing_queue_down(QPid, Reason, State = #ch{unconfirmed = UC,
													   mandatory   = Mand}) ->
	%%% 根据队列进程从mandatory结构中拿出已经没有能够接收该消息的队列，则将该消息通过basic.return消息返回给客户端(只有消息里mandatory字段设置为true才会有该操作)
	{MMsgs, Mand1} = dtree:take(QPid, Mand),
	%% 将没有队列处理的消息通过basic.return消息发送给客户端
	[basic_return(Msg, State, no_route) || {_, Msg} <- MMsgs],
	%% 更新最新的mandatory数据
	State1 = State#ch{mandatory = Mand1},
	case rabbit_misc:is_abnormal_exit(Reason) of
		%% 消息队列是异常中断，
		true  -> {MXs, UC1} = dtree:take_all(QPid, UC),
				 %% 将该队列进程里面还没有confirm的消息通过basic.nack消息告诉客户端
				 send_nacks(MXs, State1#ch{unconfirmed = UC1});
		%% 消息队列是正常中断，将该队列进程Pid从unconfirmed结构中删除，如果有消息已经全部得到confirm，则通知客户端消息已经得到confirm
		false -> {MXs, UC1} = dtree:take(QPid, UC),
				 %% 将已经confirm的消息放入confirmed字段中
				 record_confirms(MXs, State1#ch{unconfirmed = UC1})
	end.


%% 消息队列中断后，将消息队列的所有消费者删除掉
handle_consuming_queue_down(QPid, State = #ch{queue_consumers  = QCons,
											  queue_names      = QNames}) ->
	%% 拿到该队列进程对应的所有消费者
	ConsumerTags = case dict:find(QPid, QCons) of
					   error       -> gb_sets:new();
					   {ok, CTags} -> CTags
				   end,
	gb_sets:fold(
	  fun (CTag, StateN = #ch{consumer_mapping = CMap}) ->
			   %% 拿到该队列进程的名字
			   QName = dict:fetch(QPid, QNames),
			   %% 队列进程中断导致消费者的删除，根据消费者参数，判断消费者是否需要删除掉
			   case queue_down_consumer_action(CTag, CMap) of
				   remove ->
					   %% 删除消费者
					   cancel_consumer(CTag, QName, StateN);
				   {recover, {NoAck, ConsumerPrefetch, Exclusive, Args}} ->
					   case catch basic_consume( %% [0]
							  QName, NoAck, ConsumerPrefetch, CTag,
							  Exclusive, Args, true, StateN) of
						   {ok, StateN1} -> StateN1;
						   %% 如果还是向队列增加该消费者失败，则立刻将该消费者删除掉
						   _             -> cancel_consumer(CTag, QName, StateN)
					   end
			   end
	  end, State#ch{queue_consumers = dict:erase(QPid, QCons)}, ConsumerTags).

%% [0] There is a slight danger here that if a queue is deleted and
%% then recreated again the reconsume will succeed even though it was
%% not an HA failover. But the likelihood is not great and most users
%% are unlikely to care.
%% 删除消费者
cancel_consumer(CTag, QName, State = #ch{capabilities     = Capabilities,
										 consumer_mapping = CMap}) ->
	%% 如果客户端有consumer_cancel_notify这个特性，则将删除的消费者通过basic.cancel消息通知客户端
	case rabbit_misc:table_lookup(
		   Capabilities, <<"consumer_cancel_notify">>) of
		{bool, true} -> ok = send(#'basic.cancel'{consumer_tag = CTag,
												  nowait       = true}, State);
		_            -> ok
	end,
	%% 向rabbit_event事件中心发布消费者删除的事件
	rabbit_event:notify(consumer_deleted, [{consumer_tag, CTag},
										   {channel,      self()},
										   {queue,        QName}]),
	%% 将该消费者从消费者信息字典中删除
	State#ch{consumer_mapping = dict:erase(CTag, CMap)}.


%% 队列进程中断导致消费者的删除，根据消费者参数，判断消费者是否需要删除掉
queue_down_consumer_action(CTag, CMap) ->
	{_, {_, _, _, Args} = ConsumeSpec} = dict:fetch(CTag, CMap),
	case rabbit_misc:table_lookup(Args, <<"x-cancel-on-ha-failover">>) of
		{bool, true} -> remove;
		_            -> {recover, ConsumeSpec}
	end.


%% 队列中断，处理delivering_queues字段中数据，将队列进程Pid从delivering_queues字段中删除掉
handle_delivering_queue_down(QPid, State = #ch{delivering_queues = DQ}) ->
	State#ch{delivering_queues = sets:del_element(QPid, DQ)}.


%% 绑定操作相关处理函数
binding_action(Fun, ExchangeNameBin, DestinationType, DestinationNameBin,
			   RoutingKey, Arguments, ReturnMethod, NoWait,
			   State = #ch{virtual_host = VHostPath,
						   conn_pid     = ConnPid }) ->
	%% 组装目标的资源数据结构
	DestinationName = name_to_resource(DestinationType, DestinationNameBin, State),
	%% 检查对该DestinationName资源是否有写的权限
	check_write_permitted(DestinationName, State),
	%% 组装exchange的资源数据结构
	ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
	%% 检查资源的合法性，名字不能为空
	[check_not_default_exchange(N) || N <- [DestinationName, ExchangeName]],
	%% 检查exchange资源对该用户读的合法性
	check_read_permitted(ExchangeName, State),
	%% 去rabbit_binding模块去添加新的绑定
	case Fun(#binding{source      = ExchangeName,
					  destination = DestinationName,
					  key         = RoutingKey,
					  args        = Arguments},
			 %% 该函数检查队列的排他性(如果一个队列被声明为排他队列，该队列仅对首次声明它的连接可见)
			 fun (_X, Q = #amqqueue{}) ->
					  try rabbit_amqqueue:check_exclusive_access(Q, ConnPid)
					  catch exit:Reason -> {error, Reason}
					  end;
				(_X, #exchange{}) ->
					 ok
			 end) of
		{error, {resources_missing, [{not_found, Name} | _]}} ->
			rabbit_misc:not_found(Name);
		{error, {resources_missing, [{absent, Q, Reason} | _]}} ->
			rabbit_misc:absent(Q, Reason);
		{error, binding_not_found} ->
			rabbit_misc:protocol_error(
			  not_found, "no binding ~s between ~s and ~s",
			  [RoutingKey, rabbit_misc:rs(ExchangeName),
			   rabbit_misc:rs(DestinationName)]);
		{error, {binding_invalid, Fmt, Args}} ->
			rabbit_misc:protocol_error(precondition_failed, Fmt, Args);
		{error, #amqp_error{} = Error} ->
			rabbit_misc:protocol_error(Error);
		%% 创建成功后向客户端发送queue.bind_ok或者queue.unbind_ok以及exchange.bind_ok,exchange_unbind_ok消息
		ok -> return_ok(State, NoWait, ReturnMethod)
	end.


%% mandatory为true，同时路由不到消息队列，则将消息原样发送给生成者
basic_return(#basic_message{exchange_name = ExchangeName,
							routing_keys  = [RoutingKey | _CcRoutes],
							content       = Content},
			 State = #ch{protocol = Protocol, writer_pid = WriterPid},
			 Reason) ->
	%% 更新ExchangeName对应return_unroutable字段对应的数量(即更新ExchangeName当前无法路由到消息队列中的消息数量)
	?INCR_STATS([{exchange_stats, ExchangeName, 1}], return_unroutable, State),
	{_Close, ReplyCode, ReplyText} = Protocol:lookup_amqp_exception(Reason),
	%% 将无法路由的消息通过rabbit_writer进程发送给生产者
	ok = rabbit_writer:send_command(
		   WriterPid,
		   #'basic.return'{reply_code  = ReplyCode,
						   reply_text  = ReplyText,
						   exchange    = ExchangeName#resource.name,
						   routing_key = RoutingKey},
		   Content).


%% 消费者拒绝接受消息的处理
reject(DeliveryTag, Requeue, Multiple,
	   State = #ch{unacked_message_q = UAMQ, tx = Tx}) ->
	%% 根据DeliveryTag和Multiple字段得到需要拒绝的消息列表
	{Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
	%% 将剩余的等待消费者ack放入到unacked_message_q字段
	State1 = State#ch{unacked_message_q = Remaining},
	{noreply, case Tx of
				  %% 如果当前rabbit_channel进程没有处在事务中，
				  %% 将得到的拒绝的消息遍历将这些消息拒绝ack，同时通知当前rabbit_channel进程对应的rabbit_limiter进程，减少rabbit_limiter进程中等待ack的消息树立
				  none         -> reject(Requeue, Acked, State1#ch.limiter),
								  State1;
				  %% 如果存在事务，则将消息添加到事务字段的第二个字段中
				  {Msgs, Acks} -> Acks1 = ack_cons(Requeue, Acked, Acks),
								  State1#ch{tx = {Msgs, Acks1}}
			  end}.

%% NB: Acked is in youngest-first order
%% 将得到的拒绝的消息遍历将这些消息拒绝ack，同时通知当前rabbit_channel进程对应的rabbit_limiter进程，减少rabbit_limiter进程中等待ack的消息树立
reject(Requeue, Acked, Limiter) ->
	%% 将得到的拒绝的消息遍历将这些消息拒绝ack
	foreach_per_queue(
	  fun (QPid, MsgIds) ->
			   rabbit_amqqueue:reject(QPid, Requeue, MsgIds, self())
	  end, Acked),
	%% 通知当前rabbit_channel进程对应的rabbit_limiter进程，减少rabbit_limiter进程中等待ack的消息树立
	ok = notify_limiter(Limiter, Acked).


%% 记录消息的发送，如果被发送的消息需要等待ack，则需要将该消息存入等待ack的队列里
record_sent(ConsumerTag, AckRequired,
			Msg = {QName, QPid, MsgId, Redelivered, _Message},
			State = #ch{unacked_message_q = UAMQ,
						next_tag          = DeliveryTag,
						trace_state       = TraceState,
						user              = #user{username = Username},
						conn_name         = ConnName,
						channel           = ChannelNum}) ->
	%% rabbit_channel进程中记录信息到进程字典中，用来信息收集用
	?INCR_STATS([{queue_stats, QName, 1}], case {ConsumerTag, AckRequired} of
											   {none,  true} -> get;
											   {none, false} -> get_no_ack;
											   {_   ,  true} -> deliver;
											   {_   , false} -> deliver_no_ack
										   end, State),
	case Redelivered of
		true  -> ?INCR_STATS([{queue_stats, QName, 1}], redeliver, State);
		false -> ok
	end,
	rabbit_trace:tap_out(Msg, ConnName, ChannelNum, Username, TraceState),
	UAMQ1 = case AckRequired of
				%% 将要等待消费者ack的消息ID记录在unacked_message_q字段中
				true  -> queue:in({DeliveryTag, ConsumerTag, {QPid, MsgId}},
								  UAMQ);
				false -> UAMQ
			end,
	%% 将next_tag字段加一，同时更新最新的unacked_message_q(等待消费者ack操作的队列字段)
	State#ch{unacked_message_q = UAMQ1, next_tag = DeliveryTag + 1}.

%% NB: returns acks in youngest-first order
%% 如果DeliveryTag等于0表示将所有等待ack的消息全部进行ack操作
collect_acks(Q, 0, true) ->
	{lists:reverse(queue:to_list(Q)), queue:new()};

collect_acks(Q, DeliveryTag, Multiple) ->
	collect_acks([], [], Q, DeliveryTag, Multiple).


%% 根据客户端传入的参数得到要进行ack的消息列表
collect_acks(ToAcc, PrefixAcc, Q, DeliveryTag, Multiple) ->
	case queue:out(Q) of
		{{value, UnackedMsg = {CurrentDeliveryTag, _ConsumerTag, _Msg}},
		 QTail} ->
			if CurrentDeliveryTag == DeliveryTag ->
				   {[UnackedMsg | ToAcc],
					case PrefixAcc of
						[] -> QTail;
						%% 如果Multiple不为true，则同时PrefixAcc有数据，则将该不要的数据插入队列中，只需要对DeliveryTag的消息进行ack操作
						_  -> queue:join(
								queue:from_list(lists:reverse(PrefixAcc)),
								QTail)
					end};
			   %% 如果Multiple为true，则表示小于DeliveryTag的都要进行ack操作
			   Multiple ->
				   collect_acks([UnackedMsg | ToAcc], PrefixAcc,
								QTail, DeliveryTag, Multiple);
			   true ->
				   collect_acks(ToAcc, [UnackedMsg | PrefixAcc],
								QTail, DeliveryTag, Multiple)
			end;
		{empty, _} ->
			precondition_failed("unknown delivery tag ~w", [DeliveryTag])
	end.

%% NB: Acked is in youngest-first order
ack(Acked, State = #ch{queue_names = QNames}) ->
	foreach_per_queue(
	  fun (QPid, MsgIds) ->
			   %% 通知队列进程消息ID列表消费者已经将他们消费掉
			   ok = rabbit_amqqueue:ack(QPid, MsgIds, self()),
			   %% 更新对应队列已经ack的消息数量
			   ?INCR_STATS(case dict:find(QPid, QNames) of
							   {ok, QName} -> Count = length(MsgIds),
											  [{queue_stats, QName, Count}];
							   error       -> []
						   end, ack, State)
	  end, Acked),
	%% 通知当前rabbit_channel进程对应的rabbit_limiter进程有消费者标识的ack操作的数量，用于让rabbit_limiter进程从锁住状态变为解锁状态
	ok = notify_limiter(State#ch.limiter, Acked).

%% {Msgs, Acks}
%%
%% Msgs is a queue.
%%
%% Acks looks s.t. like this:
%% [{false,[5,4]},{true,[3]},{ack,[2,1]}, ...]
%%
%% Each element is a pair consisting of a tag and a list of
%% ack'ed/reject'ed msg ids. The tag is one of 'ack' (to ack), 'true'
%% (reject w requeue), 'false' (reject w/o requeue). The msg ids, as
%% well as the list overall, are in "most-recent (generally youngest)
%% ack first" order.
%% 事务模式字段的初始化结构
new_tx() -> {queue:new(), []}.


%% 通知该channel进程下的所有队列rabbit_channel进程停止运行
notify_queues(State = #ch{state = closing}) ->
	{ok, State};

notify_queues(State = #ch{consumer_mapping  = Consumers,
						  delivering_queues = DQ }) ->
	QPids = sets:to_list(
			  sets:union(sets:from_list(consumer_queues(Consumers)), DQ)),
	{rabbit_amqqueue:notify_down_all(QPids, self()), State#ch{state = closing}}.


foreach_per_queue(_F, []) ->
	ok;

foreach_per_queue(F, [{_DTag, _CTag, {QPid, MsgId}}]) -> %% common case
	F(QPid, [MsgId]);

%% NB: UAL should be in youngest-first order; the tree values will
%% then be in oldest-first order
foreach_per_queue(F, UAL) ->
	%% 根据队列进程Pid区分出每个队列对应的消息ID列表
	T = lists:foldl(fun ({_DTag, _CTag, {QPid, MsgId}}, T) ->
							 rabbit_misc:gb_trees_cons(QPid, MsgId, T)
					end, gb_trees:empty(), UAL),
	rabbit_misc:gb_trees_foreach(F, T).


%% 收集所有的消费者对应的消息队列的Pid列表
consumer_queues(Consumers) ->
	lists:usort([QPid || {_Key, {#amqqueue{pid = QPid}, _CParams}}
							 <- dict:to_list(Consumers)]).

%% tell the limiter about the number of acks that have been received
%% for messages delivered to subscribed consumers, but not acks for
%% messages sent in a response to a basic.get (identified by their
%% 'none' consumer tag)
%% 通知当前rabbit_channel进程对应的rabbit_limiter进程，减少rabbit_limiter进程中等待ack的消息数量
notify_limiter(Limiter, Acked) ->
	%% optimisation: avoid the potentially expensive 'foldl' in the
	%% common case.
	case rabbit_limiter:is_active(Limiter) of
		false -> ok;
		true  -> case lists:foldl(fun ({_, none, _}, Acc) -> Acc;
									 %% 有消费者标识的才加一
									 ({_,    _, _}, Acc) -> Acc + 1
								  end, 0, Acked) of
					 0     -> ok;
					 %% 通知rabbit_limiter进程已经ack的消息数量
					 Count -> rabbit_limiter:ack(Limiter, Count)
				 end
	end.


%% 交付给路由到的队列进程，将message消息交付到各个队列中(此处是没有根据绑定信息路由到对应的消息队列)
deliver_to_queues({#delivery{message   = #basic_message{exchange_name = XName},
							 confirm   = false,
							 mandatory = false},
				   []}, State) -> %% optimisation(优化)
	%% 没有找到对应的消息队列，则将消息丢弃，同时将新当前rabbit_channel进程中向交换机XName传递次数加一
	?INCR_STATS([{exchange_stats, XName, 1}], publish, State),
	State;

%% 交付给路由到的队列进程，将message消息交付到各个队列中
deliver_to_queues({Delivery = #delivery{message    = Message = #basic_message{
																			  exchange_name = XName},
										mandatory  = Mandatory,
										confirm    = Confirm,
										msg_seq_no = MsgSeqNo},
				   DelQNames}, State = #ch{queue_names    = QNames,
										   queue_monitors = QMons}) ->
	Qs = rabbit_amqqueue:lookup(DelQNames),
	%% 向已经路由出来的消息队列发送消息
	DeliveredQPids = rabbit_amqqueue:deliver(Qs, Delivery),
	%% The pmon:monitor_all/2 monitors all queues to which we
	%% delivered. But we want to monitor even queues we didn't deliver
	%% to, since we need their 'DOWN' messages to clean
	%% queue_names. So we also need to monitor each QPid from
	%% queues. But that only gets the masters (which is fine for
	%% cleaning queue_names), so we need the union of both.
	%%
	%% ...and we need to add even non-delivered queues to queue_names
	%% since alternative(替代) algorithms(算法) to update queue_names less
	%% frequently would in fact be more expensive in the common case.
	%% 当前channel进程监视路由到的消息队列进程
	{QNames1, QMons1} =
		lists:foldl(fun (#amqqueue{pid = QPid, name = QName},
						 {QNames0, QMons0}) ->
							 {case dict:is_key(QPid, QNames0) of
								  true  -> QNames0;
								  false -> dict:store(QPid, QName, QNames0)
							  end, pmon:monitor(QPid, QMons0)}
					end, {QNames, pmon:monitor_all(DeliveredQPids, QMons)}, Qs),
	%% 更新queue_names字段，queue_monitors字段
	State1 = State#ch{queue_names    = QNames1,
					  queue_monitors = QMons1},
	%% NB: the order here is important since basic.returns must be
	%% sent before confirms.
	%% 将mandatory为true，且发送到的队列Pid不为空，则将数据存入到mandatory(dtree数据结构)字段中
	State2 = process_routing_mandatory(Mandatory, DeliveredQPids, MsgSeqNo,
									   Message, State1),
	%% 如果confirm为true，同时发送到的队列Pid不为空，则将数据存入到unconfirmed(dtree数据结构)字段中
	State3 = process_routing_confirm(  Confirm,   DeliveredQPids, MsgSeqNo,
									   XName,   State2),
	%% 将新当前rabbit_channel进程中向交换机XName传递次数加一，同时更新queue_exchange_stats字段
	?INCR_STATS([{exchange_stats, XName, 1} |
					 [{queue_exchange_stats, {QName, XName}, 1} ||
					  QPid        <- DeliveredQPids,
					  {ok, QName} <- [dict:find(QPid, QNames1)]]],
				publish, State3),
	State3.


%% 将mandatory为true，且发送到的队列Pid不为空，则将数据存入到mandatory(dtree数据结构)字段中
process_routing_mandatory(false,     _, _MsgSeqNo, _Msg, State) ->
	State;

process_routing_mandatory(true,     [], _MsgSeqNo,  Msg, State) ->
	%% mandatory为true，同时路由不到消息队列，则将消息原样发送给生成者
	ok = basic_return(Msg, State, no_route),
	State;

process_routing_mandatory(true,  QPids,  MsgSeqNo,  Msg, State) ->
	State#ch{mandatory = dtree:insert(MsgSeqNo, QPids, Msg,
									  State#ch.mandatory)}.


%% 如果confirm为true，同时发送到的队列Pid不为空，则将数据存入到unconfirmed(dtree数据结构)字段中
process_routing_confirm(false,    _, _MsgSeqNo, _XName, State) ->
	State;

process_routing_confirm(true,    [],  MsgSeqNo,  XName, State) ->
	record_confirms([{MsgSeqNo, XName}], State);

process_routing_confirm(true, QPids,  MsgSeqNo,  XName, State) ->
	State#ch{unconfirmed = dtree:insert(MsgSeqNo, QPids, XName,
										State#ch.unconfirmed)}.


%% 消息队列进程异常中断，则将该队列还没有confirm的消息告诉客户端
send_nacks([], State) ->
	State;

send_nacks(_MXs, State = #ch{state = closing,
							 tx    = none}) -> %% optimisation
	State;

send_nacks(MXs, State = #ch{tx = none}) ->
	coalesce_and_send([MsgSeqNo || {MsgSeqNo, _} <- MXs],
					  fun(MsgSeqNo, Multiple) ->
							  #'basic.nack'{delivery_tag = MsgSeqNo,
											multiple     = Multiple}
					  end, State);

send_nacks(_MXs, State = #ch{state = closing}) -> %% optimisation
	State#ch{tx = failed};

send_nacks(_, State) ->
	maybe_complete_tx(State#ch{tx = failed}).


%% 向客户端发送已经得到confirm的消息(即消息已经存储到消息队列中)
send_confirms(State = #ch{tx = none, confirmed = []}) ->
	State;

send_confirms(State = #ch{tx = none, confirmed = C}) ->
	case rabbit_node_monitor:pause_partition_guard() of
		ok      -> MsgSeqNos =
					   lists:foldl(
						 fun ({MsgSeqNo, XName}, MSNs) ->
								  %% 统计交换机exchange XName得到confirm的数量
								  ?INCR_STATS([{exchange_stats, XName, 1}],
											  confirm, State),
								  [MsgSeqNo | MSNs]
						 end, [], lists:append(C)),
				   send_confirms(MsgSeqNos, State#ch{confirmed = []});
		pausing -> State
	end;

send_confirms(State) ->
	case rabbit_node_monitor:pause_partition_guard() of
		ok      -> maybe_complete_tx(State);
		pausing -> State
	end.


%% 实际的组装消息进行confirm，通知客户端消息已经存储在消息队列中
send_confirms([], State) ->
	State;

send_confirms(_Cs, State = #ch{state = closing}) -> %% optimisation
	State;

%% 只有一个消息进行confirm的操作
send_confirms([MsgSeqNo], State) ->
	ok = send(#'basic.ack'{delivery_tag = MsgSeqNo}, State),
	State;

%% 同一时间多个消息进行confirm的操作
send_confirms(Cs, State) ->
	coalesce_and_send(Cs, fun(MsgSeqNo, Multiple) ->
								  #'basic.ack'{delivery_tag = MsgSeqNo,
											   multiple     = Multiple}
					  end, State).


%% coalesce：合并
%% 将需要confirm的消息进行合并，一次性发送到客户端
coalesce_and_send(MsgSeqNos, MkMsgFun, State = #ch{unconfirmed = UC}) ->
	%% 对需要confirm的id进行排序
	SMsgSeqNos = lists:usort(MsgSeqNos),
	CutOff = case dtree:is_empty(UC) of
				 true  -> lists:last(SMsgSeqNos) + 1;
				 false -> {SeqNo, _XName} = dtree:smallest(UC), SeqNo
			 end,
	{Ms, Ss} = lists:splitwith(fun(X) -> X < CutOff end, SMsgSeqNos),
	case Ms of
		[] -> ok;
		%% 此处是通知客户端小于Ms最后一个元素的所有消息都已经confirm过(这是进行多次confirm缩小消息量的解决办法)
		_  -> ok = send(MkMsgFun(lists:last(Ms), true), State)
	end,
	%% Ss列表中的元素则需要一个个的通知客户端消息已经存储到消息队列中(因为有可能小于Ss列表中的元素还没有得到confirm，所以只能进行一个个的进行confirm操作)
	[ok = send(MkMsgFun(SeqNo, false), State) || SeqNo <- Ss],
	State.


%% 事务中记录已经消费者已经ack的列表
ack_cons(Tag, Acked, [{Tag, Acks} | L]) -> [{Tag, Acked ++ Acks} | L];

ack_cons(Tag, Acked, Acks)              -> [{Tag, Acked} | Acks].


ack_len(Acks) -> lists:sum([length(L) || {ack, L} <- Acks]).


%% 判断事务提交操作是否完成
maybe_complete_tx(State = #ch{tx = {_, _}}) ->
	State;

maybe_complete_tx(State = #ch{unconfirmed = UC}) ->
	case dtree:is_empty(UC) of
		false -> State;
		true  -> complete_tx(State#ch{confirmed = []})
	end.


%% 完成事务提交操作后的操作函数，将事务字段tx重新置为初始化状态
complete_tx(State = #ch{tx = committing}) ->
	ok = send(#'tx.commit_ok'{}, State),
	State#ch{tx = new_tx()};

%% 如果事务字段tx为failed，则通知客户端事务提交失败，然后将当前rabbit_channel进程中的事务字段重新置为初始化状态
complete_tx(State = #ch{tx = failed}) ->
	{noreply, State1} = handle_exception(
						  rabbit_misc:amqp_error(
							precondition_failed, "partial tx completion", [],
							'tx.commit'),
						  State),
	State1#ch{tx = new_tx()}.


%% 根据Items中的key得到channel进程中的相关信息
infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].


%% 拿到当前channel进程的Pid
i(pid,            _)                               -> self();

%% 拿到连接进程的Pid
i(connection,     #ch{conn_pid         = ConnPid}) -> ConnPid;

%% 拿到当前channel进程的对应的数字编号
i(number,         #ch{channel          = Channel}) -> Channel;

%% 拿到当前channel进程中的用户名字
i(user,           #ch{user             = User})    -> User#user.username;

%% 拿到当前channel进程中的VHost
i(vhost,          #ch{virtual_host     = VHost})   -> VHost;

%% transactional：事务，得到当前rabbit_channel进程是否处在事务状态中
i(transactional,  #ch{tx               = Tx})      -> Tx =/= none;

%% 得到当前rabbit_channel进程是否开启confirm
i(confirm,        #ch{confirm_enabled  = CE})      -> CE;

%% 得到当前rabbit_channel进程的名字
i(name,           State)                           -> name(State);

%% 拿到当前channel中消费者的数量
i(consumer_count,          #ch{consumer_mapping = CM})    -> dict:size(CM);

%% 拿到当前rabbit_channel进程中还没有confirm的消息数量
i(messages_unconfirmed,    #ch{unconfirmed = UC})         -> dtree:size(UC);

%% 拿到当前rabbit_channel进程中还没有ack的消息数量
i(messages_unacknowledged, #ch{unacked_message_q = UAMQ}) -> queue:len(UAMQ);

%% 拿到当前rabbit_channel进程中事务中尚未提交的消息数量
i(messages_uncommitted,    #ch{tx = {Msgs, _Acks}})       -> queue:len(Msgs);

%% 拿到当前rabbit_channel进程中事务中尚未提交的消息数量
i(messages_uncommitted,    #ch{})                         -> 0;

%% 拿到当前rabbit_channel进程中事务中尚未ack的消息数量
i(acks_uncommitted,        #ch{tx = {_Msgs, Acks}})       -> ack_len(Acks);

%% 拿到当前rabbit_channel进程中事务中尚未ack的消息数量
i(acks_uncommitted,        #ch{})                         -> 0;

%% 拿到当前rabbit_channel进程的状态
i(state,                   #ch{state = running})         -> credit_flow:state();

%% 拿到当前rabbit_channel进程的状态
i(state,                   #ch{state = State})            -> State;

%% 拿到当前rabbit_channel进程中设置的预取的消息数量
i(prefetch_count,          #ch{consumer_prefetch = C})    -> C;

%% 拿到当前rabbit_channel进程中全局设置的预取的消息数量
i(global_prefetch_count, #ch{limiter = Limiter}) ->
	rabbit_limiter:get_prefetch_limit(Limiter);

%% 不能解释的Key
i(Item, _) ->
	throw({bad_argument, Item}).


%% 组装得到当前rabbit_channel进程的名字
name(#ch{conn_name = ConnName, channel = Channel}) ->
	list_to_binary(rabbit_misc:format("~s (~p)", [ConnName, Channel])).


%% 增加进程字典中的操作数据
incr_stats(Incs, Measure) ->
	[update_measures(Type, Key, Inc, Measure) || {Type, Key, Inc} <- Incs].


update_measures(Type, Key, Inc, Measure) ->
	Measures = case get({Type, Key}) of
				   undefined -> [];
				   D         -> D
			   end,
	Cur = case orddict:find(Measure, Measures) of
			  error   -> 0;
			  {ok, C} -> C
		  end,
	put({Type, Key}, orddict:store(Measure, Cur + Inc, Measures)).


%% 发布rabbit_channel进程的相关信息到rabbit_event
emit_stats(State) -> emit_stats(State, []).


emit_stats(State, Extra) ->
	%% 根据收集信息中的key列表得到channel进程中的相关信息
	Coarse = infos(?STATISTICS_KEYS, State),
	case rabbit_event:stats_level(State, #ch.stats_timer) of
		%% 如果stats_timer结构中的level是coarse则立刻将得到的rabbit_channel进行的信息发布到rabbit_event
		coarse -> rabbit_event:notify(channel_stats, Extra ++ Coarse);
		%% 如果stats_timer结构中的level是fine，则还要收集队列状态，交换机状态等信息
		fine   -> Fine = [
						  {channel_queue_stats,
						   [{QName, Stats} ||
							{{queue_stats,       QName}, Stats} <- get()]},
						  {channel_exchange_stats,
						   [{XName, Stats} ||
							{{exchange_stats,    XName}, Stats} <- get()]},
						  {channel_queue_exchange_stats,
						   [{QX, Stats} ||
							{{queue_exchange_stats, QX}, Stats} <- get()]}],
				  %% 将收集得到的所有关键信息发布到rabbit_event
				  rabbit_event:notify(channel_stats, Extra ++ Coarse ++ Fine)
	end.


%% 擦除rabbit_channel进程保存在进程字典中的队列信息
erase_queue_stats(QName) ->
	erase({queue_stats, QName}),
	[erase({queue_exchange_stats, QX}) ||
	   {{queue_exchange_stats, QX = {QName0, _}}, _} <- get(),
	   QName0 =:= QName].
