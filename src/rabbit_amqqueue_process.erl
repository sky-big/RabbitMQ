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

-module(rabbit_amqqueue_process).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(gen_server2).

-define(SYNC_INTERVAL,                 200). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL, 5000).
-define(CONSUMER_BIAS_RATIO,           1.1). %% i.e. consume 10% faster

-export([info_keys/0]).

-export([init_with_backing_queue_state/7]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).

%% Queue's state
-record(q, {
			q,															%% 队列信息数据结构amqqueue
			exclusive_consumer,											%% 当前队列的独有消费者
			has_had_consumers,											%% 当前队列中是否有消费者的标识
			backing_queue,												%% backing_queue对应的模块名字
			backing_queue_state,										%% backing_queue对应的状态结构
			consumers,													%% 消费者存储的优先级队列
			expires,													%% 当前队列未使用就删除自己的时间
			sync_timer_ref,												%% 同步confirm的定时器，当前队列大部分接收一次消息就要确保当前定时器的存在(200ms的定时器)
			rate_timer_ref,												%% 队列中消息进入和出去的速率定时器
			expiry_timer_ref,											%% 队列中未使用就删除自己的定时器
			stats_timer,												%% 向rabbit_event发布信息的数据结构状态字段
			msg_id_to_channel,											%% 当前队列进程中等待confirm的消息gb_trees结构，里面的结构是Key:MsgId Value:{SenderPid, MsgSeqNo}
			ttl,														%% 队列中设置的消息存在的时间
			ttl_timer_ref,												%% 队列中消息存在的定时器
			ttl_timer_expiry,											%% 当前队列头部消息的过期时间点
			senders,													%% 向当前队列发送消息的rabbit_channel进程列表
			dlx,														%% 死亡消息要发送的exchange交换机(通过队列声明的参数或者policy接口来设置)
			dlx_routing_key,											%% 死亡消息要发送的路由规则(通过队列声明的参数或者policy接口来设置)
			max_length,													%% 当前队列中消息的最大上限(通过队列声明的参数或者policy接口来设置)
			max_bytes,													%% 队列中消息内容占的最大空间
			args_policy_version,										%% 当前队列中参数设置对应的版本号，每设置一次都会将版本号加一
			status														%% 当前队列的状态
		   }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(init_with_backing_queue_state/7 ::
        (rabbit_types:amqqueue(), atom(), tuple(), any(),
         [rabbit_types:delivery()], pmon:pmon(), dict:dict()) -> #q{}).

-endif.

%%----------------------------------------------------------------------------
%% 统计消息队列相关信息的关键key列表
-define(STATISTICS_KEYS,
		[
		 name,
		 policy,
		 exclusive_consumer_pid,
		 exclusive_consumer_tag,
		 messages_ready,
		 messages_unacknowledged,
		 messages,
		 consumers,
		 consumer_utilisation,
		 memory,
		 slave_pids,
		 synchronised_slave_pids,
		 recoverable_slaves,
		 state
		]).

%% 消息队列创建的时候向rabbit_event事件中心发布的信息关键key列表
-define(CREATION_EVENT_KEYS,
		[
		 name,
		 durable,
		 auto_delete,
		 arguments,
		 owner_pid
		]).

%% 消息队列所有信息的关键key列表
-define(INFO_KEYS, [pid | ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [name]]).

%%----------------------------------------------------------------------------
%% 列出队列宏定义里关键key对应的信息，包括backing_queue的关键key
info_keys()       -> ?INFO_KEYS       ++ rabbit_backing_queue:info_keys().


%% 列出消息队列统计信息的关键key列表，包括backing_queue的关键key
statistics_keys() -> ?STATISTICS_KEYS ++ rabbit_backing_queue:info_keys().

%%----------------------------------------------------------------------------
%% 队列进程的初始化函数，该进程启动在rabbit_amqqueue_sup监督进程下
init(Q) ->
	process_flag(trap_exit, true),
	%% 通过队列的名字保存当前队列进程的进程名字
	?store_proc_name(Q#amqqueue.name),
	%% 每次队列重启或者第一次启动都会在此处更新当前队列的Pid
	{ok, init_state(Q#amqqueue{pid = self()}), hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE},
	 %% 在此处返回最新的回调模块(此功能是gen_server2行为新增加的功能，在此处改变gen_server2的回调模块)
	 ?MODULE}.


%% 初始化队列的基本信息
init_state(Q) ->
	%% 组装队列状态的数据结构
	State = #q{q                   = Q,
			   exclusive_consumer  = none,
			   has_had_consumers   = false,
			   consumers           = rabbit_queue_consumers:new(),
			   senders             = pmon:new(delegate),
			   msg_id_to_channel   = gb_trees:empty(),
			   status              = running,
			   args_policy_version = 0},
	%% 初始化向rabbit_event事件中心发布事件的数据结构
	rabbit_event:init_stats_timer(State, #q.stats_timer).


%% 初始化队列，如果该消息队列没有独有拥有者，则直接后续的初始化
init_it(Recover, From, State = #q{q = #amqqueue{exclusive_owner = none}}) ->
	init_it2(Recover, From, State);

%% You used to be able to declare an exclusive durable queue. Sadly we
%% need to still tidy up after that case, there could be the remnants
%% of one left over from an upgrade. So that's why we don't enforce
%% Recover = new here.
%% 如果当前队列设置有exclusive_owner拥有者(该拥有者是rabbit_reader进程Pid)
%% 如果拥有者死亡，将当前队列进程立刻停止，missing_owner的DOWN原因会将当前队列的信息全部删除，包括持久化队列信息
init_it(Recover, From, State = #q{q = #amqqueue{exclusive_owner = Owner}}) ->
	case rabbit_misc:is_process_alive(Owner) of
		%% 如果拥有者当前是存活的，则队列继续下面的初始化
		true  -> erlang:monitor(process, Owner),
				 init_it2(Recover, From, State);
		%% 拥有者已经死亡
		false -> #q{backing_queue       = undefined,
					backing_queue_state = undefined,
					q                   = Q} = State,
				 %% 通知让自己初始化的进程，拥有者已经死亡
				 send_reply(From, {owner_died, Q}),
				 BQ = backing_queue_module(Q),
				 %% 根据恢复信息，将当前backing_queue的数据恢复出来，当队列处理missing_owner的时候，会将backing_queue的数据清除掉
				 {_, Terms} = recovery_status(Recover),
				 BQS = bq_init(BQ, Q, Terms),
				 %% Rely on terminate to delete the queue.
				 %% 将当前队列进程立刻停止，missing_owner的DOWN原因会将当前队列的信息全部删除，包括持久化队列信息
				 {stop, {shutdown, missing_owner},
				  State#q{backing_queue = BQ, backing_queue_state = BQS}}
	end.


%% 消息队列进程启动后，通知队列进程进行相关的初始化(如果该队列是持久化队列，且上次RabbitMQ系统崩溃，则在此处可以恢复)
init_it2(Recover, From, State = #q{q                   = Q,
								   backing_queue       = undefined,
								   backing_queue_state = undefined}) ->
	{Barrier, TermsOrNew} = recovery_status(Recover),
	%% 将新增加的队列存入rabbit_queue数据库表(如果当前队列不是重新创建，则只用更新队列的状态为live，如果是新的队列，则将队列的数据写入到数据库中)
	case rabbit_amqqueue:internal_declare(Q, Recover /= new) of
		#amqqueue{} = Q1 ->
			%% Q和Q1中的字段进行比较
			case matches(Recover, Q, Q1) of
				true ->
					%% 向file_handle_cache进行注册
					ok = file_handle_cache:register_callback(
						   rabbit_amqqueue, set_maximum_since_use, [self()]),
					%% rabbit_memory_monitor进程通知消息队列最新的内存持续时间
					ok = rabbit_memory_monitor:register(
						   self(), {rabbit_amqqueue,
									set_ram_duration_target, [self()]}),
					%% 拿到rabbit应用中backing_queue_module对应的配置参数(现在是rabbit_priority_queue)，如果队列是镜像队列，则为rabbit_mirror_queue_master
					BQ = backing_queue_module(Q1),
					%% 根据得到的模块名(rabbit_priority_queue)进行相关的初始化
					BQS = bq_init(BQ, Q, TermsOrNew),
					%% 通知启动该队列进程{new, Q}消息结构
					send_reply(From, {new, Q}),
					%% 恢复屏障，阻塞等待rabbit进程发送go的消息(如果是恢复持久化队列，则该队列进程阻塞等待启动进程发过来的go消息)
					recovery_barrier(Barrier),
					%% 通过队列声明时候的参数或者通过rabbitmqctl设置过来的参数初始化队列状态
					State1 = process_args_policy(
							   State#q{backing_queue       = BQ,
									   backing_queue_state = BQS}),
					%% 通知队列修饰模块startup启动事件
					notify_decorators(startup, State),
					%% 发布队列创建的事件
					rabbit_event:notify(queue_created,
										infos(?CREATION_EVENT_KEYS, State1)),
					%% 如果配置文件配置，则将当前队列的信息发布到rabbit_event中
					rabbit_event:if_enabled(State1, #q.stats_timer,
											fun() -> emit_stats(State1) end),
					noreply(State1);
				false ->
					{stop, normal, {existing, Q1}, State}
			end;
		Err ->
			{stop, normal, Err, State}
	end.


%% barrier:屏障
recovery_status(new)              -> {no_barrier, new};

recovery_status({Recover, Terms}) -> {Recover,    Terms}.


%% 向From返回Q
send_reply(none, _Q) -> ok;

send_reply(From, Q)  -> gen_server2:reply(From, Q).


%% 判断Q1和Q2队列是否匹配
matches(new, Q1, Q2) ->
    %% i.e. not policy
    Q1#amqqueue.name            =:= Q2#amqqueue.name            andalso
    Q1#amqqueue.durable         =:= Q2#amqqueue.durable         andalso
    Q1#amqqueue.auto_delete     =:= Q2#amqqueue.auto_delete     andalso
    Q1#amqqueue.exclusive_owner =:= Q2#amqqueue.exclusive_owner andalso
    Q1#amqqueue.arguments       =:= Q2#amqqueue.arguments       andalso
    Q1#amqqueue.pid             =:= Q2#amqqueue.pid             andalso
    Q1#amqqueue.slave_pids      =:= Q2#amqqueue.slave_pids;

matches(_,  Q,   Q) -> true;

matches(_, _Q, _Q1) -> false.


%% 恢复屏障，阻塞等待rabbit进程发送go的消息
recovery_barrier(no_barrier) ->
	ok;

recovery_barrier(BarrierPid) ->
	MRef = erlang:monitor(process, BarrierPid),
	receive
		{BarrierPid, go}              -> erlang:demonitor(MRef, [flush]);
		{'DOWN', MRef, process, _, _} -> ok
	end.


%% 副镜像队列成为主镜像队列后会调用该接口进行backing_queue的状态的初始化
init_with_backing_queue_state(Q = #amqqueue{exclusive_owner = Owner}, BQ, BQS,
							  RateTRef, Deliveries, Senders, MTC) ->
	case Owner of
		none -> ok;
		_    -> erlang:monitor(process, Owner)
	end,
	%% 初始化队列的基本信息
	State = init_state(Q),
	%% 初始化backing_queue模块名字和状态数据结构，速率定时器，rabbit_channel进程列表，以及消息confirm相关的数据结构
	State1 = State#q{backing_queue       = BQ,
					 backing_queue_state = BQS,
					 rate_timer_ref      = RateTRef,
					 senders             = Senders,
					 msg_id_to_channel   = MTC},
	%% 通过队列声明时候的参数或者通过rabbitmqctl设置过来的参数初始化队列状态
	State2 = process_args_policy(State1),
	State3 = lists:foldl(fun (Delivery, StateN) ->
								  deliver_or_enqueue(Delivery, true, StateN)
						 end, State2, Deliveries),
	%% 通知队列修饰模块当前主镜像队列已经启动
	notify_decorators(startup, State3),
	State3.


%% 队列中断的接口(shutdown的原因，则消息队列直接停止，不会将自己的信息全部删除)
terminate(shutdown = R,      State = #q{backing_queue = BQ}) ->
	terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);

%% 队列进程关闭，原因是丢失拥有者，拥有者丢失后，立刻将队列删除掉，会将该队列直接从RabbitMQ系统中删除，包括持久化队列
terminate({shutdown, missing_owner} = Reason, State) ->
	%% if the owner was missing then there will be no queue, so don't emit stats
	terminate_shutdown(terminate_delete(false, Reason, State), State);

%% 队列进程关闭，消息队列直接停止，不会将自己的信息全部删除
terminate({shutdown, _} = R, State = #q{backing_queue = BQ}) ->
	terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);

%% 消息队列正常终止，会将该队列直接从RabbitMQ系统中删除，包括持久化队列
terminate(normal,            State) -> %% delete case
	terminate_shutdown(terminate_delete(true, normal, State), State);

%% If we crashed don't try to clean up the BQS, probably best to leave it.
%% 其它原因致使队列进程中断
terminate(_Reason,           State = #q{q = Q}) ->
	terminate_shutdown(fun (BQS) ->
								%% 现将队列的状态置为crashed
								Q2 = Q#amqqueue{state = crashed},
								%% 将amqqueue的数据结构写入mnesia数据库(如果需要持久化的则写入rabbit_durable_queue数据表)
								rabbit_misc:execute_mnesia_transaction(
								  fun() ->
										  rabbit_amqqueue:store_queue(Q2)
								  end),
								BQS
					   end, State).


%% 将该队列的backing_queue的数据删除掉，同时将当前队列从Mnesia数据库中删除掉
terminate_delete(EmitStats, Reason,
				 State = #q{q = #amqqueue{name          = QName},
							backing_queue = BQ}) ->
	fun (BQS) ->
			 %% 先让backing_queue执行delete_and_terminate操作
			 BQS1 = BQ:delete_and_terminate(Reason, BQS),
			 %% 将当前队列的状态发布到rabbit_event事件中心中
			 if EmitStats -> rabbit_event:if_enabled(State, #q.stats_timer,
													 fun() -> emit_stats(State) end);
				true      -> ok
			 end,
			 %% 然后执行内部的队列删除操作
			 %% don't care if the internal delete doesn't return 'ok'.
			 rabbit_amqqueue:internal_delete(QName),
			 BQS1
	end.


%% 先将当前队列中所有的定时器立刻关闭
%% 队列进程从rabbit_memory_monitor进程中取消注册，将所有的消费者信息发布到rabbit_event事件中心，然后执行Fun函数
terminate_shutdown(Fun, State) ->
	State1 = #q{backing_queue_state = BQS, consumers = Consumers} =
				   lists:foldl(fun (F, S) -> F(S) end, State,
							   [%% 停止同步confirm的定时器
								fun stop_sync_timer/1,
								%% 停止速率监控定时器
								fun stop_rate_timer/1,
								%% 将队列expires未使用就删除的定时器停止掉
								fun stop_expiry_timer/1,
								%% 停止消息存在时间过期删除消息的定时器
								fun stop_ttl_timer/1]),
	case BQS of
		undefined -> State1;
		_         -> %% 取消在rabbit_memory_monitor内存速率监控进程的注册
					 ok = rabbit_memory_monitor:deregister(self()),
					 %% 取得当前队列的队列名字
					 QName = qname(State),
					 %% 通知队列的修饰模块，队列进程终止
					 notify_decorators(shutdown, State),
					 %% 将当前队列的所有消费者被删除的信息发布到rabbit_event
					 [emit_consumer_deleted(Ch, CTag, QName) ||
						{Ch, CTag, _, _, _} <-
							rabbit_queue_consumers:all(Consumers)],
					 %% 对backing_queue_state的状态执行Fun函数
					 State1#q{backing_queue_state = Fun(BQS)}
	end.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------------

maybe_notify_decorators(false, State) -> State;

maybe_notify_decorators(true,  State) -> notify_decorators(State), State.


%% 通知队列修饰模块Event事件
notify_decorators(Event, State) -> decorator_callback(qname(State), Event, []).


notify_decorators(State = #q{consumers           = Consumers,
							 backing_queue       = BQ,
							 backing_queue_state = BQS}) ->
	P = rabbit_queue_consumers:max_active_priority(Consumers),
	decorator_callback(qname(State), consumer_state_changed,
					   [P, BQ:is_empty(BQS)]).


decorator_callback(QName, F, A) ->
	%% Look up again in case policy and hence decorators have changed
	case rabbit_amqqueue:lookup(QName) of
		{ok, Q = #amqqueue{decorators = Ds}} ->
			[ok = apply(M, F, [Q | A]) || M <- rabbit_queue_decorator:select(Ds)];
		{error, not_found} ->
			ok
	end.


%% 根据得到的backing_queue模块名BQ进行相关的初始化
bq_init(BQ, Q, Recover) ->
	Self = self(),
	BQ:init(Q, Recover,
			fun (Mod, Fun) ->
					 rabbit_amqqueue:run_backing_queue(Self, Mod, Fun)
			end).


%% 通过队列声明时候的参数或者通过rabbitmqctl设置过来的参数初始化队列状态
process_args_policy(State = #q{q                   = Q,
							   args_policy_version = N}) ->
	%% 第二个参数是解决队列参数和队列policy设置的值的冲突解决函数
	ArgsTable =
		[
		 %% 控制queue被自动删除前可以处于未使用状态的时间，未使用的意思是queue上没有任何consumer，queue没有被重新声明，并且在过期时间段内未调用过basic.get命令
		 {<<"expires">>,                 fun res_min/2, fun init_exp/2},
		 %% 将死的信息重新发布到该exchange交换机上(死信息包括：1.消息被拒绝（basic.reject or basic.nack）；2.消息TTL过期；3.队列达到最大长度)
		 {<<"dead-letter-exchange">>,    fun res_arg/2, fun init_dlx/2},
		 %% 将死的新重新发布的时候的路由规则
		 {<<"dead-letter-routing-key">>, fun res_arg/2, fun init_dlx_rkey/2},
		 %% 控制被publish到queue中的 message 被丢弃前能够存活的时间
		 {<<"message-ttl">>,             fun res_min/2, fun init_ttl/2},
		 %% 当前队列最大消息数量参数
		 {<<"max-length">>,              fun res_min/2, fun init_max_length/2},
		 %% 当前队列中消息的内容最大上限
		 {<<"max-length-bytes">>,        fun res_min/2, fun init_max_bytes/2}
		],
	drop_expired_msgs(
	  lists:foldl(fun({Name, Resolve, Fun}, StateN) ->
						  Fun(args_policy_lookup(Name, Resolve, Q), StateN)
				  end, State#q{args_policy_version = N + 1}, ArgsTable)).


%% 解决rabbit_policy定制的当前队列的策略和消息队列自己带的策略的冲突的解决函数，如果两边都有Name的配置，则调用Resolve函数进行解决
args_policy_lookup(Name, Resolve, Q = #amqqueue{arguments = Args}) ->
	%% 队列声明初始化的时候会在参数前面加上"x-"前缀
	AName = <<"x-", Name/binary>>,
	case {rabbit_policy:get(Name, Q), rabbit_misc:table_lookup(Args, AName)} of
		{undefined, undefined}       -> undefined;
		{undefined, {_Type, Val}}    -> Val;
		{Val,       undefined}       -> Val;
		{PolVal,    {_Type, ArgVal}} -> Resolve(PolVal, ArgVal)
	end.


%% 以队列声明的时候参数为准
res_arg(_PolVal, ArgVal) -> ArgVal.


%% 获取最小值
res_min(PolVal, ArgVal)  -> erlang:min(PolVal, ArgVal).

%% In both these we init with the undefined variant first to stop any
%% existing timer, then start a new one which may fire after a
%% different time.
%% 如果expires处于未使用删除队列时间没有设置，则将expires定时器停止掉
init_exp(undefined, State) -> stop_expiry_timer(State#q{expires = undefined});

init_exp(Expires,   State) -> State1 = init_exp(undefined, State),
							  %% 启动队列未使用就删除的定时器
							  ensure_expiry_timer(State1#q{expires = Expires}).


%% 初始化队列中消息存在的时间
init_ttl(undefined, State) -> stop_ttl_timer(State#q{ttl = undefined});

init_ttl(TTL,       State) -> (init_ttl(undefined, State))#q{ttl = TTL}.


%% 初始化队列死亡消息要发送的exchange交换机
init_dlx(undefined, State) ->
	State#q{dlx = undefined};

init_dlx(DLX, State = #q{q = #amqqueue{name = QName}}) ->
	State#q{dlx = rabbit_misc:r(QName, exchange, DLX)}.


%% 初始化队列死亡消息要发送的路由规则
init_dlx_rkey(RoutingKey, State) -> State#q{dlx_routing_key = RoutingKey}.


%% 根据参数初始化当前队列中消息的最大上限
init_max_length(MaxLen, State) ->
	{_Dropped, State1} = maybe_drop_head(State#q{max_length = MaxLen}),
	State1.


%% 初始化队列中消息内容占的最大空间
init_max_bytes(MaxBytes, State) ->
	{_Dropped, State1} = maybe_drop_head(State#q{max_bytes = MaxBytes}),
	State1.


%% 队列进程处理消息后，返回消息给发送进程，同时如果需要做同步操作则进行同步操作
reply(Reply, NewState) ->
	{NewState1, Timeout} = next_state(NewState),
	{reply, Reply, ensure_stats_timer(ensure_rate_timer(NewState1)), Timeout}.


%% 队列进程处理消息后，没有必要返回消息给发送进程，同时如果需要做同步操作则进行同步操作
noreply(NewState) ->
	{NewState1, Timeout} = next_state(NewState),
	{noreply, ensure_stats_timer(ensure_rate_timer(NewState1)), Timeout}.


next_state(State = #q{backing_queue       = BQ,
					  backing_queue_state = BQS,
					  msg_id_to_channel   = MTC}) ->
	%% 进行断言判断，如果有消费者同时消息队列中有消息，则认为出错了，有消费者同时又有消息，则是不应该出现的情况
	assert_invariant(State),
	%% 从backing_queue得到已经confirm的消息ID列表
	{MsgIds, BQS1} = BQ:drain_confirmed(BQS),
	%% 将得到的已经confirm消息ID发送到rabbit_channel进程进程confirm操作
	MTC1 = confirm_messages(MsgIds, MTC),
	State1 = State#q{backing_queue_state = BQS1, msg_id_to_channel = MTC1},
	%% 通过backing_queue模块判断是否需要同步confirm操作
	case BQ:needs_timeout(BQS1) of
		%% 如果消息索引模块中有没有尚未confirm的消息，则停止同步的定时器
		false -> {stop_sync_timer(State1),   hibernate     };
		%% 如果消息索引模块中有没有尚未confirm的消息，则停止同步的定时器，但是定一个200ms的超时，进行后续的同步操作
		idle  -> {stop_sync_timer(State1),   ?SYNC_INTERVAL};
		%% 消息索引模块中有尚未confirm的消息，则开启同步的定时器
		timed -> {ensure_sync_timer(State1), 0             }
	end.


%% 拿到rabbit应用中backing_queue_module对应的配置参数(现在是rabbit_priority_queue)，如果队列是镜像队列，则为rabbit_mirror_queue_master
backing_queue_module(Q) ->
	case rabbit_mirror_queue_misc:is_mirrored(Q) of
		%% 该队列没有设置镜像队列，则拿到backing_queue的模块为rabbit_priority_queue
		false -> {ok, BQM} = application:get_env(backing_queue_module),
				 BQM;
		%% 当期队列是镜像队列，且该队列为主镜像队列
		true  -> rabbit_mirror_queue_master
	end.


%% 启动同步confirm的定时器
ensure_sync_timer(State) ->
	rabbit_misc:ensure_timer(State, #q.sync_timer_ref,
							 ?SYNC_INTERVAL, sync_timeout).


%% 停止同步confirm的定时器
stop_sync_timer(State) -> rabbit_misc:stop_timer(State, #q.sync_timer_ref).


%% 启动速率监控的定时器
ensure_rate_timer(State) ->
	rabbit_misc:ensure_timer(State, #q.rate_timer_ref,
							 ?RAM_DURATION_UPDATE_INTERVAL,
							 update_ram_duration).


%% 停止速率监控定时器
stop_rate_timer(State) -> rabbit_misc:stop_timer(State, #q.rate_timer_ref).

%% We wish to expire only when there are no consumers *and* the expiry
%% hasn't been refreshed (by queue.declare or basic.get) for the
%% configured period.
%% 将队列expires未使用就删除的定时器开启
ensure_expiry_timer(State = #q{expires = undefined}) ->
	State;

ensure_expiry_timer(State = #q{expires             = Expires,
							   args_policy_version = Version}) ->
	case is_unused(State) of
		%% 如果没有消费者，则将当前未使用定时器停止掉，重新启动定时器，如果在这个时间段没有basic_get操作，则将队列删除掉
		true  -> NewState = stop_expiry_timer(State),
				 rabbit_misc:ensure_timer(NewState, #q.expiry_timer_ref,
										  Expires, {maybe_expire, Version});
		false -> State
	end.


%% 将队列expires未使用就删除的定时器停止掉
stop_expiry_timer(State) -> rabbit_misc:stop_timer(State, #q.expiry_timer_ref).


%% 启动消息过期检查的定时器(将队列头部的第一个消息的过期时间拿出来设置过期时间定时器)
ensure_ttl_timer(undefined, State) ->
	State;

ensure_ttl_timer(Expiry, State = #q{ttl_timer_ref       = undefined,
									args_policy_version = Version}) ->
	%% 消息中存储的过期时间是微秒单位
	After = (case Expiry - now_micros() of
				 V when V > 0 -> V + 999; %% always fire later
				 _            -> 0
			 end) div 1000,
	%% 向自己当前的队列进程发送消息过期消息
	TRef = rabbit_misc:send_after(After, self(), {drop_expired, Version}),
	State#q{ttl_timer_ref = TRef, ttl_timer_expiry = Expiry};

%% 新传入的消息只有小于当前消息的过期时间，才取消定时器，重新设置定时器
ensure_ttl_timer(Expiry, State = #q{ttl_timer_ref    = TRef,
									ttl_timer_expiry = TExpiry})
  %% 如果当前的消息过期时间TExpiry大于Expiry加上一毫秒
  when Expiry + 1000 < TExpiry ->
	%% 则取消当前的定时器，用Expiry这个小的时间开启定时器
	rabbit_misc:cancel_timer(TRef),
	%% 使用新的时间设置消息过期定时器
	ensure_ttl_timer(Expiry, State#q{ttl_timer_ref = undefined});

ensure_ttl_timer(_Expiry, State) ->
	State.


%% 停止消息存在时间过期删除消息的定时器
stop_ttl_timer(State) -> rabbit_misc:stop_timer(State, #q.ttl_timer_ref).


%% 确认状态定时器的启动
ensure_stats_timer(State) ->
	rabbit_event:ensure_stats_timer(State, #q.stats_timer, emit_stats).


%% 进行断言判断，如果有消费者同时消息队列中有消息，则认为出错了，有消费者同时又有消息，则是不应该出现的情况
assert_invariant(State = #q{consumers = Consumers}) ->
	true = (rabbit_queue_consumers:inactive(Consumers) orelse is_empty(State)).


%% 判断当前消息队列是否为空
is_empty(#q{backing_queue = BQ, backing_queue_state = BQS}) -> BQ:is_empty(BQS).


%% 队列中的消息为空后，将当前队列中credit中的mode为drain的消费者取出来，通知这些消费者队列中的消息为空
maybe_send_drained(WasEmpty, State) ->
	case (not WasEmpty) andalso is_empty(State) of
		%% 当前情况是原始状态的队列是非空的，但是做了相关的操作后成为空队列
		true  -> notify_decorators(State),
				 rabbit_queue_consumers:send_drained();
		false -> ok
	end,
	State.


%% 进行消息的confirm
confirm_messages([], MTC) ->
	MTC;

confirm_messages(MsgIds, MTC) ->
	{CMs, MTC1} =
		lists:foldl(
		  fun(MsgId, {CMs, MTC0}) ->
				  %% 从未confirm的gb_trees结构中查找该消息对应的信息
				  case gb_trees:lookup(MsgId, MTC0) of
					  {value, {SenderPid, MsgSeqNo}} ->
						  %% 将rabbit_channel进程的Pid对应的键值对更新插入MsgSeqNo
						  {rabbit_misc:gb_trees_cons(SenderPid,
													 MsgSeqNo, CMs),
						   %% 将该消息ID对应的key-value键值对从gb_trees结构中删除掉
						   gb_trees:delete(MsgId, MTC0)};
					  none ->
						  {CMs, MTC0}
				  end
		  end, {gb_trees:empty(), MTC}, MsgIds),
	%% 将得到已经confirm的消息confirm号发送到rabbit_channel进程
	rabbit_misc:gb_trees_foreach(fun rabbit_misc:confirm_to_sender/2, CMs),
	MTC1.


%% 消息队列记录要confirm的消息，如果confirm为false，则不记录要confirm
send_or_record_confirm(#delivery{confirm    = false}, State) ->
	{never, State};

%% 如果confirm为true，同时durable为true，则将消息记录要msg_id_to_channel字段
send_or_record_confirm(#delivery{confirm    = true,
								 sender     = SenderPid,
								 msg_seq_no = MsgSeqNo,
								 message    = #basic_message {
															  is_persistent = true,
															  id            = MsgId}},
					   State = #q{q                 = #amqqueue{durable = true},
								  msg_id_to_channel = MTC}) ->
	%% 将需要confirm的消息插入msg_id_to_channel平衡二叉树字段中
	MTC1 = gb_trees:insert(MsgId, {SenderPid, MsgSeqNo}, MTC),
	%% 表示消息是持久化消息且消息队列也是持久化队列，需要等到消息存储到磁盘才能够进行confirm操作
	{eventually, State#q{msg_id_to_channel = MTC1}};

%% 如果消息需要confirm，但是队列不是持久化队列，则通知rabbit_channel进程已经收到该消息，即向rabbit_channel进程进行confirm
send_or_record_confirm(#delivery{confirm    = true,
								 sender     = SenderPid,
								 msg_seq_no = MsgSeqNo}, State) ->
	%% 向rabbit_channel进程发送confirm消息
	rabbit_misc:confirm_to_sender(SenderPid, [MsgSeqNo]),
	%% 消息是非持久化消息，但是需要进行confirm，则当前已经到消息队列，可以立刻通知rabbit_channel进程进行confirm操作
	{immediately, State}.


%% 如果当前消息mandatory字段为true，则立刻通知该消息对应的rabbit_channel进程
send_mandatory(#delivery{mandatory  = false}) ->
	ok;

send_mandatory(#delivery{mandatory  = true,
						 sender     = SenderPid,
						 msg_seq_no = MsgSeqNo}) ->
	gen_server2:cast(SenderPid, {mandatory_received, MsgSeqNo}).


%% 丢弃消息
discard(#delivery{confirm = Confirm,
				  sender  = SenderPid,
				  flow    = Flow,
				  message = #basic_message{id = MsgId}}, BQ, BQS, MTC) ->
	%% 丢弃的消息如果需要进行confirm操作，则立刻通知rabbit_channel进程
	MTC1 = case Confirm of
			   %% 进行消息的confirm
			   true  -> confirm_messages([MsgId], MTC);
			   false -> MTC
		   end,
	%% backing_queue消息丢弃则不做任何操作
	BQS1 = BQ:discard(MsgId, SenderPid, Flow, BQS),
	{BQS1, MTC1}.


%% 当前队列有没有被锁住的消费者，同时队列中还有消息，则将消息一条条的下发给没有锁住的消费者
run_message_queue(State) -> run_message_queue(false, State).


run_message_queue(ActiveConsumersChanged, State) ->
	case is_empty(State) of
		%% 如果消息队列为空，则不继续向消费者发送消息
		true  -> maybe_notify_decorators(ActiveConsumersChanged, State);
		false -> case rabbit_queue_consumers:deliver(
						%% 从backing_queue模块中取得一个消息
						fun(AckRequired) -> fetch(AckRequired, State) end,
						qname(State), State#q.consumers) of
					 %% 传递成功后，继续将消息发送给消费者，如果成功则不断的往后发送直到没有消息或者没有满足条件的消费者
					 {delivered, ActiveConsumersChanged1, State1, Consumers} ->
						 run_message_queue(
						   ActiveConsumersChanged or ActiveConsumersChanged1,
						   State1#q{consumers = Consumers});
					 %% 传递失败后，则不再继续将消息发送给消费者
					 {undelivered, ActiveConsumersChanged1, Consumers} ->
						 maybe_notify_decorators(
						   ActiveConsumersChanged or ActiveConsumersChanged1,
						   State#q{consumers = Consumers})
				 end
	end.


%% 尝试将消息发送给当前队列中的消费者
attempt_delivery(Delivery = #delivery{sender  = SenderPid,
									  flow    = Flow,
									  message = Message},
				 Props, Delivered, State = #q{backing_queue       = BQ,
											  backing_queue_state = BQS,
											  msg_id_to_channel   = MTC}) ->
	%% 检查当前队列中的消费者，如果有能够发送的消费者，则直接发送给消费者，如果没有满足条件的消费者，则将消息存入到队列中
	case rabbit_queue_consumers:deliver(
		   %% 需要确认的消息需要发送到backing_queue模块中记录下来
		   fun (true)  -> %% 如果当前能够将消息直接发送给消费者，则当前队列必须是为空
						  true = BQ:is_empty(BQS),
						  {AckTag, BQS1} =
							  %% 将消息发送到backing_queue中(将已经发给消费者等待ack的消息通知backing_queue)
							  BQ:publish_delivered(
								Message, Props, SenderPid, Flow, BQS),
						  {{Message, Delivered, AckTag}, {BQS1, MTC}};
			  (false) ->  %% 将消息发送给消费者后，如果消息不要确认，则直接将消息丢弃掉
				   		  {{Message, Delivered, undefined},
						  discard(Delivery, BQ, BQS, MTC)}
		   end, qname(State), State#q.consumers) of
		%% 已经将消息发送给消费者
		{delivered, ActiveConsumersChanged, {BQS1, MTC1}, Consumers} ->
			{delivered,   maybe_notify_decorators(
			   ActiveConsumersChanged,
			   State#q{backing_queue_state = BQS1,
					   msg_id_to_channel   = MTC1,
					   consumers           = Consumers})};
		%% 没有能够接收消息的消费者
		{undelivered, ActiveConsumersChanged, Consumers} ->
			{undelivered, maybe_notify_decorators(
			   ActiveConsumersChanged,
			   State#q{consumers = Consumers})}
	end.


%% 传递给消费者或者没有消费者则将消息存储在队列中
deliver_or_enqueue(Delivery = #delivery{message = Message,
										sender  = SenderPid,
										flow    = Flow},
				   Delivered, State = #q{backing_queue       = BQ,
										 backing_queue_state = BQS}) ->
	%% 如果当前消息mandatory字段为true，则立刻通知该消息对应的rabbit_channel进程
	send_mandatory(Delivery), %% must do this before confirms
	%% 消息队列记录要confirm的消息，如果confirm为false，则不记录要confirm(如果消息需要进行confirm，则将该消息的信息存入msg_id_to_channel字段中)
	{Confirm, State1} = send_or_record_confirm(Delivery, State),
	%% 得到消息特性特性数据结构
	Props = message_properties(Message, Confirm, State1),
	%% 让backing_queue去判断当前消息是否重复(rabbit_variable_queue没有实现，直接返回的false)
	{IsDuplicate, BQS1} = BQ:is_duplicate(Message, BQS),
	State2 = State1#q{backing_queue_state = BQS1},
	case IsDuplicate orelse attempt_delivery(Delivery, Props, Delivered,
											 State2) of
		true ->
			State2;
		%% 已经将消息发送给消费者的情况
		{delivered, State3} ->
			State3;
		%% The next one is an optimisation(优化)
		%% 没有消费者来取消息的情况(discard:抛弃)
		%% 当前消息没有发送到对应的消费者，同时当前队列中设置的消息过期时间为0，同时重新发送的exchange交换机为undefined，则立刻将该消息丢弃掉
		{undelivered, State3 = #q{ttl = 0, dlx = undefined,
								  backing_queue_state = BQS2,
								  msg_id_to_channel   = MTC}} ->
			%% 直接将消息丢弃掉，如果需要confirm的消息则立刻通知rabbit_channel进程进行confirm操作
			{BQS3, MTC1} = discard(Delivery, BQ, BQS2, MTC),
			State3#q{backing_queue_state = BQS3, msg_id_to_channel = MTC1};
		%% 没有消费者来取消息的情况
		{undelivered, State3 = #q{backing_queue_state = BQS2}} ->
			%% 将消息发布到backing_queue中
			BQS3 = BQ:publish(Message, Props, Delivered, SenderPid, Flow, BQS2),
			%% 判断当前队列中的消息数量超过上限或者消息的占的空间大小超过上限
			{Dropped, State4 = #q{backing_queue_state = BQS4}} =
				maybe_drop_head(State3#q{backing_queue_state = BQS3}),
			%% 得到当前队列中的消息数量
			QLen = BQ:len(BQS4),
			%% optimisation(优化): it would be perfectly safe to always
			%% invoke drop_expired_msgs here, but that is expensive so
			%% we only do that if a new message that might have an
			%% expiry ends up at the head of the queue. If the head
			%% remains unchanged, or if the newly published message
			%% has no expiry and becomes the head of the queue then
			%% the call is unnecessary.
			case {Dropped, QLen =:= 1, Props#message_properties.expiry} of
				%% 该情况是头部没有变化，同时消息队列消息树立不为一，则不管当前加入的消息是否设置有超时时间，都不执行drop_expired_msgs函数
				{false, false,         _} -> State4;
				%% 有丢弃消息，同时当前队列中只有当前这个新的消息，同时消息自己的特性过期时间没有定义，则不检查消息过期
				%% 此时消息的头部有变化，但是消息队列中只有一个消息，该消息还没有设置超时时间，则不执行drop_expired_msgs函数
				{true,  true,  undefined} -> State4;
				%% 当向队列中插入消息后需要做检查消息过期，同时设置定时器的操作只有三种情况
				%% 1.当消息头部根据队列上限有变化，同时消息插入后当前队列消息数量为一，且该消息设置有过期时间，则需要做一次操作(该情况是消息头部有删除消息，都会进行一次消息过期检查)
				%% 2.当消息头部根据队列上限有变化，同时消息插入后当前队列消息数量不为一，且该消息设置有过期时间，则需要做一次操作(该情况是消息头部有删除消息，都会进行一次消息过期检查)
				%% 3.当消息头部根据队列上限没有变化，同时消息插入后当前队列消息数量为一，不管消息有没有过期时间，都要做一次操作(该情况下是当前队列进入第一条消息)
				%% 最重要的是只要消息队列的头部消息有变化，则立刻执行drop_expired_msgs函数，将队列头部超时的消息删除掉
				{_,     _,             _} -> drop_expired_msgs(State4)
			end
	end.


%% 判断当前队列中的消息数量超过上限或者消息的占的空间大小超过上限，如果有查过上限，则将删除第一个元素，然后继续判断是否超过上限，只当当前队列中的消息没有超过上限
maybe_drop_head(State = #q{max_length = undefined,
						   max_bytes  = undefined}) ->
	{false, State};

maybe_drop_head(State) ->
	maybe_drop_head(false, State).


maybe_drop_head(AlreadyDropped, State = #q{backing_queue       = BQ,
										   backing_queue_state = BQS}) ->
	%% 判断当前队列中的消息数量超过上限或者消息的占的空间大小超过上限
	case over_max_length(State) of
		true ->
			maybe_drop_head(true,
							with_dlx(
							  State#q.dlx,
							  %% 如果当前队列配置有重新发送的exchange交换机，则从当前队列中取出一个消息将该消息发布到配置的重新发送的exchange交换机上
							  fun (X) -> dead_letter_maxlen_msg(X, State) end,
							  %% 如果当前队列没有配置有重新发送的exchange交换机，则将当前队列中的头部消息丢弃掉
							  fun () ->
									   {_, BQS1} = BQ:drop(false, BQS),
									   State#q{backing_queue_state = BQS1}
							  end));
		false ->
			{AlreadyDropped, State}
	end.


%% 判断当前队列中的消息数量超过上限或者消息的占的空间大小超过上限
over_max_length(#q{max_length          = MaxLen,
				   max_bytes           = MaxBytes,
				   backing_queue       = BQ,
				   backing_queue_state = BQS}) ->
	BQ:len(BQS) > MaxLen orelse BQ:info(message_bytes_ready, BQS) > MaxBytes.


%% 将AckTags重新放入到队列中，然后对队列进行重新排序，调用该接口的情况：
%% 1.requeue操作
%% 2.rabbit_channel进程挂掉，然后将还没有ack的消息重新放入到队列中
requeue_and_run(AckTags, State = #q{backing_queue       = BQ,
									backing_queue_state = BQS}) ->
	%% 判断当前队列是否为空
	WasEmpty = BQ:is_empty(BQS),
	%% 让backing_queue将AckTags重新放入到队列后，重新排序
	{_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
	%% 判断当前队列中的消息数量超过上限或者消息的占的空间大小超过上限，如果有查过上限，则将删除第一个元素，然后继续判断是否超过上限，只当当前队列中的消息没有超过上限
	{_Dropped, State1} = maybe_drop_head(State#q{backing_queue_state = BQS1}),
	%% 先将队列中过期的消息删除到没有过期的消息，然后队列中的消息为空后，将当前队列中credit中的mode为drain的消费者取出来，通知这些消费者队列中的消息为空
	run_message_queue(maybe_send_drained(WasEmpty, drop_expired_msgs(State1))).


%% 在队列进程中，自己读取消息的接口，执行该函数的情况：
%% 1.客户端单独发送消息来从消息队列中来获取一个消息
%% 2.消息队列自动通过该接口将消息发送到对应的消费者
%% 3.当消息队列中的消息数量或者消息的大小超过了设置的上限，同时设置了死亡消息转发的exchange，则调用该接口，将消息取出来转发，如果没有设置转发exchange，则将消息丢弃掉
fetch(AckRequired, State = #q{backing_queue       = BQ,
							  backing_queue_state = BQS}) ->
	%% 从backing_queue模块中读取一个消息
	{Result, BQS1} = BQ:fetch(AckRequired, BQS),
	%% 删除队列中过期的消息(该删除从队列头部开始，如果当下一个消息没有过期，则停止删除过期消息)
	State1 = drop_expired_msgs(State#q{backing_queue_state = BQS1}),
	%% 队列中的消息为空后，将当前队列中credit中的mode为drain的消费者取出来，通知这些消费者队列中的消息为空
	{Result, maybe_send_drained(Result =:= empty, State1)}.


%% 实际处理消费者ack操作的函数，调用该接口德尔情况：
%% 1.客户端发送ack消息来进行确认
%% 2.客户端拒绝消息，但是不将消息重新放入队列
%% 3.超过上限的消息在丢弃的时候，需要进行ack操作
ack(AckTags, ChPid, State) ->
	subtract_acks(ChPid, AckTags, State,
				  fun (State1 = #q{backing_queue       = BQ,
								   backing_queue_state = BQS}) ->
						   {_Guids, BQS1} = BQ:ack(AckTags, BQS),
						   State1#q{backing_queue_state = BQS1}
				  end).


%% AckTags这些消息重新放入到队列中，然后对队列进行重新排序(该操作是客户端发送消息拒绝接受需要ack的消息，则将这些还没有ack的消息重新放入到队列中)
%% 1.客户端直接通知将消息重新放入到队列中
%% 2.客户端拒绝消息，但是需要将消息重新放入到队列中
requeue(AckTags, ChPid, State) ->
	subtract_acks(ChPid, AckTags, State,
				  fun (State1) -> requeue_and_run(AckTags, State1) end).


%% 执行完Update函数后，有可能有锁住状态的消费者被激活，如果有被激活的消费者，则将队列中的消息按顺序发送给消费者
possibly_unblock(Update, ChPid, State = #q{consumers = Consumers}) ->
	case rabbit_queue_consumers:possibly_unblock(Update, ChPid, Consumers) of
		%% 如果没有消费者被解锁，则什么都不做
		unchanged               -> State;
		%% 如果有消费者被解锁，将消息一条条的下发给没有锁住的消费者
		{unblocked, Consumers1} -> State1 = State#q{consumers = Consumers1},
								   run_message_queue(true, State1)
	end.


%% auto_delete为false，则不自动删除队列
should_auto_delete(#q{q = #amqqueue{auto_delete = false}}) -> false;

%% 如果有消费者，也不自动删除队列
should_auto_delete(#q{has_had_consumers = false}) -> false;

%% 如果auto_delete为true，同时没有消费者了，则将当前队列删除
should_auto_delete(State) -> is_unused(State).


%% 处理rabbit_channel进程down掉的消息
handle_ch_down(DownPid, State = #q{consumers          = Consumers,
								   exclusive_consumer = Holder,
								   senders            = Senders}) ->
	%% 取消对rabbit_channel进程的监控
	State1 = State#q{senders = case pmon:is_monitored(DownPid, Senders) of
								   false -> Senders;
								   true  -> %% 先清除掉对应rabbit_channel进程流量控制信息
									   		credit_flow:peer_down(DownPid),
											%% 然后解除对rabbit_channel进程的监控
											pmon:demonitor(DownPid, Senders)
							   end},
	%% 删除ChPid对应的cr数据结构
	case rabbit_queue_consumers:erase_ch(DownPid, Consumers) of
		not_found ->
			{ok, State1};
		{ChAckTags, ChCTags, Consumers1} ->
			QName = qname(State1),
			%% 将消费者被删除的信息发布到rabbit_event
			[emit_consumer_deleted(DownPid, CTag, QName) || CTag <- ChCTags],
			Holder1 = case Holder of
						  {DownPid, _} -> none;
						  Other        -> Other
					  end,
			State2 = State1#q{consumers          = Consumers1,
							  exclusive_consumer = Holder1},
			%% 回调消息队列的修饰模块
			notify_decorators(State2),
			%% 判断当前队列是否需要自动删除(当前自动删除的条件是auto_delete字段为true，同时当前队列没有消费者的存在)
			case should_auto_delete(State2) of
				true  -> {stop, State2};
				%% rabbit_channel进程down掉后，将还没有ack的消息重新放入到队列中，等待后续消费者的消费
				false -> {ok, requeue_and_run(ChAckTags,
											  %% rabbit_channel进程终止，则重新启动队列未操作删除队列自己的定时器
											  ensure_expiry_timer(State2))}
			end
	end.


%% 检查当前队列的独有消费者的正确性
check_exclusive_access({_ChPid, _ConsumerTag}, _ExclusiveConsume, _State) ->
	in_use;

check_exclusive_access(none, false, _State) ->
	ok;

check_exclusive_access(none, true, State) ->
	case is_unused(State) of
		true  -> ok;
		false -> in_use
	end.


%% 判断当前队列是否存在消费者
is_unused(_State) -> rabbit_queue_consumers:count() == 0.


maybe_send_reply(_ChPid, undefined) -> ok;

maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).


%% 取得当前队列的队列名字
qname(#q{q = #amqqueue{name = QName}}) -> QName.


%% 通知backing_queue进行confirm同步操作
backing_queue_timeout(State = #q{backing_queue       = BQ,
								 backing_queue_state = BQS}) ->
	State#q{backing_queue_state = BQ:timeout(BQS)}.


%% 从消费者状态中减去消费者ack过来的消息
subtract_acks(ChPid, AckTags, State = #q{consumers = Consumers}, Fun) ->
	%% 从rabbit_channel进程对应的数据结构中删除已经Ack的标志
	case rabbit_queue_consumers:subtract_acks(ChPid, AckTags, Consumers) of
		not_found               -> State;
		%% 如果没有变化，则只执行Fun函数
		unchanged               -> Fun(State);
		%% 如果空闲消费者有增加，则更新最新的消费者数据，同时向消费者发布消息，直到消息为0或者全部的消费者都已经锁住才停止
		{unblocked, Consumers1} -> State1 = State#q{consumers = Consumers1},
								   run_message_queue(true, Fun(State1))
	end.


%% 得到消息特性特性数据结构
message_properties(Message = #basic_message{content = Content},
				   Confirm, #q{ttl = TTL}) ->
	#content{payload_fragments_rev = PFR} = Content,
	#message_properties{expiry           = calculate_msg_expiry(Message, TTL),
						%% 如果消息需要持久化，则到达存储位置还需要进行confirm操作，如果消息是非持久化的，同时需要confirm，则立刻通知rabbit_channel进程进行confirm操作
						needs_confirming = Confirm == eventually,
						size             = iolist_size(PFR)}.


%% 根据队列中设置的消息过期时间，以及消息特性中设置的过期时间得到小的过期时间作为当前消息的过期时间
calculate_msg_expiry(#basic_message{content = Content}, TTL) ->
	#content{properties = Props} =
				rabbit_binary_parser:ensure_content_decoded(Content),
	%% We assert that the expiration must be valid - we check in the channel.
	%% 拿到P_basic结构中expiration过期的整数时间
	{ok, MsgTTL} = rabbit_basic:parse_expiration(Props),
	%% 根据队列中设置的消息过期时间，以及消息特性中设置的过期时间得到小的过期时间作为当前消息的过期时间
	case lists:min([TTL, MsgTTL]) of
		undefined -> undefined;
		T         -> now_micros() + T * 1000
	end.

%% Logically this function should invoke maybe_send_drained/2.
%% However, that is expensive. Since some frequent callers of
%% drop_expired_msgs/1, in particular deliver_or_enqueue/3, cannot
%% possibly cause the queue to become empty, we push the
%% responsibility to the callers. So be cautious(谨慎的) when adding new ones.
%% 删除队列中过期的消息(该删除从队列头部开始，如果当下一个消息没有过期，则停止删除过期消息)
drop_expired_msgs(State) ->
	case is_empty(State) of
		true  -> State;
		false -> drop_expired_msgs(now_micros(), State)
	end.


%% 实际的删除过期消息的接口
drop_expired_msgs(Now, State = #q{backing_queue_state = BQS,
								  backing_queue       = BQ }) ->
	%% 根据消息本身发布过来的过期时间参数，来将过期的消息删除掉
	ExpirePred = fun (#message_properties{expiry = Exp}) -> Now >= Exp end,
	%% Props是消息队列中还没有过期的消息特性
	{Props, State1} =
		with_dlx(
		  State#q.dlx,
		  fun (X) -> dead_letter_expired_msgs(ExpirePred, X, State) end,
		  %% 如果没有设置死亡消息重新发送的exchange交换机，则将该消息取出来直接扔掉
		  fun () -> {Next, BQS1} = BQ:dropwhile(ExpirePred, BQS),
					{Next, State#q{backing_queue_state = BQS1}} end),
	%% 将最后一个没有过期的消息的过期时间取出来
	ensure_ttl_timer(case Props of
						 %% 如果最后取出来的消息没有定义超时时间，则不启动到时间删除消息的定时器
						 undefined                         -> undefined;
						 #message_properties{expiry = Exp} -> Exp
					 end, State1).


%% 根据参数配置是否有exchange交换机做不通的处理
with_dlx(undefined, _With,  Without) -> Without();

with_dlx(DLX,        With,  Without) -> case rabbit_exchange:lookup(DLX) of
											{ok, X}            -> With(X);
											{error, not_found} -> Without()
										end.


%% 处理消息过期的死亡消息(彻底从消息队列中删除掉，同时根据参数配置的路由规则将该消息发布到参数配置的exchange交换机上)
%% 会从消息队列的头部开始检查，如果消息过期则从队列中删除掉，然后继续检查队列后面的函数，直到消息的过期时间没有到期停止
dead_letter_expired_msgs(ExpirePred, X, State = #q{backing_queue = BQ}) ->
	dead_letter_msgs(fun (DLFun, Acc, BQS1) ->
							  BQ:fetchwhile(ExpirePred, DLFun, Acc, BQS1)
					 end, expired, X, State).


%% 消费者拒绝接受的消息，同时配置为不能重新放入到队列中，则将该死亡消息根据参数配置的路由规则将该消息发布到参数配置的exchange交换机上
%% 将要拒绝的消息全部从消息队列中删除
dead_letter_rejected_msgs(AckTags, X,  State = #q{backing_queue = BQ}) ->
	{ok, State1} =
		dead_letter_msgs(
		  fun (DLFun, Acc, BQS) ->
				   {Acc1, BQS1} = BQ:ackfold(DLFun, Acc, BQS, AckTags),
				   {ok, Acc1, BQS1}
		  end, rejected, X, State),
	State1.


%% 队列中的消息数量或者消息战友空间超过参数配置的上限，则将该类型的死亡消息根据参数配置的路由规则将该消息发布到参数配置的exchange交换机上
%% 只是将消息队列的头部的消息删除掉
dead_letter_maxlen_msg(X, State = #q{backing_queue = BQ}) ->
    {ok, State1} =
        dead_letter_msgs(
          fun (DLFun, Acc, BQS) ->
                  {{Msg, _, AckTag}, BQS1} = BQ:fetch(true, BQS),
                  {ok, DLFun(Msg, AckTag, Acc), BQS1}
          end, maxlen, X, State),
    State1.


%% 处理死亡消息
dead_letter_msgs(Fun, Reason, X, State = #q{dlx_routing_key     = RK,
											backing_queue_state = BQS,
											backing_queue       = BQ}) ->
	QName = qname(State),
	{Res, Acks1, BQS1} =
		Fun(fun (Msg, AckTag, Acks) ->
					 %% 根据参数设置的路由规则将死亡的消息重新发布到参数设置的exchange交换机上
					 rabbit_dead_letter:publish(Msg, Reason, X, RK, QName),
					 [AckTag | Acks]
			end, [], BQS),
	%% 将所有死亡的消息进行ack操作，彻底从队列中删除掉
	{_Guids, BQS2} = BQ:ack(Acks1, BQS1),
	{Res, State#q{backing_queue_state = BQS2}}.


stop(State) -> stop(noreply, State).


%% 没有返回值的停止队列的接口
stop(noreply, State) -> {stop, normal, State};

%% 有返回值的定制队列的接口
stop(Reply,   State) -> {stop, normal, Reply, State}.


%% 得到当前时间距离{0, 0, 0}的微秒数
now_micros() -> timer:now_diff(now(), {0, 0, 0}).


%% 根据关键key列表Items获取消息队列的关键信息
infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].


%% 获取消息队列的名字
i(name,        #q{q = #amqqueue{name        = Name}})       -> Name;

%% 获取消息队列是否是持久化队列
i(durable,     #q{q = #amqqueue{durable     = Durable}})    -> Durable;

%% 获取消息队列是否是自动删除队列
i(auto_delete, #q{q = #amqqueue{auto_delete = AutoDelete}}) -> AutoDelete;

%% 获取消息队列的参数
i(arguments,   #q{q = #amqqueue{arguments   = Arguments}})  -> Arguments;

%% 获取消息队列的进程Pid
i(pid, _) ->
	self();

%% 获取消息队列独有消费者信息
i(owner_pid, #q{q = #amqqueue{exclusive_owner = none}}) ->
	'';

%% 获取消息队列独有消费者信息
i(owner_pid, #q{q = #amqqueue{exclusive_owner = ExclusiveOwner}}) ->
	ExclusiveOwner;

%% 获取消息队列在rabbit_policy中的策略
i(policy,    #q{q = Q}) ->
	case rabbit_policy:name(Q) of
		none   -> '';
		Policy -> Policy
	end;

%% 获取消息队列独有的消费者对应的rabbit_channel的进程Pid
i(exclusive_consumer_pid, #q{exclusive_consumer = none}) ->
	'';

%% 获取消息队列独有的消费者对应的rabbit_channel的进程Pid
i(exclusive_consumer_pid, #q{exclusive_consumer = {ChPid, _ConsumerTag}}) ->
	ChPid;

%% 获取消息队列独有的消费者对应的消费者标识
i(exclusive_consumer_tag, #q{exclusive_consumer = none}) ->
	'';

%% 获取消息队列独有的消费者对应的消费者标识
i(exclusive_consumer_tag, #q{exclusive_consumer = {_ChPid, ConsumerTag}}) ->
	ConsumerTag;

%% 获取消息队列当前消息的数量
i(messages_ready, #q{backing_queue_state = BQS, backing_queue = BQ}) ->
	BQ:len(BQS);

%% 获得消息队列当前还没有得到ack的消息数量
i(messages_unacknowledged, _) ->
	rabbit_queue_consumers:unacknowledged_message_count();

%% 获得消息队列当前消息的数量和当前消息队列还没有ack的消息数量
i(messages, State) ->
	lists:sum([i(Item, State) || Item <- [messages_ready,
										  messages_unacknowledged]]);

%% 获得消息队列消费者的数量
i(consumers, _) ->
	rabbit_queue_consumers:count();

%% utilisation：利用
i(consumer_utilisation, #q{consumers = Consumers}) ->
	case rabbit_queue_consumers:count() of
		0 -> '';
		_ -> rabbit_queue_consumers:utilisation(Consumers)
	end;

%% 获得消息队列当前占用的内存大小
i(memory, _) ->
	{memory, M} = process_info(self(), memory),
	M;

%% 获得消息队列的镜像队列的Pid列表
i(slave_pids, #q{q = #amqqueue{name = Name}}) ->
	{ok, Q = #amqqueue{slave_pids = SPids}} =
		rabbit_amqqueue:lookup(Name),
	case rabbit_mirror_queue_misc:is_mirrored(Q) of
		false -> '';
		true  -> SPids
	end;

%% 高可用队列相关
i(synchronised_slave_pids, #q{q = #amqqueue{name = Name}}) ->
	{ok, Q = #amqqueue{sync_slave_pids = SSPids}} =
		rabbit_amqqueue:lookup(Name),
	case rabbit_mirror_queue_misc:is_mirrored(Q) of
		false -> '';
		true  -> SSPids
	end;

%% 高可用队列相关
i(recoverable_slaves, #q{q = #amqqueue{name    = Name,
									   durable = Durable}}) ->
	{ok, Q = #amqqueue{recoverable_slaves = Nodes}} =
		rabbit_amqqueue:lookup(Name),
	case Durable andalso rabbit_mirror_queue_misc:is_mirrored(Q) of
		false -> '';
		true  -> Nodes
	end;

%% 获得消息队列当前的状态
i(state, #q{status = running}) -> credit_flow:state();

%% 获得消息队列当前的状态
i(state, #q{status = State})   -> State;

%% 从backing_queue模块中获取Item对应的关键信息
i(Item, #q{backing_queue_state = BQS, backing_queue = BQ}) ->
	BQ:info(Item, BQS).


%% 向rabbit_event事件中心发布当前消息队列的详细信息
emit_stats(State) ->
	emit_stats(State, []).


%% 向rabbit_event事件中心发布当前消息队列的详细信息
emit_stats(State, Extra) ->
	ExtraKs = [K || {K, _} <- Extra],
	Infos = [{K, V} || {K, V} <- infos(statistics_keys(), State),
					   not lists:member(K, ExtraKs)],
	rabbit_event:notify(queue_stats, Extra ++ Infos).


%% 通知rabbit_event当前队列有新的消费者创建
emit_consumer_created(ChPid, CTag, Exclusive, AckRequired, QName,
					  PrefetchCount, Args, Ref) ->
	rabbit_event:notify(consumer_created,
						[{consumer_tag,   CTag},
						 {exclusive,      Exclusive},
						 {ack_required,   AckRequired},
						 {channel,        ChPid},
						 {queue,          QName},
						 {prefetch_count, PrefetchCount},
						 {arguments,      Args}],
						Ref).


%% 将消费者被删除的信息发布到rabbit_event
emit_consumer_deleted(ChPid, ConsumerTag, QName) ->
	rabbit_event:notify(consumer_deleted,
						[{consumer_tag, ConsumerTag},
						 {channel,      ChPid},
						 {queue,        QName}]).

%%----------------------------------------------------------------------------

prioritise_call(Msg, _From, _Len, State) ->
	case Msg of
		info                                       -> 9;
		{info, _Items}                             -> 9;
		consumers                                  -> 9;
		stat                                       -> 7;
		{basic_consume, _, _, _, _, _, _, _, _, _} -> consumer_bias(State);
		{basic_cancel, _, _, _}                    -> consumer_bias(State);
		_                                          -> 0
	end.


prioritise_cast(Msg, _Len, State) ->
	case Msg of
		delete_immediately                   -> 8;
		{set_ram_duration_target, _Duration} -> 8;
		{set_maximum_since_use, _Age}        -> 8;
		{run_backing_queue, _Mod, _Fun}      -> 6;
		{ack, _AckTags, _ChPid}              -> 3; %% [1]
		{resume, _ChPid}                     -> 2;
		{notify_sent, _ChPid, _Credit}       -> consumer_bias(State);
		_                                    -> 0
	end.

%% [1] It should be safe to always prioritise ack / resume since they
%% will be rate limited by how fast consumers receive messages -
%% i.e. by notify_sent. We prioritise ack and resume to discourage
%% starvation caused by prioritising notify_sent. We don't vary their
%% prioritiy since acks should stay in order (some parts of the queue
%% stack are optimised for that) and to make things easier to reason
%% about. Finally, we prioritise ack over resume since it should
%% always reduce memory use.

consumer_bias(#q{backing_queue = BQ, backing_queue_state = BQS}) ->
	case BQ:msg_rates(BQS) of
		{0.0,          _} -> 0;
		{Ingress, Egress} when Egress / Ingress < ?CONSUMER_BIAS_RATIO -> 1;
		{_,            _} -> 0
	end.


prioritise_info(Msg, _Len, #q{q = #amqqueue{exclusive_owner = DownPid}}) ->
	case Msg of
		{'DOWN', _, process, DownPid, _}     -> 8;
		update_ram_duration                  -> 8;
		{maybe_expire, _Version}             -> 8;
		{drop_expired, _Version}             -> 8;
		emit_stats                           -> 7;
		sync_timeout                         -> 6;
		_                                    -> 0
	end.


%% 同步处理发送过来让队列进程初始化的消息
handle_call({init, Recover}, From, State) ->
	init_it(Recover, From, State);

%% 处理同步获取消息队列关键信息的消息
handle_call(info, _From, State) ->
	reply(infos(info_keys(), State), State);

%% 处理获取Items列表中关键key对应的消息队列信息
handle_call({info, Items}, _From, State) ->
	try
		reply({ok, infos(Items, State)}, State)
	catch Error -> reply({error, Error}, State)
	end;

%% 处理获取当前队列所有消费者的消息
handle_call(consumers, _From, State = #q{consumers = Consumers}) ->
	reply(rabbit_queue_consumers:all(Consumers), State);

%% 处理ChPid对应的rabbit_channel进程down掉的消息(消息队列正常停止，则当前消息队列会从RabbitMQ系统中完全清除掉)
handle_call({notify_down, ChPid}, _From, State) ->
	%% we want to do this synchronously, so that auto_deleted queues
	%% are no longer visible by the time we send a response to the
	%% client.  The queue is ultimately deleted in terminate/2; if we
	%% return stop with a reply, terminate/2 will be called by
	%% gen_server2 *before* the reply is sent.
	case handle_ch_down(ChPid, State) of
		{ok, State1}   -> reply(ok, State1);
		%% 让消息队列正常停止，则当前消息队列会从RabbitMQ系统中完全清除掉
		{stop, State1} -> stop(ok, State1)
	end;

%% 处理从队列中读取消息的接口
handle_call({basic_get, ChPid, NoAck, LimiterPid}, _From,
			State = #q{q = #amqqueue{name = QName}}) ->
	AckRequired = not NoAck,
	%% 当前队列处理了basic_get操作，则重新启动队列未操作删除队列自己的定时器
	State1 = ensure_expiry_timer(State),
	%% 从队列中读取消息
	case fetch(AckRequired, State1) of
		{empty, State2} ->
			reply(empty, State2);
		{{Message, IsDelivered, AckTag},
		 #q{backing_queue = BQ, backing_queue_state = BQS} = State2} ->
			case AckRequired of
				%% 需要等待消费者的ack操作，则记录该信息
				true  -> ok = rabbit_queue_consumers:record_ack(
								ChPid, LimiterPid, AckTag);
				false -> ok
			end,
			%% 组装取得的消息信息，返回给rabbit_channel进程
			Msg = {QName, self(), AckTag, IsDelivered, Message},
			%% 返回的信息加上当前队列中剩余的消息数量
			reply({ok, BQ:len(BQS), Msg}, State2)
	end;

%% 处理队列进程中增加消费者的消息
handle_call({basic_consume, NoAck, ChPid, LimiterPid, LimiterActive,
			 PrefetchCount, ConsumerTag, ExclusiveConsume, Args, OkMsg},
			_From, State = #q{consumers          = Consumers,
							  exclusive_consumer = Holder}) ->
	%% 检查当前队列的独有消费者的正确性
	case check_exclusive_access(Holder, ExclusiveConsume, State) of
		in_use -> reply({error, exclusive_consume_unavailable}, State);
		ok     -> %% 向消费者字段中添加消费者
				  Consumers1 = rabbit_queue_consumers:add(
								 ChPid, ConsumerTag, NoAck,
								 LimiterPid, LimiterActive,
								 PrefetchCount, Args, is_empty(State),
								 Consumers),
				  %% 如果当前消费者是当前消息队列的独有消费者，则记录在exclusive_consumer字段
				  ExclusiveConsumer =
					  if ExclusiveConsume -> {ChPid, ConsumerTag};
						 true             -> Holder
					  end,
				  %% 消费者添加成功后，更新队列中的consumers字段
				  State1 = State#q{consumers          = Consumers1,
								   has_had_consumers  = true,
								   exclusive_consumer = ExclusiveConsumer},
				  %% 将消费者成功加入到队列中后，如果需要通知客户端成功的消息，则通过rabbit_channel进程再通过rabbit_writer进程通知客户端
				  ok = maybe_send_reply(ChPid, OkMsg),
				  %% 通知rabbit_event当前队列有新的消费者创建
				  emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
										not NoAck, qname(State1),
										PrefetchCount, Args, none),
				  %% 回调消息队列的修饰模块
				  notify_decorators(State1),
				  %% 当前队列有没有被锁住的消费者，同时队列中还有消息，则将消息一条条的下发给没有锁住的消费者
				  reply(ok, run_message_queue(State1))
	end;

%% 同步处理删除消费者的消息
handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg}, _From,
			State = #q{consumers          = Consumers,
					   exclusive_consumer = Holder}) ->
	%% 如果需要向客户端发送回复消息，则先通知客户端
	ok = maybe_send_reply(ChPid, OkMsg),
	%% 从消费者模块根据消费者标识CTag删除消费者
	case rabbit_queue_consumers:remove(ChPid, ConsumerTag, Consumers) of
		not_found ->
			reply(ok, State);
		Consumers1 ->
			%% 看看删除的消费者是否是消息队列的独有消费者
			Holder1 = case Holder of
						  {ChPid, ConsumerTag} -> none;
						  _                    -> Holder
					  end,
			State1 = State#q{consumers          = Consumers1,
							 exclusive_consumer = Holder1},
			%% 向rabbit_event发布消费者被删除的信息
			emit_consumer_deleted(ChPid, ConsumerTag, qname(State1)),
			%% 消息队列的修饰模块的回调
			notify_decorators(State1),
			%% 如果当前队列需要自动删除，同时当前队列中没有消费者了，则直接将该队列删除掉
			case should_auto_delete(State1) of
				%% 删除消费者后，队列不删除自己后，则重新启动队列未操作删除队列自己的定时器
				false -> reply(ok, ensure_expiry_timer(State1));
				true  -> stop(ok, State1)
			end
	end;

%% 拿到当前队列中消息的数量以及消费者的数量
handle_call(stat, _From, State) ->
	%% 处理拿当前队列信息的消息后，则重新启动队列未操作删除队列自己的定时器
	State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
				   ensure_expiry_timer(State),
	reply({ok, BQ:len(BQS), rabbit_queue_consumers:count()}, State1);

%% 同步处理删除队列的消息
%% IfEmpty为true表示只有队列为空的时候才能够进行删除队列的操作
%% IfUnused为true表示队列中消费者为0的时候才能够进行删除队列的操作
handle_call({delete, IfUnused, IfEmpty}, _From,
			State = #q{backing_queue_state = BQS, backing_queue = BQ}) ->
	IsEmpty  = BQ:is_empty(BQS),
	IsUnused = is_unused(State),
	if
		%% IfEmpty为true表示只有队列为空的时候才能够进行删除队列的操作
		IfEmpty  and not(IsEmpty)  -> reply({error, not_empty}, State);
		%% IfUnused为true表示队列中消费者为0的时候才能够进行删除队列的操作
		IfUnused and not(IsUnused) -> reply({error,    in_use}, State);
		%% IfUnused和IfEmpty都为false，则立刻进行删除操作
		true                       -> stop({ok, BQ:len(BQS)}, State)
	end;

%% 同步处理将队列中的所有消息清除掉的消息
handle_call(purge, _From, State = #q{backing_queue       = BQ,
									 backing_queue_state = BQS}) ->
	%% backing_queue进行清除队列中所有消息
	{Count, BQS1} = BQ:purge(BQS),
	State1 = State#q{backing_queue_state = BQS1},
	reply({ok, Count}, maybe_send_drained(Count =:= 0, State1));

%% 同步处理将AckTags重新放入到队列后，对队列重新排序的消息
handle_call({requeue, AckTags, ChPid}, From, State) ->
	%% 该操作比较耗时，则先通知成功
	gen_server2:reply(From, ok),
	%% 通知ok后，然后让队列进程backing_queue执行重新排序的操作
	noreply(requeue(AckTags, ChPid, State));

%% 处理同步镜像队列消息的消息(只有当前队列的backing_queue模块名字为rabbit_mirror_queue_master才会进行镜像队列的消息同步)
handle_call(sync_mirrors, _From,
			State = #q{backing_queue       = rabbit_mirror_queue_master,
					   backing_queue_state = BQS}) ->
	S = fun(BQSN) -> State#q{backing_queue_state = BQSN} end,
	%% 处理接收消息的函数
	HandleInfo = fun (Status) ->
						  receive {'$gen_call', From, {info, Items}} ->
									  Infos = infos(Items, State#q{status = Status}),
									  gen_server2:reply(From, {ok, Infos})
							  after 0 ->
								  ok
						  end
				 end,
	%% 向rabbit_event事件中心发布当前消息队列的详细信息的函数
	EmitStats = fun (Status) ->
						 rabbit_event:if_enabled(
						   State, #q.stats_timer,
						   fun() -> emit_stats(State#q{status = Status}) end)
				end,
	case rabbit_mirror_queue_master:sync_mirrors(HandleInfo, EmitStats, BQS) of
		{ok, BQS1}           -> reply(ok, S(BQS1));
		{stop, Reason, BQS1} -> {stop, Reason, S(BQS1)}
	end;

%% 处理同步镜像队列消息的消息
handle_call(sync_mirrors, _From, State) ->
	reply({error, not_mirrored}, State);

%% By definition if we get this message here we do not have to do anything.
%% 高可用队列相关
handle_call(cancel_sync_mirrors, _From, State) ->
	reply({ok, not_syncing}, State).


%% 通知队列进程进行相关的初始化操作
handle_cast(init, State) ->
	init_it({no_barrier, non_clean_shutdown}, none, State);

%% 处理run_backing_queue消息，让backing_queue执行Mod，Fun函数
handle_cast({run_backing_queue, Mod, Fun},
			State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
	noreply(State#q{backing_queue_state = BQ:invoke(Mod, Fun, BQS)});

%% 有新的消息传递到本队列进程(客户端把消息发送到本队列进程里)
handle_cast({deliver, Delivery = #delivery{sender = Sender,
										   flow   = Flow}, SlaveWhenPublished},
			State = #q{senders = Senders}) ->
	Senders1 = case Flow of
				   %% RabbitMQ系统进程间消息流控制
				   flow   -> credit_flow:ack(Sender),
							 case SlaveWhenPublished of
								 true  -> credit_flow:ack(Sender); %% [0]
								 false -> ok
							 end,
							 %% 监视发送进程rabbit_channel进程
							 pmon:monitor(Sender, Senders);
				   noflow -> Senders
			   end,
	%% 更新当前队列进程的senders字段(有流量监控的则当前队列进程会监视发送者rabbit_channel进程)
	State1 = State#q{senders = Senders1},
	%% deliver_or_enqueue实际的传递函数，如果当前队列中有能够接收消息的消费者，则将消息发送给消费者，如果没有则直接放入到队列中，等待消费者来消费
	noreply(deliver_or_enqueue(Delivery, SlaveWhenPublished, State1));

%% [0] The second ack is since the channel thought we were a slave at
%% the time it published this message, so it used two credits (see
%% rabbit_amqqueue:deliver/2).
%% 处理rabbit_channel进程发送过来的处理消费者进行ack的消息
handle_cast({ack, AckTags, ChPid}, State) ->
	noreply(ack(AckTags, ChPid, State));

%% 表示客户端拒绝的消息可以被重新分配
handle_cast({reject, true,  AckTags, ChPid}, State) ->
	noreply(requeue(AckTags, ChPid, State));

%% 表示客户端拒绝的消息不能够被重新分配
handle_cast({reject, false, AckTags, ChPid}, State) ->
	noreply(with_dlx(
			  State#q.dlx,
			  %% 如果配置有重新将消息分配的exchange交换机，则将该消息重新发送到exchange中
			  fun (X) -> subtract_acks(ChPid, AckTags, State,
									   fun (State1) ->
												%% 消费者拒绝接受的消息，同时配置为不能重新放入到队列中，则将该死亡消息根据参数配置的路由规则将该消息发布到参数配置的exchange交换机上
												dead_letter_rejected_msgs(
												  AckTags, X, State1)
									   end) end,
			  %% 如果没有配置重新将消息分配的exchange交换机，则直接将消息ack后丢弃掉
			  fun () -> ack(AckTags, ChPid, State) end));

handle_cast(delete_immediately, State) ->
	stop(State);

%% rabbit_limiter进程通知队列进程重新开始，可以向消费者发送消息
handle_cast({resume, ChPid}, State) ->
	noreply(possibly_unblock(rabbit_queue_consumers:resume_fun(),
							 ChPid, State));

%% rabbit_writer进程通知队列进程unsent_message_count字段增加Credit个流量
handle_cast({notify_sent, ChPid, Credit}, State) ->
	noreply(possibly_unblock(rabbit_queue_consumers:notify_sent_fun(Credit),
							 ChPid, State));

%% 通知队列进程ChPid对应的rabbit_limiter进程处于激活状态
handle_cast({activate_limit, ChPid}, State) ->
	noreply(possibly_unblock(rabbit_queue_consumers:activate_limit_fun(),
							 ChPid, State));

handle_cast({set_ram_duration_target, Duration},
			State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
	BQS1 = BQ:set_ram_duration_target(Duration, BQS),
	noreply(State#q{backing_queue_state = BQS1});

handle_cast({set_maximum_since_use, Age}, State) ->
	ok = file_handle_cache:set_maximum_since_use(Age),
	noreply(State);

handle_cast(start_mirroring, State = #q{backing_queue       = BQ,
										backing_queue_state = BQS}) ->
	%% lookup again to get policy for init_with_existing_bq
	{ok, Q} = rabbit_amqqueue:lookup(qname(State)),
	true = BQ =/= rabbit_mirror_queue_master, %% assertion
	BQ1 = rabbit_mirror_queue_master,
	BQS1 = BQ1:init_with_existing_bq(Q, BQ, BQS),
	noreply(State#q{backing_queue       = BQ1,
					backing_queue_state = BQS1});

handle_cast(stop_mirroring, State = #q{backing_queue       = BQ,
									   backing_queue_state = BQS}) ->
	BQ = rabbit_mirror_queue_master, %% assertion
	{BQ1, BQS1} = BQ:stop_mirroring(BQS),
	noreply(State#q{backing_queue       = BQ1,
					backing_queue_state = BQS1});

handle_cast({credit, ChPid, CTag, Credit, Drain},
			State = #q{consumers           = Consumers,
					   backing_queue       = BQ,
					   backing_queue_state = BQS}) ->
	%% 得到当前队列中的消息数量
	Len = BQ:len(BQS),
	%% rabbit_channel进程通过basic.credit_ok消息通知客户端当前队列中的消息数量
	rabbit_channel:send_credit_reply(ChPid, Len),
	noreply(
	  case rabbit_queue_consumers:credit(Len == 0, Credit, Drain, ChPid, CTag,
										 Consumers) of
		  unchanged               -> State;
		  %% 有新获得解锁的消费者出现
		  {unblocked, Consumers1} -> State1 = State#q{consumers = Consumers1},
									 run_message_queue(true, State1)
	  end);

%% 异步处理强制时间刷新的消息(直接忽略掉配置文件中配置的信息发布限制，将队列的创建信息，当前队列的所有消费者信息都发布到rabbit_event事件中心去)
handle_cast({force_event_refresh, Ref},
			State = #q{consumers          = Consumers,
					   exclusive_consumer = Exclusive}) ->
	%% 强制向rabbit_event发布队列被创建的信息
	rabbit_event:notify(queue_created, infos(?CREATION_EVENT_KEYS, State), Ref),
	%% 得到当前队列的名字
	QName = qname(State),
	%% 拿到当前队列的所有消费者
	AllConsumers = rabbit_queue_consumers:all(Consumers),
	case Exclusive of
		%% 通知rabbit_event当前队列有新的消费者创建
		none       -> [emit_consumer_created(
						 Ch, CTag, false, AckRequired, QName, Prefetch,
						 Args, Ref) ||
						 {Ch, CTag, AckRequired, Prefetch, Args}
							 <- AllConsumers];
		{Ch, CTag} -> [{Ch, CTag, AckRequired, Prefetch, Args}] = AllConsumers,
					  emit_consumer_created(
						Ch, CTag, true, AckRequired, QName, Prefetch, Args, Ref)
	end,
	%% 重新初始化当前队列信息发布字段信息
	noreply(rabbit_event:init_stats_timer(State, #q.stats_timer));

handle_cast(notify_decorators, State) ->
	notify_decorators(State),
	noreply(State);

%% 处理policy_changed消息，更新消息队列的最新信息，同时出发一次发布队列信息的事件
handle_cast(policy_changed, State = #q{q = #amqqueue{name = Name}}) ->
	%% We depend on the #q.q field being up to date at least WRT
	%% policy (but not slave pids) in various places, so when it
	%% changes we go and read it from Mnesia again.
	%%
	%% This also has the side effect of waking us up so we emit a
	%% stats event - so event consumers see the changed policy.
	{ok, Q} = rabbit_amqqueue:lookup(Name),
	%% 同时将消息队列的策略重新更新一次
	noreply(process_args_policy(State#q{q = Q})).


%% 处理当前队列未操作删除队列的消息(需要将参数的版本号对应上才能够做处理)
handle_info({maybe_expire, Vsn}, State = #q{args_policy_version = Vsn}) ->
	%% 判断当前队列是否存在消费者
	case is_unused(State) of
		%% 如果没有消费者，则将当前队列删除
		true  -> stop(State);
		%% 如果有消费者，则将未操作删除队列的定时器字段重置
		false -> noreply(State#q{expiry_timer_ref = undefined})
	end;

handle_info({maybe_expire, _Vsn}, State) ->
	noreply(State);

%% 处理删除队列中过期消息的消息(需要将参数的版本号对应上才能够做处理)
handle_info({drop_expired, Vsn}, State = #q{args_policy_version = Vsn}) ->
	%% 得到当前队列是否为空
	WasEmpty = is_empty(State),
	%% 删除队列中过期的消息(该删除从队列头部开始，如果当下一个消息没有过期，则停止删除过期消息)
	State1 = drop_expired_msgs(State#q{ttl_timer_ref = undefined}),
	%% 队列中的消息为空后，将当前队列中credit中的mode为drain的消费者取出来，通知这些消费者队列中的消息为空
	noreply(maybe_send_drained(WasEmpty, State1));

handle_info({drop_expired, _Vsn}, State) ->
	noreply(State);

%% 处理将消息队列的信息发送到rabbit_event事件中心的消息
handle_info(emit_stats, State) ->
	%% 向rabbit_event事件中心发布当前消息队列的详细信息
	emit_stats(State),
	%% Don't call noreply/1, we don't want to set timers
	%% 重置信息发送定时器，然后调用next_state
	{State1, Timeout} = next_state(rabbit_event:reset_stats_timer(
									 State, #q.stats_timer)),
	{noreply, State1, Timeout};

%% 处理rabbit_reader进程down的消息
handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason},
			State = #q{q = #amqqueue{exclusive_owner = DownPid}}) ->
	%% Exclusively owned queues must disappear with their owner.  In
	%% the case of clean shutdown we delete the queue synchronously in
	%% the reader - although not required by the spec this seems to
	%% match what people expect (see bug 21824). However we need this
	%% monitor-and-async- delete in case the connection goes away
	%% unexpectedly.
	%% 如果拥有者rabbit_reader进程死亡，则让队列进程正常退出，正常退出的同时会将该队列全部从当前RabbitMQ系统中清除掉
	stop(State);

%% 处理rabbit_channel进程down掉的消息
handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason}, State) ->
	%% 处理rabbit_channel进程down掉的消息
	case handle_ch_down(DownPid, State) of
		{ok, State1}   -> noreply(State1);
		{stop, State1} -> stop(State1)
	end;

%% 处理更新当前队列消息进入和出去的速率消息
handle_info(update_ram_duration, State = #q{backing_queue = BQ,
											backing_queue_state = BQS}) ->
	{RamDuration, BQS1} = BQ:ram_duration(BQS),
	DesiredDuration =
		rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
	BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
	%% Don't call noreply/1, we don't want to set timers
	{State1, Timeout} = next_state(State#q{rate_timer_ref      = undefined,
										   backing_queue_state = BQS2}),
	{noreply, State1, Timeout};

%% 处理sync_timeout消息，该消息是sync_timer_ref定时器发送给自己
handle_info(sync_timeout, State) ->
	noreply(backing_queue_timeout(State#q{sync_timer_ref = undefined}));

%% 处理timeout消息，该消息是指定时间后队列进程还没有收到消息，则会收到该消息
handle_info(timeout, State) ->
	noreply(backing_queue_timeout(State));

handle_info({'EXIT', _Pid, Reason}, State) ->
	{stop, Reason, State};

handle_info({bump_credit, Msg}, State = #q{backing_queue       = BQ,
										   backing_queue_state = BQS}) ->
	credit_flow:handle_bump_msg(Msg),
	noreply(State#q{backing_queue_state = BQ:resume(BQS)});

handle_info(Info, State) ->
	{stop, {unhandled_info, Info}, State}.


handle_pre_hibernate(State = #q{backing_queue_state = undefined}) ->
	{hibernate, State};

handle_pre_hibernate(State = #q{backing_queue = BQ,
								backing_queue_state = BQS}) ->
	{RamDuration, BQS1} = BQ:ram_duration(BQS),
	DesiredDuration =
		rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
	BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
	BQS3 = BQ:handle_pre_hibernate(BQS2),
	rabbit_event:if_enabled(
	  State, #q.stats_timer,
	  fun () -> emit_stats(State, [{idle_since,           now()},
								   {consumer_utilisation, ''}]) end),
	State1 = rabbit_event:stop_stats_timer(State#q{backing_queue_state = BQS3},
												  #q.stats_timer),
	{hibernate, stop_rate_timer(State1)}.


format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).
