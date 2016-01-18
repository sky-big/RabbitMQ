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

-module(rabbit_mirror_queue_master).

-export([init/3, terminate/2, delete_and_terminate/2,
         purge/1, purge_acks/1, publish/6, publish_delivered/5,
         discard/4, fetch/2, drop/2, ack/2, requeue/2, ackfold/4, fold/3,
         len/1, is_empty/1, depth/1, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, set_ram_duration_target/2, ram_duration/1,
         needs_timeout/1, timeout/1, handle_pre_hibernate/1, resume/1,
         msg_rates/1, info/2, invoke/3, is_duplicate/2]).

-export([start/1, stop/0, delete_crashed/1]).

-export([promote_backing_queue_state/8, sender_death_fun/0, depth_fun/0]).

-export([init_with_existing_bq/3, stop_mirroring/1, sync_mirrors/3]).

-behaviour(rabbit_backing_queue).

-include("rabbit.hrl").

-record(state, {
				name,								%% 队列的名字
				gm,									%% 该主镜像队列的gm进程Pid
				coordinator,						%% 该主镜像队列的协调进程Pid
				backing_queue,						%% backing_queue的模块名字
				backing_queue_state,				%% 当前backing_queue的状态数据结构
				seen_status,
				confirmed,
				known_senders						%% 所有关联的rabbit_channel进程的sets数据结构集合
			   }).

-ifdef(use_specs).

-export_type([death_fun/0, depth_fun/0, stats_fun/0]).

-type(death_fun() :: fun ((pid()) -> 'ok')).
-type(depth_fun() :: fun (() -> 'ok')).
-type(stats_fun() :: fun ((any()) -> 'ok')).
-type(master_state() :: #state { name                :: rabbit_amqqueue:name(),
                                 gm                  :: pid(),
                                 coordinator         :: pid(),
                                 backing_queue       :: atom(),
                                 backing_queue_state :: any(),
                                 seen_status         :: dict:dict(),
                                 confirmed           :: [rabbit_guid:guid()],
                                 known_senders       :: sets:set()
                               }).

-spec(promote_backing_queue_state/8 ::
        (rabbit_amqqueue:name(), pid(), atom(), any(), pid(), [any()],
         dict:dict(), [pid()]) -> master_state()).
-spec(sender_death_fun/0 :: () -> death_fun()).
-spec(depth_fun/0 :: () -> depth_fun()).
-spec(init_with_existing_bq/3 :: (rabbit_types:amqqueue(), atom(), any()) ->
                                      master_state()).
-spec(stop_mirroring/1 :: (master_state()) -> {atom(), any()}).
-spec(sync_mirrors/3 :: (stats_fun(), stats_fun(), master_state()) ->
    {'ok', master_state()} | {stop, any(), master_state()}).

-endif.

%% For general documentation of HA design, see
%% rabbit_mirror_queue_coordinator

%% ---------------------------------------------------------------------------
%% Backing queue
%% ---------------------------------------------------------------------------
%% 消息队列主镜像backing_queue的启动函数(是不会调用的接口，如果有调用则直接停止调用的进程)
start(_DurableQueues) ->
	%% This will never get called as this module will never be
	%% installed as the default BQ implementation.
	exit({not_valid_for_generic_backing_queue, ?MODULE}).


%% 消息队列主镜像backing_queue的停止函数(是不会调用的接口，如果有调用则直接停止调用的进程)
stop() ->
	%% Same as start/1.
	exit({not_valid_for_generic_backing_queue, ?MODULE}).


%% 消息队列主镜像backing_queue的崩溃处理函数(是不会调用的接口，如果有调用则直接停止调用的进程)
delete_crashed(_QName) ->
	exit({not_valid_for_generic_backing_queue, ?MODULE}).


%% 主镜像队列的初始化
init(Q, Recover, AsyncCallback) ->
	%% 拿到backing_queue的处理模块名字
	{ok, BQ} = application:get_env(backing_queue_module),
	%% 使用backing_queue模块进行初始化(实际消息存储相关初始化)
	BQS = BQ:init(Q, Recover, AsyncCallback),
	%% 启动主镜像队列的协调进程和gm进程，同时初始化本镜像队列的数据结构
	State = #state{gm = GM} = init_with_existing_bq(Q, BQ, BQS),
	ok = gm:broadcast(GM, {depth, BQ:depth(BQS)}),
	State.


%% 启动主镜像队列的协调进程和gm进程，同时初始化本镜像队列的数据结构
init_with_existing_bq(Q = #amqqueue{name = QName}, BQ, BQS) ->
	%% coordinator：协调员，启动主镜像队列进程的协调进程
	{ok, CPid} = rabbit_mirror_queue_coordinator:start_link(
				   Q, undefined, sender_death_fun(), depth_fun()),
	%% 获取协调进程下启动的gm进程的Pid
	GM = rabbit_mirror_queue_coordinator:get_gm(CPid),
	Self = self(),
	%% 更新队列结构中gm_pids字段，同时将队列状态置为存活状态live
	ok = rabbit_misc:execute_mnesia_transaction(
		   fun () ->
					%% 主镜像队列只更新GM进程信息到消息队列数据结构中gm_pids字段中
					[Q1 = #amqqueue{gm_pids = GMPids}]
						= mnesia:read({rabbit_queue, QName}),
					ok = rabbit_amqqueue:store_queue(
						   Q1#amqqueue{gm_pids = [{GM, Self} | GMPids],
									   state   = live})
		   end),
	%% 获取该队列需要做副镜像的节点列表
	{_MNode, SNodes} = rabbit_mirror_queue_misc:suggested_queue_nodes(Q),
	%% We need synchronous add here (i.e. do not return until the
	%% slave is running) so that when queue declaration is finished
	%% all slaves are up; we don't want to end up with unsynced slaves
	%% just by declaring a new queue. But add can't be synchronous all
	%% the time as it can be called by slaves and that's
	%% deadlock-prone.
	%% 在SNodes节点上启动QName队列的镜像队列
	rabbit_mirror_queue_misc:add_mirrors(QName, SNodes, sync),
	%% 组装主镜像队列的数据结构
	#state { name                = QName,
			 gm                  = GM,
			 coordinator         = CPid,
			 backing_queue       = BQ,
			 backing_queue_state = BQS,
			 seen_status         = dict:new(),
			 confirmed           = [],
			 known_senders       = sets:new() }.


stop_mirroring(State = #state { coordinator         = CPid,
								backing_queue       = BQ,
								backing_queue_state = BQS }) ->
	unlink(CPid),
	stop_all_slaves(shutdown, State),
	{BQ, BQS}.


%% 向副的镜像队列中同步消息
sync_mirrors(HandleInfo, EmitStats,
			 State = #state { name                = QName,
							  gm                  = GM,
							  backing_queue       = BQ,
							  backing_queue_state = BQS }) ->
	%% 日志函数
	Log = fun (Fmt, Params) ->
				   rabbit_mirror_queue_misc:log_info(
					 QName, "Synchronising: " ++ Fmt ++ "~n", Params)
		  end,
	%% 打印有多少消息需要同步
	Log("~p messages to synchronise", [BQ:len(BQS)]),
	%% 拿到队列所有的副镜像队列进程的Pid
	{ok, #amqqueue{slave_pids = SPids}} = rabbit_amqqueue:lookup(QName),
	%% 生成唯一标识
	Ref = make_ref(),
	%% 启动做同步工作的同步进程
	Syncer = rabbit_mirror_queue_sync:master_prepare(Ref, QName, Log, SPids),
	%% 向镜像循环队列广播镜像队列同步的开始消息
	gm:broadcast(GM, {sync_start, Ref, Syncer, SPids}),
	S = fun(BQSN) -> State#state{backing_queue_state = BQSN} end,
	%% 主镜像队列开始进行同步的接口(主镜像队列从backing_queue中拿到所有的消息然后向副镜像队列进行同步操作)
	case rabbit_mirror_queue_sync:master_go(
		   Syncer, Ref, Log, HandleInfo, EmitStats, BQ, BQS) of
		{shutdown,  R, BQS1}   -> {stop, R, S(BQS1)};
		{sync_died, R, BQS1}   -> Log("~p", [R]),
								  {ok, S(BQS1)};
		{already_synced, BQS1} -> {ok, S(BQS1)};
		{ok, BQS1}             -> Log("complete", []),
								  {ok, S(BQS1)}
	end.


terminate({shutdown, dropped} = Reason,
		  State = #state { backing_queue       = BQ,
						   backing_queue_state = BQS }) ->
	%% Backing queue termination - this node has been explicitly
	%% dropped. Normally, non-durable queues would be tidied up on
	%% startup, but there's a possibility that we will be added back
	%% in without this node being restarted. Thus we must do the full
	%% blown delete_and_terminate now, but only locally: we do not
	%% broadcast delete_and_terminate.
	State#state{backing_queue_state = BQ:delete_and_terminate(Reason, BQS)};

terminate(Reason,
		  State = #state { name                = QName,
						   backing_queue       = BQ,
						   backing_queue_state = BQS }) ->
	%% Backing queue termination. The queue is going down but
	%% shouldn't be deleted. Most likely safe shutdown of this
	%% node.
	{ok, Q = #amqqueue{sync_slave_pids = SSPids}} =
		rabbit_amqqueue:lookup(QName),
	case SSPids =:= [] andalso
			 rabbit_policy:get(<<"ha-promote-on-shutdown">>, Q) =/= <<"always">> of
		true  -> %% Remove the whole queue to avoid data loss
			rabbit_mirror_queue_misc:log_warning(
			  QName, "Stopping all nodes on master shutdown since no "
				  "synchronised slave is available~n", []),
			stop_all_slaves(Reason, State);
		false -> %% Just let some other slave take over.
			ok
	end,
	State #state { backing_queue_state = BQ:terminate(Reason, BQS) }.


delete_and_terminate(Reason, State = #state { backing_queue       = BQ,
											  backing_queue_state = BQS }) ->
	stop_all_slaves(Reason, State),
	State#state{backing_queue_state = BQ:delete_and_terminate(Reason, BQS)}.


stop_all_slaves(Reason, #state{name = QName, gm = GM}) ->
	{ok, #amqqueue{slave_pids = SPids}} = rabbit_amqqueue:lookup(QName),
	PidsMRefs = [{Pid, erlang:monitor(process, Pid)} || Pid <- [GM | SPids]],
	ok = gm:broadcast(GM, {delete_and_terminate, Reason}),
	%% It's possible that we could be partitioned from some slaves
	%% between the lookup and the broadcast, in which case we could
	%% monitor them but they would not have received the GM
	%% message. So only wait for slaves which are still
	%% not-partitioned.
	[receive {'DOWN', MRef, process, _Pid, _Info} -> ok end
	 || {Pid, MRef} <- PidsMRefs, rabbit_mnesia:on_running_node(Pid)],
	%% Normally when we remove a slave another slave or master will
	%% notice and update Mnesia. But we just removed them all, and
	%% have stopped listening ourselves. So manually clean up.
	rabbit_misc:execute_mnesia_transaction(
	  fun () ->
			   [Q] = mnesia:read({rabbit_queue, QName}),
			   rabbit_mirror_queue_misc:store_updated_slaves(
				 Q #amqqueue { gm_pids = [], slave_pids = [] })
	  end),
	ok = gm:forget_group(QName).


%% 主镜像队列清除该队列中所有的消息
purge(State = #state { gm                  = GM,
					   backing_queue       = BQ,
					   backing_queue_state = BQS }) ->
	%% 向镜像循环队列进行广播
	ok = gm:broadcast(GM, {drop, 0, BQ:len(BQS), false}),
	%% 调用backing_queue模块进行操作
	{Count, BQS1} = BQ:purge(BQS),
	%% 更新最新的backing_queue状态
	{Count, State #state { backing_queue_state = BQS1 }}.


%% 将当前队列中所有等待ack的消息从消息索引和消息存储服务器中删除掉(该接口尚未实现)
purge_acks(_State) -> exit({not_implemented, {?MODULE, purge_acks}}).


%% 主镜像队列中发布消息的接口
publish(Msg = #basic_message { id = MsgId }, MsgProps, IsDelivered, ChPid, Flow,
		State = #state { gm                  = GM,
						 seen_status         = SS,
						 backing_queue       = BQ,
						 backing_queue_state = BQS }) ->
	false = dict:is_key(MsgId, SS), %% ASSERTION
	%% 向该消息队列广播发布消息
	ok = gm:broadcast(GM, {publish, ChPid, Flow, MsgProps, Msg},
					  rabbit_basic:msg_size(Msg)),
	%% 主镜像队列中将消息直接发布到backing_queue中(将消息发送到主镜像队列的backing_queue中)
	BQS1 = BQ:publish(Msg, MsgProps, IsDelivered, ChPid, Flow, BQS),
	%% 监视rabbit_channel进程
	ensure_monitoring(ChPid, State #state { backing_queue_state = BQS1 }).


%% 主镜像队列发布已经发送给消费者的消息
publish_delivered(Msg = #basic_message { id = MsgId }, MsgProps,
				  ChPid, Flow, State = #state { gm                  = GM,
												seen_status         = SS,
												backing_queue       = BQ,
												backing_queue_state = BQS }) ->
	false = dict:is_key(MsgId, SS), %% ASSERTION
	%% 向该消息队列广播发布消息
	ok = gm:broadcast(GM, {publish_delivered, ChPid, Flow, MsgProps, Msg},
					  rabbit_basic:msg_size(Msg)),
	%% 主镜像队列中将消息直接发布到backing_queue中(将消息发送到主镜像队列的backing_queue中)
	{AckTag, BQS1} = BQ:publish_delivered(Msg, MsgProps, ChPid, Flow, BQS),
	State1 = State #state { backing_queue_state = BQS1 },
	%% 监视rabbit_channel进程
	{AckTag, ensure_monitoring(ChPid, State1)}.


%% 主镜像队列丢弃消息的接口
discard(MsgId, ChPid, Flow, State = #state { gm                  = GM,
											 backing_queue       = BQ,
											 backing_queue_state = BQS,
											 seen_status         = SS }) ->
	false = dict:is_key(MsgId, SS), %% ASSERTION
	%% 向该消息队列广播发布消息
	ok = gm:broadcast(GM, {discard, ChPid, Flow, MsgId}),
	%% 监视rabbit_channel进程，同时更新最新的backing_queue的状态
	ensure_monitoring(ChPid,
					  State #state { backing_queue_state =
										 BQ:discard(MsgId, ChPid, Flow, BQS) }).


%% 通过消息message_properties的特性字段去执行Pred函数，如果条件满足，则将该消息从backing_queue中丢弃掉
dropwhile(Pred, State = #state{backing_queue       = BQ,
							   backing_queue_state = BQS }) ->
	Len  = BQ:len(BQS),
	{Next, BQS1} = BQ:dropwhile(Pred, BQS),
	{Next, drop(Len, false, State #state { backing_queue_state = BQS1 })}.


%% 通过消息message_properties的特性字段去执行Pred函数，如果条件满足，将消息执行Fun函数，然后将结果放入到Acc列表中
%% 直到找到一个不满足的消息，则停止操作(该操作主要是将过期的消息删除掉)
fetchwhile(Pred, Fun, Acc, State = #state{backing_queue       = BQ,
										  backing_queue_state = BQS }) ->
	Len  = BQ:len(BQS),
	{Next, Acc1, BQS1} = BQ:fetchwhile(Pred, Fun, Acc, BQS),
	{Next, Acc1, drop(Len, true, State #state { backing_queue_state = BQS1 })}.


%% 得到backing_queue中已经得到confirm的消息列表
drain_confirmed(State = #state { backing_queue       = BQ,
								 backing_queue_state = BQS,
								 seen_status         = SS,
								 confirmed           = Confirmed }) ->
	%% 先主镜像队列进行drain_confirmed操作得到已经得到confirm的消息列表
	{MsgIds, BQS1} = BQ:drain_confirmed(BQS),
	{MsgIds1, SS1} =
		lists:foldl(
		  fun (MsgId, {MsgIdsN, SSN}) ->
				   %% We will never see 'discarded' here
				   case dict:find(MsgId, SSN) of
					   error ->
						   {[MsgId | MsgIdsN], SSN};
					   {ok, published} ->
						   %% It was published when we were a slave,
						   %% and we were promoted before we saw the
						   %% publish from the channel. We still
						   %% haven't seen the channel publish, and
						   %% consequently we need to filter out the
						   %% confirm here. We will issue the confirm
						   %% when we see the publish from the channel.
						   {MsgIdsN, dict:store(MsgId, confirmed, SSN)};
					   {ok, confirmed} ->
						   %% Well, confirms are racy by definition.
						   {[MsgId | MsgIdsN], SSN}
				   end
		  end, {[], SS}, MsgIds),
	{Confirmed ++ MsgIds1, State #state { backing_queue_state = BQS1,
										  seen_status         = SS1,
										  confirmed           = [] }}.


%% 主镜像队列从队列中取出头部的一个消息
fetch(AckRequired, State = #state { backing_queue       = BQ,
									backing_queue_state = BQS }) ->
	%% 主镜像队列backing_queue先做读取操作
	{Result, BQS1} = BQ:fetch(AckRequired, BQS),
	%% 更新backing_queue的状态
	State1 = State #state { backing_queue_state = BQS1 },
	{Result, case Result of
				 empty                          -> State1;
				 %% 向镜像队列循环队列中广播丢弃队列中的第一个元素
				 {_MsgId, _IsDelivered, AckTag} -> drop_one(AckTag, State1)
			 end}.


%% 主镜像队列丢弃队列的头部元素
drop(AckRequired, State = #state { backing_queue       = BQ,
								   backing_queue_state = BQS }) ->
	%% 主镜像队列backing_queue先做操作
	{Result, BQS1} = BQ:drop(AckRequired, BQS),
	%% 更新backing_queue的状态
	State1 = State #state { backing_queue_state = BQS1 },
	{Result, case Result of
				 empty            -> State1;
				 %% 向镜像队列循环队列中广播丢弃队列中的第一个元素
				 {_MsgId, AckTag} -> drop_one(AckTag, State1)
			 end}.


%% 主镜像队列进行ack操作
ack(AckTags, State = #state { gm                  = GM,
							  backing_queue       = BQ,
							  backing_queue_state = BQS }) ->
	%% 主镜像队列backing_queue先做操作
	{MsgIds, BQS1} = BQ:ack(AckTags, BQS),
	case MsgIds of
		[] -> ok;
		%% 向镜像队列循环队列进行广播
		_  -> ok = gm:broadcast(GM, {ack, MsgIds})
	end,
	{MsgIds, State #state { backing_queue_state = BQS1 }}.


%% 主镜像队列将AckTags消息重新放入到队列中
requeue(AckTags, State = #state { gm                  = GM,
								  backing_queue       = BQ,
								  backing_queue_state = BQS }) ->
	%% 主镜像队列backing_queue先做操作
	{MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
	%% 向镜像队列循环队列进行广播
	ok = gm:broadcast(GM, {requeue, MsgIds}),
	{MsgIds, State #state { backing_queue_state = BQS1 }}.


%% 主镜像队列对等待ack的每个消息进行MsgFun函数操作(不必广播给循环镜像队列)
ackfold(MsgFun, Acc, State = #state { backing_queue       = BQ,
									  backing_queue_state = BQS }, AckTags) ->
	{Acc1, BQS1} = BQ:ackfold(MsgFun, Acc, BQS, AckTags),
	{Acc1, State #state { backing_queue_state =  BQS1 }}.


%% 主镜像队列对消息队列中的所有消息执行Fun函数(不必广播给循环镜像队列)
fold(Fun, Acc, State = #state { backing_queue = BQ,
								backing_queue_state = BQS }) ->
	{Result, BQS1} = BQ:fold(Fun, Acc, BQS),
	{Result, State #state { backing_queue_state = BQS1 }}.


%% 主镜像队列中获取队列长度(不必广播给循环镜像队列)
len(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
	BQ:len(BQS).


%% 主镜像队列判断队列是否为空(不包括等待ack的所有消息)(不必广播给循环镜像队列)
is_empty(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
	BQ:is_empty(BQS).


%% 得到队列中所有的消息长度(包括等待ack的所有消息)(不必广播给循环镜像队列)
depth(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
	BQ:depth(BQS).


%% rabbit_memory_monitor进程通知消息队列最新的内存持续时间
set_ram_duration_target(Target, State = #state { backing_queue       = BQ,
												 backing_queue_state = BQS }) ->
	State #state { backing_queue_state =
					   BQ:set_ram_duration_target(Target, BQS) }.


%% 获得当前消息队列内存中消息速率中分母持续时间大小
ram_duration(State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
	{Result, BQS1} = BQ:ram_duration(BQS),
	{Result, State #state { backing_queue_state = BQS1 }}.


%% 判断是否需要进行同步confirm操作
needs_timeout(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
	BQ:needs_timeout(BQS).


%% confirm同步的操作
timeout(State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
	State #state { backing_queue_state = BQ:timeout(BQS) }.


%% 刷新日志文件，将日志文件中的操作项存入对应的操作项的磁盘文件(队列进程从休眠状态接收到一个消息后，则会调用该接口进行一次日志文件的刷新)
handle_pre_hibernate(State = #state { backing_queue       = BQ,
									  backing_queue_state = BQS }) ->
	State #state { backing_queue_state = BQ:handle_pre_hibernate(BQS) }.


%% 睡眠接口，RabbitMQ系统中使用的内存过多，此操作是将内存中的队列数据写入到磁盘中
resume(State = #state { backing_queue       = BQ,
						backing_queue_state = BQS }) ->
	State #state { backing_queue_state = BQ:resume(BQS) }.


%% 获取消息队列中消息进入和出队列的速率大小
msg_rates(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
	BQ:msg_rates(BQS).


%% 主镜像队列中获取队列信息的接口
info(backing_queue_status,
	 State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
	BQ:info(backing_queue_status, BQS) ++
		[ {mirror_seen,    dict:size(State #state.seen_status)},
		  {mirror_senders, sets:size(State #state.known_senders)} ];

%% 主镜像队列中获取队列Item关键key对应的信息接口
info(Item, #state { backing_queue = BQ, backing_queue_state = BQS }) ->
	BQ:info(Item, BQS).


%% backing_queue执行Fun函数的接口
invoke(?MODULE, Fun, State) ->
	Fun(?MODULE, State);

invoke(Mod, Fun, State = #state { backing_queue       = BQ,
								  backing_queue_state = BQS }) ->
	State #state { backing_queue_state = BQ:invoke(Mod, Fun, BQS) }.


%% 主镜像队列判断消息是否重复
is_duplicate(Message = #basic_message { id = MsgId },
			 State = #state { seen_status         = SS,
							  backing_queue       = BQ,
							  backing_queue_state = BQS,
							  confirmed           = Confirmed }) ->
	%% Here, we need to deal with the possibility that we're about to
	%% receive a message that we've already seen when we were a slave
	%% (we received it via gm). Thus if we do receive such message now
	%% via the channel, there may be a confirm waiting to issue for
	%% it.
	
	%% We will never see {published, ChPid, MsgSeqNo} here.
	case dict:find(MsgId, SS) of
		error ->
			%% We permit the underlying BQ to have a peek at it, but
			%% only if we ourselves are not filtering out the msg.
			{Result, BQS1} = BQ:is_duplicate(Message, BQS),
			{Result, State #state { backing_queue_state = BQS1 }};
		{ok, published} ->
			%% It already got published when we were a slave and no
			%% confirmation is waiting. amqqueue_process will have, in
			%% its msg_id_to_channel mapping, the entry for dealing
			%% with the confirm when that comes back in (it's added
			%% immediately after calling is_duplicate). The msg is
			%% invalid. We will not see this again, nor will we be
			%% further involved in confirming this message, so erase.
			{true, State #state { seen_status = dict:erase(MsgId, SS) }};
		{ok, Disposition}
		  when Disposition =:= confirmed
		  %% It got published when we were a slave via gm, and
		  %% confirmed some time after that (maybe even after
		  %% promotion), but before we received the publish from the
		  %% channel, so couldn't previously know what the
		  %% msg_seq_no was (and thus confirm as a slave). So we
		  %% need to confirm now. As above, amqqueue_process will
		  %% have the entry for the msg_id_to_channel mapping added
		  %% immediately after calling is_duplicate/2.
				   orelse Disposition =:= discarded ->
			%% Message was discarded while we were a slave. Confirm now.
			%% As above, amqqueue_process will have the entry for the
			%% msg_id_to_channel mapping.
			{true, State #state { seen_status = dict:erase(MsgId, SS),
								  confirmed = [MsgId | Confirmed] }}
	end.

%% ---------------------------------------------------------------------------
%% Other exported functions
%% ---------------------------------------------------------------------------
%% 副镜像队列成为主镜像队列后用来组装主镜像队列的状态数据结构
promote_backing_queue_state(QName, CPid, BQ, BQS, GM, AckTags, Seen, KS) ->
	%% 将等待ack的消息重新放入到消息队列中
	{_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
	%% 获得当前消息队列的实际长度
	Len   = BQ:len(BQS1),
	%% 获得将ack重新放入队列后的实际长度
	Depth = BQ:depth(BQS1),
	true = Len == Depth, %% ASSERTION: everything must have been requeued
	ok = gm:broadcast(GM, {depth, Depth}),
	#state { name                = QName,
			 gm                  = GM,
			 coordinator         = CPid,
			 backing_queue       = BQ,
			 backing_queue_state = BQS1,
			 seen_status         = Seen,
			 confirmed           = [],
			 known_senders       = sets:from_list(KS) }.


%% 向循环镜像队列发布rabbit_channel进程死亡的接口
sender_death_fun() ->
	Self = self(),
	fun (DeadPid) ->
			 rabbit_amqqueue:run_backing_queue(
			   Self, ?MODULE,
			   fun (?MODULE, State = #state { gm = GM, known_senders = KS }) ->
						%% 广播DeadPid进程死亡的消息
						ok = gm:broadcast(GM, {sender_death, DeadPid}),
						%% 将死亡的rabbit_channel进程从known_senders字段中删除掉
						KS1 = sets:del_element(DeadPid, KS),
						%% 更新known_senders字段
						State #state { known_senders = KS1 }
			   end)
	end.


%% 让主镜像队列广播当前队列中所有消息包括等待ack的消息的总的数量
depth_fun() ->
	Self = self(),
	fun () ->
			 rabbit_amqqueue:run_backing_queue(
			   Self, ?MODULE,
			   fun (?MODULE, State = #state { gm                  = GM,
											  backing_queue       = BQ,
											  backing_queue_state = BQS }) ->
						ok = gm:broadcast(GM, {depth, BQ:depth(BQS)}),
						State
			   end)
	end.

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------
%% 向镜像队列循环队列中广播丢弃队列中的第一个元素
drop_one(AckTag, State = #state { gm                  = GM,
								  backing_queue       = BQ,
								  backing_queue_state = BQS }) ->
	%% 向镜像循环队列发布丢弃一个消息的消息
	ok = gm:broadcast(GM, {drop, BQ:len(BQS), 1, AckTag =/= undefined}),
	State.


%% 丢弃消息队列中的消息，第一个参数PrevLen表示丢弃前的数量
drop(PrevLen, AckRequired, State = #state { gm                  = GM,
											backing_queue       = BQ,
											backing_queue_state = BQS }) ->
	Len = BQ:len(BQS),
	case PrevLen - Len of
		0       -> State;
		%% Dropped表示丢弃的消息数量
		%% 向镜像循环队列发布丢弃Dropped消息
		Dropped -> ok = gm:broadcast(GM, {drop, Len, Dropped, AckRequired}),
				   State
	end.


%% 监视所有的发送者
ensure_monitoring(ChPid, State = #state { coordinator = CPid,
										  known_senders = KS }) ->
	case sets:is_element(ChPid, KS) of
		true  -> State;
		false -> ok = rabbit_mirror_queue_coordinator:ensure_monitoring(
						CPid, [ChPid]),
				 State #state { known_senders = sets:add_element(ChPid, KS) }
	end.
