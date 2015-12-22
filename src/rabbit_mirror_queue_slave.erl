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

-module(rabbit_mirror_queue_slave).

%% For general documentation of HA design, see
%% rabbit_mirror_queue_coordinator
%%
%% We receive messages from GM and from publishers, and the gm
%% messages can arrive either before or after the 'actual' message.
%% All instructions from the GM group must be processed in the order
%% in which they're received.

-export([set_maximum_since_use/2, info/1, go/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).

-export([joined/2, members_changed/3, handle_msg/3, handle_terminate/2]).

-behaviour(gen_server2).
-behaviour(gm).

-include("rabbit.hrl").

-include("gm_specs.hrl").

%%----------------------------------------------------------------------------
%% 副镜像队列中所有关键信息对应的关键key列表
-define(INFO_KEYS,
		[
		 pid,
		 name,
		 master_pid,
		 is_synchronised
		]).

-define(SYNC_INTERVAL,                 25). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).
-define(DEATH_TIMEOUT,                 20000). %% 20 seconds

-record(state, {
				q,															%% 消息队列的整体数据信息
				gm,															%% 当前镜像队列启动的GM进程的Pid
				backing_queue,												%% 实际处理消息的backing_queue模块名字
				backing_queue_state,										%% 实际处理消息存储消息的状态数据信息
				sync_timer_ref,												%% 同步confirm的定时器，当前队列大部分接收一次消息就要确保当前定时器的存在(200ms的定时器)
				rate_timer_ref,												%% 队列中消息进入和出去的速率定时器
				
				sender_queues, %% :: Pid -> {Q Msg, Set MsgId, ChState}		%% rabbit_channel进程对应的相关消息状态
																			%% tuple的第一个元素是存储rabbit_channel进程发送过来的消息，但是这些消息还没有从GM进程发送过来
																			%% tuple的第二个元素是记录的是GM进程发送过来的消息，但是rabbit_channel进程没有发送过来
																			%% tuple的第三个元素记录的是当前rabbit_channel进程的状态
				msg_id_ack,    %% :: MsgId -> AckTag							%% 当前副镜像队列中等待ack的消息字典(数据存储格式是：MsgId -> AckTag)
				
				msg_id_status,												%% 消息ID对应该消息当前的状态
				known_senders,												%% 监视rabbit_channel进程的数据结构
				
				%% Master depth - local depth
				depth_delta													%% 当前副镜像队列和主镜像队列相差的消息数量
			   }).

%%----------------------------------------------------------------------------
%% 将当前进程客户端中文件句柄打开时间超过MaximumAge的句柄软关闭
set_maximum_since_use(QPid, Age) ->
	gen_server2:cast(QPid, {set_maximum_since_use, Age}).


%% 获取副镜像队列进程中关键key对应的关键信息
info(QPid) -> gen_server2:call(QPid, info, infinity).


%% 副镜像队列进程的初始化
init(Q) ->
	%% 存储队列进程的名字
	?store_proc_name(Q#amqqueue.name),
	{ok, {not_started, Q}, hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
	  %% 在此处将队列进程的回调模块改为rabbit_mirror_queue_slave本模块
	  ?DESIRED_HIBERNATE}, ?MODULE}.


%% 同步的启动副镜像队列进程
go(SPid, sync)  -> gen_server2:call(SPid, go, infinity);

%% 异步的启动副镜像队列进程
go(SPid, async) -> gen_server2:cast(SPid, go).


%% 运行副镜像队列
handle_go(Q = #amqqueue{name = QName}) ->
	%% We join the GM group before we add ourselves to the amqqueue
	%% record. As a result:
	%% 1. We can receive msgs from GM that correspond to messages we will
	%% never receive from publishers.
	%% 2. When we receive a message from publishers, we must receive a
	%% message from the GM group for it.
	%% 3. However, that instruction from the GM group can arrive either
	%% before or after the actual message. We need to be able to
	%% distinguish between GM instructions arriving early, and case (1)
	%% above.
	%%
	process_flag(trap_exit, true), %% amqqueue_process traps exits too.
	%% 启动gm进程
	{ok, GM} = gm:start_link(QName, ?MODULE, [self()],
							 fun rabbit_misc:execute_mnesia_transaction/1),
	%% 监视启动的gm进程
	MRef = erlang:monitor(process, GM),
	%% We ignore the DOWN message because we are also linked and
	%% trapping exits, we just want to not get stuck and we will exit
	%% later.
	receive
		%% 收到GM进程成功加入镜像队列群组的消息
		{joined, GM}            -> erlang:demonitor(MRef, [flush]),
								   ok;
		{'DOWN', MRef, _, _, _} -> ok
	end,
	Self = self(),
	Node = node(),
	case rabbit_misc:execute_mnesia_transaction(
		   fun() -> init_it(Self, GM, Node, QName) end) of
		%% 启动的镜像队列是本节点上新的镜像队列
		{new, QPid, GMPids} ->
			%% 打开文件的客户端进程注册的回调清理函数的接口(用来当句柄数量不够用的时候，关闭当前进程中打开时间操作某值的句柄)
			ok = file_handle_cache:register_callback(
				   rabbit_amqqueue, set_maximum_since_use, [Self]),
			%% rabbit_memory_monitor进程通知消息队列最新的内存持续时间
			ok = rabbit_memory_monitor:register(
				   Self, {rabbit_amqqueue, set_ram_duration_target, [Self]}),
			%% 获得实际存储消息的backing_queue模块名字
			{ok, BQ} = application:get_env(backing_queue_module),
			Q1 = Q #amqqueue { pid = QPid },
			%% 删除消息队列名字为QName对应的目录下面所有的消息索引磁盘文件
			ok = rabbit_queue_index:erase(QName), %% For crash recovery
			%% 实际存储消息的backing_queue的初始化
			BQS = bq_init(BQ, Q1, new),
			%% 组装副镜像队列进程中的数据结构信息
			State = #state { q                   = Q1,
							 gm                  = GM,
							 backing_queue       = BQ,
							 backing_queue_state = BQS,
							 rate_timer_ref      = undefined,
							 sync_timer_ref      = undefined,
							 
							 sender_queues       = dict:new(),
							 msg_id_ack          = dict:new(),
							 
							 msg_id_status       = dict:new(),
							 known_senders       = pmon:new(delegate),
							 
							 depth_delta         = undefined
						   },
			%% 向循环队列镜像队列发送请求消息队列实际长度的请求消息
			ok = gm:broadcast(GM, request_depth),
			%% 向新启动的GM进程验证成员
			ok = gm:validate_members(GM, [GM | [G || {G, _} <- GMPids]]),
			%% 根据队列的策略如果启动的副镜像队列需要自动同步，则进行同步操作
			rabbit_mirror_queue_misc:maybe_auto_sync(Q1),
			{ok, State};
		%% 队列的主进程Pid是陈旧的
		{stale, StalePid} ->
			%% 打印主队列进程Pid过期的日志
			rabbit_mirror_queue_misc:log_warning(
			  QName, "Detected stale HA master: ~p~n", [StalePid]),
			%% 将新启动的gm进程停止
			gm:leave(GM),
			{error, {stale_master_pid, StalePid}};
		%% 重复启动主进程Pid
		duplicate_live_master ->
			%% 将新启动的gm进程停止
			gm:leave(GM),
			{error, {duplicate_live_master, Node}};
		%% 该镜像队列已经在当前节点已经存在
		existing ->
			%% 将新启动的gm进程停止
			gm:leave(GM),
			{error, normal};
		%% 队列信息没有读取到
		master_in_recovery ->
			%% 将新启动的gm进程停止
			gm:leave(GM),
			%% The queue record vanished - we must have a master starting
			%% concurrently with us. In that case we can safely decide to do
			%% nothing here, and the master will start us in
			%% master:init_with_existing_bq/3
			{error, normal}
	end.


%% 初始化副镜像队列
init_it(Self, GM, Node, QName) ->
	case mnesia:read({rabbit_queue, QName}) of
		[Q = #amqqueue { pid = QPid, slave_pids = SPids, gm_pids = GMPids }] ->
			%% 查找当前队列启动在本节点上的副镜像队列进程的Pid
			case [Pid || Pid <- [QPid | SPids], node(Pid) =:= Node] of
				%% 如果为空，则表示当前启动的镜像队列是正确的镜像队列，则将当前镜像队列对应的GM进程加入队列信息中
				[]     -> add_slave(Q, Self, GM),
						  {new, QPid, GMPids};
				%% 如果查找到Pid且Pid为主镜像队列进程的Pid
				[QPid] -> case rabbit_mnesia:is_process_alive(QPid) of
							  true  -> duplicate_live_master;
							  false -> {stale, QPid}
						  end;
				%% 查找到有副镜像队列，如果该进程存活，则表示已经存在镜像队列，如果死亡，则将死亡的镜像队列从队列信息中删除，
				%% 然后将新的副镜像队列加入到队列信息中
				[SPid] -> case rabbit_mnesia:is_process_alive(SPid) of
							  true  -> existing;
							  false -> GMPids1 = [T || T = {_, S} <- GMPids,
													   S =/= SPid],
									   Q1 = Q#amqqueue{
													   slave_pids = SPids -- [SPid],
													   gm_pids    = GMPids1},
									   add_slave(Q1, Self, GM),
									   {new, QPid, GMPids1}
						  end
			end;
		[] ->
			master_in_recovery
	end.

%% Add to the end, so they are in descending order of age, see
%% rabbit_mirror_queue_misc:promote_slave/1
%% 增加副镜像队列的接口，将新的副镜像队列Pid相关信息写入队列数据结构中
add_slave(Q = #amqqueue { slave_pids = SPids, gm_pids = GMPids }, New, GM) ->
	rabbit_mirror_queue_misc:store_updated_slaves(
	  Q#amqqueue{slave_pids = SPids ++ [New], gm_pids = [{GM, New} | GMPids]}).


%% 同步处理让当前镜像队列运行的消息
handle_call(go, _From, {not_started, Q} = NotStarted) ->
	case handle_go(Q) of
		{ok, State}    -> {reply, ok, State};
		{error, Error} -> {stop, Error, NotStarted}
	end;

%% 副镜像队列处理有镜像队列成员死亡的消息(副镜像队列接收到主镜像队列死亡的消息)
handle_call({gm_deaths, DeadGMPids}, From,
			State = #state { gm = GM, q = Q = #amqqueue {
														 name = QName, pid = MPid }}) ->
	Self = self(),
	%% 返回新的主镜像队列进程，死亡的镜像队列进程列表，需要新增加镜像队列的节点列表
	case rabbit_mirror_queue_misc:remove_from_queue(QName, Self, DeadGMPids) of
		{error, not_found} ->
			gen_server2:reply(From, ok),
			{stop, normal, State};
		{ok, Pid, DeadPids, ExtraNodes} ->
			%% 打印镜像队列死亡的日志(Self是副镜像队列)
			rabbit_mirror_queue_misc:report_deaths(Self, false, QName,
												   DeadPids),
			case Pid of
				%% 此情况是主镜像队列没有变化
				MPid ->
					%% master hasn't changed
					gen_server2:reply(From, ok),
					%% 异步在ExtraNodes的所有节点上增加QName队列的副镜像队列
					rabbit_mirror_queue_misc:add_mirrors(
					  QName, ExtraNodes, async),
					noreply(State);
				%% 此情况是本副镜像队列成为主镜像队列
				Self ->
					%% we've become master
					%% 将自己这个副镜像队列提升为主镜像队列
					QueueState = promote_me(From, State),
					%% 异步在ExtraNodes的所有节点上增加QName队列的副镜像队列
					rabbit_mirror_queue_misc:add_mirrors(
					  QName, ExtraNodes, async),
					%% 返回消息，告知自己这个副镜像队列成为主镜像队列
					{become, rabbit_amqqueue_process, QueueState, hibernate};
				_ ->
					%% 主镜像队列已经发生变化
					%% master has changed to not us
					gen_server2:reply(From, ok),
					%% assertion, we don't need to add_mirrors/2 in this
					%% branch, see last clause in remove_from_queue/2
					[] = ExtraNodes,
					%% Since GM is by nature lazy we need to make sure
					%% there is some traffic when a master dies, to
					%% make sure all slaves get informed of the
					%% death. That is all process_death does, create
					%% some traffic.
					ok = gm:broadcast(GM, process_death),
					noreply(State #state { q = Q #amqqueue { pid = Pid } })
			end
	end;

%% 获取当前副镜像队列INFO_KEYS关键key列表对应的信息
handle_call(info, _From, State) ->
	reply(infos(?INFO_KEYS, State), State).


%% 处理主镜像队列进程启动副镜像队列进程后发送过来的让副镜像队列运行的消息
handle_cast(go, {not_started, Q} = NotStarted) ->
	case handle_go(Q) of
		{ok, State}    -> {noreply, State};
		{error, Error} -> {stop, Error, NotStarted}
	end;

%% 让副镜像队列的backing_queue模块执行Fun函数
handle_cast({run_backing_queue, Mod, Fun}, State) ->
	noreply(run_backing_queue(Mod, Fun, State));

%% 副镜像队列处理镜像循环队列中发送过来的消息
handle_cast({gm, Instruction}, State) ->
	handle_process_result(process_instruction(Instruction, State));

%% 处理直接由rabbit_channel进程发送过来的消息
handle_cast({deliver, Delivery = #delivery{sender = Sender, flow = Flow}, true},
			State) ->
	%% Asynchronous, non-"mandatory", deliver mode.
	case Flow of
		flow   -> credit_flow:ack(Sender);
		noflow -> ok
	end,
	%% 处理由rabbit_channel进程直接发布过来的消息
	noreply(maybe_enqueue_message(Delivery, State));

%% 处理主镜像队列发送过来的同步开始的消息
handle_cast({sync_start, Ref, Syncer},
			State = #state { depth_delta         = DD,
							 backing_queue       = BQ,
							 backing_queue_state = BQS }) ->
	%% 启动速率监控的定时器
	State1 = #state{rate_timer_ref = TRef} = ensure_rate_timer(State),
	S = fun({MA, TRefN, BQSN}) ->
				State1#state{depth_delta         = undefined,
							 msg_id_ack          = dict:from_list(MA),
							 rate_timer_ref      = TRefN,
							 backing_queue_state = BQSN}
		end,
	case rabbit_mirror_queue_sync:slave(
		   DD, Ref, TRef, Syncer, BQ, BQS,
		   fun (BQN, BQSN) ->
					%% 处理更新当前队列消息进入和出去的速率消息
					BQSN1 = update_ram_duration(BQN, BQSN),
					TRefN = rabbit_misc:send_after(?RAM_DURATION_UPDATE_INTERVAL,
												   self(), update_ram_duration),
					{TRefN, BQSN1}
		   end) of
		denied              -> noreply(State1);
		{ok,           Res} -> noreply(set_delta(0, S(Res)));
		{failed,       Res} -> noreply(S(Res));
		{stop, Reason, Res} -> {stop, Reason, S(Res)}
	end;

%% 将当前进程客户端中文件句柄打开时间超过Age的句柄软关闭
handle_cast({set_maximum_since_use, Age}, State) ->
	ok = file_handle_cache:set_maximum_since_use(Age),
	noreply(State);

%% rabbit_memory_monitor进程通知消息队列最新的内存持续时间
handle_cast({set_ram_duration_target, Duration},
			State = #state { backing_queue       = BQ,
							 backing_queue_state = BQS }) ->
	BQS1 = BQ:set_ram_duration_target(Duration, BQS),
	noreply(State #state { backing_queue_state = BQS1 }).


%% 处理更新当前队列消息进入和出去的速率消息
handle_info(update_ram_duration, State = #state{backing_queue       = BQ,
												backing_queue_state = BQS}) ->
	BQS1 = update_ram_duration(BQ, BQS),
	%% Don't call noreply/1, we don't want to set timers
	{State1, Timeout} = next_state(State #state {
												 rate_timer_ref      = undefined,
												 backing_queue_state = BQS1 }),
	{noreply, State1, Timeout};

%% 处理sync_timeout消息，该消息是sync_timer_ref定时器发送给自己
handle_info(sync_timeout, State) ->
	noreply(backing_queue_timeout(
			  State #state { sync_timer_ref = undefined }));

%% 处理timeout消息，该消息是指定时间后队列进程还没有收到消息，则会收到该消息
handle_info(timeout, State) ->
	noreply(backing_queue_timeout(State));

%% 处理监视的rabbit_channel进程挂掉的消息
handle_info({'DOWN', _MonitorRef, process, ChPid, _Reason}, State) ->
	local_sender_death(ChPid, State),
	%% 清除rabbit_channel进程ChPid相关的信息
	noreply(maybe_forget_sender(ChPid, down_from_ch, State));

handle_info({'EXIT', _Pid, Reason}, State) ->
	{stop, Reason, State};

handle_info({bump_credit, Msg}, State) ->
	credit_flow:handle_bump_msg(Msg),
	noreply(State);

%% In the event of a short partition during sync we can detect the
%% master's 'death', drop out of sync, and then receive sync messages
%% which were still in flight. Ignore them.
handle_info({sync_msg, _Ref, _Msg, _Props, _Unacked}, State) ->
	noreply(State);

handle_info({sync_complete, _Ref}, State) ->
	noreply(State);

handle_info(Msg, State) ->
	{stop, {unexpected_info, Msg}, State}.


terminate(_Reason, {not_started, _Q}) ->
	ok;

terminate(_Reason, #state { backing_queue_state = undefined }) ->
	%% We've received a delete_and_terminate from gm, thus nothing to
	%% do here.
	ok;

terminate({shutdown, dropped} = R, State = #state{backing_queue       = BQ,
												  backing_queue_state = BQS}) ->
	%% See rabbit_mirror_queue_master:terminate/2
	terminate_common(State),
	BQ:delete_and_terminate(R, BQS);

terminate(shutdown, State) ->
	terminate_shutdown(shutdown, State);

terminate({shutdown, _} = R, State) ->
	terminate_shutdown(R, State);

terminate(Reason, State = #state{backing_queue       = BQ,
								 backing_queue_state = BQS}) ->
	terminate_common(State),
	BQ:delete_and_terminate(Reason, BQS).

%% If the Reason is shutdown, or {shutdown, _}, it is not the queue
%% being deleted: it's just the node going down. Even though we're a
%% slave, we have no idea whether or not we'll be the only copy coming
%% back up. Thus we must assume we will be, and preserve anything we
%% have on disk.
terminate_shutdown(Reason, State = #state{backing_queue       = BQ,
										  backing_queue_state = BQS}) ->
	terminate_common(State),
	BQ:terminate(Reason, BQS).


terminate_common(State) ->
	ok = rabbit_memory_monitor:deregister(self()),
	stop_rate_timer(stop_sync_timer(State)).


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


handle_pre_hibernate({not_started, _Q} = State) ->
	{hibernate, State};

handle_pre_hibernate(State = #state { backing_queue       = BQ,
									  backing_queue_state = BQS }) ->
	{RamDuration, BQS1} = BQ:ram_duration(BQS),
	DesiredDuration =
		rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
	BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
	BQS3 = BQ:handle_pre_hibernate(BQS2),
	{hibernate, stop_rate_timer(State #state { backing_queue_state = BQS3 })}.


prioritise_call(Msg, _From, _Len, _State) ->
	case Msg of
		info                                 -> 9;
		{gm_deaths, _Dead}                   -> 5;
		_                                    -> 0
	end.


prioritise_cast(Msg, _Len, _State) ->
	case Msg of
		{set_ram_duration_target, _Duration} -> 8;
		{set_maximum_since_use, _Age}        -> 8;
		{run_backing_queue, _Mod, _Fun}      -> 6;
		{gm, _Msg}                           -> 5;
		_                                    -> 0
	end.


prioritise_info(Msg, _Len, _State) ->
	case Msg of
		update_ram_duration                  -> 8;
		sync_timeout                         -> 6;
		_                                    -> 0
	end.


format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------
%% GM进程通知副镜像队列成功的加入镜像队列的循环队列中
joined([SPid], _Members) -> SPid ! {joined, self()}, ok.


%% GM进程通知副镜像队列中的循环队列中有成员变动
members_changed([_SPid], _Births, []) ->
	ok;

members_changed([ SPid], _Births, Deaths) ->
	case rabbit_misc:with_exit_handler(
		   rabbit_misc:const(ok),
		   fun() ->
				   gen_server2:call(SPid, {gm_deaths, Deaths}, infinity)
		   end) of
		ok              -> ok;
		%% 返回给GM进程告知自己成为主镜像队列
		{promote, CPid} -> {become, rabbit_mirror_queue_coordinator, [CPid]}
	end.


handle_msg([_SPid], _From, hibernate_heartbeat) ->
	%% See rabbit_mirror_queue_coordinator:handle_pre_hibernate/1
	ok;

%% 副镜像队列对于request_depth不做任何处理(该消息只是主镜像队列进行处理)
handle_msg([_SPid], _From, request_depth) ->
	%% This is only of value to the master
	ok;

%% 该消息只是主镜像队列进行处理
handle_msg([_SPid], _From, {ensure_monitoring, _Pid}) ->
	%% This is only of value to the master
	ok;

%% 副镜像队列不处理process_death这个消息
handle_msg([_SPid], _From, process_death) ->
	%% We must not take any notice of the master death here since it
	%% comes without ordering guarantees - there could still be
	%% messages from the master we have yet to receive. When we get
	%% members_changed, then there will be no more messages.
	ok;

handle_msg([CPid], _From, {delete_and_terminate, _Reason} = Msg) ->
	ok = gen_server2:cast(CPid, {gm, Msg}),
	{stop, {shutdown, ring_shutdown}};

%% 收到主镜像队列广播的同步镜像队列的消息
handle_msg([SPid], _From, {sync_start, Ref, Syncer, SPids}) ->
	case lists:member(SPid, SPids) of
		true  -> gen_server2:cast(SPid, {sync_start, Ref, Syncer});
		false -> ok
	end;

handle_msg([SPid], _From, Msg) ->
	ok = gen_server2:cast(SPid, {gm, Msg}).


handle_terminate([_SPid], _Reason) ->
	ok.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------
%% 获取当前副镜像队列Items关键key列表对应的信息
infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].


%% 获得当前副镜像队列的Pid
i(pid,             _State)                                   -> self();

%% 获得当前副镜像队列的名字
i(name,            #state { q = #amqqueue { name = Name } }) -> Name;

%% 获得当前副镜像队列的主镜像队列的Pid
i(master_pid,      #state { q = #amqqueue { pid  = MPid } }) -> MPid;

%% synchronised：同步
%% 判断当前副镜像队列是否已经跟主镜像队列同步过
i(is_synchronised, #state { depth_delta = DD })              -> DD =:= 0;

%% 其它不识别的关键key
i(Item,            _State) -> throw({bad_argument, Item}).


%% 实际存储消息的backing_queue的初始化
bq_init(BQ, Q, Recover) ->
	Self = self(),
	BQ:init(Q, Recover,
			fun (Mod, Fun) ->
					 rabbit_amqqueue:run_backing_queue(Self, Mod, Fun)
			end).


%% 让副镜像队列的backing_queue模块执行Fun函数
run_backing_queue(rabbit_mirror_queue_master, Fun, State) ->
	%% Yes, this might look a little crazy, but see comments in
	%% confirm_sender_death/1
	Fun(?MODULE, State);

run_backing_queue(Mod, Fun, State = #state { backing_queue       = BQ,
											 backing_queue_state = BQS }) ->
	State #state { backing_queue_state = BQ:invoke(Mod, Fun, BQS) }.


%% 通知rabbit_channel进程该消息已经得到接收，不必要将该消息重新发送给客户端
%% mandatory:当mandatory标志位设置为true时，如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，
%% 那么会调用basic.return方法将消息返还给生产者；当mandatory设为false时，出现上述情形broker会直接将消息扔掉
send_mandatory(#delivery{mandatory  = false}) ->
	ok;

send_mandatory(#delivery{mandatory  = true,
						 sender     = SenderPid,
						 msg_seq_no = MsgSeqNo}) ->
	gen_server2:cast(SenderPid, {mandatory_received, MsgSeqNo}).


%% 处理消息的confirm
send_or_record_confirm(_, #delivery{ confirm = false }, MS, _State) ->
	MS;

%% 消息需要进行confirm，同时该消息是持久化消息，则现在不能立刻进行confirm操作，需要等待持久化完毕后才能进行confirm操作
%% 因此先将消息状态存储起来
send_or_record_confirm(published, #delivery { sender     = ChPid,
											  confirm    = true,
											  msg_seq_no = MsgSeqNo,
											  message    = #basic_message {
																		   id            = MsgId,
																		   is_persistent = true } },
					   MS, #state { q = #amqqueue { durable = true } }) ->
	dict:store(MsgId, {published, ChPid, MsgSeqNo} , MS);

%% 消息需要进行confirm操作，同时消息非持久化消息，则立刻通知rabbit_channel进程进行confirm操作
send_or_record_confirm(_Status, #delivery { sender     = ChPid,
											confirm    = true,
											msg_seq_no = MsgSeqNo },
					   MS, _State) ->
	ok = rabbit_misc:confirm_to_sender(ChPid, [MsgSeqNo]),
	MS.


%% 持久化消息已经存储到磁盘后的消息最后进行confirm操作
confirm_messages(MsgIds, State = #state { msg_id_status = MS }) ->
	{CMs, MS1} =
		lists:foldl(
		  fun (MsgId, {CMsN, MSN} = Acc) ->
				   %% We will never see 'discarded' here
				   case dict:find(MsgId, MSN) of
					   error ->
						   %% If it needed confirming, it'll have
						   %% already been done.
						   Acc;
					   {ok, published} ->
						   %% Still not seen it from the channel, just
						   %% record that it's been confirmed.
						   {CMsN, dict:store(MsgId, confirmed, MSN)};
					   %% 此种情况是该消息已经从GM进程和rabbit_channel进程都同时可见，则立刻进行confirm操作
					   {ok, {published, ChPid, MsgSeqNo}} ->
						   %% Seen from both GM and Channel. Can now
						   %% confirm.
						   {rabbit_misc:gb_trees_cons(ChPid, MsgSeqNo, CMsN),
							dict:erase(MsgId, MSN)};
					   %% 如果已经得到confirm则什么都不做
					   {ok, confirmed} ->
						   %% It's already been confirmed. This is
						   %% probably it's been both sync'd to disk
						   %% and then delivered and ack'd before we've
						   %% seen the publish from the
						   %% channel. Nothing to do here.
						   Acc
				   end
		  end, {gb_trees:empty(), MS}, MsgIds),
	%% 根据寻找到的rabbit_channel进程对应的MsgId列表二叉树，对每个rabbit_channel进程分别进行confirm操作
	rabbit_misc:gb_trees_foreach(fun rabbit_misc:confirm_to_sender/2, CMs),
	State #state { msg_id_status = MS1 }.


handle_process_result({ok,   State}) -> noreply(State);

handle_process_result({stop, State}) -> {stop, normal, State}.

-ifdef(use_specs).
-spec(promote_me/2 :: ({pid(), term()}, #state{}) -> no_return()).
-endif.
%% 将自己这个副镜像队列提升为主镜像队列
promote_me(From, #state { q                   = Q = #amqqueue { name = QName },
						  gm                  = GM,
						  backing_queue       = BQ,
						  backing_queue_state = BQS,
						  rate_timer_ref      = RateTRef,
						  sender_queues       = SQ,
						  msg_id_ack          = MA,
						  msg_id_status       = MS,
						  known_senders       = KS }) ->
	%% 打印将自己这个副镜像队列提升为主镜像队列的日志
	rabbit_mirror_queue_misc:log_info(QName, "Promoting slave ~s to master~n",
									  [rabbit_misc:pid_to_string(self())]),
	%% 更新最新的主镜像队列进程Pid
	Q1 = Q #amqqueue { pid = self() },
	%% 启动新的主镜像队列的协调进程
	{ok, CPid} = rabbit_mirror_queue_coordinator:start_link(
				   Q1, GM, rabbit_mirror_queue_master:sender_death_fun(),
				   rabbit_mirror_queue_master:depth_fun()),
	true = unlink(GM),
	%% 返回GM进程新的主镜像队列的协调进程
	gen_server2:reply(From, {promote, CPid}),
	
	%% Everything that we're monitoring, we need to ensure our new
	%% coordinator is monitoring.
	%% 获得当前副镜像队列监视的所有rabbit_channel进程列表
	MPids = pmon:monitored(KS),
	%% 让新的主镜像队列的协调进程去监视这些rabbit_channel进程
	ok = rabbit_mirror_queue_coordinator:ensure_monitoring(CPid, MPids),
	
	%% 成为主镜像队列后，需要将以前rabbit_channel进程发送过来的消息，但是这些消息
	%% 还没有从副镜像队列获取到，则将这些队列直接放入到主镜像队列中
	%% We find all the messages that we've received from channels but
	%% not from gm, and pass them to the
	%% queue_process:init_with_backing_queue_state to be enqueued.
	%%
	%% We also have to requeue messages which are pending acks: the
	%% consumers from the master queue have been lost and so these
	%% messages need requeuing. They might also be pending
	%% confirmation, and indeed they might also be pending arrival of
	%% the publication from the channel itself, if we received both
	%% the publication and the fetch via gm first! Requeuing doesn't
	%% affect confirmations: if the message was previously pending a
	%% confirmation then it still will be, under the same msg_id. So
	%% as a master, we need to be prepared to filter out the
	%% publication of said messages from the channel (is_duplicate
	%% (thus such requeued messages must remain in the msg_id_status
	%% (MS) which becomes seen_status (SS) in the master)).
	%%
	%% Then there are messages we already have in the queue, which are
	%% not currently pending acknowledgement:
	%% 1. Messages we've only received via gm:
	%%    Filter out subsequent publication from channel through
	%%    validate_message. Might have to issue confirms then or
	%%    later, thus queue_process state will have to know that
	%%    there's a pending confirm.
	%% 2. Messages received via both gm and channel:
	%%    Queue will have to deal with issuing confirms if necessary.
	%%
	%% MS contains the following three entry types:
	%%
	%% a) published:
	%%   published via gm only; pending arrival of publication from
	%%   channel, maybe pending confirm.
	%%
	%% b) {published, ChPid, MsgSeqNo}:
	%%   published via gm and channel; pending confirm.
	%%
	%% c) confirmed:
	%%   published via gm only, and confirmed; pending publication
	%%   from channel.
	%%
	%% d) discarded:
	%%   seen via gm only as discarded. Pending publication from
	%%   channel
	%%
	%% The forms a, c and d only, need to go to the master state
	%% seen_status (SS).
	%%
	%% The form b only, needs to go through to the queue_process
	%% state to form the msg_id_to_channel mapping (MTC).
	%%
	%% No messages that are enqueued from SQ at this point will have
	%% entries in MS.
	%%
	%% Messages that are extracted from MA may have entries in MS, and
	%% those messages are then requeued. However, as discussed above,
	%% this does not affect MS, nor which bits go through to SS in
	%% Master, or MTC in queue_process.
	
	St = [published, confirmed, discarded],
	%% 获得指定状态的消息
	SS = dict:filter(fun (_MsgId, Status) -> lists:member(Status, St) end, MS),
	%% 获得等待ack操作的消息列表
	AckTags = [AckTag || {_MsgId, AckTag} <- dict:to_list(MA)],
	
	%% 副镜像队列成为主镜像队列后用来组装主镜像队列的状态数据结构
	MasterState = rabbit_mirror_queue_master:promote_backing_queue_state(
					QName, CPid, BQ, BQS, GM, AckTags, SS, MPids),
	
	%% 组装已经发布但是等待confirm的消息
	MTC = dict:fold(fun (MsgId, {published, ChPid, MsgSeqNo}, MTC0) ->
							 gb_trees:insert(MsgId, {ChPid, MsgSeqNo}, MTC0);
					   (_Msgid, _Status, MTC0) ->
							MTC0
					end, gb_trees:empty(), MS),
	%% 获得rabbit_channel进程已经发送过来的消息但是没有从GM进程获取的消息
	%% 这些消息在该副镜像队列成为主镜像队列后，直接将该消息发布到主镜像队列的backing_queue模块中
	Deliveries = [promote_delivery(Delivery) ||
					{_ChPid, {PubQ, _PendCh, _ChState}} <- dict:to_list(SQ),
					Delivery <- queue:to_list(PubQ)],
	%% 获得跟当前队列相关的存活的rabbit_channel进程列表
	AwaitGmDown = [ChPid || {ChPid, {_, _, down_from_ch}} <- dict:to_list(SQ)],
	%% 将已经死亡的rabbit_channel进程剔除掉
	KS1 = lists:foldl(fun (ChPid0, KS0) ->
							   pmon:demonitor(ChPid0, KS0)
					  end, KS, AwaitGmDown),
	%% 存储主镜像队列的进程名字
	rabbit_misc:store_proc_name(rabbit_amqqueue_process, QName),
	%% 副镜像队列成为主镜像队列后会调用该接口进行主镜像队列状态初始化
	rabbit_amqqueue_process:init_with_backing_queue_state(
	  Q1, rabbit_mirror_queue_master, MasterState, RateTRef, Deliveries, KS1,
	  MTC).

%% We reset mandatory to false here because we will have sent the
%% mandatory_received already as soon as we got the message. We also
%% need to send an ack for these messages since the channel is waiting
%% for one for the via-GM case and we will not now receive one.
%% 副镜像队列转化为主镜像队列后，然后将rabbit_channel进程发送来的消息向rabbit_channel进程进行流量控制
promote_delivery(Delivery = #delivery{sender = Sender, flow = Flow}) ->
	case Flow of
		flow   -> credit_flow:ack(Sender);
		noflow -> ok
	end,
	Delivery#delivery{mandatory = false}.


noreply(State) ->
	{NewState, Timeout} = next_state(State),
	{noreply, ensure_rate_timer(NewState), Timeout}.


reply(Reply, State) ->
	{NewState, Timeout} = next_state(State),
	{reply, Reply, ensure_rate_timer(NewState), Timeout}.


%% 进行消息的confirm操作
next_state(State = #state{backing_queue = BQ, backing_queue_state = BQS}) ->
	%% 得到backing_queue中已经得到confirm的消息列表
	{MsgIds, BQS1} = BQ:drain_confirmed(BQS),
	%% 持久化消息已经存储到磁盘后的消息最后进行confirm操作
	State1 = confirm_messages(MsgIds,
							  State #state { backing_queue_state = BQS1 }),
	%% 判断是否需要进行同步confirm操作
	case BQ:needs_timeout(BQS1) of
		false -> {stop_sync_timer(State1),   hibernate     };
		idle  -> {stop_sync_timer(State1),   ?SYNC_INTERVAL};
		timed -> {ensure_sync_timer(State1), 0             }
	end.


%% 通知backing_queue进行confirm同步操作
backing_queue_timeout(State = #state { backing_queue       = BQ,
									   backing_queue_state = BQS }) ->
	State#state{backing_queue_state = BQ:timeout(BQS)}.


%% 启动同步confirm的定时器
ensure_sync_timer(State) ->
	rabbit_misc:ensure_timer(State, #state.sync_timer_ref,
							 ?SYNC_INTERVAL, sync_timeout).


%% 停止同步confirm的定时器
stop_sync_timer(State) -> rabbit_misc:stop_timer(State, #state.sync_timer_ref).


%% 启动速率监控的定时器
ensure_rate_timer(State) ->
	rabbit_misc:ensure_timer(State, #state.rate_timer_ref,
							 ?RAM_DURATION_UPDATE_INTERVAL,
							 update_ram_duration).


%% 停止速率监控的定时器
stop_rate_timer(State) -> rabbit_misc:stop_timer(State, #state.rate_timer_ref).


%% 确保监视发布消息的rabbit_channel进程
ensure_monitoring(ChPid, State = #state { known_senders = KS }) ->
	State #state { known_senders = pmon:monitor(ChPid, KS) }.


%% rabbit_channel进程挂掉，则从监视数据结构中将它删除掉
local_sender_death(ChPid, #state { known_senders = KS }) ->
	%% The channel will be monitored iff we have received a delivery
	%% from it but not heard about its death from the master. So if it
	%% is monitored we need to point the death out to the master (see
	%% essay).
	ok = case pmon:is_monitored(ChPid, KS) of
			 false -> ok;
			 %% 该函数不断的询问自己监视信息中是否存在Pid，如果存在则继续询问自己(该函数主要是通知主镜像队列察觉rabbit_channel进程的挂掉的情况)
			 true  -> confirm_sender_death(ChPid)
		 end.


%% 该函数不断的询问自己监视信息中是否存在Pid，如果存在则继续询问自己(该函数主要是通知主镜像队列察觉rabbit_channel进程的挂掉的情况)
confirm_sender_death(Pid) ->
	%% We have to deal with the possibility that we'll be promoted to
	%% master before this thing gets run. Consequently(因此) we set the
	%% module to rabbit_mirror_queue_master so that if we do become a
	%% rabbit_amqqueue_process before then, sane things will happen.
	%% 该函数不断的询问自己监视信息中是否存在Pid，如果存在则继续询问自己
	Fun =
		fun (?MODULE, State = #state { known_senders = KS,
									   gm            = GM }) ->
				 %% We're running still as a slave
				 %%
				 %% See comment in local_sender_death/2; we might have
				 %% received a sender_death in the meanwhile so check
				 %% again.
				 ok = case pmon:is_monitored(Pid, KS) of
						  false -> ok;
						  true  -> gm:broadcast(GM, {ensure_monitoring, [Pid]}),
								   confirm_sender_death(Pid)
					  end,
				 State;
		   (rabbit_mirror_queue_master, State) ->
				%% We've become a master. State is now opaque to
				%% us. When we became master, if Pid was still known
				%% to us then we'd have set up monitoring of it then,
				%% so this is now a noop.
				State
		end,
	%% Note that we do not remove our knowledge of this ChPid until we
	%% get the sender_death from GM as well as a DOWN notification.
	{ok, _TRef} = timer:apply_after(
					?DEATH_TIMEOUT, rabbit_amqqueue, run_backing_queue,
					[self(), rabbit_mirror_queue_master, Fun]),
	ok.


forget_sender(_, running)                        -> false;

forget_sender(down_from_gm, down_from_gm)        -> false; %% [1]

forget_sender(Down1, Down2) when Down1 =/= Down2 -> true.

%% [1] If another slave goes through confirm_sender_death/1 before we
%% do we can get two GM sender_death messages in a row for the same
%% channel - don't treat that as anything special.

%% Record and process lifetime events from channels. Forget all about a channel
%% only when down notifications are received from both the channel and from gm.
%% 清除rabbit_channel进程ChPid相关的信息
maybe_forget_sender(ChPid, ChState, State = #state { sender_queues = SQ,
													 msg_id_status = MS,
													 known_senders = KS }) ->
	case dict:find(ChPid, SQ) of
		error ->
			State;
		{ok, {MQ, PendCh, ChStateRecord}} ->
			case forget_sender(ChState, ChStateRecord) of
				true ->
					credit_flow:peer_down(ChPid),
					State #state { sender_queues = dict:erase(ChPid, SQ),
								   msg_id_status = lists:foldl(
													 fun dict:erase/2,
													 MS, sets:to_list(PendCh)),
								   known_senders = pmon:demonitor(ChPid, KS) };
				false ->
					SQ1 = dict:store(ChPid, {MQ, PendCh, ChState}, SQ),
					State #state { sender_queues = SQ1 }
			end
	end.


%% 处理由rabbit_channel进程直接发布过来的消息
maybe_enqueue_message(
  Delivery = #delivery { message = #basic_message { id = MsgId },
						 sender  = ChPid },
  State = #state { sender_queues = SQ, msg_id_status = MS }) ->
	%% 通知rabbit_channel进程消息队列已经得到该消息
	send_mandatory(Delivery), %% must do this before confirms
	%% 确保监视发布消息的rabbit_channel进程
	State1 = ensure_monitoring(ChPid, State),
	%% We will never see {published, ChPid, MsgSeqNo} here.
	case dict:find(MsgId, MS) of
		%% 消息队列循环镜像队列下属的GM进程还没有将消息发送过来的情况
		error ->
			{MQ, PendingCh, ChState} = get_sender_queue(ChPid, SQ),
			MQ1 = queue:in(Delivery, MQ),
			SQ1 = dict:store(ChPid, {MQ1, PendingCh, ChState}, SQ),
			State1 #state { sender_queues = SQ1 };
		%% 消息队列循环镜像队列下属的GM进程已经先将消息发送过来的情况
		{ok, Status} ->
			%% 处理消息的confirm
			MS1 = send_or_record_confirm(
					Status, Delivery, dict:erase(MsgId, MS), State1),
			%% 将MsgId等待rabbit_channel进程ChPid的Sets数据结构中删除掉
			SQ1 = remove_from_pending_ch(MsgId, ChPid, SQ),
			State1 #state { msg_id_status = MS1,
							sender_queues = SQ1 }
	end.


%% 获取rabbit_channel进程对应的队列信息
get_sender_queue(ChPid, SQ) ->
	case dict:find(ChPid, SQ) of
		error     -> {queue:new(), sets:new(), running};
		{ok, Val} -> Val
	end.


%% 将MsgId等待rabbit_channel进程ChPid的Sets数据结构中删除掉
remove_from_pending_ch(MsgId, ChPid, SQ) ->
	case dict:find(ChPid, SQ) of
		error ->
			SQ;
		{ok, {MQ, PendingCh, ChState}} ->
			dict:store(ChPid, {MQ, sets:del_element(MsgId, PendingCh), ChState},
					   SQ)
	end.


%% 处理循环镜像消息队列发送过来的发布或者丢弃的消息
publish_or_discard(Status, ChPid, MsgId,
				   State = #state { sender_queues = SQ, msg_id_status = MS }) ->
	%% We really are going to do the publish/discard right now, even
	%% though we may not have seen it directly from the channel. But
	%% we cannot issue confirms until the latter has happened. So we
	%% need to keep track of the MsgId and its confirmation status in
	%% the meantime.
	%% 监视rabbit_channel进程
	State1 = ensure_monitoring(ChPid, State),
	%% 获取rabbit_channel进程对应的队列信息
	{MQ, PendingCh, ChState} = get_sender_queue(ChPid, SQ),
	{MQ1, PendingCh1, MS1} =
		case queue:out(MQ) of
			%% 此处的情况是GM循环队列发布的消息先过来，则更新等待rabbit_channel进程发布消息的信息
			%% 将消息ID放入到等待rabbit_channel进程消息的数据结构中
			{empty, _MQ2} ->
				{MQ, sets:add_element(MsgId, PendingCh),
				 dict:store(MsgId, Status, MS)};
			%% 此处情况是rabbit_channel进程直接发送过来的消息比GM循环队列发送过来的消息先到(rabbit_channel进程发送的消息先到达)
			{{value, Delivery = #delivery {
										   message = #basic_message { id = MsgId } }}, MQ2} ->
				{MQ2, PendingCh,
				 %% We received the msg from the channel first. Thus
				 %% we need to deal with confirms here.
				 %% 处理消息的confirm操作
				 send_or_record_confirm(Status, Delivery, MS, State1)};
			%% 此处的情况异常(GM循环队列发送过来的数据和rabbit_channel进程发送过来的消息对不上)
			{{value, #delivery {}}, _MQ2} ->
				%% The instruction(指令) was sent to us before we were
				%% within the slave_pids within the #amqqueue{}
				%% record. We'll never receive the message directly
				%% from the channel. And the channel will not be
				%% expecting any confirms from us.
				{MQ, PendingCh, MS}
		end,
	%% 将rabbit_channel进程对应的信息重新存储到字典中
	SQ1 = dict:store(ChPid, {MQ1, PendingCh1, ChState}, SQ),
	%% 更新最新的sender_queues和msg_id_status字段
	State1 #state { sender_queues = SQ1, msg_id_status = MS1 }.


%% 副镜像队列处理主镜像队列发送过来的发布消息
process_instruction({publish, ChPid, Flow, MsgProps,
					 Msg = #basic_message { id = MsgId }}, State) ->
	%% 消息流控制
	maybe_flow_ack(ChPid, Flow),
	%% 处理循环镜像消息队列发送过来的发布或者丢弃的消息
	State1 = #state { backing_queue = BQ, backing_queue_state = BQS } =
						publish_or_discard(published, ChPid, MsgId, State),
	%% 调用副镜像队列的backing_queue模块进行消息的实际操作
	BQS1 = BQ:publish(Msg, MsgProps, true, ChPid, Flow, BQS),
	%% 更新最新的backing_queue状态信息
	{ok, State1 #state { backing_queue_state = BQS1 }};

%% 副镜像队列处理主镜像队列发送过来的已经将消息发送给消费者的消息
process_instruction({publish_delivered, ChPid, Flow, MsgProps,
					 Msg = #basic_message { id = MsgId }}, State) ->
	%% 消息流控制
	maybe_flow_ack(ChPid, Flow),
	%% 处理循环镜像消息队列发送过来的发布或者丢弃的消息
	State1 = #state { backing_queue = BQ, backing_queue_state = BQS } =
						publish_or_discard(published, ChPid, MsgId, State),
	%% 做断言处理，即当前队列中没有消息
	true = BQ:is_empty(BQS),
	%% 调用副镜像队列的backing_queue模块进行消息的实际操作
	{AckTag, BQS1} = BQ:publish_delivered(Msg, MsgProps, ChPid, Flow, BQS),
	{ok, maybe_store_ack(true, MsgId, AckTag,
						 State1 #state { backing_queue_state = BQS1 })};

%% 副镜像队列处理主镜像队列发送过来的丢弃消息的消息
process_instruction({discard, ChPid, Flow, MsgId}, State) ->
	%% 消息流控制
	maybe_flow_ack(ChPid, Flow),
	%% 处理循环镜像消息队列发送过来的发布或者丢弃的消息
	State1 = #state { backing_queue = BQ, backing_queue_state = BQS } =
						publish_or_discard(discarded, ChPid, MsgId, State),
	%% 调用副镜像队列的backing_queue模块进行消息的实际操作
	BQS1 = BQ:discard(MsgId, ChPid, Flow, BQS),
	{ok, State1 #state { backing_queue_state = BQS1 }};

%% 副镜像队列处理主镜像队列发送过来的丢弃队列中元素的消息
%% Length：表示当前消息队列中最新的消息数量
%% Dropped：表示丢弃的消息数量
%% AckRequired：表示是否需要进行ack操作
process_instruction({drop, Length, Dropped, AckRequired},
					State = #state { backing_queue       = BQ,
									 backing_queue_state = BQS }) ->
	%% 获得当前副镜像队列中的消息数量
	QLen = BQ:len(BQS),
	ToDrop = case QLen - Length of
				 N when N > 0 -> N;
				 _            -> 0
			 end,
	State1 = lists:foldl(
			   fun (const, StateN = #state{backing_queue_state = BQSN}) ->
						%% 调用副镜像队列的backing_queue模块进行消息的实际操作
						{{MsgId, AckTag}, BQSN1} = BQ:drop(AckRequired, BQSN),
						%% 存储需要进行ack操作的消息
						maybe_store_ack(
						  AckRequired, MsgId, AckTag,
						  StateN #state { backing_queue_state = BQSN1 })
			   end, State, lists:duplicate(ToDrop, const)),
	{ok, case AckRequired of
			 true  -> State1;
			 false -> update_delta(ToDrop - Dropped, State1)
		 end};

%% 副镜像队列处理主镜像队列发送过来的进行ack的消息
process_instruction({ack, MsgIds},
					State = #state { backing_queue       = BQ,
									 backing_queue_state = BQS,
									 msg_id_ack          = MA }) ->
	%% MsgIds列表中的消息已经得到ack，得到这些消息ID对应的ack标识
	{AckTags, MA1} = msg_ids_to_acktags(MsgIds, MA),
	%% 调用副镜像队列的backing_queue模块进行消息的实际操作
	{MsgIds1, BQS1} = BQ:ack(AckTags, BQS),
	%% 断言所有进行ack的消息全部成功
	[] = MsgIds1 -- MsgIds, %% ASSERTION
	{ok, update_delta(length(MsgIds1) - length(MsgIds),
					  State #state { msg_id_ack          = MA1,
									 backing_queue_state = BQS1 })};

%% 副镜像队列处理主镜像队列发送过来的将MsgIds重新放入到队列的消息
process_instruction({requeue, MsgIds},
					State = #state { backing_queue       = BQ,
									 backing_queue_state = BQS,
									 msg_id_ack          = MA }) ->
	%% 获取到这些消息ID对应的ack标识
	{AckTags, MA1} = msg_ids_to_acktags(MsgIds, MA),
	%% 调用副镜像队列的backing_queue模块进行消息的实际操作
	{_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
	{ok, State #state { msg_id_ack          = MA1,
						backing_queue_state = BQS1 }};

%% 主镜像队列发送过来rabbit_channel进程死亡的消息
process_instruction({sender_death, ChPid},
					State = #state { known_senders = KS }) ->
	%% The channel will be monitored iff we have received a message
	%% from it. In this case we just want to avoid doing work if we
	%% never got any messages.
	{ok, case pmon:is_monitored(ChPid, KS) of
			 false -> State;
			 true  -> maybe_forget_sender(ChPid, down_from_gm, State)
		 end};

%% 处理广播消息消息队列中长度的消息，包括等待ack的消息数量
process_instruction({depth, Depth},
					State = #state { backing_queue       = BQ,
									 backing_queue_state = BQS }) ->
	{ok, set_delta(Depth - BQ:depth(BQS), State)};

process_instruction({delete_and_terminate, Reason},
					State = #state { backing_queue       = BQ,
									 backing_queue_state = BQS }) ->
	BQ:delete_and_terminate(Reason, BQS),
	{stop, State #state { backing_queue_state = undefined }}.


%% 消息流控制
maybe_flow_ack(ChPid, flow)    -> credit_flow:ack(ChPid);

maybe_flow_ack(_ChPid, noflow) -> ok.


%% MsgIds列表中的消息已经得到ack，得到这些消息ID对应的ack标识
msg_ids_to_acktags(MsgIds, MA) ->
	{AckTags, MA1} =
		lists:foldl(
		  fun (MsgId, {Acc, MAN}) ->
				   case dict:find(MsgId, MA) of
					   error        -> {Acc, MAN};
					   {ok, AckTag} -> {[AckTag | Acc], dict:erase(MsgId, MAN)}
				   end
		  end, {[], MA}, MsgIds),
	{lists:reverse(AckTags), MA1}.


%% 存储需要进行ack操作的消息
maybe_store_ack(false, _MsgId, _AckTag, State) ->
	State;

%% 将需要进行ack操作的消息存储起来
maybe_store_ack(true, MsgId, AckTag, State = #state { msg_id_ack = MA }) ->
	State #state { msg_id_ack = dict:store(MsgId, AckTag, MA) }.


%% 设置当前副镜像队列中和主镜像队列中相差的消息数量
set_delta(0,        State = #state { depth_delta = undefined }) ->
	ok = record_synchronised(State#state.q),
	State #state { depth_delta = 0 };

set_delta(NewDelta, State = #state { depth_delta = undefined }) ->
	true = NewDelta > 0, %% assertion
	State #state { depth_delta = NewDelta };

set_delta(NewDelta, State = #state { depth_delta = Delta     }) ->
	update_delta(NewDelta - Delta, State).


%% 更新当前副镜像队列中和主镜像队列中相差的消息数量
update_delta(_DeltaChange, State = #state { depth_delta = undefined }) ->
	State;

update_delta( DeltaChange, State = #state { depth_delta = 0         }) ->
	0 = DeltaChange, %% assertion: we cannot become unsync'ed
	State;

update_delta( DeltaChange, State = #state { depth_delta = Delta     }) ->
	true = DeltaChange =< 0, %% assertion: we cannot become 'less' sync'ed
	set_delta(Delta + DeltaChange, State #state { depth_delta = undefined }).


%% 处理更新当前队列消息进入和出去的速率消息
update_ram_duration(BQ, BQS) ->
	{RamDuration, BQS1} = BQ:ram_duration(BQS),
	DesiredDuration =
		rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
	BQ:set_ram_duration_target(DesiredDuration, BQS1).


%% 将自己记录需要同步的副镜像队列进程的列表中
record_synchronised(#amqqueue { name = QName }) ->
	Self = self(),
	case rabbit_misc:execute_mnesia_transaction(
		   fun () ->
					case mnesia:read({rabbit_queue, QName}) of
						[] ->
							ok;
						[Q1 = #amqqueue { sync_slave_pids = SSPids }] ->
							Q2 = Q1#amqqueue{sync_slave_pids = [Self | SSPids]},
							rabbit_mirror_queue_misc:store_updated_slaves(Q2),
							{ok, Q2}
					end
		   end) of
		ok      -> ok;
		{ok, Q} -> rabbit_mirror_queue_misc:maybe_drop_master_after_sync(Q)
	end.
