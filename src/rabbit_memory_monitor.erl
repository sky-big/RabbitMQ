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


%% This module handles the node-wide memory statistics.
%% It receives statistics from all queues, counts the desired(想要的)
%% queue length (in seconds), and sends this information back to
%% queues.

-module(rabbit_memory_monitor).

-behaviour(gen_server2).

-export([start_link/0, register/2, deregister/1,
         report_ram_duration/2, stop/0, conserve_resources/3, memory_use/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(process, {pid,				  %% 消息队列进程的Pid
				  reported,			  %% 消息队列进程自己报告的内存使用持续时间
				  sent,				  %% 最后一次发送给消息队列当前rabbit_memory_monitor进程期望的内存使用速率
				  callback,			  %% 注册时的回调函数
				  monitor			  %% rabbit_memory_monitor进程监视该队列进程的标识
				 }).

-record(state, {timer,                %% 'internal_update' timer				%% 定时检查更新的定时器
                queue_durations,      %% ets #process							%% 队列对应的信息process数据结构对应的ETS表
                queue_duration_sum,   %% sum of all queue_durations				%% 所有消息队列的持续时间之和
                queue_duration_count, %% number of elements in sum				%% 总的速率中消息队列占有的数量
                desired_duration,     %% the desired queue duration				%% 消息队列期望的消息速率
                disk_alarm            %% disable paging, disk alarm has fired	%% 是否有磁盘报警的状态信息
               }).

-define(SERVER, ?MODULE).
-define(DEFAULT_UPDATE_INTERVAL, 2500).
-define(TABLE_NAME, ?MODULE).

%% If all queues are pushed to disk (duration 0), then the sum of
%% their reported lengths will be 0. If memory then becomes available,
%% unless we manually intervene, the sum will remain 0, and the queues
%% will never get a non-zero duration.  Thus when the mem use is <
%% SUM_INC_THRESHOLD, increase the sum artificially by SUM_INC_AMOUNT.
-define(SUM_INC_THRESHOLD, 0.95).
-define(SUM_INC_AMOUNT, 1.0).

-define(EPSILON, 0.000001). %% less than this and we clamp to 0

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(register/2 :: (pid(), {atom(),atom(),[any()]}) -> 'ok').
-spec(deregister/1 :: (pid()) -> 'ok').
-spec(report_ram_duration/2 ::
        (pid(), float() | 'infinity') -> number() | 'infinity').
-spec(stop/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
%% rabbit_memory_monitor进程的启动函数入口
start_link() ->
	gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).


%% 注册接口
register(Pid, MFA = {_M, _F, _A}) ->
	gen_server2:call(?SERVER, {register, Pid, MFA}, infinity).


%% deregister：取消，取消注册的接口
deregister(Pid) ->
	gen_server2:cast(?SERVER, {deregister, Pid}).


%% Pid为队列进程Pid，此接口是通报Pid对应的队列进程的内存速率使用情况
report_ram_duration(Pid, QueueDuration) ->
	gen_server2:call(?SERVER,
					 {report_ram_duration, Pid, QueueDuration}, infinity).


%% 停止rabbit_memory_monitor进程的接口
stop() ->
	gen_server2:cast(?SERVER, stop).


%% Conserve为true，表示磁盘使用已经报警，Conserve为false，表示磁盘的使用警报解除
%% 该接口是向rabbit_alarm进程注册后的回调接口
conserve_resources(Pid, disk, Conserve) ->
	gen_server2:cast(Pid, {disk_alarm, Conserve});
conserve_resources(_Pid, _Source, _Conserve) ->
	ok.


%% 得到当前RabbitMQ系统能够使用的内存上限
memory_use(bytes) ->
	%% 从vm_memory_monitor进程取得配置文件乘以虚拟机能够使用的内存上限后的数量(即当前RabbitMQ系统能够使用的内存上限)
	MemoryLimit = vm_memory_monitor:get_memory_limit(),
	{erlang:memory(total), case MemoryLimit > 0.0 of
							   true  -> MemoryLimit;
							   false -> infinity
						   end};
%% 得到当前虚拟机使用的内存大小占内存上限的百分比
memory_use(ratio) ->
	%% 从vm_memory_monitor进程取得配置文件乘以虚拟机能够使用的内存上限后的数量(即当前RabbitMQ系统能够使用的内存上限)
	MemoryLimit = vm_memory_monitor:get_memory_limit(),
	case MemoryLimit > 0.0 of
		true  -> erlang:memory(total) / MemoryLimit;
		false -> infinity
	end.

%%----------------------------------------------------------------------------
%% Gen_server callbacks
%%----------------------------------------------------------------------------
%% rabbit_memeory_monitor进程启动的回调初始化函数
init([]) ->
	%% 启动定时更新的定时器
	{ok, TRef} = timer:send_interval(?DEFAULT_UPDATE_INTERVAL, update),
	
	Ets = ets:new(?TABLE_NAME, [set, private, {keypos, #process.pid}]),
	%% 向rabbit_alarm进程注册，如果有报警信息，则会回调
	Alarms = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
	{ok, internal_update(
	   #state { timer                = TRef,
				queue_durations      = Ets,
				queue_duration_sum   = 0.0,
				queue_duration_count = 0,
				desired_duration     = infinity,
				disk_alarm           = lists:member(disk, Alarms)})}.


%% 处理队列进程报告内存使用速率(desired：渴望的)
handle_call({report_ram_duration, Pid, QueueDuration}, From,
			State = #state { queue_duration_sum = Sum,
							 queue_duration_count = Count,
							 queue_durations = Durations,
							 desired_duration = SendDuration }) ->
	%% 查找出带队列进程的信息
	[Proc = #process { reported = PrevQueueDuration }] =
		ets:lookup(Durations, Pid),
	
	gen_server2:reply(From, SendDuration),
	
	{Sum1, Count1} =
		case {PrevQueueDuration, QueueDuration} of
			{infinity, infinity} -> {Sum, Count};
			{infinity, _}        -> {Sum + QueueDuration,    Count + 1};
			{_, infinity}        -> {Sum - PrevQueueDuration, Count - 1};
			{_, _}               -> {Sum - PrevQueueDuration + QueueDuration,
									 Count}
		end,
	%% 将队列对应的信息更新后插入ETS表
	true = ets:insert(Durations, Proc #process { reported = QueueDuration,
												 sent = SendDuration }),
	{noreply, State #state { queue_duration_sum = zero_clamp(Sum1),
							 queue_duration_count = Count1 }};


%% 处理注册消息
handle_call({register, Pid, MFA}, _From,
			State = #state { queue_durations = Durations }) ->
	%% 监视Pid进程
	MRef = erlang:monitor(process, Pid),
	%% 将注册信息process插入ETS表中
	true = ets:insert(Durations, #process { pid = Pid, reported = infinity,
											sent = infinity, callback = MFA,
											monitor = MRef }),
	{reply, ok, State};


handle_call(_Request, _From, State) ->
	{noreply, State}.


%% Alarm为true，表示磁盘使用已经报警，Alarm为false，表示磁盘的使用警报解除
handle_cast({disk_alarm, Alarm}, State = #state{disk_alarm = Alarm}) ->
	{noreply, State};


%% Alarm为true，表示磁盘使用已经报警，Alarm为false，表示磁盘的使用警报解除
handle_cast({disk_alarm, Alarm}, State) ->
	{noreply, internal_update(State#state{disk_alarm = Alarm})};


%% 处理队列进程取消注册的消息
handle_cast({deregister, Pid}, State) ->
	{noreply, internal_deregister(Pid, true, State)};


handle_cast(stop, State) ->
	{stop, normal, State};


handle_cast(_Request, State) ->
	{noreply, State}.


handle_info(update, State) ->
	{noreply, internal_update(State)};


%% 队列进程直接挂掉后的回调，将该队列进程取消注册
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
	{noreply, internal_deregister(Pid, false, State)};


handle_info(_Info, State) ->
	{noreply, State}.


terminate(_Reason, #state { timer = TRef }) ->
	timer:cancel(TRef),
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------
zero_clamp(Sum) when Sum < ?EPSILON -> 0.0;
zero_clamp(Sum)                     -> Sum.


%% rabbit_memory_monitor进程内部的取消注册的接口
internal_deregister(Pid, Demonitor,
					State = #state { queue_duration_sum = Sum,
									 queue_duration_count = Count,
									 queue_durations = Durations }) ->
	case ets:lookup(Durations, Pid) of
		[] -> State;
		[#process { reported = PrevQueueDuration, monitor = MRef }] ->
			true = case Demonitor of
					   %% 取消对Pid进程的监视
					   true  -> erlang:demonitor(MRef);
					   false -> true
				   end,
			{Sum1, Count1} =
				case PrevQueueDuration of
					infinity -> {Sum, Count};
					_        -> {zero_clamp(Sum - PrevQueueDuration),
								 Count - 1}
				end,
			%% 将Pid对应的信息从ETS表中删除掉
			true = ets:delete(Durations, Pid),
			State #state { queue_duration_sum = Sum1,
						   queue_duration_count = Count1 }
	end.


%% rabbit_memory_monitor进程内部的定时更新操作函数
internal_update(State = #state{queue_durations  = Durations,
							   desired_duration = DesiredDurationAvg,
							   disk_alarm       = DiskAlarm}) ->
	%% 得到消息队列期望的内存使用持续时间
	DesiredDurationAvg1 = desired_duration_average(State),
	%% 得到判断是否需要通知队列进程的函数
	ShouldInform = should_inform_predicate(DiskAlarm),
	%% 如果没有报警信息，则DesiredDurationAvg大于新的DesiredDurationAvg1则通知所有的消息队列新的内存持续时间，反之
	case ShouldInform(DesiredDurationAvg, DesiredDurationAvg1) of
		true  -> inform_queues(ShouldInform, DesiredDurationAvg1, Durations);
		false -> ok
	end,
	State#state{desired_duration = DesiredDurationAvg1}.


%% desired：想要的
%% 得到消息队列期望的内存使用持续时间
desired_duration_average(#state{disk_alarm           = true}) ->
	infinity;
desired_duration_average(#state{disk_alarm           = false,
								queue_duration_sum   = Sum,
								queue_duration_count = Count}) ->
	%% 得到配置文件配置的RabbitMQ系统使用的内存占配置内存上限的百分比
	{ok, LimitThreshold} =
		application:get_env(rabbit, vm_memory_high_watermark_paging_ratio),
	%% 得到当前虚拟机使用的内存大小占设置的内存上限的百分比
	MemoryRatio = memory_use(ratio),
	if MemoryRatio =:= infinity ->
		   0.0;
	   %% 如果当前系统内存使用率少于配置文件中vm_memory_high_watermark_paging_ratio或者Count为0，则消息队列使用内存不用限制
	   MemoryRatio < LimitThreshold orelse Count == 0 ->
		   infinity;
	   MemoryRatio < ?SUM_INC_THRESHOLD ->
		   ((Sum + ?SUM_INC_AMOUNT) / Count) / MemoryRatio;
	   true ->
		   (Sum / Count) / MemoryRatio
	end.


%% 通知当前节点的所有消息队列最新的内存持续时间
inform_queues(ShouldInform, DesiredDurationAvg, Durations) ->
	true =
		ets:foldl(
		  fun (Proc = #process{reported = QueueDuration,
							   sent     = PrevSendDuration,
							   callback = {M, F, A}}, true) ->
				   case ShouldInform(PrevSendDuration, DesiredDurationAvg)
							andalso ShouldInform(QueueDuration, DesiredDurationAvg) of
					   true  -> ok = erlang:apply(
									   M, F, A ++ [DesiredDurationAvg]),
								ets:insert(
								  Durations,
								  Proc#process{sent = DesiredDurationAvg});
					   false -> true
				   end
		  end, true, Durations).


%% In normal use, we only inform queues immediately if the desired
%% duration has decreased, we want to ensure timely paging.
should_inform_predicate(false) -> fun greater_than/2;
%% When the disk alarm has gone off though, we want to inform queues
%% immediately if the desired duration has *increased* - we want to
%% ensure timely stopping paging.
should_inform_predicate(true) ->  fun (D1, D2) -> greater_than(D2, D1) end.


greater_than(infinity, infinity) -> false;
greater_than(infinity, _D2)      -> true;
greater_than(_D1,      infinity) -> false;
greater_than(D1,       D2)       -> D1 > D2.
