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

-module(rabbit_queue_consumers).

-export([new/0, max_active_priority/1, inactive/1, all/1, count/0,
         unacknowledged_message_count/0, add/9, remove/3, erase_ch/2,
         send_drained/0, deliver/3, record_ack/3, subtract_acks/3,
         possibly_unblock/3,
         resume_fun/0, notify_sent_fun/1, activate_limit_fun/0,
         credit/6, utilisation/1]).

%%----------------------------------------------------------------------------
%% 向消息队列对应的rabbit_channel进程发送消息的上限，超过上限，则当前cr结构处于锁住状态
-define(UNSENT_MESSAGE_LIMIT,          200).

%% Utilisation(利用) average calculations are all in μs.
-define(USE_AVG_HALF_LIFE, 1000000.0).

%% 消费者模块存储于消息队列中的数据结构
-record(state, {consumers, use}).

%% 每个消费者的信息数据结构
-record(consumer, {
				   tag,															%% 消费者的标识
				   ack_required,												%% 当前消费者的消息是否需要进行ack操作
				   prefetch,													%% 预取的消息数量
				   args															%% 消费者的参数
				  }).

%% These are held in our process dictionary
%% 每个rabbit_channel进程在队列进程消费者模块中的数据结构
-record(cr, {
			 ch_pid,															%% rabbit_channel进程的Pid
			 monitor_ref,
			 acktags,															%% 等待消费者ack操作的消息集合
			 consumer_count,													%% rabbit_channel进程下对应的消费者数量
			 %% Queue of {ChPid, #consumer{}} for consumers which have
			 %% been blocked for any reason
			 blocked_consumers,													%% rabbit_channel进程下阻塞住的消费者集合
			 %% The limiter itself
			 limiter,															%% 保存注册到rabbit_channel进程对应的rabbit_limiter下的客户端数据结构
			 %% Internal flow control for queue -> writer
			 unsent_message_count												%% 当前消息队列向对应的rabbit_channel进程发送的消息数量
			}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type time_micros() :: non_neg_integer().
-type ratio() :: float().
-type state() :: #state{consumers ::priority_queue:q(),
                        use       :: {'inactive',
                                      time_micros(), time_micros(), ratio()} |
                                     {'active', time_micros(), ratio()}}.
-type ch() :: pid().
-type ack() :: non_neg_integer().
-type cr_fun() :: fun ((#cr{}) -> #cr{}).
-type fetch_result() :: {rabbit_types:basic_message(), boolean(), ack()}.

-spec new() -> state().
-spec max_active_priority(state()) -> integer() | 'infinity' | 'empty'.
-spec inactive(state()) -> boolean().
-spec all(state()) -> [{ch(), rabbit_types:ctag(), boolean(),
                        non_neg_integer(), rabbit_framing:amqp_table()}].
-spec count() -> non_neg_integer().
-spec unacknowledged_message_count() -> non_neg_integer().
-spec add(ch(), rabbit_types:ctag(), boolean(), pid(), boolean(),
          non_neg_integer(), rabbit_framing:amqp_table(), boolean(), state())
         -> state().
-spec remove(ch(), rabbit_types:ctag(), state()) ->
                    'not_found' | state().
-spec erase_ch(ch(), state()) ->
                      'not_found' | {[ack()], [rabbit_types:ctag()],
                                     state()}.
-spec send_drained() -> 'ok'.
-spec deliver(fun ((boolean()) -> {fetch_result(), T}),
              rabbit_amqqueue:name(), state()) ->
                     {'delivered',   boolean(), T, state()} |
                     {'undelivered', boolean(), state()}.
-spec record_ack(ch(), pid(), ack()) -> 'ok'.
-spec subtract_acks(ch(), [ack()], state()) ->
                           'not_found' | 'unchanged' | {'unblocked', state()}.
-spec possibly_unblock(cr_fun(), ch(), state()) ->
                              'unchanged' | {'unblocked', state()}.
-spec resume_fun()                       -> cr_fun().
-spec notify_sent_fun(non_neg_integer()) -> cr_fun().
-spec activate_limit_fun()               -> cr_fun().
-spec credit(boolean(), integer(), boolean(), ch(), rabbit_types:ctag(),
             state()) -> 'unchanged' | {'unblocked', state()}.
-spec utilisation(state()) -> ratio().

-endif.

%%----------------------------------------------------------------------------
%% 当创建一个新的队列的时候创建一个保存消费者状态的数据结构
new() -> #state{consumers = priority_queue:new(),
				use       = {active, now_micros(), 1.0}}.


%% 拿到优先级最高的消费者信息
max_active_priority(#state{consumers = Consumers}) ->
	priority_queue:highest(Consumers).


%% 查看是否有消费者
inactive(#state{consumers = Consumers}) ->
	priority_queue:is_empty(Consumers).


%% 得到当前消息队列进程中的所有消费者
all(#state{consumers = Consumers}) ->
	lists:foldl(fun (C, Acc) -> consumers(C#cr.blocked_consumers, Acc) end,
				consumers(Consumers, []), all_ch_record()).


%% 拿到Consumers这个优先级队列中所有的消费者
consumers(Consumers, Acc) ->
	priority_queue:fold(
	  fun ({ChPid, Consumer}, _P, Acc1) ->
			   #consumer{tag = CTag, ack_required = Ack, prefetch = Prefetch,
						 args = Args} = Consumer,
			   [{ChPid, CTag, Ack, Prefetch, Args} | Acc1]
	  end, Acc, Consumers).


%% 拿到当前队列中所有的消费者数量
count() -> lists:sum([Count || #cr{consumer_count = Count} <- all_ch_record()]).


%% 得到当前队列中所有的还没有被消费者ack的消息数量
unacknowledged_message_count() ->
	lists:sum([queue:len(C#cr.acktags) || C <- all_ch_record()]).


%% 对应队列中增加一个消费者(ChPid:为rabbit_channel进程Pid)
add(ChPid, CTag, NoAck, LimiterPid, LimiterActive, Prefetch, Args, IsEmpty,
	State = #state{consumers = Consumers,
				   use       = CUInfo}) ->
	%% 如果没有cr数据结构，则组装一个新的cr数据结构，否则直接从进程字典中读取
	C = #cr{consumer_count = Count,
			limiter        = Limiter} = ch_record(ChPid, LimiterPid),
	Limiter1 = case LimiterActive of
				   %% 如果rabbit_limiter进程是预取激活状态，则将该队列进程向rabbit_limiter进程进行激活
				   true  -> rabbit_limiter:activate(Limiter);
				   false -> Limiter
			   end,
	%% 将消费者数量增加一
	C1 = C#cr{consumer_count = Count + 1, limiter = Limiter1},
	%% 更新ch数据结构
	update_ch_record(
	  %% 解析消费者预取消息的数量，预取的消息数量以消费者传入的配置参数为准，然后以rabbit_channel进程中的配置的预取数为准
	  case parse_credit_args(Prefetch, Args) of
		  {0,       auto}            -> C1;
		  %% 不需要消费者进行ack，则不要设置消费者流量限制
		  {_Credit, auto} when NoAck -> C1;
		  {Credit,  Mode}            -> credit_and_drain(
										  C1, CTag, Credit, Mode, IsEmpty)
	  end),
	%% 组装consumer数据结构
	Consumer = #consumer{tag          = CTag,
						 ack_required = not NoAck,
						 prefetch     = Prefetch,
						 args         = Args},
	%% 更新消费者字段，同时更新use字段
	State#state{consumers = add_consumer({ChPid, Consumer}, Consumers),
				use       = update_use(CUInfo, active)}.


%% 根据消费者标识CTag删除消费者
remove(ChPid, CTag, State = #state{consumers = Consumers}) ->
	%% 先根据rabbit_channel进程的Pid得到当前队列中对应的cr数据结构
	case lookup_ch(ChPid) of
		not_found ->
			not_found;
		C = #cr{consumer_count    = Count,
				limiter           = Limiter,
				blocked_consumers = Blocked} ->
			%% 从阻塞消费者队列中根据CTag标识删除消费者
			Blocked1 = remove_consumer(ChPid, CTag, Blocked),
			%% 如果当前消息队列被删除的消费者是最后一个消费者，则如果rabbit_limiter全局激活，则需要向rabbit_limiter取消注册
			Limiter1 = case Count of
						   %% 当前队列中没有消费者，则从rabbit_limiter进程中解除激活状态
						   1 -> rabbit_limiter:deactivate(Limiter);
						   _ -> Limiter
					   end,
			%% 从rabbit_limiter进程的数据结构中删除CTag的消费者流量限制信息
			Limiter2 = rabbit_limiter:forget_consumer(Limiter1, CTag),
			%% 更新cr数据结构
			update_ch_record(C#cr{consumer_count    = Count - 1,
								  limiter           = Limiter2,
								  blocked_consumers = Blocked1}),
			State#state{consumers =
							%% 从当前未阻塞的消费者队列中删除掉CTag的消费者
							remove_consumer(ChPid, CTag, Consumers)}
	end.


%% 删除ChPid对应的cr数据结构
erase_ch(ChPid, State = #state{consumers = Consumers}) ->
	%% 先根据rabbit_channel进程的Pid得到当前队列中对应的cr数据结构
	case lookup_ch(ChPid) of
		not_found ->
			not_found;
		C = #cr{ch_pid            = ChPid,
				acktags           = ChAckTags,
				blocked_consumers = BlockedQ} ->
			%% 将ChPid对应的rabbit_channel对应的阻塞的消费者放入到未阻塞的消费者队列中
			All = priority_queue:join(Consumers, BlockedQ),
			%% 删除ch数据结构，从进程字典中抹掉ch结构
			ok = erase_ch_record(C),
			%% 过滤出ChPid对应的所有消费者
			Filtered = priority_queue:filter(chan_pred(ChPid, true), All),
			{%% 拿到ChPid对应的所有的消费者还没有ack的消息的AckTag列表
			 [AckTag || {AckTag, _CTag} <- queue:to_list(ChAckTags)],
			 %% 拿到ChPid下的所有消费者标识
			 tags(priority_queue:to_list(Filtered)),
			 %% 从消费者优先级队列中过滤掉ChPid对应的消费者
			 State#state{consumers = remove_consumers(ChPid, Consumers)}}
	end.


%% 队列中的消息为空后，将当前队列中credit中的mode为drain的消费者取出来，通知这些消费者队列中的消息为空
send_drained() -> [update_ch_record(send_drained(C)) || C <- all_ch_record()],
				  ok.


%% 尝试将消息发送给当前队列的消费者
deliver(FetchFun, QName, State) -> deliver(FetchFun, QName, false, State).


deliver(FetchFun, QName, ConsumersChanged,
		State = #state{consumers = Consumers}) ->
	%% 从最高优先级队列中得到一个元素
	case priority_queue:out_p(Consumers) of
		{empty, _} ->
			%% 当前消费者列表中为空的时候，则停止向消费者发送消息
			{undelivered, ConsumersChanged,
			 State#state{use = update_use(State#state.use, inactive)}};
		{{value, QEntry, Priority}, Tail} ->
			%% 尝试判断能否传递给等待消息的指定消费者
			case deliver_to_consumer(FetchFun, QEntry, QName) of
				{delivered, R} ->
					%% 将消息成功的传递到QEntry对应的消费者后，将消费者信息重新让入到优先级队列中
					{delivered, ConsumersChanged, R,
					 State#state{consumers = priority_queue:in(QEntry, Priority,
															   Tail)}};
				undelivered ->
					%% 继续搜索下一个能够接受消息的消费者
					deliver(FetchFun, QName, true,
							State#state{consumers = Tail})
			end
	end.


%% 尝试判断能否传递给等待消息的指定消费者
deliver_to_consumer(FetchFun, E = {ChPid, Consumer}, QName) ->
	%% 根据rabbit_channel进程的Pid得到该进程对应的cr结构
	C = lookup_ch(ChPid),
	%% 查看rabbit_channel进程对应的信息是否已经被锁住
	case is_ch_blocked(C) of
		%% 将消费者加入锁住blocked_consumers字段中
		true  -> block_consumer(C, E),
				 undelivered;
		false -> case rabbit_limiter:can_send(C#cr.limiter,
											  Consumer#consumer.ack_required,
											  Consumer#consumer.tag) of
					 {suspend, Limiter} ->
						 %% 将消费者加入锁住blocked_consumers字段中
						 block_consumer(C#cr{limiter = Limiter}, E),
						 undelivered;
					 {continue, Limiter} ->
						 %% 能够将消息发送到指定的消费者，则立刻将消息发送到指定的消费者
						 {delivered, deliver_to_consumer(
							FetchFun, Consumer,
							C#cr{limiter = Limiter}, QName)}
				 end
	end.


%% 所有的条件检查通过，直接将消息发送给CTag标识对应的消费者(执行到此处，是所有的条件都满足，能够将消息发送到CTag对应的消费者)
deliver_to_consumer(FetchFun,
					#consumer{tag          = CTag,
							  ack_required = AckRequired},
					C = #cr{ch_pid               = ChPid,
							acktags              = ChAckTags,
							unsent_message_count = Count},
					QName) ->
	%% 如果消息是要进行confirm操作，则需要将消息发布到backing_queue中(FetchFun函数是从backing_queue结构中读取消息)
	{{Message, IsDelivered, AckTag}, R} = FetchFun(AckRequired),
	%% 通知rabbit_channel进程将消息发送给CTag标识的消费者
	rabbit_channel:deliver(ChPid, CTag, AckRequired,
						   {QName, self(), AckTag, IsDelivered, Message}),
	%% 如果消息需要消费者进行ack操作，则将消息信息记录到acktags字段
	ChAckTags1 = case AckRequired of
					 true  -> queue:in({AckTag, CTag}, ChAckTags);
					 false -> ChAckTags
				 end,
	%% 更新acktags字段和unsent_message_count字段
	update_ch_record(C#cr{acktags              = ChAckTags1,
						  unsent_message_count = Count + 1}),
	R.


%% 记录需要消费者进行ack的消息
record_ack(ChPid, LimiterPid, AckTag) ->
	%% 根据rabbit_channel进程Pid得到cr数据结构
	C = #cr{acktags = ChAckTags} = ch_record(ChPid, LimiterPid),
	%% 更新ch数据结构
	update_ch_record(C#cr{acktags = queue:in({AckTag, none}, ChAckTags)}),
	ok.


%% 减去等待消费者ack操作的消息
subtract_acks(ChPid, AckTags, State) ->
	%% 根据rabbit_channel进程的Pid查找当前队列进程中的cr结构
	case lookup_ch(ChPid) of
		not_found ->
			not_found;
		C = #cr{acktags = ChAckTags, limiter = Lim} ->
			%% 将当前所有等待消费者ack操作的消息减去掉已经得到消费者ack操作的消息列表
			{CTagCounts, AckTags2} = subtract_acks(
									   AckTags, [], orddict:new(), ChAckTags),
			%% 更新对应的消费者Credit值，同时判断是否有以前阻塞的消费者变为非阻塞的消费者
			{Unblocked, Lim2} =
				orddict:fold(
				  fun (CTag, Count, {UnblockedN, LimN}) ->
						   {Unblocked1, LimN1} =
							   rabbit_limiter:ack_from_queue(LimN, CTag, Count),
						   {UnblockedN orelse Unblocked1, LimN1}
				  end, {false, Lim}, CTagCounts),
			C2 = C#cr{acktags = AckTags2, limiter = Lim2},
			%% Unblocked：表示有消费者ack之前是锁住，ack后获得解锁
			case Unblocked of
				%% 有以前是锁住状态的消费者进入了未锁住状态，因此需要将从锁住的数据结构中将未锁住的消费者拿到未锁住的数据结构中
				true  -> unblock(C2, State);
				%% 有消费者进行ack操作了，但是阻塞的消费者中没有进入未阻塞状态的消费者，则只更新cr结构
				false -> update_ch_record(C2),
						 unchanged
			end
	end.


%% 实际减去等待消费者ack列表的操作
subtract_acks([], [], CTagCounts, AckQ) ->
	{CTagCounts, AckQ};

subtract_acks([], Prefix, CTagCounts, AckQ) ->
	%% 将前面剩下的等待ack的消息重新放入到队列的头部
	{CTagCounts, queue:join(queue:from_list(lists:reverse(Prefix)), AckQ)};

%% 队列中等待ack的消息SeqId，是先出队列的是最小的等待ack的消息SeqId，所以可以从队列一个个出来同已经得到ack的消息进行对比
subtract_acks([T | TL] = AckTags, Prefix, CTagCounts, AckQ) ->
	case queue:out(AckQ) of
		{{value, {T, CTag}}, QTail} ->
			subtract_acks(TL, Prefix,
						  orddict:update_counter(CTag, 1, CTagCounts), QTail);
		{{value, V}, QTail} ->
			subtract_acks(AckTags, [V | Prefix], CTagCounts, QTail)
	end.


%% 有可能有消费者从锁住状态变为未锁住的状态的处理结果
possibly_unblock(Update, ChPid, State) ->
	%% 根据rabbit_channel进程的Pid查找当前队列进程中的cr结构
	case lookup_ch(ChPid) of
		not_found -> unchanged;
		C         -> %% 先执行更新函数
					 C1 = Update(C),
					 case is_ch_blocked(C) andalso not is_ch_blocked(C1) of
						 false -> update_ch_record(C1),
								  unchanged;
						 %% 老状态ch处于锁住状态，新的状态处于未锁住的状态，则去寻找是否有重新解锁的消费者出现
						 true  -> unblock(C1, State)
					 end
	end.


%% 解锁已经阻塞的消费者
unblock(C = #cr{blocked_consumers = BlockedQ, limiter = Limiter},
		State = #state{consumers = Consumers, use = Use}) ->
	%% 从cr结构中的已经阻塞队列中取出阻塞和没有阻塞的消费者
	case lists:partition(
		   fun({_P, {_ChPid, #consumer{tag = CTag}}}) ->
				   rabbit_limiter:is_consumer_blocked(Limiter, CTag)
		   end, priority_queue:to_list(BlockedQ)) of
		%% 该情况是锁住的消费者列表中没有一个重新获得解锁
		{_, []} ->
			update_ch_record(C),
			unchanged;
		{Blocked, Unblocked} ->
			BlockedQ1  = priority_queue:from_list(Blocked),
			UnblockedQ = priority_queue:from_list(Unblocked),
			%% 将剩余的阻塞的消费者更新到cr结构中的blocked_consumers字段
			update_ch_record(C#cr{blocked_consumers = BlockedQ1}),
			{unblocked,
			 %% 将重新获得解锁的消费者放入到consumers字段中
			 State#state{consumers = priority_queue:join(Consumers, UnblockedQ),
						 use       = update_use(Use, active)}}
	end.


%% 得到让rabbit_limiter进程从暂停状态变为激活状态的函数
resume_fun() ->
	fun (C = #cr{limiter = Limiter}) ->
			 C#cr{limiter = rabbit_limiter:resume(Limiter)}
	end.


%% 得到将unsent_message_count字段增加Credit个流量的函数
notify_sent_fun(Credit) ->
	fun (C = #cr{unsent_message_count = Count}) ->
			 C#cr{unsent_message_count = Count - Credit}
	end.


%% 得到将rabbit_limiter进程激活的函数
activate_limit_fun() ->
	fun (C = #cr{limiter = Limiter}) ->
			 C#cr{limiter = rabbit_limiter:activate(Limiter)}
	end.


credit(IsEmpty, Credit, Drain, ChPid, CTag, State) ->
	%% 先根据rabbit_channel进程的Pid得到对应的cr数据结构
	case lookup_ch(ChPid) of
		not_found ->
			unchanged;
		#cr{limiter = Limiter} = C ->
			C1 = #cr{limiter = Limiter1} =
						credit_and_drain(C, CTag, Credit, drain_mode(Drain), IsEmpty),
			case is_ch_blocked(C1) orelse
					 (not rabbit_limiter:is_consumer_blocked(Limiter, CTag)) orelse
					 rabbit_limiter:is_consumer_blocked(Limiter1, CTag) of
				true  -> update_ch_record(C1),
						 unchanged;
				%% 解锁已经阻塞的消费者
				false -> unblock(C1, State)
			end
	end.


%% drain：排水，流干
drain_mode(true)  -> drain;

%% manual：手工
drain_mode(false) -> manual.


%% utilisation：利用
utilisation(#state{use = {active, Since, Avg}}) ->
	use_avg(now_micros() - Since, 0, Avg);

utilisation(#state{use = {inactive, Since, Active, Avg}}) ->
	use_avg(Active, now_micros() - Since, Avg).

%%----------------------------------------------------------------------------
%% 解析消费者预取消息的数量，预取的消息数量以消费者传入的配置参数为准，然后以rabbit_channel进程中的配置的预取数为准
parse_credit_args(Default, Args) ->
	case rabbit_misc:table_lookup(Args, <<"x-credit">>) of
		{table, T} -> case {rabbit_misc:table_lookup(T, <<"credit">>),
							rabbit_misc:table_lookup(T, <<"drain">>)} of
						  {{long, C}, {bool, D}} -> {C, drain_mode(D)};
						  _                      -> {Default, auto}
					  end;
		undefined  -> {Default, auto}
	end.


%% 根据rabbit_channel进程的Pid查找当前队列进程中的cr结构
lookup_ch(ChPid) ->
	case get({ch, ChPid}) of
		undefined -> not_found;
		C         -> C
	end.


%% 得到ch数据机构(ChPid:为rabbit_channel进程的Pid)
ch_record(ChPid, LimiterPid) ->
	Key = {ch, ChPid},
	case get(Key) of
		%% 如果是rabbit_channel进程第一次到该队列来增加消费者，则创建一个新的cr数据结构，然后存储到进程字典中
		undefined -> %% 监视rabbit_channel进程
					 MonitorRef = erlang:monitor(process, ChPid),
					 %% 得到一个rabbit_limiter进程的客户端状态结构
					 Limiter = rabbit_limiter:client(LimiterPid),
					 C = #cr{ch_pid               = ChPid,
							 monitor_ref          = MonitorRef,
							 acktags              = queue:new(),
							 consumer_count       = 0,
							 blocked_consumers    = priority_queue:new(),
							 limiter              = Limiter,
							 unsent_message_count = 0},
					 put(Key, C),
					 C;
		C = #cr{} -> C
	end.


%% 更新ch数据结构(如果消费者数量，acktags为空，unsent_message_count为0则将cr结构删除掉)
update_ch_record(C = #cr{consumer_count       = ConsumerCount,
						 acktags              = ChAckTags,
						 unsent_message_count = UnsentMessageCount}) ->
	case {queue:is_empty(ChAckTags), ConsumerCount, UnsentMessageCount} of
		%% 如果当前等待ack的队列为空，则将当前cr进程字典删除掉
		{true, 0, 0} -> ok = erase_ch_record(C);
		%% 如果当前等待ack的队列不为空，则将当前最新的cr数据结构存储到进程字典中
		_            -> ok = store_ch_record(C)
	end,
	C.


%% 存储ch数据结构
store_ch_record(C = #cr{ch_pid = ChPid}) ->
	put({ch, ChPid}, C),
	ok.


%% 删除ch数据结构，从进程字典中抹掉ch结构
erase_ch_record(#cr{ch_pid = ChPid, monitor_ref = MonitorRef}) ->
	erlang:demonitor(MonitorRef),
	erase({ch, ChPid}),
	ok.


%% 拿到所有的cr数据结构
all_ch_record() -> [C || {{ch, _}, C} <- get()].


%% 将消费者加入锁住blocked_consumers字段中
block_consumer(C = #cr{blocked_consumers = Blocked}, QEntry) ->
	update_ch_record(C#cr{blocked_consumers = add_consumer(QEntry, Blocked)}).


%% 查看rabbit_channel进程对应的信息是否已经被锁住
is_ch_blocked(#cr{unsent_message_count = Count, limiter = Limiter}) ->
	Count >= ?UNSENT_MESSAGE_LIMIT orelse rabbit_limiter:is_suspended(Limiter).


%% 队列中的消息为空后，将当前队列中credit中的mode为drain的消费者取出来，通知这些消费者队列中的消息为空
send_drained(C = #cr{ch_pid = ChPid, limiter = Limiter}) ->
	%% 拿到所有credit结构中mode为drain的消费者
	case rabbit_limiter:drained(Limiter) of
		{[],         Limiter}  -> C;
		%% 通知对应的rabbit_channel进程
		{CTagCredit, Limiter2} -> rabbit_channel:send_drained(
									ChPid, CTagCredit),
								  C#cr{limiter = Limiter2}
	end.


%% 创建CTag对应的credit数据结构，如果当前队列中的消息为空，则通知对应的消费者
credit_and_drain(C = #cr{ch_pid = ChPid, limiter = Limiter},
				 CTag, Credit, Mode, IsEmpty) ->
	case rabbit_limiter:credit(Limiter, CTag, Credit, Mode, IsEmpty) of
		{true,  Limiter1} -> rabbit_channel:send_drained(ChPid,
														 [{CTag, Credit}]),
							 C#cr{limiter = Limiter1};
		{false, Limiter1} -> C#cr{limiter = Limiter1}
	end.


%% 根据消费者结构列表得到所有消费者的标识列表
tags(CList) -> [CTag || {_P, {_ChPid, #consumer{tag = CTag}}} <- CList].


%% 增加新的消费者到优先级队列中
add_consumer({ChPid, Consumer = #consumer{args = Args}}, Queue) ->
	%% 先拿到该消费者的优先级
	Priority = case rabbit_misc:table_lookup(Args, <<"x-priority">>) of
				   {_, P} -> P;
				   _      -> 0
			   end,
	%% 将消费者放入到优先级队列中
	priority_queue:in({ChPid, Consumer}, Priority, Queue).


%% 根据CTag消费者标识删除消费者
remove_consumer(ChPid, CTag, Queue) ->
	priority_queue:filter(fun ({CP, #consumer{tag = CT}}) ->
								   (CP /= ChPid) or (CT /= CTag)
						  end, Queue).


%% 从消费者优先级队列中过滤掉ChPid对应的消费者
remove_consumers(ChPid, Queue) ->
	priority_queue:filter(chan_pred(ChPid, false), Queue).


%% 过滤函数
chan_pred(ChPid, Want) ->
	fun ({CP, _Consumer}) when CP =:= ChPid -> Want;
	   (_)                                 -> not Want
	end.


%% 更新消费者结构中的use字段
update_use({inactive, _, _, _}   = CUInfo, inactive) ->
	CUInfo;

update_use({active,   _, _}      = CUInfo,   active) ->
	CUInfo;

update_use({active,   Since,         Avg}, inactive) ->
	Now = now_micros(),
	{inactive, Now, Now - Since, Avg};

update_use({inactive, Since, Active, Avg},   active) ->
	Now = now_micros(),
	{active, Now, use_avg(Active, Now - Since, Avg)}.


use_avg(Active, Inactive, Avg) ->
	Time = Inactive + Active,
	rabbit_misc:moving_average(Time, ?USE_AVG_HALF_LIFE, Active / Time, Avg).


now_micros() -> timer:now_diff(now(), {0, 0, 0}).
