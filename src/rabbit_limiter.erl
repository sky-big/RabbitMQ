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

%% The purpose of the limiter is to stem(阻止) the flow of messages from
%% queues to channels, in order to act upon various protocol-level
%% flow control mechanisms, specifically AMQP 0-9-1's basic.qos
%% prefetch_count, our consumer prefetch extension, and AMQP 1.0's
%% link (aka consumer) credit mechanism.
%%
%% Each channel has an associated(相关的) limiter process, created with
%% start_link/1, which it passes to queues on consumer creation with
%% rabbit_amqqueue:basic_consume/10, and rabbit_amqqueue:basic_get/4.
%% The latter(后者) isn't strictly(严格的) necessary, since basic.get is not
%% subject to limiting, but it means that whenever a queue knows about
%% a channel, it also knows about its limiter, which is less fiddly.
%%
%% The limiter process holds state that is, in effect(实际上), shared between
%% the channel and all queues from which the channel is
%% consuming. Essentially(本质上) all these queues are competing(竞争) for access to
%% a single, limited resource - the ability to deliver messages via
%% the channel - and it is the job of the limiter process to mediate
%% that access.
%%
%% The limiter process is separate from the channel process for two
%% reasons: separation of concerns(关注点), and efficiency(效率). Channels can get
%% very busy, particularly if they are also dealing with publishes.
%% With a separate limiter process all the aforementioned(之前提到的) access
%% mediation can take place without touching the channel.
%%
%% For efficiency, both the channel and the queues keep some local
%% state, initialised(初始化) from the limiter pid with new/1 and client/1,
%% respectively(分别). In particular(特别是) this allows them to avoid any
%% interaction(交互) with the limiter process when it is 'inactive(不活跃的)', i.e. no
%% protocol-level flow control is taking place.
%%
%% This optimisation does come at the cost of some complexity though:
%% when a limiter becomes active, the channel needs to inform all its
%% consumer queues of this change in status. It does this by invoking
%% rabbit_amqqueue:activate_limit_all/2. Note that there is no inverse
%% transition, i.e. once a queue has been told about an active
%% limiter, it is not subsequently told when that limiter becomes
%% inactive. In practice it is rare for that to happen, though we
%% could optimise this case in the future.
%%
%% Consumer credit (for AMQP 1.0) and per-consumer prefetch (for AMQP
%% 0-9-1) are treated as essentially the same thing, but with the
%% exception that per-consumer prefetch gets an auto-topup when
%% acknowledgments come in.
%%
%% The bookkeeping for this is local to queues, so it is not necessary
%% to store information about it in the limiter process. But for
%% abstraction(抽象) we hide it from the queue behind the limiter API, and
%% it therefore becomes part of the queue local state.
%%
%% The interactions(交互) with the limiter are as follows:
%%
%% 1. Channels tell the limiter about basic.qos prefetch counts -
%%    that's what the limit_prefetch/3, unlimit_prefetch/1,
%%    get_prefetch_limit/1 API functions are about. They also tell the
%%    limiter queue state (via the queue) about consumer credit
%%    changes and message acknowledgement - that's what credit/5 and
%%    ack_from_queue/3 are for.
%%
%% 2. Queues also tell the limiter queue state about the queue
%%    becoming empty (via drained/1) and consumers leaving (via
%%    forget_consumer/2).
%%
%% 3. Queues register with the limiter - this happens as part of
%%    activate/1.
%%
%% 4. The limiter process maintains an internal counter of 'messages
%%    sent but not yet acknowledged', called the 'volume'.
%%
%% 5. Queues ask the limiter for permission (with can_send/3) whenever
%%    they want to deliver a message to a channel. The limiter checks
%%    whether a) the volume has not yet reached the prefetch limit,
%%    and b) whether the consumer has enough credit. If so it
%%    increments the volume and tells the queue to proceed. Otherwise
%%    it marks the queue as requiring notification (see below) and
%%    tells the queue not to proceed.
%%
%% 6. A queue that has been told to proceed (by the return value of
%%    can_send/3) sends the message to the channel. Conversely, a
%%    queue that has been told not to proceed, will not attempt to
%%    deliver that message, or any future messages, to the
%%    channel. This is accomplished by can_send/3 capturing the
%%    outcome in the local state, where it can be accessed with
%%    is_suspended/1.
%%
%% 7. When a channel receives an ack it tells the limiter (via ack/2)
%%    how many messages were ack'ed. The limiter process decrements
%%    the volume and if it falls below the prefetch_count then it
%%    notifies (through rabbit_amqqueue:resume/2) all the queues
%%    requiring notification, i.e. all those that had a can_send/3
%%    request denied.
%%
%% 8. Upon receipt of such a notification, queues resume delivery to
%%    the channel, i.e. they will once again start asking limiter, as
%%    described in (5).
%%
%% 9. When a queue has no more consumers associated with a particular
%%    channel, it deactivates use of the limiter with deactivate/1,
%%    which alters the local state such that no further interactions
%%    with the limiter process take place until a subsequent
%%    activate/1.

-module(rabbit_limiter).

-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/1]).
%% channel API
-export([new/1, limit_prefetch/3, unlimit_prefetch/1, is_active/1,
         get_prefetch_limit/1, ack/2, pid/1]).
%% queue API
-export([client/1, activate/1, can_send/3, resume/1, deactivate/1,
         is_suspended/1, is_consumer_blocked/2, credit/5, ack_from_queue/3,
         drained/1, forget_consumer/2]).
%% callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, prioritise_call/4]).

%%----------------------------------------------------------------------------
%% 保存在rabbit_channel进程中的rabbit_limiter进程的数据结构
-record(lstate, {pid, prefetch_limited}).

%% rabbit_limiter进程的客户端状态数据机构
-record(qstate, {pid, state, credits}).

-ifdef(use_specs).

-type(lstate() :: #lstate{pid              :: pid(),
                          prefetch_limited :: boolean()}).
-type(qstate() :: #qstate{pid :: pid(),
                          state :: 'dormant' | 'active' | 'suspended'}).

-type(credit_mode() :: 'manual' | 'drain' | 'auto').

-spec(start_link/1 :: (rabbit_types:proc_name()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(new/1 :: (pid()) -> lstate()).

-spec(limit_prefetch/3      :: (lstate(), non_neg_integer(), non_neg_integer())
                               -> lstate()).
-spec(unlimit_prefetch/1    :: (lstate()) -> lstate()).
-spec(is_active/1           :: (lstate()) -> boolean()).
-spec(get_prefetch_limit/1  :: (lstate()) -> non_neg_integer()).
-spec(ack/2                 :: (lstate(), non_neg_integer()) -> 'ok').
-spec(pid/1                 :: (lstate()) -> pid()).

-spec(client/1       :: (pid()) -> qstate()).
-spec(activate/1     :: (qstate()) -> qstate()).
-spec(can_send/3     :: (qstate(), boolean(), rabbit_types:ctag()) ->
                             {'continue' | 'suspend', qstate()}).
-spec(resume/1       :: (qstate()) -> qstate()).
-spec(deactivate/1   :: (qstate()) -> qstate()).
-spec(is_suspended/1 :: (qstate()) -> boolean()).
-spec(is_consumer_blocked/2 :: (qstate(), rabbit_types:ctag()) -> boolean()).
-spec(credit/5 :: (qstate(), rabbit_types:ctag(), non_neg_integer(),
                   credit_mode(), boolean()) -> {boolean(), qstate()}).
-spec(ack_from_queue/3 :: (qstate(), rabbit_types:ctag(), non_neg_integer())
                          -> {boolean(), qstate()}).
-spec(drained/1 :: (qstate())
                   -> {[{rabbit_types:ctag(), non_neg_integer()}], qstate()}).
-spec(forget_consumer/2 :: (qstate(), rabbit_types:ctag()) -> qstate()).

-endif.

%%----------------------------------------------------------------------------
%% rabbit_limiter进程中保存的数据结构
-record(lim, {
			  prefetch_count = 0,
			  ch_pid,																%% rabbit_channel进程的Pid
			  %% 'Notify' is a boolean that indicates whether a queue should be
			  %% notified of a change in the limit or volume that may allow it to
			  %% deliver more messages via the limiter's channel.
			  queues = orddict:new(), % QPid -> {MonitorRef, Notify}
			  volume = 0
			 }).

%% mode is of type credit_mode()
-record(credit, {credit = 0, mode}).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------
%% 在rabbit_channel_sup监督进程下启动rabbit_limiter进程
start_link(ProcName) -> gen_server2:start_link(?MODULE, [ProcName], []).


%% rabbit_channel进程初始化的时候调用过来初始化Pid对应的rabbit_limiter进程
new(Pid) ->
	%% this a 'call' to ensure that it is invoked(调用) at most once.
	ok = gen_server:call(Pid, {new, self()}, infinity),
	%% 返回给rabbit_channel进程lstate结构，该结构保存在rabbit_channel进程中的信息结构中
	#lstate{pid = Pid, prefetch_limited = false}.


%% 设置限制对应rabbit_channel进程预取的数量
limit_prefetch(L, PrefetchCount, UnackedCount) when PrefetchCount > 0 ->
	ok = gen_server:call(
		   L#lstate.pid,
		   {limit_prefetch, PrefetchCount, UnackedCount}, infinity),
	L#lstate{prefetch_limited = true}.


%% 设置取消对应rabbit_channel进程预取的数量
unlimit_prefetch(L) ->
	ok = gen_server:call(L#lstate.pid, unlimit_prefetch, infinity),
	L#lstate{prefetch_limited = false}.


%% 得到prefetch_limited字段的值
is_active(#lstate{prefetch_limited = Limited}) -> Limited.


%% 得到对应rabbit_channel进程对应的消息预取数量
get_prefetch_limit(#lstate{prefetch_limited = false}) -> 0;

get_prefetch_limit(L) ->
	gen_server:call(L#lstate.pid, get_prefetch_limit, infinity).


%% 消费者进行ack操作后，立刻通知rabbit_channel进程对应的rabbit_limiter进程
ack(#lstate{prefetch_limited = false}, _AckCount) -> ok;

ack(L, AckCount) -> gen_server:cast(L#lstate.pid, {ack, AckCount}).


%% rabbit_channel进程中得到rabbit_limiter进程的Pid接口
pid(#lstate{pid = Pid}) -> Pid.


%% 得到rabbit_limiter进程的客户端状态数据机构(dormant:休眠)
client(Pid) -> #qstate{pid = Pid, state = dormant, credits = gb_trees:empty()}.


%% 将队列进程注册到rabbit_limiter进程
activate(L = #qstate{state = dormant}) ->
	ok = gen_server:cast(L#qstate.pid, {register, self()}),
	%% 队列客户端结构中的状态设置为active激活状态
	L#qstate{state = active};

activate(L) -> L.


%% 查看能否向CTag对应的消费者标识的消费者发送消息
can_send(L = #qstate{pid = Pid, state = State, credits = Credits},
		 AckRequired, CTag) ->
	%% 查看消费者是否被锁住
	case is_consumer_blocked(L, CTag) of
		false -> case (State =/= active orelse
						   safe_call(Pid, {can_send, self(), AckRequired}, true)) of
					 %% rabbit_limiter进程中没有全局的锁住状态
					 true  -> Credits1 = decrement_credit(CTag, Credits),
							  {continue, L#qstate{credits = Credits1}};
					 %% rabbit_limiter进程中有全局的锁住状态，将当前队列设置为暂停状态
					 false -> {suspend,  L#qstate{state = suspended}}
				 end;
		%% 消费者自己的流量数量已经用完，则返回暂停状态
		true  -> {suspend, L}
	end.


safe_call(Pid, Msg, ExitValue) ->
	rabbit_misc:with_exit_handler(
	  fun () -> ExitValue end,
	  fun () -> gen_server2:call(Pid, Msg, infinity) end).


resume(L = #qstate{state = suspended}) ->
	L#qstate{state = active};

resume(L) -> L.


%% deactivate：禁用
deactivate(L = #qstate{state = dormant}) -> L;

deactivate(L) ->
	%% 队列进程取消注册
	ok = gen_server:cast(L#qstate.pid, {unregister, self()}),
	L#qstate{state = dormant}.


%% suspended：暂停
%% 判断当前客户端状态是否是暂停状态
is_suspended(#qstate{state = suspended}) -> true;

is_suspended(#qstate{})                  -> false.


%% 查看消费者是否被锁住
is_consumer_blocked(#qstate{credits = Credits}, CTag) ->
	case gb_trees:lookup(CTag, Credits) of
		none                                    -> false;
		{value, #credit{credit = C}} when C > 0 -> false;
		{value, #credit{}}                      -> true
	end.


%% 创建CTag标识的消费者对应的credit数据结构
credit(Limiter = #qstate{credits = Credits}, CTag, Crd, Mode, IsEmpty) ->
	{Res, Cr} =
		case IsEmpty andalso Mode =:= drain of
			%% 如果当前队列中的消息为空，通知Mode为drain，则将credit结构中的credit设置为0，同时mode为manual
			true  -> {true,  #credit{credit = 0,   mode = manual}};
			false -> {false, #credit{credit = Crd, mode = Mode}}
		end,
	{Res, Limiter#qstate{credits = enter_credit(CTag, Cr, Credits)}}.


%% 队列进程通知消费者进行ack操作的数量
ack_from_queue(Limiter = #qstate{credits = Credits}, CTag, Credit) ->
	{Credits1, Unblocked} =
		case gb_trees:lookup(CTag, Credits) of
			{value, C = #credit{mode = auto, credit = C0}} ->
				{update_credit(CTag, C#credit{credit = C0 + Credit}, Credits),
				 %% 此处是判断是否是该消费者以前是阻塞状态，增加Credit后变为非阻塞状态
				 C0 =:= 0 andalso Credit =/= 0};
			_ ->
				{Credits, false}
		end,
	{Unblocked, Limiter#qstate{credits = Credits1}}.


%% 拿到所有credit结构中mode为drain的消费者
drained(Limiter = #qstate{credits = Credits}) ->
	Drain = fun(C) -> C#credit{credit = 0, mode = manual} end,
	{CTagCredits, Credits2} =
		rabbit_misc:gb_trees_fold(
		  fun (CTag, C = #credit{credit = Crd, mode = drain},  {Acc, Creds0}) ->
				   {[{CTag, Crd} | Acc], update_credit(CTag, Drain(C), Creds0)};
			 (_CTag,   #credit{credit = _Crd, mode = _Mode}, {Acc, Creds0}) ->
				  {Acc, Creds0}
		  end, {[], Credits}, Credits),
	{CTagCredits, Limiter#qstate{credits = Credits2}}.


%% 删除消费者标识符为CTag对应的流量限制
forget_consumer(Limiter = #qstate{credits = Credits}, CTag) ->
	Limiter#qstate{credits = gb_trees:delete_any(CTag, Credits)}.

%%----------------------------------------------------------------------------
%% Queue-local code
%%----------------------------------------------------------------------------

%% We want to do all the AMQP 1.0-ish link level credit calculations
%% in the queue (to do them elsewhere introduces a ton of
%% races). However, it's a big chunk of code that is conceptually very
%% linked to the limiter concept. So we get the queue to hold a bit of
%% state for us (#qstate.credits), and maintain a fiction that the
%% limiter is making the decisions...
%% 将CTag标识的消费者的流量控制减一
decrement_credit(CTag, Credits) ->
	case gb_trees:lookup(CTag, Credits) of
		{value, C = #credit{credit = Credit}} ->
			update_credit(CTag, C#credit{credit = Credit - 1}, Credits);
		none ->
			Credits
	end.


%% 如果credit字段gb_trees数据结构中没有CTag对应的节点，则插入，否则更新CTag对应的节点
enter_credit(CTag, C, Credits) ->
	gb_trees:enter(CTag, ensure_credit_invariant(C), Credits).


%% 通过消费者的标识CTag更新credit字段
update_credit(CTag, C, Credits) ->
	gb_trees:update(CTag, ensure_credit_invariant(C), Credits).


%% invariant：不变
ensure_credit_invariant(C = #credit{credit = 0, mode = drain}) ->
	%% Using up all credit implies no need to send a 'drained' event
	C#credit{mode = manual};

ensure_credit_invariant(C) ->
	C.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------
%% rabbit_limiter进程的初始化回调函数
init([ProcName]) -> ?store_proc_name(ProcName),
					{ok, #lim{}}.


%% 当前rabbit_limiter进程对消息做优先级排序
prioritise_call(get_prefetch_limit, _From, _Len, _State) -> 9;

prioritise_call(_Msg,               _From, _Len, _State) -> 0.


%% 同步处理new的call消息
handle_call({new, ChPid}, _From, State = #lim{ch_pid = undefined}) ->
	{reply, ok, State#lim{ch_pid = ChPid}};

%% 当前prefetch_count=0的时候处理限制预取的消息
handle_call({limit_prefetch, PrefetchCount, UnackedCount}, _From,
			State = #lim{prefetch_count = 0}) ->
	{reply, ok, maybe_notify(State, State#lim{prefetch_count = PrefetchCount,
											  volume         = UnackedCount})};

%% 当前prefetch_count/=0的时候处理限制预取的消息
handle_call({limit_prefetch, PrefetchCount, _UnackedCount}, _From, State) ->
	{reply, ok, maybe_notify(State, State#lim{prefetch_count = PrefetchCount})};

%% 处理取消限制预取的消息
handle_call(unlimit_prefetch, _From, State) ->
	{reply, ok, maybe_notify(State, State#lim{prefetch_count = 0,
											  volume         = 0})};

%% 处理拿到当前rabbit_channel进程限制的预取的数量
handle_call(get_prefetch_limit, _From,
			State = #lim{prefetch_count = PrefetchCount}) ->
	{reply, PrefetchCount, State};

%% 查看能否发送消息
handle_call({can_send, QPid, AckRequired}, _From,
			State = #lim{volume = Volume}) ->
	case prefetch_limit_reached(State) of
		true  -> {reply, false, limit_queue(QPid, State)};
		false -> {reply, true,  State#lim{volume = if AckRequired -> Volume + 1;
													  true        -> Volume
												   end}}
	end.


%% 处理消费者ack的操作的数量消息
handle_cast({ack, Count}, State = #lim{volume = Volume}) ->
	NewVolume = if Volume == 0 -> 0;
				   true        -> Volume - Count
				end,
	{noreply, maybe_notify(State, State#lim{volume = NewVolume})};

%% QPid为队列进程，将队列进程注册到rabbit_limiter进程
handle_cast({register, QPid}, State) ->
	{noreply, remember_queue(QPid, State)};

%% QPid为队列进程，将队列进程从rabbit_limiter进程中取消注册
handle_cast({unregister, QPid}, State) ->
	{noreply, forget_queue(QPid, State)}.


%% 监听的队列进程挂掉，则将队列进程从rabbit_limiter进程中取消注册
handle_info({'DOWN', _MonitorRef, _Type, QPid, _Info}, State) ->
	{noreply, forget_queue(QPid, State)}.


terminate(_, _) ->
	ok.


code_change(_, State, _) ->
	{ok, State}.

%%----------------------------------------------------------------------------
%% Internal plumbing(内部接口)
%%----------------------------------------------------------------------------
%% 如果OldState状态下rabbit_limiter进程处于锁住状态，NewState状态下rabbit_limiter进程处于未锁住状态，则通知当前已经锁住的队列重新向消费者发送消息
maybe_notify(OldState, NewState) ->
	case prefetch_limit_reached(OldState) andalso
			 not prefetch_limit_reached(NewState) of
		%% OldState状态下rabbit_limiter进程处于锁住状态，NewState状态下rabbit_limiter进程处于未锁住状态
		true  -> notify_queues(NewState);
		false -> NewState
	end.


%% 判断当前rabbit_limiter进程是否处于锁住状态
prefetch_limit_reached(#lim{prefetch_count = Limit, volume = Volume}) ->
	Limit =/= 0 andalso Volume >= Limit.


%% 增加新的队列进程
remember_queue(QPid, State = #lim{queues = Queues}) ->
	case orddict:is_key(QPid, Queues) of
		false -> MRef = erlang:monitor(process, QPid),
				 %% 将队列进程Pid做key，将数据存储到orddict数据结构queues字段中
				 State#lim{queues = orddict:store(QPid, {MRef, false}, Queues)};
		true  -> State
	end.


%% 删除队列进程
forget_queue(QPid, State = #lim{queues = Queues}) ->
	case orddict:find(QPid, Queues) of
		{ok, {MRef, _}} -> true = erlang:demonitor(MRef),
						   %% 将队列进程Pid从queues字段中删除掉
						   State#lim{queues = orddict:erase(QPid, Queues)};
		error           -> State
	end.


%% 当前队列进入锁住状态
limit_queue(QPid, State = #lim{queues = Queues}) ->
	UpdateFun = fun ({MRef, _}) -> {MRef, true} end,
	State#lim{queues = orddict:update(QPid, UpdateFun, Queues)}.


%% 通知当前rabbit_limiter进程中的所有消息队列
notify_queues(State = #lim{ch_pid = ChPid, queues = Queues}) ->
	%% 得到所有已经锁住的队列进程Pid列表
	{QList, NewQueues} =
		orddict:fold(fun (_QPid, {_, false}, Acc) -> Acc;
						(QPid, {MRef, true}, {L, D}) ->
							 {[QPid | L], orddict:store(QPid, {MRef, false}, D)}
					 end, {[], Queues}, Queues),
	case length(QList) of
		0 -> ok;
		%% 只有一个锁住的队列进程的情况
		1 -> ok = rabbit_amqqueue:resume(hd(QList), ChPid); %% common case
		L ->
			%% We randomly vary the position of queues in the list,
			%% thus ensuring that each queue has an equal chance of
			%% being notified first.
			%% 通知队列进程重新可以向消费者发送消息
			{L1, L2} = lists:split(random:uniform(L), QList),
			[[ok = rabbit_amqqueue:resume(Q, ChPid) || Q <- L3]
			 || L3 <- [L2, L1]],
			ok
	end,
	State#lim{queues = NewQueues}.
