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

-module(gm).

%% Guaranteed(保证) Multicast(组播)
%% ====================
%%
%% This module provides the ability to create named groups of
%% processes to which members can be dynamically added and removed,
%% and for messages to be broadcast within the group that are
%% guaranteed to reach all members of the group during the lifetime of
%% the message. The lifetime of a message is defined as being, at a
%% minimum, the time from which the message is first sent to any
%% member of the group, up until the time at which it is known by the
%% member who published the message that the message has reached all
%% group members.
%%
%% The guarantee given is that provided a message, once sent, makes it
%% to members who do not all leave the group, the message will
%% continue to propagate(传播) to all group members.
%%
%% Another way of stating the guarantee is that if member P publishes
%% messages m and m', then for all members P', if P' is a member of
%% the group prior to the publication of m, and P' receives m', then
%% P' will receive m.
%%
%% Note that only local-ordering is enforced: i.e. if member P sends
%% message m and then message m', then for-all members P', if P'
%% receives m and m', then they will receive m' after m. Causality
%% ordering is _not_ enforced. I.e. if member P receives message m
%% and as a result publishes message m', there is no guarantee that
%% other members P' will receive m before m'.
%%
%%
%% API Use
%% -------
%%
%% Mnesia must be started. Use the idempotent create_tables/0 function
%% to create the tables required.
%%
%% start_link/3
%% Provide the group name, the callback module name, and any arguments
%% you wish to be passed into the callback module's functions. The
%% joined/2 function will be called when we have joined the group,
%% with the arguments passed to start_link and a list of the current
%% members of the group. See the callbacks specs and the comments
%% below for further details of the callback functions.
%%
%% leave/1
%% Provide the Pid. Removes the Pid from the group. The callback
%% handle_terminate/2 function will be called.
%%
%% broadcast/2
%% Provide the Pid and a Message. The message will be sent to all
%% members of the group as per the guarantees given above. This is a
%% cast and the function call will return immediately. There is no
%% guarantee that the message will reach any member of the group.
%%
%% confirmed_broadcast/2
%% Provide the Pid and a Message. As per broadcast/2 except that this
%% is a call, not a cast, and only returns 'ok' once the Message has
%% reached every member of the group. Do not call
%% confirmed_broadcast/2 directly from the callback module otherwise
%% you will deadlock the entire group.
%%
%% info/1
%% Provide the Pid. Returns a proplist with various facts, including
%% the group name and the current group members.
%%
%% validate_members/2
%% Check whether a given member list agrees with the chosen member's
%% view. Any differences will be communicated via the members_changed
%% callback. If there are no differences then there will be no reply.
%% Note that members will not necessarily share the same view.
%%
%% forget_group/1
%% Provide the group name. Removes its mnesia record. Makes no attempt
%% to ensure the group is empty.
%%
%% Implementation Overview
%% -----------------------
%%
%% One possible means of implementation would be a fan-out from the
%% sender to every member of the group. This would require that the
%% group is fully connected, and, in the event that the original
%% sender of the message disappears from the group before the message
%% has made it to every member of the group, raises questions as to
%% who is responsible for sending on the message to new group members.
%% In particular, the issue is with [ Pid ! Msg || Pid <- Members ] -
%% if the sender dies part way through, who is responsible for
%% ensuring that the remaining Members receive the Msg? In the event
%% that within the group, messages sent are broadcast from a subset of
%% the members, the fan-out arrangement has the potential to
%% substantially impact the CPU and network workload of such members,
%% as such members would have to accommodate the cost of sending each
%% message to every group member.
%%
%% Instead, if the members of the group are arranged in a chain, then
%% it becomes easier to reason about who within the group has received
%% each message and who has not. It eases issues of responsibility: in
%% the event of a group member disappearing, the nearest upstream
%% member of the chain is responsible for ensuring that messages
%% continue to propagate down the chain. It also results in equal
%% distribution of sending and receiving workload, even if all
%% messages are being sent from just a single group member. This
%% configuration has the further advantage that it is not necessary
%% for every group member to know of every other group member, and
%% even that a group member does not have to be accessible from all
%% other group members.
%%
%% Performance is kept high by permitting pipelining and all
%% communication between joined group members is asynchronous. In the
%% chain A -> B -> C -> D, if A sends a message to the group, it will
%% not directly contact C or D. However, it must know that D receives
%% the message (in addition to B and C) before it can consider the
%% message fully sent. A simplistic implementation would require that
%% D replies to C, C replies to B and B then replies to A. This would
%% result in a propagation delay of twice the length of the chain. It
%% would also require, in the event of the failure of C, that D knows
%% to directly contact B and issue the necessary replies. Instead, the
%% chain forms a ring: D sends the message on to A: D does not
%% distinguish A as the sender, merely as the next member (downstream)
%% within the chain (which has now become a ring). When A receives
%% from D messages that A sent, it knows that all members have
%% received the message. However, the message is not dead yet: if C
%% died as B was sending to C, then B would need to detect the death
%% of C and forward the message on to D instead: thus every node has
%% to remember every message published until it is told that it can
%% forget about the message. This is essential not just for dealing
%% with failure of members, but also for the addition of new members.
%%
%% Thus once A receives the message back again, it then sends to B an
%% acknowledgement for the message, indicating that B can now forget
%% about the message. B does so, and forwards the ack to C. C forgets
%% the message, and forwards the ack to D, which forgets the message
%% and finally forwards the ack back to A. At this point, A takes no
%% further action: the message and its acknowledgement have made it to
%% every member of the group. The message is now dead, and any new
%% member joining the group at this point will not receive the
%% message.
%%
%% We therefore have two roles:
%%
%% 1. The sender, who upon receiving their own messages back, must
%% then send out acknowledgements, and upon receiving their own
%% acknowledgements back perform no further action.
%%
%% 2. The other group members who upon receiving messages and
%% acknowledgements must update their own internal state accordingly
%% (the sending member must also do this in order to be able to
%% accommodate failures), and forwards messages on to their downstream
%% neighbours.
%%
%%
%% Implementation: It gets trickier
%% --------------------------------
%%
%% Chain A -> B -> C -> D
%%
%% A publishes a message which B receives. A now dies. B and D will
%% detect the death of A, and will link up, thus the chain is now B ->
%% C -> D. B forwards A's message on to C, who forwards it to D, who
%% forwards it to B. Thus B is now responsible for A's messages - both
%% publications and acknowledgements that were in flight at the point
%% at which A died. Even worse is that this is transitive: after B
%% forwards A's message to C, B dies as well. Now C is not only
%% responsible for B's in-flight messages, but is also responsible for
%% A's in-flight messages.
%%
%% Lemma 1: A member can only determine which dead members they have
%% inherited responsibility for if there is a total ordering on the
%% conflicting additions and subtractions of members from the group.
%%
%% Consider the simultaneous death of B and addition of B' that
%% transitions a chain from A -> B -> C to A -> B' -> C. Either B' or
%% C is responsible for in-flight messages from B. It is easy to
%% ensure that at least one of them thinks they have inherited B, but
%% if we do not ensure that exactly one of them inherits B, then we
%% could have B' converting publishes to acks, which then will crash C
%% as C does not believe it has issued acks for those messages.
%%
%% More complex scenarios are easy to concoct: A -> B -> C -> D -> E
%% becoming A -> C' -> E. Who has inherited which of B, C and D?
%%
%% However, for non-conflicting membership changes, only a partial
%% ordering is required. For example, A -> B -> C becoming A -> A' ->
%% B. The addition of A', between A and B can have no conflicts with
%% the death of C: it is clear that A has inherited C's messages.
%%
%% For ease of implementation, we adopt the simple solution, of
%% imposing a total order on all membership changes.
%%
%% On the death of a member, it is ensured the dead member's
%% neighbours become aware of the death, and the upstream neighbour
%% now sends to its new downstream neighbour its state, including the
%% messages pending acknowledgement. The downstream neighbour can then
%% use this to calculate which publishes and acknowledgements it has
%% missed out on, due to the death of its old upstream. Thus the
%% downstream can catch up, and continues the propagation of messages
%% through the group.
%%
%% Lemma 2: When a member is joining, it must synchronously
%% communicate with its upstream member in order to receive its
%% starting state atomically with its addition to the group.
%%
%% New members must start with the same state as their nearest
%% upstream neighbour. This ensures that it is not surprised by
%% acknowledgements they are sent, and that should their downstream
%% neighbour die, they are able to send the correct state to their new
%% downstream neighbour to ensure it can catch up. Thus in the
%% transition A -> B -> C becomes A -> A' -> B -> C becomes A -> A' ->
%% C, A' must start with the state of A, so that it can send C the
%% correct state when B dies, allowing C to detect any missed
%% messages.
%%
%% If A' starts by adding itself to the group membership, A could then
%% die, without A' having received the necessary state from A. This
%% would leave A' responsible for in-flight messages from A, but
%% having the least knowledge of all, of those messages. Thus A' must
%% start by synchronously calling A, which then immediately sends A'
%% back its state. A then adds A' to the group. If A dies at this
%% point then A' will be able to see this (as A' will fail to appear
%% in the group membership), and thus A' will ignore the state it
%% receives from A, and will simply repeat the process, trying to now
%% join downstream from some other member. This ensures that should
%% the upstream die as soon as the new member has been joined, the new
%% member is guaranteed to receive the correct state, allowing it to
%% correctly process messages inherited due to the death of its
%% upstream neighbour.
%%
%% The canonical definition of the group membership is held by a
%% distributed database. Whilst this allows the total ordering of
%% changes to be achieved, it is nevertheless undesirable to have to
%% query this database for the current view, upon receiving each
%% message. Instead, we wish for members to be able to cache a view of
%% the group membership, which then requires a cache invalidation
%% mechanism. Each member maintains its own view of the group
%% membership. Thus when the group's membership changes, members may
%% need to become aware of such changes in order to be able to
%% accurately process messages they receive. Because of the
%% requirement of a total ordering of conflicting membership changes,
%% it is not possible to use the guaranteed broadcast mechanism to
%% communicate these changes: to achieve the necessary ordering, it
%% would be necessary for such messages to be published by exactly one
%% member, which can not be guaranteed given that such a member could
%% die.
%%
%% The total ordering we enforce on membership changes gives rise to a
%% view version number: every change to the membership creates a
%% different view, and the total ordering permits a simple
%% monotonically increasing view version number.
%%
%% Lemma 3: If a message is sent from a member that holds view version
%% N, it can be correctly processed by any member receiving the
%% message with a view version >= N.
%%
%% Initially, let us suppose that each view contains the ordering of
%% every member that was ever part of the group. Dead members are
%% marked as such. Thus we have a ring of members, some of which are
%% dead, and are thus inherited by the nearest alive downstream
%% member.
%%
%% In the chain A -> B -> C, all three members initially have view
%% version 1, which reflects reality. B publishes a message, which is
%% forward by C to A. B now dies, which A notices very quickly. Thus A
%% updates the view, creating version 2. It now forwards B's
%% publication, sending that message to its new downstream neighbour,
%% C. This happens before C is aware of the death of B. C must become
%% aware of the view change before it interprets the message its
%% received, otherwise it will fail to learn of the death of B, and
%% thus will not realise it has inherited B's messages (and will
%% likely crash).
%%
%% Thus very simply, we have that each subsequent view contains more
%% information than the preceding view.
%%
%% However, to avoid the views growing indefinitely, we need to be
%% able to delete members which have died _and_ for which no messages
%% are in-flight. This requires that upon inheriting a dead member, we
%% know the last publication sent by the dead member (this is easy: we
%% inherit a member because we are the nearest downstream member which
%% implies that we know at least as much than everyone else about the
%% publications of the dead member), and we know the earliest message
%% for which the acknowledgement is still in flight.
%%
%% In the chain A -> B -> C, when B dies, A will send to C its state
%% (as C is the new downstream from A), allowing C to calculate which
%% messages it has missed out on (described above). At this point, C
%% also inherits B's messages. If that state from A also includes the
%% last message published by B for which an acknowledgement has been
%% seen, then C knows exactly which further acknowledgements it must
%% receive (also including issuing acknowledgements for publications
%% still in-flight that it receives), after which it is known there
%% are no more messages in flight for B, thus all evidence that B was
%% ever part of the group can be safely removed from the canonical
%% group membership.
%%
%% Thus, for every message that a member sends, it includes with that
%% message its view version. When a member receives a message it will
%% update its view from the canonical copy, should its view be older
%% than the view version included in the message it has received.
%%
%% The state held by each member therefore includes the messages from
%% each publisher pending acknowledgement, the last publication seen
%% from that publisher, and the last acknowledgement from that
%% publisher. In the case of the member's own publications or
%% inherited members, this last acknowledgement seen state indicates
%% the last acknowledgement retired, rather than sent.
%%
%%
%% Proof sketch
%% ------------
%%
%% We need to prove that with the provided operational semantics, we
%% can never reach a state that is not well formed from a well-formed
%% starting state.
%%
%% Operational semantics (small step): straight-forward message
%% sending, process monitoring, state updates.
%%
%% Well formed state: dead members inherited by exactly one non-dead
%% member; for every entry in anyone's pending-acks, either (the
%% publication of the message is in-flight downstream from the member
%% and upstream from the publisher) or (the acknowledgement of the
%% message is in-flight downstream from the publisher and upstream
%% from the member).
%%
%% Proof by induction on the applicable operational semantics.
%%
%%
%% Related work
%% ------------
%%
%% The ring configuration and double traversal of messages around the
%% ring is similar (though developed independently) to the LCR
%% protocol by [Levy 2008]. However, LCR differs in several
%% ways. Firstly, by using vector clocks, it enforces a total order of
%% message delivery, which is unnecessary for our purposes. More
%% significantly, it is built on top of a "group communication system"
%% which performs the group management functions, taking
%% responsibility away from the protocol as to how to cope with safely
%% adding and removing members. When membership changes do occur, the
%% protocol stipulates that every member must perform communication
%% with every other member of the group, to ensure all outstanding
%% deliveries complete, before the entire group transitions to the new
%% view. This, in total, requires two sets of all-to-all synchronous
%% communications.
%%
%% This is not only rather inefficient, but also does not explain what
%% happens upon the failure of a member during this process. It does
%% though entirely avoid the need for inheritance of responsibility of
%% dead members that our protocol incorporates.
%%
%% In [Marandi et al 2010], a Paxos-based protocol is described. This
%% work explicitly focuses on the efficiency of communication. LCR
%% (and our protocol too) are more efficient, but at the cost of
%% higher latency. The Ring-Paxos protocol is itself built on top of
%% IP-multicast, which rules it out for many applications where
%% point-to-point communication is all that can be required. They also
%% have an excellent related work section which I really ought to
%% read...
%%
%%
%% [Levy 2008] The Complexity of Reliable Distributed Storage, 2008.
%% [Marandi et al 2010] Ring Paxos: A High-Throughput Atomic Broadcast
%% Protocol


-behaviour(gen_server2).

-export([create_tables/0, start_link/4, leave/1, broadcast/2, broadcast/3,
         confirmed_broadcast/2, info/1, validate_members/2, forget_group/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, prioritise_info/3]).

%% For INSTR_MOD callbacks
-export([call/3, cast/2, monitor/1, demonitor/1]).

-ifndef(use_specs).
-export([behaviour_info/1]).
-endif.

-export([table_definitions/0]).

-define(GROUP_TABLE, gm_group).							%%
-define(MAX_BUFFER_SIZE, 100000000). 					%% 100MB
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).
-define(BROADCAST_TIMER, 25).
-define(VERSION_START, 0).
-define(SETS, ordsets).									%% 排序的SETS数据结构的模块名字
-define(DICT, orddict).									%% 排序的DICT数据结构的模块名字

%% gm进程的状态数据结构
-record(state,
		{ self,										%% gm本身的ID
		  left,										%% 该节点左边的节点
		  right,									%% 该节点右边的节点
		  group_name,								%% group名称与队列名一致
		  module,									%% 回调模块rabbit_mirror_queue_slave或者rabbit_mirror_queue_coordinator
		  view,										%% group成员列表视图信息，记录了成员的ID及每个成员的左右邻居节点(组装成一个循环列表)
		  pub_count,								%% 当前已发布的消息计数
		  members_state,							%% group成员状态列表 记录了广播状态:[#member{}]
		  callback_args,							%% 回调函数的参数信息，rabbit_mirror_queue_slave/rabbit_mirror_queue_coordinator进程PID
		  confirms,									%% confirm列表
		  broadcast_buffer,							%% 缓存待广播的消息
		  broadcast_buffer_sz,						%% 当前缓存带广播中消息实体总的大小
		  broadcast_timer,							%% 广播消息定时器
		  txn_executor								%% 操作Mnesia数据库的操作函数
		}).

%% 整个镜像队列群组的信息，该信息会存储到Mnesia数据库
-record(gm_group, { name,							%% group的名称,与queue的名称一致
					version,						%% group的版本号, 新增节点/节点失效时会递增
					members							%% group的成员列表, 按照节点组成的链表顺序进行排序
				  }).

%% 镜像队列群组视图成员数据结构
-record(view_member, { id,							%% 单个镜像队列(结构是{版本号，该镜像队列的Pid})
					   aliases,						%% 记录id对应的左侧死亡的GM进程列表
					   left,						%% 当前镜像队列左边的镜像队列(结构是{版本号，该镜像队列的Pid})
					   right						%% 当前镜像队列右边的镜像队列(结构是{版本号，该镜像队列的Pid})
					 }).

%% 记录单个GM进程中信息的数据结构
-record(member, { pending_ack,						%% 待确认的消息,也就是已发布的消息缓存的地方
				  last_pub,							%% 最后一次发布的消息计数
				  last_ack							%% 最后一次确认的消息计数
				}).

%% gm_group数据库表的定义
-define(TABLE, {?GROUP_TABLE, [{record_name, gm_group},
                               {attributes, record_info(fields, gm_group)}]}).

-define(TABLE_MATCH, {match, #gm_group { _ = '_' }}).

%% 自己添加的代码
-define(INSTR_MOD, ?MODULE).

-define(TAG, '$gm').

-ifdef(use_specs).

-export_type([group_name/0]).

-type(group_name() :: any()).
-type(txn_fun() :: fun((fun(() -> any())) -> any())).

-spec(create_tables/0 :: () -> 'ok' | {'aborted', any()}).
-spec(start_link/4 :: (group_name(), atom(), any(), txn_fun()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(leave/1 :: (pid()) -> 'ok').
-spec(broadcast/2 :: (pid(), any()) -> 'ok').
-spec(confirmed_broadcast/2 :: (pid(), any()) -> 'ok').
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(validate_members/2 :: (pid(), [pid()]) -> 'ok').
-spec(forget_group/1 :: (group_name()) -> 'ok').

%% The joined, members_changed and handle_msg callbacks can all return
%% any of the following terms:
%%
%% 'ok' - the callback function returns normally
%%
%% {'stop', Reason} - the callback indicates the member should stop
%% with reason Reason and should leave the group.
%%
%% {'become', Module, Args} - the callback indicates that the callback
%% module should be changed to Module and that the callback functions
%% should now be passed the arguments Args. This allows the callback
%% module to be dynamically changed.

%% Called when we've successfully joined the group. Supplied with Args
%% provided in start_link, plus current group members.
-callback joined(Args :: term(), Members :: [pid()]) ->
    ok | {stop, Reason :: term()} | {become, Module :: atom(), Args :: any()}.

%% Supplied with Args provided in start_link, the list of new members
%% and the list of members previously known to us that have since
%% died. Note that if a member joins and dies very quickly, it's
%% possible that we will never see that member appear in either births
%% or deaths. However we are guaranteed that (1) we will see a member
%% joining either in the births here, or in the members passed to
%% joined/2 before receiving any messages from it; and (2) we will not
%% see members die that we have not seen born (or supplied in the
%% members to joined/2).
-callback members_changed(Args :: term(),
                          Births :: [pid()], Deaths :: [pid()]) ->
    ok | {stop, Reason :: term()} | {become, Module :: atom(), Args :: any()}.

%% Supplied with Args provided in start_link, the sender, and the
%% message. This does get called for messages injected by this member,
%% however, in such cases, there is no special significance of this
%% invocation: it does not indicate that the message has made it to
%% any other members, let alone all other members.
-callback handle_msg(Args :: term(), From :: pid(), Message :: term()) ->
    ok | {stop, Reason :: term()} | {become, Module :: atom(), Args :: any()}.

%% Called on gm member termination as per rules in gen_server, with
%% the Args provided in start_link plus the termination Reason.
-callback handle_terminate(Args :: term(), Reason :: term()) ->
    ok | term().

-else.

behaviour_info(callbacks) ->
    [{joined, 2}, {members_changed, 3}, {handle_msg, 3}, {handle_terminate, 2}];
behaviour_info(_Other) ->
    undefined.

-endif.

create_tables() ->
	create_tables([?TABLE]).


create_tables([]) ->
	ok;
create_tables([{Table, Attributes} | Tables]) ->
	case mnesia:create_table(Table, Attributes) of
		{atomic, ok}                       -> create_tables(Tables);
		{aborted, {already_exists, Table}} -> create_tables(Tables);
		Err                                -> Err
	end.


%% 返回gm_group数据库表的定义(rabbit_table模块调用，用来创建gm_group数据库表)
table_definitions() ->
	{Name, Attributes} = ?TABLE,
	[{Name, [?TABLE_MATCH | Attributes]}].


%% gm进程的启动入口函数
start_link(GroupName, Module, Args, TxnFun) ->
	gen_server2:start_link(?MODULE, [GroupName, Module, Args, TxnFun], []).


%% 停止GM进程的接口
leave(Server) ->
	gen_server2:cast(Server, leave).


%% 广播Msg消息的接口
broadcast(Server, Msg) -> broadcast(Server, Msg, 0).


%% 广播Msg消息的接口(SizeHint表示消息内容的大小)
broadcast(Server, Msg, SizeHint) ->
	gen_server2:cast(Server, {broadcast, Msg, SizeHint}).


%% 此接口无人调用，暂时废弃
confirmed_broadcast(Server, Msg) ->
	gen_server2:call(Server, {confirmed_broadcast, Msg}, infinity).


%% 获取GM进程中镜像队列的相关信息的接口
info(Server) ->
	gen_server2:call(Server, info, infinity).


%% 在Server这个GM进程里验证成功的接口
validate_members(Server, Members) ->
	gen_server2:cast(Server, {validate_members, Members}).


forget_group(GroupName) ->
	{atomic, ok} = mnesia:sync_transaction(
					 fun () ->
							  mnesia:delete({?GROUP_TABLE, GroupName})
					 end),
	ok.


%% gm进程启动的回调初始化函数(gm_group数据库表是非持久化表，则RabbitMQ系统每次重启gm_group表中都没有数据)
init([GroupName, Module, Args, TxnFun]) ->
	%% 存储进程名字
	put(process_name, {?MODULE, GroupName}),
	%% 设置随机种子
	{MegaSecs, Secs, MicroSecs} = now(),
	random:seed(MegaSecs, Secs, MicroSecs),
	%% 组装单个成员信息(组装自己的成员ID)
	Self = make_member(GroupName),
	%% 通知自己加入镜像队列的群组中
	gen_server2:cast(self(), join),
	{ok, #state { self                = Self,
				  left                = {Self, undefined},
				  right               = {Self, undefined},
				  group_name          = GroupName,
				  module              = Module,
				  view                = undefined,
				  pub_count           = -1,
				  members_state       = undefined,
				  callback_args       = Args,
				  confirms            = queue:new(),
				  broadcast_buffer    = [],
				  broadcast_buffer_sz = 0,
				  broadcast_timer     = undefined,
				  txn_executor        = TxnFun }, hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


%% 此消息无人调用，暂时废弃
handle_call({confirmed_broadcast, _Msg}, _From,
			State = #state { members_state = undefined }) ->
	reply(not_joined, State);

%% 此消息无人调用，暂时废弃
handle_call({confirmed_broadcast, Msg}, _From,
			State = #state { self          = Self,
							 right         = {Self, undefined},
							 module        = Module,
							 callback_args = Args }) ->
	handle_callback_result({Module:handle_msg(Args, get_pid(Self), Msg),
							ok, State});

%% 此消息无人调用，暂时废弃
handle_call({confirmed_broadcast, Msg}, From, State) ->
	{Result, State1 = #state { pub_count = PubCount, confirms = Confirms }} =
		internal_broadcast(Msg, 0, State),
	Confirms1 = queue:in({PubCount, From}, Confirms),
	handle_callback_result({Result, flush_broadcast_buffer(
							  State1 #state { confirms = Confirms1 })});

%% 同步处理获取当前GM进程的相关信息的消息(如果members_state为undefined，则表示当前GM进程没有加入到镜像队列的循环队列中)
handle_call(info, _From,
			State = #state { members_state = undefined }) ->
	reply(not_joined, State);

%% 同步处理获取当前GM进程的相关信息的消息
handle_call(info, _From, State = #state { group_name = GroupName,
										  module     = Module,
										  view       = View }) ->
	%% 返回镜像队列的群组名字，回调模块名字，群组成员即所有存活的镜像队列的Pid
	reply([{group_name,    GroupName},
		   {module,        Module},
		   {group_members, get_pids(alive_view_members(View))}], State);

%% 处理将新的镜像队列加入到本镜像队列的右侧的消息，如果members_state为undefined，则通知发送者该队列还未准备好
handle_call({add_on_right, _NewMember}, _From,
			State = #state { members_state = undefined }) ->
	reply(not_ready, State);

%% 处理将新的镜像队列加入到本镜像队列的右侧的消息
handle_call({add_on_right, NewMember}, _From,
			State = #state { self          = Self,
							 group_name    = GroupName,
							 members_state = MembersState,
							 txn_executor  = TxnFun }) ->
	%% 记录将新的镜像队列成员加入到镜像队列组中，将新加入的镜像队列写入gm_group结构中的members字段中(有新成员加入群组的时候，则将版本号增加一)
	Group = record_new_member_in_group(NewMember, Self, GroupName, TxnFun),
	%% 根据组成员信息生成新的镜像队列视图数据结构
	View1 = group_to_view(Group),
	%% 删除擦除的成员
	MembersState1 = remove_erased_members(MembersState, View1),
	%% 向新加入的成员即右边成员发送加入成功的消息
	ok = send_right(NewMember, View1,
					{catchup, Self, prepare_members_state(MembersState1)}),
	%% 根据新的镜像队列循环队列视图和老的视图修改视图，同时根据镜像队列循环视图更新自己左右邻居信息
	{Result, State1} = change_view(View1, State #state {
														members_state = MembersState1 }),
	%% 向请求加入的镜像队列发送最新的当前镜像队列的群组信息
	handle_callback_result({Result, {ok, Group}, State1}).

%% add_on_right causes a catchup to be sent immediately from the left,
%% so we can never see this from the left neighbour. However, it's
%% possible for the right neighbour to send us a check_neighbours
%% immediately before that. We can't possibly handle it, but if we're
%% in this state we know a catchup is coming imminently anyway. So
%% just ignore it.
handle_cast({?TAG, _ReqVer, check_neighbours},
			State = #state { members_state = undefined }) ->
	noreply(State);

%% 处理GM进程间发送的消息
handle_cast({?TAG, ReqVer, Msg},
			State = #state { view          = View,
							 members_state = MembersState,
							 group_name    = GroupName }) ->
	{Result, State1} =
		%% 判断是否需要对镜像队列群组进行更新升级(根据发送消息的GM进程的版本号和自己GM进程中的版本号)
		case needs_view_update(ReqVer, View) of
			%% 如果新接收的消息的版本号大于自己当前视图的版本号，则立刻更新自己的镜像队列群组视图
			true  -> %% 根据Mnesia数据库里面的gm_group数据结构创建新的镜像队列群组循环队列视图
				View1 = group_to_view(dirty_read_group(GroupName)),
				%% 删除擦除的成员
				MemberState1 = remove_erased_members(MembersState, View1),
				%% 根据新的镜像队列循环队列视图和老的视图修改视图，同时根据镜像队列循环视图更新自己左右邻居信息
				change_view(View1, State #state {
												 members_state = MemberState1 });
			false -> {ok, State}
		end,
	handle_callback_result(
	  if_callback_success(
		Result, fun handle_msg_true/3, fun handle_msg_false/3, Msg, State1));

%% 处理广播的消息(当前GM进程的成员信息还没有初始化的情况，则什么都不做)
handle_cast({broadcast, _Msg, _SizeHint},
			State = #state { members_state = undefined }) ->
	noreply(State);

%% 处理广播的消息(此时GM进程还没有加入镜像队列群组循环队列)
handle_cast({broadcast, Msg, _SizeHint},
			State = #state { self          = Self,
							 right         = {Self, undefined},
							 module        = Module,
							 callback_args = Args }) ->
	handle_callback_result({Module:handle_msg(Args, get_pid(Self), Msg),
							State});

%% 处理广播的消息(将消息发送给回调模块进程进行相关处理)
handle_cast({broadcast, Msg, SizeHint}, State) ->
	{Result, State1} = internal_broadcast(Msg, SizeHint, State),
	handle_callback_result({Result, maybe_flush_broadcast_buffer(State1)});

%% 同步处理将自己加入到镜像队列的群组中的消息
handle_cast(join, State = #state { self          = Self,
								   group_name    = GroupName,
								   members_state = undefined,
								   module        = Module,
								   callback_args = Args,
								   txn_executor  = TxnFun }) ->
	%% 将当前镜像队列加入到镜像队列的循环队列视图中，返回最新的镜像队列循环队列视图
	%% 如果当前GM进程不是群组的第一个GM进程，则会call到自己要加入的位置左侧的GM进程，直到左侧成功的将当前GM加入群组，然后返回给
	%% 当前GM进程最新的gm_group数据结构信息
	View = join_group(Self, GroupName, TxnFun),
	MembersState =
		%% 获取镜像队列视图的所有key列表
		case alive_view_members(View) of
			%% 如果是第一个GM进程的启动则初始化成员状态数据结构
			[Self] -> blank_member_state();
			%% 如果是第一个以后的GM进程加入到Group中，则成员状态先不做初始化，让自己左侧的GM进程发送过来的信息进行初始化
			_      -> undefined
		end,
	%% 检查当前镜像队列的邻居信息(根据消息镜像队列的群组循环视图更新自己最新的左右两边的镜像队列)
	State1 = check_neighbours(State #state { view          = View,
											 members_state = MembersState }),
	%% 向启动该GM进程的进程通知他们已经加入镜像队列群组成功(rabbit_mirror_queue_coordinator和rabbit_mirror_queue_slave模块回调)
	handle_callback_result(
	  {Module:joined(Args, get_pids(all_known_members(View))), State1});

%% 处理验证成员的消息
handle_cast({validate_members, OldMembers},
			State = #state { view          = View,
							 module        = Module,
							 callback_args = Args }) ->
	NewMembers = get_pids(all_known_members(View)),
	Births = NewMembers -- OldMembers,
	Deaths = OldMembers -- NewMembers,
	case {Births, Deaths} of
		{[], []}  -> noreply(State);
		_         -> Result = Module:members_changed(Args, Births, Deaths),
					 handle_callback_result({Result, State})
	end;

%% 处理关闭GM进程的消息
handle_cast(leave, State) ->
	{stop, normal, State}.


%% 处理刷新广播缓存的定时器消息
handle_info(flush, State) ->
	noreply(
	  %% 则立刻将广播缓存中的数据广播给右侧的镜像队列
	  flush_broadcast_buffer(State #state { broadcast_timer = undefined }));

handle_info(timeout, State) ->
	%% 则立刻将广播缓存中的数据广播给右侧的镜像队列
	noreply(flush_broadcast_buffer(State));

%% 接收到自己左右两边的镜像队列GM进程挂掉的消息
handle_info({'DOWN', MRef, process, _Pid, Reason},
			State = #state { self          = Self,
							 left          = Left,
							 right         = Right,
							 group_name    = GroupName,
							 confirms      = Confirms,
							 txn_executor  = TxnFun }) ->
	%% 得到挂掉的GM进程
	Member = case {Left, Right} of
				 %% 左侧的镜像队列GM进程挂掉的情况
				 {{Member1, MRef}, _} -> Member1;
				 %% 右侧的镜像队列GM进程挂掉的情况
				 {_, {Member1, MRef}} -> Member1;
				 _                    -> undefined
			 end,
	case {Member, Reason} of
		{undefined, _} ->
			noreply(State);
		{_, {shutdown, ring_shutdown}} ->
			noreply(State);
		_ ->
			%% In the event of a partial partition we could see another member
			%% go down and then remove them from Mnesia. While they can
			%% recover from this they'd have to restart the queue - not
			%% ideal. So let's sleep here briefly just in case this was caused
			%% by a partial partition; in which case by the time we record the
			%% member death in Mnesia we will probably be in a full
			%% partition and will not be assassinating another member.
			timer:sleep(100),
			%% 先记录有镜像队列成员死亡的信息，然后将所有存活的镜像队列组装镜像队列群组循环队列视图
			%% 有成员死亡的时候会将版本号增加一，record_dead_member_in_group函数是更新gm_group数据库表中的数据，将死亡信息写入数据库表
			View1 = group_to_view(record_dead_member_in_group(
									Member, GroupName, TxnFun)),
			handle_callback_result(
			  case alive_view_members(View1) of
				  %% 当存活的镜像队列GM进程只剩自己的情况
				  [Self] -> maybe_erase_aliases(
							  State #state {
											members_state = blank_member_state(),
											confirms      = purge_confirms(Confirms) },
										   View1);
				  %% 当存活的镜像队列GM进程不止自己(根据新的镜像队列循环队列视图和老的视图修改视图，同时根据镜像队列循环视图更新自己左右邻居信息)
				  %% 同时将当前自己节点的消息信息发布到自己右侧的GM进程
				  _      -> change_view(View1, State)
			  end)
	end.


terminate(Reason, State = #state { module        = Module,
								   callback_args = Args }) ->
	flush_broadcast_buffer(State),
	Module:handle_terminate(Args, Reason).


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


prioritise_info(flush, _Len, _State) ->
	1;
%% DOWN messages should not overtake initial catchups; if they do we
%% will receive a DOWN we do not know what to do with.
prioritise_info({'DOWN', _MRef, process, _Pid, _Reason}, _Len,
				#state { members_state = undefined }) ->
	0;
%% We should not prioritise DOWN messages from our left since
%% otherwise the DOWN can overtake any last activity from the left,
%% causing that activity to be lost.
prioritise_info({'DOWN', _MRef, process, LeftPid, _Reason}, _Len,
				#state { left = {{_LeftVer, LeftPid}, _MRef2} }) ->
	0;
%% But prioritise all other DOWNs - we want to make sure we are not
%% sending activity into the void for too long because our right is
%% down but we don't know it.
prioritise_info({'DOWN', _MRef, process, _Pid, _Reason}, _Len, _State) ->
	1;
prioritise_info(_, _Len, _State) ->
	0.


%% 处理检查邻居的消息(已经在handle_cast接口处处理过)
handle_msg(check_neighbours, State) ->
	%% no-op - it's already been done by the calling handle_cast
	{ok, State};

%% 新增节点的情况
%% 左侧的GM进程通知右侧的GM进程最新的成员状态(此情况是本GM进程是新加入Group的，等待左侧GM进程发送过来的消息进行初始化成员状态)
handle_msg({catchup, Left, MembersStateLeft},
		   State = #state { self          = Self,
							left          = {Left, _MRefL},
							right         = {Right, _MRefR},
							view          = View,
							%% 新加入的GM进程在加入后是没有初始化成员状态，是等待左侧玩家发送消息来进行初始化
							members_state = undefined }) ->
	%% 异步向自己右侧的镜像队列发送最新的所有成员信息，让Group中的所有成员更新成员信息
	ok = send_right(Right, View, {catchup, Self, MembersStateLeft}),
	%% 将成员信息转化成字典数据结构
	MembersStateLeft1 = build_members_state(MembersStateLeft),
	%% 新增加的GM进程更新最新的成员信息
	{ok, State #state { members_state = MembersStateLeft1 }};

%% 节点挂掉的情况或者新增节点发送给自己右侧GM进程的信息
%% 左侧的GM进程通知右侧的GM进程最新的成员状态(此情况是有新GM进程加入Group，但是自己不是最新加入的GM进程，但是自己仍然需要更新成员信息)
handle_msg({catchup, Left, MembersStateLeft},
		   State = #state { self = Self,
							left = {Left, _MRefL},
							view = View,
							members_state = MembersState })
  when MembersState =/= undefined ->
	%% 将最新的成员信息转化成字典数据结构
	MembersStateLeft1 = build_members_state(MembersStateLeft),
	%% 拿到左侧镜像队列传入过来的成员信息和自己进程存储的成员信息的ID的去重
	AllMembers = lists:usort(?DICT:fetch_keys(MembersState) ++
								 ?DICT:fetch_keys(MembersStateLeft1)),
	%% 根据左侧GM进程发送过来的成员状态和自己GM进程里的成员状态得到需要广播给后续GM进程的信息
	{MembersState1, Activity} =
		lists:foldl(
		  fun (Id, MembersStateActivity) ->
				   %% 拿到左侧镜像队列传入过来的Id对应的镜像队列成员信息
				   #member { pending_ack = PALeft, last_ack = LA } =
							   find_member_or_blank(Id, MembersStateLeft1),
				   with_member_acc(
					 %% 函数的第一个参数是Id对应的自己进程存储的镜像队列成员信息
					 fun (#member { pending_ack = PA } = Member, Activity1) ->
							  %% 发送者和自己是一个人则表示消息已经发送回来，或者判断发送者是否在死亡列表中
							  case is_member_alias(Id, Self, View) of
								  %% 此情况是发送者和自己是同一个人或者发送者已经死亡
								  true ->
									  %% 根据左侧GM进程发送过来的ID最新的成员信息和本GM进程ID对应的成员信息得到已经发布的信息
									  {_AcksInFlight, Pubs, _PA1} =
										  find_prefix_common_suffix(PALeft, PA),
									  %% 重新将自己的消息发布
									  {Member #member { last_ack = LA },
													  %% 组装发送的发布和ack消息结构
													  activity_cons(Id, pubs_from_queue(Pubs),
																	[], Activity1)};
								  false ->
									  %% 根据左侧GM进程发送过来的ID最新的成员信息和本GM进程ID对应的成员信息得到Ack和Pub列表
									  %% 上一个节点少的消息就是已经得到确认的消息，多出来的是新发布的消息
									  {Acks, _Common, Pubs} =
										  find_prefix_common_suffix(PA, PALeft),
									  {Member,
									   %% 组装发送的发布和ack消息结构
									   activity_cons(Id, pubs_from_queue(Pubs),
													 acks_from_queue(Acks),
													 Activity1)}
							  end
					 end, Id, MembersStateActivity)
		  end, {MembersState, activity_nil()}, AllMembers),
	handle_msg({activity, Left, activity_finalise(Activity)},
			   State #state { members_state = MembersState1 });

handle_msg({catchup, _NotLeft, _MembersState}, State) ->
	{ok, State};

%% 处理左侧镜像队列的GM进程发送过来的广播消息(GM进程循环列表中循环发送消息的实际处理函数)
handle_msg({activity, Left, Activity},
		   State = #state { self          = Self,
							left          = {Left, _MRefL},
							view          = View,
							members_state = MembersState,
							confirms      = Confirms })
  when MembersState =/= undefined ->
	{MembersState1, {Confirms1, Activity1}} =
		lists:foldl(
		  fun ({Id, Pubs, Acks}, MembersStateConfirmsActivity) ->
				   with_member_acc(
					 fun (Member = #member { pending_ack = PA,
											 last_pub    = LP,
											 last_ack    = LA },
						  {Confirms2, Activity2}) ->
							  %% 发送者和自己是一个人则表示消息已经发送回来，或者判断发送者是否在死亡列表中
							  case is_member_alias(Id, Self, View) of
								  true ->
									  %% 根据Pubs寻找到可以进行ack的消息和剩余的等待ack的列表
									  {ToAck, PA1} =
										  find_common(queue_from_pubs(Pubs), PA,
													  queue:new()),
									  LA1 = last_ack(Acks, LA),
									  %% 得到Ack的相当于的ID
									  AckNums = acks_from_queue(ToAck),
									  %% 进行相关的confirm操作
									  Confirms3 = maybe_confirm(
													Self, Id, Confirms2, AckNums),
									  {Member #member { %% 保存剩余的还未ack的消息内容
														pending_ack = PA1,
														%% 更新最新的最后一次确认的消息计数
														last_ack    = LA1 },
													  {Confirms3,
													   %% 向右侧的镜像队列GM进程发送需要进行ack操作的AckNums列表
													   activity_cons(
														 Id, [], AckNums, Activity2)}};
								  %% Id对应的镜像队列成员没有死亡的情况或者消息发送者不是自己
								  false ->
									  %% 获得最新的消息缓存
									  PA1 = apply_acks(Acks, join_pubs(PA, Pubs)),
									  %% 得到最后一次ack的计数
									  LA1 = last_ack(Acks, LA),
									  %% 得到最后一次pub的计数
									  LP1 = last_pub(Pubs, LP),
									  {Member #member { pending_ack = PA1,
														last_pub    = LP1,
														last_ack    = LA1 },
													  {Confirms2,
													   activity_cons(Id, Pubs, Acks, Activity2)}}
							  end
					 end, Id, MembersStateConfirmsActivity)
		  end, {MembersState, {Confirms, activity_nil()}}, Activity),
	%% 更新最新的成员数据结构和confirms字段
	State1 = State #state { members_state = MembersState1,
							confirms      = Confirms1 },
	%% 获得需要继续向右侧镜像队列发送的Activity数据
	Activity3 = activity_finalise(Activity1),
	%% 将广播数据发送给自己右侧的镜像队列的GM进程
	ok = maybe_send_activity(Activity3, State1),
	%% 如果死亡的镜像队列pub和ack相同后，则将死亡的镜像队列从群组中删除掉
	{Result, State2} = maybe_erase_aliases(State1, View),
	if_callback_success(
	  %% GM进程获得左侧的镜像队列GM进程发送过来的数据，然后回调自己的队列模块处理数据
	  Result, fun activity_true/3, fun activity_false/3, Activity3, State2);

handle_msg({activity, _NotLeft, _Activity}, State) ->
	{ok, State}.


%% 对客户端请求没有回复的接口
noreply(State) ->
	{noreply, ensure_broadcast_timer(State), flush_timeout(State)}.


%% 对客户度请求有回复数据的接口
reply(Reply, State) ->
	{reply, Reply, ensure_broadcast_timer(State), flush_timeout(State)}.


%% 当当前进程中没有消息的情况，且广播缓存中有数据，则立刻将缓存数据进行广播
flush_timeout(#state{broadcast_buffer = []}) -> hibernate;
flush_timeout(_)                             -> 0.


%% 确保广播定时器的关闭和开启，当广播缓存中有数据则启动定时器，当广播缓存中没有数据则停止定时器
%% 广播缓存中没有数据，同时广播定时器不存在的情况
ensure_broadcast_timer(State = #state { broadcast_buffer = [],
										broadcast_timer  = undefined }) ->
	State;
%% 广播缓存中没有数据，同时广播定时器存在，则直接将定时器删除掉
ensure_broadcast_timer(State = #state { broadcast_buffer = [],
										broadcast_timer  = TRef }) ->
	erlang:cancel_timer(TRef),
	State #state { broadcast_timer = undefined };
%% 广播缓存中有数据且没有定时器的情况
ensure_broadcast_timer(State = #state { broadcast_timer = undefined }) ->
	TRef = erlang:send_after(?BROADCAST_TIMER, self(), flush),
	State #state { broadcast_timer = TRef };
ensure_broadcast_timer(State) ->
	State.


%% GM进程内部广播的接口(先调用本GM进程的回调进程进行处理消息，然后将广播数据放入广播缓存中)
internal_broadcast(Msg, SizeHint,
				   State = #state { self                = Self,
									pub_count           = PubCount,
									module              = Module,
									callback_args       = Args,
									broadcast_buffer    = Buffer,
									broadcast_buffer_sz = BufferSize }) ->
	%% 将发布次数加一
	PubCount1 = PubCount + 1,
	{%% 先将消息调用回调模块进行处理
	 Module:handle_msg(Args, get_pid(Self), Msg),
	 %% 然后将广播消息放入广播缓存
	 State #state { pub_count           = PubCount1,
					broadcast_buffer    = [{PubCount1, Msg} | Buffer],
					broadcast_buffer_sz = BufferSize + SizeHint}}.

%% The Erlang distribution mechanism has an interesting quirk - it
%% will kill the VM cold with "Absurdly large distribution output data
%% buffer" if you attempt to send a message which serialises out to
%% more than 2^31 bytes in size. It's therefore a very good idea to
%% make sure that we don't exceed that size!
%%
%% Now, we could figure out the size of messages as they come in using
%% size(term_to_binary(Msg)) or similar. The trouble is, that requires
%% us to serialise the message only to throw the serialised form
%% away. Hard to believe that's a sensible thing to do. So instead we
%% accept a size hint from the application, via broadcast/3. This size
%% hint can be the size of anything in the message which we expect
%% could be large, and we just ignore the size of any small bits of
%% the message term. Therefore MAX_BUFFER_SIZE is set somewhat
%% conservatively at 100MB - but the buffer is only to allow us to
%% buffer tiny messages anyway, so 100MB is plenty.

maybe_flush_broadcast_buffer(State = #state{broadcast_buffer_sz = Size}) ->
	case Size > ?MAX_BUFFER_SIZE of
		true  -> flush_broadcast_buffer(State);
		false -> State
	end.


%% 将广播缓存中的缓存数据向自己右侧的镜像队列GM进程进行广播
flush_broadcast_buffer(State = #state { broadcast_buffer = [] }) ->
	State;
flush_broadcast_buffer(State = #state { self             = Self,
										members_state    = MembersState,
										broadcast_buffer = Buffer,
										pub_count        = PubCount }) ->
	%% 确保缓存中的发布次数和自己状态中记录的发布次数一致
	[{PubCount, _Msg} | _] = Buffer, %% ASSERTION match on PubCount
	%% 将缓存中的所有消息发布给自己右侧的GM群组成员
	Pubs = lists:reverse(Buffer),
	%% 组装要广播的数据结构
	Activity = activity_cons(Self, Pubs, [], activity_nil()),
	%% 将广播数据发送给自己右侧的镜像队列的GM进程
	ok = maybe_send_activity(activity_finalise(Activity), State),
	%% 根据广播消息更新自己GM进程对应的成员信息
	MembersState1 = with_member(
					  fun (Member = #member { pending_ack = PA }) ->
							   PA1 = queue:join(PA, queue:from_list(Pubs)),
							   Member #member { pending_ack = PA1,
												last_pub = PubCount }
					  end, Self, MembersState),
	%% 更新自己GM进程对应的成员信息字段，将当前GM进程中的广播缓存数据清空
	State #state { members_state       = MembersState1,
				   broadcast_buffer    = [],
				   broadcast_buffer_sz = 0}.

%% ---------------------------------------------------------------------------
%% View construction and inspection
%% ---------------------------------------------------------------------------
%% 根据版本号判断是否需要进行视图的更新
needs_view_update(ReqVer, {Ver, _View}) -> Ver < ReqVer.


%% 获得视图的版本号
view_version({Ver, _View}) -> Ver.


%% 判断群成员是否还存活
is_member_alive({dead, _Member}) -> false;
is_member_alive(_)               -> true.


%% 发送者和自己是一个人则表示消息已经发送回来，或者判断发送者是否在死亡列表中
%% 发送者和自己是一个人则表示消息已经发送回来
is_member_alias(Self, Self, _View) ->
	true;
%% 判断发送者是否在死亡列表中
is_member_alias(Member, Self, View) ->
	?SETS:is_element(Member,
					 ((fetch_view_member(Self, View)) #view_member.aliases)).


dead_member_id({dead, Member}) -> Member.


%% 存储视图成员信息
store_view_member(VMember = #view_member { id = Id }, {Ver, View}) ->
	{Ver, ?DICT:store(Id, VMember, View)}.


%% 对Id对应的视图信息执行Fun函数
with_view_member(Fun, View, Id) ->
	store_view_member(Fun(fetch_view_member(Id, View)), View).


%% 根据ID将视图信息从字典中读取出来
fetch_view_member(Id, {_Ver, View}) -> ?DICT:fetch(Id, View).


%% 根据ID从字典中查找视图信息
find_view_member(Id, {_Ver, View}) -> ?DICT:find(Id, View).


%% 初始化一个空白的视图数据结构信息
blank_view(Ver) -> {Ver, ?DICT:new()}.


%% 获取镜像队列视图的所有key列表
alive_view_members({_Ver, View}) -> ?DICT:fetch_keys(View).


%% 获得镜像队列循环队列视图中的所有成员
all_known_members({_Ver, View}) ->
	?DICT:fold(
	  fun (Member, #view_member { aliases = Aliases }, Acc) ->
			   ?SETS:to_list(Aliases) ++ [Member | Acc]
	  end, [], View).


%% 将所有存活的镜像队列组装镜像队列群组循环队列视图
group_to_view(#gm_group { members = Members, version = Ver }) ->
	Alive = lists:filter(fun is_member_alive/1, Members),
	[_ | _] = Alive, %% ASSERTION - can't have all dead members
	%% 此处Alive ++ Alive ++ Alive是为了只有一个镜像队列的情况(这样能处理任何数量的镜像队列)
	%% add_aliases函数是将死亡的成员放入到自己右侧成员的aliases字段中
	add_aliases(link_view(Alive ++ Alive ++ Alive, blank_view(Ver)), Members).


%% 连接镜像队列GM进程的群信息，组成一个循环列表(将都是存活的镜像队列GM进程组成循环队列)
link_view([Left, Middle, Right | Rest], View) ->
	case find_view_member(Middle, View) of
		error ->
			link_view(
			  [Middle, Right | Rest],
			  %% 存储视图成员信息
			  store_view_member(#view_member { id      = Middle,
											   aliases = ?SETS:new(),			%% aliases：别名
											   left    = Left,
											   right   = Right }, View));
		{ok, _} ->
			View
	end;
link_view(_, View) ->
	View.


%% suffix：后缀
%% 将死亡的成员放入到自己右侧成员的aliases字段中
add_aliases(View, Members) ->
	%% 确保镜像队列成员列表中的最后一个成员是存活状态
	Members1 = ensure_alive_suffix(Members),
	{EmptyDeadSet, View1} =
		lists:foldl(
		  fun (Member, {DeadAcc, ViewAcc}) ->
				   case is_member_alive(Member) of
					   true ->
						   %% 如果当前成员是存活，
						   {?SETS:new(),
							with_view_member(
							  fun (VMember =
									   #view_member { aliases = Aliases }) ->
									   VMember #view_member {
															 aliases = ?SETS:union(Aliases, DeadAcc) }
							  end, ViewAcc, Member)};
					   false ->
						   {?SETS:add_element(dead_member_id(Member), DeadAcc),
							ViewAcc}
				   end
		  end, {?SETS:new(), View}, Members1),
	0 = ?SETS:size(EmptyDeadSet), %% ASSERTION
	View1.


%% 确保镜像队列成员列表中的最后一个成员是存活状态
%% queue:from_list(List)函数是将列表中的头部作为队列的头部，尾部作为队列数据结构的尾部
ensure_alive_suffix(Members) ->
	queue:to_list(ensure_alive_suffix1(queue:from_list(Members))).


%% 确保列表的最后一个元素是存活状态
ensure_alive_suffix1(MembersQ) ->
	%% 获得成员列表的最后一个元素
	{{value, Member}, MembersQ1} = queue:out_r(MembersQ),
	%% 如果最后一个元素死亡状态则将成员放入到列表的头部
	case is_member_alive(Member) of
		true  -> MembersQ;
		false -> ensure_alive_suffix1(queue:in_r(Member, MembersQ1))
	end.

%% ---------------------------------------------------------------------------
%% View modification
%% ---------------------------------------------------------------------------
%% 当前GM进程加入到镜像队列群组中的接口
join_group(Self, GroupName, TxnFun) ->
	join_group(Self, GroupName, dirty_read_group(GroupName), TxnFun).


%% 镜像队列群主第一次启动
join_group(Self, GroupName, {error, not_found}, TxnFun) ->
	join_group(Self, GroupName,
			   %% 创建消息队列的镜像群信息结构
			   prune_or_create_group(Self, GroupName, TxnFun), TxnFun);

%% 群成员结构中只有一个成员的情况，且该成员就是自己
join_group(Self, _GroupName, #gm_group { members = [Self] } = Group, _TxnFun) ->
	%% 将所有存活的镜像队列组装镜像队列群组循环队列视图
	group_to_view(Group);

join_group(Self, GroupName, #gm_group { members = Members } = Group, TxnFun) ->
	case lists:member(Self, Members) of
		true ->
			%% 将所有存活的镜像队列组装镜像队列群组循环队列视图
			group_to_view(Group);
		false ->
			case lists:filter(fun is_member_alive/1, Members) of
				[] ->
					%% 当前GM群组中没有一个处于存活状态，则重新创建gm_group数据库数据
					join_group(Self, GroupName,
							   prune_or_create_group(Self, GroupName, TxnFun),
							   TxnFun);
				Alive ->
					%% 从存活的镜像队列中随机一个镜像队列(准备将自己加入到这个随机的gm进程的右侧)
					Left = lists:nth(random:uniform(length(Alive)), Alive),
					Handler =
						fun () ->
								 join_group(
								   Self, GroupName,
								   record_dead_member_in_group(
									 Left, GroupName, TxnFun),
								   TxnFun)
						end,
					try
						%% 将新加入的镜像队列加入到从存活的镜像队列随机出来的镜像队列的右侧
						case neighbour_call(Left, {add_on_right, Self}) of
							%% 将所有存活的镜像队列组装镜像队列群组循环队列视图
							{ok, Group1} -> group_to_view(Group1);
							not_ready    -> join_group(Self, GroupName, TxnFun)
						end
					catch
						exit:{R, _}
						  when R =:= noproc; R =:= normal; R =:= shutdown ->
							Handler();
						exit:{{R, _}, _}
						  when R =:= nodedown; R =:= shutdown ->
							Handler()
					end
			end
	end.


%% 从Mnesia数据库根据群组名字得到群组信息
dirty_read_group(GroupName) ->
	case mnesia:dirty_read(?GROUP_TABLE, GroupName) of
		[]      -> {error, not_found};
		[Group] -> Group
	end.


%% 根据组名字从Mnesia数据库读取群数据结构信息
read_group(GroupName) ->
	case mnesia:read({?GROUP_TABLE, GroupName}) of
		[]      -> {error, not_found};
		[Group] -> Group
	end.


%% 根据组名字将组数据结构信息写入Mnesia数据库
write_group(Group) -> mnesia:write(?GROUP_TABLE, Group, write), Group.


%% 创建消息队列的镜像群信息结构
prune_or_create_group(Self, GroupName, TxnFun) ->
	TxnFun(
	  fun () ->
			   %% 创建镜像队列群数据结构
			   GroupNew = #gm_group { name    = GroupName,
									  members = [Self],
									  version = get_version(Self) },
			   %% 根据群名字从Mnesia数据库读取群数据结构信息
			   case read_group(GroupName) of
				   {error, not_found} ->
					   write_group(GroupNew);
				   Group = #gm_group { members = Members } ->
					   case lists:any(fun is_member_alive/1, Members) of
						   true  -> Group;
						   %% 镜像队列成员中没有一个存活，则直接将新的镜像队列群组写入Mnesia数据库
						   false -> write_group(GroupNew)
					   end
			   end
	  end).


%% 记录有镜像队列成员死亡的信息(有成员死亡的时候会将版本号增加一)
record_dead_member_in_group(Member, GroupName, TxnFun) ->
	TxnFun(
	  fun () ->
			   Group = #gm_group { members = Members, version = Ver } =
									 read_group(GroupName),
			   case lists:splitwith(
					  fun (Member1) -> Member1 =/= Member end, Members) of
				   {_Members1, []} -> %% not found - already recorded dead
					   Group;
				   {Members1, [Member | Members2]} ->
					   Members3 = Members1 ++ [{dead, Member} | Members2],
					   write_group(Group #gm_group { members = Members3,
													 %% 有成员死亡的时候会将版本号增加一
													 version = Ver + 1 })
			   end
	  end).


%% 记录将新的镜像队列成员加入到镜像队列组中，将新加入的镜像队列写入gm_group结构中的members字段中(有新成员加入群组的时候，则将版本号增加一)
record_new_member_in_group(NewMember, Left, GroupName, TxnFun) ->
	TxnFun(
	  fun () ->
			   Group = #gm_group { members = Members, version = Ver } =
									 %% 根据组名字从Mnesia数据库读取群数据结构信息
									 read_group(GroupName),
			   {Prefix, [Left | Suffix]} =
				   lists:splitwith(fun (M) -> M =/= Left end, Members),
			   write_group(Group #gm_group {
											members = Prefix ++ [Left, NewMember | Suffix],
											%% 有新成员加入群组的时候，则将版本号增加一
											version = Ver + 1 })
	  end).


%% 将GM成员列表从GM群组中删除掉(将GM成员从Group群组中删除后，则将版本号增加一)
erase_members_in_group(Members, GroupName, TxnFun) ->
	DeadMembers = [{dead, Id} || Id <- Members],
	TxnFun(
	  fun () ->
			   Group = #gm_group { members = [_ | _] = Members1, version = Ver } =
									 read_group(GroupName),
			   case Members1 -- DeadMembers of
				   Members1 -> Group;
				   Members2 -> write_group(
								 Group #gm_group { members = Members2,
												   %% 将GM成员从Group群组中删除后，则将版本号增加一
												   version = Ver + 1 })
			   end
	  end).


%% 删除能够删除的死亡的镜像队列进程信息
maybe_erase_aliases(State = #state { self          = Self,
									 group_name    = GroupName,
									 members_state = MembersState,
									 txn_executor  = TxnFun }, View) ->
	#view_member { aliases = Aliases } = fetch_view_member(Self, View),
	{Erasable, MembersState1}
		= ?SETS:fold(
			fun (Id, {ErasableAcc, MembersStateAcc} = Acc) ->
					 #member { last_pub = LP, last_ack = LA } =
								 find_member_or_blank(Id, MembersState),
					 %% 判断能否删除镜像队列成员
					 case can_erase_view_member(Self, Id, LA, LP) of
						 true  -> {[Id | ErasableAcc],
								   erase_member(Id, MembersStateAcc)};
						 false -> Acc
					 end
			end, {[], MembersState}, Aliases),
	View1 = case Erasable of
				[] -> View;
				_  -> group_to_view(
						erase_members_in_group(Erasable, GroupName, TxnFun))
			end,
	%% 根据新的镜像队列循环队列视图和老的视图修改视图，同时根据镜像队列循环视图更新自己左右邻居信息
	change_view(View1, State #state { members_state = MembersState1 }).


%% 判断能否删除镜像队列成员(只有pub和ack列表相同，则才能将GM进程从群组中删除掉)
can_erase_view_member(Self, Self, _LA, _LP) -> false;
can_erase_view_member(_Self, _Id,   N,   N) -> true;
can_erase_view_member(_Self, _Id, _LA, _LP) -> false.


%% 异步向邻居N发送Msg信息
neighbour_cast(N, Msg) -> ?INSTR_MOD:cast(get_pid(N), Msg).


%% 同步向邻居N发送Msg信息
neighbour_call(N, Msg) -> ?INSTR_MOD:call(get_pid(N), Msg, infinity).

%% ---------------------------------------------------------------------------
%% View monitoring and maintanence
%% ---------------------------------------------------------------------------
%% 该情况是只有一个镜像队列时候的情况
ensure_neighbour(_Ver, Self, {Self, undefined}, Self) ->
	{Self, undefined};
%% 此情况是RealNeighbour成为了Self镜像队列最新邻居，因此需要更新邻居信息，同时需要监视这个邻居镜像队列进程
ensure_neighbour(Ver, Self, {Self, undefined}, RealNeighbour) ->
	ok = neighbour_cast(RealNeighbour, {?TAG, Ver, check_neighbours}),
	{RealNeighbour, maybe_monitor(RealNeighbour, Self)};
%% 此情况是最新的邻居镜像队列和老的邻居镜像队列是一样的，则直接返回邻居镜像队列信息
ensure_neighbour(_Ver, _Self, {RealNeighbour, MRef}, RealNeighbour) ->
	{RealNeighbour, MRef};
%% 此情况是最新的邻居镜像队列和老的邻居镜像队列是不一样的，则需要将老的邻居镜像队列删除，解除监视，然后更新最新的邻居镜像队列
ensure_neighbour(Ver, Self, {RealNeighbour, MRef}, Neighbour) ->
	%% 解除对老的邻居镜像队列的监视
	true = ?INSTR_MOD:demonitor(MRef),
	Msg = {?TAG, Ver, check_neighbours},
	%% 通知老的邻居镜像队列检查邻居信息
	ok = neighbour_cast(RealNeighbour, Msg),
	ok = case Neighbour of
			 Self -> ok;
			 _    -> neighbour_cast(Neighbour, Msg)
		 end,
	%% 返回最新的邻居镜像队列信息，同时监视这个心的邻居镜像队列
	{Neighbour, maybe_monitor(Neighbour, Self)}.


%% 监视对应进程
maybe_monitor( Self,  Self) -> undefined;
maybe_monitor(Other, _Self) -> ?INSTR_MOD:monitor(get_pid(Other)).


%% 检查当前镜像队列的邻居信息(根据消息镜像队列的群组循环视图更新自己最新的左右两边的镜像队列)
check_neighbours(State = #state { self             = Self,
								  left             = Left,
								  right            = Right,
								  view             = View,
								  broadcast_buffer = Buffer }) ->
	#view_member { left = VLeft, right = VRight }
					 = fetch_view_member(Self, View),
	%% 从视图中获取版本号
	Ver = view_version(View),
	%% 更新自己左侧最新的GM进程信息
	Left1 = ensure_neighbour(Ver, Self, Left, VLeft),
	%% 更新自己右侧最新的GM进程信息
	Right1 = ensure_neighbour(Ver, Self, Right, VRight),
	Buffer1 = case Right1 of
				  {Self, undefined} -> [];
				  _                 -> Buffer
			  end,
	%% 更新最新的左右邻居镜像队列信息
	State1 = State #state { left = Left1, right = Right1,
							broadcast_buffer = Buffer1 },
	ok = maybe_send_catchup(Right, State1),
	State1.


%% 自己右侧成员没有变化的情况
maybe_send_catchup(Right, #state { right = Right }) ->
	ok;
%% 自己右侧还没有GM群组成员的情况
maybe_send_catchup(_Right, #state { self  = Self,
									right = {Self, undefined} }) ->
	ok;
%% 当前GM群组成员的存储数据的数据结构还未初始化的情况
maybe_send_catchup(_Right, #state { members_state = undefined }) ->
	ok;
%% 自己右侧GM群组成员有变化同时已经成功的监视到它，则通知它最新的GM群组成员的数据信息
%% 此处是有GM进程挂掉后，通知挂掉的GM进程右侧的GM进程
maybe_send_catchup(_Right, #state { self          = Self,
									right         = {Right, _MRef},
									view          = View,
									members_state = MembersState }) ->
	send_right(Right, View,
			   {catchup, Self, prepare_members_state(MembersState)}).


%% ---------------------------------------------------------------------------
%% Catch_up delta detection
%% ---------------------------------------------------------------------------
%% 得到用B队列作为分割线来分割A队列的三段数据，相同前面的元素列表，相同的元素列表，相同之后的元素列表
find_prefix_common_suffix(A, B) ->
	%% 得到A队列跟B队列中元素相同之前的元素列表
	{Prefix, A1} = find_prefix(A, B, queue:new()),
	%% 得到A队列和B队列相同的元素和剩下不同的元素
	{Common, Suffix} = find_common(A1, B, queue:new()),
	{Prefix, Common, Suffix}.

%% Returns the elements of A that occur before the first element of B,
%% plus the remainder of A.
find_prefix(A, B, Prefix) ->
	case {queue:out(A), queue:out(B)} of
		{{{value, Val}, _A1}, {{value, Val}, _B1}} ->
			{Prefix, A};
		{{empty, A1}, {{value, _A}, _B1}} ->
			{Prefix, A1};
		{{{value, {NumA, _MsgA} = Val}, A1},
		 {{value, {NumB, _MsgB}}, _B1}} when NumA < NumB ->
			find_prefix(A1, B, queue:in(Val, Prefix));
		{_, {empty, _B1}} ->
			{A, Prefix} %% Prefix well be empty here
	end.

%% A should be a prefix of B. Returns the commonality plus the
%% remainder of B.
%% 将A和B队列中头部相同的元素放入到Common队列中
find_common(A, B, Common) ->
	case {queue:out(A), queue:out(B)} of
		{{{value, Val}, A1}, {{value, Val}, B1}} ->
			find_common(A1, B1, queue:in(Val, Common));
		{{empty, _A}, _} ->
			{Common, B}
	end.


%% ---------------------------------------------------------------------------
%% Members helpers
%% ---------------------------------------------------------------------------

with_member(Fun, Id, MembersState) ->
	store_member(
	  Id, Fun(find_member_or_blank(Id, MembersState)), MembersState).


%% 对Id对应的镜像队列执行Fun函数，然后将新的镜像队列成员信息存储起来
with_member_acc(Fun, Id, {MembersState, Acc}) ->
	{MemberState, Acc1} = Fun(find_member_or_blank(Id, MembersState), Acc),
	{store_member(Id, MemberState, MembersState), Acc1}.


%% 根据ID从成员字典中查找对应的信息
find_member_or_blank(Id, MembersState) ->
	case ?DICT:find(Id, MembersState) of
		{ok, Result} -> Result;
		error        -> blank_member()
	end.


%% 将ID对应的成员从成员字典中删除
erase_member(Id, MembersState) -> ?DICT:erase(Id, MembersState).


%% 得到空白成员的数据结构
blank_member() ->
	#member { pending_ack = queue:new(), last_pub = -1, last_ack = -1 }.


%% 空白的成员数据结构，返回的是字典数据结构
blank_member_state() -> ?DICT:new().


store_member(Id, MemberState, MembersState) ->
	?DICT:store(Id, MemberState, MembersState).


%% 将存储成员信息的字典数据结构中的数据转换为列表
prepare_members_state(MembersState) -> ?DICT:to_list(MembersState).


%% 将成员信息转化成字典数据结构
build_members_state(MembersStateList) -> ?DICT:from_list(MembersStateList).


%% 组装单个成员信息
make_member(GroupName) ->
	{case dirty_read_group(GroupName) of
		 #gm_group { version = Version } -> Version;
		 {error, not_found}              -> ?VERSION_START
	 end, self()}.


%% 删除擦除的成员
remove_erased_members(MembersState, View) ->
	lists:foldl(fun (Id, MembersState1) ->
						 store_member(Id, find_member_or_blank(Id, MembersState),
									  MembersState1)
				end, blank_member_state(), all_known_members(View)).


%% 获取版本号
get_version({Version, _Pid}) -> Version.


get_pid({_Version, Pid}) -> Pid.


get_pids(Ids) -> [Pid || {_Version, Pid} <- Ids].

%% ---------------------------------------------------------------------------
%% Activity assembly
%% ---------------------------------------------------------------------------

activity_nil() -> queue:new().


%% 组装发送的发布和ack消息结构
activity_cons(   _Id,   [],   [], Tail) -> Tail;
activity_cons(Sender, Pubs, Acks, Tail) -> queue:in({Sender, Pubs, Acks}, Tail).


activity_finalise(Activity) -> queue:to_list(Activity).


%% 将广播数据发送给自己右侧的镜像队列的GM进程(如果要发送的数据为空则什么都不做)
maybe_send_activity([], _State) ->
	ok;
maybe_send_activity(Activity, #state { self  = Self,
									   right = {Right, _MRefR},
									   view  = View }) ->
	send_right(Right, View, {activity, Self, Activity}).


%% 向右侧镜像队列的GM进程发送广播消息的接口
send_right(Right, View, Msg) ->
	ok = neighbour_cast(Right, {?TAG, view_version(View), Msg}).


%% 将收到的左侧镜像队列GM进程发送过来的队列操作数据使用本GM进程的回调模块进行回调
%% 即回调给自己的队列进程进行相关的pub，fetch，ack等操作
callback(Args, Module, Activity) ->
	Result =
		lists:foldl(
		  fun ({Id, Pubs, _Acks}, {Args1, Module1, ok}) ->
				   lists:foldl(fun ({_PubNum, Pub}, Acc = {Args2, Module2, ok}) ->
										%% 实际的回调到自己对应的队列进程去处理发布的消息
										case Module2:handle_msg(
											   Args2, get_pid(Id), Pub) of
											ok ->
												Acc;
											{become, Module3, Args3} ->
												{Args3, Module3, ok};
											{stop, _Reason} = Error ->
												Error
										end;
								  (_, Error = {stop, _Reason}) ->
									   Error
							   end, {Args1, Module1, ok}, Pubs);
			 (_, Error = {stop, _Reason}) ->
				  Error
		  end, {Args, Module, ok}, Activity),
	case Result of
		{Args, Module, ok}      -> ok;
		%% 调换回调模块和回调参数即队列进程Pid
		{Args1, Module1, ok}    -> {become, Module1, Args1};
		{stop, _Reason} = Error -> Error
	end.


%% 根据新的镜像队列循环队列视图和老的视图修改视图，同时根据镜像队列循环视图更新自己左右邻居信息
change_view(View, State = #state { view          = View0,
								   module        = Module,
								   callback_args = Args }) ->
	%% 得到老的视图中的所有镜像队列成员
	OldMembers = all_known_members(View0),
	%% 得到新的视图中的所有镜像队列成员
	NewMembers = all_known_members(View),
	%% 得到新生成的镜像队列成员
	Births = NewMembers -- OldMembers,
	%% 得到删除的镜像队列成员
	Deaths = OldMembers -- NewMembers,
	Result = case {Births, Deaths} of
				 {[], []} -> ok;
				 _        -> Module:members_changed(
							   Args, get_pids(Births), get_pids(Deaths))
			 end,
	%% 检查当前镜像队列的邻居信息
	{Result, check_neighbours(State #state { view = View })}.


%% 有结果返回的接口
handle_callback_result({Result, State}) ->
	if_callback_success(
	  Result, fun no_reply_true/3, fun no_reply_false/3, undefined, State);

%% 无结果返回的接口
handle_callback_result({Result, Reply, State}) ->
	if_callback_success(
	  Result, fun reply_true/3, fun reply_false/3, Reply, State).


%% 没有结果返回的接口
no_reply_true (_Result,        _Undefined, State) -> noreply(State).
%% 没有结果返回的接口(停止本GM进程的接口)
no_reply_false({stop, Reason}, _Undefined, State) -> {stop, Reason, State}.


%% 有结果返回的接口
reply_true (_Result,        Reply, State) -> reply(Reply, State).
%% 有结果返回的接口(停止本GM进程的接口)
reply_false({stop, Reason}, Reply, State) -> {stop, Reason, Reply, State}.


%% 可以正确处理消息的接口
handle_msg_true (_Result, Msg, State) -> handle_msg(Msg, State).
%% 不能正确处理消息，将Result原因返回
handle_msg_false(Result, _Msg, State) -> {Result, State}.


%% GM进程获得左侧的镜像队列GM进程发送过来的数据，然后回调自己的队列模块处理数据
activity_true(_Result, Activity, State = #state { module        = Module,
												  callback_args = Args }) ->
	{callback(Args, Module, Activity), State}.


activity_false(Result, _Activity, State) ->
	{Result, State}.


%% 处理回调给对应的消息队列的接口
if_callback_success(ok, True, _False, Arg, State) ->
	True(ok, Arg, State);
%% 处理回调给对应的消息队列的接口(此处是改变回调的模块名字和参数，即修改消息队列的Pid)
if_callback_success(
  {become, Module, Args} = Result, True, _False, Arg, State) ->
	%% 更新自己最新的回调模块已经回调队列进程
	True(Result, Arg, State #state { module        = Module,
									 callback_args = Args });
%% 此处是回调出错，直接调用Flase函数
if_callback_success({stop, _Reason} = Result, _True, False, Arg, State) ->
	False(Result, Arg, State).


%% 进行相关的confirm操作
maybe_confirm(_Self, _Id, Confirms, []) ->
	Confirms;
maybe_confirm(Self, Self, Confirms, [PubNum | PubNums]) ->
	case queue:out(Confirms) of
		{empty, _Confirms} ->
			Confirms;
		{{value, {PubNum, From}}, Confirms1} ->
			gen_server2:reply(From, ok),
			maybe_confirm(Self, Self, Confirms1, PubNums);
		{{value, {PubNum1, _From}}, _Confirms} when PubNum1 > PubNum ->
			maybe_confirm(Self, Self, Confirms, PubNums)
	end;
maybe_confirm(_Self, _Id, Confirms, _PubNums) ->
	Confirms.


purge_confirms(Confirms) ->
	[gen_server2:reply(From, ok) || {_PubNum, From} <- queue:to_list(Confirms)],
	queue:new().


%% ---------------------------------------------------------------------------
%% Msg transformation
%% ---------------------------------------------------------------------------

acks_from_queue(Q) -> [PubNum || {PubNum, _Msg} <- queue:to_list(Q)].


pubs_from_queue(Q) -> queue:to_list(Q).


queue_from_pubs(Pubs) -> queue:from_list(Pubs).


apply_acks(  [], Pubs) -> Pubs;
apply_acks(List, Pubs) -> {_, Pubs1} = queue:split(length(List), Pubs),
						  Pubs1.


%% 将新的发布信息加入到老的发布队列后面
join_pubs(Q, [])   -> Q;
join_pubs(Q, Pubs) -> queue:join(Q, queue_from_pubs(Pubs)).


last_ack(  [], LA) -> LA;
last_ack(List, LA) -> LA1 = lists:last(List),
					  true = LA1 > LA, %% ASSERTION
					  LA1.


last_pub(  [], LP) -> LP;
last_pub(List, LP) -> {PubNum, _Msg} = lists:last(List),
					  true = PubNum > LP, %% ASSERTION
					  PubNum.

%% ---------------------------------------------------------------------------

%% Uninstrumented versions

call(Pid, Msg, Timeout) -> gen_server2:call(Pid, Msg, Timeout).


cast(Pid, Msg)          -> gen_server2:cast(Pid, Msg).


monitor(Pid)            -> erlang:monitor(process, Pid).


demonitor(MRef)         -> erlang:demonitor(MRef).
