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

-module(rabbit_amqqueue).

-export([recover/0, stop/0, start/1, declare/5, declare/6,
         delete_immediately/1, delete/3, purge/1, forget_all_durable/1,
         delete_crashed/1, delete_crashed_internal/1]).
-export([pseudo_queue/2, immutable/1]).
-export([lookup/1, not_found_or_absent/1, with/2, with/3, with_or_die/2,
         assert_equivalence/5,
         check_exclusive_access/2, with_exclusive_access_or_die/3,
         stat/1, deliver/2, requeue/3, ack/3, reject/4]).
-export([list/0, list/1, info_keys/0, info/1, info/2, info_all/1, info_all/2]).
-export([list_down/1]).
-export([force_event_refresh/1, notify_policy_changed/1]).
-export([consumers/1, consumers_all/1, consumer_info_keys/0]).
-export([basic_get/4, basic_consume/10, basic_cancel/4, notify_decorators/1]).
-export([notify_sent/2, notify_sent_queue_down/1, resume/2]).
-export([notify_down_all/2, activate_limit_all/2, credit/5]).
-export([on_node_up/1, on_node_down/1]).
-export([update/2, store_queue/1, update_decorators/1, policy_changed/2]).
-export([start_mirroring/1, stop_mirroring/1, sync_mirrors/1,
         cancel_sync_mirrors/1]).

%% internal
-export([internal_declare/2, internal_delete/1, run_backing_queue/3,
         set_ram_duration_target/2, set_maximum_since_use/2]).

-include("rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(INTEGER_ARG_TYPES, [byte, short, signedint, long]).					%% RabbitMQ系统中队列支持的数据类型

-define(MORE_CONSUMER_CREDIT_AFTER, 50).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([name/0, qmsg/0, absent_reason/0]).

-type(name() :: rabbit_types:r('queue')).
-type(qpids() :: [pid()]).
-type(qlen() :: rabbit_types:ok(non_neg_integer())).
-type(qfun(A) :: fun ((rabbit_types:amqqueue()) -> A | no_return())).
-type(qmsg() :: {name(), pid(), msg_id(), boolean(), rabbit_types:message()}).
-type(msg_id() :: non_neg_integer()).
-type(ok_or_errors() ::
        'ok' | {'error', [{'error' | 'exit' | 'throw', any()}]}).
-type(absent_reason() :: 'nodedown' | 'crashed').
-type(queue_or_absent() :: rabbit_types:amqqueue() |
                           {'absent', rabbit_types:amqqueue(),absent_reason()}).
-type(not_found_or_absent() ::
        'not_found' | {'absent', rabbit_types:amqqueue(), absent_reason()}).
-spec(recover/0 :: () -> [rabbit_types:amqqueue()]).
-spec(stop/0 :: () -> 'ok').
-spec(start/1 :: ([rabbit_types:amqqueue()]) -> 'ok').
-spec(declare/5 ::
        (name(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid()))
        -> {'new' | 'existing' | 'absent' | 'owner_died',
            rabbit_types:amqqueue()} | rabbit_types:channel_exit()).
-spec(declare/6 ::
        (name(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid()), node())
        -> {'new' | 'existing' | 'owner_died', rabbit_types:amqqueue()} |
           {'absent', rabbit_types:amqqueue(), absent_reason()} |
           rabbit_types:channel_exit()).
-spec(internal_declare/2 ::
        (rabbit_types:amqqueue(), boolean())
        -> queue_or_absent() | rabbit_misc:thunk(queue_or_absent())).
-spec(update/2 ::
        (name(),
         fun((rabbit_types:amqqueue()) -> rabbit_types:amqqueue()))
         -> 'not_found' | rabbit_types:amqqueue()).
-spec(lookup/1 ::
        (name()) -> rabbit_types:ok(rabbit_types:amqqueue()) |
                    rabbit_types:error('not_found');
        ([name()]) -> [rabbit_types:amqqueue()]).
-spec(not_found_or_absent/1 :: (name()) -> not_found_or_absent()).
-spec(with/2 :: (name(), qfun(A)) ->
                     A | rabbit_types:error(not_found_or_absent())).
-spec(with/3 :: (name(), qfun(A), fun((not_found_or_absent()) -> B)) -> A | B).
-spec(with_or_die/2 ::
        (name(), qfun(A)) -> A | rabbit_types:channel_exit()).
-spec(assert_equivalence/5 ::
        (rabbit_types:amqqueue(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:maybe(pid()))
        -> 'ok' | rabbit_types:channel_exit() |
           rabbit_types:connection_exit()).
-spec(check_exclusive_access/2 ::
        (rabbit_types:amqqueue(), pid())
        -> 'ok' | rabbit_types:channel_exit()).
-spec(with_exclusive_access_or_die/3 ::
        (name(), pid(), qfun(A)) -> A | rabbit_types:channel_exit()).
-spec(list/0 :: () -> [rabbit_types:amqqueue()]).
-spec(list/1 :: (rabbit_types:vhost()) -> [rabbit_types:amqqueue()]).
-spec(list_down/1 :: (rabbit_types:vhost()) -> [rabbit_types:amqqueue()]).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (rabbit_types:amqqueue()) -> rabbit_types:infos()).
-spec(info/2 ::
        (rabbit_types:amqqueue(), rabbit_types:info_keys())
        -> rabbit_types:infos()).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(info_all/2 :: (rabbit_types:vhost(), rabbit_types:info_keys())
                    -> [rabbit_types:infos()]).
-spec(force_event_refresh/1 :: (reference()) -> 'ok').
-spec(notify_policy_changed/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(consumers/1 :: (rabbit_types:amqqueue())
                     -> [{pid(), rabbit_types:ctag(), boolean(),
                          non_neg_integer(), rabbit_framing:amqp_table()}]).
-spec(consumer_info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(consumers_all/1 ::
        (rabbit_types:vhost())
        -> [{name(), pid(), rabbit_types:ctag(), boolean(),
             non_neg_integer(), rabbit_framing:amqp_table()}]).
-spec(stat/1 ::
        (rabbit_types:amqqueue())
        -> {'ok', non_neg_integer(), non_neg_integer()}).
-spec(delete_immediately/1 :: (qpids()) -> 'ok').
-spec(delete/3 ::
        (rabbit_types:amqqueue(), 'false', 'false')
        -> qlen();
        (rabbit_types:amqqueue(), 'true' , 'false')
        -> qlen() | rabbit_types:error('in_use');
        (rabbit_types:amqqueue(), 'false', 'true' )
        -> qlen() | rabbit_types:error('not_empty');
        (rabbit_types:amqqueue(), 'true' , 'true' )
        -> qlen() |
           rabbit_types:error('in_use') |
           rabbit_types:error('not_empty')).
-spec(delete_crashed/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(delete_crashed_internal/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(purge/1 :: (rabbit_types:amqqueue()) -> qlen()).
-spec(forget_all_durable/1 :: (node()) -> 'ok').
-spec(deliver/2 :: ([rabbit_types:amqqueue()], rabbit_types:delivery()) ->
                        qpids()).
-spec(requeue/3 :: (pid(), [msg_id()],  pid()) -> 'ok').
-spec(ack/3 :: (pid(), [msg_id()], pid()) -> 'ok').
-spec(reject/4 :: (pid(), [msg_id()], boolean(), pid()) -> 'ok').
-spec(notify_down_all/2 :: (qpids(), pid()) -> ok_or_errors()).
-spec(activate_limit_all/2 :: (qpids(), pid()) -> ok_or_errors()).
-spec(basic_get/4 :: (rabbit_types:amqqueue(), pid(), boolean(), pid()) ->
                          {'ok', non_neg_integer(), qmsg()} | 'empty').
-spec(credit/5 :: (rabbit_types:amqqueue(), pid(), rabbit_types:ctag(),
                   non_neg_integer(), boolean()) -> 'ok').
-spec(basic_consume/10 ::
        (rabbit_types:amqqueue(), boolean(), pid(), pid(), boolean(),
         non_neg_integer(), rabbit_types:ctag(), boolean(),
         rabbit_framing:amqp_table(), any())
        -> rabbit_types:ok_or_error('exclusive_consume_unavailable')).
-spec(basic_cancel/4 ::
        (rabbit_types:amqqueue(), pid(), rabbit_types:ctag(), any()) -> 'ok').
-spec(notify_decorators/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(notify_sent/2 :: (pid(), pid()) -> 'ok').
-spec(notify_sent_queue_down/1 :: (pid()) -> 'ok').
-spec(resume/2 :: (pid(), pid()) -> 'ok').
-spec(internal_delete/1 ::
        (name()) -> rabbit_types:ok_or_error('not_found') |
                    rabbit_types:connection_exit() |
                    fun (() -> rabbit_types:ok_or_error('not_found') |
                               rabbit_types:connection_exit())).
-spec(run_backing_queue/3 ::
        (pid(), atom(),
         (fun ((atom(), A) -> {[rabbit_types:msg_id()], A}))) -> 'ok').
-spec(set_ram_duration_target/2 :: (pid(), number() | 'infinity') -> 'ok').
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').
-spec(pseudo_queue/2 :: (name(), pid()) -> rabbit_types:amqqueue()).
-spec(immutable/1 :: (rabbit_types:amqqueue()) -> rabbit_types:amqqueue()).
-spec(store_queue/1 :: (rabbit_types:amqqueue()) -> 'ok').
-spec(update_decorators/1 :: (name()) -> 'ok').
-spec(policy_changed/2 ::
        (rabbit_types:amqqueue(), rabbit_types:amqqueue()) -> 'ok').
-spec(start_mirroring/1 :: (pid()) -> 'ok').
-spec(stop_mirroring/1 :: (pid()) -> 'ok').
-spec(sync_mirrors/1 :: (pid()) -> 'ok' | rabbit_types:error('not_mirrored')).
-spec(cancel_sync_mirrors/1 :: (pid()) -> 'ok' | {'ok', 'not_syncing'}).

-endif.

%%----------------------------------------------------------------------------
%% 队列中消费者的关键信息key
-define(CONSUMER_INFO_KEYS,
        [queue_name, channel_pid, consumer_tag, ack_required, prefetch_count,
         arguments]).

%% RabbitMQ启动消息队列的恢复(RabbitMQ系统启动，如果有持久化队列，则启动这些持久化队列)
recover() ->
	%% Clear out(清除) remnants(残存) of old incarnation(化身), in case we restarted
	%% faster than other nodes handled DOWN messages from us.
	on_node_down(node()),
	%% 查找当前节点所有的持久化队列
	DurableQueues = find_durable_queues(),
	{ok, BQ} = application:get_env(rabbit, backing_queue_module),
	
	%% We rely on(依靠) BQ:start/1 returning the recovery terms in the same
	%% order as the supplied(提供) queue names, so that we can zip them together
	%% for further processing in recover_durable_queues.
	{ok, OrderedRecoveryTerms} =
		BQ:start([QName || #amqqueue{name = QName} <- DurableQueues]),
	%% 启动rabbit_amqqueue_sup_sup监督进程，在该监督进程下面会启动rabbit_amqqueue_sup监督进程，在rabbit_amqqueue_sup监督进程下启动队列进程
	%% 每创建一个队列都会在rabbit_amqqueue_sup_sup监督进程下创建rabbit_amqqueue_sup监督进程，然后在在rabbit_amqqueue_sup监督进程下启动队列进程
	{ok,_} = supervisor:start_child(
			   rabbit_sup,
			   {rabbit_amqqueue_sup_sup,
				{rabbit_amqqueue_sup_sup, start_link, []},
				transient, infinity, supervisor, [rabbit_amqqueue_sup_sup]}),
	%% 恢复持久化队列(RabbitMQ系统启动后需要启动持久化的的队列进程)
	recover_durable_queues(lists:zip(DurableQueues, OrderedRecoveryTerms)).


%% 停止所有消息队列进程的接口
stop() ->
	%% 终止rabbit_sup下的rabbit_amqqueue_sup_sup进程
	ok = supervisor:terminate_child(rabbit_sup, rabbit_amqqueue_sup_sup),
	%% 删除rabbit_sup下的rabbit_amqqueue_sup_sup进程
	ok = supervisor:delete_child(rabbit_sup, rabbit_amqqueue_sup_sup),
	{ok, BQ} = application:get_env(rabbit, backing_queue_module),
	ok = BQ:stop().


%% 通知启动的所有队列进程开始的消息(刚开始恢复的持久化队列进程，阻塞等待go的消息)
start(Qs) ->
	%% At this point all recovered queues and their bindings are
	%% visible to routing, so now it is safe for them to complete
	%% their initialisation (which may involve interacting with other
	%% queues).
	[Pid ! {self(), go} || #amqqueue{pid = Pid} <- Qs],
	ok.


%% 查找当前节点所有的持久化队列(找到需要在本节点启动的持久化队列)
find_durable_queues() ->
	Node = node(),
	mnesia:async_dirty(
	  fun () ->
			   qlc:e(qlc:q([Q || Q = #amqqueue{name = Name,
											   pid  = Pid}
												  <- mnesia:table(rabbit_durable_queue),
								 %% 上次消息队列启动的节点和当前节点相同
								 node(Pid) == Node,
								 %% 从rabbit_queue非持久化消息队列数据库表里读出来的数据为空
								 mnesia:read(rabbit_queue, Name, read) =:= []]))
	  end).


%% 恢复持久化队列(RabbitMQ系统启动后需要启动持久化的的队列进程)
recover_durable_queues(QueuesAndRecoveryTerms) ->
	{Results, Failures} =
		%% 将所有的持久化队列进程启动起来，然后同步向他们发送恢复消息
		gen_server2:mcall(
		  [{rabbit_amqqueue_sup_sup:start_queue_process(node(), Q, recovery),
			{init, {self(), Terms}}} || {Q, Terms} <- QueuesAndRecoveryTerms]),
	%% 将启动失败的队列进程打印日志
	[rabbit_log:error("Queue ~p failed to initialise: ~p~n",
					  [Pid, Error]) || {Pid, Error} <- Failures],
	%% 接收到所有启动的持久化队列返回的{new, Q}消息
	[Q || {_, {new, Q}} <- Results].


%% 创建一个新的队列
declare(QueueName, Durable, AutoDelete, Args, Owner) ->
	declare(QueueName, Durable, AutoDelete, Args, Owner, node()).


%% The Node argument suggests(意味着) where the queue (master if mirrored)
%% should be. Note that in some cases (e.g. with "nodes" policy in
%% effect) this might not be possible to satisfy.
%% 在Node节点创建一个新的名字为QueueName的队列进程
declare(QueueName, Durable, AutoDelete, Args, Owner, Node) ->
	%% 检查队列声明时的参数
	ok = check_declare_arguments(QueueName, Args),
	%% 对队列中的参数做初始化
	Q = rabbit_queue_decorator:set(
		  %% 从RabbitMQ系统中公用的策略中拿到当前声明的队列的策略
		  rabbit_policy:set(#amqqueue{name               = QueueName,
									  durable            = Durable,
									  auto_delete        = AutoDelete,
									  arguments          = Args,
									  exclusive_owner    = Owner,
									  pid                = none,
									  slave_pids         = [],
									  sync_slave_pids    = [],
									  recoverable_slaves = [],
									  gm_pids            = [],
									  state              = live})),
	%% 获得消息队列的主镜像队列启动的节点，新启动的消息队列获得主镜像队列和创建时的节点是一样的
	Node = rabbit_mirror_queue_misc:initial_queue_node(Q, Node),
	%% 在rabbit_amqqueue_sup_sup监督进程下启动rabbit_amqqueue_sup监督进程,同时在rabbit_amqqueue_sup监督进程启动rabbit_amqqueue进程(同时向新启动的队列进程发送init的消息)
	gen_server2:call(
	  rabbit_amqqueue_sup_sup:start_queue_process(Node, Q, declare),
	  %% 向新启动的队列进程发送{init, new}，让该进程进行初始化相关操作
	  {init, new}, infinity).


%% 将队列信息写入rabbit_queue表中
%% 此接口是队列已经存在，只是更新队列存活的状态(将队列数据写入rabbit_durable_queue，rabbit_queue表)
internal_declare(Q, true) ->
	rabbit_misc:execute_mnesia_tx_with_tail(
	  fun () ->
			   %% 持久化队列在节点启动后，该持久化队列会随节点的启动而启动，然后调用到此处，重新将队列数据写入rabbit_durable_queue，rabbit_queue表
			   ok = store_queue(Q#amqqueue{state = live}),
			   %% 然后返回队列数据
			   rabbit_misc:const(Q)
	  end);

%% 此处是将新创建的队列信息插入mnesia数据库
internal_declare(Q = #amqqueue{name = QueueName}, false) ->
	rabbit_misc:execute_mnesia_tx_with_tail(
	  fun () ->
			   case mnesia:wread({rabbit_queue, QueueName}) of
				   [] ->
					   case not_found_or_absent(QueueName) of
						   not_found           -> Q1 = rabbit_policy:set(Q),
												  Q2 = Q1#amqqueue{state = live},
												  %% 将amqqueue的数据结构写入mnesia数据库(如果需要持久化的则写入rabbit_durable_queue数据表)
												  ok = store_queue(Q2),
												  %% 增加默认的绑定
												  B = add_default_binding(Q1),
												  %% 新启动的队列数据，再写入mnesia数据库表后，将最新的队列数据返回
												  fun () -> B(), Q1 end;
						   {absent, _Q, _} = R -> rabbit_misc:const(R)
					   end;
				   [ExistingQ] ->
					   rabbit_misc:const(ExistingQ)
			   end
	  end).


%% 更新队列信息的接口
update(Name, Fun) ->
	case mnesia:wread({rabbit_queue, Name}) of
		[Q = #amqqueue{durable = Durable}] ->
			Q1 = Fun(Q),
			ok = mnesia:write(rabbit_queue, Q1, write),
			case Durable of
				true -> ok = mnesia:write(rabbit_durable_queue, Q1, write);
				_    -> ok
			end,
			Q1;
		[] ->
			not_found
	end.


%% 将amqqueue的数据结构写入mnesia数据库(如果需要持久化的则写入rabbit_durable_queue数据表)
store_queue(Q = #amqqueue{durable = true}) ->
	ok = mnesia:write(rabbit_durable_queue,
					  Q#amqqueue{slave_pids      = [],
								 sync_slave_pids = [],
								 gm_pids         = [],
								 decorators      = undefined}, write),
	store_queue_ram(Q);

%% 存储的队列是非持久化的
store_queue(Q = #amqqueue{durable = false}) ->
	store_queue_ram(Q).


%% 同时写入rabbit_queue内存表
store_queue_ram(Q) ->
	ok = mnesia:write(rabbit_queue, rabbit_queue_decorator:set(Q), write).


%% 队列修饰模块的更新
update_decorators(Name) ->
	rabbit_misc:execute_mnesia_transaction(
	  fun() ->
			  case mnesia:wread({rabbit_queue, Name}) of
				  [Q] -> store_queue_ram(Q),
						 ok;
				  []  -> ok
			  end
	  end).


%% 队列全局策略变化的回调
policy_changed(Q1 = #amqqueue{decorators = Decorators1},
			   Q2 = #amqqueue{decorators = Decorators2}) ->
	rabbit_mirror_queue_misc:update_mirrors(Q1, Q2),
	%% 拿到老的队列修饰模块列表
	D1 = rabbit_queue_decorator:select(Decorators1),
	%% 拿到新的队列修饰模块列表
	D2 = rabbit_queue_decorator:select(Decorators2),
	%% 让所有的修饰模块回调policy_changed函数
	[ok = M:policy_changed(Q1, Q2) || M <- lists:usort(D1 ++ D2)],
	%% Make sure we emit a stats event even if nothing
	%% mirroring-related has changed - the policy may have changed anyway.
	%% 通知队列进程全局策略的变化
	notify_policy_changed(Q1).


%% 增加默认的绑定
add_default_binding(#amqqueue{name = QueueName}) ->
	%% 组装exchange交换机资源结构(exchange交换机的名字为<<>>)
	ExchangeName = rabbit_misc:r(QueueName, exchange, <<>>),
	RoutingKey = QueueName#resource.name,
	%% 将队列默认同<<>>名字的exchange交换机进行绑定
	rabbit_binding:add(#binding{source      = ExchangeName,
								destination = QueueName,
								key         = RoutingKey,
								args        = []}).


%% 查找Name信息的队列
lookup([])     -> [];                             %% optimisation

lookup([Name]) -> ets:lookup(rabbit_queue, Name); %% optimisation

lookup(Names) when is_list(Names) ->
	%% Normally we'd call mnesia:dirty_read/1 here, but that is quite
	%% expensive for reasons explained in rabbit_misc:dirty_read/1.
	lists:append([ets:lookup(rabbit_queue, Name) || Name <- Names]);

lookup(Name) ->
	rabbit_misc:dirty_read({rabbit_queue, Name}).


not_found_or_absent(Name) ->
    %% NB: we assume that the caller has already performed a lookup on
    %% rabbit_queue and not found anything
    case mnesia:read({rabbit_durable_queue, Name}) of
        []  -> not_found;
        [Q] -> {absent, Q, nodedown} %% Q exists on stopped node
    end.


%% 当队列不在rabbit_queue表中的时候，来检查role_durable_queue表里是否有该队列的信息
not_found_or_absent_dirty(Name) ->
	%% We should read from both tables inside a tx, to get a
	%% consistent view. But the chances of an inconsistency are small,
	%% and only affect the error kind.
	case rabbit_misc:dirty_read({rabbit_durable_queue, Name}) of
		{error, not_found} -> not_found;
		{ok, Q}            -> {absent, Q, nodedown}
	end.


%% 从mnesia数据库查找是否已经有该队列
with(Name, F, E) ->
	case lookup(Name) of
		%% 查找的队列进程已经崩溃
		{ok, Q = #amqqueue{state = crashed}} ->
			E({absent, Q, crashed});
		%% 成功得到队列的信息
		{ok, Q = #amqqueue{pid = QPid}} ->
			%% We check is_process_alive(QPid) in case we receive a
			%% nodedown (for example) in F() that has nothing to do
			%% with the QPid. F() should be written s.t. that this
			%% cannot happen, so we bail if it does since that
			%% indicates a code bug and we don't want to get stuck in
			%% the retry loop.
			rabbit_misc:with_exit_handler(
			  fun () -> false = rabbit_mnesia:is_process_alive(QPid),
						timer:sleep(25),
						with(Name, F, E)
			  end, fun () -> F(Q) end);
		{error, not_found} ->
			E(not_found_or_absent_dirty(Name))
	end.


%% 对队列名字为Name的队列执行F函数，如果失败则执行fun (E) -> {error, E} end函数
with(Name, F) -> with(Name, F, fun (E) -> {error, E} end).


%% 如果Name的队列不存在，则会让当前进程终止掉，同时将异常消息发送给客户端
with_or_die(Name, F) ->
	with(Name, F, fun (not_found)           -> rabbit_misc:not_found(Name);
					 ({absent, Q, Reason}) -> rabbit_misc:absent(Q, Reason)
		 end).


%% 断言判断处理后的队列信息跟客户端发送过来的信息一致性
assert_equivalence(#amqqueue{name        = QName,
							 durable     = Durable,
							 auto_delete = AD} = Q,
				   Durable1, AD1, Args1, Owner) ->
	rabbit_misc:assert_field_equivalence(Durable, Durable1, QName, durable),
	rabbit_misc:assert_field_equivalence(AD, AD1, QName, auto_delete),
	assert_args_equivalence(Q, Args1),
	check_exclusive_access(Q, Owner, strict).


%% 检查exclusive_owner字段的正确性
check_exclusive_access(Q, Owner) -> check_exclusive_access(Q, Owner, lax).


check_exclusive_access(#amqqueue{exclusive_owner = Owner}, Owner, _MatchType) ->
	ok;

check_exclusive_access(#amqqueue{exclusive_owner = none}, _ReaderPid, lax) ->
	ok;

check_exclusive_access(#amqqueue{name = QueueName}, _ReaderPid, _MatchType) ->
	rabbit_misc:protocol_error(
	  resource_locked,
	  "cannot obtain exclusive access to locked ~s",
	  [rabbit_misc:rs(QueueName)]).


with_exclusive_access_or_die(Name, ReaderPid, F) ->
	with_or_die(Name,
				fun (Q) -> check_exclusive_access(Q, ReaderPid), F(Q) end).


assert_args_equivalence(#amqqueue{name = QueueName, arguments = Args},
						RequiredArgs) ->
	rabbit_misc:assert_args_equivalence(Args, RequiredArgs, QueueName,
										[Key || {Key, _Fun} <- declare_args()]).


%% 检查队列声明时的参数
check_declare_arguments(QueueName, Args) ->
	check_arguments(QueueName, Args, declare_args()).


%% 检查消费者传上来的参数
check_consume_arguments(QueueName, Args) ->
	check_arguments(QueueName, Args, consume_args()).


%% 根据队列参数配置检查客户端传过来的参数是否正确
check_arguments(QueueName, Args, Validators) ->
	[case rabbit_misc:table_lookup(Args, Key) of
		 undefined -> ok;
		 TypeVal   -> case Fun(TypeVal, Args) of
						  ok             -> ok;
						  {error, Error} -> rabbit_misc:protocol_error(
											  precondition_failed,
											  "invalid arg '~s' for ~s: ~255p",
											  [Key, rabbit_misc:rs(QueueName),
											   Error])
					  end
	 end || {Key, Fun} <- Validators],
	ok.


%% 队列声明的默认参数
declare_args() ->
	[
	 %% 控制queue被自动删除前可以处于未使用状态的时间，未使用的意思是queue上没有任何consumer，queue 没有被重新声明，并且在过期时间段内未调用过basic.get命令
	 {<<"x-expires">>,                 fun check_expires_arg/2},
	 %% 控制被publish到queue中的 message 被丢弃前能够存活的时间
	 {<<"x-message-ttl">>,             fun check_message_ttl_arg/2},
	 %% 将死的信息重新发布到该exchange交换机上(死信息包括：1.消息被拒绝（basic.reject or basic.nack）；2.消息TTL过期；3.队列达到最大长度)
	 {<<"x-dead-letter-exchange">>,    fun check_dlxname_arg/2},
	 %% 将死的新重新发布的时候的路由规则
	 {<<"x-dead-letter-routing-key">>, fun check_dlxrk_arg/2},
	 %% 当前队列最大消息数量参数
	 {<<"x-max-length">>,              fun check_non_neg_int_arg/2},
	 %% 当前队列中消息的内容最大上限
	 {<<"x-max-length-bytes">>,        fun check_non_neg_int_arg/2},
	 %% 当前队列的优先级
	 {<<"x-max-priority">>,            fun check_non_neg_int_arg/2}
	].


%% 检查消费者参数的正确性
consume_args() -> [{<<"x-priority">>,              fun check_int_arg/2},
				   {<<"x-cancel-on-ha-failover">>, fun check_bool_arg/2}].


%% 检查传入的整形参数是否是系统支持的参数
check_int_arg({Type, _}, _) ->
	case lists:member(Type, ?INTEGER_ARG_TYPES) of
		true  -> ok;
		false -> {error, {unacceptable_type, Type}}
	end.


%% 检查传入的bool值类型是否正确
check_bool_arg({bool, _}, _) -> ok;

check_bool_arg({Type, _}, _) -> {error, {unacceptable_type, Type}}.


check_non_neg_int_arg({Type, Val}, Args) ->
	%% 检查传入的整形参数是否是系统支持的参数
	case check_int_arg({Type, Val}, Args) of
		ok when Val >= 0 -> ok;
		ok               -> {error, {value_negative, Val}};
		Error            -> Error
	end.


%% 检查队列x-expires参数是否正确
check_expires_arg({Type, Val}, Args) ->
	%% 检查传入的整形参数是否是系统支持的参数
	case check_int_arg({Type, Val}, Args) of
		%% x-expires参数不能为0
		ok when Val == 0 -> {error, {value_zero, Val}};
		%% 检查传入的过期时间不能为负数
		ok               -> rabbit_misc:check_expiry(Val);
		Error            -> Error
	end.


%% 检查消息在队列中存活时间参数的检测
check_message_ttl_arg({Type, Val}, Args) ->
	%% 检查传入的整形参数是否是系统支持的参数
	case check_int_arg({Type, Val}, Args) of
		%% 检查传入的过期时间不能为负数
		ok    -> rabbit_misc:check_expiry(Val);
		Error -> Error
	end.

%% Note that the validity of x-dead-letter-exchange is already verified
%% by rabbit_channel's queue.declare handler.
check_dlxname_arg({longstr, _}, _) -> ok;

check_dlxname_arg({Type,    _}, _) -> {error, {unacceptable_type, Type}}.


check_dlxrk_arg({longstr, _}, Args) ->
	case rabbit_misc:table_lookup(Args, <<"x-dead-letter-exchange">>) of
		undefined -> {error, routing_key_but_no_dlx_defined};
		_         -> ok
	end;

check_dlxrk_arg({Type,    _}, _Args) ->
	{error, {unacceptable_type, Type}}.


%% 列出当前所有的队列信息
list() -> mnesia:dirty_match_object(rabbit_queue, #amqqueue{_ = '_'}).


%% 列出VHostPath下的所有队列
list(VHostPath) -> list(VHostPath, rabbit_queue).

%% Not dirty_match_object since that would not be transactional when used in a
%% tx context
list(VHostPath, TableName) ->
	mnesia:async_dirty(
	  fun () ->
			   mnesia:match_object(
				 TableName,
				 #amqqueue{name = rabbit_misc:r(VHostPath, queue), _ = '_'},
				 read)
	  end).


%% 列出是持久化队列，但是该队列启动的队列(即列出是持久化队列，但是该队列信息没有存在rabbit_queue表中)
list_down(VHostPath) ->
	Present = list(VHostPath),
	Durable = list(VHostPath, rabbit_durable_queue),
	PresentS = sets:from_list([N || #amqqueue{name = N} <- Present]),
	sets:to_list(sets:filter(fun (#amqqueue{name = N}) ->
									  not sets:is_element(N, PresentS)
							 end, sets:from_list(Durable))).


%% 列出队列宏定义里关键key对应的信息
info_keys() -> rabbit_amqqueue_process:info_keys().


%% 对Qs列表执行map操作
map(Qs, F) -> rabbit_misc:filter_exit_map(F, Qs).


%% 获取队列的信息，如果状态是crashed，则info_down接口
info(Q = #amqqueue{ state = crashed }) -> info_down(Q, crashed);

%% 获取队列的信息，如果状态不是crashed，则直接同步call队列进程，让队列进程返回信息列表
info(#amqqueue{ pid = QPid }) -> delegate:call(QPid, info).


%% 获取队列Items列表中key对应的信息，如果状态是crashed，则info_down接口
info(Q = #amqqueue{ state = crashed }, Items) ->
	info_down(Q, Items, crashed);

%% 获取队列Items列表中key对应的信息，如果状态不是crashed，则直接同步call队列进程，让队列进程返回信息列表
info(#amqqueue{ pid = QPid }, Items) ->
	case delegate:call(QPid, {info, Items}) of
		{ok, Res}      -> Res;
		{error, Error} -> throw(Error)
	end.


%% 获取队列Q的信息，关键key通过rabbit_amqqueue_process:info_keys()获取
info_down(Q, DownReason) ->
	info_down(Q, rabbit_amqqueue_process:info_keys(), DownReason).


%% 获取队列Q Items列表中key对应的信息
info_down(Q, Items, DownReason) ->
	[{Item, i_down(Item, Q, DownReason)} || Item <- Items].


%% 获取队列的名字
i_down(name,               #amqqueue{name               = Name}, _) -> Name;

%% 获取队列是否是持久化队列
i_down(durable,            #amqqueue{durable            = Dur},  _) -> Dur;

%% 获取队列是否是自动删除
i_down(auto_delete,        #amqqueue{auto_delete        = AD},   _) -> AD;

%% 获取队列的参数列表
i_down(arguments,          #amqqueue{arguments          = Args}, _) -> Args;

%% 获取队列进程的Pid
i_down(pid,                #amqqueue{pid                = QPid}, _) -> QPid;

%% 获取队列recoverable_slaves字段值(字段含义待查)
i_down(recoverable_slaves, #amqqueue{recoverable_slaves = RS},   _) -> RS;

i_down(state, _Q, DownReason)                                     -> DownReason;

i_down(K, _Q, _DownReason) ->
	case lists:member(K, rabbit_amqqueue_process:info_keys()) of
		true  -> '';
		false -> throw({bad_argument, K})
	end.


info_all(VHostPath) ->
	map(list(VHostPath), fun (Q) -> info(Q) end) ++
		map(list_down(VHostPath), fun (Q) -> info_down(Q, down) end).


%% 列出VHostPath路径下Items中所有key对应队列信息
info_all(VHostPath, Items) ->
	%% 列出当前已经启动的队列信息
	map(list(VHostPath), fun (Q) -> info(Q, Items) end) ++
		%% 列出没有启动的队列信息
		map(list_down(VHostPath), fun (Q) -> info_down(Q, Items, down) end).


%% 异步处理强制时间刷新的消息(直接忽略掉配置文件中配置的信息发布限制，将队列的创建信息，当前队列的所有消费者信息都发布到rabbit_event事件中心去)
force_event_refresh(Ref) ->
	[gen_server2:cast(Q#amqqueue.pid,
					  {force_event_refresh, Ref}) || Q <- list()],
	ok.


%% 通知已经启动的队列进程队列的策略已经改变
notify_policy_changed(#amqqueue{pid = QPid}) ->
	gen_server2:cast(QPid, policy_changed).


%% 拿到QPid对应队列进程中所有的消费者(包括已经阻塞的消费者)
consumers(#amqqueue{ pid = QPid }) -> delegate:call(QPid, consumers).


%% 得到队列中消费者的关键信息key
consumer_info_keys() -> ?CONSUMER_INFO_KEYS.


%% 拿到当前启动的队列下所有队列里的所有消费者关键信息
consumers_all(VHostPath) ->
	ConsumerInfoKeys = consumer_info_keys(),
	lists:append(
	  map(list(VHostPath),
		  fun (Q) ->
				   [lists:zip(
					  ConsumerInfoKeys,
					  [Q#amqqueue.name, ChPid, CTag, AckRequired, Prefetch, Args]) ||
					  {ChPid, CTag, AckRequired, Prefetch, Args} <- consumers(Q)]
		  end)).


%% 拿到当前队列中消息的数量以及消费者的数量
stat(#amqqueue{pid = QPid}) -> delegate:call(QPid, stat).


%% 立即删除队列进程列表QPids
delete_immediately(QPids) ->
	[gen_server2:cast(QPid, delete_immediately) || QPid <- QPids],
	ok.


%% 删除队列的接口
delete(#amqqueue{ pid = QPid }, IfUnused, IfEmpty) ->
	delegate:call(QPid, {delete, IfUnused, IfEmpty}).


%% 删除已经崩溃的队列，将该队列的残留信息删除(比如残留的持久化信息)，当前队列进程已经停止运行，但是该队列进程的Pid可以得到该队列曾今启动的节点
delete_crashed(#amqqueue{ pid = QPid } = Q) ->
	ok = rpc:call(node(QPid), ?MODULE, delete_crashed_internal, [Q]).


%% 删除已经崩溃的队列，将该队列的残留信息删除(比如残留的持久化信息)
delete_crashed_internal(Q = #amqqueue{ name = QName }) ->
	%% 拿到backing_queue_module对应的模块名字
	{ok, BQ} = application:get_env(rabbit, backing_queue_module),
	%% 删除消息队列名字为Name对应的目录下面所有的消息索引磁盘文件
	BQ:delete_crashed(Q),
	%% 内部删除队列信息，同时会将绑定等相关信息删除掉
	ok = internal_delete(QName).


%% 清除QPid队列中的所有消息
purge(#amqqueue{ pid = QPid }) -> delegate:call(QPid, purge).


%% 对QPid队列中的消息进程重新排序
requeue(QPid, MsgIds, ChPid) -> delegate:call(QPid, {requeue, MsgIds, ChPid}).


%% 向QPid队列进程ack MsgIds消息列表
ack(QPid, MsgIds, ChPid) -> delegate:cast(QPid, {ack, MsgIds, ChPid}).


%% 通知队列进程拒绝的消息可以被重新分配
reject(QPid, Requeue, MsgIds, ChPid) ->
	delegate:cast(QPid, {reject, Requeue, MsgIds, ChPid}).


%% 通知列表QPids中的队列进程，ChPid进程down掉
notify_down_all(QPids, ChPid) ->
	%% 通过代理将ChPid挂掉的消息发送到队列进程中
	{_, Bads} = delegate:call(QPids, {notify_down, ChPid}),
	case lists:filter(
		   fun ({_Pid, {exit, {R, _}, _}}) -> rabbit_misc:is_abnormal_exit(R);
			  ({_Pid, _})                 -> false
		   end, Bads) of
		[]    -> ok;
		Bads1 -> {error, Bads1}
	end.


%% 通知队列进程rabbit_limiter进程处于激活状态
activate_limit_all(QPids, ChPid) ->
	delegate:cast(QPids, {activate_limit, ChPid}).


credit(#amqqueue{pid = QPid}, ChPid, CTag, Credit, Drain) ->
	delegate:cast(QPid, {credit, ChPid, CTag, Credit, Drain}).


%% 从队列中取消息的接口
basic_get(#amqqueue{pid = QPid}, ChPid, NoAck, LimiterPid) ->
	%% 通过代理进程去QPid队列进程取消息
	delegate:call(QPid, {basic_get, ChPid, NoAck, LimiterPid}).


%% 向队列进程QPid中添加消费者的接口
basic_consume(#amqqueue{pid = QPid, name = QName}, NoAck, ChPid, LimiterPid,
			  LimiterActive, ConsumerPrefetchCount, ConsumerTag,
			  ExclusiveConsume, Args, OkMsg) ->
	%% 检查消费者传上来的参数
	ok = check_consume_arguments(QName, Args),
	delegate:call(QPid, {basic_consume, NoAck, ChPid, LimiterPid, LimiterActive,
						 ConsumerPrefetchCount, ConsumerTag, ExclusiveConsume,
						 Args, OkMsg}).


%% 删除QPid队列中ConsumerTag标识的消费者
basic_cancel(#amqqueue{pid = QPid}, ChPid, ConsumerTag, OkMsg) ->
	delegate:call(QPid, {basic_cancel, ChPid, ConsumerTag, OkMsg}).


notify_decorators(#amqqueue{pid = QPid}) ->
	delegate:cast(QPid, notify_decorators).


%% rabbit_writer进程和队列进程之间的流量控制，rabbit_writer进程向客户端发完50个消息后，会通知队列进程ChPid对应的ch结构中的字段unsent_message_count加50
%% 如果队列进程中的ChPid对应的ch结构中的字段unsent_message_count减少为1后，则不能够继续向客户端发送消息，该ChPid对应的ch结构处于锁住状态
notify_sent(QPid, ChPid) ->
	Key = {consumer_credit_to, QPid},
	put(Key, case get(Key) of
				 %% 当QPid对应的50个消息发送完毕后，立刻通知队列进程增加50个消息数量
				 1         -> gen_server2:cast(
								QPid, {notify_sent, ChPid,
									   ?MORE_CONSUMER_CREDIT_AFTER}),
							  ?MORE_CONSUMER_CREDIT_AFTER;
				 undefined -> erlang:monitor(process, QPid),
							  ?MORE_CONSUMER_CREDIT_AFTER - 1;
				 C         -> C - 1
			 end),
	ok.


notify_sent_queue_down(QPid) ->
	erase({consumer_credit_to, QPid}),
	ok.


%% rabbit_limiter进程通知队列进程重新开始，可以向消费者发送消息
resume(QPid, ChPid) -> delegate:cast(QPid, {resume, ChPid}).


%% 将队列名字为QueueName的队列全部从mnesia数据库中删除掉
internal_delete1(QueueName, OnlyDurable) ->
	%% 将QueueName队列从rabbit_queue表中删除
	ok = mnesia:delete({rabbit_queue, QueueName}),
	%% this 'guarded' delete prevents unnecessary writes to the mnesia
	%% disk log
	case mnesia:wread({rabbit_durable_queue, QueueName}) of
		[]  -> ok;
		%% 将rabbit_durable_queue队列从rabbit_durable_queue表中删除掉
		[_] -> ok = mnesia:delete({rabbit_durable_queue, QueueName})
	end,
	%% we want to execute some things, as decided by rabbit_exchange,
	%% after the transaction.
	%% 将队列名字为QueueName的队列的绑定信息全部删除掉
	rabbit_binding:remove_for_destination(QueueName, OnlyDurable).


%% 内部的队列删除操作接口
internal_delete(QueueName) ->
	rabbit_misc:execute_mnesia_tx_with_tail(
	  fun () ->
			   %% 先根据队列名字查找rabbit_queue和rabbit_durable_queue两个表
			   case {mnesia:wread({rabbit_queue, QueueName}),
					 mnesia:wread({rabbit_durable_queue, QueueName})} of
				   {[], []} ->
					   rabbit_misc:const({error, not_found});
				   _ ->
					   %% 将队列名字为QueueName的队列全部从mnesia数据库中删除掉
					   Deletions = internal_delete1(QueueName, false),
					   %% 处理删除信息
					   T = rabbit_binding:process_deletions(Deletions),
					   fun() ->
							   ok = T(),
							   %% 将队列删除的信息发布到rabbit_event事件中心去
							   ok = rabbit_event:notify(queue_deleted,
														[{name, QueueName}])
					   end
			   end
	  end).


%% 将Node节点上所有的持久化队列全部删除掉
forget_all_durable(Node) ->
	%% Note rabbit is not running so we avoid e.g. the worker pool. Also why
	%% we don't invoke the return from rabbit_binding:process_deletions/1.
	{atomic, ok} =
		mnesia:sync_transaction(
		  fun () ->
				   Qs = mnesia:match_object(rabbit_durable_queue,
											#amqqueue{_ = '_'}, write),
				   [forget_node_for_queue(Node, Q) ||
					  #amqqueue{pid = Pid} = Q <- Qs,
					  node(Pid) =:= Node],
				   ok
		  end),
	ok.

%% Try to promote a slave while down - it should recover as a
%% master. We try to take the oldest slave here for best chance of
%% recovery.
forget_node_for_queue(DeadNode, Q = #amqqueue{recoverable_slaves = RS}) ->
	forget_node_for_queue(DeadNode, RS, Q).


forget_node_for_queue(_DeadNode, [], #amqqueue{name = Name}) ->
	%% No slaves to recover from, queue is gone.
	%% Don't process_deletions since that just calls callbacks and we
	%% are not really up.
	%% 内部删除掉Name名字的队列的所有信息
	internal_delete1(Name, true);

%% Should not happen, but let's be conservative.
forget_node_for_queue(DeadNode, [DeadNode | T], Q) ->
	forget_node_for_queue(DeadNode, T, Q);

forget_node_for_queue(DeadNode, [H|T], Q) ->
	case node_permits_offline_promotion(H) of
		false -> forget_node_for_queue(DeadNode, T, Q);
		true  -> Q1 = Q#amqqueue{pid = rabbit_misc:node_to_fake_pid(H)},
				 ok = mnesia:write(rabbit_durable_queue, Q1, write)
	end.


node_permits_offline_promotion(Node) ->
	case node() of
		Node -> not rabbit:is_running(); %% [1]
		_    -> Running = rabbit_mnesia:cluster_nodes(running),
				not lists:member(Node, Running) %% [2]
	end.
%% [1] In this case if we are a real running node (i.e. rabbitmqctl
%% has RPCed into us) then we cannot allow promotion. If on the other
%% hand we *are* rabbitmqctl impersonating the node for offline
%% node-forgetting then we can.
%%
%% [2] This is simpler; as long as it's down that's OK
%% 让消息队列的backing_queue执行Fun函数
run_backing_queue(QPid, Mod, Fun) ->
	gen_server2:cast(QPid, {run_backing_queue, Mod, Fun}).


%% rabbit_memory_monitor进程通知消息队列最新的内存持续时间
set_ram_duration_target(QPid, Duration) ->
	gen_server2:cast(QPid, {set_ram_duration_target, Duration}).


%% 将当前进程客户端中文件句柄打开时间超过Age的句柄软关闭
set_maximum_since_use(QPid, Age) ->
	gen_server2:cast(QPid, {set_maximum_since_use, Age}).


%% 停止QPid这个镜像队列
start_mirroring(QPid) -> ok = delegate:cast(QPid, start_mirroring).


%% 开始QPid这个镜像队列
stop_mirroring(QPid)  -> ok = delegate:cast(QPid, stop_mirroring).


%% 队列QPid队列进行镜像队列的同步
sync_mirrors(QPid)        -> delegate:call(QPid, sync_mirrors).


%% 队列QPid队列取消镜像队列的同步
cancel_sync_mirrors(QPid) -> delegate:call(QPid, cancel_sync_mirrors).


%% 此处是rabbit_node_monitor模块回调过来监视的其他集群Node节点停止
on_node_up(Node) ->
	ok = rabbit_misc:execute_mnesia_transaction(
		   fun () ->
					%% 拿到所有的队列信息
					Qs = mnesia:match_object(rabbit_queue,
											 #amqqueue{_ = '_'}, write),
					%% 更新队列数据结构中的recoverable_slaves字段
					[case lists:member(Node, RSs) of
						 true  -> RSs1 = RSs -- [Node],
								  store_queue(
									Q#amqqueue{recoverable_slaves = RSs1});
						 false -> ok
					 end || #amqqueue{recoverable_slaves = RSs} = Q <- Qs],
					ok
		   end).


%% 节点正常退出或者节点挂掉回调做相关的处理(rabbit_node_monitor回调过来是通知集群中的其他节点Node节点已经停止，rabbit回调回来是本节点的回调，做后续的数据清除)
on_node_down(Node) ->
	rabbit_misc:execute_mnesia_tx_with_tail(
	  fun () -> QsDels =
					%% 拿到在Node节点上的队列数据，且该队列进程没有启动
					qlc:e(qlc:q([{QName, delete_queue(QName)} ||
								 #amqqueue{name = QName, pid = Pid,
										   slave_pids = []}
											  <- mnesia:table(rabbit_queue),
								 node(Pid) == Node andalso
									 not rabbit_mnesia:is_process_alive(Pid)])),
				{Qs, Dels} = lists:unzip(QsDels),
				%% 处理最后的绑定信息的删除结果(有可能包括exchange交换机，绑定信息的删除)
				T = rabbit_binding:process_deletions(
					  %% 合并所有的删除信息
					  lists:foldl(fun rabbit_binding:combine_deletions/2,
								  rabbit_binding:new_deletions(), Dels)),
				%% 发布队列被删除的事件
				fun () ->
						 %% 最终的执行绑定信息的删除处理
						 T(),
						 %% 向rabbit_event事件中心发布队列被删除的事件
						 lists:foreach(
						   fun(QName) ->
								   ok = rabbit_event:notify(queue_deleted,
															[{name, QName}])
						   end, Qs)
				end
	  end).


%% 将QueueName的队列信息从rabbit_queue mnesia数据库表中删除
delete_queue(QueueName) ->
	%% 将rabbit_queue表中的数据删除掉
	ok = mnesia:delete({rabbit_queue, QueueName}),
	%% 删除QueueName非持久化队列名字对应的路由信息
	rabbit_binding:remove_transient_for_destination(QueueName).


%% 不相干模块调用
pseudo_queue(QueueName, Pid) ->
	#amqqueue{name         = QueueName,
			  durable      = false,
			  auto_delete  = false,
			  arguments    = [],
			  pid          = Pid,
			  slave_pids   = []}.


%% 不相干模块调用
immutable(Q) -> Q#amqqueue{pid                = none,
						   slave_pids         = none,
						   sync_slave_pids    = none,
						   recoverable_slaves = none,
						   gm_pids            = none,
						   policy             = none,
						   decorators         = none,
						   state              = none}.


%% 向消息队列进程传递消息
deliver([], _Delivery) ->
	%% /dev/null optimisation
	[];

%% 向Qs队列发送消息
deliver(Qs, Delivery = #delivery{flow = Flow}) ->
	{MPids, SPids} = qpids(Qs),
	QPids = MPids ++ SPids,
	%% We use up two credits to send to a slave since the message
	%% arrives at the slave from two directions. We will ack one when
	%% the slave receives the message direct from the channel, and the
	%% other when it receives it via GM.
	%% 进程间消息流的控制
	case Flow of
		flow   -> [credit_flow:send(QPid) || QPid <- QPids],
				  [credit_flow:send(QPid) || QPid <- SPids];
		noflow -> ok
	end,
	
	%% We let slaves know that they were being addressed as slaves at
	%% the time - if they receive such a message from the channel
	%% after they have become master they should mark the message as
	%% 'delivered' since they do not know what the master may have
	%% done with it.
	MMsg = {deliver, Delivery, false},
	SMsg = {deliver, Delivery, true},
	%% 如果有队列进程Pid在远程节点，则通过远程节点的代理进程统一在远程节点上向自己节点上的队列进程发送对应的消息
	delegate:cast(MPids, MMsg),
	delegate:cast(SPids, SMsg),
	QPids.


%% 根据amqqueue数据结构列表得到所有队列的进程Pid列表
qpids([]) -> {[], []}; %% optimisation

%% 路由到一个消息队列的情况
qpids([#amqqueue{pid = QPid, slave_pids = SPids}]) -> {[QPid], SPids}; %% opt

%% 路由到多个队列的情况
qpids(Qs) ->
	{MPids, SPids} = lists:foldl(fun (#amqqueue{pid = QPid, slave_pids = SPids},
									  {MPidAcc, SPidAcc}) ->
										  {[QPid | MPidAcc], [SPids | SPidAcc]}
								 end, {[], []}, Qs),
	{MPids, lists:append(SPids)}.
