%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_priority_queue).

-include_lib("rabbit.hrl").
-include_lib("rabbit_framing.hrl").
-behaviour(rabbit_backing_queue).

%% enabled unconditionally. Disabling priority queueing after
%% it has been enabled is dangerous.
-rabbit_boot_step({?MODULE,
                   [{description, "enable priority queue"},
                    {mfa,         {?MODULE, enable, []}},
                    {requires,    pre_boot},
                    {enables,     kernel_ready}]}).

-export([enable/0]).

-export([start/1, stop/0]).

-export([init/3, terminate/2, delete_and_terminate/2, delete_crashed/1,
         purge/1, purge_acks/1,
         publish/6, publish_delivered/5, discard/4, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/2,
         ackfold/4, fold/3, len/1, is_empty/1, depth/1,
         set_ram_duration_target/2, ram_duration/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         info/2, invoke/3, is_duplicate/2]).

-record(state, {bq, bqss}).
-record(passthrough, {bq, bqs}).

%% See 'note on suffixes' below
-define(passthrough1(F), State#passthrough{bqs = BQ:F}).

-define(passthrough2(F),
        {Res, BQS1} = BQ:F, {Res, State#passthrough{bqs = BQS1}}).

-define(passthrough3(F),
        {Res1, Res2, BQS1} = BQ:F, {Res1, Res2, State#passthrough{bqs = BQS1}}).

%% This module adds suport for priority queues.
%%
%% Priority queues have one backing queue per priority. Backing queue functions
%% then produce a list of results for each BQ and fold over them, sorting
%% by priority.
%%
%% For queues that do not
%% have priorities enabled, the functions in this module delegate to
%% their "regular" backing queue module counterparts. See the `passthrough`
%% record and passthrough{1,2,3} macros.
%%
%% Delivery to consumers happens by first "running" the queue with
%% the highest priority until there are no more messages to deliver,
%% then the next one, and so on. This offers good prioritisation
%% but may result in lower priority messages not being delivered
%% when there's a high ingress rate of messages with higher priority.

%% 在RabbitMQ rabbit_priority_queue启动步骤中启动执行(将backing_queue_module配置参数改为rabbit_priority_queue)
enable() ->
	%% RealBQ = rabbit_variable_queue
	{ok, RealBQ} = application:get_env(rabbit, backing_queue_module),
	case RealBQ of
		?MODULE -> ok;
		%% 先打印优先级队列实际的模块是RabbitMQ系统启动时配置文件中配置的参数
		_       -> rabbit_log:info("Priority queues enabled, real BQ is ~s~n",
								   [RealBQ]),
				   %% 设置rabbitmq_priority_queue应用中的backing_queue_module对应的模块为RealBQ
				   application:set_env(
					 rabbitmq_priority_queue, backing_queue_module, RealBQ),
				   %% 将rabbit应用中的backing_queue_module对应的模块改为rabbit_priority_queue
				   application:set_env(rabbit, backing_queue_module, ?MODULE)
	end.

%%----------------------------------------------------------------------------
%% RabbitMQ系统恢复步骤的时候，会调用过来，QNames表示持久化队列名字列表
start(QNames) ->
	BQ = bq(),
	%% TODO this expand-collapse dance is a bit ridiculous(可笑的) but it's what
	%% rabbit_amqqueue:recover/0 expects. We could probably simplify
	%% this if we rejigged(修正) recovery a bit.
	%% 如果消息队列配置有优先级，则在此处的队列恢复需要将优先级打开单独恢复
	{DupNames, ExpNames} = expand_queues(QNames),
	case BQ:start(ExpNames) of
		{ok, ExpRecovery} ->
			{ok, collapse_recovery(QNames, DupNames, ExpRecovery)};
		Else ->
			Else
	end.


stop() ->
	BQ = bq(),
	BQ:stop().

%%----------------------------------------------------------------------------
%% 将队列名字变异，将优先级数字放入到队列名字后
mutate_name(P, Q = #amqqueue{name = QName = #resource{name = QNameBin}}) ->
	Q#amqqueue{name = QName#resource{name = mutate_name_bin(P, QNameBin)}}.


mutate_name_bin(P, NameBin) -> <<NameBin/binary, 0, P:8>>.


expand_queues(QNames) ->
	lists:unzip(
	  lists:append([expand_queue(QName) || QName <- QNames])).


%% 扩展队列的特性
expand_queue(QName = #resource{name = QNameBin}) ->
	{ok, Q} = rabbit_misc:dirty_read({rabbit_durable_queue, QName}),
	case priorities(Q) of
		none -> [{QName, QName}];
		Ps   -> [{QName, QName#resource{name = mutate_name_bin(P, QNameBin)}}
				 || P <- Ps]
	end.


collapse_recovery(QNames, DupNames, Recovery) ->
	NameToTerms = lists:foldl(fun({Name, RecTerm}, Dict) ->
									  dict:append(Name, RecTerm, Dict)
							  end, dict:new(), lists:zip(DupNames, Recovery)),
	[dict:fetch(Name, NameToTerms) || Name <- QNames].


%% 拿到当前队列的优先级
priorities(#amqqueue{arguments = Args}) ->
	Ints = [long, short, signedint, byte],
	case rabbit_misc:table_lookup(Args, <<"x-max-priority">>) of
		{Type, Max} -> case lists:member(Type, Ints) of
						   false -> none;
						   true  -> lists:reverse(lists:seq(0, Max))
					   end;
		_           -> none
	end.

%%----------------------------------------------------------------------------
%% 优先级队列的初始化
init(Q, Recover, AsyncCallback) ->
	%% 拿到rabbitmq_priority_queue应用backing_queue_module对应的参数(RabbitMQ系统启动的时候将它设置为rabbit_variable_queue)
	BQ = bq(),
	case priorities(Q) of
		none -> RealRecover = case Recover of
								  [R] -> R; %% [0]
								  R   -> R
							  end,
				#passthrough{bq  = BQ,
							 bqs = BQ:init(Q, RealRecover, AsyncCallback)};
		Ps   -> Init = fun (P, Term) ->
								BQ:init(
								  mutate_name(P, Q), Term,
								  fun (M, F) -> AsyncCallback(M, {P, F}) end)
					   end,
				BQSs = case have_recovery_terms(Recover) of
						   false -> [{P, Init(P, Recover)} || P <- Ps];
						   _     -> PsTerms = lists:zip(Ps, Recover),
									[{P, Init(P, Term)} || {P, Term} <- PsTerms]
					   end,
				#state{bq   = BQ,
					   bqss = BQSs}
	end.
%% [0] collapse_recovery has the effect of making a list of recovery
%% terms in priority order, even for non priority queues. It's easier
%% to do that and "unwrap" in init/3 than to have collapse_recovery be
%% aware of non-priority queues.
%% 判断当前队列的启动有没有恢复信息
have_recovery_terms(new)                -> false;

have_recovery_terms(non_clean_shutdown) -> false;

have_recovery_terms(_)                  -> true.


%% 队列中断接口
terminate(Reason, State = #state{bq = BQ}) ->
	foreach1(fun (_P, BQSN) -> BQ:terminate(Reason, BQSN) end, State);

terminate(Reason, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(terminate(Reason, BQS)).


%% 删除队列同时将队列中断的接口
delete_and_terminate(Reason, State = #state{bq = BQ}) ->
	foreach1(fun (_P, BQSN) ->
					  BQ:delete_and_terminate(Reason, BQSN)
			 end, State);

delete_and_terminate(Reason, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(delete_and_terminate(Reason, BQS)).


%% 队列crash的接口
delete_crashed(Q) ->
	BQ = bq(),
	case priorities(Q) of
		%% 删除消息队列名字为Name对应的目录下面所有的消息索引磁盘文件
		none -> BQ:delete_crashed(Q);
		%% 各个优先级队列删除消息队列名字为Name对应的目录下面所有的消息索引磁盘文件
		Ps   -> [BQ:delete_crashed(mutate_name(P, Q)) || P <- Ps]
	end.


%% 清除队列中的信息数据接口
purge(State = #state{bq = BQ}) ->
	fold_add2(fun (_P, BQSN) -> BQ:purge(BQSN) end, State);

purge(State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(purge(BQS)).

%% 清除队列中ack信息
purge_acks(State = #state{bq = BQ}) ->
	foreach1(fun (_P, BQSN) -> BQ:purge_acks(BQSN) end, State);

purge_acks(State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(purge_acks(BQS)).


%% 向backing_queue发布消息的接口(state表明当前队列有队列优先级)
publish(Msg, MsgProps, IsDelivered, ChPid, Flow, State = #state{bq = BQ}) ->
	pick1(fun (_P, BQSN) ->
				   BQ:publish(Msg, MsgProps, IsDelivered, ChPid, Flow, BQSN)
		  end, Msg, State);

%% 向backing_queue发布消息的接口(passthrough数据结构表明当前队列没有队列优先级)
publish(Msg, MsgProps, IsDelivered, ChPid, Flow,
		State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(publish(Msg, MsgProps, IsDelivered, ChPid, Flow, BQS)).


%% 消息已经发送给客户端，等待消费者进行ack操作，将该消息发布到backing_queue数据结构中的接口(state表明当前队列有队列优先级)
publish_delivered(Msg, MsgProps, ChPid, Flow, State = #state{bq = BQ}) ->
	pick2(fun (P, BQSN) ->
				   {AckTag, BQSN1} = BQ:publish_delivered(
									   Msg, MsgProps, ChPid, Flow, BQSN),
				   {{P, AckTag}, BQSN1}
		  end, Msg, State);

%% 消息已经发送给客户端，等待消费者进行ack操作，将该消息发布到backing_queue数据结构中的接口(passthrough数据结构表明当前队列没有队列优先级)
publish_delivered(Msg, MsgProps, ChPid, Flow,
				  State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(publish_delivered(Msg, MsgProps, ChPid, Flow, BQS)).

%% TODO this is a hack. The BQ api does not give us enough information
%% here - if we had the Msg we could look at its priority and forward
%% to the appropriate sub-BQ. But we don't so we are stuck.
%%
%% But fortunately VQ ignores discard/4, so we can too, *assuming we
%% are talking to VQ*. discard/4 is used by HA, but that's "above" us
%% (if in use) so we don't break that either, just some hypothetical
%% alternate BQ implementation.
%% 消息丢弃的接口
discard(_MsgId, _ChPid, _Flow, State = #state{}) ->
	State;
    %% We should have something a bit like this here:
    %% pick1(fun (_P, BQSN) ->
    %%               BQ:discard(MsgId, ChPid, Flow, BQSN)
    %%       end, Msg, State);

discard(MsgId, ChPid, Flow, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(discard(MsgId, ChPid, Flow, BQS)).


%% 从backing_queue得到已经confirm的消息ID列表
drain_confirmed(State = #state{bq = BQ}) ->
	fold_append2(fun (_P, BQSN) -> BQ:drain_confirmed(BQSN) end, State);

drain_confirmed(State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(drain_confirmed(BQS)).


dropwhile(Pred, State = #state{bq = BQ}) ->
	find2(fun (_P, BQSN) -> BQ:dropwhile(Pred, BQSN) end, undefined, State);

dropwhile(Pred, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(dropwhile(Pred, BQS)).

%% TODO this is a bit nasty. In the one place where fetchwhile/4 is
%% actually used the accumulator is a list of acktags, which of course
%% we need to mutate - so we do that although we are encoding an
%% assumption here.
fetchwhile(Pred, Fun, Acc, State = #state{bq = BQ}) ->
	findfold3(
	  fun (P, BQSN, AccN) ->
			   {Res, AccN1, BQSN1} = BQ:fetchwhile(Pred, Fun, AccN, BQSN),
			   {Res, priority_on_acktags(P, AccN1), BQSN1}
	  end, Acc, undefined, State);

fetchwhile(Pred, Fun, Acc, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough3(fetchwhile(Pred, Fun, Acc, BQS)).


%% 从backing_queue中取一条消息
fetch(AckRequired, State = #state{bq = BQ}) ->
	find2(
	  fun (P, BQSN) ->
			   case BQ:fetch(AckRequired, BQSN) of
				   {empty,            BQSN1} -> {empty, BQSN1};
				   {{Msg, Del, ATag}, BQSN1} -> {{Msg, Del, {P, ATag}}, BQSN1}
			   end
	  end, empty, State);

fetch(AckRequired, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(fetch(AckRequired, BQS)).


%% 将消息队列中头部的消息删除掉
drop(AckRequired, State = #state{bq = BQ}) ->
	find2(fun (P, BQSN) ->
				   case BQ:drop(AckRequired, BQSN) of
					   {empty,           BQSN1} -> {empty, BQSN1};
					   {{MsgId, AckTag}, BQSN1} -> {{MsgId, {P, AckTag}}, BQSN1}
				   end
		  end, empty, State);

drop(AckRequired, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(drop(AckRequired, BQS)).


%% 通知backing_queue处理消费者ack操作
ack(AckTags, State = #state{bq = BQ}) ->
	fold_by_acktags2(fun (AckTagsN, BQSN) ->
							  BQ:ack(AckTagsN, BQSN)
					 end, AckTags, State);

ack(AckTags, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(ack(AckTags, BQS)).


%% 将AckTags列表中的消息重新放入到队列中，就是消费者不能进行ack的操作，将消息重新放入到队列中，然后将队列重新排序
requeue(AckTags, State = #state{bq = BQ}) ->
	fold_by_acktags2(fun (AckTagsN, BQSN) ->
							  BQ:requeue(AckTagsN, BQSN)
					 end, AckTags, State);

requeue(AckTags, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(requeue(AckTags, BQS)).


%% Similar problem to fetchwhile/4
%% 对等待ack的每个消息进行MsgFun函数操作
ackfold(MsgFun, Acc, State = #state{bq = BQ}, AckTags) ->
	AckTagsByPriority = partition_acktags(AckTags),
	fold2(
	  fun (P, BQSN, AccN) ->
			   case orddict:find(P, AckTagsByPriority) of
				   {ok, ATagsN} -> {AccN1, BQSN1} =
									   BQ:ackfold(MsgFun, AccN, BQSN, ATagsN),
								   {priority_on_acktags(P, AccN1), BQSN1};
				   error        -> {AccN, BQSN}
			   end
	  end, Acc, State);

ackfold(MsgFun, Acc, State = #passthrough{bq = BQ, bqs = BQS}, AckTags) ->
	?passthrough2(ackfold(MsgFun, Acc, BQS, AckTags)).


%% 对队列中的所有消息执行foldl操作
fold(Fun, Acc, State = #state{bq = BQ}) ->
	fold2(fun (_P, BQSN, AccN) -> BQ:fold(Fun, AccN, BQSN) end, Acc, State);

fold(Fun, Acc, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(fold(Fun, Acc, BQS)).


%% 得到当前队列中消息的数量
len(#state{bq = BQ, bqss = BQSs}) ->
	add0(fun (_P, BQSN) -> BQ:len(BQSN) end, BQSs);

len(#passthrough{bq = BQ, bqs = BQS}) ->
	BQ:len(BQS).


%% 判断当前队列是否为空
is_empty(#state{bq = BQ, bqss = BQSs}) ->
	all0(fun (_P, BQSN) -> BQ:is_empty(BQSN) end, BQSs);

is_empty(#passthrough{bq = BQ, bqs = BQS}) ->
	BQ:is_empty(BQS).


%% 得到队列中所有的消息长度(包括等待ack的所有消息)
depth(#state{bq = BQ, bqss = BQSs}) ->
	add0(fun (_P, BQSN) -> BQ:depth(BQSN) end, BQSs);

depth(#passthrough{bq = BQ, bqs = BQS}) ->
	BQ:depth(BQS).


%% 设置内存速率接口
set_ram_duration_target(DurationTarget, State = #state{bq = BQ}) ->
	foreach1(fun (_P, BQSN) ->
					  BQ:set_ram_duration_target(DurationTarget, BQSN)
			 end, State);

set_ram_duration_target(DurationTarget,
						State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(set_ram_duration_target(DurationTarget, BQS)).


%% 内存速率相关接口
ram_duration(State = #state{bq = BQ}) ->
	fold_min2(fun (_P, BQSN) -> BQ:ram_duration(BQSN) end, State);

ram_duration(State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(ram_duration(BQS)).


%% 判断是否需要进行同步confirm操作
needs_timeout(#state{bq = BQ, bqss = BQSs}) ->
	fold0(fun (_P, _BQSN, timed) -> timed;
			 (_P, BQSN,  idle)  -> case BQ:needs_timeout(BQSN) of
									   timed -> timed;
									   _     -> idle
								   end;
			 (_P, BQSN,  false) -> BQ:needs_timeout(BQSN)
		  end, false, BQSs);

needs_timeout(#passthrough{bq = BQ, bqs = BQS}) ->
	BQ:needs_timeout(BQS).


%% confirm同步的操作
timeout(State = #state{bq = BQ}) ->
	foreach1(fun (_P, BQSN) -> BQ:timeout(BQSN) end, State);

timeout(State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(timeout(BQS)).


%% gen_server2类型中处理睡眠接口
handle_pre_hibernate(State = #state{bq = BQ}) ->
	foreach1(fun (_P, BQSN) ->
					  BQ:handle_pre_hibernate(BQSN)
			 end, State);

handle_pre_hibernate(State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(handle_pre_hibernate(BQS)).


resume(State = #state{bq = BQ}) ->
	foreach1(fun (_P, BQSN) -> BQ:resume(BQSN) end, State);

resume(State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(resume(BQS)).


%% 得到当前backing_queue中消息进出速率
msg_rates(#state{bq = BQ, bqss = BQSs}) ->
	fold0(fun(_P, BQSN, {InN, OutN}) ->
				  {In, Out} = BQ:msg_rates(BQSN),
				  {InN + In, OutN + Out}
		  end, {0.0, 0.0}, BQSs);

msg_rates(#passthrough{bq = BQ, bqs = BQS}) ->
	BQ:msg_rates(BQS).


%% 拿到队列中的信息
info(backing_queue_status, #state{bq = BQ, bqss = BQSs}) ->
	fold0(fun (P, BQSN, Acc) ->
				   combine_status(P, BQ:info(backing_queue_status, BQSN), Acc)
		  end, nothing, BQSs);

info(Item, #state{bq = BQ, bqss = BQSs}) ->
	fold0(fun (_P, BQSN, Acc) ->
				   Acc + BQ:info(Item, BQSN)
		  end, 0, BQSs);

info(Item, #passthrough{bq = BQ, bqs = BQS}) ->
	BQ:info(Item, BQS).


%% 让backing_queue执行Fun函数
invoke(Mod, {P, Fun}, State = #state{bq = BQ}) ->
	pick1(fun (_P, BQSN) -> BQ:invoke(Mod, Fun, BQSN) end, P, State);

invoke(Mod, Fun, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough1(invoke(Mod, Fun, BQS)).


%% 判断当前消息是否存在于backing_queue中(目前rabbit_variable_queue尚未实现)
is_duplicate(Msg, State = #state{bq = BQ}) ->
	pick2(fun (_P, BQSN) -> BQ:is_duplicate(Msg, BQSN) end, Msg, State);

is_duplicate(Msg, State = #passthrough{bq = BQ, bqs = BQS}) ->
	?passthrough2(is_duplicate(Msg, BQS)).

%%----------------------------------------------------------------------------
%% 拿到rabbitmq_priority_queue应用backing_queue_module对应的参数(RabbitMQ系统启动的时候将它设置为rabbit_variable_queue)
bq() ->
	{ok, RealBQ} = application:get_env(
					 rabbitmq_priority_queue, backing_queue_module),
	RealBQ.

%% Note on suffixes: Many utility functions here have suffixes telling
%% you the arity of the return type of the BQ function they are
%% designed to work with.
%%
%% 0 - BQ function returns a value and does not modify state
%% 1 - BQ function just returns a new state
%% 2 - BQ function returns a 2-tuple of {Result, NewState}
%% 3 - BQ function returns a 3-tuple of {Result1, Result2, NewState}

%% Fold over results
fold0(Fun,  Acc, [{P, BQSN} | Rest]) -> fold0(Fun, Fun(P, BQSN, Acc), Rest);

fold0(_Fun, Acc, [])                 -> Acc.


%% Do all BQs match?
all0(Pred, BQSs) -> fold0(fun (_P, _BQSN, false) -> false;
							 (P,  BQSN,  true)  -> Pred(P, BQSN)
						  end, true, BQSs).


%% Sum results
add0(Fun, BQSs) -> fold0(fun (P, BQSN, Acc) -> Acc + Fun(P, BQSN) end, 0, BQSs).


%% Apply for all states
foreach1(Fun, State = #state{bqss = BQSs}) ->
	a(State#state{bqss = foreach1(Fun, BQSs, [])}).


foreach1(Fun, [{P, BQSN} | Rest], BQSAcc) ->
	BQSN1 = Fun(P, BQSN),
	foreach1(Fun, Rest, [{P, BQSN1} | BQSAcc]);

foreach1(_Fun, [], BQSAcc) ->
	lists:reverse(BQSAcc).


%% For a given thing, just go to its BQ
%% 根据Prioritisable(1.可能是优先级整数；2.可能是消息basic_message，优先级则在消息属性中)拿到对应优先级的backing_queue数据结构
pick1(Fun, Prioritisable, #state{bqss = BQSs} = State) ->
	{P, BQSN} = priority(Prioritisable, BQSs),
	a(State#state{bqss = bq_store(P, Fun(P, BQSN), BQSs)}).


%% Fold over results
%% foldl操作
fold2(Fun, Acc, State = #state{bqss = BQSs}) ->
	{Res, BQSs1} = fold2(Fun, Acc, BQSs, []),
	{Res, a(State#state{bqss = BQSs1})}.


fold2(Fun, Acc, [{P, BQSN} | Rest], BQSAcc) ->
	{Acc1, BQSN1} = Fun(P, BQSN, Acc),
	fold2(Fun, Acc1, Rest, [{P, BQSN1} | BQSAcc]);

fold2(_Fun, Acc, [], BQSAcc) ->
	{Acc, lists:reverse(BQSAcc)}.


%% Fold over results assuming results are lists and we want to append them
fold_append2(Fun, State) ->
	fold2(fun (P, BQSN, Acc) ->
				   {Res, BQSN1} = Fun(P, BQSN),
				   {Res ++ Acc, BQSN1}
		  end, [], State).


%% Fold over results assuming results are numbers and we want to sum them
fold_add2(Fun, State) ->
	fold2(fun (P, BQSN, Acc) ->
				   {Res, BQSN1} = Fun(P, BQSN),
				   {add_maybe_infinity(Res, Acc), BQSN1}
		  end, 0, State).


%% Fold over results assuming results are numbers and we want the minimum
fold_min2(Fun, State) ->
	fold2(fun (P, BQSN, Acc) ->
				   {Res, BQSN1} = Fun(P, BQSN),
				   {erlang:min(Res, Acc), BQSN1}
		  end, infinity, State).


%% Fold over results assuming results are lists and we want to append
%% them, and also that we have some AckTags we want to pass in to each
%% invocation.
fold_by_acktags2(Fun, AckTags, State) ->
	AckTagsByPriority = partition_acktags(AckTags),
	fold_append2(fun (P, BQSN) ->
						  case orddict:find(P, AckTagsByPriority) of
							  {ok, AckTagsN} -> Fun(AckTagsN, BQSN);
							  error          -> {[], BQSN}
						  end
				 end, State).


%% For a given thing, just go to its BQ
pick2(Fun, Prioritisable, #state{bqss = BQSs} = State) ->
	{P, BQSN} = priority(Prioritisable, BQSs),
	{Res, BQSN1} = Fun(P, BQSN),
	{Res, a(State#state{bqss = bq_store(P, BQSN1, BQSs)})}.


%% Run through BQs in priority order until one does not return
%% {NotFound, NewState} or we have gone through them all.
find2(Fun, NotFound, State = #state{bqss = BQSs}) ->
	{Res, BQSs1} = find2(Fun, NotFound, BQSs, []),
	{Res, a(State#state{bqss = BQSs1})}.


find2(Fun, NotFound, [{P, BQSN} | Rest], BQSAcc) ->
	case Fun(P, BQSN) of
		{NotFound, BQSN1} -> find2(Fun, NotFound, Rest, [{P, BQSN1} | BQSAcc]);
		{Res, BQSN1}      -> {Res, lists:reverse([{P, BQSN1} | BQSAcc]) ++ Rest}
	end;

find2(_Fun, NotFound, [], BQSAcc) ->
	{NotFound, lists:reverse(BQSAcc)}.


%% Run through BQs in priority order like find2 but also folding as we go.
findfold3(Fun, Acc, NotFound, State = #state{bqss = BQSs}) ->
	{Res, Acc1, BQSs1} = findfold3(Fun, Acc, NotFound, BQSs, []),
	{Res, Acc1, a(State#state{bqss = BQSs1})}.


findfold3(Fun, Acc, NotFound, [{P, BQSN} | Rest], BQSAcc) ->
	case Fun(P, BQSN, Acc) of
		{NotFound, Acc1, BQSN1} ->
			findfold3(Fun, Acc1, NotFound, Rest, [{P, BQSN1} | BQSAcc]);
		{Res, Acc1, BQSN1} ->
			{Res, Acc1, lists:reverse([{P, BQSN1} | BQSAcc]) ++ Rest}
	end;

findfold3(_Fun, Acc, NotFound, [], BQSAcc) ->
	{NotFound, Acc, lists:reverse(BQSAcc)}.


%% 根据传入的消息优先级得到对应的backing_queue保存数据的数据结构
bq_fetch(P, [])               -> exit({not_found, P});

bq_fetch(P, [{P,  BQSN} | _]) -> BQSN;

bq_fetch(P, [{_, _BQSN} | T]) -> bq_fetch(P, T).


bq_store(P, BQS, BQSs) ->
	[{PN, case PN of
			  P -> BQS;
			  _ -> BQSN
		  end} || {PN, BQSN} <- BQSs].


%%
a(State = #state{bqss = BQSs}) ->
	Ps = [P || {P, _} <- BQSs],
	case lists:reverse(lists:usort(Ps)) of
		Ps -> State;
		_  -> exit({bad_order, Ps})
	end.

%%----------------------------------------------------------------------------
%% 根据传入的消息优先级得到对应的backing_queue保存数据的数据结构
priority(P, BQSs) when is_integer(P) ->
	{P, bq_fetch(P, BQSs)};

%% 如果传入的basic_message数据结构
priority(#basic_message{content = Content}, BQSs) ->
	priority1(rabbit_binary_parser:ensure_content_decoded(Content), BQSs).


priority1(_Content, [{P, BQSN}]) ->
	{P, BQSN};

%% 从消息内容特性中拿到当前队列的优先级
priority1(Content = #content{properties = Props},
		  [{P, BQSN} | Rest]) ->
	#'P_basic'{priority = Priority0} = Props,
	Priority = case Priority0 of
				   undefined                    -> 0;
				   _ when is_integer(Priority0) -> Priority0
			   end,
	%% 只要当前消息的优先级大于等于一个优先级backing_queue的优先级，则将消息存储到该backing_queue中
	case Priority >= P of
		true  -> {P, BQSN};
		false -> priority1(Content, Rest)
	end.


add_maybe_infinity(infinity, _) -> infinity;

add_maybe_infinity(_, infinity) -> infinity;

add_maybe_infinity(A, B)        -> A + B.


partition_acktags(AckTags) -> partition_acktags(AckTags, orddict:new()).


partition_acktags([], Partitioned) ->
	orddict:map(fun (_P, RevAckTags) ->
						 lists:reverse(RevAckTags)
				end, Partitioned);

partition_acktags([{P, AckTag} | Rest], Partitioned) ->
	partition_acktags(Rest, rabbit_misc:orddict_cons(P, AckTag, Partitioned)).


priority_on_acktags(P, AckTags) ->
	[case Tag of
		 _ when is_integer(Tag) -> {P, Tag};
		 _                      -> Tag
	 end || Tag <- AckTags].


combine_status(P, New, nothing) ->
	[{priority_lengths, [{P, proplists:get_value(len, New)}]} | New];

combine_status(P, New, Old) ->
	Combined = [{K, cse(V, proplists:get_value(K, Old))} || {K, V} <- New],
	Lens = [{P, proplists:get_value(len, New)} |
				proplists:get_value(priority_lengths, Old)],
	[{priority_lengths, Lens} | Combined].


cse(infinity, _)            -> infinity;

cse(_, infinity)            -> infinity;

cse(A, B) when is_number(A) -> A + B;

cse({delta, _, _, _}, _)    -> {delta, todo, todo, todo};

cse(A, B)                   -> exit({A, B}).
