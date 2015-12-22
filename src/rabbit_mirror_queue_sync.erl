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
%% Copyright (c) 2010-2012 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_sync).

-include("rabbit.hrl").

-export([master_prepare/4, master_go/7, slave/7]).

-define(SYNC_PROGRESS_INTERVAL, 1000000).

%% There are three processes around, the master, the syncer and the
%% slave(s). The syncer is an intermediary, linked to the master in
%% order to make sure we do not mess with the master's credit flow or
%% set of monitors.
%%
%% Interactions
%% ------------
%%
%% '*' indicates repeating messages. All are standard Erlang messages
%% except sync_start which is sent over GM to flush out any other
%% messages that we might have sent that way already. (credit) is the
%% usual credit_flow bump message every so often.
%%
%%               Master             Syncer                 Slave(s)
%% sync_mirrors -> ||                                         ||
%% (from channel)  || -- (spawns) --> ||                      ||
%%                 || --------- sync_start (over GM) -------> ||
%%                 ||                 || <--- sync_ready ---- ||
%%                 ||                 ||         (or)         ||
%%                 ||                 || <--- sync_deny ----- ||
%%                 || <--- ready ---- ||                      ||
%%                 || <--- next* ---- ||                      ||  }
%%                 || ---- msg* ----> ||                      ||  } loop
%%                 ||                 || ---- sync_msg* ----> ||  }
%%                 ||                 || <--- (credit)* ----- ||  }
%%                 || <--- next  ---- ||                      ||
%%                 || ---- done ----> ||                      ||
%%                 ||                 || -- sync_complete --> ||
%%                 ||               (Dies)                    ||

-ifdef(use_specs).

-type(log_fun() :: fun ((string(), [any()]) -> 'ok')).
-type(bq() :: atom()).
-type(bqs() :: any()).
-type(ack() :: any()).
-type(slave_sync_state() :: {[{rabbit_types:msg_id(), ack()}], timer:tref(),
                             bqs()}).

-spec(master_prepare/4 :: (reference(), rabbit_amqqueue:name(),
                               log_fun(), [pid()]) -> pid()).
-spec(master_go/7 :: (pid(), reference(), log_fun(),
                      rabbit_mirror_queue_master:stats_fun(),
                      rabbit_mirror_queue_master:stats_fun(),
                      bq(), bqs()) ->
                          {'already_synced', bqs()} | {'ok', bqs()} |
                          {'shutdown', any(), bqs()} |
                          {'sync_died', any(), bqs()}).
-spec(slave/7 :: (non_neg_integer(), reference(), timer:tref(), pid(),
                  bq(), bqs(), fun((bq(), bqs()) -> {timer:tref(), bqs()})) ->
                      'denied' |
                      {'ok' | 'failed', slave_sync_state()} |
                      {'stop', any(), slave_sync_state()}).

-endif.

%% ---------------------------------------------------------------------------
%% Master
%% 镜像队列之间进行同步的准备
master_prepare(Ref, QName, Log, SPids) ->
	MPid = self(),
	%% 启动镜像队列同步进程
	spawn_link(fun () ->
						?store_proc_name(QName),
						syncer(Ref, Log, MPid, SPids)
			   end).


%% 主镜像队列开始进行同步的接口
master_go(Syncer, Ref, Log, HandleInfo, EmitStats, BQ, BQS) ->
	Args = {Syncer, Ref, Log, HandleInfo, EmitStats, rabbit_misc:get_parent()},
	receive
		{'EXIT', Syncer, normal} -> {already_synced, BQS};
		{'EXIT', Syncer, Reason} -> {sync_died, Reason, BQS};
		%% 收取到同步进程发送过来的同步准备完毕的消息后则立刻开始进行同步操作
		{ready, Syncer}          -> EmitStats({syncing, 0}),
									master_go0(Args, BQ, BQS)
	end.


%% 主镜像队列进程开始进行同步操作
master_go0(Args, BQ, BQS) ->
	case BQ:fold(fun (Msg, MsgProps, Unacked, Acc) ->
						  master_send(Msg, MsgProps, Unacked, Args, Acc)
				 end, {0, erlang:now()}, BQS) of
		{{shutdown,  Reason}, BQS1} -> {shutdown,  Reason, BQS1};
		{{sync_died, Reason}, BQS1} -> {sync_died, Reason, BQS1};
		{_,                   BQS1} -> master_done(Args, BQS1)
	end.


%% 主镜像队列将消息发送出去进行同步的实际接口
master_send(Msg, MsgProps, Unacked,
			{Syncer, Ref, Log, HandleInfo, EmitStats, Parent}, {I, Last}) ->
	T = case timer:now_diff(erlang:now(), Last) > ?SYNC_PROGRESS_INTERVAL of
			%% 当一个消息同步的时间超过一秒，则发布当前队列的信息的事件
			true  -> EmitStats({syncing, I}),
					 %% 然后打印日志当前已经同步过的消息数量
					 Log("~p messages", [I]),
					 erlang:now();
			false -> Last
		end,
	%% 向调用同步的调用者发布当前同步到第几个消息
	HandleInfo({syncing, I}),
	%% 准备接收跟句柄相关的消息
	receive
		{'$gen_cast', {set_maximum_since_use, Age}} ->
			ok = file_handle_cache:set_maximum_since_use(Age)
		after 0 ->
			ok
	end,
	receive
		{'$gen_call', From,
		 cancel_sync_mirrors}    -> stop_syncer(Syncer, {cancel, Ref}),
									gen_server2:reply(From, ok),
									{stop, cancelled};
		%% 当收取到请求下一个消息的请求后则立刻发送下一个消息进行同步
		{next, Ref}              -> Syncer ! {msg, Ref, Msg, MsgProps, Unacked},
									{cont, {I + 1, T}};
		{'EXIT', Parent, Reason} -> {stop, {shutdown,  Reason}};
		{'EXIT', Syncer, Reason} -> {stop, {sync_died, Reason}}
	end.


master_done({Syncer, Ref, _Log, _HandleInfo, _EmitStats, Parent}, BQS) ->
	receive
		{next, Ref}              -> stop_syncer(Syncer, {done, Ref}),
									{ok, BQS};
		{'EXIT', Parent, Reason} -> {shutdown,  Reason, BQS};
		{'EXIT', Syncer, Reason} -> {sync_died, Reason, BQS}
	end.


stop_syncer(Syncer, Msg) ->
	unlink(Syncer),
	Syncer ! Msg,
	receive {'EXIT', Syncer, _} -> ok
		after 0 -> ok
	end.

%% Master
%% ---------------------------------------------------------------------------
%% Syncer
%% 镜像队列进程进行同步的时候启动的同步进程执行的函数
syncer(Ref, Log, MPid, SPids) ->
	[erlang:monitor(process, SPid) || SPid <- SPids],
	%% We wait for a reply from the slaves so that we know they are in
	%% a receive block and will thus receive messages we send to them
	%% *without* those messages ending up in their gen_server2 pqueue.
	%% 等待所有的副镜像队列发送过来的同步准备完毕的消息
	case await_slaves(Ref, SPids) of
		[]     -> Log("all slaves already synced", []);
		SPids1 -> %% 向主镜像队列发送同步准备完毕的消息
				  MPid ! {ready, self()},
				  %% 打印镜像队列同步的日志
				  Log("mirrors ~p to sync", [[node(SPid) || SPid <- SPids1]]),
				  syncer_loop(Ref, MPid, SPids1)
	end.


%% 等待所有的副镜像队列发送过来的同步准备完毕的消息
await_slaves(Ref, SPids) ->
	[SPid || SPid <- SPids,
			 %% 判断SPid对应的节点是否正在运行中
			 rabbit_mnesia:on_running_node(SPid) andalso %% [0]
				 receive
					 {sync_ready, Ref, SPid}       -> true;
					 {sync_deny,  Ref, SPid}       -> false;
					 {'DOWN', _, process, SPid, _} -> false
				 end].
%% [0] This check is in case there's been a partition which has then
%% healed in between the master retrieving the slave pids from Mnesia
%% and sending 'sync_start' over GM. If so there might be slaves on the
%% other side of the partition which we can monitor (since they have
%% rejoined the distributed system with us) but which did not get the
%% 'sync_start' and so will not reply. We need to act as though they are
%% down.
%% 同步镜像循环执行的函数
syncer_loop(Ref, MPid, SPids) ->
	%% 向主镜像队列发送请求下一个消息的消息
	MPid ! {next, Ref},
	receive
		%% 接收到主镜像队列发送的需要同步的消息
		{msg, Ref, Msg, MsgProps, Unacked} ->
			SPids1 = wait_for_credit(SPids),
			[begin
				 credit_flow:send(SPid),
				 %% 向副镜像队列发送同步的消息
				 SPid ! {sync_msg, Ref, Msg, MsgProps, Unacked}
			 end || SPid <- SPids1],
			syncer_loop(Ref, MPid, SPids1);
		{cancel, Ref} ->
			%% We don't tell the slaves we will die - so when we do
			%% they interpret that as a failure, which is what we
			%% want.
			ok;
		{done, Ref} ->
			[SPid ! {sync_complete, Ref} || SPid <- SPids]
	end.


wait_for_credit(SPids) ->
	case credit_flow:blocked() of
		true  -> receive
					 {bump_credit, Msg} ->
						 credit_flow:handle_bump_msg(Msg),
						 wait_for_credit(SPids);
					 {'DOWN', _, process, SPid, _} ->
						 credit_flow:peer_down(SPid),
						 wait_for_credit(lists:delete(SPid, SPids))
				 end;
		false -> SPids
	end.

%% Syncer
%% ---------------------------------------------------------------------------
%% Slave
%% 副镜像队列在同步的时候执行的函数(此处是该副镜像队列已经同步过)
slave(0, Ref, _TRef, Syncer, _BQ, _BQS, _UpdateRamDuration) ->
	Syncer ! {sync_deny, Ref, self()},
	denied;

%% 副镜像队列在同步的时候执行的函数
slave(_DD, Ref, TRef, Syncer, BQ, BQS, UpdateRamDuration) ->
	%% 监视同步进程
	MRef = erlang:monitor(process, Syncer),
	%% 向同步进程发送自己已经准备完毕，可以进行消息的同步
	Syncer ! {sync_ready, Ref, self()},
	%% 先将自己消息队列中的所有消息清除掉，准备全部接受主镜像队列的同步
	{_MsgCount, BQS1} = BQ:purge(BQ:purge_acks(BQS)),
	%% 实际的跟同步进行进行同步操作
	slave_sync_loop({Ref, MRef, Syncer, BQ, UpdateRamDuration,
					 rabbit_misc:get_parent()}, {[], TRef, BQS1}).


%% 实际副镜像队列同步的函数
slave_sync_loop(Args = {Ref, MRef, Syncer, BQ, UpdateRamDuration, Parent},
				State = {MA, TRef, BQS}) ->
	receive
		%% 收到同步进行挂掉的消息
		{'DOWN', MRef, process, Syncer, _Reason} ->
			%% If the master dies half way we are not in the usual
			%% half-synced state (with messages nearer the tail of the
			%% queue); instead we have ones nearer the head. If we then
			%% sync with a newly promoted master, or even just receive
			%% messages from it, we have a hole in the middle. So the
			%% only thing to do here is purge.
			%% 则将当前副镜像队列的消息队列清空
			{_MsgCount, BQS1} = BQ:purge(BQ:purge_acks(BQS)),
			%% 解除对同步进程的消息流控制
			credit_flow:peer_down(Syncer),
			%% 返回同步失败的消息
			{failed, {[], TRef, BQS1}};
		%% 收到控制流相关的消息
		{bump_credit, Msg} ->
			credit_flow:handle_bump_msg(Msg),
			slave_sync_loop(Args, State);
		%% 收到镜像队列同步成功的消息
		{sync_complete, Ref} ->
			%% 解除对同步进程的监视
			erlang:demonitor(MRef, [flush]),
			%% 解除对同步进程的消息流控制
			credit_flow:peer_down(Syncer),
			{ok, State};
		%% 收到将当前进程客户端中文件句柄打开时间超过Age的句柄软关闭的消息
		{'$gen_cast', {set_maximum_since_use, Age}} ->
			ok = file_handle_cache:set_maximum_since_use(Age),
			slave_sync_loop(Args, State);
		%% rabbit_memory_monitor进程通知消息队列最新的内存持续时间
		{'$gen_cast', {set_ram_duration_target, Duration}} ->
			BQS1 = BQ:set_ram_duration_target(Duration, BQS),
			slave_sync_loop(Args, {MA, TRef, BQS1});
		%% 收到让backing_queue模块执行Fun函数的消息
		{'$gen_cast', {run_backing_queue, Mod, Fun}} ->
			BQS1 = BQ:invoke(Mod, Fun, BQS),
			slave_sync_loop(Args, {MA, TRef, BQS1});
		update_ram_duration ->
			{TRef1, BQS1} = UpdateRamDuration(BQ, BQS),
			slave_sync_loop(Args, {MA, TRef1, BQS1});
		%% 收到同步进程发送过来的需要同步的消息
		{sync_msg, Ref, Msg, Props, Unacked} ->
			%% 向同步进程做消息流控制
			credit_flow:ack(Syncer),
			Props1 = Props#message_properties{needs_confirming = false},
			{MA1, BQS1} =
				case Unacked of
					%% 消息不需要进行ack操作
					false -> {MA,
							  BQ:publish(Msg, Props1, true, none, noflow, BQS)};
					%% 消息需要进行ack操作
					true  -> {AckTag, BQS2} = BQ:publish_delivered(
												Msg, Props1, none, noflow, BQS),
							 {[{Msg#basic_message.id, AckTag} | MA], BQS2}
				end,
			slave_sync_loop(Args, {MA1, TRef, BQS1});
		%% 收到父进程挂掉的消息
		{'EXIT', Parent, Reason} ->
			{stop, Reason, State};
		%% If the master throws an exception
		%% 主镜像队列抛出异常
		{'$gen_cast', {gm, {delete_and_terminate, Reason}}} ->
			BQ:delete_and_terminate(Reason, BQS),
			{stop, Reason, {[], TRef, undefined}}
	end.
