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

-module(gatherer).

%% Gatherer is a queue which has producer and consumer processes. Before producers
%% push items to the queue using gatherer:in/2 they need to declare their intent
%% to do so with gatherer:fork/1. When a publisher's work is done, it states so
%% using gatherer:finish/1.
%%
%% Consumers pop messages off queues with gatherer:out/1. If a queue is empty
%% and there are producers that haven't finished working, the caller is blocked
%% until an item is available. If there are no active producers, gatherer:out/1
%% immediately returns 'empty'.
%%
%% This module is primarily used to collect results from asynchronous tasks
%% running in a worker pool, e.g. when recovering bindings or rebuilding
%% message store indices.

-behaviour(gen_server2).

-export([start_link/0, stop/1, fork/1, finish/1, in/2, sync_in/2, out/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(stop/1 :: (pid()) -> 'ok').
-spec(fork/1 :: (pid()) -> 'ok').
-spec(finish/1 :: (pid()) -> 'ok').
-spec(in/2 :: (pid(), any()) -> 'ok').
-spec(sync_in/2 :: (pid(), any()) -> 'ok').
-spec(out/1 :: (pid()) -> {'value', any()} | 'empty').

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

-record(gstate, { forks, values, blocked }).

%%----------------------------------------------------------------------------
%% gatherer进程的启动入口函数
start_link() ->
	gen_server2:start_link(?MODULE, [], [{timeout, infinity}]).


%% 停止gatherer进程的入口函数
stop(Pid) ->
	gen_server2:call(Pid, stop, infinity).


%% fork(将forks的值加一)
fork(Pid) ->
	gen_server2:call(Pid, fork, infinity).


%% finish(将forks的值减一，当该值为0的时候先通知阻塞等待队列中的进程empty的消息，同时直接将阻塞等待队列清空)
finish(Pid) ->
	gen_server2:cast(Pid, finish).


%% 异步插入数据，如果阻塞等待队列为空，则将该值直接放入队列
in(Pid, Value) ->
	gen_server2:cast(Pid, {in, Value}).


%% 同步插入数据，如果阻塞等待队列为空，则将该值直接放入队列，但是调用该接口的进程会一直阻塞，直到有进程来取值，则会通知调用该接口的进程ok，让该进程解除阻塞状态
sync_in(Pid, Value) ->
	gen_server2:call(Pid, {in, Value}, infinity).


%% out(进程来取值)(进程同步取值，当值的队列为空，同时forks的值为0则表示值为空，如果队列为空，同时forks不为0则将该进程放入阻塞等待队列，如果有值，则通知发值的进程ok，解除它的阻塞，然后得到值)
out(Pid) ->
	gen_server2:call(Pid, out, infinity).

%%----------------------------------------------------------------------------
%% 收集进程的回调初始化函数
init([]) ->
	{ok, #gstate { forks = 0, values = queue:new(), blocked = queue:new() },
	 hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


%% 同步处理stop的消息
handle_call(stop, _From, State) ->
	{stop, normal, ok, State};


%% 同步处理fork的消息(将forks的值加一)
handle_call(fork, _From, State = #gstate { forks = Forks }) ->
	{reply, ok, State #gstate { forks = Forks + 1 }, hibernate};


%% 处理同步插入数据的消息
handle_call({in, Value}, From, State) ->
	{noreply, in(Value, From, State), hibernate};


%% 处理同步out的消息
handle_call(out, From, State = #gstate { forks   = Forks,
										 values  = Values,
										 blocked = Blocked }) ->
	case queue:out(Values) of
		{empty, _} when Forks == 0 ->
			{reply, empty, State, hibernate};
		{empty, _} ->
			%% 将该进程加入阻塞等待队列，直到有新的值进入
			{noreply, State #gstate { blocked = queue:in(From, Blocked) },
			 hibernate};
		{{value, {PendingIn, Value}}, NewValues} ->
			%% 通知发送该Value值的进程ok，让该进程解除阻塞状态
			reply(PendingIn, ok),
			%% 通知取值的进程的值Value
			{reply, {value, Value}, State #gstate { values = NewValues },
			 hibernate}
	end;


handle_call(Msg, _From, State) ->
	{stop, {unexpected_call, Msg}, State}.


%% 异步处理finish的消息(将forks的值减一，当该值为0的时候先通知阻塞等待队列中的进程empty的消息，同时直接将阻塞等待队列清空)
handle_cast(finish, State = #gstate { forks = Forks, blocked = Blocked }) ->
	NewForks = Forks - 1,
	NewBlocked = case NewForks of
					 0 -> [gen_server2:reply(From, empty) ||
							 From <- queue:to_list(Blocked)],
						  queue:new();
					 _ -> Blocked
				 end,
	{noreply, State #gstate { forks = NewForks, blocked = NewBlocked },
	 hibernate};


%% 异步处理数据的插入
handle_cast({in, Value}, State) ->
	{noreply, in(Value, undefined, State), hibernate};


handle_cast(Msg, State) ->
	{stop, {unexpected_cast, Msg}, State}.


handle_info(Msg, State) ->
	{stop, {unexpected_info, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


terminate(_Reason, State) ->
	State.

%%----------------------------------------------------------------------------

in(Value, From,  State = #gstate { values = Values, blocked = Blocked }) ->
	case queue:out(Blocked) of
		{empty, _} ->
			%% 如果没有阻塞等待的进程，则先将该消息存在队列里(此时如果From为进程Pid则需要等待有人来取该值，才会通知该Pid成功的消息，在此之前，此Pid会一直阻塞)
			State #gstate { values = queue:in({From, Value}, Values) };
		{{value, PendingOut}, NewBlocked} ->
			reply(From, ok),
			%% 如果有阻塞等待的进程，则将该值发给该阻塞的进程
			gen_server2:reply(PendingOut, {value, Value}),
			State #gstate { blocked = NewBlocked }
	end.


reply(undefined, _Reply) -> ok;
reply(From,       Reply) -> gen_server2:reply(From, Reply).
