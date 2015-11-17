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

-module(worker_pool_worker).

%% Executes jobs (functions) submitted to a worker pool with worker_pool:submit/1,
%% worker_pool:submit/2 or worker_pool:submit_async/1.
%%
%% See worker_pool for an overview.

-behaviour(gen_server2).

-export([start_link/0, next_job_from/2, submit/3, submit_async/2, run/1]).

-export([set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_cast/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/0 :: () -> {'ok', pid()} | {'error', any()}).
-spec(next_job_from/2 :: (pid(), pid()) -> 'ok').
-spec(submit/3 :: (pid(), fun (() -> A) | mfargs(), 'reuse' | 'single') -> A).
-spec(submit_async/2 :: (pid(), fun (() -> any()) | mfargs()) -> 'ok').
-spec(run/1 :: (fun (() -> A)) -> A; (mfargs()) -> any()).
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------
%% worker_pool_worker pool工作进程启动入口
start_link() ->
	gen_server2:start_link(?MODULE, [], [{timeout, infinity}]).


%% 通知进程池进程有客户端工作过来
next_job_from(Pid, CPid) ->
	gen_server2:cast(Pid, {next_job_from, CPid}).


%% 向进程池进程提交任务
submit(Pid, Fun, ProcessModel) ->
	gen_server2:call(Pid, {submit, Fun, self(), ProcessModel}, infinity).


%% 异步提交任务
submit_async(Pid, Fun) ->
	gen_server2:cast(Pid, {submit_async, Fun}).


set_maximum_since_use(Pid, Age) ->
	gen_server2:cast(Pid, {set_maximum_since_use, Age}).


run({M, F, A}) -> apply(M, F, A);
run(Fun)       -> Fun().


run(Fun, reuse) ->
	run(Fun);
%% single单独启动一个进程来执行任务，然后工作进程阻塞等待任务结果的返回
run(Fun, single) ->
	Self = self(),
	Ref = make_ref(),
	spawn_link(fun () ->
						put(worker_pool_worker, true),
						Self ! {Ref, run(Fun)},
						unlink(Self)
			   end),
	receive
		{Ref, Res} -> Res
	end.

%%----------------------------------------------------------------------------

init([]) ->
	%% 向文件处理模块注册回调函数
	ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
											 [self()]),
	%% 通知管理进程自己当前已经准备好工作
	ok = worker_pool:ready(self()),
	put(worker_pool_worker, true),
	{ok, undefined, hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


prioritise_cast({set_maximum_since_use, _Age}, _Len, _State) -> 8;
prioritise_cast({next_job_from, _CPid},        _Len, _State) -> 7;
prioritise_cast(_Msg,                          _Len, _State) -> 0.


%% 处理客户端提交任务
handle_call({submit, Fun, CPid, ProcessModel}, From, undefined) ->
	{noreply, {job, CPid, From, Fun, ProcessModel}, hibernate};


%% 提交任务，同时客户端的进程的Pid已经提交过来，则直接进行任务的处理
handle_call({submit, Fun, CPid, ProcessModel}, From, {from, CPid, MRef}) ->
	%% 取消对对客户端进程的监视
	erlang:demonitor(MRef),
	%% 向客户端进程发送任务执行的结果
	gen_server2:reply(From, run(Fun, ProcessModel)),
	%% 通知管理进程自己处于待机状态，可以工作
	ok = worker_pool:idle(self()),
	{noreply, undefined, hibernate};


handle_call(Msg, _From, State) ->
	{stop, {unexpected_call, Msg}, State}.


%% 处理客户端任务到来的消息，但是实际执行的任务还没有到达
handle_cast({next_job_from, CPid}, undefined) ->
	%% 则监视客户端检查
	MRef = erlang:monitor(process, CPid),
	{noreply, {from, CPid, MRef}, hibernate};


%% 处理客户端任务到来的消息，但是实际执行的任务已经到达
handle_cast({next_job_from, CPid}, {job, CPid, From, Fun, ProcessModel}) ->
	%% 向客户端进程发送任务执行的结果
	gen_server2:reply(From, run(Fun, ProcessModel)),
	%% 通知管理进程自己处于待机状态，可以工作
	ok = worker_pool:idle(self()),
	{noreply, undefined, hibernate};


%% 异步任务的处理
handle_cast({submit_async, Fun}, undefined) ->
	%% 执行任务
	run(Fun),
	%% 通知管理进程自己处于待机状态，可以工作
	ok = worker_pool:idle(self()),
	{noreply, undefined, hibernate};


handle_cast({set_maximum_since_use, Age}, State) ->
	ok = file_handle_cache:set_maximum_since_use(Age),
	{noreply, State, hibernate};


handle_cast(Msg, State) ->
	{stop, {unexpected_cast, Msg}, State}.


%% 客户端进程挂掉，则通知管理进程自己处于待机状态，可以工作
handle_info({'DOWN', MRef, process, CPid, _Reason}, {from, CPid, MRef}) ->
	ok = worker_pool:idle(self()),
	{noreply, undefined, hibernate};


handle_info({'DOWN', _MRef, process, _Pid, _Reason}, State) ->
	{noreply, State, hibernate};


handle_info(Msg, State) ->
	{stop, {unexpected_info, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


terminate(_Reason, State) ->
	State.
