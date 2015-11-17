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

-module(rabbit_msg_store_gc).

-behaviour(gen_server2).

-export([start_link/1, combine/3, delete/2, no_readers/2, stop/1]).

-export([set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_cast/3]).

-record(state,
		{
		 pending_no_readers,						%% 在等待文件没有读取者的操作字典
		 on_action,									%% 正在进行中的操作
		 msg_store_state							%% 垃圾回收进程中保存的存储服务器进程中的相关信息，包括ETS等信息
		}).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (rabbit_msg_store:gc_state()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(combine/3 :: (pid(), rabbit_msg_store:file_num(),
                    rabbit_msg_store:file_num()) -> 'ok').
-spec(delete/2 :: (pid(), rabbit_msg_store:file_num()) -> 'ok').
-spec(no_readers/2 :: (pid(), rabbit_msg_store:file_num()) -> 'ok').
-spec(stop/1 :: (pid()) -> 'ok').
-spec(set_maximum_since_use/2 :: (pid(), non_neg_integer()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% rabbit_msg_store_gc进程启动的入口函数(RabbitMQ系统存储服务器进程垃圾回收进程)
start_link(MsgStoreState) ->
	gen_server2:start_link(?MODULE, [MsgStoreState],
						   [{timeout, infinity}]).


%% 合并存储消息的磁盘文件的接口
combine(Server, Source, Destination) ->
	gen_server2:cast(Server, {combine, Source, Destination}).


%% 删除文件操作
delete(Server, File) ->
	gen_server2:cast(Server, {delete, File}).


%% 通知垃圾回收进程File文件已经没有读取者了
no_readers(Server, File) ->
	gen_server2:cast(Server, {no_readers, File}).


%% 垃圾回收进程的停止入口
stop(Server) ->
	gen_server2:call(Server, stop, infinity).


set_maximum_since_use(Pid, Age) ->
	gen_server2:cast(Pid, {set_maximum_since_use, Age}).

%%----------------------------------------------------------------------------
%% rabbit_msg_store_gc进程启动的回调初始化函数
init([MsgStoreState]) ->
	ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
											 [self()]),
	{ok, #state { pending_no_readers = dict:new(),
				  on_action          = [],
				  msg_store_state    = MsgStoreState }, hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


prioritise_cast({set_maximum_since_use, _Age}, _Len, _State) -> 8;
prioritise_cast(_Msg,                          _Len, _State) -> 0.


%% 处理停止垃圾回收进程的消息
handle_call(stop, _From, State) ->
	{stop, normal, ok, State}.


%% 合并存储消息的磁盘文件的接口
handle_cast({combine, Source, Destination}, State) ->
	{noreply, attempt_action(combine, [Source, Destination], State), hibernate};

%% 处理删除File磁盘文件的消息
handle_cast({delete, File}, State) ->
	{noreply, attempt_action(delete, [File], State), hibernate};

%% File文件已经没有读取者，则等待该文件没有读取者的操作就立刻执行
handle_cast({no_readers, File},
			State = #state { pending_no_readers = Pending }) ->
	{noreply, case dict:find(File, Pending) of
				  error ->
					  State;
				  {ok, {Action, Files}} ->
					  Pending1 = dict:erase(File, Pending),
					  attempt_action(
						Action, Files,
						State #state { pending_no_readers = Pending1 })
			  end, hibernate};

handle_cast({set_maximum_since_use, Age}, State) ->
	ok = file_handle_cache:set_maximum_since_use(Age),
	{noreply, State, hibernate}.


handle_info(Info, State) ->
	{stop, {unhandled_info, Info}, State}.


terminate(_Reason, State) ->
	State.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%% 尝试进行垃圾回收操作，如果Files中的磁盘文件没有读取者，则立刻对Files文件执行Action操作
attempt_action(Action, Files,
			   State = #state { pending_no_readers = Pending,
								on_action          = Thunks,
								msg_store_state    = MsgStoreState }) ->
	%% 如果当前操作的文件列表Files都没有读取者，则立刻进行相关的操作，否则放入等待队列，直到所有的文件没有读取者
	case [File || File <- Files,
				  rabbit_msg_store:has_readers(File, MsgStoreState)] of
		%% 如果Files中的磁盘文件都没有读取者，则立刻对Files磁盘文件执行Action操作
		[]         -> State #state {
									on_action = lists:filter(
												  fun (Thunk) -> not Thunk() end,
												  [do_action(Action, Files, MsgStoreState) |
													   Thunks]) };
		%% 如果被操作的磁盘文件中有磁盘文件还有读取者，则将有读取者的文件做key，操作作为value，存储在pending_no_readers字典中
		[File | _] -> Pending1 = dict:store(File, {Action, Files}, Pending),
					  State #state { pending_no_readers = Pending1 }
	end.


%% 根据Action不同，对不用的磁盘文件执行不同的操作
%% 此处是对Source和Destination这个两个磁盘文件进行合并操作
do_action(combine, [Source, Destination], MsgStoreState) ->
	rabbit_msg_store:combine_files(Source, Destination, MsgStoreState);

%% 此处是对File磁盘文件进行删除操作
do_action(delete, [File], MsgStoreState) ->
	rabbit_msg_store:delete_file(File, MsgStoreState).
