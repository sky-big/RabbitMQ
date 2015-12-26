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
%%  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_tracing_traces).

-behaviour(gen_server).

-import(rabbit_misc, [pget/2]).

-export([list/0, lookup/2, create/3, stop/2, announce/3]).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { table }).

%%--------------------------------------------------------------------
%% RabbitMQ系统跟踪插件实际工作进程的启动接口
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% 列出当前所有的跟踪信息
list() ->
	gen_server:call(?MODULE, list, infinity).


%% 查询某一个跟踪信息
lookup(VHost, Name) ->
	gen_server:call(?MODULE, {lookup, VHost, Name}, infinity).


%% 创建跟踪信息
create(VHost, Name, Trace) ->
	gen_server:call(?MODULE, {create, VHost, Name, Trace}, infinity).


%% 删除跟踪信息
stop(VHost, Name) ->
	gen_server:call(?MODULE, {stop, VHost, Name}, infinity).


%% 设置跟踪信息对应的消费进程Pid
announce(VHost, Name, Pid) ->
	gen_server:cast(?MODULE, {announce, {VHost, Name}, Pid}).

%%--------------------------------------------------------------------
%% 跟踪进程的回调初始化接口
init([]) ->
	{ok, #state{table = ets:new(anon, [private])}}.


%% 列出当前所有的跟踪信息
handle_call(list, _From, State = #state{table = Table}) ->
	{reply, [augment(Trace) || {_K, Trace} <- ets:tab2list(Table)], State};

%% 查询某一个跟踪信息
handle_call({lookup, VHost, Name}, _From, State = #state{table = Table}) ->
	{reply, case ets:lookup(Table, {VHost, Name}) of
				[]            -> not_found;
				[{_K, Trace}] -> augment(Trace)
			end, State};

%% 创建跟踪信息
handle_call({create, VHost, Name, Trace0}, _From,
			State = #state{table = Table}) ->
	%% 判断VHost是否已经存在于跟踪信息
	Already = vhost_tracing(VHost, Table),
	Trace = pset(vhost, VHost, pset(name, Name, Trace0)),
	true = ets:insert(Table, {{VHost, Name}, Trace}),
	case Already of
		true  -> ok;
		%% 如果VHost没有开启跟踪，则在此处开启跟踪
		false -> rabbit_trace:start(VHost)
	end,
	%% 启动接收跟踪信息的进程
	{reply, rabbit_tracing_sup:start_child({VHost, Name}, Trace), State};

%% 删除跟踪信息
handle_call({stop, VHost, Name}, _From, State = #state{table = Table}) ->
	%% 将ETS里的跟踪信息删除掉
	true = ets:delete(Table, {VHost, Name}),
	%% 判断VHost是否已经存在于跟踪信息
	case vhost_tracing(VHost, Table) of
		true  -> ok;
		%% 如果没有VHost的跟踪信息，则将该VHost的跟踪功能关闭
		false -> rabbit_trace:stop(VHost)
	end,
	%% 关闭接收跟踪信息的进程
	rabbit_tracing_sup:stop_child({VHost, Name}),
	{reply, ok, State};

handle_call(_Req, _From, State) ->
	{reply, unknown_request, State}.


%% 设置跟踪信息对应的消费进程Pid
handle_cast({announce, Key, Pid}, State = #state{table = Table}) ->
	case ets:lookup(Table, Key) of
		[]           -> ok;
		[{_, Trace}] -> ets:insert(Table, {Key, pset(pid, Pid, Trace)})
	end,
	{noreply, State};

handle_cast(_C, State) ->
	{noreply, State}.


handle_info(_I, State) ->
	{noreply, State}.


terminate(_, _) -> ok.


code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

pset(Key, Value, List) -> [{Key, Value} | proplists:delete(Key, List)].


%% 判断VHost是否已经存在于跟踪信息
vhost_tracing(VHost, Table) ->
	case [true || {{V, _}, _} <- ets:tab2list(Table), V =:= VHost] of
		[] -> false;
		_  -> true
	end.


augment(Trace) ->
	Pid = pget(pid, Trace),
	Trace1 = lists:keydelete(pid, 1, Trace),
	case Pid of
		undefined -> Trace1;
		_         -> rabbit_tracing_consumer:info_all(Pid) ++ Trace1
	end.
