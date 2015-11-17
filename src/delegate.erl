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

-module(delegate).

%% delegate(代理) is an alternative(代替) way of doing remote calls. Compared to
%% the rpc module, it reduces inter-node(节点间) communication(通信). For example,
%% if a message is routed to 1,000 queues on node A and needs to be
%% propagated(传播) to nodes B and C, it would be nice to avoid doing 2,000
%% remote casts to queue processes.
%%
%% An important issue(问题) here is preserving order(维持秩序) - we need to make sure
%% that messages from a certain channel to a certain queue take a
%% consistent(一贯) route, to prevent them being reordered. In fact all
%% AMQP-ish things (such as queue declaration(声明) results and basic.get)
%% must take the same route as well, to ensure that clients see causal
%% ordering correctly. Therefore we have a rather generic(通用) mechanism(机制)
%% here rather than just a message-reflector. That's also why we pick
%% the delegate process to use based on a hash of the source pid.
%%
%% When a function is invoked using delegate:invoke/2, delegate:call/2
%% or delegate:cast/2 on a group of pids, the pids are first split
%% into local and remote ones. Remote processes are then grouped by
%% node. The function is then invoked locally and on every node (using
%% gen_server2:multi/4) as many times as there are processes on that
%% node, sequentially(顺序).
%%
%% Errors returned when executing functions on remote nodes are re-raised
%% in the caller.
%%
%% RabbitMQ starts a pool of delegate processes on boot. The size of
%% the pool is configurable(可配置的), the aim(目的) is to make sure we don't have too
%% few delegates and thus limit performance on many-CPU machines.(保证系统有不少代理进程这样能够保证在多CPU上的性能)

-behaviour(gen_server2).

-export([start_link/1, invoke_no_result/2, invoke/2,
         monitor/2, demonitor/1, call/2, cast/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {node, monitors, name}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([monitor_ref/0]).

-type(monitor_ref() :: reference() | {atom(), pid()}).
-type(fun_or_mfa(A) :: fun ((pid()) -> A) | {atom(), atom(), [any()]}).

-spec(start_link/1 ::
        (non_neg_integer()) -> {'ok', pid()} | ignore | {'error', any()}).
-spec(invoke/2 :: ( pid(),  fun_or_mfa(A)) -> A;
                  ([pid()], fun_or_mfa(A)) -> {[{pid(), A}],
                                               [{pid(), term()}]}).
-spec(invoke_no_result/2 :: (pid() | [pid()], fun_or_mfa(any())) -> 'ok').
-spec(monitor/2 :: ('process', pid()) -> monitor_ref()).
-spec(demonitor/1 :: (monitor_ref()) -> 'true').

-spec(call/2 ::
        ( pid(),  any()) -> any();
        ([pid()], any()) -> {[{pid(), any()}], [{pid(), term()}]}).
-spec(cast/2 :: (pid() | [pid()], any()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE,   10000).

%%----------------------------------------------------------------------------
%% delegate工作进程的启动入口函数
start_link(Num) ->
	Name = delegate_name(Num),
	gen_server2:start_link({local, Name}, ?MODULE, [Name], []).


%% 向Pid进程调用函数(如果Pid是本地节点的进程则直接在本节点执行函数)
invoke(Pid, FunOrMFA) when is_pid(Pid) andalso node(Pid) =:= node() ->
	apply1(FunOrMFA, Pid);
%% 如果是单个Pid且是远程节点的进程则将它组装成Pid列表
invoke(Pid, FunOrMFA) when is_pid(Pid) ->
	case invoke([Pid], FunOrMFA) of
		{[{Pid, Result}], []} ->
			Result;
		{[], [{Pid, {Class, Reason, StackTrace}}]} ->
			erlang:raise(Class, Reason, StackTrace)
	end;

invoke([], _FunOrMFA) -> %% optimisation(优化)
	{[], []};
%% 如果只有一个Pid，且该进程为本地进程，则在当前节点直接执行函数
invoke([Pid], FunOrMFA) when node(Pid) =:= node() -> %% optimisation
	case safe_invoke(Pid, FunOrMFA) of
		{ok,    _, Result} -> {[{Pid, Result}], []};
		{error, _, Error}  -> {[], [{Pid, Error}]}
	end;
%% 给定的有多个进程Pid
invoke(Pids, FunOrMFA) when is_list(Pids) ->
	{LocalPids, Grouped} = group_pids_by_node(Pids),
	%% The use of multi_call is only safe because the timeout is
	%% infinity, and thus there is no process spawned in order to do
	%% the sending. Thus calls can't overtake preceding(超越) calls/casts.
	{Replies, BadNodes} =
		case orddict:fetch_keys(Grouped) of
			[]          -> {[], []};
			%% call到远程节点上的一个随机代理进程，代理进程然后在它的本地节点对各个进程发送相关的信息或者执行相关的函数代码
			RemoteNodes -> gen_server2:multi_call(
							 RemoteNodes, delegate(self(), RemoteNodes),
							 {invoke, FunOrMFA, Grouped}, infinity)
		end,
	%% 组装失败节点列表
	BadPids = [{Pid, {exit, {nodedown, BadNode}, []}} ||
			   BadNode <- BadNodes,
			   Pid     <- orddict:fetch(BadNode, Grouped)],
	%% 在本地节点执行相关的函数代码
	ResultsNoNode = lists:append([safe_invoke(LocalPids, FunOrMFA) |
									  [Results || {_Node, Results} <- Replies]]),
	%% 组装成功和失败的进程函数调用结果
	lists:foldl(
	  fun ({ok,    Pid, Result}, {Good, Bad}) -> {[{Pid, Result} | Good], Bad};
		 ({error, Pid, Error},  {Good, Bad}) -> {Good, [{Pid, Error} | Bad]}
	  end, {[], BadPids}, ResultsNoNode).


%% 代理进程异步对进程列表执行操作
invoke_no_result(Pid, FunOrMFA) when is_pid(Pid) andalso node(Pid) =:= node() ->
	safe_invoke(Pid, FunOrMFA), %% we don't care about any error
	ok;
invoke_no_result(Pid, FunOrMFA) when is_pid(Pid) ->
	invoke_no_result([Pid], FunOrMFA);

invoke_no_result([], _FunOrMFA) -> %% optimisation
	ok;
invoke_no_result([Pid], FunOrMFA) when node(Pid) =:= node() -> %% optimisation
	safe_invoke(Pid, FunOrMFA), %% must not die
	ok;
invoke_no_result(Pids, FunOrMFA) when is_list(Pids) ->
	%% 将节点列表分割为本地节点Pid列表和远程节点的Pid字典
	{LocalPids, Grouped} = group_pids_by_node(Pids),
	case orddict:fetch_keys(Grouped) of
		[]          -> ok;
		%% 调用gen_server2的abcast向远程节点列表对应的代理进程进行广播，然后代理进程去找到它本地的节点进程，然后对本地的进程执行相关的函数调用
		RemoteNodes -> gen_server2:abcast(
						 RemoteNodes, delegate(self(), RemoteNodes),
						 {invoke, FunOrMFA, Grouped})
	end,
	safe_invoke(LocalPids, FunOrMFA), %% must not die
	ok.


monitor(process, Pid) when node(Pid) =:= node() ->
	erlang:monitor(process, Pid);
monitor(process, Pid) ->
	Name = delegate(Pid, [node(Pid)]),
	gen_server2:cast(Name, {monitor, self(), Pid}),
	{Name, Pid}.


demonitor(Ref) when is_reference(Ref) ->
	erlang:demonitor(Ref);
demonitor({Name, Pid}) ->
	gen_server2:cast(Name, {demonitor, self(), Pid}).


%% 代理进程同步对相关的进程数组进行相关的操作
call(PidOrPids, Msg) ->
	invoke(PidOrPids, {gen_server2, call, [Msg, infinity]}).


%% 代理进程异步对相关的进程数组进行相关的操作
cast(PidOrPids, Msg) ->
	invoke_no_result(PidOrPids, {gen_server2, cast, [Msg]}).

%%----------------------------------------------------------------------------
%% 将节点列表分割为本地节点Pid列表和远程节点的Pid字典
group_pids_by_node(Pids) ->
	LocalNode = node(),
	lists:foldl(
	  fun (Pid, {Local, Remote}) when node(Pid) =:= LocalNode ->
			   {[Pid | Local], Remote};
		 (Pid, {Local, Remote}) ->
			  {Local,
			   orddict:update(
				 node(Pid), fun (List) -> [Pid | List] end, [Pid], Remote)}
	  end, {[], orddict:new()}, Pids).


%% 根据Hash得到当前delegate进程的名字
delegate_name(Hash) ->
	list_to_atom("delegate_" ++ integer_to_list(Hash)).


%% 得到远程节点的代理进程的名字
delegate(Pid, RemoteNodes) ->
	%% erlang:phash2(Term, Range) 是随机散列返回Range范围类的数字
	case get(delegate) of
		undefined -> Name = delegate_name(
							  erlang:phash2(Pid,
											delegate_sup:count(RemoteNodes))),
					 put(delegate, Name),
					 Name;
		Name      -> Name
	end.


%% 安全的进行函数调用
safe_invoke(Pids, FunOrMFA) when is_list(Pids) ->
	[safe_invoke(Pid, FunOrMFA) || Pid <- Pids];
safe_invoke(Pid, FunOrMFA) when is_pid(Pid) ->
	try
		{ok, Pid, apply1(FunOrMFA, Pid)}
	catch Class:Reason ->
			  {error, Pid, {Class, Reason, erlang:get_stacktrace()}}
	end.


apply1({M, F, A}, Arg) -> apply(M, F, [Arg | A]);
apply1(Fun,       Arg) -> Fun(Arg).

%%----------------------------------------------------------------------------
%% delegate进程启动后回调的初始化函数
init([Name]) ->
	{ok, #state{node = node(), monitors = dict:new(), name = Name}, hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


%% 同步处理调用的消息
handle_call({invoke, FunOrMFA, Grouped}, _From, State = #state{node = Node}) ->
	{reply, safe_invoke(orddict:fetch(Node, Grouped), FunOrMFA), State,
	 hibernate}.


%% 处理监视的消息
handle_cast({monitor, MonitoringPid, Pid},
			State = #state{monitors = Monitors}) ->
	Monitors1 = case dict:find(Pid, Monitors) of
					{ok, {Ref, Pids}} ->
						Pids1 = gb_sets:add_element(MonitoringPid, Pids),
						dict:store(Pid, {Ref, Pids1}, Monitors);
					error ->
						Ref = erlang:monitor(process, Pid),
						Pids = gb_sets:singleton(MonitoringPid),
						dict:store(Pid, {Ref, Pids}, Monitors)
				end,
	{noreply, State#state{monitors = Monitors1}, hibernate};


%% 处理取消监视的消息
handle_cast({demonitor, MonitoringPid, Pid},
			State = #state{monitors = Monitors}) ->
	Monitors1 = case dict:find(Pid, Monitors) of
					{ok, {Ref, Pids}} ->
						Pids1 = gb_sets:del_element(MonitoringPid, Pids),
						case gb_sets:is_empty(Pids1) of
							true  -> erlang:demonitor(Ref),
									 dict:erase(Pid, Monitors);
							false -> dict:store(Pid, {Ref, Pids1}, Monitors)
						end;
					error ->
						Monitors
				end,
	{noreply, State#state{monitors = Monitors1}, hibernate};


%% 异步处理调用的消息
handle_cast({invoke, FunOrMFA, Grouped}, State = #state{node = Node}) ->
	safe_invoke(orddict:fetch(Node, Grouped), FunOrMFA),
	{noreply, State, hibernate}.


%% 监控进程down掉后消息的处理
handle_info({'DOWN', Ref, process, Pid, Info},
			State = #state{monitors = Monitors, name = Name}) ->
	{noreply,
	 case dict:find(Pid, Monitors) of
		 {ok, {Ref, Pids}} ->
			 Msg = {'DOWN', {Name, Pid}, process, Pid, Info},
			 gb_sets:fold(fun (MonitoringPid, _) -> MonitoringPid ! Msg end,
						  none, Pids),
			 State#state{monitors = dict:erase(Pid, Monitors)};
		 error ->
			 State
	 end, hibernate};

handle_info(_Info, State) ->
	{noreply, State, hibernate}.


terminate(_Reason, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
