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

-module(rabbit_alarm).

-behaviour(gen_event).

-export([start_link/0, start/0, stop/0, register/2, set_alarm/1,
         clear_alarm/1, get_alarms/0, on_node_up/1, on_node_down/1]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-export([remote_conserve_resources/3]). %% Internal use only

-define(SERVER, ?MODULE).

-record(alarms, {alertees,				%% 注册警报的进程Pid对应的执行函数字典数据结构
				 alarmed_nodes,			%% 注册警报节点名字对应的警报类型的字典数据结构
				 alarms					%% 所有的报警注册信息({Pid, {M, F, C}}数据机构)
				}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(start/0 :: () -> 'ok').
-spec(stop/0 :: () -> 'ok').
-spec(register/2 :: (pid(), rabbit_types:mfargs()) -> [atom()]).
-spec(set_alarm/1 :: (any()) -> 'ok').
-spec(clear_alarm/1 :: (any()) -> 'ok').
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% rabbit_alarm进程启动的入口(RabbitMQ App应用启动的时候第一个执行的接口)
start_link() ->
	gen_event:start_link({local, ?SERVER}).


start() ->
	%% 启动rabbit_alarm_sup监督进程以及在该监督进程下启动rabbit_alarm进程
	ok = rabbit_sup:start_restartable_child(?MODULE),
	%% 启动rabbit_alarm对应于rabbit_alarm事件服务器进程的事件处理进程
	ok = gen_event:add_handler(?SERVER, ?MODULE, []),
	%% 启动vm_memory_monitor_sup监督进程以及在该监督进程下启动vm_memory_monitor进程(虚拟机内存监控进程)
	{ok, MemoryWatermark} = application:get_env(vm_memory_high_watermark),
	rabbit_sup:start_restartable_child(
	  vm_memory_monitor, [MemoryWatermark,
						  fun (Alarm) ->
								   %% 对当前RabbitMQ系统进行一次垃圾回收
								   background_gc:run(),
								   %% 设置报警器的函数
								   set_alarm(Alarm)
						  end,
						  %% 清除报警器的函数
						  fun clear_alarm/1]),
	%% 启动rabbit_disk_monitor_sup监督进程以及在该监督进程下启动rabbit_disk_monitor进程(硬盘使用监控进程)
	{ok, DiskLimit} = application:get_env(disk_free_limit),
	rabbit_sup:start_delayed_restartable_child(
	  rabbit_disk_monitor, [DiskLimit]),
	ok.


stop() -> ok.


%% 注册接口
register(Pid, AlertMFA) ->
	gen_event:call(?SERVER, ?MODULE, {register, Pid, AlertMFA}, infinity).


%% 向rabbit_alarm进程设置报警器
set_alarm(Alarm)   -> gen_event:notify(?SERVER, {set_alarm,   Alarm}).


%% 向rabbit_alarm进程清除报警器
clear_alarm(Alarm) -> gen_event:notify(?SERVER, {clear_alarm, Alarm}).


%% 从rabbit_alarm进程拿到当前所有的报警器
get_alarms() -> gen_event:call(?SERVER, ?MODULE, get_alarms, infinity).


%% 节点启动的通知接口函数
on_node_up(Node)   -> gen_event:notify(?SERVER, {node_up,   Node}).


%% 节点关闭的通知接口
on_node_down(Node) -> gen_event:notify(?SERVER, {node_down, Node}).


%% conserve：节约
%% 通知Pid对应的远程节点本地节点Source资源已经报警(向Pid对应的远程节点发布set_alarm事件)
remote_conserve_resources(Pid, Source, true) ->
	gen_event:notify({?SERVER, node(Pid)},
					 {set_alarm, {{resource_limit, Source, node()}, []}});

%% 通知Pid对应的远程节点本地节点Source资源警报已经解除(向Pid对应的远程节点发布clear_alarm事件)
remote_conserve_resources(Pid, Source, false) ->
	gen_event:notify({?SERVER, node(Pid)},
					 {clear_alarm, {resource_limit, Source, node()}}).

%%----------------------------------------------------------------------------
%% rabbit_alarm进程的初始化
init([]) ->
	{ok, #alarms{alertees      = dict:new(),
				 alarmed_nodes = dict:new(),
				 alarms        = []}}.


%% 处理注册的消息
handle_call({register, Pid, AlertMFA}, State = #alarms{alarmed_nodes = AN}) ->
	{ok, lists:usort(lists:append([V || {_, V} <- dict:to_list(AN)])),
	 %% 进行内部注册
	 internal_register(Pid, AlertMFA, State)};


%% 处理读取所有报警器的消息
handle_call(get_alarms, State = #alarms{alarms = Alarms}) ->
	{ok, Alarms, State};


handle_call(_Request, State) ->
	{ok, not_understood, State}.


%% 处理设置报警器的事件
handle_event({set_alarm, Alarm}, State = #alarms{alarms = Alarms}) ->
	case lists:member(Alarm, Alarms) of
		true  -> {ok, State};
		false -> UpdatedAlarms = lists:usort([Alarm | Alarms]),
				 %% 处理设置报警器
				 handle_set_alarm(Alarm, State#alarms{alarms = UpdatedAlarms})
	end;


%% 处理清除报警器的事件
handle_event({clear_alarm, Alarm}, State = #alarms{alarms = Alarms}) ->
	case lists:keymember(Alarm, 1, Alarms) of
		true  -> handle_clear_alarm(
				   Alarm, State#alarms{alarms = lists:keydelete(
												  Alarm, 1, Alarms)});
		false -> {ok, State}
	
	end;


%% 处理Node节点启动的事件
%% conserve：节约
handle_event({node_up, Node}, State) ->
	%% Must do this via notify and not call to avoid possible deadlock.
	ok = gen_event:notify(
		   {?SERVER, Node},
		   {register, self(), {?MODULE, remote_conserve_resources, []}}),
	{ok, State};


%% 处理Node节点挂掉的事件
handle_event({node_down, Node}, State) ->
	{ok, maybe_alert(fun dict_unappend_all/3, Node, [], false, State)};


%% 处理注册的事件
handle_event({register, Pid, AlertMFA}, State) ->
	{ok, internal_register(Pid, AlertMFA, State)};


handle_event(_Event, State) ->
	{ok, State}.


%% 注册的进程down掉，则从alertees字典将该进程对应的执行函数信息清除掉
handle_info({'DOWN', _MRef, process, Pid, _Reason},
			State = #alarms{alertees = Alertees}) ->
	{ok, State#alarms{alertees = dict:erase(Pid, Alertees)}};


handle_info(_Info, State) ->
	{ok, State}.


terminate(_Arg, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------------
%% 将Val值添加到Key对应的列表中
dict_append(Key, Val, Dict) ->
	L = case dict:find(Key, Dict) of
			{ok, V} -> V;
			error   -> []
		end,
	dict:store(Key, lists:usort([Val | L]), Dict).


dict_unappend_all(Key, _Val, Dict) ->
	dict:erase(Key, Dict).


%% 将Val值从Key对应的值列表中删除掉，如果Key对应的值列表为空，则将Key从字典中删除掉
dict_unappend(Key, Val, Dict) ->
	L = case dict:find(Key, Dict) of
			{ok, V} -> V;
			error   -> []
		end,
	
	case lists:delete(Val, L) of
		[] -> dict:erase(Key, Dict);
		X  -> dict:store(Key, X, Dict)
	end.


%% 执行警报
maybe_alert(UpdateFun, Node, Source, Alert,
			State = #alarms{alarmed_nodes = AN,
							alertees      = Alertees}) ->
	%% 更新alarmed_nodes字段
	AN1 = UpdateFun(Node, Source, AN),
	case node() of
		%% 如果是当前节点的警报变化，则需要通知远程节点将警报做相应的处理
		Node -> ok = alert_remote(Alert,  Alertees, Source);
		_    -> ok
	end,
	%% 执行本地警报
	ok = alert_local(Alert, Alertees, Source),
	State#alarms{alarmed_nodes = AN1}.


%% 只执行本地警报
alert_local(Alert, Alertees, Source) ->
	alert(Alertees, Source, Alert, fun erlang:'=:='/2).


%% 只执行远程节点的警报(通知远程节点本地节点警报情况)
alert_remote(Alert, Alertees, Source) ->
	alert(Alertees, Source, Alert, fun erlang:'=/='/2).


%% 真正的执行警报的函数(Alert为true，表示增加的警报，Alert为false，表示去掉警报)
alert(Alertees, Source, Alert, NodeComparator) ->
	Node = node(),
	dict:fold(fun (Pid, {M, F, A}, ok) ->
					   case NodeComparator(Node, node(Pid)) of
						   true  -> apply(M, F, A ++ [Pid, Source, Alert]);
						   false -> ok
					   end
			  end, ok, Alertees).


%% rabbit_alarm进程内部处理注册的函数接口
internal_register(Pid, {M, F, A} = AlertMFA,
				  State = #alarms{alertees = Alertees}) ->
	%% 监视Pid进程
	_MRef = erlang:monitor(process, Pid),
	%% 如果当前节点已经有报警信息，则立刻进行回调操作
	case dict:find(node(), State#alarms.alarmed_nodes) of
		{ok, Sources} -> [apply(M, F, A ++ [Pid, R, true]) || R <- Sources];
		error          -> ok
	end,
	%% 将Pid对应的AlertMFA执行函数存放到alertees字典中
	NewAlertees = dict:store(Pid, AlertMFA, Alertees),
	State#alarms{alertees = NewAlertees}.


%% 处理设置报警器的接口函数
handle_set_alarm({{resource_limit, Source, Node}, []}, State) ->
	%% 将报警信息打印在日志中
	rabbit_log:warning(
	  "~s resource limit alarm set on node ~p.~n~n"
	  "**********************************************************~n"
	  "*** Publishers will be blocked until this alarm clears ***~n"
	  "**********************************************************~n",
	  [Source, Node]),
	{ok, maybe_alert(fun dict_append/3, Node, Source, true, State)};

handle_set_alarm({file_descriptor_limit, []}, State) ->
	rabbit_log:warning(
	  "file descriptor limit alarm set.~n~n"
	  "********************************************************************~n"
	  "*** New connections will not be accepted until this alarm clears ***~n"
	  "********************************************************************~n"),
	{ok, State};

handle_set_alarm(Alarm, State) ->
	rabbit_log:warning("alarm '~p' set~n", [Alarm]),
	{ok, State}.


%% 处理取消报警器的接口函数
handle_clear_alarm({resource_limit, Source, Node}, State) ->
	rabbit_log:warning("~s resource limit alarm cleared on node ~p~n",
					   [Source, Node]),
	{ok, maybe_alert(fun dict_unappend/3, Node, Source, false, State)};

handle_clear_alarm(file_descriptor_limit, State) ->
	rabbit_log:warning("file descriptor limit alarm cleared~n"),
	{ok, State};

handle_clear_alarm(Alarm, State) ->
	rabbit_log:warning("alarm '~p' cleared~n", [Alarm]),
	{ok, State}.
