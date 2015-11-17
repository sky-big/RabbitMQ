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

-module(rabbit_autoheal).

-export([init/0, enabled/0, maybe_start/1, rabbit_down/2, node_down/2,
         handle_msg/3]).

%% The named process we are running in.
-define(SERVER, rabbit_node_monitor).

-define(MNESIA_STOPPED_PING_INTERNAL, 200).

-define(AUTOHEAL_STATE_AFTER_RESTART, rabbit_autoheal_state_after_restart).

%%----------------------------------------------------------------------------
%% heal：治愈
%% In order to autoheal we want to:
%%
%% * Find the winning partition
%% * Stop all nodes in other partitions
%% * Wait for them all to be stopped
%% * Start them again
%%
%% To keep things simple, we assume(假设) all nodes are up. We don't start
%% unless all nodes are up, and if a node goes down we abandon(放弃) the
%% whole process. To further keep things simple we also defer the
%% decision(决定) as to the winning node to the "leader" - arbitrarily(任意)
%% selected as the first node in the cluster.
%%
%% To coordinate(协调) the restarting nodes we pick a special node from the
%% winning partition - the "winner". Restarting nodes then stop, and
%% wait for it to tell them it is safe to start again. The winner
%% determines(决定) that a node has stopped just by seeing if its rabbit app
%% stops - if a node stops for any other reason it just gets a message
%% it will ignore, and otherwise we carry on.
%%
%% Meanwhile, the leader may continue to receive new autoheal requests:
%% all of them are ignored. The winner notifies the leader when the
%% current autoheal process is finished (ie. when all losers stopped and
%% were asked to start again) or was aborted. When the leader receives
%% the notification or if it looses contact with the winner, it can
%% accept new autoheal requests.
%%
%% The winner and the leader are not necessarily the same node.
%%
%% The leader can be a loser and will restart in this case. It remembers
%% there is an autoheal in progress by temporarily saving the autoheal
%% state to the application environment.
%%
%% == Possible states ==
%%
%% not_healing
%%   - the default
%%
%% {winner_waiting, OutstandingStops, Notify}
%%   - we are the winner and are waiting for all losing nodes to stop
%%   before telling them they can restart
%%
%% {leader_waiting, Winner, Notify}
%%   - we are the leader, and have already assigned the winner and losers.
%%   We are waiting for a confirmation from the winner that the autoheal
%%   process has ended. Meanwhile we can ignore autoheal requests.
%%   Because we may be a loser too, this state is saved to the application
%%   environment and restored on startup.
%%
%% restarting
%%   - we are restarting. Of course the node monitor immediately dies
%%   then so this state does not last long. We therefore send the
%%   autoheal_safe_to_start message to the rabbit_outside_app_process
%%   instead.
%%
%% == Message flow ==(消息流)
%%
%% 1. 当Mnesia检测出集群出现分区，则从集群中找到第一个节点作为leader节点进行autoheal操作
%% 1. Any node (leader included) >> {request_start, node()} >> Leader
%%      When Mnesia detects(检测) it is running partitioned(分区) or
%%      when a remote node starts, rabbit_node_monitor calls
%%      rabbit_autoheal:maybe_start/1. The message above is sent to the
%%      leader so the leader can take a decision.
%%
%% 2. Leader节点找出胜利进程和失败进程，胜利者节点收到Leader节点发送过来通知自己成为胜利者的消息
%% 2. Leader >> {become_winner, Losers} >> Winner
%%      The leader notifies the winner so the latter can proceed with
%%      the autoheal.
%%
%% 3.胜利者节点通知所有的失败者进程自己是Winner节点，然后失败者节点新启动一个进程进行rabbit应用的重启
%% 3. Winner >> {winner_is, Winner} >> All losers
%%      The winner notifies losers they must stop.
%%
%% 4. 当胜利者节点收取到了所有失败者节点的停止后，立刻向所有失败者节点上的新启动的进程发送autoheal_safe_to_start消息，通知失败者节点重新启动rabbit应用
%% 4. Winner >> autoheal_safe_to_start >> All losers
%%      When either all losers stopped or the autoheal process was
%%      aborted, the winner notifies losers they can start again.
%%
%% 5. Leader进程向胜利者进程发送report_autoheal_status消息
%% 5. Leader >> report_autoheal_status >> Winner
%%      The leader asks the autoheal status to the winner. This only
%%      happens when the leader is a loser too. If this is not the case,
%%      this message is never sent.
%%
%% 6. 胜利者进程通知Leader进程autoheal操作结束
%% 6. Winner >> {autoheal_finished, Winner} >> Leader
%%      The winner notifies the leader that the autoheal process was
%%      either finished or aborted (ie. autoheal_safe_to_start was sent
%%      to losers).

%%----------------------------------------------------------------------------
%% RabbitMQ系统自动自愈autoheal模式的初始化接口
init() ->
	%% We check the application environment for a saved autoheal state
	%% saved during a restart. If this node is a leader, it is used
	%% to determine if it needs to ask the winner to report about the
	%% autoheal progress.
	State = case application:get_env(rabbit, ?AUTOHEAL_STATE_AFTER_RESTART) of
				{ok, S}   -> S;
				undefined -> not_healing
			end,
	ok = application:unset_env(rabbit, ?AUTOHEAL_STATE_AFTER_RESTART),
	case State of
		{leader_waiting, Winner, _} ->
			rabbit_log:info(
			  "Autoheal: in progress, requesting report from ~p~n", [Winner]),
			send(Winner, report_autoheal_status);
		_ ->
			ok
	end,
	State.


maybe_start(not_healing) ->
	%% 通过配置文件判断是否需要autoheal操作
	case enabled() of
		true  -> Leader = leader(),
				 %% 向Leader节点的rabbit_node_monitor进程发送请求开始的消息
				 send(Leader, {request_start, node()}),
				 rabbit_log:info("Autoheal request sent to ~p~n", [Leader]),
				 not_healing;
		false -> not_healing
	end;
maybe_start(State) ->
	State.


%% 通过配置文件判断是否需要autoheal操作
enabled() ->
	case application:get_env(rabbit, cluster_partition_handling) of
		{ok, autoheal}                         -> true;
		{ok, {pause_if_all_down, _, autoheal}} -> true;
		_                                      -> false
	end.


%% 得到领导者节点(其实是集群中的第一个节点)
leader() ->
	[Leader | _] = lists:usort(rabbit_mnesia:cluster_nodes(all)),
	Leader.

%% This is the winner receiving its last notification that a node has
%% stopped - all nodes can now start again
%% 失败者节点重启挂掉后，胜利者节点会通过监视信息得到信息
%% 此处得到失败者所有节点rabbit应用停止完毕
rabbit_down(Node, {winner_waiting, [Node], Notify}) ->
	rabbit_log:info("Autoheal: final node has stopped, starting...~n",[]),
	winner_finish(Notify);

rabbit_down(Node, {winner_waiting, WaitFor, Notify}) ->
	{winner_waiting, WaitFor -- [Node], Notify};

rabbit_down(Winner, {leader_waiting, Winner, Losers}) ->
	abort([Winner], Losers);

rabbit_down(_Node, State) ->
	%% Ignore. Either:
	%%     o  we already cancelled the autoheal process;
	%%     o  we are still waiting the winner's report.
	State.

node_down(_Node, not_healing) ->
	not_healing;

node_down(Node, {winner_waiting, _, Notify}) ->
	abort([Node], Notify);

node_down(Node, _State) ->
	rabbit_log:info("Autoheal: aborting - ~p went down~n", [Node]),
	not_healing.

%% By receiving this message we become the leader
%% TODO should we try to debounce this?
%% 处理请求开始的消息
handle_msg({request_start, Node},
		   not_healing, Partitions) ->
	%% 打印Autoheal开始的消息
	rabbit_log:info("Autoheal request received from ~p~n", [Node]),
	case check_other_nodes(Partitions) of
		{error, E} ->
			rabbit_log:info("Autoheal request denied: ~s~n", [fmt_error(E)]),
			not_healing;
		{ok, AllPartitions} ->
			%% 根据不同的分区，得到不同分区的客户端连接数，用客户端连接数最多的分区中的第一个节点作为胜利节点，其他节点为失败节点
			{Winner, Losers} = make_decision(AllPartitions),
			%% 将分区信息，以及胜利节点和失败节点打印日志
			rabbit_log:info("Autoheal decision~n"
							 "  * Partitions: ~p~n"
							 "  * Winner:     ~p~n"
							 "  * Losers:     ~p~n",
							 [AllPartitions, Winner, Losers]),
			%% 通知胜利者节点，它成为胜利节点
			case node() =:= Winner of
				true  -> handle_msg({become_winner, Losers},
									not_healing, Partitions);
				false -> send(Winner, {become_winner, Losers}),
						 {leader_waiting, Winner, Losers}
			end
	end;

handle_msg({request_start, Node},
		   State, _Partitions) ->
	rabbit_log:info("Autoheal request received from ~p when healing; "
						"ignoring~n", [Node]),
	State;

%% 处理自己成为胜利者节点的消息
handle_msg({become_winner, Losers},
		   not_healing, _Partitions) ->
	%% 打印自己成为胜利节点，等待失败节点rabbit的停止
	rabbit_log:info("Autoheal: I am the winner, waiting for ~p to stop~n",
					[Losers]),
	%% The leader said everything was ready - do we agree? If not then
	%% give up.
	Down = Losers -- rabbit_node_monitor:alive_rabbit_nodes(Losers),
	case Down of
		%% 如果没有挂掉的节点，则通知所有的失败者节点胜利者节点
		[] -> [send(L, {winner_is, node()}) || L <- Losers],
			  {winner_waiting, Losers, Losers};
		_  -> abort(Down, Losers)
	end;

%% 失败者节点处理胜利者节点是哪一个的消息
handle_msg({winner_is, Winner}, State = not_healing,
		   _Partitions) ->
	%% This node is a loser, nothing else.
	%% 重启当前节点RabbitMQ系统(当前节点为失败节点)
	restart_loser(State, Winner),
	restarting;
handle_msg({winner_is, Winner}, State = {leader_waiting, Winner, _},
		   _Partitions) ->
	%% This node is the leader and a loser at the same time.(此节点既是Leader节点又是失败节点)
	%% 重启当前节点RabbitMQ系统(当前节点为失败节点)
	restart_loser(State, Winner),
	restarting;

handle_msg(_, restarting, _Partitions) ->
	%% ignore, we can contribute no further
	restarting;

handle_msg(report_autoheal_status, not_healing, _Partitions) ->
	%% The leader is asking about the autoheal status to us (the
	%% winner). This happens when the leader is a loser and it just
	%% restarted. We are in the "not_healing" state, so the previous
	%% autoheal process ended: let's tell this to the leader.
	send(leader(), {autoheal_finished, node()}),
	not_healing;

handle_msg(report_autoheal_status, State, _Partitions) ->
	%% Like above, the leader is asking about the autoheal status. We
	%% are not finished with it. There is no need to send anything yet
	%% to the leader: we will send the notification when it is over.
	State;

%% leader进程处理autoheal完成的消息
handle_msg({autoheal_finished, Winner},
		   {leader_waiting, Winner, _}, _Partitions) ->
	%% The winner is finished with the autoheal process and notified us
	%% (the leader). We can transition to the "not_healing" state and
	%% accept new requests.
	rabbit_log:info("Autoheal finished according to winner ~p~n", [Winner]),
	not_healing;

handle_msg({autoheal_finished, Winner}, not_healing, _Partitions)
  when Winner =:= node() ->
	%% We are the leader and the winner. The state already transitioned
	%% to "not_healing" at the end of the autoheal process.
	rabbit_log:info("Autoheal finished according to winner ~p~n", [node()]),
	not_healing.

%%----------------------------------------------------------------------------
%% 向Node节点的rabbit_node_monitor进程发送{autoheal_msg开头的消息
send(Node, Msg) -> {?SERVER, Node} ! {autoheal_msg, Msg}.


abort(Down, Notify) ->
	rabbit_log:info("Autoheal: aborting - ~p down~n", [Down]),
	%% Make sure any nodes waiting for us start - it won't necessarily
	%% heal the partition but at least they won't get stuck.
	winner_finish(Notify).

winner_finish(Notify) ->
	%% There is a race in Mnesia causing a starting loser to hang
	%% forever if another loser stops at the same time: the starting
	%% node connects to the other node, negotiates the protocol and
	%% attempts to acquire a write lock on the schema on the other node.
	%% If the other node stops between the protocol negotiation and lock
	%% request, the starting node never gets an answer to its lock
	%% request.
	%%
	%% To work around the problem, we make sure Mnesia is stopped on all
	%% losing nodes before sending the "autoheal_safe_to_start" signal.
	%% 等待Notify节点中所有的mnesia数据库全部停止
	wait_for_mnesia_shutdown(Notify),
	%% 通知失败者节点上的rabbit_outside_app_process进程autoheal_safe_to_start消息
	[{rabbit_outside_app_process, N} ! autoheal_safe_to_start || N <- Notify],
	send(leader(), {autoheal_finished, node()}),
	not_healing.


%% 等待AllNodes节点中所有的mnesia数据库全部停止
wait_for_mnesia_shutdown([Node | Rest] = AllNodes) ->
	case rpc:call(Node, mnesia, system_info, [is_running]) of
		no ->
			wait_for_mnesia_shutdown(Rest);
		Running when
		  Running =:= yes orelse
			  Running =:= starting orelse
			  Running =:= stopping ->
			timer:sleep(?MNESIA_STOPPED_PING_INTERNAL),
			wait_for_mnesia_shutdown(AllNodes);
		_ ->
			wait_for_mnesia_shutdown(Rest)
	end;
wait_for_mnesia_shutdown([]) ->
	ok.


%% 重启当前节点RabbitMQ系统(当前节点为失败节点)
restart_loser(State, Winner) ->
	%% 失败节点打印胜利节点信息
	rabbit_log:warning(
	  "Autoheal: we were selected to restart; winner is ~p~n", [Winner]),
	%% 失败者节点在rabbit应用外启动一个rabbit_outside_app_process名字的进程来执行Fun函数(即rabbit_outside_app_process进程完成失败者节点的重启)
	rabbit_node_monitor:run_outside_applications(
	  fun () ->
			   %% 监视胜利者节点
			   MRef = erlang:monitor(process, {?SERVER, Winner}),
			   %% 将rabbit应用停止
			   rabbit:stop(),
			   NextState = receive
							   {'DOWN', MRef, process, {?SERVER, Winner}, _Reason} ->
								   not_healing;
							   autoheal_safe_to_start ->
								   State
						   end,
			   %% 解除对胜利者节点的监视
			   erlang:demonitor(MRef, [flush]),
			   %% During the restart, the autoheal state is lost so we
			   %% store it in the application environment temporarily so
			   %% init/0 can pick it up.
			   %%
			   %% This is useful to the leader which is a loser at the
			   %% same time: because the leader is restarting, there
			   %% is a great chance it misses the "autoheal finished!"
			   %% notification from the winner. Thanks to the saved
			   %% state, it knows it needs to ask the winner if the
			   %% autoheal process is finished or not.
			   %% 当前节点即失败者节点设置rabbit_autoheal_state_after_restart对应的配置信息为NextState
			   application:set_env(rabbit,
								   ?AUTOHEAL_STATE_AFTER_RESTART, NextState),
			   %% 当前节点即失败者节点重启rabbit应用
			   rabbit:start()
	  end, true).


%% 根据不同的分区，得到不同分区的客户端连接数，用客户端连接数最多的分区中的第一个节点作为胜利节点，其他节点为失败节点
make_decision(AllPartitions) ->
	Sorted = lists:sort([{partition_value(P), P} || P <- AllPartitions]),
	[[Winner | _] | Rest] = lists:reverse([P || {_, P} <- Sorted]),
	{Winner, lists:append(Rest)}.


%% 得到Partition分区节点列表中的所有客户端连接数
partition_value(Partition) ->
	Connections = [Res || Node <- Partition,
						  Res <- [rpc:call(Node, rabbit_networking,
										   connections_local, [])],
						  is_list(Res)],
	{length(lists:append(Connections)), length(Partition)}.

%% We have our local understanding of what partitions exist; but we
%% only know which nodes we have been partitioned from, not which
%% nodes are partitioned from each other.
check_other_nodes(LocalPartitions) ->
	%% 得到集群中所有的节点
	Nodes = rabbit_mnesia:cluster_nodes(all),
	{Results, Bad} = rabbit_node_monitor:status(Nodes -- [node()]),
	%% 从其他节点上拿到各自节点上的分区节点
	RemotePartitions = [{Node, proplists:get_value(partitions, Res)}
						|| {Node, Res} <- Results],
	%% 从其他节点上拿到挂掉的节点
	RemoteDown = [{Node, Down}
				  || {Node, Res} <- Results,
					 Down <- [Nodes -- proplists:get_value(nodes, Res)],
					 Down =/= []],
	case {Bad, RemoteDown} of
		{[], []} -> Partitions = [{node(), LocalPartitions} | RemotePartitions],
					{ok, all_partitions(Partitions, [Nodes])};
		{[], _}  -> {error, {remote_down, RemoteDown}};
		{_,  _}  -> {error, {nodes_down, Bad}}
	end.


%% 根据所有节点本地的分区信息将所有节点分成不同的区
all_partitions([], Partitions) ->
	Partitions;
all_partitions([{Node, CantSee} | Rest], Partitions) ->
	{[Containing], Others} =
		lists:partition(fun (Part) -> lists:member(Node, Part) end, Partitions),
	%% 从Containing列表中得到同Node节点在同一分区的节点
	A = Containing -- CantSee,
	%% 从Containing列表中得到同Node节点在不同一分区的节点
	B = Containing -- A,
	%% 如果A，B都不为空，则表示有两个新的分区
	Partitions1 = case {A, B} of
					  {[], _}  -> Partitions;
					  {_,  []} -> Partitions;
					  _        -> [A, B | Others]
				  end,
	all_partitions(Rest, Partitions1).


fmt_error({remote_down, RemoteDown}) ->
	rabbit_misc:format("Remote nodes disconnected:~n ~p", [RemoteDown]);
fmt_error({nodes_down, NodesDown}) ->
	rabbit_misc:format("Local nodes down: ~p", [NodesDown]).
