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

-module(rabbit_node_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([running_nodes_filename/0,
         cluster_status_filename/0, prepare_cluster_status_files/0,
         write_cluster_status/1, read_cluster_status/0,
         update_cluster_status/0, reset_cluster_status/0]).
-export([notify_node_up/0, notify_joined_cluster/0, notify_left_cluster/1]).
-export([partitions/0, partitions/1, status/1, subscribe/1]).
-export([pause_partition_guard/0]).
-export([global_sync/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

 %% Utils
-export([all_rabbit_nodes_up/0, run_outside_applications/2, ping_all/0,
         alive_nodes/1, alive_rabbit_nodes/1]).

-define(SERVER, ?MODULE).
-define(RABBIT_UP_RPC_TIMEOUT, 2000).
-define(RABBIT_DOWN_PING_INTERVAL, 1000).

-record(state, {
				monitors,						%% 监视集群其他节点的rabbit应用的数据结构
				partitions,						%% 跟本节点位于不同分区的节点列表(当网络发生网络分裂)
				subscribers,					%% 订阅者监视数据结构
				down_ping_timer,				%% 当有节点挂掉后，重新ping同集群中所有节点的定时器
				keepalive_timer,				%% rabbit_node_monitor进程检查集群存活的定时器
				autoheal,						%% RabbitMQ集群系统发生网络分裂的时候，自动修复的状态保存数据结构字段
				guid,							%% 当前节点的唯一标识
				node_guids						%% 集群中其他节点以及它们对应的guid Orddict数据结构
			   }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).

-spec(running_nodes_filename/0 :: () -> string()).
-spec(cluster_status_filename/0 :: () -> string()).
-spec(prepare_cluster_status_files/0 :: () -> 'ok').
-spec(write_cluster_status/1 :: (rabbit_mnesia:cluster_status()) -> 'ok').
-spec(read_cluster_status/0 :: () -> rabbit_mnesia:cluster_status()).
-spec(update_cluster_status/0 :: () -> 'ok').
-spec(reset_cluster_status/0 :: () -> 'ok').

-spec(notify_node_up/0 :: () -> 'ok').
-spec(notify_joined_cluster/0 :: () -> 'ok').
-spec(notify_left_cluster/1 :: (node()) -> 'ok').

-spec(partitions/0 :: () -> [node()]).
-spec(partitions/1 :: ([node()]) -> [{node(), [node()]}]).
-spec(status/1 :: ([node()]) -> {[{node(), [node()]}], [node()]}).
-spec(subscribe/1 :: (pid()) -> 'ok').
-spec(pause_partition_guard/0 :: () -> 'ok' | 'pausing').

-spec(all_rabbit_nodes_up/0 :: () -> boolean()).
-spec(run_outside_applications/2 :: (fun (() -> any()), boolean()) -> pid()).
-spec(ping_all/0 :: () -> 'ok').
-spec(alive_nodes/1 :: ([node()]) -> [node()]).
-spec(alive_rabbit_nodes/1 :: ([node()]) -> [node()]).

-endif.

%%----------------------------------------------------------------------------
%% Start
%%----------------------------------------------------------------------------
%% 启动rabbit_node_monitor进程的API接口
start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%----------------------------------------------------------------------------
%% Cluster file operations(集群文件操作)
%%----------------------------------------------------------------------------

%% The cluster file information is kept in two files.  The "cluster
%% status file" contains all the clustered nodes(集群节点) and the disc nodes(磁盘节点).
%% The "running nodes file" contains the currently running nodes or
%% the running nodes at shutdown when the node is down.
%%
%% We strive(努力) to keep the files up to date and we rely on this
%% assumption in various situations. Obviously when mnesia is offline
%% the information we have will be outdated(过时的), but it cannot be
%% otherwise.
%% google翻译：我们努力保持这些文件是最新的。我们依靠这种假设在各种情况下。显然，当函数mnesia离线我们的资料就会过时，而且它不能以其他方式。

%% 拿到所有的运行中的节点记录文件绝对路径
running_nodes_filename() ->
	filename:join(rabbit_mnesia:dir(), "nodes_running_at_shutdown").


%% 该文件中写入有所有的运行节点已经宕掉的节点列表
cluster_status_filename() ->
	rabbit_mnesia:dir() ++ "/cluster_nodes.config".


%% 准备集群节点状态文件(将集群节点的信息写入文件)
prepare_cluster_status_files() ->
	%% 确保mnesia数据库保存目录的存在
	rabbit_mnesia:ensure_mnesia_dir(),
	Corrupt = fun(F) -> throw({error, corrupt_cluster_status_files, F}) end,
	%% 拿到集群中的所有的正在运行中的节点
	RunningNodes1 = case try_read_file(running_nodes_filename()) of
						{ok, [Nodes]} when is_list(Nodes) -> Nodes;
						{ok, Other}                       -> Corrupt(Other);
						{error, enoent}                   -> []					%% 文件不存在
					end,
	ThisNode = [node()],
	%% The running nodes file might contain a set or a list, in case
	%% of the legacy file
	%% lists:usort会对列表排序，同时会删除重复的元素
	RunningNodes2 = lists:usort(ThisNode ++ RunningNodes1),
	{AllNodes1, DiscNodes} =
		case try_read_file(cluster_status_filename()) of
			{ok, [{AllNodes, DiscNodes0}]} ->
				{AllNodes, DiscNodes0};
			{ok, [AllNodes0]} when is_list(AllNodes0) ->
				{legacy_cluster_nodes(AllNodes0), legacy_disc_nodes(AllNodes0)};
			{ok, Files} ->
				Corrupt(Files);
			{error, enoent} ->
				%% 当本地集群状态的文件不存在的时候调用(新节点会走到此处，此处会将自己当前的节点名字作为所有的节点和磁盘节点)
				LegacyNodes = legacy_cluster_nodes([]),
				{LegacyNodes, LegacyNodes}
		end,
	%% lists:usort会对列表排序，同时会删除重复的元素
	AllNodes2 = lists:usort(AllNodes1 ++ RunningNodes2),
	%% 将所有节点和磁盘节点信息写入文件(将集群的信息写入本地文件)，同时会将当前正在运行的节点写入本地磁盘文件
	ok = write_cluster_status({AllNodes2, DiscNodes, RunningNodes2}).


%% 将所有节点和磁盘节点信息写入文件(将集群的信息写入本地文件)，同时会将当前正在运行的节点写入本地磁盘文件
write_cluster_status({All, Disc, Running}) ->
	ClusterStatusFN = cluster_status_filename(),
	%% 在数据库目录下的cluster_nodes.config文件中写入集群中所有的节点和磁盘节点
	Res = case rabbit_file:write_term_file(ClusterStatusFN, [{All, Disc}]) of
			  ok ->
				  %% 在数据库目录下nodes_running_at_shutdown的文件中写入当前集群运行的节点列表
				  RunningNodesFN = running_nodes_filename(),
				  {RunningNodesFN,
				   rabbit_file:write_term_file(RunningNodesFN, [Running])};
			  E1 = {error, _} ->
				  {ClusterStatusFN, E1}
		  end,
	case Res of
		{_, ok}           -> ok;
		{FN, {error, E2}} -> throw({error, {could_not_write_file, FN, E2}})
	end.


%% 从cluster_nodes.config文件中读取集群中所有的节点，磁盘节点，在nodes_running_at_shutdown文件中读出出正在运行的节点列表
read_cluster_status() ->
	case {try_read_file(cluster_status_filename()),
		  try_read_file(running_nodes_filename())} of
		{{ok, [{All, Disc}]}, {ok, [Running]}} when is_list(Running) ->
			{All, Disc, Running};
		{Stat, Run} ->
			throw({error, {corrupt_or_missing_cluster_files, Stat, Run}})
	end.


%% 更新集群节点的状态，将当前集群中的所有节点，磁盘节点，正在运行的所有节点写入磁盘文件
update_cluster_status() ->
	%% 从mnesia数据库中拿到当前集群的信息
	{ok, Status} = rabbit_mnesia:cluster_status_from_mnesia(),
	%% 然后写入集群状态信息文件中
	write_cluster_status(Status).


%% 重置集群节点的状态(全部重置为本节点)
reset_cluster_status() ->
	write_cluster_status({[node()], [node()], [node()]}).

%%----------------------------------------------------------------------------
%% Cluster notifications(集群通知)
%%----------------------------------------------------------------------------
%% 通知有节点启动
notify_node_up() ->
	gen_server:cast(?SERVER, notify_node_up).


%% 通知集群中的其他节点自己加入集群
notify_joined_cluster() ->
	%% 拿到当前集群中不包括自己的正在运行的节点列表
	Nodes = rabbit_mnesia:cluster_nodes(running) -- [node()],
	gen_server:abcast(Nodes, ?SERVER,
					  {joined_cluster, node(), rabbit_mnesia:node_type()}),
	ok.


%% 通知集群中正在运行中的节点自己离开集群
notify_left_cluster(Node) ->
	Nodes = rabbit_mnesia:cluster_nodes(running),
	gen_server:abcast(Nodes, ?SERVER, {left_cluster, Node}),
	ok.

%%----------------------------------------------------------------------------
%% Server calls(服务器calls)
%%----------------------------------------------------------------------------
%% rabbit_node_monitor进程获得分区信息的接口
partitions() ->
	gen_server:call(?SERVER, partitions, infinity).


%% rabbit_node_monitor进程获得分区信息的接口
partitions(Nodes) ->
	{Replies, _} = gen_server:multi_call(Nodes, ?SERVER, partitions, infinity),
	Replies.


%% rabbit_node_monitor进程获取状态的接口
status(Nodes) ->
	gen_server:multi_call(Nodes, ?SERVER, status, infinity).


%% rabbit_node_monitor进程的订阅接口
subscribe(Pid) ->
	gen_server:cast(?SERVER, {subscribe, Pid}).

%%----------------------------------------------------------------------------
%% pause_minority/pause_if_all_down safety
%%----------------------------------------------------------------------------

%% If we are in a minority(少数的) and pause_minority mode then a) we are
%% going to shut down imminently and b) we should not confirm anything
%% until then, since anything we confirm is likely to be lost.
%%
%% The same principles apply to a node which isn't part of the preferred
%% partition when we are in pause_if_all_down mode.
%%
%% We could confirm something by having an HA queue see the pausing
%% state (and fail over into it) before the node monitor stops us, or
%% by using unmirrored queues and just having them vanish (and
%% confiming messages as thrown away).
%%
%% So we have channels call in here before issuing confirms, to do a
%% lightweight check that we have not entered a pausing state.
%% 判断当前节点是否发生了网络分区，如果发生了，自己通过配置文件的信息来决定自己节点是否需要pausing
pause_partition_guard() ->
	case get(pause_partition_guard) of
		not_pause_mode ->
			ok;
		undefined ->
			{ok, M} = application:get_env(rabbit, cluster_partition_handling),
			case M of
				pause_minority ->
					%% 如果当期集群存活的节点数不超过当前集群所有的节点数的一半，则返回pausing状态
					pause_minority_guard([], ok);
				{pause_if_all_down, PreferredNodes, _} ->
					%% 如果配置文件中配置的首选节点都联系不上，则当前节点进入pausing状态
					pause_if_all_down_guard(PreferredNodes, [], ok);
				_ ->
					put(pause_partition_guard, not_pause_mode),
					ok
			end;
		{minority_mode, Nodes, LastState} ->
			%% 如果当期集群存活的节点数不超过当前集群所有的节点数的一半，则返回pausing状态
			pause_minority_guard(Nodes, LastState);
		{pause_if_all_down_mode, PreferredNodes, Nodes, LastState} ->
			%% 如果配置文件中配置的首选节点都联系不上，则当前节点进入pausing状态
			pause_if_all_down_guard(PreferredNodes, Nodes, LastState)
	end.


%% 如果当期集群存活的节点数不超过当前集群所有的节点数的一半，则返回pausing状态
pause_minority_guard(LastNodes, LastState) ->
	case nodes() of
		%% 如果当前集群中存活的节点列表和之前的相比没有变化，则直接将老的状态返回
		LastNodes -> LastState;
		%% 判断当前集群存活的节点数是否超过当前集群所有的节点数一半
		_         -> NewState = case majority() of
									%% 如果当期集群存活的节点数不超过当前集群所有的节点数的一半，则返回pausing状态
									false -> pausing;
									true  -> ok
								end,
					 %% 将最新的状态存储到pause_partition_guard进程字典中
					 put(pause_partition_guard,
						 {minority_mode, nodes(), NewState}),
					 NewState
	end.


%% preferred：首选
%% 如果配置文件中配置的首选节点都联系不上，则当前节点进入pausing状态
pause_if_all_down_guard(PreferredNodes, LastNodes, LastState) ->
	case nodes() of
		%% 如果当前集群中存活的节点列表和之前的相比没有变化，则直接将老的状态返回
		LastNodes -> LastState;
		_         -> NewState = case in_preferred_partition(PreferredNodes) of
									%% 如果配置文件中配置的首选节点都联系不上，则当前节点进入pausing状态
									false -> pausing;
									true  -> ok
								end,
					 put(pause_partition_guard,
						 {pause_if_all_down_mode, PreferredNodes, nodes(),
						  NewState}),
					 NewState
	end.

%%----------------------------------------------------------------------------
%% "global" hang workaround.
%%----------------------------------------------------------------------------

%% This code works around a possible inconsistency(前后矛盾) in the "global"
%% state, causing global:sync/0 to never return.
%%
%%     1. A process is spawned.
%%     2. If after 15", global:sync() didn't return, the "global"
%%        state is parsed.
%%     3. If it detects that a sync is blocked for more than 10",
%%        the process sends fake nodedown/nodeup events to the two
%%        nodes involved (one local, one remote).
%%     4. Both "global" instances restart their synchronisation.
%%     5. globao:sync() finally returns.
%%
%% FIXME: Remove this workaround, once we got rid of the change to
%% "dist_auto_connect" and fixed the bugs uncovered.
%% 如果操作15秒内没有global:sync()同步成功，则自己手动去同步
global_sync() ->
	Pid = spawn(fun workaround_global_hang/0),
	ok = global:sync(),
	Pid ! global_sync_done,
	ok.


workaround_global_hang() ->
	receive
		global_sync_done ->
			ok
		after 15000 ->
			find_blocked_global_peers()
	end.


find_blocked_global_peers() ->
	{status, _, _, [Dict | _]} = sys:get_status(global_name_server),
	find_blocked_global_peers1(Dict).


find_blocked_global_peers1([{{sync_tag_his, Peer}, Timestamp} | Rest]) ->
	Diff = timer:now_diff(erlang:now(), Timestamp),
	if
		Diff >= 10000 -> unblock_global_peer(Peer);
		true          -> ok
	end,
	find_blocked_global_peers1(Rest);

find_blocked_global_peers1([_ | Rest]) ->
	find_blocked_global_peers1(Rest);

find_blocked_global_peers1([]) ->
	ok.


unblock_global_peer(PeerNode) ->
	ThisNode = node(),
	PeerState = rpc:call(PeerNode, sys, get_status, [global_name_server]),
	error_logger:info_msg(
	  "Global hang workaround: global state on ~s seems broken~n"
	  " * Peer global state:  ~p~n"
	  " * Local global state: ~p~n"
	  "Faking nodedown/nodeup between ~s and ~s~n",
	  [PeerNode, PeerState, sys:get_status(global_name_server),
				   PeerNode, ThisNode]),
	{global_name_server, ThisNode} ! {nodedown, PeerNode},
	{global_name_server, PeerNode} ! {nodedown, ThisNode},
	{global_name_server, ThisNode} ! {nodeup, PeerNode},
	{global_name_server, PeerNode} ! {nodeup, ThisNode},
	ok.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------
%% rabbit_node_monitor进程启动的回调初始化函数
init([]) ->
	%% trap exits(退出陷阱)
	%% We trap exits so that the supervisor will not just kill us. We
	%% want to be sure that we are not going to be killed while
	%% writing out the cluster status files - bad things can then
	%% happen.
	process_flag(trap_exit, true),
	net_kernel:monitor_nodes(true, [nodedown_reason]),
	%% 订阅mnesia数据库的系统事件(出现系统事件后，mnesia将向订阅进程发出消息)
	{ok, _} = mnesia:subscribe(system),
	{ok, ensure_keepalive_timer(#state{monitors    = pmon:new(),
									   subscribers = pmon:new(),
									   partitions  = [],
									   guid        = rabbit_guid:gen(),
									   node_guids  = orddict:new(),
									   %% RabbitMQ系统自动自愈autoheal模式的初始化接口
									   autoheal    = rabbit_autoheal:init()})}.


%% 同步处理获取分区信息
handle_call(partitions, _From, State = #state{partitions = Partitions}) ->
	{reply, Partitions, State};


%% 同步处理获取状态信息
handle_call(status, _From, State = #state{partitions = Partitions}) ->
	{reply, [{partitions, Partitions},
			 {nodes,      [node() | nodes()]}], State};


handle_call(_Request, _From, State) ->
	{noreply, State}.


%% 处理新节点启动的消息
handle_cast(notify_node_up, State = #state{guid = GUID}) ->
	%% 拿到当前不包括自己节点的所有集群正在运行的节点列表
	Nodes = rabbit_mnesia:cluster_nodes(running) -- [node()],
	%% 通知集群中的其他节点本节点启动
	gen_server:abcast(Nodes, ?SERVER,
					  {node_up, node(), rabbit_mnesia:node_type(), GUID}),
	%% register other active rabbits with this rabbit
	DiskNodes = rabbit_mnesia:cluster_nodes(disc),
	%% 通知本节点集群中的其他节点的启动
	[gen_server:cast(?SERVER, {node_up, N, case lists:member(N, DiskNodes) of
											   true  -> disc;
											   false -> ram
										   end}) || N <- Nodes],
	{noreply, State};

%%----------------------------------------------------------------------------
%% Partial partition detection(检测)
%%
%% Every node generates a GUID each time it starts, and announces that
%% GUID in 'node_up', with 'announce_guid' sent by return so the new
%% node knows the GUIDs of the others. These GUIDs are sent in all the
%% partial partition related messages to ensure that we ignore partial
%% partition messages from before we restarted (to avoid getting stuck
%% in a loop).
%%
%% When one node gets nodedown from another, it then sends
%% 'check_partial_partition' to all the nodes it still thinks are
%% alive. If any of those (intermediate) nodes still see the "down"
%% node as up, they inform it that this has happened. The original
%% node (in 'ignore', 'pause_if_all_down' or 'autoheal' mode) will then
%% disconnect from the intermediate node to "upgrade" to a full
%% partition.
%%
%% In pause_minority mode it will instead immediately pause until all
%% nodes come back. This is because the contract for pause_minority is
%% that nodes should never sit in a partitioned state - if it just
%% disconnected, it would become a minority, pause, realise it's not
%% in a minority any more, and come back, still partitioned (albeit no
%% longer partially).
%% ----------------------------------------------------------------------------
%% 有新节点启动通知到自己(自己节点已经启动在集群里)
handle_cast({node_up, Node, NodeType, GUID},
			State = #state{guid       = MyGUID,
						   node_guids = GUIDs}) ->
	%% 通知新启动的Node节点本节点的GUID
	cast(Node, {announce_guid, node(), MyGUID}),
	%% 将新启动的Node节点以及GUID存储到本节点
	GUIDs1 = orddict:store(Node, GUID, GUIDs),
	handle_cast({node_up, Node, NodeType}, State#state{node_guids = GUIDs1});


%% 收到其他节点发送过来的guid
handle_cast({announce_guid, Node, GUID}, State = #state{node_guids = GUIDs}) ->
	{noreply, State#state{node_guids = orddict:store(Node, GUID, GUIDs)}};


handle_cast({check_partial_partition, Node, Rep, NodeGUID, MyGUID, RepGUID},
			State = #state{guid       = MyGUID,
						   node_guids = GUIDs}) ->
	case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) andalso
			 orddict:find(Node, GUIDs) =:= {ok, NodeGUID} of
		true  -> spawn_link( %%[1]
				   fun () ->
							case rpc:call(Node, rabbit, is_running, []) of
								{badrpc, _} -> ok;
								_           -> cast(Rep, {partial_partition,
														  Node, node(), RepGUID})
							end
				   end);
		false -> ok
	end,
	{noreply, State};
%% [1] We checked that we haven't heard the node go down - but we
%% really should make sure we can actually communicate with
%% it. Otherwise there's a race where we falsely detect a partial
%% partition.
%%
%% Now of course the rpc:call/4 may take a long time to return if
%% connectivity with the node is actually interrupted - but that's OK,
%% we only really want to do something in a timely manner if
%% connectivity is OK. However, of course as always we must not block
%% the node monitor, so we do the check in a separate process.

handle_cast({check_partial_partition, _Node, _Reporter,
			 _NodeGUID, _GUID, _ReporterGUID}, State) ->
	{noreply, State};


handle_cast({partial_partition, NotReallyDown, Proxy, MyGUID},
			State = #state{guid = MyGUID}) ->
	FmtBase = "Partial partition detected:~n"
				  " * We saw DOWN from ~s~n"
					  " * We can still see ~s which can see ~s~n",
	ArgsBase = [NotReallyDown, Proxy, NotReallyDown],
	case application:get_env(rabbit, cluster_partition_handling) of
		{ok, pause_minority} ->
			rabbit_log:error(
			  FmtBase ++ " * pause_minority mode enabled~n"
				  "We will therefore pause until the *entire* cluster recovers~n",
			  ArgsBase),
			await_cluster_recovery(fun all_nodes_up/0),
			{noreply, State};
		{ok, {pause_if_all_down, PreferredNodes, _}} ->
			case in_preferred_partition(PreferredNodes) of
				true  -> rabbit_log:error(
						   FmtBase ++ "We will therefore intentionally "
							   "disconnect from ~s~n", ArgsBase ++ [Proxy]),
						 upgrade_to_full_partition(Proxy);
				false -> rabbit_log:info(
						   FmtBase ++ "We are about to pause, no need "
							   "for further actions~n", ArgsBase)
			end,
			{noreply, State};
		{ok, _} ->
			rabbit_log:error(
			  FmtBase ++ "We will therefore intentionally disconnect from ~s~n",
			  ArgsBase ++ [Proxy]),
			upgrade_to_full_partition(Proxy),
			{noreply, State}
	end;


handle_cast({partial_partition, _GUID, _Reporter, _Proxy}, State) ->
	{noreply, State};

%% Sometimes it appears the Erlang VM does not give us nodedown
%% messages reliably when another node disconnects from us. Therefore
%% we are told just before the disconnection so we can reciprocate.
handle_cast({partial_partition_disconnect, Other}, State) ->
	rabbit_log:error("Partial partition disconnect from ~s~n", [Other]),
	disconnect(Other),
	{noreply, State};

%% Note: when updating the status file, we can't simply write the
%% mnesia information since the message can (and will) overtake the
%% mnesia propagation.
%% 通知本节点有新节点的启动
handle_cast({node_up, Node, NodeType},
			State = #state{monitors = Monitors}) ->
	case pmon:is_monitored({rabbit, Node}, Monitors) of
		true  -> {noreply, State};
		false -> rabbit_log:info("rabbit on node ~p up~n", [Node]),
				 %% 从cluster_nodes.config文件中读取集群中所有的节点，磁盘节点，在nodes_running_at_shutdown文件中读出出正在运行的节点列表
				 {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
				 %% 将所有节点和磁盘节点信息写入文件(将集群的信息写入本地文件)，同时会将当前正在运行的节点写入本地磁盘文件
				 write_cluster_status({add_node(Node, AllNodes),
									   case NodeType of
										   disc -> add_node(Node, DiscNodes);
										   ram  -> DiscNodes
									   end,
									   add_node(Node, RunningNodes)}),
				 %% 本节点处理有Node节点加入集群的HOOK
				 ok = handle_live_rabbit(Node),
				 %% 监控新加入的节点Node上的rabbit进程
				 Monitors1 = pmon:monitor({rabbit, Node}, Monitors),
				 {noreply, maybe_autoheal(State#state{monitors = Monitors1})}
	end;


%% 新节点加入集群通知集群的其他节点
handle_cast({joined_cluster, Node, NodeType}, State) ->
	%% 从cluster_nodes.config文件中读取集群中所有的节点，磁盘节点，在nodes_running_at_shutdown文件中读出出正在运行的节点列表
	{AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
	%% 将所有节点和磁盘节点信息写入文件(将集群的信息写入本地文件)，同时会将当前正在运行的节点写入本地磁盘文件
	write_cluster_status({add_node(Node, AllNodes),
						  case NodeType of
							  disc -> add_node(Node, DiscNodes);
							  ram  -> DiscNodes
						  end,
						  RunningNodes}),
	{noreply, State};


%% Node节点离开集群
handle_cast({left_cluster, Node}, State) ->
	%% 从cluster_nodes.config文件中读取集群中所有的节点，磁盘节点，在nodes_running_at_shutdown文件中读出出正在运行的节点列表
	{AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
	%% 将所有节点和磁盘节点信息写入文件(将集群的信息写入本地文件)，同时会将当前正在运行的节点写入本地磁盘文件
	write_cluster_status({del_node(Node, AllNodes), del_node(Node, DiscNodes),
						  del_node(Node, RunningNodes)}),
	{noreply, State};


%% subscribe：订阅
handle_cast({subscribe, Pid}, State = #state{subscribers = Subscribers}) ->
	{noreply, State#state{subscribers = pmon:monitor(Pid, Subscribers)}};


handle_cast(keepalive, State) ->
	{noreply, State};


handle_cast(_Msg, State) ->
	{noreply, State}.


%% 有RabbitMQ节点宕机
handle_info({'DOWN', _MRef, process, {rabbit, Node}, _Reason},
			State = #state{monitors = Monitors, subscribers = Subscribers}) ->
	%% 写Node节点挂掉的日志
	rabbit_log:info("rabbit on node ~p down~n", [Node]),
	%% 从cluster_nodes.config文件中读取集群中所有的节点，磁盘节点，在nodes_running_at_shutdown文件中读出出正在运行的节点列表
	{AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
	%% 将所有节点和磁盘节点信息写入文件(将集群的信息写入本地文件)，同时会将当前正在运行的节点写入本地磁盘文件
	write_cluster_status({AllNodes, DiscNodes, del_node(Node, RunningNodes)}),
	%% 向订阅者发送Node节点挂掉的消息
	[P ! {node_down, Node} || P <- pmon:monitored(Subscribers)],
	%% 处理Node节点挂掉的后续操作
	{noreply, handle_dead_rabbit(
	   Node,
	   %% 当前节点取消对Node节点rabbit应用的监视
	   State#state{monitors = pmon:erase({rabbit, Node}, Monitors)})};


%% 处理订阅者进程挂掉的消息
handle_info({'DOWN', _MRef, process, Pid, _Reason},
			State = #state{subscribers = Subscribers}) ->
	%% 将挂掉的订阅者进程从监视数据结构中删除掉
	{noreply, State#state{subscribers = pmon:erase(Pid, Subscribers)}};


%% 处理集群中有节点挂掉的消息
handle_info({nodedown, Node, Info}, State = #state{guid       = MyGUID,
												   node_guids = GUIDs}) ->
	rabbit_log:info("node ~p down: ~p~n",
					[Node, proplists:get_value(nodedown_reason, Info)]),
	Check = fun (N, CheckGUID, DownGUID) ->
					 cast(N, {check_partial_partition,
							  Node, node(), DownGUID, CheckGUID, MyGUID})
			end,
	case orddict:find(Node, GUIDs) of
		{ok, DownGUID} -> Alive = rabbit_mnesia:cluster_nodes(running)
									  -- [node(), Node],
						  [case orddict:find(N, GUIDs) of
							   {ok, CheckGUID} -> Check(N, CheckGUID, DownGUID);
							   error           -> ok
						   end || N <- Alive];
		error          -> ok
	end,
	{noreply, handle_dead_node(Node, State)};


handle_info({nodeup, Node, _Info}, State) ->
	rabbit_log:info("node ~p up~n", [Node]),
	{noreply, State};


%% 处理mnesia数据库系统事件
%% running_partitioned_network：运行时对端节点up或启动完成同步数据后，发现对端节点曾经down过
%% inconsistent：不一致
handle_info({mnesia_system_event,
			 {inconsistent_database, running_partitioned_network, Node}},
			State = #state{partitions = Partitions,
						   monitors   = Monitors}) ->
	%% We will not get a node_up from this node - yet we should treat it as
	%% up (mostly).
	%% 监视Node节点
	State1 = case pmon:is_monitored({rabbit, Node}, Monitors) of
				 true  -> State;
				 false -> State#state{
									  monitors = pmon:monitor({rabbit, Node}, Monitors)}
			 end,
	%% 本节点处理有Node节点加入集群的HOOK
	ok = handle_live_rabbit(Node),
	%% 将Node节点加入到分区partitions字段中
	Partitions1 = lists:usort([Node | Partitions]),
	{noreply, maybe_autoheal(State1#state{partitions = Partitions1})};


%% 处理{autoheal_msg开头的消息
handle_info({autoheal_msg, Msg}, State = #state{autoheal   = AState,
												partitions = Partitions}) ->
	%% 用rabbit_autoheal模块来处理Msg消息
	AState1 = rabbit_autoheal:handle_msg(Msg, AState, Partitions),
	{noreply, State#state{autoheal = AState1}};


%% 处理ping通挂掉的节点，如果集群中挂掉的节点没有ping通，则继续启动定时器进行ping操作
handle_info(ping_down_nodes, State) ->
	%% We ping nodes when some are down to ensure that we find out
	%% about healed partitions quickly. We ping all nodes rather than
	%% just the ones we know are down for simplicity; it's not expensive
	%% to ping the nodes that are up, after all.
	State1 = State#state{down_ping_timer = undefined},
	Self = self(),
	%% We ping in a separate process since in a partition it might
	%% take some noticeable length of time and we don't want to block
	%% the node monitor for that long.
	spawn_link(fun () ->
						%% ping通集群中所有的节点
						ping_all(),
						%% 判断集群中的所有节点是否都在运行中
						case all_nodes_up() of
							true  -> ok;
							false -> Self ! ping_down_nodes_again
						end
			   end),
	{noreply, State1};


%% 收到继续ping挂掉的节点的消息，则启动定时器，继续ping通集群中所有的节点
handle_info(ping_down_nodes_again, State) ->
	{noreply, ensure_ping_timer(State)};


%% 处理Ping集群中的其他节点
handle_info(ping_up_nodes, State) ->
	%% In this case we need to ensure that we ping "quickly" -
	%% i.e. only nodes that we know to be up.
	[cast(N, keepalive) || N <- alive_nodes() -- [node()]],
	%% 同时启动定时器
	{noreply, ensure_keepalive_timer(State#state{keepalive_timer = undefined})};


handle_info(_Info, State) ->
	{noreply, State}.


terminate(_Reason, State) ->
	rabbit_misc:stop_timer(State, #state.down_ping_timer),
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------------
%% Functions that call the module specific(特定) hooks when nodes go up/down
%%----------------------------------------------------------------------------

handle_dead_node(Node, State = #state{autoheal = Autoheal}) ->
	%% In general in rabbit_node_monitor we care about whether the
	%% rabbit application is up rather than the node; we do this so
	%% that we can respond in the same way to "rabbitmqctl stop_app"
	%% and "rabbitmqctl stop" as much as possible.
	%%
	%% However, for pause_minority and pause_if_all_down modes we can't do
	%% this, since we depend on looking at whether other nodes are up
	%% to decide whether to come back up ourselves - if we decide that
	%% based on the rabbit application we would go down and never come
	%% back.
	case application:get_env(rabbit, cluster_partition_handling) of
		{ok, pause_minority} ->
			case majority([Node]) of
				true  -> ok;
				false -> await_cluster_recovery(fun majority/0)
			end,
			State;
		{ok, {pause_if_all_down, PreferredNodes, HowToRecover}} ->
			case in_preferred_partition(PreferredNodes, [Node]) of
				true  -> ok;
				false -> await_cluster_recovery(
						   fun in_preferred_partition/0)
			end,
			case HowToRecover of
				autoheal -> State#state{autoheal =
											rabbit_autoheal:node_down(Node, Autoheal)};
				_        -> State
			end;
		{ok, ignore} ->
			State;
		{ok, autoheal} ->
			State#state{autoheal = rabbit_autoheal:node_down(Node, Autoheal)};
		{ok, Term} ->
			rabbit_log:warning("cluster_partition_handling ~p unrecognised, "
								   "assuming 'ignore'~n", [Term]),
			State
	end.


await_cluster_recovery(Condition) ->
	rabbit_log:warning("Cluster minority/secondary status detected - "
						   "awaiting recovery~n", []),
	run_outside_applications(fun () ->
									  rabbit:stop(),
									  wait_for_cluster_recovery(Condition)
							 end, false),
	ok.


%% 失败者节点在rabbit应用外启动一个rabbit_outside_app_process名字的进程来执行Fun函数(即rabbit_outside_app_process进程完成失败者节点的重启)
run_outside_applications(Fun, WaitForExistingProcess) ->
	spawn(fun () ->
				   %% If our group leader is inside an application we are about
				   %% to stop, application:stop/1 does not return.
				   group_leader(whereis(init), self()),
				   register_outside_app_process(Fun, WaitForExistingProcess)
		  end).


%% 注册rabbit_outside_app_process进程完成失败者节点的重启
register_outside_app_process(Fun, WaitForExistingProcess) ->
    %% Ensure only one such process at a time, the exit(badarg) is
    %% harmless if one is already running.
    %%
    %% If WaitForExistingProcess is false, the given fun is simply not
    %% executed at all and the process exits.
    %%
    %% If WaitForExistingProcess is true, we wait for the end of the
    %% currently running process before executing the given function.
    try register(rabbit_outside_app_process, self()) of
        true ->
            do_run_outside_app_fun(Fun)
    catch
        error:badarg when WaitForExistingProcess ->
            MRef = erlang:monitor(process, rabbit_outside_app_process),
            receive
                {'DOWN', MRef, _, _, _} ->
                    %% The existing process exited, let's try to
                    %% register again.
                    register_outside_app_process(Fun, WaitForExistingProcess)
            end;
        error:badarg ->
            ok
    end.


do_run_outside_app_fun(Fun) ->
	try
		Fun()
	catch _:E ->
			  rabbit_log:error(
				"rabbit_outside_app_process:~n~p~n~p~n",
				[E, erlang:get_stacktrace()])
	end.


wait_for_cluster_recovery(Condition) ->
	ping_all(),
	case Condition() of
		true  -> rabbit:start();
		false -> timer:sleep(?RABBIT_DOWN_PING_INTERVAL),
				 wait_for_cluster_recovery(Condition)
	end.


%% 处理Node节点挂掉的后续操作
handle_dead_rabbit(Node, State = #state{partitions = Partitions,
										autoheal   = Autoheal}) ->
	%% TODO: This may turn out to be a performance hog when there are
	%% lots of nodes.  We really only need to execute some of these
	%% statements on *one* node, rather than all of them.
	%% 各个系统的回调操作
	ok = rabbit_networking:on_node_down(Node),
	ok = rabbit_amqqueue:on_node_down(Node),
	ok = rabbit_alarm:on_node_down(Node),
	ok = rabbit_mnesia:on_node_down(Node),
	%% If we have been partitioned, and we are now in the only remaining
	%% partition, we no longer care about partitions - forget them. Note
	%% that we do not attempt to deal with individual (other) partitions
	%% going away. It's only safe to forget anything about partitions when
	%% there are no partitions.
	%% alive_rabbit_nodes：得到集群中所有正在运行rabbit应用的节点列表
	Down = Partitions -- alive_rabbit_nodes(),
	%% 得到当前集群中所有正在运行的节点
	NoLongerPartitioned = rabbit_mnesia:cluster_nodes(running),
	Partitions1 = case Partitions -- Down -- NoLongerPartitioned of
					  [] -> [];
					  _  -> Partitions
				  end,
	ensure_ping_timer(
	  State#state{partitions = Partitions1,
				  autoheal   = rabbit_autoheal:rabbit_down(Node, Autoheal)}).


%% 启动ping通挂掉的节点的定时器
ensure_ping_timer(State) ->
	rabbit_misc:ensure_timer(
	  State, #state.down_ping_timer, ?RABBIT_DOWN_PING_INTERVAL,
	  ping_down_nodes).


%% 启动定时ping集群中其他节点的定时器
ensure_keepalive_timer(State) ->
	{ok, Interval} = application:get_env(rabbit, cluster_keepalive_interval),
	rabbit_misc:ensure_timer(
	  State, #state.keepalive_timer, Interval, ping_up_nodes).


%% 本节点处理有Node节点加入集群的HOOK
handle_live_rabbit(Node) ->
	ok = rabbit_amqqueue:on_node_up(Node),
	ok = rabbit_alarm:on_node_up(Node),
	ok = rabbit_mnesia:on_node_up(Node).


maybe_autoheal(State = #state{partitions = []}) ->
	State;


maybe_autoheal(State = #state{autoheal = AState}) ->
	%% 判断集群中的所有节点是否都在运行中
	case all_nodes_up() of
		true  -> State#state{autoheal = rabbit_autoheal:maybe_start(AState)};
		false -> State
	end.

%%--------------------------------------------------------------------
%% Internal utils
%%--------------------------------------------------------------------
%% 从FileName文件中读出Term类型的数据
try_read_file(FileName) ->
	case rabbit_file:read_term_file(FileName) of
		{ok, Term}      -> {ok, Term};
		{error, enoent} -> {error, enoent};
		{error, E}      -> throw({error, {cannot_read_file, FileName, E}})
	end.


%% 将mnesia数据库中的节点加入到当前集群中的所有节点中
legacy_cluster_nodes(Nodes) ->
	%% We get all the info that we can, including the nodes from
	%% mnesia, which will be there if the node is a disc node (empty
	%% list otherwise)
	lists:usort(Nodes ++ mnesia:system_info(db_nodes)).


%% 得到磁盘节点
legacy_disc_nodes(AllNodes) ->
	case AllNodes == [] orelse lists:member(node(), AllNodes) of
		true  -> [node()];
		false -> []
	end.


%% 集群增加新节点的接口
add_node(Node, Nodes) -> lists:usort([Node | Nodes]).


del_node(Node, Nodes) -> Nodes -- [Node].


%% 向Node节点的rabbit_node_monitor进程异步发送Msg消息
cast(Node, Msg) -> gen_server:cast({?SERVER, Node}, Msg).


upgrade_to_full_partition(Proxy) ->
	cast(Proxy, {partial_partition_disconnect, node()}),
	disconnect(Proxy).

%% When we call this, it's because we want to force Mnesia to detect a
%% partition. But if we just disconnect_node/1 then Mnesia won't
%% detect a very short partition. So we want to force a slightly
%% longer disconnect. Unfortunately we don't have a way to blacklist
%% individual nodes; the best we can do is turn off auto-connect
%% altogether.
disconnect(Node) ->
	application:set_env(kernel, dist_auto_connect, never),
	erlang:disconnect_node(Node),
	timer:sleep(1000),
	application:unset_env(kernel, dist_auto_connect),
	ok.

%%--------------------------------------------------------------------

%% mnesia:system_info(db_nodes) (and hence
%% rabbit_mnesia:cluster_nodes(running)) does not return all nodes
%% when partitioned, just those that we are sharing Mnesia state
%% with. So we have a small set of replacement functions
%% here. "rabbit" in a function's name implies we test if the rabbit
%% application is up, not just the node.

%% As we use these functions to decide what to do in pause_minority or
%% pause_if_all_down states, they *must* be fast, even in the case where
%% TCP connections are timing out. So that means we should be careful
%% about whether we connect to nodes which are currently disconnected.
%% majority：多数
majority() ->
	majority([]).


majority(NodesDown) ->
	%% 拿到当前集群的所有节点
	Nodes = rabbit_mnesia:cluster_nodes(all),
	%% 得到当前集群存活的所有节点
	AliveNodes = alive_nodes(Nodes) -- NodesDown,
	%% 判断当前集群存活的节点数是否超过当前集群所有的节点数一半
	length(AliveNodes) / length(Nodes) > 0.5.


%% preferred：首选
%% 如果配置文件中配置的首选节点都联系不上，则返回true，否则返回false
in_preferred_partition() ->
	{ok, {pause_if_all_down, PreferredNodes, _}} =
		application:get_env(rabbit, cluster_partition_handling),
	in_preferred_partition(PreferredNodes).


in_preferred_partition(PreferredNodes) ->
	in_preferred_partition(PreferredNodes, []).


in_preferred_partition(PreferredNodes, NodesDown) ->
	%% 拿到当前集群的所有节点
	Nodes = rabbit_mnesia:cluster_nodes(all),
	RealPreferredNodes = [N || N <- PreferredNodes, lists:member(N, Nodes)],
	AliveNodes = alive_nodes(RealPreferredNodes) -- NodesDown,
	RealPreferredNodes =:= [] orelse AliveNodes =/= [].


%% 判断集群中的所有节点是否都在运行中
all_nodes_up() ->
	Nodes = rabbit_mnesia:cluster_nodes(all),
	length(alive_nodes(Nodes)) =:= length(Nodes).


all_rabbit_nodes_up() ->
	Nodes = rabbit_mnesia:cluster_nodes(all),
	length(alive_rabbit_nodes(Nodes)) =:= length(Nodes).


%% 得到所有存活的节点
alive_nodes() -> alive_nodes(rabbit_mnesia:cluster_nodes(all)).


%% 得到所有存活的节点
alive_nodes(Nodes) -> [N || N <- Nodes, lists:member(N, [node() | nodes()])].


%% 得到集群中所有正在运行rabbit应用的节点列表
alive_rabbit_nodes() -> alive_rabbit_nodes(rabbit_mnesia:cluster_nodes(all)).


%% 得到集群中所有正在运行rabbit应用的节点列表
alive_rabbit_nodes(Nodes) ->
	[N || N <- alive_nodes(Nodes), rabbit:is_running(N)].


%% This one is allowed to connect!
%% ping通集群中所有的节点
ping_all() ->
	[net_adm:ping(N) || N <- rabbit_mnesia:cluster_nodes(all)],
	ok.
