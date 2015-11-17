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

-module(rabbit_mnesia).

-export([init/0,
         join_cluster/2,
         reset/0,
         force_reset/0,
         update_cluster_nodes/1,
         change_cluster_node_type/1,
         forget_cluster_node/2,
         force_load_next_boot/0,

         status/0,
         is_clustered/0,
         on_running_node/1,
         is_process_alive/1,
         cluster_nodes/1,
         node_type/0,
         dir/0,
         cluster_status_from_mnesia/0,

         init_db_unchecked/2,
         copy_db/1,
         check_cluster_consistency/0,
         ensure_mnesia_dir/0,

         on_node_up/1,
         on_node_down/1
        ]).

%% Used internally in rpc calls
-export([node_info/0, remove_node_if_mnesia_running/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([node_type/0, cluster_status/0]).

-type(node_type() :: disc | ram).
-type(cluster_status() :: {[node()], [node()], [node()]}).

%% Main interface
-spec(init/0 :: () -> 'ok').
-spec(join_cluster/2 :: (node(), node_type())
                        -> 'ok' | {'ok', 'already_member'}).
-spec(reset/0 :: () -> 'ok').
-spec(force_reset/0 :: () -> 'ok').
-spec(update_cluster_nodes/1 :: (node()) -> 'ok').
-spec(change_cluster_node_type/1 :: (node_type()) -> 'ok').
-spec(forget_cluster_node/2 :: (node(), boolean()) -> 'ok').
-spec(force_load_next_boot/0 :: () -> 'ok').

%% Various queries to get the status of the db
-spec(status/0 :: () -> [{'nodes', [{node_type(), [node()]}]} |
                         {'running_nodes', [node()]} |
                         {'partitions', [{node(), [node()]}]}]).
-spec(is_clustered/0 :: () -> boolean()).
-spec(on_running_node/1 :: (pid()) -> boolean()).
-spec(is_process_alive/1 :: (pid()) -> boolean()).
-spec(cluster_nodes/1 :: ('all' | 'disc' | 'ram' | 'running') -> [node()]).
-spec(node_type/0 :: () -> node_type()).
-spec(dir/0 :: () -> file:filename()).
-spec(cluster_status_from_mnesia/0 :: () -> rabbit_types:ok_or_error2(
                                              cluster_status(), any())).

%% Operations on the db and utils, mainly used in `rabbit_upgrade' and `rabbit'
-spec(init_db_unchecked/2 :: ([node()], node_type()) -> 'ok').
-spec(copy_db/1 :: (file:filename()) ->  rabbit_types:ok_or_error(any())).
-spec(check_cluster_consistency/0 :: () -> 'ok').
-spec(ensure_mnesia_dir/0 :: () -> 'ok').

%% Hooks used in `rabbit_node_monitor'
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Main interface(接口)
%%----------------------------------------------------------------------------
%% RabbitMQ系统mnesia database启动步骤中执行的函数
init() ->
	%% 确保mnesia数据库的启动
	ensure_mnesia_running(),
	%% 确保mnesia数据库保存目录的存在
	ensure_mnesia_dir(),
	%% 判断当前节点是否是新启动的节点
	case is_virgin_node() of
		true  -> init_from_config();
		false -> NodeType = node_type(),
				 init_db_and_upgrade(cluster_nodes(all), NodeType,
									 NodeType =:= ram)
	end,
	%% We intuitively(直观地) expect the global name server to be synced when
	%% Mnesia is up. In fact that's not guaranteed(保证) to be the case -
	%% let's make it so.
	ok = rabbit_node_monitor:global_sync(),
	ok.


%% 新节点第一次启动调用的函数
init_from_config() ->
	%% 找出配置文件配置的集群节点不是atom的节点名字的函数
	FindBadNodeNames = fun
						  (Name, BadNames) when is_atom(Name) -> BadNames;
						  (Name, BadNames)                    -> [Name | BadNames]
					   end,
	%% 新启动的节点从配置文件中拿到集群节点列表以及自己这个节点的类型
	{TryNodes, NodeType} =
		case application:get_env(rabbit, cluster_nodes) of
			{ok, {Nodes, Type} = Config}
			  when is_list(Nodes) andalso (Type == disc orelse Type == ram) ->
				case lists:foldr(FindBadNodeNames, [], Nodes) of
					[]       -> Config;
					BadNames -> e({invalid_cluster_node_names, BadNames})
				end;
			{ok, {_, BadType}} when BadType /= disc andalso BadType /= ram ->
				e({invalid_cluster_node_type, BadType});
			{ok, Nodes} when is_list(Nodes) ->
				%% The legacy syntax (a nodes list without the node
				%% type) is unsupported.
				case lists:foldr(FindBadNodeNames, [], Nodes) of
					[] -> e(cluster_node_type_mandatory);
					_  -> e(invalid_cluster_nodes_conf)
				end;
			{ok, _} ->
				e(invalid_cluster_nodes_conf)
		end,
	%% TryNodes表示所有的集群节点，新节点启动的时候，需要尝试连接点集群中去
	case TryNodes of
		%% 如果没有需要连接的RabbitMQ集群，则将当前节点作为集群的第一个节点，该节点的节点类型以disc启动
		[] -> init_db_and_upgrade([node()], disc, false);
		%% 当前节点是第一次启动，根据配置文件需要将当前节点连接到TryNodes的RabbitMQ系统集群中
		_  -> auto_cluster(TryNodes, NodeType)
	end.


%% 当前节点是第一次启动，根据配置文件需要将当前节点连接到TryNodes的RabbitMQ系统集群中
auto_cluster(TryNodes, NodeType) ->
	case find_auto_cluster_node(nodes_excl_me(TryNodes)) of
		{ok, Node} ->
			rabbit_log:info("Node '~p' selected for auto-clustering~n", [Node]),
			%% rpc:call集群中的Node节点,拿到集群中的集群状态信息
			{ok, {_, DiscNodes, _}} = discover_cluster0(Node),
			init_db_and_upgrade(DiscNodes, NodeType, true),
			%% 通知rabbit_node_monitor进程自己加入集群
			rabbit_node_monitor:notify_joined_cluster();
		none ->
			rabbit_log:warning(
			  "Could not find any node for auto-clustering from: ~p~n"
				  "Starting blank node...~n", [TryNodes]),
			%% 如果没有成功连接到需要自动连接的RabbitMQ集群，则初始化自己为一个磁盘节点，等待其他节点的连接
			init_db_and_upgrade([node()], disc, false)
	end.

%% Make the node join a cluster. The node will be reset automatically
%% before we actually cluster it. The nodes provided will be used to
%% find out about the nodes in the cluster.
%%
%% This function will fail if:
%%
%%   * The node is currently the only disc node of its cluster
%%   * We can't connect to any of the nodes provided
%%   * The node is currently already clustered with the cluster of the nodes
%%     provided
%%
%% Note that we make no attempt to verify that the nodes provided are
%% all in the same cluster, we simply pick the first online node and
%% we cluster to its cluster.
%% DiscoveryNode表示集群node名称，--ram表示node以ram node加入集群中。默认node以disc node加入集群，在一个node加入cluster之前，必须先停止该node的rabbitmq应用，即先执行stop_app
%% 将本地节点加入到DiscoveryNode节点对应的集群中
join_cluster(DiscoveryNode, NodeType) ->
	%% 确保本地节点的mnesia数据库停止运行
	ensure_mnesia_not_running(),
	%% 确保mnesia数据库保存目录的存在
	ensure_mnesia_dir(),
	%% 判断当前节点是否是集群唯一磁盘节点
	case is_only_clustered_disc_node() of
		true  -> e(clustering_only_disc_node);
		false -> ok
	end,
	%% 拿到本地节点要加入的集群中的所有节点
	{ClusterNodes, _, _} = discover_cluster([DiscoveryNode]),
	%% 判断自己节点是否已经在集群中
	case me_in_nodes(ClusterNodes) of
		false ->
			%% 检查要加入的集群的OTP，RabbitMQ同本节点的一致性
			case check_cluster_consistency(DiscoveryNode, false) of
				{ok, _} ->
					%% reset the node. this simplifies(简化) things and it
					%% will be needed in this case - we're joining a new
					%% cluster with new nodes which are not in synch
					%% with the current node. It also lifts the burden
					%% of resetting the node from the user.
					%% 优雅的初始化节点状态，同时通知集群其他节点，自己退出集群，将当前节点从集群断开，同时将本地节点的mnesia数据库目录全部删除掉，将当前节点的集群信息重置
					reset_gracefully(),
					
					%% Join the cluster
					%% 打印日志本节点加入集群
					rabbit_log:info("Clustering with ~p as ~p node~n",
									[ClusterNodes, NodeType]),
					%% 初始化自己节点连通到ClusterNodes这个集群节点列表
					ok = init_db_with_mnesia(ClusterNodes, NodeType,
											 true, true),
					%% 通知集群中的其他节点自己加入集群
					rabbit_node_monitor:notify_joined_cluster(),
					ok;
				{error, Reason} ->
					{error, Reason}
			end;
		true ->
			rabbit_log:info("Already member of cluster: ~p~n", [ClusterNodes]),
			{ok, already_member}
	end.

%% return node to its virgin state, where it is not member of any
%% cluster, has no cluster configuration, no local database, and no
%% persisted messages
%% 初始化节点状态，会从集群中删除该节点，从管理数据库中删除所有数据，例如vhosts等等。在初始化之前rabbitmq的应用必须先停止
reset() ->
	%% 确保当前节点的mnesia数据库没有在运行中
    ensure_mnesia_not_running(),
    rabbit_log:info("Resetting Rabbit~n", []),
	%% 优雅的初始化节点状态，同时通知集群其他节点，自己退出集群，将当前节点从集群断开，同时将本地节点的mnesia数据库目录全部删除掉，将当前节点的集群信息重置
    reset_gracefully().


%% 强制初始化节点状态(暴力拆解)
force_reset() ->
	%% 确保mnesia数据库没有运行
	ensure_mnesia_not_running(),
	rabbit_log:info("Resetting Rabbit forcefully~n", []),
	%% 本地节点离开集群的后续清除工作(将当前节点从集群断开，同时将本地节点的mnesia数据库目录全部删除掉，将当前节点的集群信息重置)
	wipe().


%% 优雅的初始化节点状态，同时通知集群其他节点，自己退出集群，将当前节点从集群断开，同时将本地节点的mnesia数据库目录全部删除掉，将当前节点的集群信息重置
reset_gracefully() ->
	%% 拿到RabbitMQ系统集群的所有节点
	AllNodes = cluster_nodes(all),
	%% Reconnecting so that we will get an up to date nodes.(重连使自己节点同步成集群中一个最新的节点)  We don't
	%% need to check for consistency because we are resetting.
	%% Force=true here so that reset still works when clustered with a
	%% node which is down.
	%% 将本节点上的mnesia数据库启动，同时跟RabbitMQ集群连接上，将集群中的数据跟本节点同步
	init_db_with_mnesia(AllNodes, node_type(), false, false),
	%% 判断当前节点是否是集群唯一磁盘节点
	case is_only_clustered_disc_node() of
		true  -> e(resetting_only_disc_node);
		false -> ok
	end,
	%% 通知集群的其他节点自己离开集群
	leave_cluster(),
	%% 本节点删除schema模式表
	rabbit_misc:ensure_ok(mnesia:delete_schema([node()]), cannot_delete_schema),
	%% 本地节点离开集群的后续清除工作(将当前节点从集群断开，同时将本地节点的mnesia数据库目录全部删除掉，将当前节点的集群信息重置)
	wipe().


%% 本地节点离开集群的后续清除工作(将当前节点从集群断开，同时将本地节点的mnesia数据库目录全部删除掉，将当前节点的集群信息重置)
wipe() ->
	%% We need to make sure that we don't end up in a distributed
	%% Erlang system with nodes while not being in an Mnesia cluster
	%% with them. We don't handle that well.
	%% 同集群中的其他节点断开连接
	[erlang:disconnect_node(N) || N <- cluster_nodes(all)],
	%% remove persisted messages and any other garbage we find
	%% 删除本地节点mnesia数据库目录的所有文件
	ok = rabbit_file:recursive_delete(filelib:wildcard(dir() ++ "/*")),
	%% 重置本地节点的集群文件(将集群节点，集群磁盘节点，集群正在运行的节点全出初始化为自己节点)
	ok = rabbit_node_monitor:reset_cluster_status(),
	ok.


%% 改变本地自己节点在集群中的存储类型(先将当前节点从集群中优雅的删除掉，然后将该节点用新的存储类型加入到集群中)
change_cluster_node_type(Type) ->
	%% 确保mnesia数据库没有在运行中
	ensure_mnesia_not_running(),
	%% 确保mnesia数据库保存目录的存在
	ensure_mnesia_dir(),
	%% 确定当前自己节点是处于集群中
	case is_clustered() of
		false -> e(not_clustered);
		true  -> ok
	end,
	%% 查找集群中当前正在运行中的节点列表
	{_, _, RunningNodes} = discover_cluster(cluster_nodes(all)),
	%% We might still be marked as running by a remote node since the
	%% information of us going down might not have propagated yet.
	Node = case RunningNodes -- [node()] of
			   []        -> e(no_online_cluster_nodes);
			   [Node0|_] -> Node0
		   end,
	%% 重置节点状态，会从集群中删除该节点，从管理数据库中删除所有数据，例如vhosts等等。在初始化之前rabbitmq的应用必须先停止
	ok = reset(),
	%% 重新加入集群
	ok = join_cluster(Node, Type).


%% 根据DiscoveryNode更新当前节点的集群信息，让自己的节点重新连接一次集群
update_cluster_nodes(DiscoveryNode) ->
	%% 确保mnesia数据库没有在运行中
	ensure_mnesia_not_running(),
	%% 确保mnesia数据库保存目录的存在
	ensure_mnesia_dir(),
	%% 查找集群中当前正在运行中的节点列表
	Status = {AllNodes, _, _} = discover_cluster([DiscoveryNode]),
	case me_in_nodes(AllNodes) of
		true ->
			%% As in `check_consistency/0', we can safely delete the
			%% schema here, since it'll be replicated from the other
			%% nodes
			%% 将本地的mnesia数据库schema删除掉
			mnesia:delete_schema([node()]),
			%% 将最新的集群信息写入到本地节点的存储文件中
			rabbit_node_monitor:write_cluster_status(Status),
			%% 打印日志
			rabbit_log:info("Updating cluster nodes from ~p~n",
							[DiscoveryNode]),
			%% 初始化自己节点连通到ClusterNodes这个集群节点列表
			init_db_with_mnesia(AllNodes, node_type(), true, true);
		false ->
			e(inconsistent_cluster)
	end,
	ok.

%% We proceed like this: try to remove the node locally. If the node
%% is offline, we remove the node if:
%%   * This node is a disc node
%%   * All other nodes are offline
%%   * This node was, at the best of our knowledge (see comment below)
%%     the last or second to last after the node we're removing to go
%%     down
%% 将Node节点从RabbitMQ集群中删除掉
forget_cluster_node(Node, RemoveWhenOffline) ->
	case lists:member(Node, cluster_nodes(all)) of
		true  -> ok;
		false -> e(not_a_cluster_node)
	end,
	case {RemoveWhenOffline, is_running()} of
		%% 本节点的mnesia数据库没有在运行中，RemoveWhenOffline为true表示离线将Node节点从RabbitMQ集群中删除掉
		{true,  false} -> remove_node_offline_node(Node);
		{true,   true} -> e(online_node_offline_flag);
		{false, false} -> e(offline_node_no_offline_flag);
		%% 当前节点mnesia正在运行中,然后从集群中删除Node节点
		{false,  true} -> rabbit_log:info(
							"Removing node ~p from cluster~n", [Node]),
						  %% 当前节点的mnesia数据库正在运行中，此时将Node节点从集群中删除(该操作在本节点成功后会自动同步到集群的其他节点上)
						  case remove_node_if_mnesia_running(Node) of
							  ok               -> ok;
							  {error, _} = Err -> throw(Err)
						  end
	end.


%% 本地节点的mnesia数据库没有启动，然后来将Node节点从RabbitMQ集群中删除掉
remove_node_offline_node(Node) ->
	%% Here `mnesia:system_info(running_db_nodes)' will RPC, but that's what we
	%% want - we need to know the running nodes *now*.  If the current node is a
	%% RAM node it will return bogus(虚假) results, but we don't care since we only do
	%% this operation from disc nodes.
	case {mnesia:system_info(running_db_nodes) -- [Node], node_type()} of
		{[], disc} ->
			%% 在当前节点启动mnesia数据库
			start_mnesia(),
			try
				%% What we want to do here is replace the last node to
				%% go down with the current node.  The way we do this
				%% is by force loading the table, and making sure that
				%% they are loaded.
				%% 强制加载mnesia数据库表根据mnesia_table中定义的数据库表
				rabbit_table:force_load(),
				%% replicated(复制)(集群中的所有节点等待所有的表复制同步成功)(等待在当前节点上有磁盘副本(disc_copies)的表和cluster完成同步)
				rabbit_table:wait_for_replicated(),
				%% 在线将Node节点从RabbitMQ集群中删除掉
				forget_cluster_node(Node, false),
				%% 生成force_load文件
				force_load_next_boot()
			after
				stop_mnesia()
			end;
		{_, _} ->
			e(removing_node_from_offline_node)
	end.

%%----------------------------------------------------------------------------
%% Queries
%%----------------------------------------------------------------------------
%% 得到RabbitMQ集群的信息(得到集群磁盘，内存节点，如果访问的节点mnesia数据库正在运行，则还返回集群中正在运行的节点，集群的名字，集群中的分区情况)
status() ->
	IfNonEmpty = fun (_,       []) -> [];
					(Type, Nodes) -> [{Type, Nodes}]
				 end,
	%% 得到集群的磁盘节点和ram内存节点和集群正在运行的节点列表和集群的名字
	[{nodes, (IfNonEmpty(disc, cluster_nodes(disc)) ++
				  IfNonEmpty(ram, cluster_nodes(ram)))}] ++
		case is_running() of
			true  -> RunningNodes = cluster_nodes(running),
					 [{running_nodes, RunningNodes},
					  {cluster_name,  rabbit_nodes:cluster_name()},
					  {partitions,    mnesia_partitions(RunningNodes)}];
			false -> []
		end.


mnesia_partitions(Nodes) ->
	Replies = rabbit_node_monitor:partitions(Nodes),
	[Reply || Reply = {_, R} <- Replies, R =/= []].


%% 判断当前mnesia数据库是否在运行中
is_running() -> mnesia:system_info(is_running) =:= yes.


%% 判断当前RabbitMQ系统是否是集群化
is_clustered() -> AllNodes = cluster_nodes(all),
				  AllNodes =/= [] andalso AllNodes =/= [node()].


%% 判断Pid对应的节点是否正在运行中
on_running_node(Pid) -> lists:member(node(Pid), cluster_nodes(running)).

%% This requires the process be in the same running cluster as us
%% (i.e. not partitioned or some random node).
%%
%% See also rabbit_misc:is_process_alive/1 which does not.
is_process_alive(Pid) ->
	on_running_node(Pid) andalso
		rpc:call(node(Pid), erlang, is_process_alive, [Pid]) =:= true.


%% 根据WhichNodes这个类型得到不同的节点列表(如果当前系统mnesia数据正在运行，则从mnesia数据库中取对应的节点列表，如果mnesia没有启动，则从磁盘文件中读取对应的节点列表)
cluster_nodes(WhichNodes) -> cluster_status(WhichNodes).

%% This function is the actual source of information, since it gets
%% the data from mnesia. Obviously it'll work only when mnesia is
%% running.
%% 这个函数是信息的实际来源，因为它从函数mnesia获取数据。显然，它会工作只有在Mnesia正在运行。
%% 如果mnesia数据库已经启动则从mneisa数据库获得当前集群的信息
cluster_status_from_mnesia() ->
	case is_running() of
		false ->
			{error, mnesia_not_running};
		true ->
			%% If the tables are not present, it means that
			%% `init_db/3' hasn't been run yet. In other words, either
			%% we are a virgin node or a restarted RAM node. In both
			%% cases we're not interested in what mnesia has to say.
			NodeType = case mnesia:system_info(use_dir) of
						   true  -> disc;
						   false -> ram
					   end,
			%% 判断当前系统中的mnesia数据库表和定义的表是否完全相同
			case rabbit_table:is_present() of
				true  -> AllNodes = mnesia:system_info(db_nodes),
						 %% 拿到磁盘节点列表
						 DiscCopies = mnesia:table_info(schema, disc_copies),
						 DiscNodes = case NodeType of
										 disc -> nodes_incl_me(DiscCopies);
										 ram  -> DiscCopies
									 end,
						 %% `mnesia:system_info(running_db_nodes)' is safe since
						 %% we know that mnesia is running
						 %% 拿到当前正在运行中的集群节点列表
						 RunningNodes = mnesia:system_info(running_db_nodes),
						 {ok, {AllNodes, DiscNodes, RunningNodes}};
				false -> {error, tables_not_present}
			end
	end.


%% 集群的状态(根据不同的WhichNodes标签获得不同的信息)
cluster_status(WhichNodes) ->
	{AllNodes, DiscNodes, RunningNodes} = Nodes =
											  case cluster_status_from_mnesia() of
												  {ok, Nodes0} ->
													  Nodes0;
												  {error, _Reason} ->
													  %% 当mnesia数据库尚未启动的时候，则从磁盘文件中读取出当前集群中所有的节点，磁盘节点，和当前集群正在运行的节点列表
													  {AllNodes0, DiscNodes0, RunningNodes0} =
														  rabbit_node_monitor:read_cluster_status(),
													  %% The cluster status file records the status when the node is
													  %% online, but we know for sure that the node is offline now, so
													  %% we can remove it from the list of running nodes.
													  {AllNodes0, DiscNodes0, nodes_excl_me(RunningNodes0)}
											  end,
	%% 根据WhichNodes这个类型得到不同的节点列表
	case WhichNodes of
		status  -> Nodes;
		all     -> AllNodes;
		disc    -> DiscNodes;
		ram     -> AllNodes -- DiscNodes;
		running -> RunningNodes
	end.


%% 得到节点当前的OTP版本号，RabbitMQ系统的版本号，集群状态信息
node_info() ->
	{rabbit_misc:otp_release(), rabbit_misc:version(),
	 cluster_status_from_mnesia()}.


%% 从集群状态文件中拿到节点mnesia数据库表的存储类型
node_type() ->
	{_AllNodes, DiscNodes, _RunningNodes} =
		rabbit_node_monitor:read_cluster_status(),
	case DiscNodes =:= [] orelse me_in_nodes(DiscNodes) of
		true  -> disc;
		false -> ram
	end.


%% 得到mnesia数据库的目录
dir() -> mnesia:system_info(directory).

%%----------------------------------------------------------------------------
%% Operations on the db
%%----------------------------------------------------------------------------
%% Adds the provided nodes to the mnesia cluster, creating a new
%% schema if there is the need to and catching up if there are other
%% nodes in the cluster already. It also updates the cluster status
%% file.
%% 首先mnesia尝试连接到集群ClusterNodes的所有节点
%% (1).如果成功连接的集群节点为空且本节点通过mnesia:system_info(use_dir)得到自己不是disc磁盘节点，但是NodeType确实要初始化为disc磁盘节点，则该节点是要把ram内存节点转化为disc磁盘节点
%%	   集群的第一个节点也会走到这种情况
%% (2).如果成功连接的集群节点为空且本节点通过mnesia:system_info(use_dir)得到自己是disc磁盘节点，且NodeType要初始化为disc磁盘节点，则当前节点是第一个磁盘节点的启动
%% (3).如果成功连接到集群中的节点，则等待在当前节点上有磁盘副本(disc_copies)的表和cluster完成同步，然后完成同步后需要在本节点创建mnesia数据库表的拷贝
%% 然后检查自己节点的mnesia数据库表和mnesia_table模块中定义的mnesia数据表是否一致
%% 更新集群节点的状态到集群状态文件中
init_db(ClusterNodes, NodeType, CheckOtherNodes) ->
	%% 强制同集群节点ClusterNodes中的节点建立连接(尝试连接到系统的其他节点上)
	Nodes = change_extra_db_nodes(ClusterNodes, CheckOtherNodes),
	%% Note that we use `system_info' here and not the cluster status
	%% since when we start rabbit for the first time the cluster
	%% status will say we are a disc node but the tables won't be
	%% present yet.
	%% 该参数可返回一个布尔值，表示Mnesia目录是否使用与否。可以调用即使Mnesia的尚未运行，得到disk表示是磁盘节点，ram表示内存节点
	WasDiscNode = mnesia:system_info(use_dir),
	case {Nodes, WasDiscNode, NodeType} of
		{[], _, ram} ->
			%% Standalone ram node, we don't want that
			throw({error, cannot_create_standalone_ram_node});
		{[], false, disc} ->
			%% RAM -> disc, starting from scratch(ram内存节点转化为disc磁盘节点，或者集群中的第一个节点启动的时候会走到此处)
			%% 或者新磁盘节点开始启动会掉到此处创建mnesia模式表等相关操作(同时创建RabbitMQ系统中的mnesia数据库的表)
			ok = create_schema();
		{[], true, disc} ->
			%% First disc node up(集群中第一个磁盘节点的启动)
			maybe_force_load(),
			ok;
		{[_ | _], _, _} ->
			%% Subsequent(随后) node in cluster, catch up(赶上)
			maybe_force_load(),
			%% 集群中的所有节点等待所有的表复制同步成功(等待在当前节点上有磁盘副本(disc_copies)的表和cluster完成同步)
			ok = rabbit_table:wait_for_replicated(),
			%% 完成同步后需要在本节点创建mnesia数据库表的拷贝
			ok = rabbit_table:create_local_copy(NodeType)
	end,
	%% 保证mnesia数据库表的完整性
	ensure_schema_integrity(),
	%% 更新集群节点的状态到集群状态文件中(将最新的集群所有节点，集群磁盘节点，集群正在运行的节点信息写入本地节点磁盘文件)
	rabbit_node_monitor:update_cluster_status(),
	ok.


init_db_unchecked(ClusterNodes, NodeType) ->
	init_db(ClusterNodes, NodeType, false).


%% 初始化数据库同时升级
init_db_and_upgrade(ClusterNodes, NodeType, CheckOtherNodes) ->
	%% 初始化本地mnesia数据库(将本节点加入集群中，同时将集群中的mnesia数据库数据同步到本地节点上)
	ok = init_db(ClusterNodes, NodeType, CheckOtherNodes),
	%% 看是否需要更新本地
	ok = case rabbit_upgrade:maybe_upgrade_local() of
			 ok                    -> ok;
			 starting_from_scratch -> rabbit_version:record_desired();
			 version_not_available -> schema_ok_or_move()
		 end,
	%% `maybe_upgrade_local' restarts mnesia, so ram nodes will forget
	%% about the cluster
	%% maybe_upgrade_local操作需要重启mnesia数据库，因此ram类型的节点可能从集群中丢失，因此ram类型的节点需要重新连接到集群中
	case NodeType of
		ram  -> start_mnesia(),
				%% ram类型的节点重新连接到RabbitMQ集群中
				change_extra_db_nodes(ClusterNodes, false);
		disc -> ok
	end,
	%% ...and all nodes will need to wait for tables
	%% 集群中的所有节点等待所有的表复制同步成功(该节点成功启动加入到RabbitMQ集群后，集群中的所有节点重新一起等待RabbitMQ系统中的mnesia数据库表的可用)
	rabbit_table:wait_for_replicated(),
	ok.


%% 初始化自己节点连通到ClusterNodes这个集群节点列表
init_db_with_mnesia(ClusterNodes, NodeType,
					CheckOtherNodes, CheckConsistency) ->
	%% 启动本地的mnesia数据库
	start_mnesia(CheckConsistency),
	try
		init_db_and_upgrade(ClusterNodes, NodeType, CheckOtherNodes)
	after
		stop_mnesia()
	end.


%% 确保mnesia数据库保存目录的存在
ensure_mnesia_dir() ->
	MnesiaDir = dir() ++ "/",
	%% 确保mnesia数据库保存文件目录的存在,如果不存在则自动创建
	case filelib:ensure_dir(MnesiaDir) of
		{error, Reason} ->
			throw({error, {cannot_create_mnesia_dir, MnesiaDir, Reason}});
		ok ->
			ok
	end.


%% 确认mnesia数据库已经在运行中的状态
ensure_mnesia_running() ->
	case mnesia:system_info(is_running) of
		yes ->
			ok;
		starting ->
			wait_for(mnesia_running),
			ensure_mnesia_running();
		Reason when Reason =:= no; Reason =:= stopping ->
			throw({error, mnesia_not_running})
	end.


%% 确保当前节点的mnesia数据库没有在运行中
ensure_mnesia_not_running() ->
	case mnesia:system_info(is_running) of
		no ->
			ok;
		stopping ->
			wait_for(mnesia_not_running),
			ensure_mnesia_not_running();
		Reason when Reason =:= yes; Reason =:= starting ->
			throw({error, mnesia_unexpectedly_running})
	end.


%% 确保mnesia数据库表的完整 integrity(完整)
ensure_schema_integrity() ->
	case rabbit_table:check_schema_integrity() of
		ok ->
			ok;
		{error, Reason} ->
			throw({error, {schema_integrity_check_failed, Reason}})
	end.


%% 拷贝mnesia数据库文件备份(recursive:递归)(将本地节点的mnesia数据库目录备份到Destination目录)
copy_db(Destination) ->
	%% 确保当前节点Mnesia数据库没有启动
	ok = ensure_mnesia_not_running(),
	rabbit_file:recursive_copy(dir(), Destination).


%% 得到强制加载的文件名字
force_load_filename() ->
	filename:join(dir(), "force_load").


%% 生成force_load文件
force_load_next_boot() ->
	rabbit_file:write_file(force_load_filename(), <<"">>).


%% 看看是否需要强制创建mnesia数据库表
maybe_force_load() ->
	case rabbit_file:is_file(force_load_filename()) of
		true  -> rabbit_table:force_load(),
				 %% 删除force_load磁盘文件
				 rabbit_file:delete(force_load_filename());
		false -> ok
	end.


%% This does not guarantee(保证) us much, but it avoids some situations that
%% will definitely(明确的) end up badly
%% consistency：一致性
check_cluster_consistency() ->
	%% We want to find 0 or 1 consistent nodes.
	case lists:foldl(
		   %% 检查集群OTP，RabbitMQ，Node节点的集群的一致性
		   fun (Node,  {error, _})    -> check_cluster_consistency(Node, true);
			  (_Node, {ok, Status})  -> {ok, Status}
		   end, {error, not_found}, nodes_excl_me(cluster_nodes(all)))
		of
		{ok, Status = {RemoteAllNodes, _, _}} ->
			case ordsets:is_subset(ordsets:from_list(cluster_nodes(all)),
								   ordsets:from_list(RemoteAllNodes)) of
				true  ->
					ok;
				false ->
					%% We delete the schema here since we think we are
					%% clustered with nodes that are no longer in the
					%% cluster and there is no other way to remove
					%% them from our schema. On the other hand, we are
					%% sure that there is another online node that we
					%% can use to sync the tables with. There is a
					%% race here: if between this check and the
					%% `init_db' invocation the cluster gets
					%% disbanded, we're left with a node with no
					%% mnesia data that will try to connect to offline
					%% nodes.
					mnesia:delete_schema([node()])
			end,
			rabbit_node_monitor:write_cluster_status(Status);
		%% 如果没有集群节点，则不检查一致性
		{error, not_found} ->
			ok;
		{error, _} = E ->
			throw(E)
	end.


%% 检查集群OTP，RabbitMQ，Node节点的集群的一致性
check_cluster_consistency(Node, CheckNodesConsistency) ->
	case rpc:call(Node, rabbit_mnesia, node_info, []) of
		{badrpc, _Reason} ->
			{error, not_found};
		{_OTP, _Rabbit, {error, _}} ->
			{error, not_found};
		{OTP, Rabbit, {ok, Status}} when CheckNodesConsistency ->
			%% 检查OTP，RabbitMQ，集群状态信息的一致性
			case check_consistency(OTP, Rabbit, Node, Status) of
				{error, _} = E -> E;
				{ok, Res}      -> {ok, Res}
			end;
		{OTP, Rabbit, {ok, Status}} ->
			case check_consistency(OTP, Rabbit) of
				{error, _} = E -> E;
				ok             -> {ok, Status}
			end;
		{_OTP, Rabbit, _Hash, _Status} ->
			%% delegate hash checking implies version mismatch
			version_error("Rabbit", rabbit_misc:version(), Rabbit)
	end.

%%--------------------------------------------------------------------
%% Hooks for `rabbit_node_monitor'
%%--------------------------------------------------------------------
on_node_up(Node) ->
	case running_disc_nodes() of
		[Node] -> rabbit_log:info("cluster contains disc nodes again~n");
		_      -> ok
	end.


on_node_down(_Node) ->
	case running_disc_nodes() of
		[] -> rabbit_log:info("only running disc node went down~n");
		_  -> ok
	end.


running_disc_nodes() ->
	{_AllNodes, DiscNodes, RunningNodes} = cluster_status(status),
	ordsets:to_list(ordsets:intersection(ordsets:from_list(DiscNodes),
										 ordsets:from_list(RunningNodes))).

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------
%% 查找RabbitMQ集群
discover_cluster(Nodes) ->
	case lists:foldl(fun (_,    {ok, Res}) -> {ok, Res};
						(Node, _)         -> discover_cluster0(Node)
					 end, {error, no_nodes_provided}, Nodes) of
		{ok, Res}        -> Res;
		{error, E}       -> throw({error, E});
		{badrpc, Reason} -> throw({badrpc_multi, Reason, Nodes})
	end.


%% rpc:call集群中的Node节点,拿到集群中的集群状态信息
discover_cluster0(Node) when Node == node() ->
	{error, cannot_cluster_node_with_itself};

discover_cluster0(Node) ->
	rpc:call(Node, rabbit_mnesia, cluster_status_from_mnesia, []).


schema_ok_or_move() ->
	case rabbit_table:check_schema_integrity() of
		ok ->
			ok;
		{error, Reason} ->
			%% NB: we cannot use rabbit_log here since it may not have been
			%% started yet
			rabbit_log:warning("schema integrity check failed: ~p~n"
								   "moving database to backup location "
									   "and recreating schema from scratch~n",
									   [Reason]),
			ok = move_db(),
			ok = create_schema()
	end.


%% We only care about disc nodes since ram nodes are supposed to catch
%% up only
%% disc磁盘节点创建mnesia模式表
create_schema() ->
	%% 停止mnesia数据库
	stop_mnesia(),
	%% 创建mnesia数据库的模式表
	rabbit_misc:ensure_ok(mnesia:create_schema([node()]), cannot_create_schema),
	%% 启动mnesia数据库
	start_mnesia(),
	%% 创建RabbitMQ系统中的mnesia数据库的表
	ok = rabbit_table:create(),
	%% 确保mnesia数据库表的完整
	ensure_schema_integrity(),
	%% 创建版本升级的记录到schema_version文件中
	%% 磁盘节点第一次启动的时候创建mnesia数据库表的时候会调用该函数(将mnesia，local有向图中的出度为0的Step列表存储到schema_version文件中，供以后RabbitMQ系统升级使用)
	ok = rabbit_version:record_desired().


move_db() ->
	stop_mnesia(),
	MnesiaDir = filename:dirname(dir() ++ "/"),
	{{Year, Month, Day}, {Hour, Minute, Second}} = erlang:universaltime(),
	BackupDir = rabbit_misc:format(
				  "~s_~w~2..0w~2..0w~2..0w~2..0w~2..0w",
				  [MnesiaDir, Year, Month, Day, Hour, Minute, Second]),
	case file:rename(MnesiaDir, BackupDir) of
		ok ->
			%% NB: we cannot use rabbit_log here since it may not have
			%% been started yet
			rabbit_log:warning("moved database from ~s to ~s~n",
							   [MnesiaDir, BackupDir]),
			ok;
		{error, Reason} -> throw({error, {cannot_backup_mnesia,
										  MnesiaDir, BackupDir, Reason}})
	end,
	ensure_mnesia_dir(),
	start_mnesia(),
	ok.


%% 将Node节点从RabbitMQ集群中删除掉(该操作在本节点成功后会自动同步到集群的其他节点上)
remove_node_if_mnesia_running(Node) ->
	case is_running() of
		false ->
			{error, mnesia_not_running};
		true ->
			%% Deleting the the schema copy of the node will result in
			%% the node being removed from the cluster, with that
			%% change being propagated(传播) to all nodes
			%% 删除Node节点的模式表schema将导致Node节点将会被移除掉集群，同时这个改变将会被传播到集群的其他节点
			case mnesia:del_table_copy(schema, Node) of
				{atomic, ok} ->
					%% 将Node节点上所有的持久化队列全部删除掉
					rabbit_amqqueue:forget_all_durable(Node),
					%% 将Node节点离开集群的信息通知给集群中正在运行的所有节点的rabbit_node_monitor进程，将该节点从所有正在运行的节点中删除掉
					rabbit_node_monitor:notify_left_cluster(Node),
					ok;
				{aborted, Reason} ->
					{error, {failed_to_remove_node, Node, Reason}}
			end
	end.


%% 通知集群的其他节点自己离开集群
leave_cluster() ->
	case nodes_excl_me(cluster_nodes(all)) of
		[]       -> ok;
		AllNodes -> case lists:any(fun leave_cluster/1, AllNodes) of
						true  -> ok;
						false -> e(no_running_cluster_nodes)
					end
	end.


%% 通知Node节点本节点离开集群
leave_cluster(Node) ->
	case rpc:call(Node,
				  rabbit_mnesia, remove_node_if_mnesia_running, [node()]) of
		ok                          -> true;
		{error, mnesia_not_running} -> false;
		{error, Reason}             -> throw({error, Reason});
		{badrpc, nodedown}          -> false
	end.


%% 等待1秒钟的时间
wait_for(Condition) ->
	rabbit_log:info("Waiting for ~p...~n", [Condition]),
	timer:sleep(1000).


%% 启动mnesia数据库
start_mnesia(CheckConsistency) ->
	case CheckConsistency of
		true  -> check_cluster_consistency();
		false -> ok
	end,
	%% 确保mnesia数据库start的执行成功
	rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
	%% 确认mnesia数据库已经在运行中的状态
	ensure_mnesia_running().


%% 启动mnesia数据库
start_mnesia() ->
	start_mnesia(true).


%% 停止mnesia数据库
stop_mnesia() ->
    stopped = mnesia:stop(),
    ensure_mnesia_not_running().


%% 通知ClusterNodes0节点列表, 有一个新的节点要加入进来
change_extra_db_nodes(ClusterNodes0, CheckOtherNodes) ->
	ClusterNodes = nodes_excl_me(ClusterNodes0),
	%% 强制同集群节点ClusterNodes0中的节点建立连接(给'extra_db_node'赋值并且强制建立一个连接)
	%% 参数extra_db_nodes包含一个节点list，Mnesia除了schema里的节点，还要和该参数的节点建立联系
	case {mnesia:change_config(extra_db_nodes, ClusterNodes), ClusterNodes} of
		{{ok, []}, [_ | _]} when CheckOtherNodes ->
			throw({error, {failed_to_cluster_with, ClusterNodes,
						   "Mnesia could not connect to any nodes."}});
		{{ok, Nodes}, _} ->
			Nodes
	end.


%% 检查OTP，RabbitMQ的一致性
check_consistency(OTP, Rabbit) ->
	rabbit_misc:sequence_error(
	  [check_otp_consistency(OTP),
	   check_rabbit_consistency(Rabbit)]).


%% 检查OTP，RabbitMQ，集群节点的一致性
check_consistency(OTP, Rabbit, Node, Status) ->
	rabbit_misc:sequence_error(
	  [check_otp_consistency(OTP),
	   check_rabbit_consistency(Rabbit),
	   check_nodes_consistency(Node, Status)]).


%% 检查集群节点列表的一致性(即判断集群中的节点是否在集群的这个节点的所有节点列表中)
check_nodes_consistency(Node, RemoteStatus = {RemoteAllNodes, _, _}) ->
	case me_in_nodes(RemoteAllNodes) of
		true ->
			{ok, RemoteStatus};
		false ->
			%% inconsistent:不符
			{error, {inconsistent_cluster,
					 rabbit_misc:format("Node ~p thinks it's clustered "
											"with node ~p, but ~p disagrees",
											[node(), Node, Node])}}
	end.


%% 检查版本的一致性
check_version_consistency(This, Remote, Name) ->
	check_version_consistency(This, Remote, Name, fun (A, B) -> A =:= B end).


check_version_consistency(This, Remote, Name, Comp) ->
	case Comp(This, Remote) of
		true  -> ok;
		false -> version_error(Name, This, Remote)
	end.


%% 版本错误处理函数
version_error(Name, This, Remote) ->
	{error, {inconsistent_cluster,
			 rabbit_misc:format("~s version mismatch: local node is ~s, "
									"remote node ~s", [Name, This, Remote])}}.


%% 检查OTP版本的一致性
check_otp_consistency(Remote) ->
	check_version_consistency(rabbit_misc:otp_release(), Remote, "OTP").


%% 检查rabbitmq系统版本的一致性
check_rabbit_consistency(Remote) ->
	check_version_consistency(
	  rabbit_misc:version(), Remote, "Rabbit",
	  fun rabbit_misc:version_minor_equivalent/2).

%% This is fairly tricky.  We want to know if the node is in the state
%% that a `reset' would leave it in.  We cannot simply check if the
%% mnesia tables aren't there because restarted RAM nodes won't have
%% tables while still being non-virgin.  What we do instead is to
%% check if the mnesia directory is non existant or empty, with the
%% exception of the cluster status files, which will be there thanks to
%% `rabbit_node_monitor:prepare_cluster_status_file/0'.
%% virgin：处女
%% 判断当前节点是否是新启动的节点
is_virgin_node() ->
	case rabbit_file:list_dir(dir()) of
		{error, enoent} ->
			true;
		{ok, []} ->
			true;
		{ok, [File1, File2]} ->
			%% 如果节点是第一次启动，则启动到此处的时候，mnesia数据库目录应该只存在两个文件，即集群状态文件和当前集群正在运行的节点保存文件
			%% 如果mnesia数据库目录下的两个文件和这两个文件的名字一样，则表明当前节点是第一次启动
			lists:usort([dir() ++ "/" ++ File1, dir() ++ "/" ++ File2]) =:=
				lists:usort([rabbit_node_monitor:cluster_status_filename(),
							 rabbit_node_monitor:running_nodes_filename()]);
		{ok, _} ->
			false
	end.


%% 找到需要自动连接到集群的集群节点
find_auto_cluster_node([]) ->
	none;
find_auto_cluster_node([Node | Nodes]) ->
	Fail = fun (Fmt, Args) ->
					rabbit_log:warning(
					  "Could not auto-cluster with ~s: " ++ Fmt, [Node | Args]),
					find_auto_cluster_node(Nodes)
		   end,
	case rpc:call(Node, rabbit_mnesia, node_info, []) of
		{badrpc, _} = Reason         -> Fail("~p~n", [Reason]);
		%% old delegate hash check
		{_OTP, RMQ, _Hash, _}        -> Fail("version ~s~n", [RMQ]);
		{_OTP, _RMQ, {error, _} = E} -> Fail("~p~n", [E]);
		{OTP, RMQ, _}                -> case check_consistency(OTP, RMQ) of
											{error, _} -> Fail("versions ~p~n",
															   [{OTP, RMQ}]);
											ok         -> {ok, Node}
										end
	end.


%% 判断当前节点是否是集群唯一磁盘节点
is_only_clustered_disc_node() ->
	node_type() =:= disc andalso is_clustered() andalso
		cluster_nodes(disc) =:= [node()].


%% Nodes节点列表清除掉自己的节点名
me_in_nodes(Nodes) -> lists:member(node(), Nodes).


%% Nodes节点列表加上自己的节点名
nodes_incl_me(Nodes) -> lists:usort([node() | Nodes]).


%% 节点列表去除自己当前节点
nodes_excl_me(Nodes) -> Nodes -- [node()].


%% 根据不同的错误表示，抛出不同的错误信息
e(Tag) -> throw({error, {Tag, error_description(Tag)}}).


error_description({invalid_cluster_node_names, BadNames}) ->
	"In the 'cluster_nodes' configuration key, the following node names "
		"are invalid: " ++ lists:flatten(io_lib:format("~p", [BadNames]));
error_description({invalid_cluster_node_type, BadType}) ->
	"In the 'cluster_nodes' configuration key, the node type is invalid "
		"(expected 'disc' or 'ram'): " ++
			lists:flatten(io_lib:format("~p", [BadType]));
error_description(cluster_node_type_mandatory) ->
	"The 'cluster_nodes' configuration key must indicate the node type: "
		"either {[...], disc} or {[...], ram}";
error_description(invalid_cluster_nodes_conf) ->
	"The 'cluster_nodes' configuration key is invalid, it must be of the "
		"form {[Nodes], Type}, where Nodes is a list of node names and "
			"Type is either 'disc' or 'ram'";
error_description(clustering_only_disc_node) ->
	"You cannot cluster a node if it is the only disc node in its existing "
		" cluster. If new nodes joined while this node was offline, use "
			"'update_cluster_nodes' to add them manually.";
error_description(resetting_only_disc_node) ->
	"You cannot reset a node when it is the only disc node in a cluster. "
		"Please convert another node of the cluster to a disc node first.";
error_description(not_clustered) ->
	"Non-clustered nodes can only be disc nodes.";
error_description(no_online_cluster_nodes) ->
	"Could not find any online cluster nodes. If the cluster has changed, "
		"you can use the 'update_cluster_nodes' command.";
error_description(inconsistent_cluster) ->
	"The nodes provided do not have this node as part of the cluster.";
error_description(not_a_cluster_node) ->
	"The node selected is not in the cluster.";
error_description(online_node_offline_flag) ->
	"You set the --offline flag, which is used to remove nodes remotely from "
		"offline nodes, but this node is online.";
error_description(offline_node_no_offline_flag) ->
	"You are trying to remove a node from an offline node. That is dangerous, "
		"but can be done with the --offline flag. Please consult the manual "
			"for rabbitmqctl for more information.";
error_description(removing_node_from_offline_node) ->
	"To remove a node remotely from an offline node, the node you are removing "
		"from must be a disc node and all the other nodes must be offline.";
error_description(no_running_cluster_nodes) ->
	"You cannot leave a cluster if no online nodes are present.".
