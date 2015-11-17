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

-module(rabbit_upgrade).

-export([maybe_upgrade_mnesia/0, maybe_upgrade_local/0,
         nodes_running/1, secondary_upgrade/1]).

-include("rabbit.hrl").

-define(VERSION_FILENAME, "schema_version").
-define(LOCK_FILENAME, "schema_upgrade_lock").				%% 升级锁住文件

%% -------------------------------------------------------------------

-ifdef(use_specs).

-spec(maybe_upgrade_mnesia/0 :: () -> 'ok').
-spec(maybe_upgrade_local/0 :: () -> 'ok' |
                                     'version_not_available' |
                                     'starting_from_scratch').

-endif.

%% -------------------------------------------------------------------

%% The upgrade logic(逻辑) is quite involved(复杂难懂), due to the existence(存在) of
%% clusters.
%%
%% Firstly, we have two different types of upgrades(升级) to do: Mnesia and
%% everything else(其他的一切). Mnesia upgrades must only be done by one node in
%% the cluster (we treat(处理) a non-clustered node as a single-node
%% cluster). This is the primary upgrader. The other upgrades need to
%% be done by all nodes.(Mnesia的升级必须只在集群一个节点中进行，而其他的升级需要在集群中的所有节点执行)
%%
%% The primary upgrader has to start first (and do its Mnesia
%% upgrades). Secondary upgraders need to reset their Mnesia database
%% and then rejoin the cluster. They can't do the Mnesia upgrades as
%% well and then merge databases since the cookie for each table will
%% end up different and the merge will fail.
%%
%% This in turn means that we need to determine(确定) whether we are the
%% primary or secondary upgrader *before* Mnesia comes up. If we
%% didn't then the secondary upgrader would try to start Mnesia, and
%% either hang waiting for a node which is not yet up, or fail since
%% its schema differs from the other nodes in the cluster.
%%
%% Also, the primary upgrader needs to start Mnesia to do its
%% upgrades, but needs to forcibly(强制) load tables rather than wait for
%% them (in case it was not the last node to shut down, in which case
%% it would wait forever).
%%
%% This in turn means that maybe_upgrade_mnesia/0 has to be patched
%% into the boot process by prelaunch(发射前) before the mnesia application is
%% started. By the time Mnesia is started the upgrades have happened
%% (on the primary), or Mnesia has been reset (on the secondary) and
%% rabbit_mnesia:init_db_unchecked/2 can then make the node rejoin the cluster
%% in the normal way.
%%
%% The non-mnesia upgrades are then triggered by
%% rabbit_mnesia:init_db_unchecked/2. Of course, it's possible for a given
%% upgrade process to only require Mnesia upgrades, or only require
%% non-Mnesia upgrades. In the latter case no Mnesia resets and
%% reclusterings occur.
%%
%% The primary upgrader needs to be a disc node. Ideally(在理想的情况下) we would like
%% it to be the last disc node to shut down (since otherwise there's a
%% risk of data loss). On each node we therefore record the disc nodes
%% that were still running when we shut down. A disc node that knows
%% other nodes were up when it shut down, or a ram node, will refuse
%% to be the primary upgrader, and will thus not start when upgrades
%% are needed.
%%
%% However, this is racy if several nodes are shut down at once. Since
%% rabbit records the running nodes, and shuts down before mnesia, the
%% race manifests as all disc nodes thinking they are not the primary
%% upgrader. Therefore the user can remove the record of the last disc
%% node to shut down to get things going again. This may lose any
%% mnesia changes that happened after the node chosen as the primary
%% upgrader was shut down.

%% -------------------------------------------------------------------
%% 确保备份的发生
ensure_backup_taken() ->
	%% 确定在RabbitMQ系统数据库目录中schema_upgrade_lock文件的不存在
	case filelib:is_file(lock_filename()) of
		false -> case filelib:is_dir(backup_dir()) of
					 %% 如果升级需要回退的备份目录绝对路径不存在，则进行备份
					 false -> ok = take_backup();
					 _     -> ok
				 end;
		true  -> throw({error, previous_upgrade_failed})
	end.


%% 备份一份mnesia数据库文件到备份目录
take_backup() ->
	BackupDir = backup_dir(),
	%% (将本地节点的mnesia数据库目录备份到BackupDir目录)
	case rabbit_mnesia:copy_db(BackupDir) of
		ok         -> info("upgrades: Mnesia dir backed up to ~p~n",
						   [BackupDir]);
		{error, E} -> throw({could_not_back_up_mnesia_dir, E})
	end.


%% 确保备份目录的删除
ensure_backup_removed() ->
	case filelib:is_dir(backup_dir()) of
		%% 如果备份目录存在，则将该目录下的所有文件递归删除掉
		true -> ok = remove_backup();
		_    -> ok
	end.


%% 删除备份目录
remove_backup() ->
	ok = rabbit_file:recursive_delete([backup_dir()]),
	info("upgrades: Mnesia backup removed~n", []).


%% upgrade:升级
%% 得到Scope范围中需要升级的名字
maybe_upgrade_mnesia() ->
	%% 拿到集群所有的节点
	AllNodes = rabbit_mnesia:cluster_nodes(all),
	%% 确保RabbitMQ节点重命名的完成
	ok = rabbit_mnesia_rename:maybe_finish(AllNodes),
	%% 得到mnesia类型需要升级的列表
	case rabbit_version:upgrades_required(mnesia) of
		{error, starting_from_scratch} ->
			ok;
		{error, version_not_available} ->
			case AllNodes of
				[] -> die("Cluster upgrade needed but upgrading from "
						  "< 2.1.1.~nUnfortunately you will need to "
						  "rebuild the cluster.", []);
				_  -> ok
			end;
		{error, _} = Err ->
			throw(Err);
		{ok, []} ->
			ok;
		{ok, Upgrades} ->
			%% 确保备份的发生
			ensure_backup_taken(),
			ok = case upgrade_mode(AllNodes) of
					 %% 得到当前是最后一个磁盘节点
					 primary   -> primary_upgrade(Upgrades, AllNodes);
					 secondary -> secondary_upgrade(AllNodes)
				 end
	end.


%% 拿到当前节点升级的模式
upgrade_mode(AllNodes) ->
	%% 判断当前集群中的节点是否有正在运行的节点
	case nodes_running(AllNodes) of
		[] ->
			AfterUs = rabbit_mnesia:cluster_nodes(running) -- [node()],
			%% 根据rabbit_durable_exchange.DCD这个持久化的exchange交换机，来判断当前节点是否是磁盘节点
			case {node_type_legacy(), AfterUs} of
				{disc, []}  ->
					primary;
				{disc, _}  ->
					Filename = rabbit_node_monitor:running_nodes_filename(),
					die("Cluster upgrade needed but other disc nodes shut "
						"down after this one.~nPlease first start the last "
						"disc node to shut down.~n~nNote: if several disc "
						"nodes were shut down simultaneously they may "
						"all~nshow this message. In which case, remove "
						"the lock file on one of them and~nstart that node. "
						"The lock file on this node is:~n~n ~s ", [Filename]);
				{ram, _} ->
					die("Cluster upgrade needed but this is a ram node.~n"
						"Please first start the last disc node to shut down.",
						[])
			end;
		[Another | _] ->
			%% 拿到mnesia范围对应的有向图的出度为0的顶点
			MyVersion = rabbit_version:desired_for_scope(mnesia),
			ErrFun = fun (ClusterVersion) ->
							  %% The other node(s) are running an
							  %% unexpected version.
							  die("Cluster upgrade needed but other nodes are "
									  "running ~p~nand I want ~p",
									  [ClusterVersion, MyVersion])
					 end,
			%% 拿到其他节点上mnesia范围对应的有向图的出度为0的顶点
			case rpc:call(Another, rabbit_version, desired_for_scope,
						  [mnesia]) of
				{badrpc, {'EXIT', {undef, _}}} -> ErrFun(unknown_old_version);
				{badrpc, Reason}               -> ErrFun({unknown, Reason});
				CV                             -> case rabbit_version:matches(
														 MyVersion, CV) of
													  true  -> secondary;
													  false -> ErrFun(CV)
												  end
			end
	end.


die(Msg, Args) ->
	%% We don't throw or exit here since that gets thrown
	%% straight out into do_boot, generating an erl_crash.dump
	%% and displaying any error message in a confusing way.
	rabbit_log:error(Msg, Args),
	Str = rabbit_misc:format(
			"~n~n****~n~n" ++ Msg ++ "~n~n****~n~n~n", Args),
	io:format(Str),
	error_logger:logfile(close),
	case application:get_env(rabbit, halt_on_upgrade_failure) of
		{ok, false} -> throw({upgrade_error, Str});
		_           -> halt(1) %% i.e. true or undefined
	end.


%% 主节点的升级
primary_upgrade(Upgrades, Nodes) ->
	Others = Nodes -- [node()],
	ok = apply_upgrades(
		   mnesia,
		   Upgrades,
		   fun () ->
					%% 强制加载mnesia数据库表根据mnesia_table中定义的数据库表
					rabbit_table:force_load(),
					case Others of
						[] -> ok;
						_  -> info("mnesia upgrades: Breaking cluster~n", []),
							  %% 将其他节点上的schema表删除
							  [{atomic, ok} = mnesia:del_table_copy(schema, Node)
												|| Node <- Others]
					end
		   end),
	ok.


%% 第二节点的升级
secondary_upgrade(AllNodes) ->
	%% must do this before we wipe out schema
	%% 根据rabbit_durable_exchange.DCD这个持久化的exchange交换机，来判断当前节点是否是磁盘节点
	NodeType = node_type_legacy(),
	%% 将schema模式表删除
	rabbit_misc:ensure_ok(mnesia:delete_schema([node()]),
						  cannot_delete_schema),
	%% 确保mnesia数据库的启动
	rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
	%% 初始化当前节点的mnesia数据库
	ok = rabbit_mnesia:init_db_unchecked(AllNodes, NodeType),
	%% 将Scope中所有出度为零的顶点存入schema_version文件(即将这次的升级名字写入磁盘文件，即将Scope范围最新的出度为0的顶点写入磁盘文件)
	ok = rabbit_version:record_desired_for_scope(mnesia),
	ok.


%% 得到Nodes节点列表中rabbit应用正在运行的节点列表
nodes_running(Nodes) ->
	[N || N <- Nodes, rabbit:is_running(N)].

%% -------------------------------------------------------------------
%% 看是否需要更新本地
maybe_upgrade_local() ->
	case rabbit_version:upgrades_required(local) of
		{error, version_not_available} -> version_not_available;
		{error, starting_from_scratch} -> starting_from_scratch;
		{error, _} = Err               -> throw(Err);
		{ok, []}                       -> ensure_backup_removed(),
										  ok;
		{ok, Upgrades}                 -> mnesia:stop(),
										  ensure_backup_taken(),
										  ok = apply_upgrades(local, Upgrades,
															  fun () -> ok end),
										  ensure_backup_removed(),
										  ok
	end.

%% -------------------------------------------------------------------
%% 真实的进行升级的操作函数
apply_upgrades(Scope, Upgrades, Fun) ->
	ok = rabbit_file:lock_file(lock_filename()),
	info("~s upgrades: ~w to apply~n", [Scope, length(Upgrades)]),
	%% 将当前节点的mnesia数据库启动
	rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
	%% 强制加载mnesia数据库表根据mnesia_table中定义的数据库表
	Fun(),
	[apply_upgrade(Scope, Upgrade) || Upgrade <- Upgrades],
	info("~s upgrades: All upgrades applied successfully~n", [Scope]),
	%% 将升级的信息存储到schema_version文件中
	ok = rabbit_version:record_desired_for_scope(Scope),
	%% 删除schema_upgrade_lock文件
	ok = file:delete(lock_filename()).


apply_upgrade(Scope, {M, F}) ->
	info("~s upgrades: Applying ~w:~w~n", [Scope, M, F]),
	ok = apply(M, F, []).

%% -------------------------------------------------------------------
%% 得到mnesia数据库的目录
dir() -> rabbit_mnesia:dir().


%% 得到schema_upgrade_lock文件的绝对路径
lock_filename() -> lock_filename(dir()).
lock_filename(Dir) -> filename:join(Dir, ?LOCK_FILENAME).


%% 得到升级需要回退的备份目录绝对路径
backup_dir() -> dir() ++ "-upgrade-backup".


%% 根据rabbit_durable_exchange.DCD这个持久化的exchange交换机，来判断当前节点是否是磁盘节点
node_type_legacy() ->
	%% This is pretty ugly(丑陋) but we can't start Mnesia and ask it (will
	%% hang), we can't look at the config file (may not include us
	%% even if we're a disc node).  We also can't use
	%% rabbit_mnesia:node_type/0 because that will give false
	%% postivies on Rabbit up to 2.5.1.
	case filelib:is_regular(filename:join(dir(), "rabbit_durable_exchange.DCD")) of
		true  -> disc;
		false -> ram
	end.


info(Msg, Args) -> rabbit_log:info(Msg, Args).
