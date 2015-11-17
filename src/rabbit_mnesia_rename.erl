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

-module(rabbit_mnesia_rename).
-include("rabbit.hrl").

-export([rename/2]).
-export([maybe_finish/1]).

-define(CONVERT_TABLES, [schema, rabbit_durable_queue]).

%% Supports renaming the nodes in the Mnesia database. In order to do
%% this, we take a backup of the database(我们需要对数据库进行备份), traverse(通过) the backup
%% changing node names and pids as we go, then restore it.
%%
%% That's enough for a standalone(独立) node, for clusters the story is more
%% complex. We can take pairs of nodes From and To, but backing up and
%% restoring the database changes schema cookies, so if we just do
%% this on all nodes the cluster will refuse to re-form with
%% "Incompatible schema cookies.". Therefore we do something similar
%% to what we do for upgrades - the first node in the cluster to
%% restart becomes the authority(权威), and other nodes wipe their own
%% Mnesia state and rejoin. They also need to tell Mnesia the old node
%% is not coming back.
%%
%% If we are renaming nodes one at a time then the running cluster
%% might not be aware that a rename has taken place, so after we wipe
%% and rejoin we then update any tables (in practice just
%% rabbit_durable_queue) which should be aware that we have changed.

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(rename/2 :: (node(), [{node(), node()}]) -> 'ok').
-spec(maybe_finish/1 :: ([node()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% 给RabbitMQ集群节点重命名
rename(Node, NodeMapList) ->
	try
		%% Check everything is correct and figure out what we are
		%% changing from and to.
		{FromNode, ToNode, NodeMap} = prepare(Node, NodeMapList),
		
		%% We backup and restore(恢复) Mnesia even if(即使) other nodes are
		%% running at the time, and defer(推迟) the final decision about
		%% whether to use our mutated(突变) copy or rejoin the cluster until
		%% we restart. That means we might be mutating(变化) our copy of the
		%% database while the cluster is running. *Do not* contact the
		%% cluster while this is happening, we are likely to get
		%% confused.
		application:set_env(kernel, dist_auto_connect, never),
		
		%% Take a copy we can restore from if we abandon(放弃) the
		%% rename. We don't restore from the "backup" since restoring
		%% that changes schema cookies and might stop us rejoining the
		%% cluster.
		%% 备份Node节点的数据库目录到rabbit_mnesia:dir() ++ "-rename" ++ "/mnesia-copy"目录下
		ok = rabbit_mnesia:copy_db(mnesia_copy_dir()),
		
		%% And make the actual(实际的) changes
		rabbit_control_main:become(FromNode),
		%% mnesia数据库执行一次完整的备份(备份Node节点的数据库目录到rabbit_mnesia:dir() ++ "-rename" ++ "/backup-before"文件)
		take_backup(before_backup_name()),
		%% 再备份一次mnesia数据库(备份Node节点的数据库目录到rabbit_mnesia:dir() ++ "-rename" ++ "/backup-after"文件)
		convert_backup(NodeMap, before_backup_name(), after_backup_name()),
		ok = rabbit_file:write_term_file(rename_config_name(),
										 [{FromNode, ToNode}]),
		%% 更新本节点集群文件中的新节点名字(将旧的节点名字替换为新的节点名字)
		convert_config_files(NodeMap),
		rabbit_control_main:become(ToNode),
		%% 让ToNode节点从/backup-after目录的备份恢复Mnesia数据库
		restore_backup(after_backup_name()),
		ok
	after
		stop_mnesia()
	end.


%% 做节点更换名字的准备工作
prepare(Node, NodeMapList) ->
	%% If we have a previous rename and haven't started since, give up.
	%% 确保mnesia数据库目录名 + -rename的目录是否存在(保证当前没有重命名动作的存在)
	case rabbit_file:is_dir(dir()) of
		true  -> exit({rename_in_progress,
					   "Restart node under old name to roll back"});
		false -> ok = rabbit_file:ensure_dir(mnesia_copy_dir())			%% 确保mnesia数据库目录 + /mnesia-copy的目录的存在
	end,
	
	%% Check we don't have two nodes mapped to the same node
	{FromNodes, ToNodes} = lists:unzip(NodeMapList),
	%% 检查是否有两个节点重命名为同一个名字
	case length(FromNodes) - length(lists:usort(ToNodes)) of
		0 -> ok;
		_ -> exit({duplicate_node, ToNodes})
	end,
	
	%% Figure out which node we are before and after the change(Figure out:弄清楚)
	FromNode = case [From || {From, To} <- NodeMapList,
							 To =:= Node] of
				   [N] -> N;
				   []  -> Node
			   end,
	NodeMap = dict:from_list(NodeMapList),
	ToNode = case dict:find(FromNode, NodeMap) of
				 {ok, N2} -> N2;
				 error    -> FromNode
			 end,
	
	%% Check that we are in the cluster, all old nodes are in the
	%% cluster, and no new nodes are.
	Nodes = rabbit_mnesia:cluster_nodes(all),
	case {FromNodes -- Nodes, ToNodes -- (ToNodes -- Nodes),
		  lists:member(Node, Nodes ++ ToNodes)} of
		{[], [], true}  -> ok;
		{[], [], false} -> exit({i_am_not_involved,        Node});
		{F,  [], _}     -> exit({nodes_not_in_cluster,     F});
		{_,  T,  _}     -> exit({nodes_already_in_cluster, T})
	end,
	{FromNode, ToNode, NodeMap}.


%% 启动Mnesia数据库进行备份操作，将Mnesia数据备份到Backup目录
take_backup(Backup) ->
	start_mnesia(),
	%% 这个函数激活一个覆盖全部 Mnesia 表的新检查点并且执行一次备份。
	ok = mnesia:backup(Backup),
	stop_mnesia().


%% 让本节点从Backup备份恢复mnesia数据库
restore_backup(Backup) ->
	%% 这个函数能够配置成从一个现存的备份重启 Mnesia并且重新加载数据库表以及可能的模式表。当数据或模式表损毁时，此函数被用于灾难恢复。
	ok = mnesia:install_fallback(Backup, [{scope, local}]),
	start_mnesia(),
	stop_mnesia(),
	rabbit_mnesia:force_load_next_boot().


%% 判断RabbitMQ集群中的节点重新命名是否完成
maybe_finish(AllNodes) ->
	case rabbit_file:read_term_file(rename_config_name()) of
		{ok, [{FromNode, ToNode}]} -> finish(FromNode, ToNode, AllNodes);
		_                          -> ok
	end.


finish(FromNode, ToNode, AllNodes) ->
	case node() of
		%% 如果当前节点是重新命名的节点
		ToNode ->
			%% 得到AllNodes节点列表中rabbit应用正在运行的节点列表
			case rabbit_upgrade:nodes_running(AllNodes) of
				[] -> finish_primary(FromNode, ToNode);
				_  -> finish_secondary(FromNode, ToNode, AllNodes)
			end;
		FromNode ->
			%% Abandoning:放弃
			%% 如果当前节点依然是更改名字之前的节点名，则放弃当前的重新命名操作
			rabbit_log:info(
			  "Abandoning rename from ~s to ~s since we are still ~s~n",
			  [FromNode, ToNode, FromNode]),
			[{ok, _} = file:copy(backup_of_conf(F), F) || F <- config_files()],
			ok = rabbit_file:recursive_delete([rabbit_mnesia:dir()]),
			ok = rabbit_file:recursive_copy(
				   mnesia_copy_dir(), rabbit_mnesia:dir()),
			delete_rename_files();
		_ ->
			%% Boot will almost certainly fail but we might as
			%% well just log this
			rabbit_log:info(
			  "Rename attempted from ~s to ~s but we are ~s - ignoring.~n",
			  [FromNode, ToNode, node()])
	end.


finish_primary(FromNode, ToNode) ->
	rabbit_log:info("Restarting as primary after rename from ~s to ~s~n",
					[FromNode, ToNode]),
	delete_rename_files(),
	ok.


finish_secondary(FromNode, ToNode, AllNodes) ->
	rabbit_log:info("Restarting as secondary after rename from ~s to ~s~n",
					[FromNode, ToNode]),
	rabbit_upgrade:secondary_upgrade(AllNodes),
	rename_in_running_mnesia(FromNode, ToNode),
	delete_rename_files(),
	ok.


%% RabbitMQ节点重新命名的相关目录取得接口
dir()                -> rabbit_mnesia:dir() ++ "-rename".
before_backup_name() -> dir() ++ "/backup-before".
after_backup_name()  -> dir() ++ "/backup-after".
rename_config_name() -> dir() ++ "/pending.config".
mnesia_copy_dir()    -> dir() ++ "/mnesia-copy".


%% 删除掉节点重命名目录
delete_rename_files() -> ok = rabbit_file:recursive_delete([dir()]).


start_mnesia() -> rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
				  rabbit_table:force_load(),
				  rabbit_table:wait_for_replicated().


stop_mnesia()  -> stopped = mnesia:stop().


convert_backup(NodeMap, FromBackup, ToBackup) ->
	%% 这个函数能用来读存在的备份，从一个现存的备份创建一个新的备份，或者在不同介质之间拷贝备份。
	mnesia:traverse_backup(
	  FromBackup, ToBackup,
	  fun
		 (Row, Acc) ->
			  case lists:member(element(1, Row), ?CONVERT_TABLES) of
				  true  -> {[update_term(NodeMap, Row)], Acc};
				  false -> {[Row], Acc}
			  end
      end, switched).


%% 得到存储集群信息的本地文件路径
config_files() ->
	[rabbit_node_monitor:running_nodes_filename(),
	 rabbit_node_monitor:cluster_status_filename()].


backup_of_conf(Path) ->
	filename:join([dir(), filename:basename(Path)]).


%% 根据NodeMap中旧节点名对应的新的节点名，将集群存储文件中的全部更改
convert_config_files(NodeMap) ->
	[convert_config_file(NodeMap, Path) || Path <- config_files()].


convert_config_file(NodeMap, Path) ->
	{ok, Term} = rabbit_file:read_term_file(Path),
	{ok, _} = file:copy(Path, backup_of_conf(Path)),
	ok = rabbit_file:write_term_file(Path, update_term(NodeMap, Term)).


lookup_node(OldNode, NodeMap) ->
	case dict:find(OldNode, NodeMap) of
		{ok, NewNode} -> NewNode;
		error         -> OldNode
	end.


mini_map(FromNode, ToNode) -> dict:from_list([{FromNode, ToNode}]).


%% 更新L中的字段
update_term(NodeMap, L) when is_list(L) ->
	[update_term(NodeMap, I) || I <- L];
update_term(NodeMap, T) when is_tuple(T) ->
	list_to_tuple(update_term(NodeMap, tuple_to_list(T)));
update_term(NodeMap, Node) when is_atom(Node) ->
	lookup_node(Node, NodeMap);
update_term(NodeMap, Pid) when is_pid(Pid) ->
	rabbit_misc:pid_change_node(Pid, lookup_node(node(Pid), NodeMap));
update_term(_NodeMap, Term) ->
	Term.


rename_in_running_mnesia(FromNode, ToNode) ->
	All = rabbit_mnesia:cluster_nodes(all),
	Running = rabbit_mnesia:cluster_nodes(running),
	case {lists:member(FromNode, Running), lists:member(ToNode, All)} of
		{false, true}  -> ok;
		{true,  _}     -> exit({old_node_running,        FromNode});
		{_,     false} -> exit({new_node_not_in_cluster, ToNode})
	end,
	{atomic, ok} = mnesia:del_table_copy(schema, FromNode),
	Map = mini_map(FromNode, ToNode),
	{atomic, _} = transform_table(rabbit_durable_queue, Map),
	ok.


transform_table(Table, Map) ->
	mnesia:sync_transaction(
	  fun () ->
			   mnesia:lock({table, Table}, write),
			   transform_table(Table, Map, mnesia:first(Table))
	  end).


transform_table(_Table, _Map, '$end_of_table') ->
	ok;
transform_table(Table, Map, Key) ->
	[Term] = mnesia:read(Table, Key, write),
	ok = mnesia:write(Table, update_term(Map, Term), write),
	transform_table(Table, Map, mnesia:next(Table, Key)).
