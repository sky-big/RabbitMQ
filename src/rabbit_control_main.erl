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

-module(rabbit_control_main).
-include("rabbit.hrl").
-include("rabbit_cli.hrl").

-export([start/0, stop/0, parse_arguments/2, action/5,
         sync_queue/1, cancel_sync_queue/1, become/1]).

-import(rabbit_cli, [rpc_call/4]).

-define(EXTERNAL_CHECK_INTERVAL, 1000).

-define(GLOBAL_DEFS(Node), [?QUIET_DEF, ?NODE_DEF(Node)]).				%% 全局的客户端参数列表

%% RabbitMQ系统支持的所有命令
-define(COMMANDS,
		[stop,															%% 将Node节点上所有RabbitMQ启动的应用关闭，同时让该节点的Erlang虚拟机停止运行
		 stop_app,														%% 将Node节点上所有RabbitMQ启动的应用关闭，但是该节点的Erlang虚拟机任然在运行，mnesia数据库，分布式服务都是可用的
		 start_app,														%% Node节点启动RabbitMQ系统所有相关应用
		 wait,
		 reset,															%% 将Node节点从集群中删除该节点，从管理数据库中删除所有数据，例如vhosts等等。
																		%% 在初始化之前rabbitmq的应用必须先停止(比force_reset命令优雅，它会通知集群中的其他节点)
		 force_reset,													%% 强制将Node节点的所有数据重置，将Node节点从集群中退出，然后将该Node节点对应的所有存储文件删除
		 rotate_logs,
		 
		 {join_cluster, [?RAM_DEF]},
		 change_cluster_node_type,
		 update_cluster_nodes,
		 {forget_cluster_node, [?OFFLINE_DEF]},
		 rename_cluster_node,
		 force_boot,
		 cluster_status,
		 {sync_queue, [?VHOST_DEF]},
		 {cancel_sync_queue, [?VHOST_DEF]},
		 
		 add_user,
		 delete_user,
		 change_password,
		 clear_password,
		 set_user_tags,
		 list_users,
		 
		 add_vhost,
		 delete_vhost,
		 list_vhosts,
		 {set_permissions, [?VHOST_DEF]},
		 {clear_permissions, [?VHOST_DEF]},
		 {list_permissions, [?VHOST_DEF]},
		 list_user_permissions,
		 
		 {set_parameter, [?VHOST_DEF]},
		 {clear_parameter, [?VHOST_DEF]},
		 {list_parameters, [?VHOST_DEF]},
		 
		 {set_policy, [?VHOST_DEF, ?PRIORITY_DEF, ?APPLY_TO_DEF]},
		 {clear_policy, [?VHOST_DEF]},
		 {list_policies, [?VHOST_DEF]},
		 
		 {list_queues, [?VHOST_DEF]},
		 {list_exchanges, [?VHOST_DEF]},
		 {list_bindings, [?VHOST_DEF]},
		 {list_connections, [?VHOST_DEF]},
		 list_channels,
		 {list_consumers, [?VHOST_DEF]},
		 status,
		 environment,
		 report,
		 set_cluster_name,
		 eval,
		 
		 close_connection,
		 {trace_on, [?VHOST_DEF]},
		 {trace_off, [?VHOST_DEF]},
		 set_vm_memory_high_watermark
		]).


%% 连接相关的信息获取定义
-define(GLOBAL_QUERIES,
		[{"Connections", rabbit_networking, connection_info_all,
		  connection_info_keys},
		 {"Channels",  rabbit_channel,  info_all, info_keys}]).


%% VHost下所有详细信息获取定义
-define(VHOST_QUERIES,
		[{"Queues",    rabbit_amqqueue, info_all, info_keys},
		 {"Exchanges", rabbit_exchange, info_all, info_keys},
		 {"Bindings",  rabbit_binding,  info_all, info_keys},
		 {"Consumers", rabbit_amqqueue, consumers_all, consumer_info_keys},
		 {"Permissions", rabbit_auth_backend_internal, list_vhost_permissions,
		  vhost_perms_info_keys},
		 {"Policies",   rabbit_policy,             list_formatted, info_keys},
		 {"Parameters", rabbit_runtime_parameters, list_formatted, info_keys}]).


%% 次列表中的命令是不需要rabbit应用启动
-define(COMMANDS_NOT_REQUIRING_APP,
		[stop, stop_app, start_app, wait, reset, force_reset, rotate_logs,
		 join_cluster, change_cluster_node_type, update_cluster_nodes,
		 forget_cluster_node, rename_cluster_node, cluster_status, status,
		 environment, eval, force_boot]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
-spec(action/5 ::
        (atom(), node(), [string()], [{string(), any()}],
         fun ((string(), [any()]) -> 'ok'))
        -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% RabbitMQ系统命令执行入口
start() ->
	rabbit_cli:main(
	  %% 解析命令行函数
	  fun (Args, NodeStr) ->
			   parse_arguments(Args, NodeStr)
	  end,
	  %% 实际的执行命令的函数
	  fun (Command, Node, Args, Opts) ->
			   %% 得到是否需要进行打印的标识
			   Quiet = proplists:get_bool(?QUIET_OPT, Opts),
			   Inform = case Quiet of
							true  -> fun (_Format, _Args1) -> ok end;
							false -> fun (Format, Args1) ->
											  io:format(Format ++ " ...~n", Args1)
									 end
						end,
			   do_action(Command, Node, Args, Opts, Inform)
	  end, rabbit_ctl_usage).


%% 解析命令参数
parse_arguments(CmdLine, NodeStr) ->
	rabbit_cli:parse_arguments(
	  ?COMMANDS, ?GLOBAL_DEFS(NodeStr), ?NODE_OPT, CmdLine).


print_report(Node, {Descr, Module, InfoFun, KeysFun}) ->
    io:format("~s:~n", [Descr]),
    print_report0(Node, {Module, InfoFun, KeysFun}, []).

print_report(Node, {Descr, Module, InfoFun, KeysFun}, VHostArg) ->
    io:format("~s on ~s:~n", [Descr, VHostArg]),
    print_report0(Node, {Module, InfoFun, KeysFun}, VHostArg).

print_report0(Node, {Module, InfoFun, KeysFun}, VHostArg) ->
    case rpc_call(Node, Module, InfoFun, VHostArg) of
        [_|_] = Results -> InfoItems = rpc_call(Node, Module, KeysFun, []),
                           display_row([atom_to_list(I) || I <- InfoItems]),
                           display_info_list(Results, InfoItems);
        _               -> ok
    end,
    io:nl().

stop() ->
    ok.

%%----------------------------------------------------------------------------
%% 实际执行命令行的函数接口
do_action(Command, Node, Args, Opts, Inform) ->
	%% 判断Command命令是否需要Node节点上的rabbit进程正在运行中
	case lists:member(Command, ?COMMANDS_NOT_REQUIRING_APP) of
		%% 确保该节点的rabbit应用已经启动
		false -> case ensure_app_running(Node) of
					 ok -> action(Command, Node, Args, Opts, Inform);
					 E  -> E
				 end;
		true  -> action(Command, Node, Args, Opts, Inform)
	end.


%% 将Node节点上所有RabbitMQ启动的应用关闭，同时让该节点的Erlang虚拟机停止运行
action(stop, Node, Args, _Opts, Inform) ->
	Inform("Stopping and halting node ~p", [Node]),
	Res = call(Node, {rabbit, stop_and_halt, []}),
	case {Res, Args} of
		{ok, [PidFile]} -> wait_for_process_death(
							 read_pid_file(PidFile, false));
		{ok, [_, _| _]} -> exit({badarg, Args});
		_               -> ok
	end,
	Res;


%% 将Node节点上所有RabbitMQ启动的应用关闭，但是该节点的Erlang虚拟机任然在运行，mnesia数据库，分布式服务都是可用的
action(stop_app, Node, [], _Opts, Inform) ->
	Inform("Stopping node ~p", [Node]),
	call(Node, {rabbit, stop, []});


%% Node节点启动RabbitMQ系统所有相关应用
action(start_app, Node, [], _Opts, Inform) ->
	Inform("Starting node ~p", [Node]),
	call(Node, {rabbit, start, []});


%% 将Node节点从集群中删除该节点，从管理数据库中删除所有数据，例如vhosts等等。在初始化之前rabbitmq的应用必须先停止(比force_reset命令优雅，它会通知集群中的其他节点)
%% 优雅的初始化节点状态，同时通知集群其他节点，自己退出集群，将当前节点从集群断开，同时将本地节点的mnesia数据库目录全部删除掉，将当前节点的集群信息重置
action(reset, Node, [], _Opts, Inform) ->
	Inform("Resetting node ~p", [Node]),
	call(Node, {rabbit_mnesia, reset, []});


%% 强制将Node节点的所有数据重置，将Node节点从集群中退出，然后将该Node节点对应的所有存储文件删除(不会通知集群中的其他节点Node节点离开集群)
action(force_reset, Node, [], _Opts, Inform) ->
	Inform("Forcefully resetting node ~p", [Node]),
	call(Node, {rabbit_mnesia, force_reset, []});


%% 加入集群的命令(clusternode表示node名称，--ram表示node以ram node加入集群中。默认node以disc node加入集群，在一个node加入cluster之前，必须先停止该node的rabbitmq应用，即先执行stop_app)
%% 将Node节点加入到ClusterNodeS对应的RabbitMQ集群中
action(join_cluster, Node, [ClusterNodeS], Opts, Inform) ->
	ClusterNode = list_to_atom(ClusterNodeS),
	NodeType = case proplists:get_bool(?RAM_OPT, Opts) of
				   true  -> ram;
				   false -> disc
			   end,
	Inform("Clustering node ~p with ~p", [Node, ClusterNode]),
	rpc_call(Node, rabbit_mnesia, join_cluster, [ClusterNode, NodeType]);


%% 改变Node节点在集群中的存储类型为ram内存类型(先将当前节点从集群中优雅的删除掉，然后将该节点用新的存储类型加入到集群中)
action(change_cluster_node_type, Node, ["ram"], _Opts, Inform) ->
	Inform("Turning ~p into a ram node", [Node]),
	rpc_call(Node, rabbit_mnesia, change_cluster_node_type, [ram]);


%% 改变Node节点在集群中的存储类型为磁盘类型(先将当前节点从集群中优雅的删除掉，然后将该节点用新的存储类型加入到集群中)
action(change_cluster_node_type, Node, [Type], _Opts, Inform)
  when Type =:= "disc" orelse Type =:= "disk" ->
	Inform("Turning ~p into a disc node", [Node]),
	rpc_call(Node, rabbit_mnesia, change_cluster_node_type, [disc]);


%% 根据ClusterNodeS更新Node节点的集群信息，让Node节点重新连接一次ClusterNodeS对应的RabbitMQ集群
action(update_cluster_nodes, Node, [ClusterNodeS], _Opts, Inform) ->
	ClusterNode = list_to_atom(ClusterNodeS),
	Inform("Updating cluster nodes for ~p from ~p", [Node, ClusterNode]),
	rpc_call(Node, rabbit_mnesia, update_cluster_nodes, [ClusterNode]);


%% 远程移除cluster中的一个node，前提是该node必须处于offline状态，如果是online状态，则需要加--offline参数
action(forget_cluster_node, Node, [ClusterNodeS], Opts, Inform) ->
	ClusterNode = list_to_atom(ClusterNodeS),
	RemoveWhenOffline = proplists:get_bool(?OFFLINE_OPT, Opts),
	Inform("Removing node ~p from cluster", [ClusterNode]),
	case RemoveWhenOffline of
		%% 离线将ClusterNode节点从Node节点对应的集群中删除掉
		true  -> become(Node),
				 rabbit_mnesia:forget_cluster_node(ClusterNode, true);
		%% 在线将ClusterNode节点从Node节点对应的集群中删除掉
		false -> rpc_call(Node, rabbit_mnesia, forget_cluster_node,
						  [ClusterNode, false])
	end;


%% 给集群节点重命名(支持群集节点重命名在本地数据库,Node节点必须停止运行)
action(rename_cluster_node, Node, NodesS, _Opts, Inform) ->
	%% 将节点列表生成{FromNode， ToNode}格式
	Nodes = split_list([list_to_atom(N) || N <- NodesS]),
	Inform("Renaming cluster nodes:~n~s~n",
		   [lists:flatten([rabbit_misc:format("  ~s -> ~s~n", [F, T]) ||
							 {F, T} <- Nodes])]),
	rabbit_mnesia_rename:rename(Node, Nodes);


%% 设置让Node节点启动的时候强制性的将Node节点上的mnesia数据库数据加载到mnesia数据库
action(force_boot, Node, [], _Opts, Inform) ->
	Inform("Forcing boot for Mnesia dir ~s", [mnesia:system_info(directory)]),
	case rabbit:is_running(Node) of
		false -> rabbit_mnesia:force_load_next_boot();
		true  -> {error, rabbit_running}
	end;


%% 同步QName对应的镜像队列
action(sync_queue, Node, [Q], Opts, Inform) ->
	VHost = proplists:get_value(?VHOST_OPT, Opts),
	QName = rabbit_misc:r(list_to_binary(VHost), queue, list_to_binary(Q)),
	Inform("Synchronising ~s", [rabbit_misc:rs(QName)]),
	rpc_call(Node, rabbit_control_main, sync_queue, [QName]);


%% 取消同步QName对应的镜像队列
action(cancel_sync_queue, Node, [Q], Opts, Inform) ->
	VHost = proplists:get_value(?VHOST_OPT, Opts),
	QName = rabbit_misc:r(list_to_binary(VHost), queue, list_to_binary(Q)),
	Inform("Stopping synchronising ~s", [rabbit_misc:rs(QName)]),
	rpc_call(Node, rabbit_control_main, cancel_sync_queue, [QName]);


%% 等待Node节点上的rabbit应用以及配置的插件应用的启动
action(wait, Node, [PidFile], _Opts, Inform) ->
	Inform("Waiting for ~p", [Node]),
	wait_for_application(Node, PidFile, rabbit_and_plugins, Inform);


%% 等待Node节点上的App应用是否启动
action(wait, Node, [PidFile, App], _Opts, Inform) ->
	Inform("Waiting for ~p on ~p", [App, Node]),
	wait_for_application(Node, PidFile, list_to_atom(App), Inform);


%% 得到Node节点上RabbitMQ系统的状态
action(status, Node, [], _Opts, Inform) ->
	Inform("Status of node ~p", [Node]),
	display_call_result(Node, {rabbit, status, []});


%% Node节点对应的RabbitMQ集群的状态(得到集群磁盘，内存节点，如果访问的节点mnesia数据库正在运行，则还返回集群中正在运行的节点，集群的名字，集群中的分区情况)
action(cluster_status, Node, [], _Opts, Inform) ->
	Inform("Cluster status of node ~p", [Node]),
	display_call_result(Node, {rabbit_mnesia, status, []});


%% 得到Node节点上所有应用的环境配置参数
action(environment, Node, _App, _Opts, Inform) ->
	Inform("Application environment of node ~p", [Node]),
	display_call_result(Node, {rabbit, environment, []});


%% 将Node节点日志轮转
action(rotate_logs, Node, [], _Opts, Inform) ->
	Inform("Reopening logs for node ~p", [Node]),
	call(Node, {rabbit, rotate_logs, [""]});


%% 将Node节点的日志轮转(即将日志写入Suffix文件里)
action(rotate_logs, Node, Args = [Suffix], _Opts, Inform) ->
	Inform("Rotating logs to files with suffix \"~s\"", [Suffix]),
	call(Node, {rabbit, rotate_logs, Args});


%% 将Node节点上pid为PidStr的rabbit_reader进程停止掉(即将关闭Node节点上的连接进程)
action(close_connection, Node, [PidStr, Explanation], _Opts, Inform) ->
	Inform("Closing connection \"~s\"", [PidStr]),
	rpc_call(Node, rabbit_networking, close_connection,
			 [rabbit_misc:string_to_pid(PidStr), Explanation]);


%% 向Node节点上增加Username的用户(即向rabbit_user表增加internal_user数据结构)
action(add_user, Node, Args = [Username, _Password], _Opts, Inform) ->
	Inform("Creating user \"~s\"", [Username]),
	call(Node, {rabbit_auth_backend_internal, add_user, Args});


%% 从Node节点上将用户_Username删除掉(即将rabbit_user表中的internal_user删除，以及将rabbit_user_permission表中的user_permission数据结构删除掉)
action(delete_user, Node, Args = [_Username], _Opts, Inform) ->
	Inform("Deleting user \"~s\"", Args),
	call(Node, {rabbit_auth_backend_internal, delete_user, Args});


%% 从Node节点上修改用户Username的密码
action(change_password, Node, Args = [Username, _Newpassword], _Opts, Inform) ->
	Inform("Changing password for user \"~s\"", [Username]),
	call(Node, {rabbit_auth_backend_internal, change_password, Args});


%% 从Node节点上将用户Username的密码清除掉
action(clear_password, Node, Args = [Username], _Opts, Inform) ->
	Inform("Clearing password for user \"~s\"", [Username]),
	call(Node, {rabbit_auth_backend_internal, clear_password, Args});


%% 从Node节点上设置用户Username的标识为TagsStr
action(set_user_tags, Node, [Username | TagsStr], _Opts, Inform) ->
	Tags = [list_to_atom(T) || T <- TagsStr],
	Inform("Setting tags for user \"~s\" to ~p", [Username, Tags]),
	rpc_call(Node, rabbit_auth_backend_internal, set_tags,
			 [list_to_binary(Username), Tags]);


%% 从Node节点上列出当前集群所有的用户
action(list_users, Node, [], _Opts, Inform) ->
	Inform("Listing users", []),
	display_info_list(
	  call(Node, {rabbit_auth_backend_internal, list_users, []}),
	  rabbit_auth_backend_internal:user_info_keys());


%% 从Node节点上增加VHost(即向rabbit_vhost表添加vhost数据结构)
action(add_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
	Inform("Creating vhost \"~s\"", Args),
	call(Node, {rabbit_vhost, add, Args});


%% 从Node节点上删除VHost(即向rabbit_vhost表删除vhost数据结构)
action(delete_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
	Inform("Deleting vhost \"~s\"", Args),
	call(Node, {rabbit_vhost, delete, Args});


%% 从Node节点上列出集群中的所有VHost(Args列表表示感兴趣的key列表)
action(list_vhosts, Node, Args, _Opts, Inform) ->
	Inform("Listing vhosts", []),
	ArgAtoms = default_if_empty(Args, [name]),
	display_info_list(call(Node, {rabbit_vhost, info_all, []}), ArgAtoms);


%% 从Node节点上列出_Username用户的所有权限
action(list_user_permissions, Node, Args = [_Username], _Opts, Inform) ->
	Inform("Listing permissions for user ~p", Args),
	display_info_list(call(Node, {rabbit_auth_backend_internal,
								  list_user_permissions, Args}),
					  rabbit_auth_backend_internal:user_perms_info_keys());


%% 从Node节点上列出列出集群所有的队列信息(Args列表表示感兴趣的key列表，-p参数必须配置VHost名字)
action(list_queues, Node, Args, Opts, Inform) ->
	Inform("Listing queues", []),
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	ArgAtoms = default_if_empty(Args, [name, messages]),
	display_info_list(rpc_call(Node, rabbit_amqqueue, info_all,
							   [VHostArg, ArgAtoms]),
					  ArgAtoms);


%% 从Node节点上列出集群所有的交换机exchange信息(Args列表表示感兴趣的key列表，-p参数必须配置VHost名字)
action(list_exchanges, Node, Args, Opts, Inform) ->
	Inform("Listing exchanges", []),
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	ArgAtoms = default_if_empty(Args, [name, type]),
	display_info_list(rpc_call(Node, rabbit_exchange, info_all,
							   [VHostArg, ArgAtoms]),
					  ArgAtoms);


%% 从Node节点上列出集群所有的bindings信息(Args列表表示感兴趣的key列表，-p参数必须配置VHost名字)
action(list_bindings, Node, Args, Opts, Inform) ->
	Inform("Listing bindings", []),
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	ArgAtoms = default_if_empty(Args, [source_name, source_kind,
									   destination_name, destination_kind,
									   routing_key, arguments]),
	display_info_list(rpc_call(Node, rabbit_binding, info_all,
							   [VHostArg, ArgAtoms]),
					  ArgAtoms);


%% 从Node节点上列出当前集群的所有连接信息
action(list_connections, Node, Args, _Opts, Inform) ->
	Inform("Listing connections", []),
	ArgAtoms = default_if_empty(Args, [user, peer_host, peer_port, state]),
	display_info_list(rpc_call(Node, rabbit_networking, connection_info_all,
							   [ArgAtoms]),
					  ArgAtoms);


%% 从Node节点上列出当前集群的所有rabbit_channel进程中的信息
action(list_channels, Node, Args, _Opts, Inform) ->
	Inform("Listing channels", []),
	ArgAtoms = default_if_empty(Args, [pid, user, consumer_count,
									   messages_unacknowledged]),
	display_info_list(rpc_call(Node, rabbit_channel, info_all, [ArgAtoms]),
					  ArgAtoms);


%% 从Node节点上列出当前集群所有的消费者信息(-p参数必须配置VHost名字)
action(list_consumers, Node, _Args, Opts, Inform) ->
	Inform("Listing consumers", []),
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	display_info_list(rpc_call(Node, rabbit_amqqueue, consumers_all, [VHostArg]),
					  rabbit_amqqueue:consumer_info_keys());


%% 打开RabbitMQ系统的性能跟踪功能
action(trace_on, Node, [], Opts, Inform) ->
	VHost = proplists:get_value(?VHOST_OPT, Opts),
	Inform("Starting tracing for vhost \"~s\"", [VHost]),
	rpc_call(Node, rabbit_trace, start, [list_to_binary(VHost)]);


%% 关闭RabbitMQ系统的性能跟踪功能
action(trace_off, Node, [], Opts, Inform) ->
	VHost = proplists:get_value(?VHOST_OPT, Opts),
	Inform("Stopping tracing for vhost \"~s\"", [VHost]),
	rpc_call(Node, rabbit_trace, stop, [list_to_binary(VHost)]);


%% 设置Node节点上%%vm_memory_monitor进程设置的能够使用虚拟机最大内存的百分比
action(set_vm_memory_high_watermark, Node, [Arg], _Opts, Inform) ->
	Frac = list_to_float(case string:chr(Arg, $.) of
							 0 -> Arg ++ ".0";
							 _ -> Arg
						 end),
	Inform("Setting memory threshold on ~p to ~p", [Node, Frac]),
	rpc_call(Node, vm_memory_monitor, set_vm_memory_high_watermark, [Frac]);


%% 从Node节点上设置Username用户配置，读，写的权限
action(set_permissions, Node, [Username, CPerm, WPerm, RPerm], Opts, Inform) ->
	VHost = proplists:get_value(?VHOST_OPT, Opts),
	Inform("Setting permissions for user \"~s\" in vhost \"~s\"",
		   [Username, VHost]),
	call(Node, {rabbit_auth_backend_internal, set_permissions,
				[Username, VHost, CPerm, WPerm, RPerm]});


%% 从Node节点上清除Username用户的全部权限
action(clear_permissions, Node, [Username], Opts, Inform) ->
	VHost = proplists:get_value(?VHOST_OPT, Opts),
	Inform("Clearing permissions for user \"~s\" in vhost \"~s\"",
		   [Username, VHost]),
	call(Node, {rabbit_auth_backend_internal, clear_permissions,
				[Username, VHost]});


%% 从Node节点上列出当前集群中所有用户的权限
action(list_permissions, Node, [], Opts, Inform) ->
	VHost = proplists:get_value(?VHOST_OPT, Opts),
	Inform("Listing permissions in vhost \"~s\"", [VHost]),
	display_info_list(call(Node, {rabbit_auth_backend_internal,
								  list_vhost_permissions, [VHost]}),
					  rabbit_auth_backend_internal:vhost_perms_info_keys());


%% 从Node节点向rabbit_runtime_parameters模块插入参数
action(set_parameter, Node, [Component, Key, Value], Opts, Inform) ->
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	Inform("Setting runtime parameter ~p for component ~p to ~p",
		   [Key, Component, Value]),
	rpc_call(
	  Node, rabbit_runtime_parameters, parse_set,
	  [VHostArg, list_to_binary(Component), list_to_binary(Key), Value, none]);


%% 从Node节点向rabbit_runtime_parameters模块清除Component, Key对应的参数
action(clear_parameter, Node, [Component, Key], Opts, Inform) ->
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	Inform("Clearing runtime parameter ~p for component ~p", [Key, Component]),
	rpc_call(Node, rabbit_runtime_parameters, clear, [VHostArg,
													  list_to_binary(Component),
													  list_to_binary(Key)]);


%% 从Node节点向rabbit_runtime_parameters模块列出当前所有的参数
action(list_parameters, Node, [], Opts, Inform) ->
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	Inform("Listing runtime parameters", []),
	display_info_list(
	  rpc_call(Node, rabbit_runtime_parameters, list_formatted, [VHostArg]),
	  rabbit_runtime_parameters:info_keys());


%% 从Node节点向rabbit_policy模块设置策略参数
action(set_policy, Node, [Key, Pattern, Defn], Opts, Inform) ->
	Msg = "Setting policy ~p for pattern ~p to ~p with priority ~p",
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	%% 优先级，数字越大优先级越高，默认为0
	PriorityArg = proplists:get_value(?PRIORITY_OPT, Opts),
	%% policy的类型，有三种：queues exchanges all 默认为all
	ApplyToArg = list_to_binary(proplists:get_value(?APPLY_TO_OPT, Opts)),
	Inform(Msg, [Key, Pattern, Defn, PriorityArg]),
	rpc_call(
	  Node, rabbit_policy, parse_set,
	  [VHostArg, list_to_binary(Key), Pattern, Defn, PriorityArg, ApplyToArg]);


%% 从Node节点向rabbit_policy模块清除策略参数
action(clear_policy, Node, [Key], Opts, Inform) ->
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	Inform("Clearing policy ~p", [Key]),
	rpc_call(Node, rabbit_policy, delete, [VHostArg, list_to_binary(Key)]);


%% 从Node节点向rabbit_policy模块列出当前所有的策略参数
action(list_policies, Node, [], Opts, Inform) ->
	VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
	Inform("Listing policies", []),
	display_info_list(rpc_call(Node, rabbit_policy, list_formatted, [VHostArg]),
					  rabbit_policy:info_keys());


%% 从Node节点报告当前集群所有的相关详细信息
action(report, Node, _Args, _Opts, Inform) ->
	Inform("Reporting server status on ~p~n~n", [erlang:universaltime()]),
	%% 从Node节点上拿到所有的正在运行的所有集群节点，然后返回每个正在运行中的节点中的status, cluster_status, environment这个三个命令运行的结果
	[begin ok = action(Action, N, [], [], Inform), io:nl() end ||
	 N      <- unsafe_rpc(Node, rabbit_mnesia, cluster_nodes, [running]),
	 Action <- [status, cluster_status, environment]],
	VHosts = unsafe_rpc(Node, rabbit_vhost, list, []),
	%% 返回所有的Connections和Channels进程中的状态
	[print_report(Node, Q)      || Q <- ?GLOBAL_QUERIES],
	%% 列出每个VHost路径下的所有详细信息(包括队列，exchange，bindings，user，policy，rabbit_runtime_parameters等信息)
	[print_report(Node, Q, [V]) || Q <- ?VHOST_QUERIES, V <- VHosts],
	ok;


%% 从Node节点设置集群的名字
action(set_cluster_name, Node, [Name], _Opts, Inform) ->
	Inform("Setting cluster name to ~s", [Name]),
	rpc_call(Node, rabbit_nodes, set_cluster_name, [list_to_binary(Name)]);


action(eval, Node, [Expr], _Opts, _Inform) ->
	case erl_scan:string(Expr) of
		{ok, Scanned, _} ->
			case erl_parse:parse_exprs(Scanned) of
				{ok, Parsed} -> {value, Value, _} =
									unsafe_rpc(
									  Node, erl_eval, exprs, [Parsed, []]),
								io:format("~p~n", [Value]),
								ok;
				{error, E}   -> {error_string, format_parse_error(E)}
			end;
		{error, E, _} ->
			{error_string, format_parse_error(E)}
	end.


format_parse_error({_Line, Mod, Err}) -> lists:flatten(Mod:format_error(Err)).


%% 同步Q对应的镜像队列
sync_queue(Q) ->
	rabbit_amqqueue:with(
	  Q, fun(#amqqueue{pid = QPid}) -> rabbit_amqqueue:sync_mirrors(QPid) end).


%% 取消同步Q对应的镜像队列
cancel_sync_queue(Q) ->
	rabbit_amqqueue:with(
	  Q, fun(#amqqueue{pid = QPid}) ->
				 rabbit_amqqueue:cancel_sync_mirrors(QPid)
	  end).

%%----------------------------------------------------------------------------
%% 等待Node节点上的Application应用的启动
wait_for_application(Node, PidFile, Application, Inform) ->
	Pid = read_pid_file(PidFile, true),
	Inform("pid is ~s", [Pid]),
	wait_for_application(Node, Pid, Application).


%% 判断Node节点上的rabbit和配置的插件应用是否启动
wait_for_application(Node, Pid, rabbit_and_plugins) ->
	wait_for_startup(Node, Pid);

%% 判断Node节点上的Application应用是否正在运行中
wait_for_application(Node, Pid, Application) ->
	while_process_is_alive(
	  Node, Pid, fun() -> rabbit_nodes:is_running(Node, Application) end).


%% 判断Node节点上的rabbit和配置的插件应用是否启动
wait_for_startup(Node, Pid) ->
	while_process_is_alive(
	  Node, Pid, fun() -> rpc:call(Node, rabbit, await_startup, []) =:= ok end).


while_process_is_alive(Node, Pid, Activity) ->
	%% 判断操作系统的进程Pid是否正在运行中
	case rabbit_misc:is_os_process_alive(Pid) of
		true  -> case Activity() of
					 true  -> ok;
					 false -> timer:sleep(?EXTERNAL_CHECK_INTERVAL),
							  while_process_is_alive(Node, Pid, Activity)
				 end;
		false -> {error, process_not_running}
	end.


wait_for_process_death(Pid) ->
	case rabbit_misc:is_os_process_alive(Pid) of
		true  -> timer:sleep(?EXTERNAL_CHECK_INTERVAL),
				 wait_for_process_death(Pid);
		false -> ok
	end.


read_pid_file(PidFile, Wait) ->
	case {file:read_file(PidFile), Wait} of
		{{ok, Bin}, _} ->
			S = binary_to_list(Bin),
			{match, [PidS]} = re:run(S, "[^\\s]+",
									 [{capture, all, list}]),
			try list_to_integer(PidS)
			catch error:badarg ->
					  exit({error, {garbage_in_pid_file, PidFile}})
			end,
			PidS;
		{{error, enoent}, true} ->
			timer:sleep(?EXTERNAL_CHECK_INTERVAL),
			read_pid_file(PidFile, Wait);
		{{error, _} = E, _} ->
			exit({error, {could_not_read_pid, E}})
	end.


%% Impersonating:冒充
%% 冒充BecomeNode节点，相当于启动BecomeNode节点，所以该节点不能在当前运行中
become(BecomeNode) ->
	error_logger:tty(false),
	ok = net_kernel:stop(),
	case net_adm:ping(BecomeNode) of
		pong -> exit({node_running, BecomeNode});
		pang -> io:format("  * Impersonating node: ~s...", [BecomeNode]),
				{ok, _} = rabbit_cli:start_distribution(BecomeNode),
				io:format(" done~n", []),
				Dir = mnesia:system_info(directory),
				io:format("  * Mnesia directory  : ~s~n", [Dir])
	end.

%%----------------------------------------------------------------------------
%% 如果List为空，则返回默认值Default，否则直接返回List
default_if_empty(List, Default) when is_list(List) ->
	if List == [] -> Default;
	   true       -> [list_to_atom(X) || X <- List]
	end.


%% 展示InfoItemKeys中key对应的信息
display_info_list(Results, InfoItemKeys) when is_list(Results) ->
	lists:foreach(
	  fun (Result) -> display_row(
						[format_info_item(proplists:get_value(X, Result)) ||
						   X <- InfoItemKeys])
	  end, lists:sort(Results)),
	ok;

display_info_list(Other, _) ->
	Other.


display_row(Row) ->
	io:fwrite(string:join(Row, "\t")),
	io:nl().

-define(IS_U8(X),  (X >= 0 andalso X =< 255)).
-define(IS_U16(X), (X >= 0 andalso X =< 65535)).
%% 标准打印信息的接口
format_info_item(#resource{name = Name}) ->
	escape(Name);

format_info_item({N1, N2, N3, N4} = Value) when
  ?IS_U8(N1), ?IS_U8(N2), ?IS_U8(N3), ?IS_U8(N4) ->
	rabbit_misc:ntoa(Value);

format_info_item({K1, K2, K3, K4, K5, K6, K7, K8} = Value) when
  ?IS_U16(K1), ?IS_U16(K2), ?IS_U16(K3), ?IS_U16(K4),
  ?IS_U16(K5), ?IS_U16(K6), ?IS_U16(K7), ?IS_U16(K8) ->
	rabbit_misc:ntoa(Value);

format_info_item(Value) when is_pid(Value) ->
	rabbit_misc:pid_to_string(Value);

format_info_item(Value) when is_binary(Value) ->
	escape(Value);

format_info_item(Value) when is_atom(Value) ->
	escape(atom_to_list(Value));

format_info_item([{TableEntryKey, TableEntryType, _TableEntryValue} | _] =
					 Value) when is_binary(TableEntryKey) andalso
									 is_atom(TableEntryType) ->
	io_lib:format("~1000000000000p", [prettify_amqp_table(Value)]);

format_info_item([T | _] = Value)
  when is_tuple(T) orelse is_pid(T) orelse is_binary(T) orelse is_atom(T) orelse
		   is_list(T) ->
	"[" ++
		lists:nthtail(2, lists:append(
						[", " ++ format_info_item(E) || E <- Value])) ++ "]";

format_info_item(Value) ->
	io_lib:format("~w", [Value]).


%% call到Node节点，执行MFA操作
display_call_result(Node, MFA) ->
	case call(Node, MFA) of
		{badrpc, _} = Res -> throw(Res);
		Res               -> io:format("~p~n", [Res]),
							 ok
	end.


unsafe_rpc(Node, Mod, Fun, Args) ->
	case rpc_call(Node, Mod, Fun, Args) of
		{badrpc, _} = Res -> throw(Res);
		Normal            -> Normal
	end.


%% 确保该节点的rabbit应用已经启动
ensure_app_running(Node) ->
	case call(Node, {rabbit, is_running, []}) of
		true  -> ok;
		false -> {error_string,
				  rabbit_misc:format(
					"rabbit application is not running on node ~s.~n"
						" * Suggestion: start it with \"rabbitmqctl start_app\" "
							"and try again", [Node])};
		Other -> Other
	end.


%% Call Node节点执行Mod模块的Fun函数，参数是Args
call(Node, {Mod, Fun, Args}) ->
	rpc_call(Node, Mod, Fun, lists:map(fun list_to_binary_utf8/1, Args)).


list_to_binary_utf8(L) ->
	B = list_to_binary(L),
	case rabbit_binary_parser:validate_utf8(B) of
		ok    -> B;
		error -> throw({error, {not_utf_8, L}})
	end.

%% escape does C-style backslash escaping of non-printable ASCII
%% characters.  We don't escape characters above 127, since they may
%% form part of UTF-8 strings.

escape(Atom) when is_atom(Atom)  -> escape(atom_to_list(Atom));
escape(Bin)  when is_binary(Bin) -> escape(binary_to_list(Bin));
escape(L)    when is_list(L)     -> escape_char(lists:reverse(L), []).


escape_char([$\\ | T], Acc) ->
	escape_char(T, [$\\, $\\ | Acc]);
escape_char([X | T], Acc) when X >= 32, X /= 127 ->
	escape_char(T, [X | Acc]);
escape_char([X | T], Acc) ->
	escape_char(T, [$\\, $0 + (X bsr 6), $0 + (X band 8#070 bsr 3),
					$0 + (X band 7) | Acc]);
escape_char([], Acc) ->
	Acc.


prettify_amqp_table(Table) ->
	[{escape(K), prettify_typed_amqp_value(T, V)} || {K, T, V} <- Table].


prettify_typed_amqp_value(longstr, Value) -> escape(Value);
prettify_typed_amqp_value(table,   Value) -> prettify_amqp_table(Value);
prettify_typed_amqp_value(array,   Value) -> [prettify_typed_amqp_value(T, V) ||
												{T, V} <- Value];
prettify_typed_amqp_value(_Type,   Value) -> Value.


split_list([])         -> [];
split_list([_])        -> exit(even_list_needed);
split_list([A, B | T]) -> [{A, B} | split_list(T)].
