%% Author: xxw
%% Created: 2015-11-28
%% Description: 集群启动模块
-module(start_cluster).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-compile(export_all).

%%
%% API Functions
%%
%% 集群启动入口
start_cluster() ->
	case run_option:get_options("../options/run.option") of
		[] ->
			io:format("start_cluster start find run.option is empty~n");
		Options ->
			Prefix = run_option:get_prefix(Options),
			Nodes = run_option:get_nodes(Options),
			BeamDir = run_option:get_beam_dir(Options),
			file:set_cwd(BeamDir),
			%% 启动配置文件中所有配置的RabbitMQ节点
			lists:foreach(fun(RunOpt) ->
										run_node(Options, Prefix, RunOpt, nowait)
									end, Nodes)
	end.


%% 停止RabbitMQ集群系统
stop_cluster() ->
	OptionFile = "../options/run.option",
	case run_option:get_options(OptionFile) of
		[] ->
			io:format("start_cluster start find run.option is empty~n");
		Options ->
			PreFix = proplists:get_value(prefix, Options),
			
			IpList = run_option:get_ip_list(Options),
			MyIpList = os_util:get_localips(),
			
			MyIps = lists:filter(fun(IpStr) ->
										 lists:member(IpStr, IpList)
								 end, MyIpList),
			case MyIps of
				[] ->
					io:format("get ip error ~n");
				[Ip | _] ->
					NodeStr = str_util:datetime_to_short_string(erlang:localtime()),
					AddOption = " -pa ../ebin -s start_cluster stop " ++ OptionFile ++ " -s init stop",
					case os:type() of
						{win32, nt} ->
							os_util:run_erl(normal, PreFix ++ NodeStr, Ip, [], smp, wait, AddOption);
						_ ->
							os_util:run_erl(hiden, PreFix ++ NodeStr, Ip, [], smp, wait, AddOption)
					end
			end
	end.


%% 启动客户端节点
start_clients() ->
	case run_option:get_options("../options/run.option") of
		[] ->
			io:format("start_cluster start find run.option is empty~n");
		Options ->
			Prefix = run_option:get_prefix(Options),
			BeamDir = run_option:get_beam_dir(Options),
			file:set_cwd(BeamDir),
			ClientInfos = proplists:get_value(clients, Options),
			lists:foreach(fun(RunOpt) ->
								  run_client_node(Prefix, RunOpt, nowait)
						  end, ClientInfos)
	end.


%%
%% Local Functions
%%
%% 单个节点的启动入口函数
run_node(Options, Prefix, RunOpt, Wait) ->
	{SNode, Host, ClientPort, Smp, _NodeType, Addition} = RunOpt,
	NodeName = str_util:sprintf("~s@~s", [Prefix ++ SNode, Host]),
	NewAddition = get_rabbitmq_node_start_args(Options, NodeName, Host, ClientPort) ++ " " ++ Addition,
	CommandLine = os_util:get_erl_cmd(normal, Prefix ++ SNode, Host, [], Smp, Wait, NewAddition),
	case Wait of
		wait ->
			os_util:wait_exe(CommandLine);
		nowait ->
			os_util:run_exe(CommandLine)
	end.


%% 获得RabbitMQ节点相关的启动参数
get_rabbitmq_node_start_args(Options, NodeName, Ip, ClientPort) ->
	%% 获得代码beam路径
	CodeBeamPath = " -pa " ++ proplists:get_value(beam_dir, Options),
	%% 启动sasl
	StartSasl = " -boot start_sasl",
	%% 启动后执行的模块和函数名
	StartModuleAndFun = " -s option_set_server start",
	%% 获得节点配置文件路径
	ConfigPath = case proplists:get_value(rabbitmq_config_file, Options) of
					 "" ->
						 "";
					 ProtoConfigPath ->
						 " -config " ++ ProtoConfigPath
				 end,
	%% 普通启动配置
	NormalStr = " +W w +A30 +P 1048576 -sasl errlog_type error -sasl sasl_error_logger false -os_mon start_cpu_sup false -os_mon start_disksup false -os_mon start_memsup false",
	%% kernel相关
	KernelStr = " -kernel inet_default_connect_options [{nodelay,true}]",
	%% 客户端监听端口
	ClientListenStr = " -rabbit tcp_listeners [{\\\"" ++ Ip ++ "\\\"," ++ integer_to_list(ClientPort) ++ "}]" ++ "",
	%% error_logger日志的路径
	ErrorLoggerLogPath = " -rabbit error_logger {file,\\\"" ++ proplists:get_value(rabbitmq_log_path, Options) ++ NodeName ++ ".log\\\"}" ++ "",
	%% sasl日志路径
	SaslLogPath = " -rabbit sasl_error_logger {file,\\\"" ++ proplists:get_value(rabbitmq_log_path, Options) ++ NodeName ++ "_sasl.log\\\"}" ++ "",
	%% 允许插件配置的文件路径
	EnabledPluginsFile = " -rabbit enabled_plugins_file \\\"" ++ proplists:get_value(plugins_enabled_file, Options) ++ "\\\"",
	%% 插件压缩包存储路径
	PluginsEzPath = " -rabbit plugins_dir \\\"" ++ proplists:get_value(plugins_path, Options) ++ "\\\"",
	%% 当前节点插件解压后存储路径
	CurNodePluginsPath = " -rabbit plugins_expand_dir \\\"" ++ proplists:get_value(rabbitmq_db_path, Options) ++ NodeName ++ "-plugins-expand" ++ "\\\"",
	%% Mnesia数据库存储路径
	MnesiaPath = " -mnesia dir \\\"" ++ proplists:get_value(rabbitmq_db_path, Options) ++ NodeName ++ "-mnesia" ++ "\\\"",
	
	%% 组合所有的启动参数
	lists:append([CodeBeamPath, StartSasl, StartModuleAndFun, ConfigPath, NormalStr, KernelStr, ClientListenStr, ErrorLoggerLogPath, SaslLogPath,
				  EnabledPluginsFile, PluginsEzPath, CurNodePluginsPath, MnesiaPath]).


%% RabbitMQ集群系统的停止接口
stop(Args) ->
	[OptionPath | _] = Args,
	case run_option:get_options(OptionPath) of
		[] ->
			io:format("start_cluster start find run.option is empty~n");
		Options ->
			%% 统一节点的cookie
			Cookie = proplists:get_value(cookie, Options),
			erlang:set_cookie(node(), Cookie),
			Prefix = run_option:get_prefix(Options),
			Nodes = run_option:get_nodes(Options),
			lists:foreach(fun({SNode, Ip, _, _, _, _}) ->
								  NodeName = str_util:sprintf("~s@~s", [Prefix ++ SNode, Ip]),
								  stop_node(NodeName)
						  end, Nodes)
	end.


%% RabbitMQ集群单个节点的停止
stop_node(NodeName) ->
	rabbit_cli:rpc_call(list_to_atom(NodeName), ?MODULE, stop_node1, []),
	io:format("stopping the node ~p ~n", [NodeName]),
	NodeName.


%% 停止节点相关操作
stop_node1() ->
	rabbit:stop_and_halt(),
	os:cmd("exit()").


%% 启动客户端节点
run_client_node(Prefix, RunOpt, Wait) ->
	{SNode, Host, Smp, _Args, Addition} = RunOpt,
	StartModStr = "-s client start_link",
	NewAddition = StartModStr ++ " " ++ Addition,
	CommandLine = os_util:get_erl_cmd(normal, Prefix ++ SNode, Host, [], Smp, Wait, NewAddition),
	case Wait of
		wait ->
			os_util:wait_exe(CommandLine);
		nowait ->
			os_util:run_exe(CommandLine)
	end.
