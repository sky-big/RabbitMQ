%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_external_stats).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([list_registry_plugins/1,cluster_links/0]).

-import(rabbit_misc, [pget/2]).

-include("rabbit.hrl").

-define(REFRESH_RATIO, 5000).
-define(KEYS, [name, partitions, os_pid, fd_used, fd_total,
               sockets_used, sockets_total, mem_used, mem_limit, mem_alarm,
               disk_free_limit, disk_free, disk_free_alarm,
               proc_used, proc_total, rates_mode,
               uptime, run_queue, processors, exchange_types,
               auth_mechanisms, applications, contexts,
               log_file, sasl_log_file, db_dir, config_files, net_ticktime,
               enabled_plugins, persister_stats]).

%%--------------------------------------------------------------------
%% rabbit_mgmt_external_stats进程数据结构
-record(state, {
				fd_total,							%% 当前操作系统上的能够打开的最大文件句柄数
				fhc_stats,							%% file_handle_cache进程所有的统计信息
				fhc_stats_derived,					%% file_handle_cache进程中各种操作的平均花费时间
				node_owners
			   }).

%%--------------------------------------------------------------------
%% rabbit_mgmt_external_stats进程启动入口函数
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% 得到当前虚拟机使用的文件句柄数量
get_used_fd() ->
	get_used_fd(os:type()).


%% 得到当前虚拟机使用的文件句柄数量
get_used_fd({unix, linux}) ->
	case file:list_dir("/proc/" ++ os:getpid() ++ "/fd") of
		{ok, Files} -> length(Files);
		{error, _}  -> get_used_fd({unix, generic})
	end;

get_used_fd({unix, BSD})
  when BSD == openbsd; BSD == freebsd; BSD == netbsd ->
	Digit = fun (D) -> lists:member(D, "0123456789*") end,
	Output = os:cmd("fstat -p " ++ os:getpid()),
	try
		length(
		  lists:filter(
			fun (Line) ->
					 lists:all(Digit, (lists:nth(4, string:tokens(Line, " "))))
			end, string:tokens(Output, "\n")))
	catch _:Error ->
			  case get(logged_used_fd_error) of
				  undefined -> rabbit_log:warning(
								 "Could not parse fstat output:~n~s~n~p~n",
								 [Output, {Error, erlang:get_stacktrace()}]),
							   put(logged_used_fd_error, true);
				  _         -> ok
			  end,
			  unknown
	end;

get_used_fd({unix, _}) ->
	Cmd = rabbit_misc:format(
			"lsof -d \"0-9999999\" -lna -p ~s || echo failed", [os:getpid()]),
	Res = os:cmd(Cmd),
	case string:right(Res, 7) of
		"failed\n" -> unknown;
		_          -> string:words(Res, $\n) - 1
								  end;

%% handle.exe can be obtained from
%% http://technet.microsoft.com/en-us/sysinternals/bb896655.aspx

%% Output looks like:

%% Handle v3.42
%% Copyright (C) 1997-2008 Mark Russinovich
%% Sysinternals - www.sysinternals.com
%%
%% Handle type summary:
%%   ALPC Port       : 2
%%   Desktop         : 1
%%   Directory       : 1
%%   Event           : 108
%%   File            : 25
%%   IoCompletion    : 3
%%   Key             : 7
%%   KeyedEvent      : 1
%%   Mutant          : 1
%%   Process         : 3
%%   Process         : 38
%%   Thread          : 41
%%   Timer           : 3
%%   TpWorkerFactory : 2
%%   WindowStation   : 2
%% Total handles: 238

%% Note that the "File" number appears to include network sockets too; I assume
%% that's the number we care about. Note also that if you omit "-s" you will
%% see a list of file handles *without* network sockets. If you then add "-a"
%% you will see a list of handles of various types, including network sockets
%% shown as file handles to \Device\Afd.

get_used_fd({win32, _}) ->
    Handle = rabbit_misc:os_cmd(
               "handle.exe /accepteula -s -p " ++ os:getpid() ++ " 2> nul"),
    case Handle of
        [] -> install_handle_from_sysinternals;
        _  -> find_files_line(string:tokens(Handle, "\r\n"))
    end;

get_used_fd(_) ->
    unknown.

find_files_line([]) ->
    unknown;
find_files_line(["  File " ++ Rest | _T]) ->
    [Files] = string:tokens(Rest, ": "),
    list_to_integer(Files);
find_files_line([_H | T]) ->
    find_files_line(T).

-define(SAFE_CALL(Fun, NoProcFailResult),
    try
        Fun
    catch exit:{noproc, _} -> NoProcFailResult
    end).


%% 获得当前RabbitMQ系统设置的最小空闲磁盘大小
get_disk_free_limit() -> ?SAFE_CALL(rabbit_disk_monitor:get_disk_free_limit(),
                                    disk_free_monitoring_disabled).


%% 得到当前RabbitMQ系统能够使用的磁盘大小
get_disk_free() -> ?SAFE_CALL(rabbit_disk_monitor:get_disk_free(),
                              disk_free_monitoring_disabled).

%%--------------------------------------------------------------------
%% 打印出Items关键key对应的信息
infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].


%% 拿到当前节点的名字
i(name,            _State) -> node();

%% 拿到当前RabbitMQ系统的网络分区信息
i(partitions,      _State) -> rabbit_node_monitor:partitions();

%% 得到当前RabbitMQ系统使用的文件句柄数量
i(fd_used,         _State) -> get_used_fd();

%% 得到当前RabbitMQ系统上的能够打开的最大文件句柄数
i(fd_total, #state{fd_total = FdTotal}) -> FdTotal;

%% 得到当前RabbitMQ系统里使用的Socket数量
i(sockets_used,    _State) ->
	proplists:get_value(sockets_used, file_handle_cache:info([sockets_used]));

%% 得到当前RabbitMQ系统里Socket数量上限
i(sockets_total,   _State) ->
	proplists:get_value(sockets_limit, file_handle_cache:info([sockets_limit]));

%% 得到当前虚拟机对应在操作系统里面的进程号
i(os_pid,          _State) -> list_to_binary(os:getpid());

%% 得到当前RabbitMQ系统已经使用的内存
i(mem_used,        _State) -> erlang:memory(total);

%% 得到当前RabbitMQ系统能够使用的内存上限
i(mem_limit,       _State) -> vm_memory_monitor:get_memory_limit();

%% 判断当前节点内存的资源类型是否处于报警状态
i(mem_alarm,       _State) -> resource_alarm_set(memory);

%% 得到当前RabbitMQ系统已经存在的进程个数
i(proc_used,       _State) -> erlang:system_info(process_count);

%% 得到当前RabbitMQ系统能够使用的进程上限个数
i(proc_total,      _State) -> erlang:system_info(process_limit);

%% 得到当前RabbitMQ系统上所有进程中所有等待处理的消息的总数量
i(run_queue,       _State) -> erlang:statistics(run_queue);


%% 得到当前RabbitMQ系统逻辑进程的数量
i(processors,      _State) -> erlang:system_info(logical_processors);

%% 获得当前RabbitMQ系统设置的最小空闲磁盘大小
i(disk_free_limit, _State) -> get_disk_free_limit();

%% 得到当前RabbitMQ系统能够使用的磁盘大小
i(disk_free,       _State) -> get_disk_free();

%% 判断当前节点磁盘的资源类型是否处于报警状态
i(disk_free_alarm, _State) -> resource_alarm_set(disk);

%% 列出rabbit_web_dispatch_registry进程启动的所有的Http服务器信息
i(contexts,        _State) -> rabbit_web_dispatch_contexts();

%% 返回总的执行时间
i(uptime,          _State) -> {Total, _} = erlang:statistics(wall_clock),
							  Total;

%% 获得当前系统配置的rates_mode对应的数据
i(rates_mode,      _State) -> rabbit_mgmt_db_handler:rates_mode();

%% 获得当前RabbitMQ系统中所有的交换机类型
i(exchange_types,  _State) -> list_registry_plugins(exchange);

%% 得到当前RabbitMQ系统kernel对应的日志路径(error_logger)
i(log_file,        _State) -> log_location(kernel);

%% 得到当前RabbitMQ系统sasl对应的日志路径(sasl_error_logger)
i(sasl_log_file,   _State) -> log_location(sasl);

%% 得到当前RabbitMQ系统Mnesia数据库存储路径
i(db_dir,          _State) -> list_to_binary(rabbit_mnesia:dir());

%% 得到RabbitMQ系统的rabbitmq.config文件路径
i(config_files,    _State) -> [list_to_binary(F) || F <- rabbit:config_files()];

i(net_ticktime,    _State) -> net_kernel:get_net_ticktime();

%% 得到当前RabbitMQ系统不同句柄的平均操作时间
i(persister_stats,  State) -> persister_stats(State);

%% 得到当前RabbitMQ系统使用的插件列表
i(enabled_plugins, _State) -> {ok, Dir} = application:get_env(
											rabbit, enabled_plugins_file),
							  rabbit_plugins:read_enabled(Dir);

%% 得到当前RabbitMQ系统的验证类型列表
i(auth_mechanisms, _State) ->
	{ok, Mechanisms} = application:get_env(rabbit, auth_mechanisms),
	%% 列出auth_mechanism对应的所有验证模块信息
	list_registry_plugins(
	  auth_mechanism,
	  fun (N) -> lists:member(list_to_atom(binary_to_list(N)), Mechanisms) end);

%% 列出当前RabbitMQ系统启动的所有应用
i(applications,    _State) ->
	[format_application(A) ||
	   A <- lists:keysort(1, rabbit_misc:which_applications())].


%% 拿到Type对应的日志存储路径
log_location(Type) ->
	case rabbit:log_location(Type) of
		tty  -> <<"tty">>;
		File -> list_to_binary(File)
	end.


%% 判断当前节点Source的资源类型是否处于报警状态
resource_alarm_set(Source) ->
	lists:member({{resource_limit, Source, node()},[]},
				 rabbit_alarm:get_alarms()).


%% 列出Type类型的所有信息
list_registry_plugins(Type) ->
	list_registry_plugins(Type, fun(_) -> true end).


%% 列出Type类型的所有信息，Fun是过滤函数
list_registry_plugins(Type, Fun) ->
	[registry_plugin_enabled(set_plugin_name(Name, Module), Fun) ||
	   {Name, Module} <- rabbit_registry:lookup_all(Type)].


registry_plugin_enabled(Desc, Fun) ->
	Desc ++ [{enabled, Fun(proplists:get_value(name, Desc))}].


%% 将rabbit_misc:which_applications()函数返回的应用信息重新组装数据
format_application({Application, Description, Version}) ->
	[{name, Application},
	 {description, list_to_binary(Description)},
	 {version, list_to_binary(Version)}].


%% 重新组装数据
set_plugin_name(Name, Module) ->
	[{name, list_to_binary(atom_to_list(Name))} |
		 proplists:delete(name, Module:description())].


%% 得到当前RabbitMQ系统不同句柄的平均操作时间
persister_stats(#state{fhc_stats         = FHC,
					   fhc_stats_derived = FHCD}) ->
	[{flatten_key(K), V} || {{_Op, Type} = K, V} <- FHC,
							Type =/= time] ++
		[{flatten_key(K), V} || {K, V} <- FHCD].


flatten_key({A, B}) ->
	list_to_atom(atom_to_list(A) ++ "_" ++ atom_to_list(B)).


%% 获得集群中的其他节点信息
cluster_links() ->
	%% 获得集群中其他节点的节点信息
	{ok, Items} = net_kernel:nodes_info(),
	[Link || Item <- Items,
			 Link <- [format_nodes_info(Item)], Link =/= undefined].


%% 打印节点信息
format_nodes_info({Node, Info}) ->
	Owner = proplists:get_value(owner, Info),
	case catch process_info(Owner, links) of
		{links, Links} ->
			case [Link || Link <- Links, is_port(Link)] of
				[Port] ->
					{Node, Owner, format_nodes_info1(Port)};
				_ ->
					undefined
			end;
		_ ->
			undefined
	end.


%% 打印启动Port的节点的socket信息
format_nodes_info1(Port) ->
	case {rabbit_net:socket_ends(Port, inbound),
		  rabbit_net:getstat(Port, [recv_oct, send_oct])} of
		{{ok, {PeerAddr, PeerPort, SockAddr, SockPort}}, {ok, Stats}} ->
			[{peer_addr, maybe_ntoab(PeerAddr)},
			 {peer_port, PeerPort},
			 {sock_addr, maybe_ntoab(SockAddr)},
			 {sock_port, SockPort},
			 {recv_bytes, pget(recv_oct, Stats)},
			 {send_bytes, pget(send_oct, Stats)}];
		_ ->
			[]
	end.


maybe_ntoab(A) when is_tuple(A) -> list_to_binary(rabbit_misc:ntoab(A));
maybe_ntoab(H)                  -> H.

%%--------------------------------------------------------------------

%% This is slightly icky in that we introduce knowledge of
%% rabbit_web_dispatch, which is not a dependency. But the last thing I
%% want to do is create a rabbitmq_mochiweb_management_agent plugin.
%% 列出rabbit_web_dispatch_registry进程启动的所有的Http服务器信息
rabbit_web_dispatch_contexts() ->
	[format_context(C) || C <- rabbit_web_dispatch_registry_list_all()].

%% For similar reasons we don't declare a dependency on
%% rabbitmq_mochiweb - so at startup there's no guarantee it will be
%% running. So we have to catch this noproc.
%% 列出rabbit_web_dispatch_registry进程启动的所有的Http服务器信息
rabbit_web_dispatch_registry_list_all() ->
	case code:is_loaded(rabbit_web_dispatch_registry) of
		false -> [];
		_     -> try
					 M = rabbit_web_dispatch_registry, %% Fool xref
					 M:list_all()
				 catch exit:{noproc, _} ->
						   []
				 end
	end.


format_context({Path, Description, Rest}) ->
	[{description, list_to_binary(Description)},
	 {path,        list_to_binary("/" ++ Path)} |
		 format_mochiweb_option_list(Rest)].


format_mochiweb_option_list(C) ->
	[{K, format_mochiweb_option(K, V)} || {K, V} <- C].


format_mochiweb_option(ssl_opts, V) ->
	format_mochiweb_option_list(V);
format_mochiweb_option(_K, V) ->
	case io_lib:printable_list(V) of
		true  -> list_to_binary(V);
		false -> list_to_binary(rabbit_misc:format("~w", [V]))
	end.

%%--------------------------------------------------------------------
%% rabbit_mgmt_external_stats进程启动回调初始化函数
init([]) ->
	State = #state{%% 得到当前RabbitMQ系统上的能够打开的最大文件句柄数
				   fd_total    = file_handle_cache:ulimit(),
				   %% 拿到file_handle_cache进程所有的统计信息
				   fhc_stats   = file_handle_cache_stats:get(),
				   node_owners = sets:new()},
	%% If we emit an update straight away we will do so just before
	%% the mgmt db starts up - and then have to wait ?REFRESH_RATIO
	%% until we send another. So let's have a shorter wait in the hope
	%% that the db will have started by the time we emit an update,
	%% and thus shorten that little gap at startup where mgmt knows
	%% nothing about any nodes.
	%% 设置1秒的定时器是为了等待rabbit_mgmt_db进程的启动初始化完毕
	erlang:send_after(1000, self(), emit_update),
	{ok, State}.


handle_call(_Req, _From, State) ->
	{reply, unknown_request, State}.


handle_cast(_C, State) ->
	{noreply, State}.


handle_info(emit_update, State) ->
	{noreply, emit_update(State)};

handle_info(_I, State) ->
	{noreply, State}.


terminate(_, _) -> ok.


code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------
%% rabbit_mgmt_external_stats进程定时更新函数
emit_update(State0) ->
	%% 更新RabbitMQ系统中文件句柄操作花费的平均时间
	State = update_state(State0),
	%% 将当前RabbitMQ系统最新的状态数据发布到rabbit_event事件中心中
	rabbit_event:notify(node_stats, infos(?KEYS, State)),
	%% 启动5秒的定时器进行下次的状态更新
	erlang:send_after(?REFRESH_RATIO, self(), emit_update),
	%% 更新集群其他节点的Socket信息
	emit_node_node_stats(State).


%% 更新集群其他节点的Socket信息
emit_node_node_stats(State = #state{node_owners = Owners}) ->
	Links = cluster_links(),
	NewOwners = sets:from_list([{Node, Owner} || {Node, Owner, _} <- Links]),
	%% 用老的节点列表减去新的节点列表则得到死去的节点列表
	Dead = sets:to_list(sets:subtract(Owners, NewOwners)),
	%% 向rabbit_event事件中心发布节点死亡的事件
	[rabbit_event:notify(
	   node_node_deleted, [{route, Route}]) || {Node, _Owner} <- Dead,
											   Route <- [{node(), Node},
														 {Node,   node()}]],
	%% 向rabbit_event事件中心发布集群其他节点的信息
	[rabbit_event:notify(
	   node_node_stats, [{route, {node(), Node}} | Stats]) ||
	   {Node, _Owner, Stats} <- Links],
	%% 更新最新的集群信息
	State#state{node_owners = NewOwners}.


%% 更新RabbitMQ系统中文件句柄操作花费的平均时间
update_state(State0 = #state{fhc_stats = FHC0}) ->
	%% 拿到file_handle_cache进程所有的统计信息
	FHC = file_handle_cache_stats:get(),
	%% 算出间隔定时器时间长度后，所有Op操作平均花费时间
	Avgs = [{{Op, avg_time}, avg_op_time(Op, V, FHC, FHC0)}
			|| {{Op, time}, V} <- FHC],
	State0#state{fhc_stats         = FHC,
				 fhc_stats_derived = Avgs}.

-define(MICRO_TO_MILLI, 1000).
%% 算出间隔定时器时间长度后，Op操作平均花费时间
avg_op_time(Op, Time, FHC, FHC0) ->
	%% 拿到Op对应的老的总时间
	Time0 = pget({Op, time}, FHC0),
	%% 得到新的总时间减去老的总时间的差值
	TimeDelta = Time - Time0,
	%% 得到新的次数总和减去老的次数总和的差值
	OpDelta = pget({Op, count}, FHC) - pget({Op, count}, FHC0),
	case OpDelta of
		0 -> 0;
		%% 算出毫秒级的平均数
		_ -> (TimeDelta / OpDelta) / ?MICRO_TO_MILLI
	end.
