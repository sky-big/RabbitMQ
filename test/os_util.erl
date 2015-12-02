%% Author: xxw
%% Created: 2014-3-11
%% Description: TODO: Add description to os_util
-module(os_util).
-compile(export_all).

%%
%% Include files
%%
-include("os_util.hrl").

%%
%% Exported Functions
%%

%%
%% API Functions
%%
run_erl(Hiden, Name, Host, MnesiaDir, SmpEnable, Wait, Option) ->
	CommandLine = get_erl_cmd(Hiden, Name, Host, MnesiaDir, SmpEnable, Wait, Option),
	case Wait of
		wait ->
			wait_exe(CommandLine);
		nowait ->
			run_exe(CommandLine)
	end.

%% 操作系统命令非同步执行(prompt:提示)
wait_exe(CmdLine) ->
	wait_exe(CmdLine, prompt).
wait_exe(CmdLine, noprompt) ->
	os:cmd(CmdLine);
wait_exe(CmdLine, _) ->
	os:cmd(CmdLine).

%% 操作系统命令同步执行(prompt:提示)
run_exe(CmdLine) ->
	run_exe(CmdLine, prompt).
run_exe(CmdLine, noprompt) ->
	cmd_ansync(CmdLine);
run_exe(CmdLine, _) ->
	cmd_ansync(CmdLine).

%% 启动linux Erl
linux_erl(Hiden, Name, Host, MnesiaDir, SmpEnable, Wait, Option) ->
	CommandLine = get_erl_cmd(Hiden, Name, Host, MnesiaDir, SmpEnable, Wait, Option),
	linux_run(CommandLine, Wait).

linux_run(CommandLine, Wait) ->
	case os:type() of
		{win32, nt} ->
			OutPrompt = str_util:sprintf("windows does not run:~p~n", [CommandLine]),
			io:format(OutPrompt);
		_ ->
			case Wait of
				wait ->
					wait_exe(CommandLine);
				_ ->
					run_exe(CommandLine)
			end
	end.

%% 启动Windwos Erl
win_erl(Hiden, Name, Host, MnesiaDir, SmpEnable, Wait, Option) ->
	CommandLine = get_erl_cmd(Hiden, Name, Host, MnesiaDir, SmpEnable, Wait, Option),
	win_run(CommandLine, Wait).

win_run(CommandLine, Wait) ->
	case os:type() of
		{win32, nt} ->
			case Wait of
				wait ->
					wait_exe(CommandLine);
				_ ->
					run_exe(CommandLine)
			end;
		_ ->
			OutPrompt = str_util:sprintf("linux does not run:~p~n", [CommandLine]),
			io:format(OutPrompt)
	end.

get_erl_cmd(Hiden, Name, Host, MnesiaDir, SmpEnable, Wait, Option) ->
	%% 是否隐藏shell窗口的参数
	HidenOption =
		case Hiden of
			hiden ->
				" -noshell -noinput";
			_ ->
				""
		end,
	%% 得到节点名字的参数
	NameOption =
		case Name of
			[] -> "";
			_ ->
				str_util:sprintf(" -name ~s@~s ", [Name, Host])
		end,
	%% mnesia数据库的存储目录
	MnesiaOption = 
		case MnesiaDir of
			[] ->
				"";
			_ ->
				str_util:sprintf(" -mnesia dir '\"~s\"' ", [MnesiaDir])
		end,
	SmpOption =
		case SmpEnable of
			smp ->
				"";
			nosmp ->
				case os:type() of
					{win32, nt} ->
						" ";
					_ ->
						%% linux系统下让smp不要启动(让服务器不要启动跟CPU个数相同的Scheduler)
						" -smp disable "
				end
		end,
	%% 根据不同的操作系统,得到对应的启动节点的命令
	ExeCmd = 
		case os:type() of
			{win32, nt} ->
				case Wait of
					wait ->
						"start erl.exe ";
					nowait ->
						case Hiden of
							hiden ->
								"erl.exe";
							normal ->
								"start cmd.exe /k erl.exe ";
							gui ->
								"werl.exe"
						end
				end;
			_ ->
				%% +P erlang节点系统的最大并发进程数, +K true | false 是否开启kernel poll，就是epoll
				%% +K true|false - 该选项用于打开(true)或关闭(false，默认)Erlang RTS的Kernel poll功能。
				%% 当Kernel poll被关闭时，RTS使用普通的用户态事件轮询接口select/poll进行进程和I/O调度，调度开销较大；
				%% 打开Kernel poll时，RTS将使用内核级事件轮询接口(如Linux上的epoll等)进行调度，开销较小，
				%% 可以提高存在大量进程时的响应速度。
				"erl +P 100000 +K true "
		end,
	%% 根据操作系统不同去处理错误输出问题
	TailOption =
		case os:type() of
			{win32, nt} ->
				"";
			_ ->
				case Wait of
					wait ->
						"";
					nowait ->
						" > /dev/null 2>&1&"
				end
		end,
	
	%% 得到真实的启动节点的节点参数命令
	lists:append([ExeCmd, HidenOption, NameOption, MnesiaOption, SmpOption, Option, TailOption]).

%% 同步启动多个节点
cmd_ansync(CommandLine) ->
	SelfPid = self(),
	Fun = fun() -> do_cmd_ansync(CommandLine, SelfPid) end,
	proc_lib:spawn(Fun),
	receive
		ok ->
			ok;
		_ ->
			error
	end.

do_cmd_ansync(CommandLine, FatherPid) ->
	case os:type() of
		{unix, _} ->
			start_unix_cmd(CommandLine, FatherPid, unix_shell);
		{win32, Wtype} ->
			Command = 
				case {os:getenv("COMSPEC"), Wtype} of
					{false, windows} ->
						lists:concat(["command.com /c ", CommandLine]);
					{false, _} ->
						lists:concat(["cmd /c ", CommandLine]);
					{Cspec, _} ->
						lists:concat([Cspec, " /c ", CommandLine])
				end,
			Port = erlang:open_port({spawn, Command}, [stream, in, eof, hide]),
			FatherPid ! ok,
			get_data(Port, [])
	end.

get_data(Port, Sofar) ->
	receive
		{Port, {data, Bytes}} ->
			get_data(Port, [Sofar | Bytes]);
		{Port, eof} ->
			Port ! {self(), close},
			receive
				{Port, closed} ->
					true
			end,
			receive
				{'EXIT', Port, _} ->
					ok
			after 1 ->
					ok
			end,
			lists:flatten(Sofar)
	end.

%% ********************************************************************************************
%%
%%								linux系统启动服务器相关的操作
%%
%% ********************************************************************************************

-define(SHELL, "/bin/sh -s unix:cmd 2>&1").
-define(SHELL_HAVE_WINDOW, "xterm -e ").
-define(PORT_CREATOR_NAME, os_cmd_port_creator).

start_unix_cmd(CommandLine, FatherPid, Type) ->
	if
		Type =:= unix_shell ->
			unix_cmd_shell(CommandLine, FatherPid);
		Type =:= no_unix_shell ->
			no_unix_shell(CommandLine, FatherPid);
		true ->
			FatherPid ! {error, "Type Error"}
	end.

%% linux 有窗口的启动系统
unix_cmd_shell(CommandLine, FatherPid) ->
	case erlang:open_port({spawn, ?SHELL_HAVE_WINDOW ++ CommandLine}, [stream]) of
		Port when erlang:is_port(Port) ->
			FatherPid ! ok;
		Error ->
			io:format("Open Port Error ~p~n", [Error]),
			FatherPid ! error
	end.

%% Linux 没有窗口的启动系统
no_unix_shell(CommandLine, FatherPid) ->
	Tag = erlang:make_ref(),
	{Pid, Mref} =
		erlang:spawn_monitor(fun() ->
									 erlang:process_flag(trap_exit, true),
									 Port = start_port(),
									 erlang:port_command(Port, mk_cmd(CommandLine)),
									 FatherPid ! ok,
									 exit({Tag, unix_get_data(Port)})
									 end),
	receive
		{'DOWN', Mref, _, Pid, {Tag, Result}} ->
			Result;
		{'DOWN', Mref, _, Pid, Reason} ->
			exit(Reason)
	end.

%% 开启端口
start_port() ->
	Ref = erlang:make_ref(),
	Request = {Ref, self()},
	{Pid, Mon} = 
		case erlang:whereis(?PORT_CREATOR_NAME) of
			undefined ->
				erlang:spawn_monitor(fun() -> start_port_srv(Request)
									 end);
			P ->
				P ! Request,
				M = erlang:monitor(process, P),
				{P, M}
		end,
	receive
		{Ref, Port} when erlang:is_port(Port) ->
			erlang:demonitor(Mon, [flush]),
			Port;
		{Ref, Error} ->
			erlang:demonitor(Mon, [flush]),
			exit(Error);
		{'DOWN', Mon, process, Pid, _Reason} ->
			start_port()
	end.

start_port_srv(Request) ->
	{group_leader, GL} = erlang:process_info(erlang:whereis(kernel_sup), group_leader),
	true = group_leader(GL, self()),
	erlang:process_flag(trap_exit, true),
	StayAlive = 
		try
			erlang:register(?PORT_CREATOR_NAME, self())
		catch
			error:_ ->
				false
		end,
	start_port_srv_handle(Request),
	case StayAlive of
		true ->
			start_port_srv_loop();
		false ->
			exiting
	end.

start_port_srv_handle({Ref, Client}) ->
	Reply = 
		try
			erlang:open_port({spawn, ?SHELL}, [stream]) of
				Port when erlang:is_port(Port) ->
					(catch erlang:port_connect(Port, Client)),
					unlink(Port),
					Port
		catch
			error:Reason ->
				{Reason, erlang:get_stacktrace()}
		end,
	Client ! {Ref, Reply}.

start_port_srv_loop() ->
	receive
		{Ref, Client} = Request when erlang:is_reference(Ref),
									 erlang:is_pid(Client) ->
			start_port_srv_handle(Request);
		_Junk ->
			ignore
	end,
	start_port_srv_loop().

%% We no not allow any input to Cmd (hence commands that want
%% to read from standard input will return immediately).
%% Standard error is redirected to standard ouput.
%% We use ^D (= EOT = 4) to mark the end of the stream
mk_cmd(Cmd) when erlang:is_atom(Cmd) ->
	mk_cmd(erlang:atom_to_list(Cmd));
mk_cmd(Cmd) ->
	%% We insert a new line after the command, in case the command
	%% contains o comment character.
	io_lib:format("(~s\n) </dev/null; echo  \"\^D\"\n", [Cmd]).

unix_get_data(Port) ->
	unix_get_data(Port, []).
unix_get_data(Port, Sofar) ->
	receive
		{Port, {data, Bytes}} ->
			case eot(Bytes) of
				{done, Last} ->
					lists:flatten([Sofar | Last]);
				more ->
					unix_get_data(Port, [Sofar | Bytes])
			end;
		{'EXIT', Port, _} ->
			lists:flatten(Sofar)
	end.

eot(Bs) ->
	eot(Bs, []).

eot([4 | _Bs], As) ->
	{done, lists:reverse(As)};
eot([B | Bs], As) ->
	eot(Bs, [B | As]);
eot([], _As) ->
	more.

	
%%
%% Local Functions
%%
%% 关于IP相关的函数工具
match_ip(GiveIp) ->
	case inet:getif() of
		{ok, IFs} ->
			lists:any(fun(IF) -> 
							  {{IP1, IP2, IP3, IP4}, _, _} = IF,
							  IpStr = erlang:integer_to_list(IP1) ++ [$.]
					  					++ erlang:integer_to_list(IP2) ++ [$.]
					  					++ erlang:integer_to_list(IP3) ++ [$.]
					  					++ erlang:integer_to_list(IP4),
							  IpStr =:= GiveIp
							  end, IFs);
		_ ->
			false
	end.

%% 得到本地的IP列表
get_localips() ->
	case inet:getif() of
		{ok, IFs} ->
			SortedIFs =
				lists:sort(fun(IP1, IP2) ->
								   {{I1, I2, I3, I4}, _, _} = IP1,
								   {{J1, J2, J3, J4}, _, _} = IP2,
								   if
									   I1 =:= 192 ->
										   true;
									   J1 =:= 192 ->
										   false;
									   I1 =:= 127 ->
										   true;
									   J1 =:= 127 ->
										   false;
									   I1 < J1 ->
										   true;
									   I1 > J1 ->
										   false;
									   I2 < J2 ->
										   true;
									   I2 > J2 ->
										   false;
									   I3 < J3 ->
										   true;
									   I3 > J3 ->
										   false;
									   I4 < J4 ->
										   true;
									   I4 > J4 ->
										   false;
									   true ->
										   false
								   end
						   end, IFs),
			lists:map(fun(IfConfig) -> 
							  case IfConfig of
								  {{192, 168, I3, I4}, _, _} ->
									  "192.168." ++ integer_to_list(I3) ++ "." ++ integer_to_list(I4);
								  {{127, 0, 0, I4}, _, _} ->
									  "127.0.0." ++ integer_to_list(I4);
								  {{10, I2, I3, I4}, _, _} ->
									  str_util:sprintf("10.~p.~p.~p", [I2, I3, I4]);
								  {{I1, I2, I3, I4}, _, _} ->
									  str_util:sprintf("~p.~p.~p.~p", [I1, I2, I3, I4])
							  end
							  end, SortedIFs);
		_ ->
			[]
	end.




%% **************************************************************************************************
%%	
%%										服务器的停止相关的操作函数
%%
%% **************************************************************************************************
stop_evm(OptionList) ->
	[OptFile | Remain] = OptionList,
	[Type | _] = Remain,
	NewOptFile = OptFile,
	case file:consult(NewOptFile) of
		{error, Reason} ->
			io:format("Error Open File :~p~n", [Reason]);
		{ok, [RunOptions]} ->
			{_, NodesOption} = lists:keyfind(nodes, 1, RunOptions),
			Cookie = get_running_cookie(),
			erlang:set_cookie(node(), Cookie),
			os_wait(2),
			PreFix = run_option:get_prefix(RunOptions),
			NeedWaitNodes =
				lists:map(fun(RunOpt) -> 
								  {SNode, Ip, _, _, _} = RunOpt,
								  case SNode of
									  "db" ->
										  [];
									  _ ->
										  case match_ip(Ip) of
											  true ->
												  stop_single_node(PreFix, SNode, Ip);
											  _ ->
												  []
										  end
								  end
								  end, NodesOption),
			wait_node_stop(lists:filter(fun(WaitNode) -> WaitNode =/= [] end, NeedWaitNodes)),
			
			%% 停止db节点
			WaitDbs = 
				lists:map(fun(RunOpt) -> 
								  {SNode, Ip, _, _, _} = RunOpt, 
								  case SNode of
									  "db" ->
										  case match_ip(Ip) of
											  true ->
												  stop_single_node(PreFix, SNode, Ip);
											  false ->
												  []
										  end;
									  _ ->
										  nothing
								  end
								  end, NodesOption),
			wait_node_stop(lists:filter(fun(WaitNode) -> WaitNode =/= [] end, WaitDbs)),
			
			%% 根据类型进行相关的停服的相关处理
			case Type of
				all ->
					stop_special_node(RunOptions, PreFix)
			end
	end.

%% 得到服务器的cookie
get_running_cookie() ->
	case file:consult(?RUN_OPTION_FILE_NAME) of
		{error, Reason} ->
			io:format("Error Open File :~p~n", [Reason]);
		{ok, [ServerOptions]} ->
			case lists:keyfind(?SERVER_COOKIE, 1, ServerOptions) of
				false ->
					undefined;
				{_, Cookie} ->
					Cookie
			end
	end.

os_wait(N) when erlang:is_integer(N) ->
	CmdLine = 
		case os:type() of
			{win32, nt} ->
				str_util:sprintf("ping 127.0.0.1 -n ~p", [N]);
			_ ->
				str_util:sprintf("sleep ~p", [N])
		end,
	os:cmd(CmdLine);
os_wait(_) ->
	io:format("error wait args~n").	

%% 单个节点的停止
stop_single_node(PreFix, SNode, Ip) ->
	Node = str_util:make_node(PreFix ++ SNode, Ip),
	rpc:cast(Node, node_stop, stop, []),
	io:format("stopping the node ~p ~n", [Node]),
	os_wait(1),
	Node.

%% 等待节点列表中所有的节点都停止掉
wait_node_stop([]) ->
	ok;
wait_node_stop(NodeList) ->
	RemainNodes = 
		lists:filter(fun(Node) -> net_adm:ping(Node) =:= pong end, NodeList),
	os_wait(2),
	wait_node_stop(RemainNodes).

%% 停止特殊的节点
stop_special_node(_RunOptions, _PreFix) ->
	todo.
	%% 现在cache不是特殊节点
%% 	case run_option:get_cache_nodes(RunOptions) of
%% 		[] ->
%% 			io:format("get cache node option error ~n");
%% 		CacheNodesOption ->
%% 			lists:foreach(fun(RunOpt) -> 
%% 								  {SNode, Ip, _, _, _} = RunOpt,
%% 								  case match_ip(Ip) of
%% 									  true ->
%% 										  stop_single_node(PreFix, SNode, Ip);
%% 									  false ->
%% 										  nothing
%% 								  end
%% 								  end, CacheNodesOption)
%% 	end.
