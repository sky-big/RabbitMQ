%%% -------------------------------------------------------------------
%%% Author  : xxw
%%% Description :
%%%
%%% Created : 2015-11-30
%%% -------------------------------------------------------------------
-module(option_set_server).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------
%% External exports
-export([
		 start/0,
		 cover_plugin_beam/1
		]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {}).

%% ====================================================================
%% External functions
%% ====================================================================
start() ->
	case erlang:whereis(?MODULE) of
		undefined ->
			gen_server:start_link({local, ?MODULE}, ?MODULE, [], []);
		_ ->
			nothing
	end.

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([]) ->
	case run_option:get_options("../options/run.option") of
		[] ->
			io:format("start_cluster start find run.option is empty~n");
		Options ->
			%% 统一节点的cookie
			Cookie = proplists:get_value(cookie, Options),
			erlang:set_cookie(node(), Cookie),
			%% 如果是windows则修改cmd窗口的标题等相关东西
			case os:type() of
				{win32, nt} ->
					SNodeName = node_util:get_node_name(node()),
					TitleCmdLine = "title " ++ SNodeName,
					os:cmd(TitleCmdLine);
				_ ->
					nothing
			end,
			%% 启动RabbitMQ系统相关应用
			rabbit:boot(),
			OtherClusterNodes = rabbit_mnesia:cluster_nodes(all) -- [node()],
			case OtherClusterNodes of
				[] ->
					%% 加入集群的动作
					join_cluster(Options);
				_ ->
					nothing
			end,
			%% 设置队列的镜像队列
			set_mirror_queue(),
			%% DBG监视测试
			dbg_test()
	end,
	%% 代码更新相关初始化
	version_up:init(),
	{ok, #state{}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
%% 加入集群的动作
join_cluster(Options) ->
	%% 如果是第一次启动的节点，如果配置有要加入集群的节点则加入该集群
	SelfNodeName = node_util:get_node_name(node()),
	Prefix = proplists:get_value(prefix, Options),
	RealNodeName = string:sub_string(SelfNodeName, string:len(Prefix) + 1, string:len(SelfNodeName)),
	MainClusterNodeName = proplists:get_value(main_cluster_node, Options),
	case MainClusterNodeName =:= RealNodeName of
		true ->
			nothing;
		false ->
			AllNodes = proplists:get_value(nodes, Options),
			case lists:keyfind(RealNodeName, 1, AllNodes) of
				false ->
					nothing;
				{_, Ip, _, _, NodeType, _} ->
					MainClusterRealNodeName = list_to_atom(str_util:sprintf("~s@~s", [Prefix ++ MainClusterNodeName, Ip])),
					%% 先将当前节点rabbit应用停止
					rabbit:stop(),
					%% 加入集群
					rabbit_mnesia:join_cluster(MainClusterRealNodeName, NodeType),
					%% 重新启动rabbit应用
					rabbit:start()
			end
	end.


%% 设置队列的镜像队列
set_mirror_queue() ->
	rabbit_policy:parse_set(<<"/">>, "queue_mirror", ".*", "{\"ha-mode\":\"all\"}", "0", <<"queues">>).


%% DBG监视测试
dbg_test() ->
	dbg_test1(),
	dbg_test2().


dbg_test1() ->
	{ok, Log} = file:open("../log/flow_trace1", [write, append]),
	Tfun = fun(Msg, _) ->
				   io:format(Log,"~p ~n", [Msg])
		   end,
	dbg:tracer(process, {Tfun, null}),
	dbg:tp({credit_flow, send, '_'}, []),
	dbg:tp({credit_flow, ack, '_'}, []),
	dbg:p(all, c).


dbg_test2() ->
	{ok, Log} = file:open("../log/flow_trace2", [write, append]),
	Tfun = fun(Msg, _) ->
				   io:format(Log,"~p ~n", [Msg])
		   end,
	dbg:tracer(process, {Tfun, null}),
	dbg:tpl({credit_flow, unblock, '_'}, []),
	dbg:p(all, c).


%% 将本工程的插件beam文件覆盖到插件beam存储的位置
cover_plugin_beam(PluginsBeamList) ->
	lists:foreach(fun(OnePluginsBeam) ->
						  RealBeamName = filename:basename(OnePluginsBeam, ".beam"),
						  SelfBeamPath = filename:join(["./", RealBeamName]) ++ ".beam",
						  case rabbit_file:is_file(SelfBeamPath) of
							  true ->
								  rabbit_file:delete(OnePluginsBeam),
								  DestionName = filename:dirname(OnePluginsBeam) ++ "/" ++ RealBeamName ++ ".beam",
								  rabbit_file:recursive_copy(SelfBeamPath, DestionName);
							  false ->
								  nothing
						  end
				  end, PluginsBeamList).