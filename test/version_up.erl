%% Author: xxw
%% Created: 2014-3-12
%% Description: TODO: Add description to version_up
-module(version_up).

-define(VERSION_UP_ETS, version_up_ets).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([
		 init/0,
		 up_all/0,
		 up_code/0,
		 get_all_plugin_mod_name/0,
		 move_project_plugin_beam/0
		 ]).

%%
%% API Functions
%%
init() ->
	try
		ets:new(?VERSION_UP_ETS, [named_table, public, set])
	catch
		E:R ->
			io:format("version_up init error:~p~n", [{E, R, erlang:get_stacktrace()}])
	end,
	ets:insert(?VERSION_UP_ETS, get_all_beam_version()).


%% 得到当前所有的beam文件的版本号
get_all_beam_version() ->
	AllBeamFile = get_all_beam("./"),
	lists:map(fun(BeamFile) -> 
					  case beam_lib:version(BeamFile) of
						  {ok, {Mod, Version}} ->
							  {Mod, Version};
						  _ ->
							  {BeamFile, 0}
					  end
			  end, AllBeamFile).


%% 得到所有的beam文件的路径
get_all_beam(Dir) ->
	case file:list_dir(Dir) of
		{ok, Files} ->
			BeamFiles = 
				lists:filter(fun(BeamFile) -> 
									 case lists:reverse(BeamFile) of
										 "maeb" ++ _ ->
											 true;
										 _ -> 
											 false
									 end
							 end, Files),
			lists:map(fun(BeamFile) -> 
							  Dir ++ BeamFile
					  end, BeamFiles);
		{error, Reason} ->
			io:format("get all beam error~p~n", [{Reason, erlang:get_stacktrace()}])
	end.


up_all() ->
	%% 获取集群的所有正在运行的节点
	AllNodes = rabbit_mnesia:cluster_nodes(running),
	[rpc:call(OneClusterNode, ?MODULE, move_project_plugin_beam, []) || OneClusterNode <- AllNodes],
	lists:foreach(fun(Node) ->
						  rpc:call(Node, ?MODULE, up_code, [])
				  end, AllNodes),
	io:format("version_up: up code success~n").


up_code() ->
	AllNewBeamFiles = get_all_beam_version(),
	NeedUpBeamFiles =
		lists:filter(fun({BeamFile, NewVersion}) -> 
							 OldVersion = get_old_beam_version(BeamFile),
							 case (OldVersion =/= NewVersion) or (NewVersion =:= 0) of
								 true ->
									 true;
								 false ->
									 false
							 end
					 end, AllNewBeamFiles),
	NeedUpBeams = lists:map(fun({BeamFile, _}) -> BeamFile end, NeedUpBeamFiles),
	up_modules(NeedUpBeams),
	%% 将最新的beam版本号存储到ets表里
	ets:insert(?VERSION_UP_ETS, AllNewBeamFiles).


get_old_beam_version(BeamFile) ->
	case ets:lookup(?VERSION_UP_ETS, BeamFile) of
		[] ->
			0;
		[{_, OldVersion}] ->
			OldVersion
	end.


up_modules(NeedUpBeams) ->
	case NeedUpBeams of
		[] ->
			nothing;
		_ ->
			io:format("----------------~p updating--------------------~n", [node()]),
			lists:foreach(fun(BeamFile) -> 
								  Result = c:l(BeamFile),
								  {file, Path} = code:is_loaded(BeamFile),
								  io:format("\t ~p~n", [{Path, BeamFile, Result}])
						  end, NeedUpBeams)
	end.

%%
%% Local Functions
%%
%% 移动工程插件beam到插件目录
move_project_plugin_beam() ->
	{ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
	[prepare_dir_plugin(PluginAppDescPath) ||
	   PluginAppDescPath <- filelib:wildcard(ExpandDir ++ "/*/ebin/*.app")].


%% 将PluginAppDescPath这个路径下的插件代码目录添加到代码搜寻路径中
prepare_dir_plugin(PluginAppDescPath) ->
	PluginEbinDir = filename:dirname(PluginAppDescPath),
	%% 将本工程的beam文件覆盖到插件的目录
	option_set_server:cover_plugin_beam(filelib:wildcard(PluginEbinDir ++ "/*.beam")).


%% 获得所有插件的模块名字
get_all_plugin_mod_name() ->
	{ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
	lists:foldl(fun(OnePluginAppDescPath, AccInfo) ->
						OnePluginBeamPath = filename:dirname(OnePluginAppDescPath),
						BeamModList = [filename:basename(OneBeamModPath) || OneBeamModPath <- filelib:wildcard(OnePluginBeamPath ++ "/*.beam")],
						BeamModList ++ AccInfo
				end, [], filelib:wildcard(ExpandDir ++ "/*/ebin/*.app")).
