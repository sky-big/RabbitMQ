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
%% Copyright (c) 2011-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_plugins).
-include("rabbit.hrl").

-export([setup/0, active/0, read_enabled/1, list/1, dependencies/3]).
-export([ensure/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(plugin_name() :: atom()).

-spec(setup/0 :: () -> [plugin_name()]).
-spec(active/0 :: () -> [plugin_name()]).
-spec(list/1 :: (string()) -> [#plugin{}]).
-spec(read_enabled/1 :: (file:filename()) -> [plugin_name()]).
-spec(dependencies/3 :: (boolean(), [plugin_name()], [#plugin{}]) ->
                             [plugin_name()]).
-spec(ensure/1  :: (string()) -> {'ok', [atom()], [atom()]} | {error, any()}).
-endif.

%%----------------------------------------------------------------------------
%% 刷新插件系统中所有插件，需要停止的插件立刻停止，需要激活的插件则立刻激活
ensure(FileJustChanged0) ->
	%% 先拿到enabled_plugins允许的插件存储的文件路径
	{ok, OurFile0} = application:get_env(rabbit, enabled_plugins_file),
	%% 从enabled_plugins文件中读取当前RabbitMQ系统配置的允许的插件列表
	FileJustChanged = filename:nativename(FileJustChanged0),
	OurFile = filename:nativename(OurFile0),
	case OurFile of
		FileJustChanged ->
			%% 从enabled_plugins文件中读取当前RabbitMQ系统配置的允许的插件列表
			Enabled = read_enabled(OurFile),
			%% 将配置中允许的插件解压放到指定目录下
			Wanted = prepare_plugins(Enabled),
			%% 得到当前RabbitMQ系统已经激活的插件app列表
			Current = active(),
			%% 得到需要激活的插件应用
			Start = Wanted -- Current,
			%% 得到需要删除的插件应用
			Stop = Current -- Wanted,
			%% 启动需要激活的插件app
			rabbit:start_apps(Start),
			%% We need sync_notify here since mgmt will attempt to look at all
			%% the modules for the disabled plugins - if they are unloaded
			%% that won't work.
			%% 发布插件的变化事件到rabbit_event
			ok = rabbit_event:sync_notify(plugins_changed, [{enabled,  Start},
															{disabled, Stop}]),
			%% 将需要停止的插件app停止掉
			rabbit:stop_apps(Stop),
			%% 将需要停止的插件清除掉
			clean_plugins(Stop),
			%% 打印插件变化的日志
			rabbit_log:info("Plugins changed; enabled ~p, disabled ~p~n",
							[Start, Stop]),
			{ok, Start, Stop};
		_ ->
			{error, {enabled_plugins_mismatch, FileJustChanged, OurFile}}
	end.


%% @doc Prepares the file system and installs all enabled plugins.
%% 先将保存插件beam文件的目录全部删除掉，然后根据配置的激活的插件列表，将插件重新解压放到指定的目录
setup() ->
	{ok, ExpandDir}   = application:get_env(rabbit, plugins_expand_dir),
	
	%% Eliminate the contents of the destination directory
	%% 消除目标目录中的内容
	case delete_recursively(ExpandDir) of
		ok          -> ok;
		{error, E1} -> throw({error, {cannot_delete_plugins_expand_dir,
									  [ExpandDir, E1]}})
	end,
	
	%% 拿到配置当前RabbitMQ系统允许的插件配置文件路径
	{ok, EnabledFile} = application:get_env(rabbit, enabled_plugins_file),
	%% 从enabled_plugins文件中读取当前RabbitMQ系统配置的允许的插件列表
	Enabled = read_enabled(EnabledFile),
	%% 将配置中允许的插件解压放到指定目录下
	prepare_plugins(Enabled).


%% @doc Lists the plugins which are currently running.
%% 得到当前RabbitMQ系统已经激活的插件app列表
active() ->
	{ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
	InstalledPlugins = plugin_names(list(ExpandDir)),
	[App || {App, _, _} <- rabbit_misc:which_applications(),
			lists:member(App, InstalledPlugins)].

%% @doc Get the list of plugins which are ready to be enabled.
%% 列出PluginsDir目录下的所有插件
list(PluginsDir) ->
	%% 所有的插件都是以.ez结尾
	EZs = [{ez, EZ} || EZ <- filelib:wildcard("*.ez", PluginsDir)],
	%% 拿到当前RabbitMQ系统中已经启动的App
	FreeApps = [{app, App} ||
				App <- filelib:wildcard("*/ebin/*.app", PluginsDir)],
	{AvailablePlugins, Problems} =
		lists:foldl(fun ({error, EZ, Reason}, {Plugins1, Problems1}) ->
							 {Plugins1, [{EZ, Reason} | Problems1]};
					   (Plugin = #plugin{}, {Plugins1, Problems1}) ->
							{[Plugin | Plugins1], Problems1}
					end, {[], []},
					%% 组装得到所有的插件已经存在的app信息
					[plugin_info(PluginsDir, Plug) || Plug <- EZs ++ FreeApps]),
	case Problems of
		[] -> ok;
		%% 如果有插件的读取有问题，则将问题打印在日志中
		_  -> rabbit_log:warning(
				"Problem reading some plugins: ~p~n", [Problems])
	end,
	%% 根据插件名字过滤掉OTP已经支持的插件
	Plugins = lists:filter(fun(P) -> not plugin_provided_by_otp(P) end,
						   AvailablePlugins),
	%% 将当前传入的插件列表中的插件依赖性减去OTP的应用
	ensure_dependencies(Plugins).

%% @doc Read the list of enabled plugins from the supplied term file.
%% 从enabled_plugins文件中读取当前RabbitMQ系统配置的允许的插件列表
read_enabled(PluginsFile) ->
	case rabbit_file:read_term_file(PluginsFile) of
		{ok, [Plugins]} -> Plugins;
		{ok, []}        -> [];
		{ok, [_|_]}     -> throw({error, {malformed_enabled_plugins_file,
										  PluginsFile}});
		{error, enoent} -> [];
		{error, Reason} -> throw({error, {cannot_read_enabled_plugins_file,
										  PluginsFile, Reason}})
	end.

%% @doc Calculate the dependency graph from <i>Sources</i>.
%% When Reverse =:= true the bottom/leaf level applications are returned in
%% the resulting list, otherwise they're skipped.
dependencies(Reverse, Sources, AllPlugins) ->
	%% 创建插件之间的依赖有向图
	{ok, G} = rabbit_misc:build_acyclic_graph(
				fun ({App, _Deps}) -> [{App, App}] end,
				fun ({App,  Deps}) -> [{App, Dep} || Dep <- Deps] end,
				[{Name, Deps} || #plugin{name         = Name,
										 dependencies = Deps} <- AllPlugins]),
	Dests = case Reverse of
				false -> digraph_utils:reachable(Sources, G);
				true  -> digraph_utils:reaching(Sources, G)
			end,
	%% 将创建的有向图删除掉
	true = digraph:delete(G),
	Dests.

%% For a few known cases, an externally provided plugin can be trusted.
%% In this special case, it overrides the plugin.
%% 查看插件是否是OTP自己有提供
plugin_provided_by_otp(#plugin{name = eldap}) ->
	%% eldap was added to Erlang/OTP R15B01 (ERTS 5.9.1). In this case,
	%% we prefer this version to the plugin.
	rabbit_misc:version_compare(erlang:system_info(version), "5.9.1", gte);

plugin_provided_by_otp(_) ->
	false.

%% Make sure we don't list OTP apps in here, and also that we detect
%% missing dependencies.
%% 将当前传入的插件列表中的插件依赖性减去OTP的应用
ensure_dependencies(Plugins) ->
	%% 根据传入的插件列表得到所有插件的名字
	Names = plugin_names(Plugins),
	NotThere = [Dep || #plugin{dependencies = Deps} <- Plugins,
					   Dep                          <- Deps,
					   not lists:member(Dep, Names)],
	{OTP, Missing} = lists:partition(fun is_loadable/1, lists:usort(NotThere)),
	case Missing of
		[] -> ok;
		%% 如果有依赖的app没有找到，则将当前插件的名字拿出来，然后抛出异常
		_  -> Blame = [Name || #plugin{name         = Name,
									   dependencies = Deps} <- Plugins,
							   lists:any(fun (Dep) ->
												  lists:member(Dep, Missing)
										 end, Deps)],
			  throw({error, {missing_dependencies, Missing, Blame}})
	end,
	%% 将当前传入的插件列表中的插件依赖性减去OTP的应用
	[P#plugin{dependencies = Deps -- OTP}
			 || P = #plugin{dependencies = Deps} <- Plugins].


%% 判断App文件是否能够加载
is_loadable(App) ->
	case application:load(App) of
		{error, {already_loaded, _}} -> true;
		ok                           -> application:unload(App),
										true;
		_                            -> false
	end.

%%----------------------------------------------------------------------------
%% 将配置中允许的插件解压放到指定目录下
prepare_plugins(Enabled) ->
	%% 拿到当前存放插件的绝对路径
	{ok, PluginsDistDir} = application:get_env(rabbit, plugins_dir),
	%% 拿到当前扩展插件的绝对路径
	{ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
	
	%% 列出插件目录下的所有插件
	AllPlugins = list(PluginsDistDir),
	%% 拿到Enabled允许的插件依赖的app
	Wanted = dependencies(false, Enabled, AllPlugins),
	%% 列出依赖的插件信息
	WantedPlugins = lookup_plugins(Wanted, AllPlugins),
	
	%% 确保扩展插件目录的存在
	case filelib:ensure_dir(ExpandDir ++ "/") of
		ok          -> ok;
		{error, E2} -> throw({error, {cannot_create_plugins_expand_dir,
									  [ExpandDir, E2]}})
	end,
	
	%% 将得到的插件解压缩到plugins-expand后缀的文件夹中
	[prepare_plugin(Plugin, ExpandDir) || Plugin <- WantedPlugins],
	
	%% 将已经加载的插件代码目录添加到代码搜寻路径中
	[prepare_dir_plugin(PluginAppDescPath) ||
	   PluginAppDescPath <- filelib:wildcard(ExpandDir ++ "/*/ebin/*.app")],
	Wanted.


%% 将需要停止的插件清除掉
clean_plugins(Plugins) ->
	{ok, ExpandDir} = application:get_env(rabbit, plugins_expand_dir),
	[clean_plugin(Plugin, ExpandDir) || Plugin <- Plugins].


clean_plugin(Plugin, ExpandDir) ->
	%% 得到当前app的所有模块名字
	{ok, Mods} = application:get_key(Plugin, modules),
	%% 将插件app卸载
	application:unload(Plugin),
	%% 将该插件app的所有模块的代码从虚拟机中卸载掉
	[begin
		 code:soft_purge(Mod),
		 code:delete(Mod),
		 false = code:is_loaded(Mod)
	 end || Mod <- Mods],
	%% 将保存改插件的目录删除掉
	delete_recursively(rabbit_misc:format("~s/~s", [ExpandDir, Plugin])).


%% 将PluginAppDescPath这个路径下的插件代码目录添加到代码搜寻路径中
prepare_dir_plugin(PluginAppDescPath) ->
	PluginEbinDir = filename:dirname(PluginAppDescPath),
	Plugin = filename:basename(PluginAppDescPath, ".app"),
	%% 添加的代码
	option_set_server:cover_plugin_beam(filelib:wildcard(PluginEbinDir++ "/*.beam")),
	code:add_patha(PluginEbinDir),
	case filelib:wildcard(PluginEbinDir++ "/*.beam") of
		[] ->
			ok;
		[BeamPath | _] ->
			Module = list_to_atom(filename:basename(BeamPath, ".beam")),
			case code:ensure_loaded(Module) of
				{module, _} ->
					ok;
				{error, badfile} ->
					rabbit_log:error("Failed to enable plugin \"~s\": "
									 "it may have been built with an "
									 "incompatible (more recent?) "
									 "version of Erlang~n", [Plugin]),
					throw({plugin_built_with_incompatible_erlang, Plugin});
				Error ->
					throw({plugin_module_unloadable, Plugin, Error})
			end
	end.

%%----------------------------------------------------------------------------

delete_recursively(Fn) ->
	case rabbit_file:recursive_delete([Fn]) of
		ok                 -> ok;
		{error, {Path, E}} -> {error, {cannot_delete, Path, E}}
	end.


%% 解压插件
prepare_plugin(#plugin{type = ez, location = Location}, ExpandDir) ->
	zip:unzip(Location, [{cwd, ExpandDir}]);

prepare_plugin(#plugin{type = dir, name = Name, location = Location},
			   ExpandDir) ->
	rabbit_file:recursive_copy(Location, filename:join([ExpandDir, Name])).


%% 组装plugin数据结构
%% 当前是真实的插件
plugin_info(Base, {ez, EZ0}) ->
	%% 组装插件压缩文件的绝对路径
	EZ = filename:join([Base, EZ0]),
	%% 根据插件的绝对路径从压缩文件中读取app文件
	case read_app_file(EZ) of
		{application, Name, Props} -> mkplugin(Name, Props, ez, EZ);
		{error, Reason}            -> {error, EZ, Reason}
	end;

%% 当前插件直接就是app文件
plugin_info(Base, {app, App0}) ->
	%% 拿到app文件的绝对路径
	App = filename:join([Base, App0]),
	%% 从app文件中直接将配置数据读取出来
	case rabbit_file:read_term_file(App) of
		{ok, [{application, Name, Props}]} ->
			mkplugin(Name, Props, dir,
					 filename:absname(
					   filename:dirname(filename:dirname(App))));
		{error, Reason} ->
			{error, App, {invalid_app, Reason}}
	end.


%% 根据传入的参数组装成内部的plugin数据结构
mkplugin(Name, Props, Type, Location) ->
	%% 拿到当前插件的版本号
	Version = proplists:get_value(vsn, Props, "0"),
	%% 拿到当前插件的描述信息
	Description = proplists:get_value(description, Props, ""),
	%% 拿到当前插件依赖的app列表
	Dependencies = proplists:get_value(applications, Props, []),
	%% 组装plugin数据结构
	#plugin{name = Name, version = Version, description = Description,
			dependencies = Dependencies, location = Location, type = Type}.


%% 根据插件的绝对路径从压缩文件中读取app文件
read_app_file(EZ) ->
	case zip:list_dir(EZ) of
		{ok, [_ | ZippedFiles]} ->
			%% 从插件压缩文件中查找app后缀的文件
			case find_app_files(ZippedFiles) of
				[AppPath | _] ->
					%% 根据从压缩文件找到的app文件名读取出该文件的二进制数据
					{ok, [{AppPath, AppFile}]} =
						zip:extract(EZ, [{file_list, [AppPath]}, memory]),
					%% 将从插件压缩文件中读取出的app二进制数据转化为Erlang内部的数据结构
					parse_binary(AppFile);
				[] ->
					{error, no_app_file}
			end;
		{error, Reason} ->
			{error, {invalid_ez, Reason}}
	end.


%% 从插件压缩文件中查找app后缀的文件
find_app_files(ZippedFiles) ->
	{ok, RE} = re:compile("^.*/ebin/.*.app$"),
	[Path || {zip_file, Path, _, _, _, _} <- ZippedFiles,
			 re:run(Path, RE, [{capture, none}]) =:= match].


%% 将从插件压缩文件中读取出的app二进制数据转化为Erlang内部的数据结构
parse_binary(Bin) ->
	try
		{ok, Ts, _} = erl_scan:string(binary_to_list(Bin)),
		{ok, Term} = erl_parse:parse_term(Ts),
		Term
	catch
		Err -> {error, {invalid_app, Err}}
	end.


%% 根据传入的插件列表得到所有插件的名字
plugin_names(Plugins) ->
	[Name || #plugin{name = Name} <- Plugins].


%% 从所有的插件中列出Names列表中的插件
lookup_plugins(Names, AllPlugins) ->
	[P || P = #plugin{name = Name} <- AllPlugins, lists:member(Name, Names)].
