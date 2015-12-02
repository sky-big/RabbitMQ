#!/usr/bin/env escript

main([]) ->
	compile_all();

main(Options) ->
	compile_all(Options).

%% 有配置参数的编译
compile_all(Options) ->
	case mmake:all(get_cpu_cores(), [Options]) of
		up_to_date ->
			halt(0);
		error ->
			halt(1)
	end.

%% 按照默认配置参数编译源文件
compile_all() ->
	code:add_patha("../ebin"),
	case mmake:all(get_cpu_cores()) of
		up_to_date ->
%% 			make_version(),
			halt(0);
		error ->
			halt(1)
	end.

%% 得到cpu的个数
get_cpu_cores() ->
	case os:type() of
		{unix, _} ->
			CoreS = erlang:system_info(logical_processors) * 2;
		_ ->
			CoreS = erlang:system_info(logical_processors) - 1
	end,
	erlang:max(CoreS, 1).
