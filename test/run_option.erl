%% Auther: xxw
%% Created :2014-3-11
%% Description:

-module(run_option).
-compile(export_all).

%% 得到run.option 文件的内容
get_options(File) ->
	case file:consult(File) of
		{error, Reason} ->
			io:format("get run.option error:~p~n", [Reason]),
			[];
		{ok, [RunOptions]} ->
			RunOptions
	end.


%% 得到节点名的前置参数
get_prefix(Options) ->
	case lists:keyfind(prefix, 1, Options) of
		false ->
			undefined;
		{_, PreFixOption} ->
			PreFixOption
	end.


%% 得到普通的参数
get_common(Options) ->
	case lists:keyfind(common_option, 1, Options) of
		false ->
			undefined;
		{_, Common} ->
			Common
	end.


%% 得到所有要启动的节点参数
get_nodes(Options) ->
	case lists:keyfind(nodes, 1, Options) of
		false ->
			undefined;
		{_, NodesOption} ->
			NodesOption
	end.


%% 得到beam文件的存储路径
get_beam_dir(Options) ->
	case lists:keyfind(beam_dir, 1, Options) of
		false ->
			undefined;
		{_, BeamDir} ->
			BeamDir
	end.


%% 得到tool节点参数
get_tool_nodes(Options) ->
	case lists:keyfind(tool_nodes, 1, Options) of
		false ->
			undefined;
		{_, ToolNodes} ->
			ToolNodes
	end.


%% 得到mochiweb节点参数
get_mochiweb_nodes(Options) ->
	case lists:keyfind(mochiweb_nodes, 1, Options) of
		false ->
			undefined;
		{_, MochiwebNodes} ->
			MochiwebNodes
	end.


%% 得到cowboy节点参数
get_cowboy_nodes(Options) ->
	case lists:keyfind(cowboy_nodes, 1, Options) of
		false ->
			undefined;
		{_, CowboyNodes} ->
			CowboyNodes
	end.


%% 得到hotwheels节点参数
get_hotwheels_nodes(Options) ->
	case lists:keyfind(hotwheels_nodes, 1, Options) of
		false ->
			undefined;
		{_, HotWheelsNodes} ->
			HotWheelsNodes
	end.


%% 得到RabbitMQ客户端节点参数
get_rabbitmq_client_nodes(Options) ->
	case lists:keyfind(rabbitmq_client_nodes, 1, Options) of
		false ->
			undefined;
		{_, RabbitMQClientNodes} ->
			RabbitMQClientNodes
	end.


%% 得到节点IP列表
get_ip_list(Options) ->
	case lists:keyfind(nodes, 1, Options) of
		false ->
			[];
		{_, NodesOption} ->
			[Ip || {_, Ip, _, _, _, _} <- NodesOption]
	end.
