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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1, reset_dispatcher/1]).

-include("rabbit_mgmt.hrl").
-include("amqp_client.hrl").

-define(CONTEXT, rabbit_mgmt).						%% 在rabbit_web_dispatch应用中注册的名字
-define(STATIC_PATH, "priv/www").

%% rabbitmq_management应用启动的入口函数接口
start(_Type, _StartArgs) ->
	%% 从rabbitmq_management应用的配置中拿到监听信息
	{ok, Listener} = application:get_env(rabbitmq_management, listener),
	%% 设置网页管理器的日志
	setup_wm_logging(),
	%% 向rabbit_web_dispatch应用注册创建一个Mochiweb开源的Http服务器
	register_context(Listener, []),
	%% 使用error_logger打印日志
	log_startup(Listener),
	rabbit_mgmt_sup_sup:start_link().


%% rabbitmq_management应用停止回调接口
stop(_State) ->
	unregister_context(),
	ok.

%% At the point at which this is invoked we have both newly enabled
%% apps and about-to-disable apps running (so that
%% rabbit_mgmt_reset_handler can look at all of them to find
%% extensions). Therefore we have to explicitly exclude
%% about-to-disable apps from our new dispatcher.
%% 向rabbit_web_dispatch应用重置Http服务器
reset_dispatcher(IgnoreApps) ->
	%% 先将rabbit_web_dispatch应用中老的Http服务器关闭
	unregister_context(),
	%% 然后根据配置信息重新在rabbit_web_dispatch应用启动Http服务器
	{ok, Listener} = application:get_env(rabbitmq_management, listener),
	register_context(Listener, IgnoreApps).


%% 向rabbit_web_dispatch应用注册创建一个Mochiweb开源的Http服务器
register_context(Listener, IgnoreApps) ->
	rabbit_web_dispatch:register_context_handler(
	  ?CONTEXT, Listener, "", make_loop(IgnoreApps), "RabbitMQ Management").


%% 向rabbit_web_dispatch应用请求关闭关闭当前管理器注册的Http服务器
unregister_context() ->
	rabbit_web_dispatch:unregister_context(?CONTEXT).


%% 自己在rabbit_web_dispatch应用启动的Http服务器的回调处理函数接口，客户端发送过来的请求都会通过该接口来进行处理
make_loop(IgnoreApps) ->
	%% 得到分发信息
	Dispatch = rabbit_mgmt_dispatcher:build_dispatcher(IgnoreApps),
	WMLoop = rabbit_webmachine:makeloop(Dispatch),
	%% 获取本地应用有"priv/www"目录的绝对路径
	LocalPaths = [filename:join(module_path(M), ?STATIC_PATH) ||
					M <- rabbit_mgmt_dispatcher:modules(IgnoreApps)],
	fun(Req) -> respond(Req, LocalPaths, WMLoop) end.


module_path(Module) ->
	{file, Here} = code:is_loaded(Module),
	filename:dirname(filename:dirname(Here)).


%% rabbitmq_management管理器处理客户端的Http请求
respond(Req, LocalPaths, WMLoop) ->
	%% 从Req请求中得到客户端请求的路径
	Path = Req:get(path),
	Redirect = fun(L) -> {301, [{"Location", L}], ""} end,
	io:format("AAAAAAAAAAAAAAAAAAA:~p~n", [Path]),
	case Path of
		"/api/" ++ Rest when length(Rest) > 0 ->
			WMLoop(Req);
		"" ->
			Req:respond(Redirect("/"));
		"/mgmt/" ->
			Req:respond(Redirect("/"));
		"/mgmt" ->
			Req:respond(Redirect("/"));
		"/" ++ Stripped ->
			serve_file(Req, Stripped, LocalPaths, Redirect)
	end.


%% 将客户端传送文件的接口
serve_file(Req, Path, [LocalPath], _Redirect) ->
	Req:serve_file(Path, LocalPath);

serve_file(Req, Path, [LocalPath | Others], Redirect) ->
	Path1 = filename:join([LocalPath, Path]),
	case filelib:is_regular(Path1) of
		true  -> Req:serve_file(Path, LocalPath);
		false -> case filelib:is_regular(Path1 ++ "/index.html") of
					 true  -> index(Req, Path, LocalPath, Redirect);
					 false -> serve_file(Req, Path, Others, Redirect)
				 end
	end.


index(Req, Path, LocalPath, Redirect) ->
	case lists:reverse(Path) of
		""       -> Req:serve_file("index.html", LocalPath);
		"/" ++ _ -> Req:serve_file(Path ++ "index.html", LocalPath);
		_        -> Req:respond(Redirect(Path ++ "/"))
	end.


%% 设置网页管理器的日志
setup_wm_logging() ->
	rabbit_webmachine:setup(),
	{ok, LogDir} = application:get_env(rabbitmq_management, http_log_dir),
	case LogDir of
		none -> ok;
		_    -> webmachine_log:add_handler(webmachine_log_handler, [LogDir])
	end.


%% 使用error_logger打印日志
log_startup(Listener) ->
	rabbit_log:info("Management plugin started. Port: ~w~n", [port(Listener)]).


port(Listener) ->
	proplists:get_value(port, Listener).