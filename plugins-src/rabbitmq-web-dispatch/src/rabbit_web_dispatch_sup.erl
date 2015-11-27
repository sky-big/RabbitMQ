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
%% Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_web_dispatch_sup).

-behaviour(supervisor).

-define(SUP, ?MODULE).

%% External exports
-export([start_link/0, ensure_listener/1, stop_listener/1]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
%% 启动rabbit_web_dispatch_sup监督进程的入口函数
start_link() ->
	supervisor:start_link({local, ?SUP}, ?MODULE, []).


%% 根据监听信息，使用mochiweb开源框架，启动一个Http服务器
ensure_listener(Listener) ->
	%% 首先从监听信息中拿到监听端口
	case proplists:get_value(port, Listener) of
		undefined ->
			{error, {no_port_given, Listener}};
		_ ->
			Child = {{rabbit_web_dispatch_web, name(Listener)},
					 {mochiweb_http, start, [mochi_options(Listener)]},
					 transient, 5000, worker, dynamic},
			case supervisor:start_child(?SUP, Child) of
				{ok,                      _}  -> new;
				{error, {already_started, _}} -> existing;
				{error, {E, _}}               -> check_error(Listener, E)
			end
	end.


stop_listener(Listener) ->
	Name = name(Listener),
	ok = supervisor:terminate_child(?SUP, {rabbit_web_dispatch_web, Name}),
	ok = supervisor:delete_child(?SUP, {rabbit_web_dispatch_web, Name}).

%% @spec init([[instance()]]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
	Registry = {rabbit_web_dispatch_registry,
				{rabbit_web_dispatch_registry, start_link, []},
				transient, 5000, worker, dynamic},
	{ok, {{one_for_one, 10, 10}, [Registry]}}.

%% ----------------------------------------------------------------------
%% 组装Http服务器的启动参数
mochi_options(Listener) ->
	[%% 名字
	 {name, name(Listener)},
	 %% 回调循环处理函数
	 {loop, loopfun(Listener)} |
		 ssl_config(proplists:delete(
					  name, proplists:delete(ignore_in_use, Listener)))].


%% Mochiweb开源Http服务器得到客户端的请求后的回调函数
loopfun(Listener) ->
	fun (Req) ->
			 %% 从rabbit_web_dispatch_registry进程读取到处理handler
			 case rabbit_web_dispatch_registry:lookup(Listener, Req) of
				 no_handler ->
					 Req:not_found();
				 {error, Reason} ->
					 Req:respond({500, [], "Registry Error: " ++ Reason});
				 {handler, Handler} ->
					 Handler(Req)
			 end
	end.


%% 获得Http服务器的名字
name(Listener) ->
	Port = proplists:get_value(port, Listener),
	list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(Port)).


%% ssl相关配置
ssl_config(Options) ->
	case proplists:get_value(ssl, Options) of
		true -> rabbit_networking:ensure_ssl(),
				case rabbit_networking:poodle_check('HTTP') of
					ok     -> case proplists:get_value(ssl_opts, Options) of
								  undefined -> auto_ssl(Options);
								  _         -> fix_ssl(Options)
							  end;
					danger -> proplists:delete(ssl, Options)
				end;
		_    -> Options
	end.


auto_ssl(Options) ->
	{ok, ServerOpts} = application:get_env(rabbit, ssl_options),
	Remove = [verify, fail_if_no_peer_cert],
	SSLOpts = [{K, V} || {K, V} <- ServerOpts,
						 not lists:member(K, Remove)],
	fix_ssl([{ssl_opts, SSLOpts} | Options]).


fix_ssl(Options) ->
	SSLOpts = proplists:get_value(ssl_opts, Options),
	rabbit_misc:pset(ssl_opts,
					 rabbit_networking:fix_ssl_options(SSLOpts), Options).


check_error(Listener, Error) ->
	Ignore = proplists:get_value(ignore_in_use, Listener, false),
	case {Error, Ignore} of
		{eaddrinuse, true} -> ignore;
		_                  -> exit({could_not_start_listener, Listener, Error})
	end.
