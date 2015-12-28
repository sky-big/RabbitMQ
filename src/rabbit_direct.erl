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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_direct).
%% 此模块是不通过TCP，而是直接调用RabbitMQ系统的接口启动rabbit_channel进程来发布或者消费消息等操作

-export([boot/0, force_event_refresh/1, list/0, connect/5,
         start_channel/9, disconnect/2]).
%% Internal
-export([list_local/0]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(boot/0 :: () -> 'ok').
-spec(force_event_refresh/1 :: (reference()) -> 'ok').
-spec(list/0 :: () -> [pid()]).
-spec(list_local/0 :: () -> [pid()]).
-spec(connect/5 :: (({'none', 'none'} | {rabbit_types:username(), 'none'} |
                     {rabbit_types:username(), rabbit_types:password()}),
                    rabbit_types:vhost(), rabbit_types:protocol(), pid(),
                    rabbit_event:event_props()) ->
                        rabbit_types:ok_or_error2(
                          {rabbit_types:user(), rabbit_framing:amqp_table()},
                          'broker_not_found_on_node' |
                          {'auth_failure', string()} | 'access_refused')).
-spec(start_channel/9 ::
        (rabbit_channel:channel_number(), pid(), pid(), string(),
         rabbit_types:protocol(), rabbit_types:user(), rabbit_types:vhost(),
         rabbit_framing:amqp_table(), pid()) -> {'ok', pid()}).
-spec(disconnect/2 :: (pid(), rabbit_event:event_props()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% rabbit_direct启动步骤执行的boot函数(启动rabbit_direct_client_sup监督进程，该监督进程的动态启动子进程是执行{rabbit_channel_sup, start_link, []})
boot() -> rabbit_sup:start_supervisor_child(
			rabbit_direct_client_sup, rabbit_client_sup,
			[{local, rabbit_direct_client_sup},
			 {rabbit_channel_sup, start_link, []}]).


force_event_refresh(Ref) ->
	[Pid ! {force_event_refresh, Ref} || Pid <- list()],
	ok.


%% 获取本节点启动的rabbit_drecit相关的进程
list_local() ->
	pg_local:get_members(rabbit_direct).


%% 列出当前所有的direct类型的连接
list() ->
	rabbit_misc:append_rpc_all_nodes(rabbit_mnesia:cluster_nodes(running),
									 rabbit_direct, list_local, []).

%%----------------------------------------------------------------------------
%% 开始连接的接口
connect({none, _}, VHost, Protocol, Pid, Infos) ->
	connect0(fun () -> {ok, rabbit_auth_backend_dummy:user()} end,
			 VHost, Protocol, Pid, Infos);


%% 开始连接的接口，只带有用户名
connect({Username, none}, VHost, Protocol, Pid, Infos) ->
	connect0(fun () -> rabbit_access_control:check_user_login(Username, []) end,
			 VHost, Protocol, Pid, Infos);


%% 开始连接的接口，带有用户名和密码
connect({Username, Password}, VHost, Protocol, Pid, Infos) ->
	%% 检查用户名和该用户的密码是否正确
	connect0(fun () -> rabbit_access_control:check_user_pass_login(
						 Username, Password) end,
			 VHost, Protocol, Pid, Infos).


%% 开始连接的接口
connect0(AuthFun, VHost, Protocol, Pid, Infos) ->
	%% 判断rabbit应用是否正在运行中
	case rabbit:is_running() of
		true  -> %% 检查用户名和该用户的密码是否正确
				 case AuthFun() of
					 {ok, User = #user{username = Username}} ->
						 %% 向rabbit_event事件中心发布direct类型的连接用户验证结果
						 notify_auth_result(Username,
											user_authentication_success, []),
						 connect1(User, VHost, Protocol, Pid, Infos);
					 {refused, Username, Msg, Args} ->
						 notify_auth_result(Username,
											user_authentication_failure,
											[{error, rabbit_misc:format(Msg, Args)}]),
						 {error, {auth_failure, "Refused"}}
				 end;
		false -> {error, broker_not_found_on_node}
	end.


%% 向rabbit_event事件中心发布direct类型的连接用户验证结果
notify_auth_result(Username, AuthResult, ExtraProps) ->
	EventProps = [{connection_type, direct},
				  {name, case Username of none -> ''; _ -> Username end}] ++
					 ExtraProps,
	rabbit_event:notify(AuthResult, [P || {_, V} = P <- EventProps, V =/= '']).


%% 实际的连接操作
connect1(User, VHost, Protocol, Pid, Infos) ->
	%% 检查当前账号的客户端能否使用该VirtualHost
	try rabbit_access_control:check_vhost_access(User, VHost, undefined) of
		ok -> ok = pg_local:join(rabbit_direct, Pid),
			  %% 向rabbit_event事件中心发布direct类型的连接连接成功
			  rabbit_event:notify(connection_created, Infos),
			  %% 返回用户名字和RabbitMQ系统的服务器特性
			  {ok, {User, rabbit_reader:server_properties(Protocol)}}
	catch
		exit:#amqp_error{name = access_refused} ->
			{error, access_refused}
	end.


%% 启动direct类型连接下的rabbit_channel进程
start_channel(Number, ClientChannelPid, ConnPid, ConnName, Protocol, User,
			  VHost, Capabilities, Collector) ->
	%% 在rabbit_direct_client_sup监督进程下启动rabbit_channel_sup监督进程，
	%% 在rabbit_channel_sup监督进程下启动rabbit_channel和rabbit_limiter进程
	{ok, _, {ChannelPid, _}} =
		supervisor2:start_child(
		  rabbit_direct_client_sup,
		  [{direct, Number, ClientChannelPid, ConnPid, ConnName, Protocol,
			User, VHost, Capabilities, Collector}]),
	{ok, ChannelPid}.


%% 关闭direct类型的连接
disconnect(Pid, Infos) ->
	pg_local:leave(rabbit_direct, Pid),
	rabbit_event:notify(connection_closed, Infos).
