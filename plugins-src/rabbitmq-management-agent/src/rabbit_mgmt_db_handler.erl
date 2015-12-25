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

-module(rabbit_mgmt_db_handler).

%% 通过RabbitMQ系统中rabbit_boot_step的启动步骤来启动rabbit_mgmt_db_handler这个rabbit_event事件服务器的事件进程

%% Make sure our database is hooked in *before* listening on the network or
%% recovering queues (i.e. so there can't be any events fired before it starts).
-rabbit_boot_step({rabbit_mgmt_db_handler,
                   [{description, "management agent"},
                    {mfa,         {?MODULE, add_handler, []}},
                    {cleanup,     {gen_event, delete_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {requires,    rabbit_event},
                    {enables,     recovery}]}).

-behaviour(gen_event).

-export([add_handler/0, gc/0, rates_mode/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------
%% 启动rabbit_mgmt_db_handler这个rabbit_event事件服务器的事件进程
add_handler() ->
	%% 确保统计数据能够使用
	ensure_statistics_enabled(),
	%% 启动rabbit_mgmt_db_handler这个rabbit_event事件服务器的事件进程
	gen_event:add_handler(rabbit_event, ?MODULE, []).


gc() ->
	erlang:garbage_collect(whereis(rabbit_event)).

%% some people have reasons to only run with the agent enabled:
%% make it possible for them to configure key management app
%% settings such as rates_mode.
%% 从rabbitmq_management或者rabbitmq_management_agent应用中获得Key对应的配置数据
get_management_env(Key) ->
	rabbit_misc:get_env(
	  rabbitmq_management, Key,
	  rabbit_misc:get_env(rabbitmq_management_agent, Key, undefined)).


%% 获得当前系统配置的rates_mode对应的数据
rates_mode() ->
	case get_management_env(rates_mode) of
		undefined -> basic;
		Mode      -> Mode
	end.


%% 处理强制统计数据配置信息
handle_force_fine_statistics() ->
	case get_management_env(force_fine_statistics) of
		undefined ->
			ok;
		X ->
            rabbit_log:warning(
              "force_fine_statistics set to ~p; ignored.~n"
              "Replaced by {rates_mode, none} in the rabbitmq_management "
              "application.~n", [X])
	end.

%%----------------------------------------------------------------------------
%% 确保统计数据能够使用
ensure_statistics_enabled() ->
	%% 获得当前系统配置的rates_mode对应的数据
    ForceStats = rates_mode() =/= none,
	%% 处理强制统计数据配置信息
    handle_force_fine_statistics(),
	%% 从rabbit应用中获得collect_statistics字段配置的数据
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    rabbit_log:info("Management plugin: using rates mode '~p'~n", [rates_mode()]),
    case {ForceStats, StatsLevel} of
        {true,  fine} ->
            ok;
        {true,  _} ->
            application:set_env(rabbit, collect_statistics, fine);
        {false, none} ->
            application:set_env(rabbit, collect_statistics, coarse);
        {_, _} ->
            ok
    end.

%%----------------------------------------------------------------------------
%% rabbit_mgmt_db_handler进程启动回调初始化函数
init([]) ->
	{ok, []}.


handle_call(_Request, State) ->
	{ok, not_understood, State}.


%% 将rabbit_event事件中心产生的事件通知rabbit_management插件中的rabbit_mgmt_db进程进行相关处理
handle_event(Event, State) ->
	gen_server:cast({global, rabbit_mgmt_db}, {event, Event}),
	{ok, State}.


handle_info(_Info, State) ->
	{ok, State}.


terminate(_Arg, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
