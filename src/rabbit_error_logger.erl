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

-module(rabbit_error_logger).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-define(LOG_EXCH_NAME, <<"amq.rabbitmq.log">>).

-behaviour(gen_event).

-export([start/0, stop/0]).

-export([init/1, terminate/2, code_change/3, handle_call/2, handle_event/2,
         handle_info/2]).

-import(rabbit_error_logger_file_h, [safe_handle_event/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> 'ok').
-spec(stop/0  :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% rabbit_error_logger是RabbitMQ系统自己新增的一个日志事件处理进程
start() ->
	{ok, DefaultVHost} = application:get_env(default_vhost),
	%% 注册本模块为error_logger管理器的事件处理器
	case error_logger:add_report_handler(?MODULE, [DefaultVHost]) of
		ok ->
			ok;
		{error, {no_such_vhost, DefaultVHost}} ->
			rabbit_log:warning("Default virtual host '~s' not found; "
								   "exchange '~s' disabled~n",
								   [DefaultVHost, ?LOG_EXCH_NAME]),
			ok
	end.


stop() ->
	case error_logger:delete_report_handler(rabbit_error_logger) of
		terminated_ok             -> ok;
		{error, module_not_found} -> ok
	end.

%%----------------------------------------------------------------------------
%% 日志事件的进程的初始化函数
init([DefaultVHost]) ->
	%% 在启动该gen_event进程的时候插入一个新的exchange交换机"amq.rabbitmq.log"
	#exchange{} = rabbit_exchange:declare(
					rabbit_misc:r(DefaultVHost, exchange, ?LOG_EXCH_NAME),
					topic, true, false, true, []),
	%% 该日志处理进程保留的数据结构是交换机名字为<<"amq.rabbitmq.log">>的交换机资源结构
	{ok, #resource{virtual_host = DefaultVHost,
				   kind = exchange,
				   name = ?LOG_EXCH_NAME}}.


terminate(_Arg, _State) ->
	terminated_ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


handle_call(_Request, State) ->
	{ok, not_understood, State}.


handle_event(Event, State) ->
	safe_handle_event(fun handle_event0/2, Event, State).


handle_event0({Kind, _Gleader, {_Pid, Format, Data}}, State) ->
	ok = publish(Kind, Format, Data, State),
	{ok, State};

handle_event0(_Event, State) ->
	{ok, State}.


handle_info(_Info, State) ->
	{ok, State}.


%% 向amq.rabbitmq.log交换机发送error信息
publish(error, Format, Data, State) ->
	publish1(<<"error">>, Format, Data, State);

%% 向amq.rabbitmq.log交换机发送warning信息
publish(warning_msg, Format, Data, State) ->
	publish1(<<"warning">>, Format, Data, State);

%% 向amq.rabbitmq.log交换机发送info信息
publish(info_msg, Format, Data, State) ->
	publish1(<<"info">>, Format, Data, State);

publish(_Other, _Format, _Data, _State) ->
	ok.


%% 最终实际的向amq.rabbitmq.log交换机发送消息的接口
publish1(RoutingKey, Format, Data, LogExch) ->
	%% 0-9-1 says the timestamp is a "64 bit POSIX timestamp". That's
	%% second resolution, not millisecond.
	Timestamp = rabbit_misc:now_ms() div 1000,
	
	%% 对参数进行截取操作
	Args = [truncate:term(A, ?LOG_TRUNC) || A <- Data],
	{ok, _DeliveredQPids} =
		%% 向LogExch(即amq.rabbitmq.log)交换机发送消息
		rabbit_basic:publish(LogExch, RoutingKey,
							 #'P_basic'{content_type = <<"text/plain">>,
										timestamp    = Timestamp},
							 list_to_binary(io_lib:format(Format, Args))),
	ok.
