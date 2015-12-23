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

-module(rabbit_trace).

-export([init/1, enabled/1, tap_in/6, tap_out/5, start/1, stop/1]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-define(TRACE_VHOSTS, trace_vhosts).
-define(XNAME, <<"amq.rabbitmq.trace">>).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(state() :: rabbit_types:exchange() | 'none').

-spec(init/1 :: (rabbit_types:vhost()) -> state()).
-spec(enabled/1 :: (rabbit_types:vhost()) -> boolean()).
-spec(tap_in/6 :: (rabbit_types:basic_message(), [rabbit_amqqueue:name()],
                   binary(), rabbit_channel:channel_number(),
                   rabbit_types:username(), state()) -> 'ok').
-spec(tap_out/5 :: (rabbit_amqqueue:qmsg(), binary(),
                    rabbit_channel:channel_number(),
                    rabbit_types:username(), state()) -> 'ok').

-spec(start/1 :: (rabbit_types:vhost()) -> 'ok').
-spec(stop/1 :: (rabbit_types:vhost()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% RabbitMQ系统性能跟踪的初始化
init(VHost) ->
	%% 判断当前RabbitMQ系统是否允许VHost上进行性能跟踪
	case enabled(VHost) of
		false -> none;
		%% 得到<<"amq.rabbitmq.trace">>名字的交换机
		true  -> {ok, X} = rabbit_exchange:lookup(
							 rabbit_misc:r(VHost, exchange, ?XNAME)),
				 X
	end.


%% 判断当前RabbitMQ系统是否允许VHost上进行性能跟踪
enabled(VHost) ->
	{ok, VHosts} = application:get_env(rabbit, ?TRACE_VHOSTS),
	lists:member(VHost, VHosts).


%% 开始进入跟踪的接口，如果交换机为none，则什么都不做
tap_in(_Msg, _QNames, _ConnName, _ChannelNum, _Username, none) -> ok;

%% 开始进入跟踪的接口
tap_in(Msg = #basic_message{exchange_name = #resource{name         = XName,
													  virtual_host = VHost}},
	   QNames, ConnName, ChannelNum, Username, TraceX) ->
	trace(TraceX, Msg, <<"publish">>, XName,
		  [{<<"vhost">>,         longstr,   VHost},
		   {<<"connection">>,    longstr,   ConnName},
		   {<<"channel">>,       signedint, ChannelNum},
		   {<<"user">>,          longstr,   Username},
		   {<<"routed_queues">>, array,
			[{longstr, QName#resource.name} || QName <- QNames]}]).


%% 消息被发送给消费者后的调用此接口
tap_out(_Msg, _ConnName, _ChannelNum, _Username, none) -> ok;

tap_out({#resource{name = QName, virtual_host = VHost},
		 _QPid, _QMsgId, Redelivered, Msg},
		ConnName, ChannelNum, Username, TraceX) ->
	RedeliveredNum = case Redelivered of true -> 1; false -> 0 end,
	trace(TraceX, Msg, <<"deliver">>, QName,
		  [{<<"redelivered">>, signedint, RedeliveredNum},
		   {<<"vhost">>,       longstr,   VHost},
		   {<<"connection">>,  longstr,   ConnName},
		   {<<"channel">>,     signedint, ChannelNum},
		   {<<"user">>,        longstr,   Username}]).

%%----------------------------------------------------------------------------
%% 启动VHost下的性能跟踪
start(VHost) ->
	%% 打印VHost开启性能跟踪的日志
	rabbit_log:info("Enabling tracing for vhost '~s'~n", [VHost]),
	%% 更新配置关键字trace_vhosts对应开启性能跟踪的VHost
	update_config(fun (VHosts) -> [VHost | VHosts -- [VHost]] end).


%% 关闭VHost下的性能跟踪
stop(VHost) ->
	%% 打印VHost关闭性能跟踪的日志
	rabbit_log:info("Disabling tracing for vhost '~s'~n", [VHost]),
	%% 更新配置关键字trace_vhosts对应开启性能跟踪的VHost
	update_config(fun (VHosts) -> VHosts -- [VHost] end).


%% 更新配置关键字trace_vhosts对应开启性能跟踪的VHost
update_config(Fun) ->
	{ok, VHosts0} = application:get_env(rabbit, ?TRACE_VHOSTS),
	VHosts = Fun(VHosts0),
	application:set_env(rabbit, ?TRACE_VHOSTS, VHosts),
	%% rabbit_trace性能跟踪的开启关闭的VHost有变化，则通知rabbit_channel进程进行重新初始化
	rabbit_channel:refresh_config_local(),
	ok.

%%----------------------------------------------------------------------------
%% 实际的将跟踪的消息发布到跟踪对应的交换机上，如果跟踪交换机和消息要发布的交换机一样，则什么都不做
trace(#exchange{name = Name}, #basic_message{exchange_name = Name},
	  _RKPrefix, _RKSuffix, _Extra) ->
	ok;

%% 实际的将跟踪的消息发布到跟踪对应的交换机上
trace(X, Msg = #basic_message{content = #content{payload_fragments_rev = PFR}},
	  RKPrefix, RKSuffix, Extra) ->
	%% 将消息发布到X对应的交换机上
	{ok, _} = rabbit_basic:publish(
				X, <<RKPrefix/binary, ".", RKSuffix/binary>>,
				#'P_basic'{headers = msg_to_table(Msg) ++ Extra}, PFR),
	ok.


%% 将消息中的关键信息转化为table
msg_to_table(#basic_message{exchange_name = #resource{name = XName},
							routing_keys  = RoutingKeys,
							content       = Content}) ->
	#content{properties = Props} =
				%% 确保消息内容的解析完全，如果properties尚未解析则通过Protocol进行解析
				rabbit_binary_parser:ensure_content_decoded(Content),
	{PropsTable, _Ix} =
		lists:foldl(fun (K, {L, Ix}) ->
							 V = element(Ix, Props),
							 NewL = case V of
										undefined -> L;
										_         -> [{a2b(K), type(V), V} | L]
									end,
							 {NewL, Ix + 1}
					end, {[], 2}, record_info(fields, 'P_basic')),
	[{<<"exchange_name">>, longstr, XName},
	 {<<"routing_keys">>,  array,   [{longstr, K} || K <- RoutingKeys]},
	 {<<"properties">>,    table,   PropsTable},
	 {<<"node">>,          longstr, a2b(node())}].


a2b(A) -> list_to_binary(atom_to_list(A)).


type(V) when is_list(V)    -> table;

type(V) when is_integer(V) -> signedint;

type(_V)                   -> longstr.
