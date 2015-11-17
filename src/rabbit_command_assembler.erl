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

-module(rabbit_command_assembler).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([analyze_frame/3, init/1, process/2]).

%%----------------------------------------------------------------------------

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([frame/0]).

-type(frame_type() :: ?FRAME_METHOD | ?FRAME_HEADER | ?FRAME_BODY |
                      ?FRAME_OOB_METHOD | ?FRAME_OOB_HEADER | ?FRAME_OOB_BODY |
                      ?FRAME_TRACE | ?FRAME_HEARTBEAT).
-type(protocol()   :: rabbit_framing:protocol()).
-type(method()     :: rabbit_framing:amqp_method_record()).
-type(class_id()   :: rabbit_framing:amqp_class_id()).
-type(weight()     :: non_neg_integer()).
-type(body_size()  :: non_neg_integer()).
-type(content()    :: rabbit_types:undecoded_content()).

-type(frame() ::
        {'method',         rabbit_framing:amqp_method_name(), binary()} |
        {'content_header', class_id(), weight(), body_size(), binary()} |
        {'content_body',   binary()}).

-type(state() ::
        {'method',         protocol()} |
        {'content_header', method(), class_id(), protocol()} |
        {'content_body',   method(), body_size(), class_id(), protocol()}).

-spec(analyze_frame/3 :: (frame_type(), binary(), protocol()) ->
                              frame() | 'heartbeat' | 'error').

-spec(init/1 :: (protocol()) -> {ok, state()}).
-spec(process/2 :: (frame(), state()) ->
                        {ok, state()} |
                        {ok, method(), state()} |
                        {ok, method(), content(), state()} |
                        {error, rabbit_types:amqp_error()}).

-endif.

%%--------------------------------------------------------------------
%% 根据消息类型解析RabbitMQ系统发送过来的消息

%% 类型1为定义的消息类型，该类型就是只包含AMQP消息协议
%% 消息结构组成为：Type(Frame类型，占用1个字节) Channel(表示channel号，占用2个字节)
%% 				 Length(后续字符串的长度，占用4个字节) ClassId(消息类型，占用2个字节)
%% 				 MethodId(消息ID，占用2个字节) MethodFields(是Frame的实际二进制数据) ?FRAME_END=206占用1个字节表示当前Frame的结束
analyze_frame(?FRAME_METHOD,
			  <<ClassId:16, MethodId:16, MethodFields/binary>>,
			  Protocol) ->
	MethodName = Protocol:lookup_method_name({ClassId, MethodId}),
	{method, MethodName, MethodFields};

%% 类型2为BODY的头描述，得到消息内容的头部数据
%% Header消息结构组成为：Type(Frame类型，占用1个字节) Channel(表示channel号，占用两个字节)
%%					   Length(后续字符串的长度，占用4个字节) ClassId(消息类型，占用2个字节)
%%					   Weight(在Header结构中为0，占用2个字节) BodySize(消息内容的长度，占用8个字节)
%%					   Properties(是该消息的特性二进制数据) ?FRAME_END=206占用1个字节表示当前Frame的结束
analyze_frame(?FRAME_HEADER,
			  <<ClassId:16, Weight:16, BodySize:64, Properties/binary>>,
			  _Protocol) ->
	{content_header, ClassId, Weight, BodySize, Properties};

%% 类型3为body内容
%% 消息内容的结构组成：Type(Frame类型，占用1个字节) Channel(表示channel号，占用两个字节)
%% 					 Body(是消息内容的实际内容)	?FRAME_END=206占用1个字节表示当前Frame的结束
analyze_frame(?FRAME_BODY, Body, _Protocol) ->
	{content_body, Body};

%% 类型8为心跳类型
%% 心跳Frame结构组成：Type(Frame类型，占用1个字节) Channel(表示channel号，占用两个字节) 消息二进制数据为空 ?FRAME_END=206占用1个字节表示当前Frame的结束
analyze_frame(?FRAME_HEARTBEAT, <<>>, _Protocol) ->
	heartbeat;

analyze_frame(_Type, _Body, _Protocol) ->
	error.


%% 组装消息协议解析编码处理文件
init(Protocol) -> {ok, {method, Protocol}}.


%% 根据不用类型的消息进行相关的处理(解析给出的二进制文件)
process({method, MethodName, FieldsBin}, {method, Protocol}) ->
	try
		Method = Protocol:decode_method_fields(MethodName, FieldsBin),
		case Protocol:method_has_content(MethodName) of
			true  -> {ClassId, _MethodId} = Protocol:method_id(MethodName),
					 %% 如果该消息能够带有内容
					 {ok, {content_header, Method, ClassId, Protocol}};
			false -> {ok, Method, {method, Protocol}}
		end
	catch exit:#amqp_error{} = Reason -> {error, Reason}
	end;

process(_Frame, {method, _Protocol}) ->
	unexpected_frame("expected method frame, "
						 "got non method frame instead", [], none);

%% 消息内容的Body长度为0的处理
process({content_header, ClassId, 0, 0, PropertiesBin},
		{content_header, Method, ClassId, Protocol}) ->
	%% 组装内容数据结构
	Content = empty_content(ClassId, PropertiesBin, Protocol),
	{ok, Method, Content, {method, Protocol}};

%% 消息内容的Body长度不为0的处理
process({content_header, ClassId, 0, BodySize, PropertiesBin},
		{content_header, Method, ClassId, Protocol}) ->
	%% 组装空的内容数据结构
	Content = empty_content(ClassId, PropertiesBin, Protocol),
	{ok, {content_body, Method, BodySize, Content, Protocol}};

process({content_header, HeaderClassId, 0, _BodySize, _PropertiesBin},
		{content_header, Method, ClassId, _Protocol}) ->
	unexpected_frame("expected content header for class ~w, "
						 "got one for class ~w instead",
						 [ClassId, HeaderClassId], Method);

process(_Frame, {content_header, Method, ClassId, _Protocol}) ->
	unexpected_frame("expected content header for class ~w, "
						 "got non content header frame instead", [ClassId], Method);

%% 最后是实际的内容
process({content_body, FragmentBin},
		{content_body, Method, RemainingSize,
		 Content = #content{payload_fragments_rev = Fragments}, Protocol}) ->
	NewContent = Content#content{
								 %% 得到的实际Frame内容数据
								 payload_fragments_rev = [FragmentBin | Fragments]},
	case RemainingSize - size(FragmentBin) of
		0  -> {ok, Method, NewContent, {method, Protocol}};		%% 客户发送到RabbitMQ的Method包括消息内容的解析完毕，随后立刻发送rabbit_channel进程进行相关的处理
		Sz -> {ok, {content_body, Method, Sz, NewContent, Protocol}}
	end;

process(_Frame, {content_body, Method, _RemainingSize, _Content, _Protocol}) ->
	unexpected_frame("expected content body, "
						 "got non content body frame instead", [], Method).

%%--------------------------------------------------------------------
%% 组装内容数据结构
empty_content(ClassId, PropertiesBin, Protocol) ->
	#content{class_id              = ClassId,
			 properties            = none,
			 properties_bin        = PropertiesBin,
			 protocol              = Protocol,
			 payload_fragments_rev = []}.


unexpected_frame(Format, Params, Method) when is_atom(Method) ->
	{error, rabbit_misc:amqp_error(unexpected_frame, Format, Params, Method)};
unexpected_frame(Format, Params, Method) ->
	unexpected_frame(Format, Params, rabbit_misc:method_record_type(Method)).
