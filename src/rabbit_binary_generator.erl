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

%% 将AMQP协议生成二进制数据
-module(rabbit_binary_generator).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([build_simple_method_frame/3,
         build_simple_content_frames/4,
         build_heartbeat_frame/0]).
-export([generate_table/1]).
-export([check_empty_frame_size/0]).
-export([ensure_content_encoded/2, clear_encoded_content/1]).
-export([map_exception/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(frame() :: [binary()]).

-spec(build_simple_method_frame/3 ::
        (rabbit_channel:channel_number(), rabbit_framing:amqp_method_record(),
         rabbit_types:protocol())
        -> frame()).
-spec(build_simple_content_frames/4 ::
        (rabbit_channel:channel_number(), rabbit_types:content(),
         non_neg_integer(), rabbit_types:protocol())
        -> [frame()]).
-spec(build_heartbeat_frame/0 :: () -> frame()).
-spec(generate_table/1 :: (rabbit_framing:amqp_table()) -> binary()).
-spec(check_empty_frame_size/0 :: () -> 'ok').
-spec(ensure_content_encoded/2 ::
        (rabbit_types:content(), rabbit_types:protocol()) ->
                                       rabbit_types:encoded_content()).
-spec(clear_encoded_content/1 ::
        (rabbit_types:content()) -> rabbit_types:unencoded_content()).
-spec(map_exception/3 :: (rabbit_channel:channel_number(),
                          rabbit_types:amqp_error() | any(),
                          rabbit_types:protocol()) ->
                              {rabbit_channel:channel_number(),
                               rabbit_framing:amqp_method_record()}).

-endif.

%%----------------------------------------------------------------------------
%% 根据不同的MethodRecord和Protocol组装数据(组装简单的AMQP消息为二进制数据)
build_simple_method_frame(ChannelInt, MethodRecord, Protocol) ->
	%% 通过AMQP协议的模板将消息生成为二进制数据
	MethodFields = Protocol:encode_method_fields(MethodRecord),
	%% 得到消息的名字
	MethodName = rabbit_misc:method_record_type(MethodRecord),
	%% 根据消息的名字得到对应的消息ID
	{ClassId, MethodId} = Protocol:method_id(MethodName),
	%% 将消息类型和channel编号和消息体组装成发送客户端的二进制数据
	create_frame(1, ChannelInt, [<<ClassId:16, MethodId:16>>, MethodFields]).


%% 根据content数据结构打包内容二进制数据
build_simple_content_frames(ChannelInt, Content, FrameMax, Protocol) ->
	#content{class_id = ClassId,
			 properties_bin = ContentPropertiesBin,
			 payload_fragments_rev = PayloadFragmentsRev} =
				ensure_content_encoded(Content, Protocol),
	%% 得到内容的打包二进制
	{BodySize, ContentFrames} =
		build_content_frames(PayloadFragmentsRev, FrameMax, ChannelInt),
	%% 得到内容头部打包的二进制(数据结构是消息头HeaderFrame + 消息内容ContentFrames内容数据)
	HeaderFrame = create_frame(2, ChannelInt,
							   [<<ClassId:16, 0:16, BodySize:64>>,
								ContentPropertiesBin]),
	[HeaderFrame | ContentFrames].


%% 组装要发送给客户端的内容二进制数据
build_content_frames(FragsRev, FrameMax, ChannelInt) ->
	BodyPayloadMax = if FrameMax == 0 -> iolist_size(FragsRev);
						true          -> FrameMax - ?EMPTY_FRAME_SIZE
					 end,
	build_content_frames(0, [], BodyPayloadMax, [],
						 lists:reverse(FragsRev), BodyPayloadMax, ChannelInt).


%% 实际打包内容的操作
build_content_frames(SizeAcc, FramesAcc, _FragSizeRem, [],
					 [], _BodyPayloadMax, _ChannelInt) ->
	{SizeAcc, lists:reverse(FramesAcc)};

build_content_frames(SizeAcc, FramesAcc, FragSizeRem, FragAcc,
					 Frags, BodyPayloadMax, ChannelInt)
  when FragSizeRem == 0 orelse Frags == [] ->
	%% 将得到的内容打包
	Frame = create_frame(3, ChannelInt, lists:reverse(FragAcc)),
	%% 得到实际打包的内容的大小
	FrameSize = BodyPayloadMax - FragSizeRem,
	%% 在此处认为打包完毕，将第四个参数设置为空，直接结束消息内容打包动作
	build_content_frames(SizeAcc + FrameSize, [Frame | FramesAcc],
						 BodyPayloadMax, [], Frags, BodyPayloadMax, ChannelInt);

build_content_frames(SizeAcc, FramesAcc, FragSizeRem, FragAcc,
					 [Frag | Frags], BodyPayloadMax, ChannelInt) ->
	Size = size(Frag),
	{NewFragSizeRem, NewFragAcc, NewFrags} =
		if Size == 0           -> {FragSizeRem, FragAcc, Frags};
		   Size =< FragSizeRem -> {FragSizeRem - Size, [Frag | FragAcc], Frags};
		   %% 当前内容数据超过最大允许大小
		   true                -> <<Head:FragSizeRem/binary, Tail/binary>> =
									  Frag,
								  {0, [Head | FragAcc], [Tail | Frags]}
		end,
	build_content_frames(SizeAcc, FramesAcc, NewFragSizeRem, NewFragAcc,
						 NewFrags, BodyPayloadMax, ChannelInt).


%% 打包心跳消息包
build_heartbeat_frame() ->
	create_frame(?FRAME_HEARTBEAT, 0, <<>>).


%% 将消息类型和channel编号和消息体组装成发送客户端的二进制数据
create_frame(TypeInt, ChannelInt, Payload) ->
	[<<TypeInt:8, ChannelInt:16, (iolist_size(Payload)):32>>, Payload,
	 ?FRAME_END].

%% table_field_to_binary supports the AMQP 0-8/0-9 standard types, S,
%% I, D, T and F, as well as the QPid extensions b, d, f, l, s, t, x,
%% and V.
%% 将不用类型的数据类型打包为二进制数据
table_field_to_binary({FName, T, V}) ->
	%% short_string_to_binary：table中key占的数据长度是定死的，不能操作256个字符(即长度占一个字节)
	%% 根据不同的类型，将不同key对应的value值组装成二进制数据
	[short_string_to_binary(FName) | field_value_to_binary(T, V)].


%% 根据数据类型将不同类型的数据组装成二进制数据

%% %% 编码长字符串(字符串的长度位占32位)
%% longstr类型的数据以$S开头
field_value_to_binary(longstr,   V) -> [$S | long_string_to_binary(V)];

%% signedint类型的数据以$I开头
field_value_to_binary(signedint, V) -> [$I, <<V:32/signed>>];

%% decimal类型的数据以$D开头
field_value_to_binary(decimal,   V) -> {Before, After} = V,
									   [$D, Before, <<After:32>>];

%% timestamp类型的数据以$T开头
field_value_to_binary(timestamp, V) -> [$T, <<V:64>>];

%% table类型的数据以$F开头
field_value_to_binary(table,     V) -> [$F | table_to_binary(V)];

%% array类型的数据以$A开头
field_value_to_binary(array,     V) -> [$A | array_to_binary(V)];

%% byte类型的数据以$b开头
field_value_to_binary(byte,      V) -> [$b, <<V:8/signed>>];

%% double类型的数据以$d开头
field_value_to_binary(double,    V) -> [$d, <<V:64/float>>];

%% float类型的数据以$f开头
field_value_to_binary(float,     V) -> [$f, <<V:32/float>>];

%% long类型的数据以$l开头
field_value_to_binary(long,      V) -> [$l, <<V:64/signed>>];

%% short类型的数据以$s开头
field_value_to_binary(short,     V) -> [$s, <<V:16/signed>>];

%% bool类型的数据以$t开头
field_value_to_binary(bool,      V) -> [$t, if V -> 1; true -> 0 end];

%% binary类型的数据以$x开头
field_value_to_binary(binary,    V) -> [$x | long_string_to_binary(V)];

%% void类型数据以$V开头
field_value_to_binary(void,     _V) -> [$V].


%% 将table类型的数据转换为二进制数据的接口
table_to_binary(Table) when is_list(Table) ->
	BinTable = generate_table_iolist(Table),
	[<<(iolist_size(BinTable)):32>> | BinTable].


%% 将array类型的数据转换为二进制数据的接口
array_to_binary(Array) when is_list(Array) ->
	BinArray = generate_array_iolist(Array),
	[<<(iolist_size(BinArray)):32>> | BinArray].


%% 编码对应的列表
generate_table(Table) when is_list(Table) ->
	list_to_binary(generate_table_iolist(Table)).


%% 将table类型的数据转换为二进制数据
generate_table_iolist(Table) ->
	lists:map(fun table_field_to_binary/1, Table).


%% 将array类型的数据转换为二进制数据
generate_array_iolist(Array) ->
	lists:map(fun ({T, V}) -> field_value_to_binary(T, V) end, Array).


%% 编码长度小于256的字符串
short_string_to_binary(String) ->
	Len = string_length(String),
	if Len < 256 -> [<<Len:8>>, String];
	   true      -> exit(content_properties_shortstr_overflow)
	end.


%% 编码长字符串(字符串的长度位占32位)
long_string_to_binary(String) ->
	Len = string_length(String),
	[<<Len:32>>, String].


%% 得到字符串或者二进制数据的长度
string_length(String) when is_binary(String) ->   size(String);

string_length(String)                        -> length(String).


%% 检查空的Frame数据的正确性
check_empty_frame_size() ->
	%% Intended to ensure that EMPTY_FRAME_SIZE is defined correctly.
	%% 旨在确保EMPTY_FRAME_SIZE正确定义
	case iolist_size(create_frame(?FRAME_BODY, 0, <<>>)) of
		?EMPTY_FRAME_SIZE -> ok;
		ComputedSize      -> exit({incorrect_empty_frame_size,
								   ComputedSize, ?EMPTY_FRAME_SIZE})
	end.


%% 确保消息内容的特性结构已经被生成为二进制文件
ensure_content_encoded(Content = #content{properties_bin = PropBin,
										  protocol = Protocol}, Protocol)
  when PropBin =/= none ->
	Content;

ensure_content_encoded(Content = #content{properties = none,
										  properties_bin = PropBin,
										  protocol = Protocol}, Protocol1)
  when PropBin =/= none ->
	Props = Protocol:decode_properties(Content#content.class_id, PropBin),
	Content#content{properties = Props,
					properties_bin = Protocol1:encode_properties(Props),
					protocol = Protocol1};

ensure_content_encoded(Content = #content{properties = Props}, Protocol)
  when Props =/= none ->
	Content#content{properties_bin = Protocol:encode_properties(Props),
					protocol = Protocol}.


%% 清除掉content结构中的特性数据
clear_encoded_content(Content = #content{properties_bin = none,
										 protocol = none}) ->
	Content;

clear_encoded_content(Content = #content{properties = none}) ->
	%% Only clear when we can rebuild the properties_bin later in
	%% accordance to the content record definition comment - maximum
	%% one of properties and properties_bin can be 'none'
	Content;

clear_encoded_content(Content = #content{}) ->
	Content#content{properties_bin = none, protocol = none}.


%% 下面的几个函数是用来组装异常错误信息，然后通过不同的消息发送给客户端
%% NB: this function is also used by the Erlang client
%% 组装channel.close消息或者connection.close消息，RabbitMQ客户端或者服务端都是在此处组装该消息，然后通知对方
map_exception(Channel, Reason, Protocol) ->
	{SuggestedClose, ReplyCode, ReplyText, FailedMethod} =
		lookup_amqp_exception(Reason, Protocol),
	{ClassId, MethodId} = case FailedMethod of
							  {_, _} -> FailedMethod;
							  none   -> {0, 0};
							  _      -> Protocol:method_id(FailedMethod)
						  end,
	case SuggestedClose orelse (Channel == 0) of
		%% 组装connection.close消息
		true  -> {0, #'connection.close'{reply_code = ReplyCode,
										 reply_text = ReplyText,
										 class_id   = ClassId,
										 method_id  = MethodId}};
		%% 组装channel.close消息
		false -> {Channel, #'channel.close'{reply_code = ReplyCode,
											reply_text = ReplyText,
											class_id   = ClassId,
											method_id  = MethodId}}
	end.


%% 从AMQP模板模块中根据异常的名字拿到对应的异常数字代码和对应文本
lookup_amqp_exception(#amqp_error{name        = Name,
								  explanation = Expl,
								  method      = Method},
					  Protocol) ->
	{ShouldClose, Code, Text} = Protocol:lookup_amqp_exception(Name),
	%% 将模板模块中返回的异常文本和异常结构中的文本组装成二进制数据
	ExplBin = amqp_exception_explanation(Text, Expl),
	{ShouldClose, Code, ExplBin, Method};

lookup_amqp_exception(Other, Protocol) ->
	rabbit_log:warning("Non-AMQP exit reason '~p'~n", [Other]),
	{ShouldClose, Code, Text} = Protocol:lookup_amqp_exception(internal_error),
	{ShouldClose, Code, Text, none}.


%% 将模板模块中返回的异常文本和异常结构中的文本组装成二进制数据
amqp_exception_explanation(Text, Expl) ->
	ExplBin = list_to_binary(Expl),
	CompleteTextBin = <<Text/binary, " - ", ExplBin/binary>>,
	if size(CompleteTextBin) > 255 -> <<CompleteTextBin:252/binary, "...">>;
	   true                        -> CompleteTextBin
	end.
