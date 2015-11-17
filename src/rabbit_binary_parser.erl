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

%% 解析AMQP协议的二进制数据
-module(rabbit_binary_parser).

-include("rabbit.hrl").

-export([parse_table/1]).
-export([ensure_content_decoded/1, clear_decoded_content/1]).
-export([validate_utf8/1, assert_utf8/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(parse_table/1 :: (binary()) -> rabbit_framing:amqp_table()).
-spec(ensure_content_decoded/1 ::
        (rabbit_types:content()) -> rabbit_types:decoded_content()).
-spec(clear_decoded_content/1 ::
        (rabbit_types:content()) -> rabbit_types:undecoded_content()).
-spec(validate_utf8/1 :: (binary()) -> 'ok' | 'error').
-spec(assert_utf8/1 :: (binary()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

%% parse_table supports the AMQP 0-8/0-9 standard types, S, I, D, T
%% and F, as well as the QPid extensions b, d, f, l, s, t, x, and V.

%% 解析简单的数据类型
-define(SIMPLE_PARSE_TABLE(BType, Pattern, RType),
        parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
                      BType, Pattern, Rest/binary>>) ->
               [{NameString, RType, Value} | parse_table(Rest)]).

%% Note that we try to put these in approximately the order we expect
%% to hit them, that's why the empty binary is half way through.
%% 对消息进行解析

%% longstr类型的数据以$S开头
parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
			  $S, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{NameString, longstr, Value} | parse_table(Rest)];

%% signedint类型的数据以$I开头
?SIMPLE_PARSE_TABLE($I, Value:32/signed,   signedint);

%% timestamp类型的数据以$T开头
?SIMPLE_PARSE_TABLE($T, Value:64/unsigned, timestamp);

%% 解析完table数据之后返回[]
parse_table(<<>>) ->
	[];

%% byte类型的数据以$b开头
?SIMPLE_PARSE_TABLE($b, Value:8/signed,  byte);

%% double类型的数据以$d开头
?SIMPLE_PARSE_TABLE($d, Value:64/float, double);

%% float类型的数据以$f开头
?SIMPLE_PARSE_TABLE($f, Value:32/float, float);

%% long类型的数据以$l开头
?SIMPLE_PARSE_TABLE($l, Value:64/signed, long);

%% short类型的数据以$s开头
?SIMPLE_PARSE_TABLE($s, Value:16/signed, short);

%% bool类型的数据以$t开头
parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
			  $t, Value:8/unsigned, Rest/binary>>) ->
	[{NameString, bool, (Value /= 0)} | parse_table(Rest)];

%% decimal类型的数据以$D开头
parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
			  $D, Before:8/unsigned, After:32/unsigned, Rest/binary>>) ->
	[{NameString, decimal, {Before, After}} | parse_table(Rest)];

%% table类型的数据以$F开头
parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
			  $F, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{NameString, table, parse_table(Value)} | parse_table(Rest)];

%% array类型的数据以$A开头
parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
			  $A, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{NameString, array, parse_array(Value)} | parse_table(Rest)];

%% binary类型的数据以$x开头
parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
			  $x, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{NameString, binary, Value} | parse_table(Rest)];

%% void类型数据以$V开头
parse_table(<<NLen:8/unsigned, NameString:NLen/binary,
			  $V, Rest/binary>>) ->
	[{NameString, void, undefined} | parse_table(Rest)].

-define(SIMPLE_PARSE_ARRAY(BType, Pattern, RType),
		parse_array(<<BType, Pattern, Rest/binary>>) ->
	[{RType, Value} | parse_array(Rest)]).


%% 解析array数据结构数据

%% longstr类型的数据以$S开头
parse_array(<<$S, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{longstr, Value} | parse_array(Rest)];

%% signedint类型的数据以$I开头
?SIMPLE_PARSE_ARRAY($I, Value:32/signed,   signedint);

%% timestamp类型的数据以$T开头
?SIMPLE_PARSE_ARRAY($T, Value:64/unsigned, timestamp);

%% 解析完array的数据后返回[]
parse_array(<<>>) ->
	[];

%% byte类型的数据以$b开头
?SIMPLE_PARSE_ARRAY($b, Value:8/signed,  byte);

%% double类型的数据以$d开头
?SIMPLE_PARSE_ARRAY($d, Value:64/float, double);

%% float类型的数据以$f开头
?SIMPLE_PARSE_ARRAY($f, Value:32/float, float);

%% long类型的数据以$l开头
?SIMPLE_PARSE_ARRAY($l, Value:64/signed, long);

%% short类型的数据以$s开头
?SIMPLE_PARSE_ARRAY($s, Value:16/signed, short);

%% bool类型的数据以$t开头
parse_array(<<$t, Value:8/unsigned, Rest/binary>>) ->
	[{bool, (Value /= 0)} | parse_array(Rest)];

%% decimal类型的数据以$D开头
parse_array(<<$D, Before:8/unsigned, After:32/unsigned, Rest/binary>>) ->
	[{decimal, {Before, After}} | parse_array(Rest)];

%% table类型的数据以$F开头
parse_array(<<$F, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{table, parse_table(Value)} | parse_array(Rest)];

%% array类型的数据以$A开头
parse_array(<<$A, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{array, parse_array(Value)} | parse_array(Rest)];

%% binary类型的数据以$x开头
parse_array(<<$x, VLen:32/unsigned, Value:VLen/binary, Rest/binary>>) ->
	[{binary, Value} | parse_array(Rest)];

%% void类型数据以$V开头
parse_array(<<$V, Rest/binary>>) ->
	[{void, undefined} | parse_array(Rest)].


%% 确保消息内容的解析完全，如果properties尚未解析则通过Protocol进行解析
ensure_content_decoded(Content = #content{properties = Props})
  when Props =/= none ->
	Content;

ensure_content_decoded(Content = #content{properties_bin = PropBin,
										  protocol = Protocol})
  when PropBin =/= none ->
	Content#content{properties = Protocol:decode_properties(
								   Content#content.class_id, PropBin)}.


%% 清除内容结构中的二进制特性结构数据，只保留已经解析过的特性结构
clear_decoded_content(Content = #content{properties = none}) ->
	Content;

clear_decoded_content(Content = #content{properties_bin = none}) ->
	%% Only clear when we can rebuild the properties later in
	%% accordance to the content record definition comment - maximum
	%% one of properties and properties_bin can be 'none'
	Content;

clear_decoded_content(Content = #content{}) ->
	Content#content{properties = none}.


assert_utf8(B) ->
	case validate_utf8(B) of
		ok    -> ok;
		error -> rabbit_misc:protocol_error(
				   frame_error, "Malformed UTF-8 in shortstr", [])
	end.


validate_utf8(Bin) ->
	try
		xmerl_ucs:from_utf8(Bin),
		ok
	catch exit:{ucs, _} ->
			  error
	end.
