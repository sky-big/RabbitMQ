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

-module(rabbit_auth_mechanism_plain).
-include("rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/1, handle_response/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "auth mechanism plain"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"PLAIN">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%% SASL PLAIN, as used by the Qpid Java client and our clients. Also,
%% apparently, by OpenAMQ.

%% TODO: reimplement this using the binary module? - that makes use of
%% BIFs to do binary matching and will thus be much faster.

description() ->
	[{description, <<"SASL PLAIN authentication mechanism">>}].


should_offer(_Sock) ->
	true.


init(_Sock) ->
	[].


handle_response(Response, _State) ->
	case extract_user_pass(Response) of
		{ok, User, Pass} ->
			rabbit_access_control:check_user_pass_login(User, Pass);
		error ->
			{protocol_error, "response ~p invalid", [Response]}
	end.


%% extract：提取(从Response中提取用户名和该用户的密码)
%% 该验证模块是字符串的结构是0开头，然后后面跟着用户名，然后又是0，后面继续跟着用户密码
extract_user_pass(Response) ->
	case extract_elem(Response) of
		{ok, User, Response1} -> case extract_elem(Response1) of
									 {ok, Pass, <<>>} -> {ok, User, Pass};
									 _                -> error
								 end;
		error                 -> error
	end.


%% 提取元素
extract_elem(<<0:8, Rest/binary>>) ->
	Count = next_null_pos(Rest, 0),
	<<Elem:Count/binary, Rest1/binary>> = Rest,
	{ok, Elem, Rest1};

extract_elem(_) ->
	error.


%% 找到下一个字符为0的字符，在此处将字符串分割为两份，第一份为用户名，第二份为用户密码
next_null_pos(<<>>, Count)                  -> Count;

next_null_pos(<<0:8, _Rest/binary>>, Count) -> Count;

next_null_pos(<<_:8, Rest/binary>>,  Count) -> next_null_pos(Rest, Count + 1).
