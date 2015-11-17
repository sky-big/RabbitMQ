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

-module(credit_flow).

%% Credit flow is controlled by a credit specification - a
%% {InitialCredit, MoreCreditAfter} tuple. For the message sender,
%% credit starts at InitialCredit and is decremented(递减) with every
%% message sent. The message receiver grants more credit to the sender
%% by sending it a {bump_credit, ...} control message after receiving
%% MoreCreditAfter messages. The sender should pass this message in to
%% handle_bump_msg/1. The sender should block when it goes below 0
%% (check by invoking blocked/0). If a process is both a sender and a
%% receiver it will not grant any more credit to its senders when it
%% is itself blocked - thus the only processes that need to check
%% blocked/0 are ones that read from network sockets.

%% RabbitMQ系统设置的当前进程能够发送给下游进程消息的最大数为200
%% RabbitMQ系统设置下游进程在接收上游进程发送的50条消息后，需要通知上游进程增加能够向下游进程发送消息的数量上限
%% 每次通知上游进程增加向下游进程的发送消息的数量也为50
-define(DEFAULT_CREDIT, {200, 50}).

-export([send/1, send/2, ack/1, ack/2, handle_bump_msg/1, blocked/0, state/0]).
-export([peer_down/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([bump_msg/0]).

-opaque(bump_msg() :: {pid(), non_neg_integer()}).
-type(credit_spec() :: {non_neg_integer(), non_neg_integer()}).

-spec(send/1 :: (pid()) -> 'ok').
-spec(send/2 :: (pid(), credit_spec()) -> 'ok').
-spec(ack/1 :: (pid()) -> 'ok').
-spec(ack/2 :: (pid(), credit_spec()) -> 'ok').
-spec(handle_bump_msg/1 :: (bump_msg()) -> 'ok').
-spec(blocked/0 :: () -> boolean()).
-spec(peer_down/1 :: (pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

%% process dict update macro - eliminates(消除) the performance-hurting(性能伤害)
%% closure creation a HOF would introduce
-define(UPDATE(Key, Default, Var, Expr),
        begin
            %% We deliberately(故意) allow Var to escape from the case here
            %% to be used in Expr. Any temporary(临时) var we introduced
            %% would also escape, and might conflict(冲突).
            case get(Key) of
                undefined -> Var = Default;
                Var       -> ok
            end,
            put(Key, Expr)
        end).

%% If current process was blocked by credit flow in the last
%% STATE_CHANGE_INTERVAL milliseconds, state/0 will report it as "in
%% flow".
-define(STATE_CHANGE_INTERVAL, 1000000).

%%----------------------------------------------------------------------------

%% There are two "flows" here; of messages and of credit, going in
%% opposite directions. The variable names "From" and "To" refer to
%% the flow of credit, but the function names refer to the flow of
%% messages. This is the clearest I can make it (since the function
%% names form the API and want to make sense externally, while the
%% variable names are used in credit bookkeeping and want to make
%% sense internally).

%% For any given pair of processes, ack/2 and send/2 must always be
%% called with the same credit_spec().

%% 向下游进程发送消息时候需要在此处来进行流控制的相关记录操作
%% (即将进程字典中｛｛credit_from, From｝, Value｝的Value减一,如果减去后值为0，则需要将自己阻塞掉，不能够接收上流的消息，也不能够向下游的进程发送消息)
send(From) -> send(From, ?DEFAULT_CREDIT).


send(From, {InitialCredit, _MoreCreditAfter}) ->
	?UPDATE({credit_from, From}, InitialCredit, C,
			%% 当前能够发送消息的个数为1，则需要将自己阻塞掉
			if C == 1 -> block(From),
						 0;
			   true   -> C - 1
			end).


%% 下游进程在接收到上游进程的消息后，{{credit_to，To}，Value}中的Value值减一，当Value值为0的时候，则通知上游进程增加｛｛credit_from, From｝, Value｝的Value值
ack(To) -> ack(To, ?DEFAULT_CREDIT).


ack(To, {_InitialCredit, MoreCreditAfter}) ->
	?UPDATE({credit_to, To}, MoreCreditAfter, C,
			%% grant操作是准许上游进程增加能够发送给自己的消息的数量
			if C == 1 -> grant(To, MoreCreditAfter),
						 MoreCreditAfter;
			   true   -> C - 1
			end).


%% 接收到增加能够向下游发送消息的数量
handle_bump_msg({From, MoreCredit}) ->
	?UPDATE({credit_from, From}, 0, C,
			if C =< 0 andalso C + MoreCredit > 0 -> unblock(From),
													C + MoreCredit;
			   true                              -> C + MoreCredit
			end).


%% 判断当前进程是否处在阻塞状态
blocked() -> case get(credit_blocked) of
				 undefined -> false;
				 []        -> false;
				 _         -> true
			 end.


state() -> case blocked() of
			   true  -> flow;
			   false -> case get(credit_blocked_at) of
							undefined -> running;
							B         -> Diff = timer:now_diff(erlang:now(), B),
										 case Diff < ?STATE_CHANGE_INTERVAL of
											 true  -> flow;
											 false -> running
										 end
						end
		   end.


peer_down(Peer) ->
	%% In theory(理论) we could also remove it from credit_deferred here, but it
	%% doesn't really matter; at some point later we will drain
	%% credit_deferred and thus send messages into the void...
	unblock(Peer),
	erase({credit_from, Peer}),
	erase({credit_to, Peer}),
	ok.

%% --------------------------------------------------------------------------
%% 通知上游进程最新的能够发送给下游进程的消息数量
grant(To, Quantity) ->
	Msg = {bump_credit, {self(), Quantity}},
	case blocked() of
		false -> To ! Msg;
		%% 如果当前进程正处在阻塞状态，则将要发送给上游的进程的消息存储在进程字典中，当该进程解除阻塞后则立刻发送给上游进程
		true  -> ?UPDATE(credit_deferred, [], Deferred, [{To, Msg} | Deferred])
	end.


%% 增加向From进程发送消息的阻塞
block(From) ->
	case blocked() of
		false -> put(credit_blocked_at, erlang:now());
		true  -> ok
	end,
	?UPDATE(credit_blocked, [], Blocks, [From | Blocks]).



%% 解除向From进程发送消息的阻塞
unblock(From) ->
	?UPDATE(credit_blocked, [], Blocks, Blocks -- [From]),
	case blocked() of
		false -> case erase(credit_deferred) of
					 undefined -> ok;
					 Credits   -> [To ! Msg || {To, Msg} <- Credits]
				 end;
		true  -> ok
	end.
