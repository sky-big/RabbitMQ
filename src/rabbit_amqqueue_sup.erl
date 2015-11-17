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

-module(rabbit_amqqueue_sup).

-behaviour(supervisor2).

-export([start_link/2]).

-export([init/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/2 :: (rabbit_types:amqqueue(), rabbit_prequeue:start_mode()) ->
                           {'ok', pid(), pid()}).

-endif.

%%----------------------------------------------------------------------------
%% 在rabbit_amqqueue_sup_sup监督进程下启动rabbit_amqqueue_sup监督进程，同时在rabbit_amqqueue_sup监督进程启动rabbit_amqqueue进程
start_link(Q, StartMode) ->
	Marker = spawn_link(fun() -> receive stop -> ok end end),
	%% 配置rabbit_amqqueue_sup监督进程下启动队列进程的参数
	ChildSpec = {rabbit_amqqueue,
				 {rabbit_prequeue, start_link, [Q, StartMode, Marker]},
				 intrinsic, ?MAX_WAIT, worker, [rabbit_amqqueue_process,
												rabbit_mirror_queue_slave]},
	%% 先启动rabbit_amqqueue_sup监督进程
	{ok, SupPid} = supervisor2:start_link(?MODULE, []),
	%% 在rabbit_amqqueue_sup监督进程下启动队列进程
	{ok, QPid} = supervisor2:start_child(SupPid, ChildSpec),
	unlink(Marker),
	Marker ! stop,
	{ok, SupPid, QPid}.


init([]) -> {ok, {{one_for_one, 5, 10}, []}}.
