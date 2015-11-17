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

-module(rabbit_connection_sup).

-behaviour(supervisor2).

-export([start_link/0, reader/1]).

-export([init/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid(), pid()}).
-spec(reader/1 :: (pid()) -> pid()).

-endif.

%%--------------------------------------------------------------------------
%% 单个客户端进程监控树中的最上层监督进程
start_link() ->
	%% 启动rabbit_connection_sup监督进程
	{ok, SupPid} = supervisor2:start_link(?MODULE, []),
	%% We need to get channels in the hierarchy(阶层) here so they get shut
	%% down after the reader, so the reader gets a chance to terminate
	%% them cleanly. But for 1.0 readers we can't start the real
	%% ch_sup_sup (because we don't know if we will be 0-9-1 or 1.0) -
	%% so we add another supervisor into the hierarchy.
	%%
	%% This supervisor also acts as an intermediary(中介) for heartbeaters and
	%% the queue collector process, since these must not be siblings(兄弟姐妹) of the
	%% reader due to the potential(潜在的) for deadlock if they are added/restarted
	%% whilst the supervision tree is shutting down.
	%% 在rabbit_connection_sup监督进程下启动rabbit_connection_helper_sup监督进程
	%% intrinsic如果child非正常退出(normal, {shutdown, _}({shutdown,restart})这些都是正常退出，其他的都是异常退出，异常退出intrinsic会重启该子进程，否则会将该监督进程下的其他子进程删除掉)
	%% supervisor也会退出并删除其他所有children。
	{ok, HelperSup} =
		supervisor2:start_child(
		  SupPid,
		  {helper_sup, {rabbit_connection_helper_sup, start_link, []},
		   intrinsic, infinity, supervisor, [rabbit_connection_helper_sup]}),
	%% 在rabbit_connection_sup监督进程下启动rabbit_reader进程
	%% intrinsic如果child非正常退出(normal, {shutdown, _}({shutdown,restart})这些都是正常退出，其他的都是异常退出，异常退出intrinsic会重启该子进程，否则会将该监督进程下的其他子进程删除掉)
	%% supervisor也会退出并删除其他所有children。
	{ok, ReaderPid} =
		supervisor2:start_child(
		  SupPid,
		  {reader, {rabbit_reader, start_link, [HelperSup]},
		   intrinsic, ?MAX_WAIT, worker, [rabbit_reader]}),
	{ok, SupPid, ReaderPid}.


reader(Pid) ->
	hd(supervisor2:find_child(Pid, reader)).

%%--------------------------------------------------------------------------

init([]) ->
	{ok, {{one_for_all, 0, 1}, []}}.
