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

-module(rabbit_disk_monitor).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_disk_free_limit/0, set_disk_free_limit/1,
         get_min_check_interval/0, set_min_check_interval/1,
         get_max_check_interval/0, set_max_check_interval/1,
         get_disk_free/0]).

-define(SERVER, ?MODULE).
-define(DEFAULT_MIN_DISK_CHECK_INTERVAL, 100).
-define(DEFAULT_MAX_DISK_CHECK_INTERVAL, 10000).
%% 250MB/s i.e. 250kB/ms
-define(FAST_RATE, (250 * 1000)).

-record(state, {dir,							%% mnesia数据库存储目录
                limit,							%% 配置文件配置的磁盘剩余报警大小
                actual,							%% 当前mnesia数据库存储目录剩余的磁盘大小
                min_interval,					%% 最小的更新检查时间间隔
                max_interval,					%% 最大的更新检查时间间隔
                timer,							%% 定时更新检查的定时器
                alarmed							%% 当前磁盘剩余大小是否报警的状态
               }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(disk_free_limit() :: (integer() | {'mem_relative', float()})).
-spec(start_link/1 :: (disk_free_limit()) -> rabbit_types:ok_pid_or_error()).
-spec(get_disk_free_limit/0 :: () -> integer()).
-spec(set_disk_free_limit/1 :: (disk_free_limit()) -> 'ok').
-spec(get_min_check_interval/0 :: () -> integer()).
-spec(set_min_check_interval/1 :: (integer()) -> 'ok').
-spec(get_max_check_interval/0 :: () -> integer()).
-spec(set_max_check_interval/1 :: (integer()) -> 'ok').
-spec(get_disk_free/0 :: () -> (integer() | 'unknown')).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
%% 从rabbit_disk_monitor进程取得磁盘大小设置限制
get_disk_free_limit() ->
	gen_server:call(?MODULE, get_disk_free_limit, infinity).


%% 设置磁盘大小报警限制
set_disk_free_limit(Limit) ->
	gen_server:call(?MODULE, {set_disk_free_limit, Limit}, infinity).


%% 得到最小的检查更新间隔时间
get_min_check_interval() ->
	gen_server:call(?MODULE, get_min_check_interval, infinity).


%% 设置最小的检查更新间隔时间
set_min_check_interval(Interval) ->
	gen_server:call(?MODULE, {set_min_check_interval, Interval}, infinity).


%% 得到最大的检查更新间隔时间
get_max_check_interval() ->
	gen_server:call(?MODULE, get_max_check_interval, infinity).


%% 设置最大的检查更新间隔时间
set_max_check_interval(Interval) ->
	gen_server:call(?MODULE, {set_max_check_interval, Interval}, infinity).


%% 得到当前磁盘剩余的磁盘大小
get_disk_free() ->
	gen_server:call(?MODULE, get_disk_free, infinity).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------
%% 硬盘监控进程启动入口函数
start_link(Args) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [Args], []).


%% rabbit_disk_monitor进程的初始化回调函数
init([Limit]) ->
	%% 拿到RabbitMQ系统数据库存储目录
	Dir = dir(),
	State = #state{dir          = Dir,
				   min_interval = ?DEFAULT_MIN_DISK_CHECK_INTERVAL,
				   max_interval = ?DEFAULT_MAX_DISK_CHECK_INTERVAL,
				   alarmed      = false},
	case {catch
		  %% 拿到Dir目录对应的未使用的磁盘大小
		  get_disk_free(Dir),
		  %% 拿到当前操作系统总的内存量
		  vm_memory_monitor:get_total_memory()} of
		{N1, N2} when is_integer(N1), is_integer(N2) ->
			%% 设置磁盘的最小空间下限，同时启动更新检查定时器
			{ok, start_timer(set_disk_limits(State, Limit))};
		Err ->
			rabbit_log:info("Disabling disk free space monitoring "
								"on unsupported platform:~n~p~n", [Err]),
			{stop, unsupported_platform}
	end.


%% 从rabbit_disk_monitor进程取得磁盘大小设置限制
handle_call(get_disk_free_limit, _From, State = #state{limit = Limit}) ->
	{reply, Limit, State};


%% 设置磁盘大小报警限制
handle_call({set_disk_free_limit, Limit}, _From, State) ->
	{reply, ok, set_disk_limits(State, Limit)};


%% 得到最小的检查更新间隔时间
handle_call(get_min_check_interval, _From, State) ->
	{reply, State#state.min_interval, State};


%% 得到最大的检查更新间隔时间
handle_call(get_max_check_interval, _From, State) ->
	{reply, State#state.max_interval, State};


%% 设置最小的检查更新间隔时间
handle_call({set_min_check_interval, MinInterval}, _From, State) ->
	{reply, ok, State#state{min_interval = MinInterval}};


%% 设置最大的检查更新间隔时间
handle_call({set_max_check_interval, MaxInterval}, _From, State) ->
	{reply, ok, State#state{max_interval = MaxInterval}};


%% 得到当前磁盘剩余的磁盘大小
handle_call(get_disk_free, _From, State = #state { actual = Actual }) ->
	{reply, Actual, State};


handle_call(_Request, _From, State) ->
	{noreply, State}.


handle_cast(_Request, State) ->
	{noreply, State}.


%% 处理定时检查更新的消息
handle_info(update, State) ->
	{noreply, start_timer(internal_update(State))};


handle_info(_Info, State) ->
	{noreply, State}.


terminate(_Reason, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------------
%% Server Internals
%%----------------------------------------------------------------------------

% the partition / drive containing this directory will be monitored
%% 拿到mnesia数据库的存储目录
dir() -> rabbit_mnesia:dir().


%% 设置磁盘的最小空间下限
set_disk_limits(State, Limit0) ->
	Limit = interpret_limit(Limit0),
	State1 = State#state { limit = Limit },
	%% 将磁盘剩余下限设置好后，将该信息写入到日志中
	rabbit_log:info("Disk free limit set to ~pMB~n",
					[trunc(Limit / 1000000)]),
	%% rabbit_disck_monitor进程第一次启动调用一次更新操作函数
	internal_update(State1).


internal_update(State = #state { limit   = Limit,
								 dir     = Dir,
								 alarmed = Alarmed}) ->
	%% 拿到Dir目录对应的未使用的磁盘大小
	CurrentFree = get_disk_free(Dir),
	NewAlarmed = CurrentFree < Limit,
	case {Alarmed, NewAlarmed} of
		{false, true} ->
			%% 如果旧状态没有报警，当前状态需要报警，则将磁盘报警信息写入日志文件中
			emit_update_info("insufficient", CurrentFree, Limit),
			%% 如果旧状态没有报警，当前状态需要报警，则通知rabbit_alarm进程，通告磁盘大小报警
			rabbit_alarm:set_alarm({{resource_limit, disk, node()}, []});
		{true, false} ->
			%% 如果旧状态没有报警，当前状态需要报警，则将磁盘报警信息写入日志文件中
			emit_update_info("sufficient", CurrentFree, Limit),
			%% 如果旧状态有报警，当前状态不需要报警，则通知rabbit_alarm进程，通告磁盘大小解除警报
			rabbit_alarm:clear_alarm({resource_limit, disk, node()});
		_ ->
			ok
	end,
	State #state {alarmed = NewAlarmed, actual = CurrentFree}.


%% 拿到Dir目录对应的未使用的磁盘大小
get_disk_free(Dir) ->
	get_disk_free(Dir, os:type()).


%% 根据操作系统类型拿到Dir目录对应的未使用的磁盘大小
get_disk_free(Dir, {unix, Sun})
  when Sun =:= sunos; Sun =:= sunos4; Sun =:= solaris ->
	parse_free_unix(rabbit_misc:os_cmd("/usr/bin/df -k " ++ Dir));
get_disk_free(Dir, {unix, _}) ->
	parse_free_unix(rabbit_misc:os_cmd("/bin/df -kP " ++ Dir));
get_disk_free(Dir, {win32, _}) ->
	parse_free_win32(rabbit_misc:os_cmd("dir /-C /W \"" ++ Dir ++ "\"")).


parse_free_unix(Str) ->
	case string:tokens(Str, "\n") of
		[_, S | _] -> case string:tokens(S, " \t") of
						  [_, _, _, Free | _] -> list_to_integer(Free) * 1024;
						  _                   -> exit({unparseable, Str})
					  end;
		_          -> exit({unparseable, Str})
	end.


parse_free_win32(CommandResult) ->
	LastLine = lists:last(string:tokens(CommandResult, "\r\n")),
	{match, [Free]} = re:run(lists:reverse(LastLine), "(\\d+)",
							 [{capture, all_but_first, list}]),
	list_to_integer(lists:reverse(Free)).


%% interpret：解释
interpret_limit({mem_relative, R}) ->
	round(R * vm_memory_monitor:get_total_memory());
interpret_limit(L) ->
	L.


%% 打印磁盘报警信息到日志文件
emit_update_info(StateStr, CurrentFree, Limit) ->
	rabbit_log:info(
	  "Disk free space ~s. Free bytes:~p Limit:~p~n",
	  [StateStr, CurrentFree, Limit]).


%% 启动定时器
start_timer(State) ->
	State#state{timer = erlang:send_after(interval(State), self(), update)}.


%% 计算更新的定时器间隔时间
interval(#state{alarmed      = true,
				max_interval = MaxInterval}) ->
	MaxInterval;
interval(#state{limit        = Limit,
				actual       = Actual,
				min_interval = MinInterval,
				max_interval = MaxInterval}) ->
	IdealInterval = 2 * (Actual - Limit) / ?FAST_RATE,
	trunc(erlang:max(MinInterval, erlang:min(MaxInterval, IdealInterval))).
