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

%% In practice Erlang shouldn't be allowed to grow to more than a half
%% of available memory. The pessimistic scenario is when the Erlang VM
%% has a single process that's consuming all memory. In such a case,
%% during garbage collection, Erlang tries to allocate a huge chunk of
%% continuous memory, which can result in a crash or heavy swapping.
%%
%% This module tries to warn Rabbit before such situations occur, so
%% that it has a higher chance to avoid running out of memory.

-module(vm_memory_monitor).

-behaviour(gen_server).

-export([start_link/1, start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_total_memory/0, get_vm_limit/0,
         get_check_interval/0, set_check_interval/1,
         get_vm_memory_high_watermark/0, set_vm_memory_high_watermark/1,
         get_memory_limit/0]).

%% for tests
-export([parse_line_linux/1]).


-define(SERVER, ?MODULE).
-define(DEFAULT_MEMORY_CHECK_INTERVAL, 1000).
-define(ONE_MB, 1048576).

%% For an unknown OS, we assume that we have 1GB of memory. It'll be
%% wrong. Scale by vm_memory_high_watermark in configuration to get a
%% sensible value.
-define(MEMORY_SIZE_FOR_UNKNOWN_OS, 1073741824).

-record(state, {total_memory,					%% 当前操作系统的总的内存量
                memory_limit,					%% 配置文件乘以虚拟机能够使用的内存上限后的数量(即当前RabbitMQ系统能够使用的内存上限)
                memory_fraction,				%% 配置文件中配置的当前RabbitMQ系统能够使用虚拟机内存上限的百分比
                timeout,						%% 更新消息定时器间隔时间
                timer,							%% 更新消息定时器
                alarmed,						%% 当前内存使用量是否超过上限设置有报警器的标识
                alarm_funs						%% 报警器设置和清除函数
               }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (float()) -> rabbit_types:ok_pid_or_error()).
-spec(start_link/3 :: (float(), fun ((any()) -> 'ok'),
                       fun ((any()) -> 'ok')) -> rabbit_types:ok_pid_or_error()).
-spec(get_total_memory/0 :: () -> (non_neg_integer() | 'unknown')).
-spec(get_vm_limit/0 :: () -> non_neg_integer()).
-spec(get_check_interval/0 :: () -> non_neg_integer()).
-spec(set_check_interval/1 :: (non_neg_integer()) -> 'ok').
-spec(get_vm_memory_high_watermark/0 :: () -> float()).
-spec(set_vm_memory_high_watermark/1 :: (float()) -> 'ok').
-spec(get_memory_limit/0 :: () -> non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
%% 拿到当前操作系统总的内存量
get_total_memory() ->
	try
		%% 根据不同的操作系统拿到总的内存量
		get_total_memory(os:type())
	catch _:Error ->
			  rabbit_log:warning(
				"Failed to get total system memory: ~n~p~n~p~n",
				[Error, erlang:get_stacktrace()]),
			  unknown
	end.


%% 根据操作系统的类型拿到对应的虚拟机的内存上限
get_vm_limit() -> get_vm_limit(os:type()).


%% 取得vm_memory_monitor进程刷新间隔时间
get_check_interval() ->
	gen_server:call(?MODULE, get_check_interval, infinity).


%% 设置vm_memory_monitor进程刷新间隔时间
set_check_interval(Fraction) ->
	gen_server:call(?MODULE, {set_check_interval, Fraction}, infinity).


%% 取得vm_memory_monitor进程设置的能够使用虚拟机最大内存的百分比
get_vm_memory_high_watermark() ->
	gen_server:call(?MODULE, get_vm_memory_high_watermark, infinity).


%% 设置vm_memory_monitor进程设置的能够使用虚拟机最大内存的百分比
set_vm_memory_high_watermark(Fraction) ->
	gen_server:call(?MODULE, {set_vm_memory_high_watermark, Fraction},
					infinity).

%% 从vm_memory_monitor进程取得配置文件乘以虚拟机能够使用的内存上限后的数量(即当前RabbitMQ系统能够使用的内存上限)
get_memory_limit() ->
	gen_server:call(?MODULE, get_memory_limit, infinity).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------
%% vm_memory_monitor的进程启动接口
start_link(MemFraction) ->
	start_link(MemFraction,
			   fun alarm_handler:set_alarm/1, fun alarm_handler:clear_alarm/1).


%% vm_memory_monitor的进程启动接口，自带有报警器设置和清除函数
start_link(MemFraction, AlarmSet, AlarmClear) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE,
						  [MemFraction, {AlarmSet, AlarmClear}], []).


%% 初始化
init([MemFraction, AlarmFuns]) ->
	%% 启动一个一秒的定时器，向自己发送update消息
	TRef = start_timer(?DEFAULT_MEMORY_CHECK_INTERVAL),
	State = #state { timeout    = ?DEFAULT_MEMORY_CHECK_INTERVAL,
					 timer      = TRef,
					 alarmed    = false,
					 alarm_funs = AlarmFuns },
	{ok, set_mem_limits(State, MemFraction)}.


%% 取得vm_memory_monitor进程设置的能够使用虚拟机最大内存的百分比
handle_call(get_vm_memory_high_watermark, _From, State) ->
	{reply, State#state.memory_fraction, State};


%% 设置vm_memory_monitor进程设置的能够使用虚拟机最大内存的百分比
handle_call({set_vm_memory_high_watermark, MemFraction}, _From, State) ->
	%% 重新设置内存限制信息
	{reply, ok, set_mem_limits(State, MemFraction)};


%% 取得vm_memory_monitor进程刷新间隔时间
handle_call(get_check_interval, _From, State) ->
	{reply, State#state.timeout, State};


%% 设置vm_memory_monitor进程刷新间隔时间
handle_call({set_check_interval, Timeout}, _From, State) ->
	{ok, cancel} = timer:cancel(State#state.timer),
	{reply, ok, State#state{timeout = Timeout, timer = start_timer(Timeout)}};


%% 从vm_memory_monitor进程取得配置文件乘以虚拟机能够使用的内存上限后的数量(即当前RabbitMQ系统能够使用的内存上限)
handle_call(get_memory_limit, _From, State) ->
	{reply, State#state.memory_limit, State};


handle_call(_Request, _From, State) ->
	{noreply, State}.


handle_cast(_Request, State) ->
	{noreply, State}.


%% 处理update消息，定时检查是否需要设置或者解除内存报警器
handle_info(update, State) ->
	{noreply, internal_update(State)};


handle_info(_Info, State) ->
	{noreply, State}.


terminate(_Reason, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------------
%% Server Internals
%%----------------------------------------------------------------------------
%% 设置内存限制信息
set_mem_limits(State, MemFraction) ->
	TotalMemory =
		%% 拿到当前操作系统总的内存量
		case get_total_memory() of
			unknown ->
				case State of
					#state { total_memory = undefined,
							 memory_limit = undefined } ->
						error_logger:warning_msg(
						  "Unknown total memory size for your OS ~p. "
							  "Assuming memory size is ~pMB.~n",
							  [os:type(),
							   trunc(?MEMORY_SIZE_FOR_UNKNOWN_OS/?ONE_MB)]);
					_ ->
						ok
				end,
				?MEMORY_SIZE_FOR_UNKNOWN_OS;
			M -> M
		end,
	%% 根据操作系统的类型拿到对应的虚拟机的内存上限
	UsableMemory =
		case get_vm_limit() of
			Limit when Limit < TotalMemory ->
				error_logger:warning_msg(
				  "Only ~pMB of ~pMB memory usable due to "
					  "limited address space.~n"
						  "Crashes due to memory exhaustion are possible - see~n"
							  "http://www.rabbitmq.com/memory.html#address-space~n",
							  [trunc(V/?ONE_MB) || V <- [Limit, TotalMemory]]),
				Limit;
			_ ->
				TotalMemory
		end,
	%% 将虚拟机能够使用的内存上限乘以配置文件中配置的能够使用百分比
	MemLim = trunc(MemFraction * UsableMemory),
	%% 将能够使用的内存上限和虚拟机真实的内存上限单位转化为MB，然后写入日志
	error_logger:info_msg("Memory limit set to ~pMB of ~pMB total.~n",
						  [trunc(MemLim / ?ONE_MB), trunc(TotalMemory / ?ONE_MB)]),
	%% vm_memory_monitor进程启动的时候就做一次检测，看是否需要设置内存报警器
	internal_update(State #state { total_memory    = TotalMemory,
								   memory_limit    = MemLim,
								   memory_fraction = MemFraction}).


%% vm_memory_monitor进程内部定时更新接口函数
internal_update(State = #state { memory_limit = MemLimit,
								 alarmed      = Alarmed,
								 alarm_funs   = {AlarmSet, AlarmClear} }) ->
	%% 拿到当前虚拟机已经使用的内存
	MemUsed = erlang:memory(total),
	NewAlarmed = MemUsed > MemLimit,
	case {Alarmed, NewAlarmed} of
		%% 老的状态内存上限报警器没有设置，现在内存使用量超过上限，则需要设置报警器
		{false, true} -> emit_update_info(set, MemUsed, MemLimit),
						 AlarmSet({{resource_limit, memory, node()}, []});
		%% 老的状态已经设置有内存上限报警器，现在内存使用率低于上限，则需要解除报警器
		{true, false} -> emit_update_info(clear, MemUsed, MemLimit),
						 AlarmClear({resource_limit, memory, node()});
		_             -> ok
	end,
	State #state {alarmed = NewAlarmed}.


%% 将内存设置或者解除报警器的信息打印到日志中
emit_update_info(AlarmState, MemUsed, MemLimit) ->
	error_logger:info_msg(
	  "vm_memory_high_watermark ~p. Memory used:~p allowed:~p~n",
	  [AlarmState, MemUsed, MemLimit]).


%% 启动定时器做更新操作
start_timer(Timeout) ->
	{ok, TRef} = timer:send_interval(Timeout, update),
	TRef.


%% According to http://msdn.microsoft.com/en-us/library/aa366778(VS.85).aspx
%% Windows has 2GB and 8TB of address space for 32 and 64 bit accordingly.
get_vm_limit({win32, _OSname}) ->
	case erlang:system_info(wordsize) of
		4 -> 2 * 1024 * 1024 * 1024;          %% 2 GB for 32 bits  2^31
		8 -> 8 * 1024 * 1024 * 1024 * 1024    %% 8 TB for 64 bits  2^42
	end;

%% On a 32-bit machine, if you're using more than 2 gigs of RAM you're
%% in big trouble anyway.
get_vm_limit(_OsType) ->
	case erlang:system_info(wordsize) of
		4 -> 2 * 1024 * 1024 * 1024;          %% 2 GB for 32 bits  2^31
		8 -> 256 * 1024 * 1024 * 1024 * 1024  %% 256 TB for 64 bits 2^48
	%%http://en.wikipedia.org/wiki/X86-64#Virtual_address_space_details
	end.

%%----------------------------------------------------------------------------
%% Internal Helpers
%%----------------------------------------------------------------------------
cmd(Command) ->
	Exec = hd(string:tokens(Command, " ")),
	case os:find_executable(Exec) of
		false -> throw({command_not_found, Exec});
		_     -> os:cmd(Command)
	end.

%% get_total_memory(OS) -> Total
%% Windows and Freebsd code based on: memsup:get_memory_usage/1
%% Original code was part of OTP and released under "Erlang Public License".
%% 根据不同的操作系统拿到总的内存量
get_total_memory({unix, darwin}) ->
	File = cmd("/usr/bin/vm_stat"),
	Lines = string:tokens(File, "\n"),
	Dict = dict:from_list(lists:map(fun parse_line_mach/1, Lines)),
	[PageSize, Inactive, Active, Free, Wired] =
		[dict:fetch(Key, Dict) ||
		   Key <- [page_size, 'Pages inactive', 'Pages active', 'Pages free',
				   'Pages wired down']],
	PageSize * (Inactive + Active + Free + Wired);

get_total_memory({unix, freebsd}) ->
	PageSize  = sysctl("vm.stats.vm.v_page_size"),
	PageCount = sysctl("vm.stats.vm.v_page_count"),
	PageCount * PageSize;

get_total_memory({unix, openbsd}) ->
	sysctl("hw.usermem");

get_total_memory({win32, _OSname}) ->
	[Result|_] = os_mon_sysinfo:get_mem_info(),
	{ok, [_MemLoad, TotPhys, _AvailPhys, _TotPage, _AvailPage, _TotV, _AvailV],
	 _RestStr} =
		io_lib:fread("~d~d~d~d~d~d~d", Result),
	TotPhys;

get_total_memory({unix, linux}) ->
	%% /proc/meminfo该文件存储了linux操作系统中内存的详细信息
	File = read_proc_file("/proc/meminfo"),
	Lines = string:tokens(File, "\n"),
	Dict = dict:from_list(lists:map(fun parse_line_linux/1, Lines)),
	dict:fetch('MemTotal', Dict);

get_total_memory({unix, sunos}) ->
	File = cmd("/usr/sbin/prtconf"),
	Lines = string:tokens(File, "\n"),
	Dict = dict:from_list(lists:map(fun parse_line_sunos/1, Lines)),
	dict:fetch('Memory size', Dict);

get_total_memory({unix, aix}) ->
	File = cmd("/usr/bin/vmstat -v"),
	Lines = string:tokens(File, "\n"),
	Dict = dict:from_list(lists:map(fun parse_line_aix/1, Lines)),
	dict:fetch('memory pages', Dict) * 4096;

get_total_memory(_OsType) ->
	unknown.

%% A line looks like "Foo bar: 123456."
parse_line_mach(Line) ->
	[Name, RHS | _Rest] = string:tokens(Line, ":"),
	case Name of
		"Mach Virtual Memory Statistics" ->
			["(page", "size", "of", PageSize, "bytes)"] =
				string:tokens(RHS, " "),
			{page_size, list_to_integer(PageSize)};
		_ ->
			[Value | _Rest1] = string:tokens(RHS, " ."),
			{list_to_atom(Name), list_to_integer(Value)}
	end.


%% A line looks like "MemTotal:         502968 kB"
%% or (with broken OS/modules) "Readahead      123456 kB"
parse_line_linux(Line) ->
	{Name, Value, UnitRest} =
		case string:tokens(Line, ":") of
			%% no colon in the line
			[S] ->
				[K, RHS] = re:split(S, "\s", [{parts, 2}, {return, list}]),
				[V | Unit] = string:tokens(RHS, " "),
				{K, V, Unit};
			[K, RHS | _Rest] ->
				[V | Unit] = string:tokens(RHS, " "),
				{K, V, Unit}
		end,
	Value1 = case UnitRest of
				 []     -> list_to_integer(Value); %% no units
				 ["kB"] -> list_to_integer(Value) * 1024
			 end,
	{list_to_atom(Name), Value1}.


%% A line looks like "Memory size: 1024 Megabytes"
parse_line_sunos(Line) ->
	case string:tokens(Line, ":") of
		[Name, RHS | _Rest] ->
			[Value1 | UnitsRest] = string:tokens(RHS, " "),
			Value2 = case UnitsRest of
						 ["Gigabytes"] ->
							 list_to_integer(Value1) * ?ONE_MB * 1024;
						 ["Megabytes"] ->
							 list_to_integer(Value1) * ?ONE_MB;
						 ["Kilobytes"] ->
							 list_to_integer(Value1) * 1024;
						 _ ->
							 Value1 ++ UnitsRest %% no known units
					 end,
			{list_to_atom(Name), Value2};
		[Name] -> {list_to_atom(Name), none}
	end.

%% Lines look like " 12345 memory pages"
%% or              "  80.1 maxpin percentage"
parse_line_aix(Line) ->
	[Value | NameWords] = string:tokens(Line, " "),
	Name = string:join(NameWords, " "),
	{list_to_atom(Name),
	 case lists:member($., Value) of
		 true  -> trunc(list_to_float(Value));
		 false -> list_to_integer(Value)
	 end}.


sysctl(Def) ->
	list_to_integer(cmd("/sbin/sysctl -n " ++ Def) -- "\n").

%% file:read_file does not work on files in /proc as it seems to get
%% the size of the file first and then read that many bytes. But files
%% in /proc always have length 0, we just have to read until we get
%% eof.
read_proc_file(File) ->
	{ok, IoDevice} = file:open(File, [read, raw]),
	Res = read_proc_file(IoDevice, []),
	file:close(IoDevice),
	lists:flatten(lists:reverse(Res)).


-define(BUFFER_SIZE, 1024).
read_proc_file(IoDevice, Acc) ->
	case file:read(IoDevice, ?BUFFER_SIZE) of
		{ok, Res} -> read_proc_file(IoDevice, [Res | Acc]);
		eof       -> Acc
	end.
