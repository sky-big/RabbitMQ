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

-module(rabbit_heartbeat).

-export([start/6, start/7]).
-export([start_heartbeat_sender/4, start_heartbeat_receiver/4,
         pause_monitor/1, resume_monitor/1]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([heartbeaters/0]).

-type(heartbeaters() :: {rabbit_types:maybe(pid()), rabbit_types:maybe(pid())}).

-type(heartbeat_callback() :: fun (() -> any())).

-spec(start/6 ::
        (pid(), rabbit_net:socket(),
         non_neg_integer(), heartbeat_callback(),
         non_neg_integer(), heartbeat_callback()) -> heartbeaters()).

-spec(start/7 ::
        (pid(), rabbit_net:socket(), rabbit_types:proc_name(),
         non_neg_integer(), heartbeat_callback(),
         non_neg_integer(), heartbeat_callback()) -> heartbeaters()).

-spec(start_heartbeat_sender/4 ::
        (rabbit_net:socket(), non_neg_integer(), heartbeat_callback(),
         rabbit_types:proc_type_and_name()) -> rabbit_types:ok(pid())).
-spec(start_heartbeat_receiver/4 ::
        (rabbit_net:socket(), non_neg_integer(), heartbeat_callback(),
         rabbit_types:proc_type_and_name()) -> rabbit_types:ok(pid())).

-spec(pause_monitor/1 :: (heartbeaters()) -> 'ok').
-spec(resume_monitor/1 :: (heartbeaters()) -> 'ok').

-spec(system_code_change/4 :: (_,_,_,_) -> {'ok',_}).
-spec(system_continue/3 :: (_,_,{_, _}) -> any()).
-spec(system_terminate/4 :: (_,_,_,_) -> none()).

-endif.

%%----------------------------------------------------------------------------
start(SupPid, Sock, SendTimeoutSec, SendFun, ReceiveTimeoutSec, ReceiveFun) ->
	start(SupPid, Sock, unknown,
		  SendTimeoutSec, SendFun, ReceiveTimeoutSec, ReceiveFun).


start(SupPid, Sock, Identity,
	  SendTimeoutSec, SendFun, ReceiveTimeoutSec, ReceiveFun) ->
	%% 启动Sock数据发送的心跳检测进程
	{ok, Sender} =
		start_heartbeater(SendTimeoutSec, SupPid, Sock,
						  SendFun, heartbeat_sender,
						  start_heartbeat_sender, Identity),
	%% 启动Sock数据接收的心跳检测进程
	{ok, Receiver} =
		start_heartbeater(ReceiveTimeoutSec, SupPid, Sock,
						  ReceiveFun, heartbeat_receiver,
						  start_heartbeat_receiver, Identity),
	{Sender, Receiver}.


%% 实际的启动Sock数据发送的心跳检测进程
start_heartbeat_sender(Sock, TimeoutSec, SendFun, Identity) ->
	%% the 'div 2' is there so that we don't end up waiting for nearly
	%% 2 * TimeoutSec before sending a heartbeat in the boundary(边界) case
	%% where the last message was sent just after a heartbeat.
	%% send_oct: 查看socket上发送的字节数
	%% 进程定时检测tcp连接上是否有数据发送（这里的发送是指rabbitmq发送数据给客户端），如果一段时间内没有数据发送给客户端，则发送一个心跳包给客户端，然后循环进行下一次检测
	%% sock数据发送的心跳检测进程在超时后执行完向客户端发送心跳消息后，则继续进行心跳检测操作
	heartbeater({Sock, TimeoutSec * 1000 div 2, send_oct, 0,
				 fun () -> SendFun(), continue end}, Identity).


%% 实际的启动Sock数据接收的心跳检测进程
start_heartbeat_receiver(Sock, TimeoutSec, ReceiveFun, Identity) ->
	%% we check for incoming data every interval, and time out after
	%% two checks with no change. As a result we will time out between
	%% 2 and 3 intervals after the last data has been received.
	%% recv_oct: 查看socket上接收的字节数
	%% 进程定时检测tcp连接上是否有数据的接收，如果一段时间内没有收到任何数据，则判定为心跳超时，最终会关闭tcp连接。另外，rabbitmq的流量控制机制可能会暂停heartbeat检测
	%% sock数据接收的心跳进程在超时后执行完向rabbit_reader进程发送停止的消息后，则自己也立刻停止
	heartbeater({Sock, TimeoutSec * 1000, recv_oct, 1,
				 fun () -> ReceiveFun(), stop end}, Identity).


pause_monitor({_Sender,     none}) -> ok;
pause_monitor({_Sender, Receiver}) -> Receiver ! pause, ok.


resume_monitor({_Sender,     none}) -> ok;
resume_monitor({_Sender, Receiver}) -> Receiver ! resume, ok.


system_continue(_Parent, Deb, {Params, State}) ->
	heartbeater(Params, Deb, State).


system_terminate(Reason, _Parent, _Deb, _State) ->
	exit(Reason).


system_code_change(Misc, _Module, _OldVsn, _Extra) ->
	{ok, Misc}.

%%----------------------------------------------------------------------------
%% 在_SupPid即rabbit_connection_sup监督进程下启动一个心跳进程
start_heartbeater(0, _SupPid, _Sock, _TimeoutFun, _Name, _Callback,
				  _Identity) ->
	{ok, none};

start_heartbeater(TimeoutSec, SupPid, Sock, TimeoutFun, Name, Callback,
				  Identity) ->
	supervisor2:start_child(
	  SupPid, {Name,
			   {rabbit_heartbeat, Callback,
				[Sock, TimeoutSec, TimeoutFun, {Name, Identity}]},
			   transient, ?MAX_WAIT, worker, [rabbit_heartbeat]}).


heartbeater(Params, Identity) ->
	Deb = sys:debug_options([]),
	{ok, proc_lib:spawn_link(fun () ->
									  rabbit_misc:store_proc_name(Identity),
									  heartbeater(Params, Deb, {0, 0})
							 end)}.


%% 实际的心跳进程循环执行的函数
heartbeater({Sock, TimeoutMillisec, StatName, Threshold, Handler} = Params,
			Deb, {StatVal, SameCount} = State) ->
	Recurse = fun (State1) -> heartbeater(Params, Deb, State1) end,
	%% 处理系统消息的函数
	System  = fun (From, Req) ->
					   sys:handle_system_msg(
						 Req, From, self(), ?MODULE, Deb, {Params, State})
			  end,
	receive
		%% 接收到暂停的消息
		pause ->
			%% 收到暂停消息后，需要收到resume苏醒消息才能继续重新开始启动心跳检查
			receive
				resume              -> Recurse({0, 0});
				{system, From, Req} -> System(From, Req);
				Other               -> exit({unexpected_message, Other})
			end;
		%% 处理系统函数
		{system, From, Req} ->
			System(From, Req);
		%% 其他消息则视为异常消息，则将当前heartbeat进程终止
		Other ->
			exit({unexpected_message, Other})
		after TimeoutMillisec ->
			%% send_oct: 查看socket上发送的字节数，recv_oct: 查看socket上接收的字节数
			case rabbit_net:getstat(Sock, [StatName]) of
				{ok, [{StatName, NewStatVal}]} ->
					%% 收发数据有变化(老的接收数据大小和过了心跳超时时间后的新的接收数据大小有变化，表明当前socket在这段时间内有接收数据)
					if NewStatVal =/= StatVal ->
						   %% 重新开始检测
						   Recurse({NewStatVal, 0});
					   %% 未达到指定次数, 发送为0, 接收为1
					   SameCount < Threshold ->
						   %% 计数加1, 再次检测，Threshold表示没有变化的次数的上限
						   Recurse({NewStatVal, SameCount + 1});
					   true ->
						   %% 对于发送检测超时, 向客户端发送heartbeat包
						   %% 对于接收检测超时, 向父进程发送超时通知，由父进程触发tcp关闭等操作
						   case Handler() of
							   %% 接收检测超时
							   stop     -> ok;
							   %% 发送检测超时，执行完向客户端发送心跳的消息后，则继续循环执行心跳操作
							   continue -> Recurse({NewStatVal, 0})
						   end
					end;
				{error, einval} ->
					%% the socket is dead, most likely because the
					%% connection is being shut down -> terminate
					ok;
				{error, Reason} ->
					exit({cannot_get_socket_stats, Reason})
			end
	end.
