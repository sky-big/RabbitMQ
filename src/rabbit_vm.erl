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

-module(rabbit_vm).

-export([memory/0, binary/0]).

-define(MAGIC_PLUGINS, ["mochiweb", "webmachine", "cowboy", "sockjs",
                        "rfc4627_jsonrpc"]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(memory/0 :: () -> rabbit_types:infos()).
-spec(binary/0 :: () -> rabbit_types:infos()).

-endif.

%%----------------------------------------------------------------------------

%% Like erlang:memory(), but with awareness of rabbit-y things
%% 得到当前节点上的内存使用情况
memory() ->
	%% 得到本节点上感兴趣的监督进程的名字(包括所有队列的监督进程，消息存储服务器进程，感兴趣的插件，RabbitMQ服务端客户端监督进程，客户端监督进程的名字)
	All = interesting_sups(),
	%% 得到感兴趣的监督进程以及被监督进程使用的内存总大小
	{Sums, _Other} = sum_processes(
					   lists:append(All), distinguishers(), [memory]),
	
	%% 分别得到当前节点所有队列进程，所有高可用队列进程，rabbit_reader，rabbit_writer，rabbit_channel，其他连接进程，消息存储进程，rabbit_mgmt_sup_sup下的进程
	%% 所有的插件进程占用的内存大小
	[Qs, QsSlave, ConnsReader, ConnsWriter, ConnsChannel, ConnsOther,
	 MsgIndexProc, MgmtDbProc, Plugins] =
		[aggregate(Names, Sums, memory, fun (X) -> X end)
		   || Names <- distinguished_interesting_sups()],
	
	%% 拿到当前节点Mnesia数据库使用的内存大小
	Mnesia       = mnesia_memory(),
	%% 拿到消息存储服务器进程ETS使用的内存大小
	MsgIndexETS  = ets_memory([msg_store_persistent, msg_store_transient]),
	%% 拿到rabbit_mgmt_db进程ETS使用的内存大小
	MgmtDbETS    = ets_memory([rabbit_mgmt_db]),
	
	%% 拿到总的内存使用量，进程数量，所有的ETS使用的内存大小，atom使用的内存大小，binary使用的内存大小，代码使用的内存大小，系统使用的内存大小
	[{total,     Total},
	 {processes, Processes},
	 {ets,       ETS},
	 {atom,      Atom},
	 {binary,    Bin},
	 {code,      Code},
	 {system,    System}] =
		erlang:memory([total, processes, ets, atom, binary, code, system]),
	
	%% 得到除去感兴趣的进程后的其他进程
	OtherProc = Processes
					- ConnsReader - ConnsWriter - ConnsChannel - ConnsOther
					- Qs - QsSlave - MsgIndexProc - Plugins - MgmtDbProc,
	
	[
	 %% 当前节点总的内存使用量
	 {total,              Total},
	 %% 当前节点从Socket读取数据的进程rabbit_reader使用的内存大小
	 {connection_readers,  ConnsReader},
	 %% 当前节点向Socket发送的进程rabbit_writer使用的内存大小
	 {connection_writers,  ConnsWriter},
	 %% 当前节点rabbit_channel进程使用的内存大小
	 {connection_channels, ConnsChannel},
	 %% 当前节点连接监督进程下除去rabbit_reader，rabbit_writer，rabbit_channel进程其他子进程所使用的内存大小
	 {connection_other,    ConnsOther},
	 %% 当前节点master类型队列进程使用的内存大小
	 {queue_procs,         Qs},
	 %% 当前节点slave类型队列进程使用的内存大小
	 {queue_slave_procs,   QsSlave},
	 %% 当前节点所有已经启动的插件使用的内存大小
	 {plugins,             Plugins},
	 %% 除去以上关心的进程使用内存外其他所有进程使用的内存大小
	 {other_proc,          lists:max([0, OtherProc])}, %% [1]
	 %% 当前节点Mnesia数据库使用的内存大小
	 {mnesia,              Mnesia},
	 %% mgmt_db进程使用的内存大小
	 {mgmt_db,             MgmtDbETS + MgmtDbProc},
	 %% 当前节点消息存储进程占用的内存大小
	 {msg_index,           MsgIndexETS + MsgIndexProc},
	 %% 除去Mnesia，消息存储进程ETS，mgmt_db进程ETS外其他进程使用的ETS内存大小
	 {other_ets,           ETS - Mnesia - MsgIndexETS - MgmtDbETS},
	 %% 当前节点使用的二进制数据的大小
	 {binary,              Bin},
	 %% 当前节点代码使用的内存大小
	 {code,                Code},
	 %% 当前节点atom原子使用的内存大小
	 {atom,                Atom},
	 %% 当前节点除去ETS，原子，二进制数据，代码占有的内存大小外其他系统占用内存大小
	 {other_system,        System - ETS - Atom - Bin - Code}].

%% [1] - erlang:memory(processes) can be less than the sum of its
%% parts. Rather than display something nonsensical, just silence any
%% claims about negative memory. See
%% http://erlang.org/pipermail/erlang-questions/2012-September/069320.html

binary() ->
	All = interesting_sups(),
	{Sums, Rest} =
		sum_processes(
		  lists:append(All),
		  fun (binary, Info, Acc) ->
				   lists:foldl(fun ({Ptr, Sz, _RefCnt}, Acc0) ->
										sets:add_element({Ptr, Sz}, Acc0)
							   end, Acc, Info)
		  end, distinguishers(), [{binary, sets:new()}]),
	[Other, Qs, QsSlave, ConnsReader, ConnsWriter, ConnsChannel, ConnsOther,
	 MsgIndexProc, MgmtDbProc, Plugins] =
		[aggregate(Names, [{other, Rest} | Sums], binary, fun sum_binary/1)
		   || Names <- [[other] | distinguished_interesting_sups()]],
	[{connection_readers,  ConnsReader},
	 {connection_writers,  ConnsWriter},
	 {connection_channels, ConnsChannel},
	 {connection_other,    ConnsOther},
	 {queue_procs,         Qs},
	 {queue_slave_procs,   QsSlave},
	 {plugins,             Plugins},
	 {mgmt_db,             MgmtDbProc},
	 {msg_index,           MsgIndexProc},
	 {other,               Other}].

%%----------------------------------------------------------------------------
%% 拿到当前节点Mnesia数据库使用的内存大小
mnesia_memory() ->
	case mnesia:system_info(is_running) of
		yes -> lists:sum([bytes(mnesia:table_info(Tab, memory)) ||
							Tab <- mnesia:system_info(tables)]);
		_   -> 0
	end.


%% 拿到OwnerNames进程所属的ETS使用的内存大小
ets_memory(OwnerNames) ->
	Owners = [whereis(N) || N <- OwnerNames],
	lists:sum([bytes(ets:info(T, memory)) || T <- ets:all(),
											 O <- [ets:info(T, owner)],
											 lists:member(O, Owners)]).


bytes(Words) ->  Words * erlang:system_info(wordsize).


%% 得到本节点上感兴趣的监督进程的名字(包括所有队列的监督进程，消息存储服务器进程，感兴趣的插件，RabbitMQ服务端客户端监督进程，客户端监督进程的名字)
interesting_sups() ->
	[[rabbit_amqqueue_sup_sup], conn_sups() | interesting_sups0()].


%% 得到消息存储服务器进程的名字,rabbit_mgmt_sup_sup名字，已经启动的插件监督进程
interesting_sups0() ->
	%% 得到消息存储服务器进程的名字
	MsgIndexProcs = [msg_store_transient, msg_store_persistent],
	MgmtDbProcs   = [rabbit_mgmt_sup_sup],
	%% 得到插件的监督进程名字
	PluginProcs   = plugin_sups(),
	[MsgIndexProcs, MgmtDbProcs, PluginProcs].


%% 得到RabbitMQ服务器端的客户端监督进程名字以及客户端监督进程名字
conn_sups()     -> [rabbit_tcp_client_sup, ssl_connection_sup, amqp_sup].


conn_sups(With) -> [{Sup, With} || Sup <- conn_sups()].


distinguishers() -> [{rabbit_amqqueue_sup_sup, fun queue_type/1} |
						 conn_sups(fun conn_type/1)].


distinguished_interesting_sups() ->
	[[{rabbit_amqqueue_sup_sup, master}],
	 [{rabbit_amqqueue_sup_sup, slave}],
	 conn_sups(reader),
	 conn_sups(writer),
	 conn_sups(channel),
	 conn_sups(other)]
	%% 得到消息存储服务器进程的名字,rabbit_mgmt_sup_sup名字，已经启动的插件监督进程
		++ interesting_sups0().


%% 得到已经启动的插件的监督进程名字
plugin_sups() ->
	lists:append([plugin_sup(App) ||
					{App, _, _} <- rabbit_misc:which_applications(),
					is_plugin(atom_to_list(App))]).


%% 得到插件的嘴上层的监督进程名字
plugin_sup(App) ->
	case application_controller:get_master(App) of
		undefined -> [];
		Master    -> case application_master:get_child(Master) of
						 {Pid, _} when is_pid(Pid) -> [process_name(Pid)];
						 Pid      when is_pid(Pid) -> [process_name(Pid)];
						 _                         -> []
					 end
	end.


process_name(Pid) ->
	case process_info(Pid, registered_name) of
		{registered_name, Name} -> Name;
		_                       -> Pid
	end.


%% 判断是否是插件
is_plugin("rabbitmq_" ++ _) -> true;

is_plugin(App)              -> lists:member(App, ?MAGIC_PLUGINS).


%% aggregate：集合
aggregate(Names, Sums, Key, Fun) ->
	lists:sum([extract(Name, Sums, Key, Fun) || Name <- Names]).


%% extract：提取
extract(Name, Sums, Key, Fun) ->
	case keyfind(Name, Sums) of
		{value, Accs} -> Fun(keyfetch(Key, Accs));
		false         -> 0
	end.


sum_binary(Set) ->
	sets:fold(fun({_Pt, Sz}, Acc) -> Acc + Sz end, 0, Set).


%% 根据队列进程中的名字得到当前队列进程的类型
queue_type(PDict) ->
	case keyfind(process_name, PDict) of
		{value, {rabbit_mirror_queue_slave, _}} -> slave;
		_                                       -> master
	end.


%% 根据连接进程中的名字得到连接进程的类型
conn_type(PDict) ->
	case keyfind(process_name, PDict) of
		{value, {rabbit_reader,  _}} -> reader;
		{value, {rabbit_writer,  _}} -> writer;
		{value, {rabbit_channel, _}} -> channel;
		_                            -> other
	end.

%%----------------------------------------------------------------------------

%% NB: this code is non-rabbit specific.

-ifdef(use_specs).
-type(process() :: pid() | atom()).
-type(info_key() :: atom()).
-type(info_value() :: any()).
-type(info_item() :: {info_key(), info_value()}).
-type(accumulate() :: fun ((info_key(), info_value(), info_value()) ->
                                  info_value())).
-type(distinguisher() :: fun (([{term(), term()}]) -> atom())).
-type(distinguishers() :: [{info_key(), distinguisher()}]).
-spec(sum_processes/3 :: ([process()], distinguishers(), [info_key()]) ->
                              {[{process(), [info_item()]}], [info_item()]}).
-spec(sum_processes/4 :: ([process()], accumulate(), distinguishers(),
                          [info_item()]) ->
                              {[{process(), [info_item()]}], [info_item()]}).
-endif.

sum_processes(Names, Distinguishers, Items) ->
	sum_processes(Names, fun (_, X, Y) -> X + Y end, Distinguishers,
				  [{Item, 0} || Item <- Items]).

%% summarize(总结) the process_info of all processes based on their
%% '$ancestor' hierarchy(等级制度), recorded in their process dictionary.
%%
%% The function takes
%%
%% 1) a list of names/pids of processes that are accumulation points
%%    in the hierarchy.
%%
%% 2) a function that aggregates individual info items -taking the
%%    info item key, value and accumulated value as the input and
%%    producing a new accumulated value.
%%
%% 3) a list of info item key / initial accumulator value pairs.
%%
%% The process_info of a process is accumulated at the nearest of its
%% ancestors that is mentioned in the first argument, or, if no such
%% ancestor exists or the ancestor information is absent, in a special
%% 'other' bucket.
%%
%% The result is a pair consisting of
%%
%% 1) a k/v list, containing for each of the accumulation names/pids a
%%    list of info items, containing the accumulated data, and
%%
%% 2) the 'other' bucket - a list of info items containing the
%%    accumulated data of all processes with no matching ancestors
%%
%% Note that this function operates on names as well as pids, but
%% these must match whatever is contained in the '$ancestor' process
%% dictionary entry. Generally that means for all registered processes
%% the name should be used.
sum_processes(Names, Fun, Distinguishers, Acc0) ->
	Items = [Item || {Item, _Blank0} <- Acc0],
	{NameAccs, OtherAcc} =
		lists:foldl(
		  fun (Pid, Acc) ->
				   %% 组装要拿去进程的关键字
				   InfoItems = [registered_name, dictionary | Items],
				   case process_info(Pid, InfoItems) of
					   undefined ->
						   Acc;
					   [{registered_name, RegName}, {dictionary, D} | Vals] ->
						   %% see docs for process_info/2 for the
						   %% special handling of 'registered_name'
						   %% info items
						   Extra = case RegName of
									   [] -> [];
									   N  -> [N]
								   end,
						   %% 找出Pid的祖先进程，该祖先进程是Names中的一员
						   Name0 = find_ancestor(Extra, D, Names),
						   Name = case keyfind(Name0, Distinguishers) of
									  {value, DistFun} -> {Name0, DistFun(D)};
									  false            -> Name0
								  end,
						   %% 进行累计操作(主要是将进程使用的内存大小累计起来)
						   accumulate(
							 Name, Fun, orddict:from_list(Vals), Acc, Acc0)
				   end
		  end, {orddict:new(), Acc0}, processes()),
	%% these conversions aren't strictly necessary; we do them simply
	%% for the sake of encapsulating the representation.
	{[{Name, orddict:to_list(Accs)} ||
	  {Name, Accs} <- orddict:to_list(NameAccs)],
	 orddict:to_list(OtherAcc)}.


%% ancestors：祖先
%% 找出Extra名字对应的进程对应的祖先进程名字是否是Names中的一员，如果是，则返回该祖先进程的名字
find_ancestor(Extra, D, Names) ->
	Ancestors = case keyfind('$ancestors', D) of
					{value, Ancs} -> Ancs;
					false         -> []
				end,
	case lists:splitwith(fun (A) -> not lists:member(A, Names) end,
						 Extra ++ Ancestors) of
		{_,         []} -> undefined;
		{_, [Name | _]} -> Name
	end.


%% 进行累计操作(主要是将进程使用的内存大小累计起来)
accumulate(undefined, Fun, ValsDict, {NameAccs, OtherAcc}, _Acc0) ->
	{NameAccs, orddict:merge(Fun, ValsDict, OtherAcc)};

accumulate(Name,      Fun, ValsDict, {NameAccs, OtherAcc}, Acc0) ->
	F = fun (NameAcc) -> orddict:merge(Fun, ValsDict, NameAcc) end,
	{case orddict:is_key(Name, NameAccs) of
		 true  -> orddict:update(Name, F,       NameAccs);
		 false -> orddict:store( Name, F(Acc0), NameAccs)
	 end, OtherAcc}.


keyfetch(K, L) -> {value, {_, V}} = lists:keysearch(K, 1, L),
				  V.

keyfind(K, L) -> case lists:keysearch(K, 1, L) of
					 {value, {_, V}} -> {value, V};
					 false           -> false
				 end.
