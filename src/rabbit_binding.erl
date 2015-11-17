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

-module(rabbit_binding).
-include("rabbit.hrl").

-export([recover/2, exists/1, add/1, add/2, remove/1, remove/2, list/1]).
-export([list_for_source/1, list_for_destination/1,
         list_for_source_and_destination/2]).
-export([new_deletions/0, combine_deletions/2, add_deletion/3,
         process_deletions/1]).
-export([info_keys/0, info/1, info/2, info_all/1, info_all/2]).
%% these must all be run inside a mnesia tx
-export([has_for_source/1, remove_for_source/1,
         remove_for_destination/2, remove_transient_for_destination/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([key/0, deletions/0]).

-type(key() :: binary()).

-type(bind_errors() :: rabbit_types:error(
                         {'resources_missing',
                          [{'not_found', (rabbit_types:binding_source() |
                                          rabbit_types:binding_destination())} |
                           {'absent', rabbit_types:amqqueue()}]})).

-type(bind_ok_or_error() :: 'ok' | bind_errors() |
                            rabbit_types:error(
                              'binding_not_found' |
                              {'binding_invalid', string(), [any()]})).
-type(bind_res() :: bind_ok_or_error() | rabbit_misc:thunk(bind_ok_or_error())).
-type(inner_fun() ::
        fun((rabbit_types:exchange(),
             rabbit_types:exchange() | rabbit_types:amqqueue()) ->
                   rabbit_types:ok_or_error(rabbit_types:amqp_error()))).
-type(bindings() :: [rabbit_types:binding()]).

%% TODO this should really be opaque but that seems to confuse 17.1's
%% dialyzer into objecting to everything that uses it.
-type(deletions() :: dict:dict()).

-spec(recover/2 :: ([rabbit_exchange:name()], [rabbit_amqqueue:name()]) ->
                        'ok').
-spec(exists/1 :: (rabbit_types:binding()) -> boolean() | bind_errors()).
-spec(add/1    :: (rabbit_types:binding())              -> bind_res()).
-spec(add/2    :: (rabbit_types:binding(), inner_fun()) -> bind_res()).
-spec(remove/1 :: (rabbit_types:binding())              -> bind_res()).
-spec(remove/2 :: (rabbit_types:binding(), inner_fun()) -> bind_res()).
-spec(list/1 :: (rabbit_types:vhost()) -> bindings()).
-spec(list_for_source/1 ::
        (rabbit_types:binding_source()) -> bindings()).
-spec(list_for_destination/1 ::
        (rabbit_types:binding_destination()) -> bindings()).
-spec(list_for_source_and_destination/2 ::
        (rabbit_types:binding_source(), rabbit_types:binding_destination()) ->
                                                bindings()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (rabbit_types:binding()) -> rabbit_types:infos()).
-spec(info/2 :: (rabbit_types:binding(), rabbit_types:info_keys()) ->
                     rabbit_types:infos()).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(info_all/2 ::(rabbit_types:vhost(), rabbit_types:info_keys())
                   -> [rabbit_types:infos()]).
-spec(has_for_source/1 :: (rabbit_types:binding_source()) -> boolean()).
-spec(remove_for_source/1 :: (rabbit_types:binding_source()) -> bindings()).
-spec(remove_for_destination/2 ::
        (rabbit_types:binding_destination(), boolean()) -> deletions()).
-spec(remove_transient_for_destination/1 ::
        (rabbit_types:binding_destination()) -> deletions()).
-spec(process_deletions/1 :: (deletions()) -> rabbit_misc:thunk('ok')).
-spec(combine_deletions/2 :: (deletions(), deletions()) -> deletions()).
-spec(add_deletion/3 :: (rabbit_exchange:name(),
                         {'undefined' | rabbit_types:exchange(),
                          'deleted' | 'not_deleted',
                          bindings()}, deletions()) -> deletions()).
-spec(new_deletions/0 :: () -> deletions()).

-endif.

%%----------------------------------------------------------------------------
%% 绑定信息的关键信息key
-define(INFO_KEYS, [source_name, source_kind,
                    destination_name, destination_kind,
                    routing_key, arguments]).

%% RabbitMQ系统启动的时候需要根据持久化的exchange交换机和持久化的队列恢复数据
recover(XNames, QNames) ->
	%% 如果持久化rabbit_durable_route表中有的绑定，应该在rabbit_semi_durable_route存储一份绑定数据
	rabbit_misc:table_filter(
	  %% 判断从rabbit_semi_durable_route表中读取不到路由信息的判断函数
	  fun (Route) ->
			   mnesia:read({rabbit_semi_durable_route, Route}) =:= []
	  end,
	  %% 将持久化中的路由信息写入rabbit_semi_durable_route表中
	  fun (Route,  true) ->
			   ok = mnesia:write(rabbit_semi_durable_route, Route, write);
		 (_Route, false) ->
			  ok
	  end, rabbit_durable_route),
	%% 将Exchange交换机列表组装成sets数据结构
	XNameSet = sets:from_list(XNames),
	%% 将队列列表组装成sets数据结构
	QNameSet = sets:from_list(QNames),
	SelectSet = fun (#resource{kind = exchange}) -> XNameSet;
				   (#resource{kind = queue})    -> QNameSet
				end,
	%% 启动收集进程
	{ok, Gatherer} = gatherer:start_link(),
	%% 将路由信息的恢复发送到工作进程中进行异步处理
	[recover_semi_durable_route(Gatherer, R, SelectSet(Dst)) ||
	   R = #route{binding = #binding{destination = Dst}} <-
					 rabbit_misc:dirty_read_all(rabbit_semi_durable_route)],
	%% 等待工作进程执行完恢复路由信息的操作，此处阻塞等待收集进程结束
	empty = gatherer:out(Gatherer),
	%% 停止收集进程
	ok = gatherer:stop(Gatherer),
	ok.


%% 根据rabbit_semi_durable_route表中的路由数据恢复rabbit_route和rabbit_reverse_route表中的路由信息
recover_semi_durable_route(Gatherer, R = #route{binding = B}, ToRecover) ->
	#binding{source = Src, destination = Dst} = B,
	case sets:is_element(Dst, ToRecover) of
		true  -> {ok, X} = rabbit_exchange:lookup(Src),
				 %% 将收集者进程等待阻塞完成的数量加一
				 ok = gatherer:fork(Gatherer),
				 %% 将恢复工作下发给工作进程
				 ok = worker_pool:submit_async(
						fun () ->
								 %% 根据rabbit_semi_durable_route这个表中的路由信息，创建rabbit_route，rabbit_reverse_route内存中的路由信息表
								 recover_semi_durable_route_txn(R, X),
								 %% 将收集者进程等待阻塞完成的数量减一
								 gatherer:finish(Gatherer)
						end);
		false -> ok
	end.


%% 根据rabbit_semi_durable_route这个表中的路由信息，创建rabbit_route，rabbit_reverse_route内存中的路由信息表
recover_semi_durable_route_txn(R = #route{binding = B}, X) ->
	rabbit_misc:execute_mnesia_transaction(
	  fun () ->
			   case mnesia:match_object(rabbit_semi_durable_route, R, read) of
				   [] -> no_recover;
				   %% 如果R存在于rabbit_semi_durable_route表，则将路由信息写入rabbit_route，rabbit_reverse_route内存表
				   _  -> ok = sync_transient_route(R, fun mnesia:write/3),
						 %% 让X对应的exchange交换机类型对应的模块和所有的修饰模块回调serialise_events函数，如果执行成功，则将XName在rabbit_exchange_serial表中的next值加一
						 rabbit_exchange:serial(X)
			   end
	  end,
	  %% 将路由信息恢复后，需要绑定起来
	  fun (no_recover, _)     -> ok;
		 %% 回调到rabbit_exchange模块，然后根据exchange的类型处理绑定信息
		 (_Serial,    true)  -> x_callback(transaction, X, add_binding, B);
		 (Serial,     false) -> x_callback(Serial,      X, add_binding, B)
	  end).


%% 判断Binding这个路由信息是否存在
exists(Binding) ->
	binding_action(
	  Binding, fun (_Src, _Dst, B) ->
						%% 所有的路由信息都会存储到rabbit_route表，因此只需要判断绑路由信息是否存在于rabbit_route表中
						rabbit_misc:const(mnesia:read({rabbit_route, B}) /= [])
	  end, fun not_found_or_absent_errs/1).


%% 增加绑定的操作(上层检查函数不检查)
add(Binding) -> add(Binding, fun (_Src, _Dst) -> ok end).


%% 增加新的绑定
add(Binding, InnerFun) ->
	binding_action(
	  Binding,
	  %% Src，Dst都是从mnesia数据库中读取到的数据
	  fun (Src, Dst, B) ->
			   %% 找到对应交换机的处理模块去验证绑定的合法性(Src必须是exchange交换机类型)
			   case rabbit_exchange:validate_binding(Src, B) of
				   ok ->
					   %% this argument is used to check queue exclusivity(排他性);
					   %% in general, we want to fail on that in preference to
					   %% anything else
					   %% 功效一：检查队列字段exclusive_owner字段的正确性，检查排他性队列(如果一个队列被声明为排他队列，该队列仅对首次声明它的连接可见)
					   case InnerFun(Src, Dst) of
						   ok ->
							   %% 查看rabbit_route表中是否有B这个数据，如果没有则添加到mnesia数据库表中
							   case mnesia:read({rabbit_route, B}) of
								   %% 增加绑定的实际操作(实际操作mnesia数据库表的函数)
								   []  -> add(Src, Dst, B);
								   [_] -> fun () -> ok end
							   end;
						   {error, _} = Err ->
							   rabbit_misc:const(Err)
					   end;
				   {error, _} = Err ->
					   rabbit_misc:const(Err)
			   end
	  end, fun not_found_or_absent_errs/1).


%% 增加绑定的实际操作(实际操作mnesia数据库表的函数)
add(Src, Dst, B) ->
	[SrcDurable, DstDurable] = [durable(E) || E <- [Src, Dst]],
	case (SrcDurable andalso DstDurable andalso
			  mnesia:read({rabbit_durable_route, B}) =/= []) of
		false -> %% 根据交换机exchange和队列的持久化状态，将路由信息写入mnesia数据库表
				 ok = sync_route(#route{binding = B}, SrcDurable, DstDurable,
								 fun mnesia:write/3),
				 %% 交换机exchange创建后的回调
				 x_callback(transaction, Src, add_binding, B),
				 %% 让Src对应的exchange交换机类型对应的模块和所有的修饰模块回调serialise_events函数，如果执行成功，则将XName在rabbit_exchange_serial表中的next值加一
				 Serial = rabbit_exchange:serial(Src),
				 fun () ->
						  x_callback(Serial, Src, add_binding, B),
						  %% 向rabbit_event事件中心发布绑定信息被创建的事件
						  ok = rabbit_event:notify(binding_created, info(B))
				 end;
		true  -> rabbit_misc:const({error, binding_not_found})
	end.


%% 删除exchange交换机和队列绑定(上层检查函数不检查)
remove(Binding) -> remove(Binding, fun (_Src, _Dst) -> ok end).


%% 删除exchange交换机和队列绑定(上层检查函数不检查)
remove(Binding, InnerFun) ->
	binding_action(
	  Binding,
	  fun (Src, Dst, B) ->
			   %% 从rabbit_route表中读取路由信息
			   case mnesia:read(rabbit_route, B, write) of
				   [] -> case mnesia:read(rabbit_durable_route, B, write) of
							 [] -> rabbit_misc:const(ok);
							 _  -> rabbit_misc:const({error, binding_not_found})
						 end;
				   _  -> case InnerFun(Src, Dst) of
							 %% 删除操作实际的删除mnesia数据库表的函数
							 ok               -> remove(Src, Dst, B);
							 {error, _} = Err -> rabbit_misc:const(Err)
						 end
			   end
	  end, fun absent_errs_only/1).


%% 删除操作实际的删除mnesia数据库表的函数
remove(Src, Dst, B) ->
	%% 将mnesia数据库表中的路由信息从各个表中删除掉
	ok = sync_route(#route{binding = B}, durable(Src), durable(Dst),
					fun mnesia:delete_object/3),
	%% 将绑定信息B删除，同时看看绑定信息的source即exchange交换机是否需要自动删除，如果配置的可以自动删除同时没有改exchange作为源的绑定信息，则将该exchange删除
	%% 同时将该exchange作为目的的绑定信息删除掉
	Deletions = maybe_auto_delete(
				  B#binding.source, [B], new_deletions(), false),
	%% 处理最终的删除信息，做删除exchange交换机，绑定信息学的回调操作，以及发布删除操作到rabbit_event事件中心
	process_deletions(Deletions).


%% 列出VHostPath下的所有路由绑定信息
list(VHostPath) ->
	VHostResource = rabbit_misc:r(VHostPath, '_'),
	Route = #route{binding = #binding{source      = VHostResource,
									  destination = VHostResource,
									  _           = '_'},
				   _       = '_'},
	[B || #route{binding = B} <- mnesia:dirty_match_object(rabbit_route,
														   Route)].


%% 列出SrcName源对应的路由绑定信息
list_for_source(SrcName) ->
	mnesia:async_dirty(
	  fun() ->
			  Route = #route{binding = #binding{source = SrcName, _ = '_'}},
			  [B || #route{binding = B}
							  <- mnesia:match_object(rabbit_route, Route, read)]
	  end).


%% 列出DstName对应的路由绑定信息
list_for_destination(DstName) ->
	mnesia:async_dirty(
	  fun() ->
			  Route = #route{binding = #binding{destination = DstName,
												_ = '_'}},
			  [reverse_binding(B) ||
				 #reverse_route{reverse_binding = B} <-
								   mnesia:match_object(rabbit_reverse_route,
													   reverse_route(Route), read)]
	  end).


%% 列出SrcName和DstName对应的路由绑定信息
list_for_source_and_destination(SrcName, DstName) ->
	mnesia:async_dirty(
	  fun() ->
			  Route = #route{binding = #binding{source      = SrcName,
												destination = DstName,
												_           = '_'}},
			  [B || #route{binding = B} <- mnesia:match_object(rabbit_route,
															   Route, read)]
	  end).


%% 拿路由结构中的信息
info_keys() -> ?INFO_KEYS.


map(VHostPath, F) ->
	%% TODO: there is scope for optimisation here, e.g. using a
	%% cursor, parallelising the function invocation
	lists:map(F, list(VHostPath)).


%% 根据绑定信息列出Items列表中key对应的路由信息
infos(Items, B) -> [{Item, i(Item, B)} || Item <- Items].


%% 拿到绑定信息源即exchange交换机的名字
i(source_name,      #binding{source      = SrcName})    -> SrcName#resource.name;

%% 拿到绑定信息源即exchange交换机的类型
i(source_kind,      #binding{source      = SrcName})    -> SrcName#resource.kind;

%% 拿到绑定信息目的(有可能是exchange交换机或者队列)的名字
i(destination_name, #binding{destination = DstName})    -> DstName#resource.name;

%% 拿到绑定信息目的(有可能是exchange交换机或者队列)的类型
i(destination_kind, #binding{destination = DstName})    -> DstName#resource.kind;

%% 拿到绑定信息的路由key
i(routing_key,      #binding{key         = RoutingKey}) -> RoutingKey;

%% 拿到绑定信息的参数字段数据
i(arguments,        #binding{args        = Arguments})  -> Arguments;

i(Item, _) -> throw({bad_argument, Item}).


%% 拿到宏定义中key对应的路由关键信息
info(B = #binding{}) -> infos(?INFO_KEYS, B).


%% 拿到Items列表中key对应的路由信息
info(B = #binding{}, Items) -> infos(Items, B).


%% 得到VHostPath下所有路由绑定信息的所有关键信息
info_all(VHostPath) -> map(VHostPath, fun (B) -> info(B) end).


%% 得到VHostPath下所有路由绑定信息的指定关键信息
info_all(VHostPath, Items) -> map(VHostPath, fun (B) -> info(B, Items) end).


%% 判断SrcName是否还有绑定信息
has_for_source(SrcName) ->
	Match = #route{binding = #binding{source = SrcName, _ = '_'}},
	%% we need to check for semi-durable routes (which subsumes(包括)
	%% durable routes) here too in case a bunch of routes to durable
	%% queues have been removed temporarily as a result of a node
	%% failure
	contains(rabbit_route, Match) orelse
		contains(rabbit_semi_durable_route, Match).


%% 删除SrcName这个exchange交换机对应的绑定信息
remove_for_source(SrcName) ->
	%% 将所有的路由表都锁住
	lock_route_tables(),
	Match = #route{binding = #binding{source = SrcName, _ = '_'}},
	%% 先得到rabbit_route和rabbit_semi_durable_route表中匹配的路由信息，然后remove_routes操作彻底将这些路由信息彻底删除掉
	remove_routes(
	  lists:usort(
		mnesia:match_object(rabbit_route, Match, write) ++
			mnesia:match_object(rabbit_semi_durable_route, Match, write))).


%% 删除DstName持久化队列名字对应的绑定信息
remove_for_destination(DstName, OnlyDurable) ->
	remove_for_destination(DstName, OnlyDurable, fun remove_routes/1).


%% 删除DstName非持久化队列名字对应的路由信息
remove_transient_for_destination(DstName) ->
	remove_for_destination(DstName, false, fun remove_transient_routes/1).

%%----------------------------------------------------------------------------
%% 拿到交换机exchange是否持久的标志
durable(#exchange{durable = D}) -> D;

%% 拿到队列是否是持久化的标志
durable(#amqqueue{durable = D}) -> D.


%% 绑定操作
binding_action(Binding = #binding{source      = SrcName,
								  destination = DstName,
								  args        = Arguments}, Fun, ErrFun) ->
	%% 查看对应的资源是否存在(即exchange和queue的数据是否存在，如果存在则才进行绑定操作)
	call_with_source_and_destination(
	  SrcName, DstName,
	  fun (Src, Dst) ->
			   %% 先给参数根据第一个字段进行排序
			   SortedArgs = rabbit_misc:sort_field_table(Arguments),
			   Fun(Src, Dst, Binding#binding{args = SortedArgs})
	  end, ErrFun).


delete_object(Tab, Record, LockKind) ->
	%% this 'guarded' delete prevents unnecessary writes to the mnesia
	%% disk log
	case mnesia:match_object(Tab, Record, LockKind) of
		[]  -> ok;
		[_] -> mnesia:delete_object(Tab, Record, LockKind)
	end.


%% 将路由信息写入表或删除(如果需要exchange和queue都需要持久化则将路由信息需要写入rabbit_durable_route表)
sync_route(Route, true, true, Fun) ->
	ok = Fun(rabbit_durable_route, Route, write),
	sync_route(Route, false, true, Fun);


%% exchange不需要持久化，queue需要持久化则需要将路由信息写入rabbit_semi_durable_route表或者删除表
sync_route(Route, false, true, Fun) ->
	ok = Fun(rabbit_semi_durable_route, Route, write),
	sync_route(Route, false, false, Fun);


%% 如果queue不需要持久化，则将路由信息写入rabbit_route表，将路由的信息翻转，然后写入rabbit_reverse_route表或者删除表
sync_route(Route, _SrcDurable, false, Fun) ->
	sync_transient_route(Route, Fun).


%% 同步短暂路由信息表
sync_transient_route(Route, Fun) ->
	ok = Fun(rabbit_route, Route, write),
	ok = Fun(rabbit_reverse_route, reverse_route(Route), write).


%% 查看对应的资源是否存在(即exchange和queue的数据是否存在)
call_with_source_and_destination(SrcName, DstName, Fun, ErrFun) ->
	%% 根据资源类型得到对应的mnesia数据库表
	SrcTable = table_for_resource(SrcName),
	%% 根据资源类型得到对应的mnesia数据库表
	DstTable = table_for_resource(DstName),
	%% 执行mnesia数据库的事务操作(如果SrcName和DstName都存在则执行Fun函数)
	rabbit_misc:execute_mnesia_tx_with_tail(
	  fun () ->
			   case {mnesia:read({SrcTable, SrcName}),
					 mnesia:read({DstTable, DstName})} of
				   {[Src], [Dst]} -> Fun(Src, Dst);
				   {[],    [_]  } -> ErrFun([SrcName]);
				   {[_],   []   } -> ErrFun([DstName]);
				   {[],    []   } -> ErrFun([SrcName, DstName])
			   end
	  end).


not_found_or_absent_errs(Names) ->
	Errs = [not_found_or_absent(Name) || Name <- Names],
	rabbit_misc:const({error, {resources_missing, Errs}}).


absent_errs_only(Names) ->
	Errs = [E || Name <- Names,
				 {absent, _Q, _Reason} = E <- [not_found_or_absent(Name)]],
	rabbit_misc:const(case Errs of
						  [] -> ok;
						  _  -> {error, {resources_missing, Errs}}
					  end).


%% 根据资源类型得到对应的mnesia数据库表
table_for_resource(#resource{kind = exchange}) -> rabbit_exchange;

table_for_resource(#resource{kind = queue})    -> rabbit_queue.


not_found_or_absent(#resource{kind = exchange} = Name) ->
	{not_found, Name};

not_found_or_absent(#resource{kind = queue}    = Name) ->
	case rabbit_amqqueue:not_found_or_absent(Name) of
		not_found                 -> {not_found, Name};
		{absent, _Q, _Reason} = R -> R
	end.


%% 从mnesia数据库Table表中根据MatchHead取得匹配的数据
contains(Table, MatchHead) ->
	continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).


continue('$end_of_table')    -> false;

continue({[_ | _], _})       -> true;

continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

%% For bulk(批量) operations we lock the tables we are operating on in order
%% to reduce the time complexity(复杂性). Without the table locks we end up
%% with num_tables*num_bulk_bindings row-level locks. Taking each lock
%% takes time proportional to the number of existing locks, thus
%% resulting in O(num_bulk_bindings^2) complexity.
%%
%% The locks need to be write locks since ultimately(最终) we end up
%% removing all these rows.
%%
%% The downside of all this is that no other binding operations except
%% lookup/routing (which uses dirty ops) can take place
%% concurrently. However, that is the case already since the bulk
%% operations involve mnesia:match_object calls with a partial key,
%% which entails taking a table lock.
%% 将所有的路由表都锁住
lock_route_tables() ->
	[mnesia:lock({table, T}, write) || T <- [rabbit_route,
											 rabbit_reverse_route,
											 rabbit_semi_durable_route,
											 rabbit_durable_route]].


%% 删除Routes列表中所有的路由信息
remove_routes(Routes) ->
	%% This partitioning allows us to suppress unnecessary delete
	%% operations on disk tables, which require an fsync.
	%% 将内存路由信息和磁盘路由信息分开，避免过多的操作磁盘
	{RamRoutes, DiskRoutes} =
		lists:partition(fun (R) -> mnesia:match_object(
									 rabbit_durable_route, R, write) == [] end,
						Routes),
	%% Of course the destination might not really be durable but it's
	%% just as easy to try to delete it from the semi-durable table
	%% than check first
	%% 非持久化的路由信息的删除
	[ok = sync_route(R, false, true, fun mnesia:delete_object/3) ||
			R <- RamRoutes],
	%% 持久化的路由信息的删除
	[ok = sync_route(R, true,  true, fun mnesia:delete_object/3) ||
			R <- DiskRoutes],
	%% 返回删除掉的所有路由信息的绑定信息
	[R#route.binding || R <- Routes].


%% 删除Routes列表中的所有非持久化的路由信息
remove_transient_routes(Routes) ->
	[begin
		 ok = sync_transient_route(R, fun delete_object/3),
		 R#route.binding
	 end || R <- Routes].


%% 删除DstName作为目的的绑定信息
remove_for_destination(DstName, OnlyDurable, Fun) ->
	%% 将所有的路由表都锁住
	lock_route_tables(),
	MatchFwd = #route{binding = #binding{destination = DstName, _ = '_'}},
	%% 得到reverse_route路由数据结构
	MatchRev = reverse_route(MatchFwd),
	Routes = case OnlyDurable of
				 false -> [reverse_route(R) ||
							 R <- mnesia:match_object(
							   rabbit_reverse_route, MatchRev, write)];
				 true  -> lists:usort(
							mnesia:match_object(
							  rabbit_durable_route, MatchFwd, write) ++
								mnesia:match_object(
							  rabbit_semi_durable_route, MatchFwd, write))
			 end,
	%% 使用Fun函数删除绑定信息
	Bindings = Fun(Routes),
	%% 判断删除的绑定信息中的源exchange中是否需要自动删除掉
	group_bindings_fold(fun maybe_auto_delete/4, new_deletions(),
						lists:keysort(#binding.source, Bindings), OnlyDurable).

%% Requires that its input binding list is sorted in exchange-name
%% order, so that the grouping of bindings (for passing to
%% group_bindings_and_auto_delete1) works properly.
%% 判断删除的绑定信息中的源exchange中是否需要自动删除掉
group_bindings_fold(_Fun, Acc, [], _OnlyDurable) ->
	Acc;

group_bindings_fold(Fun, Acc, [B = #binding{source = SrcName} | Bs],
					OnlyDurable) ->
	group_bindings_fold(Fun, SrcName, Acc, Bs, [B], OnlyDurable).


group_bindings_fold(
  Fun, SrcName, Acc, [B = #binding{source = SrcName} | Bs], Bindings,
  OnlyDurable) ->
	group_bindings_fold(Fun, SrcName, Acc, Bs, [B | Bindings], OnlyDurable);

group_bindings_fold(Fun, SrcName, Acc, Removed, Bindings, OnlyDurable) ->
	%% Either Removed is [], or its head has a non-matching SrcName.
	group_bindings_fold(Fun, Fun(SrcName, Bindings, Acc, OnlyDurable), Removed,
						OnlyDurable).


%% 绑定删除后，exchange交换机如果设置为自动删除后，则如果该交换机没有绑定信息之后自动将该exchange删除
maybe_auto_delete(XName, Bindings, Deletions, OnlyDurable) ->
	{Entry, Deletions1} =
		case mnesia:read({case OnlyDurable of
							  true  -> rabbit_durable_exchange;
							  false -> rabbit_exchange
						  end, XName}) of
			[]  -> {{undefined, not_deleted, Bindings}, Deletions};
			[X] -> case rabbit_exchange:maybe_auto_delete(X, OnlyDurable) of
					   not_deleted ->
						   {{X, not_deleted, Bindings}, Deletions};
					   {deleted, Deletions2} ->
						   {{X, deleted, Bindings},
							%% 合并exchange交换机XName对应的删除信息
							combine_deletions(Deletions, Deletions2)}
				   end
		end,
	%% 增加exchange交换机XName对应的删除信息
	add_deletion(XName, Entry, Deletions1).


%% 得到翻转的reverse_route表
reverse_route(#route{binding = Binding}) ->
	#reverse_route{reverse_binding = reverse_binding(Binding)};


reverse_route(#reverse_route{reverse_binding = Binding}) ->
	#route{binding = reverse_binding(Binding)}.


%% 翻转绑定
reverse_binding(#reverse_binding{source      = SrcName,
								 destination = DstName,
								 key         = Key,
								 args        = Args}) ->
	#binding{source      = SrcName,
			 destination = DstName,
			 key         = Key,
			 args        = Args};

reverse_binding(#binding{source      = SrcName,
						 destination = DstName,
						 key         = Key,
						 args        = Args}) ->
	#reverse_binding{source      = SrcName,
					 destination = DstName,
					 key         = Key,
					 args        = Args}.

%% ----------------------------------------------------------------------------
%% Binding / exchange deletion abstraction API
%% ----------------------------------------------------------------------------

anything_but( NotThis, NotThis, NotThis) -> NotThis;

anything_but( NotThis, NotThis,    This) -> This;

anything_but( NotThis,    This, NotThis) -> This;

anything_but(_NotThis,    This,    This) -> This.


%% 创建一个新的删除信息dict数据结构
new_deletions() -> dict:new().


add_deletion(XName, Entry, Deletions) ->
	dict:update(XName, fun (Entry1) -> merge_entry(Entry1, Entry) end,
				Entry, Deletions).


%% 合并删除信息
combine_deletions(Deletions1, Deletions2) ->
	dict:merge(fun (_XName, Entry1, Entry2) -> merge_entry(Entry1, Entry2) end,
			   Deletions1, Deletions2).


%% 合并exchange交换机XName对应的值
merge_entry({X1, Deleted1, Bindings1}, {X2, Deleted2, Bindings2}) ->
	{anything_but(undefined, X1, X2),
	 anything_but(not_deleted, Deleted1, Deleted2),
	 [Bindings1 | Bindings2]}.


%% 处理删除信息
process_deletions(Deletions) ->
	AugmentedDeletions =
		dict:map(fun (_XName, {X, deleted, Bindings}) ->
						  Bs = lists:flatten(Bindings),
						  %% 回调exchange交换机的删除
						  x_callback(transaction, X, delete, Bs),
						  {X, deleted, Bs, none};
					(_XName, {X, not_deleted, Bindings}) ->
						 Bs = lists:flatten(Bindings),
						 %% 回调绑定信息的删除
						 x_callback(transaction, X, remove_bindings, Bs),
						 {X, not_deleted, Bs, rabbit_exchange:serial(X)}
				 end, Deletions),
	fun() ->
			dict:fold(fun (XName, {X, deleted, Bs, Serial}, ok) ->
							   %% 向rabbit_event事件中心发布交换机被删除的事件
							   ok = rabbit_event:notify(
									  exchange_deleted, [{name, XName}]),
							   %% 向rabbit_event事件中心发布绑定信息被删除的事件
							   del_notify(Bs),
							   %% 回调exchange交换机的删除
							   x_callback(Serial, X, delete, Bs);
						 (_XName, {X, not_deleted, Bs, Serial}, ok) ->
							  %% 向rabbit_event事件中心发布绑定信息被删除的事件
							  del_notify(Bs),
							  %% 回调将绑定信息删除
							  x_callback(Serial, X, remove_bindings, Bs)
					  end, ok, AugmentedDeletions)
	end.


%% 向rabbit_event事件中心发布绑定信息被删除的事件
del_notify(Bs) -> [rabbit_event:notify(binding_deleted, info(B)) || B <- Bs].


%% 交换机exchange创建后的回调
x_callback(Serial, X, F, Bs) ->
	ok = rabbit_exchange:callback(X, F, Serial, [X, Bs]).
