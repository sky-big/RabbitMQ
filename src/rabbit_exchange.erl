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

-module(rabbit_exchange).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([recover/0, policy_changed/2, callback/4, declare/6,
         assert_equivalence/6, assert_args_equivalence/2, check_type/1,
         lookup/1, lookup_or_die/1, list/0, list/1, lookup_scratch/2,
         update_scratch/3, update_decorators/1, immutable/1,
         info_keys/0, info/1, info/2, info_all/1, info_all/2,
         route/2, delete/2, validate_binding/2]).
%% these must be run inside a mnesia tx
-export([maybe_auto_delete/2, serial/1, peek_serial/1, update/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([name/0, type/0]).

-type(name() :: rabbit_types:r('exchange')).
-type(type() :: atom()).
-type(fun_name() :: atom()).

-spec(recover/0 :: () -> [name()]).
-spec(callback/4::
        (rabbit_types:exchange(), fun_name(),
         fun((boolean()) -> non_neg_integer()) | atom(), [any()]) -> 'ok').
-spec(policy_changed/2 ::
        (rabbit_types:exchange(), rabbit_types:exchange()) -> 'ok').
-spec(declare/6 ::
        (name(), type(), boolean(), boolean(), boolean(),
         rabbit_framing:amqp_table())
        -> rabbit_types:exchange()).
-spec(check_type/1 ::
        (binary()) -> atom() | rabbit_types:connection_exit()).
-spec(assert_equivalence/6 ::
        (rabbit_types:exchange(), atom(), boolean(), boolean(), boolean(),
         rabbit_framing:amqp_table())
        -> 'ok' | rabbit_types:connection_exit()).
-spec(assert_args_equivalence/2 ::
        (rabbit_types:exchange(), rabbit_framing:amqp_table())
        -> 'ok' | rabbit_types:connection_exit()).
-spec(lookup/1 ::
        (name()) -> rabbit_types:ok(rabbit_types:exchange()) |
                    rabbit_types:error('not_found')).
-spec(lookup_or_die/1 ::
        (name()) -> rabbit_types:exchange() |
                    rabbit_types:channel_exit()).
-spec(list/0 :: () -> [rabbit_types:exchange()]).
-spec(list/1 :: (rabbit_types:vhost()) -> [rabbit_types:exchange()]).
-spec(lookup_scratch/2 :: (name(), atom()) ->
                               rabbit_types:ok(term()) |
                               rabbit_types:error('not_found')).
-spec(update_scratch/3 :: (name(), atom(), fun((any()) -> any())) -> 'ok').
-spec(update/2 ::
        (name(),
         fun((rabbit_types:exchange()) -> rabbit_types:exchange()))
         -> not_found | rabbit_types:exchange()).
-spec(update_decorators/1 :: (name()) -> 'ok').
-spec(immutable/1 :: (rabbit_types:exchange()) -> rabbit_types:exchange()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (rabbit_types:exchange()) -> rabbit_types:infos()).
-spec(info/2 ::
        (rabbit_types:exchange(), rabbit_types:info_keys())
        -> rabbit_types:infos()).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(info_all/2 ::(rabbit_types:vhost(), rabbit_types:info_keys())
                   -> [rabbit_types:infos()]).
-spec(route/2 :: (rabbit_types:exchange(), rabbit_types:delivery())
                 -> [rabbit_amqqueue:name()]).
-spec(delete/2 ::
        (name(),  'true') -> 'ok' | rabbit_types:error('not_found' | 'in_use');
        (name(), 'false') -> 'ok' | rabbit_types:error('not_found')).
-spec(validate_binding/2 ::
        (rabbit_types:exchange(), rabbit_types:binding())
        -> rabbit_types:ok_or_error({'binding_invalid', string(), [any()]})).
-spec(maybe_auto_delete/2::
        (rabbit_types:exchange(), boolean())
        -> 'not_deleted' | {'deleted', rabbit_binding:deletions()}).
-spec(serial/1 :: (rabbit_types:exchange()) ->
                       fun((boolean()) -> 'none' | pos_integer())).
-spec(peek_serial/1 :: (name()) -> pos_integer() | 'undefined').

-endif.

%%----------------------------------------------------------------------------
%% exchange交换机关键信息key列表
-define(INFO_KEYS, [name, type, durable, auto_delete, internal, arguments,
                    policy]).

%% 持久化exchange交换机的恢复数据接口，将持久化exchange交换机信息(在持久化表rabbit_durable_exchange中)存入rabbit_exchange表非持久化表
recover() ->
	Xs = rabbit_misc:table_filter(
		   %% 得到持久化数据不在rabbit_exchange表中存在的交换机数据
		   fun (#exchange{name = XName}) ->
					mnesia:read({rabbit_exchange, XName}) =:= []
		   end,
		   fun (X, Tx) ->
					%% Tx为true，表示X在持久化表里存在，则将exchange信息存入非持久化的表rabbit_exchange表中
					X1 = case Tx of
							 %% 写入rabbit_exchange数据库表(同时初始化exchange交换机的描述信息)
							 true  -> store_ram(X);
							 false -> rabbit_exchange_decorator:set(X)
						 end,
					callback(X1, create, map_create_tx(Tx), [X1])
		   end,
		   rabbit_durable_exchange),
	%% 最终获得所有的持久化exchange交换机的名字
	[XName || #exchange{name = XName} <- Xs].


%% 交换机exchange创建后的回调
callback(X = #exchange{type       = XType,
					   decorators = Decorators}, Fun, Serial0, Args) ->
	Serial = if is_function(Serial0) -> Serial0;
				is_atom(Serial0)     -> fun (_Bool) -> Serial0 end
			 end,
	%% 回调exchange的描述模块
	[ok = apply(M, Fun, [Serial(M:serialise_events(X)) | Args]) ||
			M <- rabbit_exchange_decorator:select(all, Decorators)],
	%% 根据交换机类型得到对应的处理模块
	Module = type_to_module(XType),
	%% 根据得到的对应处理模块，执行该模块下的Fun函数
	apply(Module, Fun, [Serial(Module:serialise_events()) | Args]).


%% rabbit_policy策略变化的回调接口(X表示策略变化之前的exchange交换机信息，X1表示策略变化后的exchange交换机信息)
policy_changed(X  = #exchange{type       = XType,
							  decorators = Decorators},
			   X1 = #exchange{decorators = Decorators1}) ->
	%% 拿到X的修饰模块名字列表
	D  = rabbit_exchange_decorator:select(all, Decorators),
	%% 拿到X1的修饰模块名字列表
	D1 = rabbit_exchange_decorator:select(all, Decorators1),
	DAll = lists:usort(D ++ D1),
	%% 对交换机类型对应的模块和所有的修饰模块进行回调policy_changed函数
	[ok = M:policy_changed(X, X1) || M <- [type_to_module(XType) | DAll]],
	ok.


%% 让exchange交换机类型对应的模块和所有的修饰模块回调serialise_events函数
serialise_events(X = #exchange{type = Type, decorators = Decorators}) ->
	lists:any(fun (M) -> M:serialise_events(X) end,
			  rabbit_exchange_decorator:select(all, Decorators))
		orelse (type_to_module(Type)):serialise_events().


%% 让XName对应的exchange交换机类型对应的模块和所有的修饰模块回调serialise_events函数，如果执行成功，则将XName在rabbit_exchange_serial表中的next值加一
serial(#exchange{name = XName} = X) ->
	%% 让XName对应的exchange交换机类型对应的模块和所有的修饰模块回调serialise_events函数
	Serial = case serialise_events(X) of
				 %% 如果执行成功，则将XName在rabbit_exchange_serial表中的next值加一
				 true  -> next_serial(XName);
				 false -> none
			 end,
	fun (true)  -> Serial;
	   (false) -> none
	end.


%% 重新声明个exchange(创建新的exchange交换机)
declare(XName, Type, Durable, AutoDelete, Internal, Args) ->
	%% 对新的exchange交换机信息设置策略和修饰模块名
	X = rabbit_exchange_decorator:set(
		  rabbit_policy:set(#exchange{name        = XName,
									  type        = Type,
									  durable     = Durable,
									  auto_delete = AutoDelete,
									  internal    = Internal,
									  arguments   = Args})),
	%% 根据exchange的类型得到对应的处理模块
	XT = type_to_module(Type),
	%% 验证
	%% We want to upset things if it isn't ok
	%% 根据得到的模块名去进行验证
	ok = XT:validate(X),
	%% mnesia数据库执行事务操作，将exchange数据插入mnesia数据库
	rabbit_misc:execute_mnesia_transaction(
	  fun () ->
			   %% 从rabbit_exchange mnesia数据库表中读取XName交换机信息
			   case mnesia:wread({rabbit_exchange, XName}) of
				   [] ->
					   %% 如果没有读取到该交换机信息，则将该信息存储起来
					   {new, store(X)};
				   [ExistingX] ->
					   {existing, ExistingX}
			   end
	  end,
	  %% 新的交换机exchange的信息插入mnesia数据库后的回调函数
	  fun ({new, Exchange}, Tx) ->
			   %% 新的exchange交换信息被创建后，进行回调create函数
			   ok = callback(X, create, map_create_tx(Tx), [Exchange]),
			   %% 发布交换机exchange创建成功的事件
			   rabbit_event:notify_if(not Tx, exchange_created, info(Exchange)),
			   Exchange;
		 ({existing, Exchange}, _Tx) ->
			  Exchange;
		 (Err, _Tx) ->
			  Err
	  end).


map_create_tx(true)  -> transaction;

map_create_tx(false) -> none.


%% 存储exchange数据的实际操作，如果是持久化exchange则需要在rabbit_durable_exchange表里插入数据
store(X = #exchange{durable = true}) ->
	%% 将exchange交换机信息存储到持久化rabbit_durable_exchange表中(将decorators字段置为undefined，少存储掉数据)
	mnesia:write(rabbit_durable_exchange, X#exchange{decorators = undefined},
				 write),
	%% 写入rabbit_exchange数据库表
	store_ram(X);

store(X = #exchange{durable = false}) ->
	%% 写入rabbit_exchange数据库表
	store_ram(X).


%% 写入rabbit_exchange数据库表
store_ram(X) ->
	%% exchange交换机描述信息的初始化
	X1 = rabbit_exchange_decorator:set(X),
	%% 将exchange数据写入rabbit_exchange表
	ok = mnesia:write(rabbit_exchange, rabbit_exchange_decorator:set(X1),
					  write),
	X1.

%% Used with binaries sent over the wire; the type may not exist.
%% 拿到该exchange类型对应的Erlang文件模块名(用来检测exchange类型名字的正确性)
check_type(TypeBin) ->
	case rabbit_registry:binary_to_type(TypeBin) of
		{error, not_found} ->
			rabbit_misc:protocol_error(
			  command_invalid, "unknown exchange type '~s'", [TypeBin]);
		T ->
			%% 根据exchange头和exchange类型得到对应的模块名字
			case rabbit_registry:lookup_module(exchange, T) of
				{error, not_found} -> rabbit_misc:protocol_error(
										command_invalid,
										"invalid exchange type '~s'", [T]);
				{ok, _Module}      -> T
			end
	end.


%% 断言是否相等(判读最新得到的exchange数据结构和客户端发送过来的是一样的判断，rabbit_channel进程里进行创建exchange的时候调用)
assert_equivalence(X = #exchange{ name        = XName,
								  durable     = Durable,
								  auto_delete = AutoDelete,
								  internal    = Internal,
								  type        = Type},
				   ReqType, ReqDurable, ReqAutoDelete, ReqInternal, ReqArgs) ->
	AFE = fun rabbit_misc:assert_field_equivalence/4,
	AFE(Type,       ReqType,       XName, type),
	AFE(Durable,    ReqDurable,    XName, durable),
	AFE(AutoDelete, ReqAutoDelete, XName, auto_delete),
	AFE(Internal,   ReqInternal,   XName, internal),
	(type_to_module(Type)):assert_args_equivalence(X, ReqArgs).


assert_args_equivalence(#exchange{ name = Name, arguments = Args },
						RequiredArgs) ->
	%% The spec says "Arguments are compared for semantic
	%% equivalence".  The only arg we care about is
	%% "alternate-exchange".
	rabbit_misc:assert_args_equivalence(Args, RequiredArgs, Name,
										[<<"alternate-exchange">>]).


%% 查找对应的exchange是否存在
lookup(Name) ->
	rabbit_misc:dirty_read({rabbit_exchange, Name}).


%% 查找对应的exchange是否存在，如果不存在则组装amqp_error这个错误结构，将这个错误结构发送给客户端，同时调用这个函数的接口会终止掉
lookup_or_die(Name) ->
	case lookup(Name) of
		{ok, X}            -> X;
		{error, not_found} -> rabbit_misc:not_found(Name)
	end.


%% 列出当前RabbitMQ系统集群下的所有exchange交换机
list() -> mnesia:dirty_match_object(rabbit_exchange, #exchange{_ = '_'}).

%% Not dirty_match_object since that would not be transactional when used in a
%% tx context
%% 列出VHostPath下所有的exchange交换机
list(VHostPath) ->
	mnesia:async_dirty(
	  fun () ->
			   mnesia:match_object(
				 rabbit_exchange,
				 #exchange{name = rabbit_misc:r(VHostPath, exchange), _ = '_'},
				 read)
	  end).


%% 查询名字为Name交换机字段scratch中App对应的值
lookup_scratch(Name, App) ->
	case lookup(Name) of
		{ok, #exchange{scratches = undefined}} ->
			{error, not_found};
		{ok, #exchange{scratches = Scratches}} ->
			case orddict:find(App, Scratches) of
				{ok, Value} -> {ok, Value};
				error       -> {error, not_found}
			end;
		{error, not_found} ->
			{error, not_found}
	end.


%% 对Name交换机App对应的值执行Fun函数，然后得到新的值更新App对应的值
update_scratch(Name, App, Fun) ->
	rabbit_misc:execute_mnesia_transaction(
	  fun() ->
			  update(Name,
					 fun(X = #exchange{scratches = Scratches0}) ->
							 Scratches1 = case Scratches0 of
											  undefined -> orddict:new();
											  _         -> Scratches0
										  end,
							 Scratch = case orddict:find(App, Scratches1) of
										   {ok, S} -> S;
										   error   -> undefined
									   end,
							 Scratches2 = orddict:store(
											App, Fun(Scratch), Scratches1),
							 X#exchange{scratches = Scratches2}
					 end),
			  ok
	  end).


%% 如果有新的exchange描述信息增加，则需要重新更新exchange的描述信息
update_decorators(Name) ->
	rabbit_misc:execute_mnesia_transaction(
	  fun() ->
			  case mnesia:wread({rabbit_exchange, Name}) of
				  [X] -> store_ram(X),
						 ok;
				  []  -> ok
			  end
	  end).


%% 将Name对应的exchange交换机信息执行Fun函数，然后得到最新的数据存储到exchange相关的表里
update(Name, Fun) ->
	case mnesia:wread({rabbit_exchange, Name}) of
		[X] -> X1 = Fun(X),
			   store(X1);
		[]  -> not_found
	end.


%% immutable：不可变的
immutable(X) -> X#exchange{scratches  = none,
						   policy     = none,
						   decorators = none}.


%% 获取exchange交换机详细信息的关键key
info_keys() -> ?INFO_KEYS.


%% 对VHostPath下所有的exchange交换机信息执行F函数
map(VHostPath, F) ->
	%% TODO: there is scope for optimisation here, e.g. using a
	%% cursor, parallelising the function invocation
	lists:map(F, list(VHostPath)).


%% 根据Items中的列表得到对应exchange的相关信息
infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].


%% 读取exchange交换机的名字
i(name,        #exchange{name        = Name})       -> Name;

%% 读取exchange交换机的类型
i(type,        #exchange{type        = Type})       -> Type;

%% 读取exchange交换机是否是持久化
i(durable,     #exchange{durable     = Durable})    -> Durable;

%% 读取exchange交换机是否自动删除
i(auto_delete, #exchange{auto_delete = AutoDelete}) -> AutoDelete;

%% 读取exchange交换机是否是内部的交换机的标识
i(internal,    #exchange{internal    = Internal})   -> Internal;

%% 读取exchange交换机的参数
i(arguments,   #exchange{arguments   = Arguments})  -> Arguments;

%% 读取exchange交换机策略中X对应的值
i(policy,      X) ->  case rabbit_policy:name(X) of
						  none   -> '';
						  Policy -> Policy
					  end;

i(Item, _) -> throw({bad_argument, Item}).


%% 读取X交换机关键key对应的信息
info(X = #exchange{}) -> infos(?INFO_KEYS, X).


%% 读取X交换机Items列表中key对应的信息
info(X = #exchange{}, Items) -> infos(Items, X).


%% 得到VHostPath下所有exchange的所有信息
info_all(VHostPath) -> map(VHostPath, fun (X) -> info(X) end).


%% 得到VHostPath下所有exchange指定的Items列表中的相关信息
info_all(VHostPath, Items) -> map(VHostPath, fun (X) -> info(X, Items) end).


%% 在X交换机下根据路由规则得到对应队列的名字(如果存在exchange交换机绑定exchange交换机，则会跟被绑定的exchange交换机继续匹配)
route(#exchange{name = #resource{virtual_host = VHost, name = RName} = XName,
				decorators = Decorators} = X,
	  #delivery{message = #basic_message{routing_keys = RKs}} = Delivery) ->
	case RName of
		<<>> ->
			%% 如果exchange交换机的名字为空，则根据路由规则判断是否有指定的消费者，如果有则直接将消息发送给指定的消费者
			RKsSorted = lists:usort(RKs),
			[rabbit_channel:deliver_reply(RK, Delivery) ||
			   RK <- RKsSorted, virtual_reply_queue(RK)],
			[rabbit_misc:r(VHost, queue, RK) || RK <- RKsSorted,
												not virtual_reply_queue(RK)];
		_ ->
			%% 获得exchange交换机的描述模块
			Decs = rabbit_exchange_decorator:select(route, Decorators),
			%% route1进行实际的路由，寻找对应的队列
			lists:usort(route1(Delivery, Decs, {[X], XName, []}))
	end.


virtual_reply_queue(<<"amq.rabbitmq.reply-to.", _/binary>>) -> true;

virtual_reply_queue(_)                                      -> false.


%% 根据路由绑定key得到对应的消息队列名字列表
route1(_, _, {[], _, QNames}) ->
	QNames;

route1(Delivery, Decorators,
	   {[X = #exchange{type = Type} | WorkList], SeenXs, QNames}) ->
	%% 根据exchange的类型得到路由的处理模块，让该模块进行相关的路由，找到对应的队列
	ExchangeDests  = (type_to_module(Type)):route(X, Delivery),
	%% 获取交换机exchange描述模块提供的额外交换机
	DecorateDests  = process_decorators(X, Decorators, Delivery),
	%% 如果参数中配置有备用的交换机，则将该交换机拿出来
	AlternateDests = process_alternate(X, ExchangeDests),
	route1(Delivery, Decorators,
		   lists:foldl(fun process_route/2, {WorkList, SeenXs, QNames},
					   AlternateDests ++ DecorateDests  ++ ExchangeDests)
		  ).


%% alternate：备用
%% 如果参数中配置有备用的交换机，则将该交换机拿出来
process_alternate(X = #exchange{name = XName}, []) ->
	case rabbit_policy:get_arg(
		   <<"alternate-exchange">>, <<"alternate-exchange">>, X) of
		undefined -> [];
		AName     -> [rabbit_misc:r(XName, exchange, AName)]
	end;

process_alternate(_X, _Results) ->
	[].


%% decorator：装饰
%% 根据exchange交换机的描述模块进行路由
process_decorators(_, [], _) -> %% optimisation
	[];

process_decorators(X, Decorators, Delivery) ->
	lists:append([Decorator:route(X, Delivery) || Decorator <- Decorators]).


%% 实际的根据路由key找到对应的队列信息
%% 交换机exchange绑定的是自己
process_route(#resource{kind = exchange} = XName,
			  {_WorkList, XName, _QNames} = Acc) ->
	Acc;

%% 交换机Exchange绑定到其他的交换机Exchange(此处是第一次出现新路由到的交换机exchange)
process_route(#resource{kind = exchange} = XName,
			  {WorkList, #resource{kind = exchange} = SeenX, QNames}) ->
	{cons_if_present(XName, WorkList),
	 gb_sets:from_list([SeenX, XName]), QNames};

%% 交换机Exchange绑定到其他的交换机Exchange(此处是第一次以上出现交换机exchange，同时判断新路由出来的交换机是否已经路由过，如果路由过则忽略掉，否者添加到列表中)
process_route(#resource{kind = exchange} = XName,
			  {WorkList, SeenXs, QNames} = Acc) ->
	case gb_sets:is_element(XName, SeenXs) of
		true  -> Acc;
		false -> {cons_if_present(XName, WorkList),
				  gb_sets:add_element(XName, SeenXs), QNames}
	end;

%% 通过路由规则获得绑定的队列名字
process_route(#resource{kind = queue} = QName,
			  {WorkList, SeenXs, QNames}) ->
	{WorkList, SeenXs, [QName | QNames]}.


%% 判断XName交换机是否存在，如果存在则将该交换机放入到L列表中
cons_if_present(XName, L) ->
	case lookup(XName) of
		{ok, X}            -> [X | L];
		{error, not_found} -> L
	end.


%% 找到XName这个exchange交换机，然后对XName执行Fun函数
call_with_exchange(XName, Fun) ->
	rabbit_misc:execute_mnesia_tx_with_tail(
	  fun () -> case mnesia:read({rabbit_exchange, XName}) of
					[]  -> rabbit_misc:const({error, not_found});
					[X] -> Fun(X)
				end
	  end).


%% 删除exchange交换机的接口
delete(XName, IfUnused) ->
	Fun = case IfUnused of
			  %% 有条件的删除，如果该exchange交换机还有作为绑定源的绑定信息，则不能删除掉
			  true  -> fun conditional_delete/2;
			  %% 无条件的删除VHost在mnesia中的数据
			  false -> fun unconditional_delete/2
		  end,
	call_with_exchange(
	  XName,
	  %% 处理将XName的交换机删除的函数
	  fun (X) ->
			   case Fun(X, false) of
				   {deleted, X, Bs, Deletions} ->
					   rabbit_binding:process_deletions(
						 rabbit_binding:add_deletion(
						   XName, {X, deleted, Bs}, Deletions));
				   {error, _InUseOrNotFound} = E ->
					   rabbit_misc:const(E)
			   end
	  end).


%% 找到对应交换机的处理模块去验证绑定的合法性
validate_binding(X = #exchange{type = XType}, Binding) ->
	Module = type_to_module(XType),
	Module:validate_binding(X, Binding).


%% 查看exchange交换机是否需要自动删除，如果auto_delete为true，则如果没有了绑定信息，则将该exchange交换机信息删除掉
maybe_auto_delete(#exchange{auto_delete = false}, _OnlyDurable) ->
	not_deleted;

maybe_auto_delete(#exchange{auto_delete = true} = X, OnlyDurable) ->
	case conditional_delete(X, OnlyDurable) of
		{error, in_use}             -> not_deleted;
		{deleted, X, [], Deletions} -> {deleted, Deletions}
	end.


%% 有条件的删除，如果该exchange交换机还有作为绑定源的绑定信息，则不能删除掉
conditional_delete(X = #exchange{name = XName}, OnlyDurable) ->
	%% 如果XName这个exchange交换机已经没有绑定信息，则将该交换机删除
	case rabbit_binding:has_for_source(XName) of
		false  -> unconditional_delete(X, OnlyDurable);
		true   -> {error, in_use}
	end.


%% 无条件的删除VHost在mnesia中的数据
unconditional_delete(X = #exchange{name = XName}, OnlyDurable) ->
	%% this 'guarded' delete prevents unnecessary writes to the mnesia
	%% disk log
	%% 删除rabbit_durable_exchange这个表中的XName对应的信息
	case mnesia:wread({rabbit_durable_exchange, XName}) of
		[]  -> ok;
		%% 如果该交换机是持久化信息，则将持久化信息删除掉
		[_] -> ok = mnesia:delete({rabbit_durable_exchange, XName})
	end,
	%% 删除rabbit_exchange表中的数据
	ok = mnesia:delete({rabbit_exchange, XName}),
	%% 删除rabbit_exchange_serial表中的数据
	ok = mnesia:delete({rabbit_exchange_serial, XName}),
	%% 删除路由信息中绑定有改exchange交换机的路由信息
	Bindings = rabbit_binding:remove_for_source(XName),
	{deleted, X, Bindings, rabbit_binding:remove_for_destination(
	   XName, OnlyDurable)}.


%% 将rabbit_exchange_serial表拿到XName对应的next值加一
next_serial(XName) ->
	Serial = peek_serial(XName, write),
	ok = mnesia:write(rabbit_exchange_serial,
					  #exchange_serial{name = XName, next = Serial + 1}, write),
	Serial.


%% 从rabbit_exchange_serial表中读取XName对应的next值
peek_serial(XName) -> peek_serial(XName, read).


%% 从rabbit_exchange_serial表拿到XName对应的next值
peek_serial(XName, LockType) ->
	case mnesia:read(rabbit_exchange_serial, XName, LockType) of
		[#exchange_serial{next = Serial}]  -> Serial;
		_                                  -> 1
	end.


invalid_module(T) ->
	rabbit_log:warning("Could not find exchange type ~s.~n", [T]),
	put({xtype_to_module, T}, rabbit_exchange_type_invalid),
	rabbit_exchange_type_invalid.

%% Used with atoms from records; e.g., the type is expected to exist.
%% 根据exchange的类型得到对应的处理模块
type_to_module(T) ->
	case get({xtype_to_module, T}) of
		undefined ->
			case rabbit_registry:lookup_module(exchange, T) of
				{ok, Module}       -> put({xtype_to_module, T}, Module),
									  Module;
				{error, not_found} -> invalid_module(T)
			end;
		Module ->
			Module
	end.
