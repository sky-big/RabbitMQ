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

-module(rabbit_exchange_decorator).
%% exchange交换机描述信息逻辑处理模块

-include("rabbit.hrl").

-export([select/2, set/1, register/2, unregister/1]).

%% This is like an exchange type except that:
%%
%% 1) It applies to all exchanges as soon as it is installed, therefore
%% 2) It is not allowed to affect validation(验证), so no validate/1 or
%%    assert_args_equivalence/2
%%
%% It's possible in the future we might make decorators
%% able to manipulate(操作) messages as they are published.

-ifdef(use_specs).

-type(tx() :: 'transaction' | 'none').
-type(serial() :: pos_integer() | tx()).

-callback description() -> [proplists:property()].

%% Should Rabbit ensure that all binding events that are
%% delivered to an individual(个体) exchange can be serialised? (they
%% might still be delivered out of order, but there'll be a
%% serial number).
-callback serialise_events(rabbit_types:exchange()) -> boolean().

%% called after declaration and recovery
-callback create(tx(), rabbit_types:exchange()) -> 'ok'.

%% called after exchange (auto)deletion.
-callback delete(tx(), rabbit_types:exchange(), [rabbit_types:binding()]) ->
    'ok'.

%% called when the policy attached to this exchange changes.
-callback policy_changed(rabbit_types:exchange(), rabbit_types:exchange()) ->
    'ok'.

%% called after a binding has been added or recovered
-callback add_binding(serial(), rabbit_types:exchange(),
                      rabbit_types:binding()) -> 'ok'.

%% called after bindings have been deleted.
-callback remove_bindings(serial(), rabbit_types:exchange(),
                          [rabbit_types:binding()]) -> 'ok'.

%% Allows additional destinations to be added to the routing decision.
-callback route(rabbit_types:exchange(), rabbit_types:delivery()) ->
    [rabbit_amqqueue:name() | rabbit_exchange:name()].

%% Whether the decorator wishes to receive callbacks for the exchange
%% none:no callbacks, noroute:all callbacks except route, all:all callbacks
-callback active_for(rabbit_types:exchange()) -> 'none' | 'noroute' | 'all'.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {serialise_events, 1}, {create, 2}, {delete, 3},
     {policy_changed, 2}, {add_binding, 3}, {remove_bindings, 3},
     {route, 2}, {active_for, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%----------------------------------------------------------------------------

%% select a subset of active decorators
select(all,   {Route, NoRoute})  -> filter(Route ++ NoRoute);
select(route, {Route, _NoRoute}) -> filter(Route);
select(raw,   {Route, NoRoute})  -> Route ++ NoRoute.


%% 过滤掉Modules中代码模块不存在的代码模块名
filter(Modules) ->
	[M || M <- Modules, code:which(M) =/= non_existing].


%% 设置交换机Exchange X的描述信息
set(X) ->
	Decs = lists:foldl(fun (D, {Route, NoRoute}) ->
								ActiveFor = D:active_for(X),
								{cons_if_eq(all,     ActiveFor, D, Route),
								 cons_if_eq(noroute, ActiveFor, D, NoRoute)}
					   end, {[], []}, list()),
	X#exchange{decorators = Decs}.


%% 列出exchange交换机所有的描述注册信息
list() -> [M || {_, M} <- rabbit_registry:lookup_all(exchange_decorator)].


cons_if_eq(Select,  Select, Item,  List) -> [Item | List];
cons_if_eq(_Select, _Other, _Item, List) -> List.


%% 注册exchange交换机描述信息
register(TypeName, ModuleName) ->
	rabbit_registry:register(exchange_decorator, TypeName, ModuleName),
	[maybe_recover(X) || X <- rabbit_exchange:list()],
	ok.


%% 取消exchange描述信息的注册
unregister(TypeName) ->
	rabbit_registry:unregister(exchange_decorator, TypeName),
	[maybe_recover(X) || X <- rabbit_exchange:list()],
	ok.


%% exchange的描述信息有变化后，对exchange进行恢复更新操作
maybe_recover(X = #exchange{name       = Name,
							decorators = Decs}) ->
	#exchange{decorators = Decs1} = set(X),
	Old = lists:sort(select(all, Decs)),
	New = lists:sort(select(all, Decs1)),
	case New of
		Old -> ok;
		_   -> %% TODO create a tx here for non-federation decorators
			%% 新增加的描述模块进行回调
			[M:create(none, X) || M <- New -- Old],
			%% 让exchange更新描述信息
			rabbit_exchange:update_decorators(Name)
	end.
