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

-module(rabbit_registry).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([register/3, unregister/2,
         binary_to_type/1, lookup_module/2, lookup_all/1]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(register/3 :: (atom(), binary(), atom()) -> 'ok').
-spec(unregister/2 :: (atom(), binary()) -> 'ok').
-spec(binary_to_type/1 ::
        (binary()) -> atom() | rabbit_types:error('not_found')).
-spec(lookup_module/2 ::
        (atom(), atom()) -> rabbit_types:ok_or_error2(atom(), 'not_found')).
-spec(lookup_all/1 :: (atom()) -> [{atom(), atom()}]).

-endif.

%%---------------------------------------------------------------------------
%% rabbit_registry进程启动的入口函数
start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%---------------------------------------------------------------------------
%% 注册入口
register(Class, TypeName, ModuleName) ->
	gen_server:call(?SERVER, {register, Class, TypeName, ModuleName}, infinity).


%% 取消注册入口
unregister(Class, TypeName) ->
	gen_server:call(?SERVER, {unregister, Class, TypeName}, infinity).

%% This is used with user-supplied arguments (e.g., on exchange
%% declare), so we restrict(限制) it to existing atoms only.  This means it
%% can throw a badarg, indicating that the type cannot have been
%% registered.
%% 根据二进制类型得到原子类型(且该原子必须存在于当前的虚拟机系统)
binary_to_type(TypeBin) when is_binary(TypeBin) ->
	case catch list_to_existing_atom(binary_to_list(TypeBin)) of
		{'EXIT', {badarg, _}} -> {error, not_found};
		TypeAtom              -> TypeAtom
	end.


%% 根据分类和类型得到对应的模块名
lookup_module(Class, T) when is_atom(T) ->
	case ets:lookup(?ETS_NAME, {Class, T}) of
		[{_, Module}] ->
			{ok, Module};
		[] ->
			{error, not_found}
	end.


%% 拿到Class类型对应的所有注册数据
lookup_all(Class) ->
	[{K, V} || [K, V] <- ets:match(?ETS_NAME, {{Class, '$1'}, '$2'})].

%%---------------------------------------------------------------------------
%% 内部使用的将特定的二进制类型数据转换为atom类型
internal_binary_to_type(TypeBin) when is_binary(TypeBin) ->
	list_to_atom(binary_to_list(TypeBin)).


%% 实际的注册操作函数
internal_register(Class, TypeName, ModuleName)
  when is_atom(Class), is_binary(TypeName), is_atom(ModuleName) ->
	%% 检查ClassModule这个行为是否在Module这个模块中定义
	ok = sanity_check_module(class_module(Class), ModuleName),
	%% 组装要存储的数据
	RegArg = {{Class, internal_binary_to_type(TypeName)}, ModuleName},
	%% 将组装的数据插入ETS表
	true = ets:insert(?ETS_NAME, RegArg),
	conditional_register(RegArg),
	ok.


%% 实际的取消注册的函数
internal_unregister(Class, TypeName) ->
	UnregArg = {Class, internal_binary_to_type(TypeName)},
	conditional_unregister(UnregArg),
	true = ets:delete(?ETS_NAME, UnregArg),
	ok.

%% register exchange decorator route callback only when implemented,
%% in order to avoid unnecessary decorator calls on the fast
%% publishing path
conditional_register({{exchange_decorator, Type}, ModuleName}) ->
	case erlang:function_exported(ModuleName, route, 2) of
		true  -> true = ets:insert(?ETS_NAME,
								   {{exchange_decorator_route, Type},
									ModuleName});
		false -> ok
	end;
conditional_register(_) ->
	ok.


conditional_unregister({exchange_decorator, Type}) ->
	true = ets:delete(?ETS_NAME, {exchange_decorator_route, Type}),
	ok;
conditional_unregister(_) ->
	ok.


%% sanity:明智(检查ClassModule这个行为是否在Module这个模块中定义)
sanity_check_module(ClassModule, Module) ->
	case catch lists:member(ClassModule,
							lists:flatten(
							  [Bs || {Attr, Bs} <-
										 Module:module_info(attributes),
									 Attr =:= behavior orelse
										 Attr =:= behaviour])) of
		{'EXIT', {undef, _}}  -> {error, not_module};
		false                 -> {error, {not_type, ClassModule}};
		true                  -> ok
	end.


%% 根据类型转换为对应的行为模式名字
%% exchange类型
class_module(exchange)            -> rabbit_exchange_type;
%% 用户验证相关
class_module(auth_mechanism)      -> rabbit_auth_mechanism;
class_module(runtime_parameter)   -> rabbit_runtime_parameter;
class_module(exchange_decorator)  -> rabbit_exchange_decorator;
class_module(queue_decorator)     -> rabbit_queue_decorator;
class_module(policy_validator)    -> rabbit_policy_validator;
class_module(ha_mode)             -> rabbit_mirror_queue_mode;
class_module(channel_interceptor) -> rabbit_channel_interceptor.

%%---------------------------------------------------------------------------
%% rabbit_registry进程的回调初始化函数
init([]) ->
	?ETS_NAME = ets:new(?ETS_NAME, [protected, set, named_table]),
	{ok, none}.


%% 同步处理注册消息
handle_call({register, Class, TypeName, ModuleName}, _From, State) ->
	ok = internal_register(Class, TypeName, ModuleName),
	{reply, ok, State};


%% 同步取消注册的消息
handle_call({unregister, Class, TypeName}, _From, State) ->
	ok = internal_unregister(Class, TypeName),
	{reply, ok, State};


handle_call(Request, _From, State) ->
	{stop, {unhandled_call, Request}, State}.


handle_cast(Request, State) ->
	{stop, {unhandled_cast, Request}, State}.


handle_info(Message, State) ->
	{stop, {unhandled_info, Message}, State}.


terminate(_Reason, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
