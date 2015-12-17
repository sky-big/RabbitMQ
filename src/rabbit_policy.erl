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

-module(rabbit_policy).

%% TODO specs

-behaviour(rabbit_runtime_parameter).

-include("rabbit.hrl").

-import(rabbit_misc, [pget/2]).

-export([register/0]).
-export([invalidate/0, recover/0]).
-export([name/1, get/2, get_arg/3, set/1]).
-export([validate/5, notify/4, notify_clear/3]).
-export([parse_set/6, set/6, delete/2, lookup/2, list/0, list/1,
         list_formatted/1, info_keys/0]).

-rabbit_boot_step({?MODULE,
				   [{description, "policy parameters"},
					{mfa, {rabbit_policy, register, []}},
					{requires, rabbit_registry},
					{enables, recovery}]}).

%% rabbit_policy启动步骤的执行的函数
register() ->
	rabbit_registry:register(runtime_parameter, <<"policy">>, ?MODULE).


%% 根据队列中字段policy拿到名字
name(#amqqueue{policy = Policy}) -> name0(Policy);

%% 根据exchange交换机中字段policy拿到名字
name(#exchange{policy = Policy}) -> name0(Policy).


%% 实际从Policy结构中拿去名字
name0(undefined) -> none;
name0(Policy)    -> pget(name, Policy).


%% 设置队列的policy字段
set(Q = #amqqueue{name = Name}) -> Q#amqqueue{policy = set0(Name)};

%% 设置exchange交换机policy字段
set(X = #exchange{name = Name}) -> X#exchange{policy = set0(Name)}.


%% 实际设置资源结构中policy字段数据的操作
set0(Name = #resource{virtual_host = VHost}) -> match(Name, list(VHost)).


%% 获取队列policy字段中definition json数据中Name对应的字段值
get(Name, #amqqueue{policy = Policy}) -> get0(Name, Policy);

%% 获取exchange交换机policy字段中definition json数据中Name对应的字段值
get(Name, #exchange{policy = Policy}) -> get0(Name, Policy);

%% Caution - SLOW.
%% 实际获取definition中Name对应的字段值
get(Name, EntityName = #resource{virtual_host = VHost}) ->
	get0(Name, match(EntityName, list(VHost))).


%% 实际获取definition中Name对应的字段值
get0(_Name, undefined) -> undefined;

get0(Name, List)       -> case pget(definition, List) of
							  undefined -> undefined;
							  Policy    -> pget(Name, Policy)
						  end.


%% Many heads for optimisation
get_arg(_AName, _PName,     #exchange{arguments = [], policy = undefined}) ->
	undefined;

get_arg(_AName,  PName, X = #exchange{arguments = []}) ->
	get(PName, X);

get_arg(AName,   PName, X = #exchange{arguments = Args}) ->
	case rabbit_misc:table_lookup(Args, AName) of
		undefined    -> get(PName, X);
		{_Type, Arg} -> Arg
	end.

%%----------------------------------------------------------------------------

%% Gets called during upgrades - therefore must not assume anything about the
%% state of Mnesia
%% 写policies_are_invalid文件，用来当节点启动的时候来让策略的恢复
invalidate() ->
	rabbit_file:write_file(invalid_file(), <<"">>).


%% 如果policies_are_invalid文件存在，则需要恢复所有交换机和队列的策略和修饰信息
recover() ->
	case rabbit_file:is_file(invalid_file()) of
		true  -> recover0(),
				 rabbit_file:delete(invalid_file());
		false -> ok
	end.

%% To get here we have to have just completed an Mnesia upgrade - i.e. we are
%% the first node starting. So we can rewrite the whole database.  Note that
%% recovery has not yet happened; we must work with the rabbit_durable_<thing>
%% variants.
%% 如果policies_are_invalid文件存在，则需要恢复所有交换机和队列的策略和修饰信息
recover0() ->
	%% 拿到所有持久化的exchange交换机信息
	Xs = mnesia:dirty_match_object(rabbit_durable_exchange, #exchange{_ = '_'}),
	%% 拿到所有持久化的queue队列信息
	Qs = mnesia:dirty_match_object(rabbit_durable_queue,    #amqqueue{_ = '_'}),
	%% 列出当前所有的策略
	Policies = list(),
	%% 重新设置所有持久化交换机的策略和修饰信息
	[rabbit_misc:execute_mnesia_transaction(
	   fun () ->
				mnesia:write(
				  rabbit_durable_exchange,
				  rabbit_exchange_decorator:set(
					X#exchange{policy = match(Name, Policies)}), write)
	   end) || X = #exchange{name = Name} <- Xs],
	%% 重新设置所有持久化队列的策略和修饰信息
	[rabbit_misc:execute_mnesia_transaction(
	   fun () ->
				mnesia:write(
				  rabbit_durable_queue,
				  rabbit_queue_decorator:set(
					Q#amqqueue{policy = match(Name, Policies)}), write)
	   end) || Q = #amqqueue{name = Name} <- Qs],
	ok.


%% 得到policies_are_invalid文件的路径
invalid_file() ->
	filename:join(rabbit_mnesia:dir(), "policies_are_invalid").

%%----------------------------------------------------------------------------
%% 设置RabbitMQ资源策略(使用的资源类型是队列和exchange交换机)
parse_set(VHost, Name, Pattern, Definition, Priority, ApplyTo) ->
	%% 优先级必须为空
	try list_to_integer(Priority) of
		Num -> parse_set0(VHost, Name, Pattern, Definition, Num, ApplyTo)
	catch
		error:badarg -> {error, "~p priority must be a number", [Priority]}
	end.


%% 实际将策略数据写入mnesia数据库的操作接口
parse_set0(VHost, Name, Pattern, Defn, Priority, ApplyTo) ->
	%% 解析传入的JSON参数信息
	case rabbit_misc:json_decode(Defn) of
		{ok, JSON} ->
			set0(VHost, Name,
				 [{<<"pattern">>,    list_to_binary(Pattern)},
				  {<<"definition">>, rabbit_misc:json_to_term(JSON)},
				  {<<"priority">>,   Priority},
				  {<<"apply-to">>,   ApplyTo}]);
		error ->
			{error_string, "JSON decoding error"}
	end.


%% 实际将策略数据写入mnesia数据库的操作接口
set(VHost, Name, Pattern, Definition, Priority, ApplyTo) ->
	PolicyProps = [{<<"pattern">>,    Pattern},
				   {<<"definition">>, Definition},
				   {<<"priority">>,   case Priority of
										  undefined -> 0;
										  _         -> Priority
									  end},
				   {<<"apply-to">>,   case ApplyTo of
										  undefined -> <<"all">>;
										  _         -> ApplyTo
									  end}],
	set0(VHost, Name, PolicyProps).


%% 将策略数据写入rabbit_runtime_parameters表
set0(VHost, Name, Term) ->
	rabbit_runtime_parameters:set_any(VHost, <<"policy">>, Name, Term, none).


%% 删除VHost下Name的策略
delete(VHost, Name) ->
	rabbit_runtime_parameters:clear_any(VHost, <<"policy">>, Name).


%% 查询VHost下Name对应的策略数据
lookup(VHost, Name) ->
	case rabbit_runtime_parameters:lookup(VHost, <<"policy">>, Name) of
		not_found  -> not_found;
		P          -> p(P, fun ident/1)
	end.


%% 列出当前所有的策略
list() ->
	list('_').


%% 列出VHost下的所有策略
list(VHost) ->
	list0(VHost, fun ident/1).


%% 将所有的策略信息根据优先级进行排序得到所有的策略信息
list_formatted(VHost) ->
	order_policies(list0(VHost, fun format/1)).


%% 列出VHost下的所有策略信息
list0(VHost, DefnFun) ->
	[p(P, DefnFun) || P <- rabbit_runtime_parameters:list(VHost, <<"policy">>)].


%% 根据优先级进行排序
order_policies(PropList) ->
	lists:sort(fun (A, B) -> pget(priority, A) < pget(priority, B) end,
			   PropList).


p(Parameter, DefnFun) ->
	Value = pget(value, Parameter),
	[{vhost,      pget(vhost, Parameter)},
	 {name,       pget(name, Parameter)},
	 {pattern,    pget(<<"pattern">>, Value)},
	 {'apply-to', pget(<<"apply-to">>, Value)},
	 {definition, DefnFun(pget(<<"definition">>, Value))},
	 {priority,   pget(<<"priority">>, Value)}].


%% 将definition对应的json数据打印出来
format(Term) ->
	{ok, JSON} = rabbit_misc:json_encode(rabbit_misc:term_to_json(Term)),
	list_to_binary(JSON).


ident(X) -> X.


info_keys() -> [vhost, name, 'apply-to', pattern, definition, priority].

%%----------------------------------------------------------------------------
%% 验证策略配置是否正确
validate(_VHost, <<"policy">>, Name, Term, _User) ->
	rabbit_parameter_validation:proplist(
	  Name, policy_validation(), Term).


%% 通知Name的策略的设置
notify(VHost, <<"policy">>, Name, Term) ->
	%% 向rabbit_event事件中心发布Name对应的策略的设置
	rabbit_event:notify(policy_set, [{name, Name} | Term]),
	%% 更新所有的exchange和队列的策略信息
	update_policies(VHost).


%% 通知Name的策略被清除掉
notify_clear(VHost, <<"policy">>, Name) ->
	%% 向rabbit_event事件中心发布Name对应的策略已经删除掉
	rabbit_event:notify(policy_cleared, [{name, Name}]),
	%% 更新所有的exchange和队列的策略信息
	update_policies(VHost).

%%----------------------------------------------------------------------------

%% [1] We need to prevent this from becoming O(n^2) in a similar
%% manner to rabbit_binding:remove_for_{source,destination}. So see
%% the comment in rabbit_binding:lock_route_tables/0 for more rationale.
%% [2] We could be here in a post-tx fun after the vhost has been
%% deleted; in which case it's fine to do nothing.
%% 更新所有的exchange和队列的策略信息
update_policies(VHost) ->
	Tabs = [rabbit_queue,    rabbit_durable_queue,
			rabbit_exchange, rabbit_durable_exchange],
	{Xs, Qs} = rabbit_misc:execute_mnesia_transaction(
				 fun() ->
						 %% 先将mnesia数据库表锁住
						 [mnesia:lock({table, T}, write) || T <- Tabs], %% [1]
						 case catch list(VHost) of
							 {error, {no_such_vhost, _}} ->
								 ok; %% [2]
							 Policies ->
								 {[update_exchange(X, Policies) ||
									 X <- rabbit_exchange:list(VHost)],
								  [update_queue(Q, Policies) ||
									 Q <- rabbit_amqqueue:list(VHost)]}
						 end
				 end),
	%% 通知所有的交换机策略信息的变化
	[catch notify(X) || X <- Xs],
	%% 通知所有的队列策略信息的变化
	[catch notify(Q) || Q <- Qs],
	ok.


%% 更新每个exchange交换机的策略信息
update_exchange(X = #exchange{name = XName, policy = OldPolicy}, Policies) ->
	case match(XName, Policies) of
		OldPolicy -> no_change;
		NewPolicy -> case rabbit_exchange:update(
							XName, fun (X0) ->
											rabbit_exchange_decorator:set(
											  X0 #exchange{policy = NewPolicy})
							end) of
						 #exchange{} = X1 -> {X, X1};
						 not_found        -> {X, X }
					 end
	end.


%% 更新每个队列的策略信息
update_queue(Q = #amqqueue{name = QName, policy = OldPolicy}, Policies) ->
	case match(QName, Policies) of
		OldPolicy -> no_change;
		NewPolicy -> case rabbit_amqqueue:update(
							QName, fun(Q1) ->
										   rabbit_queue_decorator:set(
											 Q1#amqqueue{policy = NewPolicy})
							end) of
						 #amqqueue{} = Q1 -> {Q, Q1};
						 not_found        -> {Q, Q }
					 end
	end.


%% 策略变化后回调各个模块
notify(no_change)->
	ok;

notify({X1 = #exchange{}, X2 = #exchange{}}) ->
	rabbit_exchange:policy_changed(X1, X2);

notify({Q1 = #amqqueue{}, Q2 = #amqqueue{}}) ->
	rabbit_amqqueue:policy_changed(Q1, Q2).


%% 根据Name得到匹配的Policy
match(Name, Policies) ->
	%% 同时拿到优先级最高的策略
	case lists:sort(fun sort_pred/2, [P || P <- Policies, matches(Name, P)]) of
		[]               -> undefined;
		[Policy | _Rest] -> Policy
	end.


%% 判断当前资源是否跟Policy是否匹配
matches(#resource{name = Name, kind = Kind, virtual_host = VHost}, Policy) ->
	matches_type(Kind, pget('apply-to', Policy)) andalso
		match =:= re:run(Name, pget(pattern, Policy), [{capture, none}]) andalso
		VHost =:= pget(vhost, Policy).


%% apply-to类型只能是all，exchanges，queues类型
matches_type(exchange, <<"exchanges">>) -> true;
matches_type(queue,    <<"queues">>)    -> true;
matches_type(exchange, <<"all">>)       -> true;
matches_type(queue,    <<"all">>)       -> true;
matches_type(_,        _)               -> false.


sort_pred(A, B) -> pget(priority, A) >= pget(priority, B).

%%----------------------------------------------------------------------------
policy_validation() ->
	[{<<"priority">>,   fun rabbit_parameter_validation:number/2, mandatory},
	 {<<"pattern">>,    fun rabbit_parameter_validation:regex/2,  mandatory},
	 {<<"apply-to">>,   fun apply_to_validation/2,                optional},
	 {<<"definition">>, fun validation/2,                         mandatory}].


validation(_Name, []) ->
	{error, "no policy provided", []};
validation(_Name, Terms) when is_list(Terms) ->
	{Keys, Modules} = lists:unzip(
						rabbit_registry:lookup_all(policy_validator)),
	[] = dups(Keys), %% ASSERTION
	Validators = lists:zipwith(fun (M, K) ->  {M, a2b(K)} end, Modules, Keys),
	case is_proplist(Terms) of
		true  -> {TermKeys, _} = lists:unzip(Terms),
				 case dups(TermKeys) of
					 []   -> validation0(Validators, Terms);
					 Dup  -> {error, "~p duplicate keys not allowed", [Dup]}
				 end;
		false -> {error, "definition must be a dictionary: ~p", [Terms]}
	end;
validation(_Name, Term) ->
	{error, "parse error while reading policy: ~p", [Term]}.


validation0(Validators, Terms) ->
	case lists:foldl(
		   fun (Mod, {ok, TermsLeft}) ->
					ModKeys = proplists:get_all_values(Mod, Validators),
					case [T || {Key, _} = T <- TermsLeft,
							   lists:member(Key, ModKeys)] of
						[]    -> {ok, TermsLeft};
						Scope -> {Mod:validate_policy(Scope), TermsLeft -- Scope}
					end;
			  (_, Acc) ->
				   Acc
		   end, {ok, Terms}, proplists:get_keys(Validators)) of
		{ok, []} ->
			ok;
		{ok, Unvalidated} ->
			{error, "~p are not recognised policy settings", [Unvalidated]};
		{Error, _} ->
			Error
	end.


a2b(A) -> list_to_binary(atom_to_list(A)).


dups(L) -> L -- lists:usort(L).


is_proplist(L) -> length(L) =:= length([I || I = {_, _} <- L]).


apply_to_validation(_Name, <<"all">>)       -> ok;
apply_to_validation(_Name, <<"exchanges">>) -> ok;
apply_to_validation(_Name, <<"queues">>)    -> ok;
apply_to_validation(_Name, Term) ->
	{error, "apply-to '~s' unrecognised; should be 'queues', 'exchanges' "
		 "or 'all'", [Term]}.
