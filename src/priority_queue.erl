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

%% Priority queues have essentially the same interface as ordinary
%% queues, except that a) there is an in/3 that takes a priority, and
%% b) we have only implemented the core API we need.
%%
%% Priorities should be integers - the higher the value the higher the
%% priority - but we don't actually check that.
%%
%% in/2 inserts items with priority 0.
%%
%% We optimise the case where a priority queue is being used just like
%% an ordinary queue. When that is the case we represent the priority
%% queue as an ordinary queue. We could just call into the 'queue'
%% module for that, but for efficiency we implement the relevant
%% functions directly in here, thus saving on inter-module calls and
%% eliminating a level of boxing.
%%
%% When the queue contains items with non-zero priorities, it is
%% represented as a sorted kv list with the inverted Priority as the
%% key and an ordinary queue as the value. Here again we use our own
%% ordinary queue implemention for efficiency, often making recursive
%% calls into the same function knowing that ordinary queues represent
%% a base case.


-module(priority_queue).

-export([new/0, is_queue/1, is_empty/1, len/1, to_list/1, from_list/1,
         in/2, in/3, out/1, out_p/1, join/2, filter/2, fold/3, highest/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([q/0]).

-type(q() :: pqueue()).
-type(priority() :: integer() | 'infinity').
-type(squeue() :: {queue, [any()], [any()], non_neg_integer()}).
-type(pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}).

-spec(new/0 :: () -> pqueue()).
-spec(is_queue/1 :: (any()) -> boolean()).
-spec(is_empty/1 :: (pqueue()) -> boolean()).
-spec(len/1 :: (pqueue()) -> non_neg_integer()).
-spec(to_list/1 :: (pqueue()) -> [{priority(), any()}]).
-spec(from_list/1 :: ([{priority(), any()}]) -> pqueue()).
-spec(in/2 :: (any(), pqueue()) -> pqueue()).
-spec(in/3 :: (any(), priority(), pqueue()) -> pqueue()).
-spec(out/1 :: (pqueue()) -> {empty | {value, any()}, pqueue()}).
-spec(out_p/1 :: (pqueue()) -> {empty | {value, any(), priority()}, pqueue()}).
-spec(join/2 :: (pqueue(), pqueue()) -> pqueue()).
-spec(filter/2 :: (fun ((any()) -> boolean()), pqueue()) -> pqueue()).
-spec(fold/3 ::
        (fun ((any(), priority(), A) -> A), A, pqueue()) -> A).
-spec(highest/1 :: (pqueue()) -> priority() | 'empty').

-endif.

%%----------------------------------------------------------------------------
%% RabbitMQ系统自带实现的优先级队列(从OTP中的queue队列基础上改装)
new() ->
	{queue, [], [], 0}.


%% 判断给出的值是否是队列
is_queue({queue, R, F, L}) when is_list(R), is_list(F), is_integer(L) ->
	true;
is_queue({pqueue, Queues}) when is_list(Queues) ->
	lists:all(fun ({infinity, Q}) -> is_queue(Q);
				 ({P,        Q}) -> is_integer(P) andalso is_queue(Q)
			  end, Queues);
is_queue(_) ->
	false.


%% 判断优先级队列是否为空
is_empty({queue, [], [], 0}) ->
	true;
is_empty(_) ->
	false.


%% 得到优先级队列的长度
len({queue, _R, _F, L}) ->
	L;
len({pqueue, Queues}) ->
	lists:sum([len(Q) || {_, Q} <- Queues]).


%% 将队列元素转化为列表格式
to_list({queue, In, Out, _Len}) when is_list(In), is_list(Out) ->
	[{0, V} || V <- Out ++ lists:reverse(In, [])];
to_list({pqueue, Queues}) ->
	[{maybe_negate_priority(P), V} || {P, Q} <- Queues,
									  {0, V} <- to_list(Q)].


%% 用列表组装一个优先级队列
from_list(L) ->
	lists:foldl(fun ({P, E}, Q) -> in(E, P, Q) end, new(), L).


%% 向优先级队列中插入元素
in(Item, Q) ->
	in(Item, 0, Q).


%% 如果out列表中没有元素且in列表中有一个元素，则将in赋值给out，同时X元素放入in列表
in(X, 0, {queue, [_] = In, [], 1}) ->
	{queue, [X], In, 2};
%% 如果in和out列表中都有元素则直接将X元素放入in列表
in(X, 0, {queue, In, Out, Len}) when is_list(In), is_list(Out) ->
	{queue, [X | In], Out, Len + 1};
%% 如果该元素是有优先级的且队列类型为queue，同时该队列为空，则将队列转化为pqueue类型队列
in(X, Priority, _Q = {queue, [], [], 0}) ->
	in(X, Priority, {pqueue, []});
in(X, Priority, Q = {queue, _, _, _}) ->
	in(X, Priority, {pqueue, [{0, Q}]});
%% 将X元素按照Priority优先级插入对应优先级的队列中去
in(X, Priority, {pqueue, Queues}) ->
	%% 将优先级取反,方便优先级队列的排序
	P = maybe_negate_priority(Priority),
	{pqueue, case lists:keysearch(P, 1, Queues) of
				 {value, {_, Q}} ->
					 %% 有该优先级的队列，则在该队列里插入元素，然后替换该优先级的队列
					 lists:keyreplace(P, 1, Queues, {P, in(X, Q)});
				 false when P == infinity ->
					 [{P, {queue, [X], [], 1}} | Queues];
				 false ->
					 %% 新插入的队列不是infinity类型的则需要重新给非infinity类型的所有队列进行重新排序
					 case Queues of
						 [{infinity, InfQueue} | Queues1] ->
							 [{infinity, InfQueue} |
								  lists:keysort(1, [{P, {queue, [X], [], 1}} | Queues1])];
						 _ ->
							 lists:keysort(1, [{P, {queue, [X], [], 1}} | Queues])
					 end
			 end}.


%% 从优先级队列去除元素
%% 队列为空
out({queue, [], [], 0} = Q) ->
	{empty, Q};
%% in列表中有一个元素，而out列表为空的情况
out({queue, [V], [], 1}) ->
	{{value, V}, {queue, [], [], 0}};
%% in列表中有多个元素，而out列表中的元素为空
out({queue, [Y | In], [], Len}) ->
	%% 取出in列表中的最后一个元素为队列头元素，然后剩下的放在out列表中去，in列表中的第一个元素放入in列表中
	[V | Out] = lists:reverse(In, []),
	{{value, V}, {queue, [Y], Out, Len - 1}};
%% out列表中有一个元素
out({queue, In, [V], Len}) when is_list(In) ->
	%% 对in，out列表中的元素进行重新分配
	{{value, V}, r2f(In, Len - 1)};
%% out列表中有一个以上的元素的情况
out({queue, In, [V | Out], Len}) when is_list(In) ->
	{{value, V}, {queue, In, Out, Len - 1}};
%% 有多个优先级队列的情况
out({pqueue, [{P, Q} | Queues]}) ->
	{R, Q1} = out(Q),
	NewQ = case is_empty(Q1) of
			   true -> case Queues of
						   []           -> {queue, [], [], 0};
						   [{0, OnlyQ}] -> OnlyQ;
						   [_ | _]      -> {pqueue, Queues}
					   end;
			   false -> {pqueue, [{P, Q1} | Queues]}
		   end,
	{R, NewQ}.


%% 从最高优先级队列中得到一个元素
out_p({queue, _, _, _}       = Q) -> add_p(out(Q), 0);
out_p({pqueue, [{P, _} | _]} = Q) -> add_p(out(Q), maybe_negate_priority(P)).


add_p(R, P) -> case R of
				   {empty, Q}      -> {empty, Q};
				   {{value, V}, Q} -> {{value, V, P}, Q}
			   end.


%% 两个优先级队列合并在一起
join(A, {queue, [], [], 0}) ->
	A;
join({queue, [], [], 0}, B) ->
	B;
%% 两个队列都有元素，则将后面的队列增加到前面队列的后面
join({queue, AIn, AOut, ALen}, {queue, BIn, BOut, BLen}) ->
	{queue, BIn, AOut ++ lists:reverse(AIn, BOut), ALen + BLen};
join(A = {queue, _, _, _}, {pqueue, BPQ}) ->
	{Pre, Post} =
		lists:splitwith(fun ({P, _}) -> P < 0 orelse P == infinity end, BPQ),
	Post1 = case Post of
				[]                        -> [ {0, A} ];
				[ {0, ZeroQueue} | Rest ] -> [ {0, join(A, ZeroQueue)} | Rest ];
				_                         -> [ {0, A} | Post ]
			end,
	{pqueue, Pre ++ Post1};
join({pqueue, APQ}, B = {queue, _, _, _}) ->
	{Pre, Post} =
		lists:splitwith(fun ({P, _}) -> P < 0 orelse P == infinity end, APQ),
	Post1 = case Post of
				[]                        -> [ {0, B} ];
				[ {0, ZeroQueue} | Rest ] -> [ {0, join(ZeroQueue, B)} | Rest ];
				_                         -> [ {0, B} | Post ]
			end,
	{pqueue, Pre ++ Post1};
join({pqueue, APQ}, {pqueue, BPQ}) ->
	{pqueue, merge(APQ, BPQ, [])}.


%% 合并优先级队列
merge([], BPQ, Acc) ->
	lists:reverse(Acc, BPQ);
merge(APQ, [], Acc) ->
	lists:reverse(Acc, APQ);
%% 两个队列的优先级相等
merge([{P, A} | As], [{P, B} | Bs], Acc) ->
	merge(As, Bs, [ {P, join(A, B)} | Acc ]);
%% 前面队列的优先级小于后面队列或则第一个队列的优先级为infinity
merge([{PA, A} | As], Bs = [{PB, _}|_], Acc) when PA < PB orelse PA == infinity ->
	merge(As, Bs, [ {PA, A} | Acc ]);
merge(As = [{_, _} | _], [{PB, B} | Bs], Acc) ->
	merge(As, Bs, [ {PB, B} | Acc ]).


%% 过滤掉Q优先级队列中的元素(处理方法是重新创建一个优先级队列)
filter(Pred, Q) -> fold(fun(V, P, Acc) ->
								case Pred(V) of
									true  -> in(V, P, Acc);
									false -> Acc
								end
						end, new(), Q).


%% 对优先级队列中的所有元素进行Fun函数操作，初始值为Init
fold(Fun, Init, Q) -> case out_p(Q) of
						  {empty, _Q}         -> Init;
						  {{value, V, P}, Q1} -> fold(Fun, Fun(V, P, Init), Q1)
					  end.


%% 拿到该优先级队列最高的优先级
highest({queue, [], [], 0})     -> empty;
highest({queue, _, _, _})       -> 0;
highest({pqueue, [{P, _} | _]}) -> maybe_negate_priority(P).


%% in列表为空的情况
r2f([],      0) -> {queue, [], [], 0};
%% in列表中有一个元素的情况，则将in列表赋予out列表
r2f([_] = R, 1) -> {queue, [], R, 1};
%% in列表中有两个元素的情况，则in，out列表中分别得到一个元素
r2f([X, Y],   2) -> {queue, [X], [Y], 2};
%% in列表中有多个元素(超过两个)，则in列表中得到两个元素，out列表得到剩下的元素
r2f([X, Y | R], L) -> {queue, [X, Y], lists:reverse(R, []), L}.


%% 反转优先级
maybe_negate_priority(infinity) -> infinity;
maybe_negate_priority(P)        -> -P.
