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

%% 交换机exchange topic RountingKey和BindingKey进行匹配的类型的逻辑处理模块
-module(rabbit_exchange_type_topic).

%% rabbit_topic_trie_node表里存储的是节点数据(里面存储的是topic_trie_node数据结构，所有的路由信息都是从root节点出发)
%% rabbit_topic_trie_edge表里存储的是边数据(里面存储的是topic_trie_node数据结构，边的数据结构里面存储的有路由信息的单个单词)
%% rabbit_topic_trie_binding表里存储的是某个节点上的绑定信息(里面存储的是topic_trie_binding数据结构)
%% 比如路由信息hello.#.nb：
%% 1.有四个节点，第一个节点始终是root节点，然后是其他三个节点，
%% 2.有三条边信息，每个边数据结构里面带有对应的单词hello，#，nb
%% 3.在第四个节点上存在绑定的队列名字


-include("rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type topic"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"topic">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%%----------------------------------------------------------------------------
%% 拿到Topic类型的交换机exchange的描述信息
description() ->
	[{description, <<"AMQP topic exchange, as per the AMQP specification">>}].


serialise_events() -> false.

%% NB: This may return duplicate results in some situations (that's ok)
%% 路由函数，根据exchange结构和delivery结构得到需要将消息发送到的队列
route(#exchange{name = X},
	  #delivery{message = #basic_message{routing_keys = Routes}}) ->
	lists:append([begin
					  %%  将路由键按照.拆分成多个单词(routing key（由生产者在发布消息时指定）有如下规定：由0个或多个以”.”分隔的单词；每个单词只能以一字母或者数字组成)
					  Words = split_topic_key(RKey),
					  mnesia:async_dirty(fun trie_match/2, [X, Words])
				  end || RKey <- Routes]).


validate(_X) -> ok.


validate_binding(_X, _B) -> ok.


create(_Tx, _X) -> ok.


%% 删除exchange的回调接口
delete(transaction, #exchange{name = X}, _Bs) ->
	%% 删除exchange对应的所有节点
	trie_remove_all_nodes(X),
	%% 删除exchange对应的所有边
	trie_remove_all_edges(X),
	%% 删除Table所有Pattern模式下的所有信息
	trie_remove_all_bindings(X),
	ok;

delete(none, _Exchange, _Bs) ->
	ok.


policy_changed(_X1, _X2) -> ok.


%% 路由绑定信息的添加回调该模块进行相关的处理(transaction：事务)
add_binding(transaction, _Exchange, Binding) ->
	internal_add_binding(Binding);

add_binding(none, _Exchange, _Binding) ->
	ok.


%% 删除绑定信息的回调，清除该绑定信息的相关数据
remove_bindings(transaction, _X, Bs) ->
	%% See rabbit_binding:lock_route_tables for the rationale for
	%% taking table locks.
	case Bs of
		[_] -> ok;
		_   -> [mnesia:lock({table, T}, write) ||
				  T <- [rabbit_topic_trie_node,
						rabbit_topic_trie_edge,
						rabbit_topic_trie_binding]]
	end,
	[begin
		 Path = [{FinalNode, _} | _] =
					follow_down_get_path(X, split_topic_key(K)),
		 trie_remove_binding(X, FinalNode, D, Args),
		 remove_path_if_empty(X, Path)
	 end ||  #binding{source = X, key = K, destination = D, args = Args} <- Bs],
	ok;

remove_bindings(none, _X, _Bs) ->
	ok.


assert_args_equivalence(X, Args) ->
	rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------
%% exchange交换机内部添加绑定信息的接口
internal_add_binding(#binding{source = X, key = K, destination = D,
							  args = Args}) ->
	%% 创建新的节点和边
	FinalNode = follow_down_create(X, split_topic_key(K)),
	%% 创建绑定信息
	trie_add_binding(X, FinalNode, D, Args),
	ok.


%% 对单个单词进行匹配的入口
trie_match(X, Words) ->
	trie_match(X, root, Words, []).


%% 如果路由键单词为空则匹配所有绑定到该exchange的队列
trie_match(X, Node, [], ResAcc) ->
	trie_match_part(X, Node, "#", fun trie_match_skip_any/4, [],
					trie_bindings(X, Node) ++ ResAcc);

trie_match(X, Node, [W | RestW] = Words, ResAcc) ->
	lists:foldl(fun ({WArg, MatchFun, RestWArg}, Acc) ->
						 trie_match_part(X, Node, WArg, MatchFun, RestWArg, Acc)
				end, ResAcc, [%% 从路由信息单词判断是否有从Node节点到下一个节点的边
							  {W, fun trie_match/4, RestW},
							  %% 用*判断是否有从Node节点到下一个节点的边
							  {"*", fun trie_match/4, RestW},
							  %% 用#判断是否有从Node节点到下一个节点的边
							  {"#", fun trie_match_skip_any/4, Words}]).


%% 根据exchange名字X，Node节点名字，Word字查找是否存在到下一个节点的边，如果有，则从下一个节点继续往下匹配
trie_match_part(X, Node, Search, MatchFun, RestW, ResAcc) ->
	%% 根据exchange名字X，Node节点名字，Word字查找是否存在到下一个节点的边
	case trie_child(X, Node, Search) of
		{ok, NextNode} -> MatchFun(X, NextNode, RestW, ResAcc);
		error          -> ResAcc
	end.


%% #：匹配0个或者多个单词的匹配函数
trie_match_skip_any(X, Node, [], ResAcc) ->
	trie_match(X, Node, [], ResAcc);

trie_match_skip_any(X, Node, [_ | RestW] = Words, ResAcc) ->
	trie_match_skip_any(X, Node, RestW,
						trie_match(X, Node, Words, ResAcc)).


follow_down_create(X, Words) ->
	case follow_down_last_node(X, Words) of
		{ok, FinalNode}      -> FinalNode;
		{error, Node, RestW} -> %% 插入新的节点和边
								lists:foldl(
								  fun (W, CurNode) ->
										   %% 生成新的节点ID
										   NewNode = new_node_id(),
										   %% 增加节点，同时增加边的接口
										   trie_add_edge(X, CurNode, NewNode, W),
										   NewNode
								  end, Node, RestW)
	end.


follow_down_last_node(X, Words) ->
	follow_down(X, fun (_, Node, _) -> Node end, root, Words).


follow_down_get_path(X, Words) ->
	{ok, Path} =
		follow_down(X, fun (W, Node, PathAcc) -> [{Node, W} | PathAcc] end,
					[{root, none}], Words),
	Path.


follow_down(X, AccFun, Acc0, Words) ->
	follow_down(X, root, AccFun, Acc0, Words).


follow_down(_X, _CurNode, _AccFun, Acc, []) ->
	{ok, Acc};

follow_down(X, CurNode, AccFun, Acc, Words = [W | RestW]) ->
	case trie_child(X, CurNode, W) of
		{ok, NextNode} -> follow_down(X, NextNode, AccFun,
									  AccFun(W, NextNode, Acc), RestW);
		error          -> {error, Acc, Words}
	end.


remove_path_if_empty(_, [{root, none}]) ->
	ok;

remove_path_if_empty(X, [{Node, W} | [{Parent, _} | _] = RestPath]) ->
	case mnesia:read(rabbit_topic_trie_node,
					 #trie_node{exchange_name = X, node_id = Node}, write) of
		[] -> trie_remove_edge(X, Parent, Node, W),
			  remove_path_if_empty(X, RestPath);
		_  -> ok
	end.


%% 根据exchange名字X，Node节点名字，Word字查找节点信息
trie_child(X, Node, Word) ->
	case mnesia:read({rabbit_topic_trie_edge,
					  #trie_edge{exchange_name = X,
								 node_id       = Node,
								 word          = Word}}) of
		[#topic_trie_edge{node_id = NextNode}] -> {ok, NextNode};
		[]                                     -> error
	end.


%% 根据exchange名字X，节点名字Node得到绑定信息，即目标信息，即队列的名字资源结构
trie_bindings(X, Node) ->
	MatchHead = #topic_trie_binding{
									trie_binding = #trie_binding{exchange_name = X,
																 node_id       = Node,
																 destination   = '$1',
																 arguments     = '_'}},
	mnesia:select(rabbit_topic_trie_binding, [{MatchHead, [], ['$1']}]).


%% 操作节点相关信息的接口，如果没有查找到该节点则插入该节点的信息
trie_update_node_counts(X, Node, Field, Delta) ->
	E = case mnesia:read(rabbit_topic_trie_node,
						 #trie_node{exchange_name = X,
									node_id       = Node}, write) of
			[]   -> #topic_trie_node{trie_node = #trie_node{
															exchange_name = X,
															node_id       = Node},
									 edge_count    = 0,
									 binding_count = 0};
			[E0] -> E0
		end,
	case setelement(Field, E, element(Field, E) + Delta) of
		#topic_trie_node{edge_count = 0, binding_count = 0} ->
			ok = mnesia:delete_object(rabbit_topic_trie_node, E, write);
		EN ->
			ok = mnesia:write(rabbit_topic_trie_node, EN, write)
	end.


%% 增加节点，同时增加边的接口
trie_add_edge(X, FromNode, ToNode, W) ->
	%% 创建FromNode节点
	trie_update_node_counts(X, FromNode, #topic_trie_node.edge_count, +1),
	%% 增加FromNode节点到ToNode的边
	trie_edge_op(X, FromNode, ToNode, W, fun mnesia:write/3).


%% 减少节点，同时较少边的接口
trie_remove_edge(X, FromNode, ToNode, W) ->
	%% 删除FromNode节点
	trie_update_node_counts(X, FromNode, #topic_trie_node.edge_count, -1),
	%% 删除FromNode节点到ToNode节点的边
	trie_edge_op(X, FromNode, ToNode, W, fun mnesia:delete_object/3).


%% 删除或者增加边的操作
trie_edge_op(X, FromNode, ToNode, W, Op) ->
	ok = Op(rabbit_topic_trie_edge,
			#topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
													node_id       = FromNode,
													word          = W},
							 node_id   = ToNode},
			write).


%% 增加绑定信息
trie_add_binding(X, Node, D, Args) ->
	%% 增加Node节点的绑定数量
	trie_update_node_counts(X, Node, #topic_trie_node.binding_count, +1),
	%% 新加入一个新的绑定信息
	trie_binding_op(X, Node, D, Args, fun mnesia:write/3).


%% 删除绑定信息
trie_remove_binding(X, Node, D, Args) ->
	%% 更新Node节点的绑定数量
	trie_update_node_counts(X, Node, #topic_trie_node.binding_count, -1),
	%% 删除Node节点的绑定信息
	trie_binding_op(X, Node, D, Args, fun mnesia:delete_object/3).


%% 增加或者删除Node节点的绑定信息
trie_binding_op(X, Node, D, Args, Op) ->
	ok = Op(rabbit_topic_trie_binding,
			#topic_trie_binding{
								trie_binding = #trie_binding{exchange_name = X,
															 node_id       = Node,
															 destination   = D,
															 arguments     = Args}},
			write).



%% 删除exchange对应的所有节点
trie_remove_all_nodes(X) ->
	remove_all(rabbit_topic_trie_node,
			   #topic_trie_node{trie_node = #trie_node{exchange_name = X,
													   _             = '_'},
								_         = '_'}).


%% 删除exchange对应的所有边
trie_remove_all_edges(X) ->
	remove_all(rabbit_topic_trie_edge,
			   #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
													   _             = '_'},
								_         = '_'}).


%% 删除所有的边信息
trie_remove_all_bindings(X) ->
	remove_all(rabbit_topic_trie_binding,
			   #topic_trie_binding{
								   trie_binding = #trie_binding{exchange_name = X, _ = '_'},
								   _            = '_'}).


%% 删除Table所有Pattern模式下的所有信息
remove_all(Table, Pattern) ->
	lists:foreach(fun (R) -> mnesia:delete_object(Table, R, write) end,
				  mnesia:match_object(Table, Pattern, write)).


%% 生成新的节点ID
new_node_id() ->
	rabbit_guid:gen().


%%  将路由键按照.拆分成多个单词(routing key（由生产者在发布消息时指定）有如下规定：由0个或多个以”.”分隔的单词；每个单词只能以一字母或者数字组成)
split_topic_key(Key) ->
	split_topic_key(Key, [], []).


split_topic_key(<<>>, [], []) ->
	[];

split_topic_key(<<>>, RevWordAcc, RevResAcc) ->
	lists:reverse([lists:reverse(RevWordAcc) | RevResAcc]);

split_topic_key(<<$., Rest/binary>>, RevWordAcc, RevResAcc) ->
	split_topic_key(Rest, [], [lists:reverse(RevWordAcc) | RevResAcc]);

split_topic_key(<<C:8, Rest/binary>>, RevWordAcc, RevResAcc) ->
	split_topic_key(Rest, [C | RevWordAcc], RevResAcc).
