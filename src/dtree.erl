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

%% A dual-index tree.
%%
%% Entries have the following shape:
%%
%% +----+--------------------+---+
%% | PK | SK1, SK2, ..., SKN | V |
%% +----+--------------------+---+
%%
%% i.e. a primary key, set of secondary keys, and a value.
%%
%% There can be only one entry per primary key, but secondary keys may
%% appear in multiple(多个) entries.
%%
%% The set of secondary keys must be non-empty. Or, to put it another
%% way, entries only exist while their secondary key set is non-empty.

-module(dtree).

-export([empty/0, insert/4, take/3, take/2, take_all/2, drop/2,
         is_defined/2, is_empty/1, smallest/1, size/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([?MODULE/0]).

-opaque(?MODULE()  :: {gb_trees:tree(), gb_trees:tree()}).

-type(pk()         :: any()).
-type(sk()         :: any()).
-type(val()        :: any()).
-type(kv()         :: {pk(), val()}).

-spec(empty/0      :: () -> ?MODULE()).
-spec(insert/4     :: (pk(), [sk()], val(), ?MODULE()) -> ?MODULE()).
-spec(take/3       :: ([pk()], sk(), ?MODULE()) -> {[kv()], ?MODULE()}).
-spec(take/2       :: (sk(), ?MODULE()) -> {[kv()], ?MODULE()}).
-spec(take_all/2   :: (sk(), ?MODULE()) -> {[kv()], ?MODULE()}).
-spec(drop/2       :: (pk(), ?MODULE()) -> ?MODULE()).
-spec(is_defined/2 :: (sk(), ?MODULE()) -> boolean()).
-spec(is_empty/1   :: (?MODULE()) -> boolean()).
-spec(smallest/1   :: (?MODULE()) -> kv()).
-spec(size/1       :: (?MODULE()) -> non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------
%% 初始化dtree(初始化两个空的gb_trees)
empty() -> {gb_trees:empty(), gb_trees:empty()}.

%% Insert an entry. Fails if there already is an entry with the given
%% primary key.
%% dtree的插入操作(第二主键列表为空)
insert(PK, [], V, {P, S}) ->
	%% dummy insert to force error if PK exists
	gb_trees:insert(PK, {gb_sets:empty(), V}, P),
	{P, S};

%% 插入的时候PK为第一主键，SKs作为第二主键列表
insert(PK, SKs, V, {P, S}) ->
	{%% 将PK做Key，将{gb_sets结构，V}作为value，插入P这个gb_trees平衡二叉树中(第一颗平衡二叉树存储主键对应的第二主键列表及对应的value值)
	 gb_trees:insert(PK, {gb_sets:from_list(SKs), V}, P),
	 %% 第二颗平衡二叉树存储第二主键到主键的映射
	 lists:foldl(fun (SK, S0) ->
						  case gb_trees:lookup(SK, S0) of
							  {value, PKS} -> PKS1 = gb_sets:insert(PK, PKS),
											  gb_trees:update(SK, PKS1, S0);
							  none         -> PKS = gb_sets:singleton(PK),
											  gb_trees:insert(SK, PKS, S0)
						  end
				 end, S, SKs)}.

%% Remove the given secondary key from the entries of the given
%% primary keys, returning the primary-key/value pairs of any entries
%% that were dropped as the result (i.e. due to their secondary key
%% set becoming empty). It is ok for the given primary keys and/or
%% secondary key to not exist.
%% dtree结构取元素的操作，删除第二主键SK，如果删除后的第二主键列表为空，则将{第一主键，value值}返回
take(PKs, SK, {P, S}) ->
	case gb_trees:lookup(SK, S) of
		none         -> {[], {P, S}};
		{value, PKS} -> %% 将传入的第一主键列表组装成gb_sets结构
						TakenPKS = gb_sets:from_list(PKs),
						%% 求得第二主键对应的第一主键列表和传入的第一主键列表的交集
						PKSInter = gb_sets:intersection(PKS, TakenPKS),
						%% 将gb_sets结构PKSInter的元素全部从gb_sets结构PKS删除掉(去掉第二主键对应的第一主键列表中的交集主键)
						PKSDiff  = gb_sets_difference  (PKS, PKSInter),
						%% PKSInter交集主键列表中删除第二主键SK后，如果第二主键列表为空，则将{第一主键，value值}返回
						{KVs, P1} = take2(PKSInter, SK, P),
						{KVs, {P1, case gb_sets:is_empty(PKSDiff) of
									   %% 如果第二主键对应的第一主键gb_sets为空，则将该第二主键从平衡二叉树中删除掉
									   true  -> gb_trees:delete(SK, S);
									   %% 如果第二主键对应的第一主键gb_sets不为空，则更新最新的第一主键列表
									   false -> gb_trees:update(SK, PKSDiff, S)
								   end}}
	end.

%% Remove the given secondary key from all entries, returning the
%% primary-key/value pairs of any entries that were dropped as the
%% result (i.e. due to their secondary key set becoming empty). It is
%% ok for the given secondary key to not exist.
%% 删除第二主键SK后，如果第二主键SK列表为空，则将{第一主键，value值}返回
take(SK, {P, S}) ->
	case gb_trees:lookup(SK, S) of
		none         -> {[], {P, S}};
		{value, PKS} -> %% PKS主键列表中删除第二主键SK后，如果第二主键列表为空，则将{第一主键，value值}返回
						{KVs, P1} = take2(PKS, SK, P),
						{KVs, {P1, gb_trees:delete(SK, S)}}
	end.


%% Drop all entries which contain the given secondary key, returning
%% the primary-key/value pairs of these entries. It is ok for the
%% given secondary key to not exist.
%% 删除第二主键SK对应的所有主键，并将这些主键对应的{第一主键，value值}值返回
take_all(SK, {P, S}) ->
	case gb_trees:lookup(SK, S) of
		none         -> {[], {P, S}};
		{value, PKS} -> %% KVs表示{第一主键，value}列表，SKS表示删除的第一主键对应的所有第二主键
						{KVs, SKS, P1} = take_all2(PKS, P),
						{KVs, {P1, prune(SKS, PKS, S)}}
	end.

%% Drop all entries for the given primary key (which does not have to exist).
%% 将第一主键PK删除掉
drop(PK, {P, S}) ->
	case gb_trees:lookup(PK, P) of
		none               -> {P, S};
		{value, {SKS, _V}} -> {gb_trees:delete(PK, P),
							   prune(SKS, gb_sets:singleton(PK), S)}
	end.


%% 判断第二主键是否存在于dtree结构中
is_defined(SK, {_P, S}) -> gb_trees:is_defined(SK, S).


%% 判断dtree结构是否为空
is_empty({P, _S}) -> gb_trees:is_empty(P).


%% 拿到dtree结构中第一主键最小的键值对
smallest({P, _S}) -> {K, {_SKS, V}} = gb_trees:smallest(P),
					 {K, V}.


%% 拿到dtree结构中的数据的数量
size({P, _S}) -> gb_trees:size(P).

%%----------------------------------------------------------------------------
%% 将PKS第一主键列表中对应的第二主键列表中存在SK这个第二主键的，将该SK从gb_sets结构中删除，如果gb_sets为空则将对应的主键删除掉，同时返回{第一主键，value值}
take2(PKS, SK, P) ->
	gb_sets:fold(fun (PK, {KVs, P0}) ->
						  %% 根据第一主键从gb_trees结构中取得元素
						  {SKS, V} = gb_trees:get(PK, P0),
						  %% 将第二主键SK从gb_sets结构中删除掉
						  SKS1 = gb_sets:delete(SK, SKS),
						  case gb_sets:is_empty(SKS1) of
							  %% 删除第二主键SK后，如果第二主键列表为空，则将该主键删除掉，同时返回{第一主键，value值}
							  true  -> KVs1 = [{PK, V} | KVs],
									   {KVs1, gb_trees:delete(PK, P0)};
							  %% 删除第二主键SK后，如果第二主键列表不为空，则更新该主键对应的键值对
							  false -> {KVs,  gb_trees:update(PK, {SKS1, V}, P0)}
						  end
				 end, {[], P}, PKS).


%% 删除所有的第一主键列表PKS，然后将他们对应的{第一主键，value}，以及它们对应的所有第二主键返回
take_all2(PKS, P) ->
	gb_sets:fold(fun (PK, {KVs, SKS0, P0}) ->
						  %% 取得PK主键对应的value值
						  {SKS, V} = gb_trees:get(PK, P0),
						  {[{PK, V} | KVs], gb_sets:union(SKS, SKS0),
						   gb_trees:delete(PK, P0)}
				 end, {[], gb_sets:empty(), P}, PKS).


%% 将第二主键列表SKS对应的主键gb_sets中，将存在第一主键PKS列表中的元素全部删除掉
prune(SKS, PKS, S) ->
	gb_sets:fold(fun (SK0, S0) ->
						  %% 拿到第二主键SK0对应的主键列表
						  PKS1 = gb_trees:get(SK0, S0),
						  %% 将PKS中存在的主键从第一主键PKS1中删除掉
						  PKS2 = gb_sets_difference(PKS1, PKS),
						  case gb_sets:is_empty(PKS2) of
							  %% 如果删除后的第一主键列表为空，则将该第二主键从gb_trees结构中删除掉
							  true  -> gb_trees:delete(SK0, S0);
							  false -> gb_trees:update(SK0, PKS2, S0)
						  end
				 end, S, SKS).


%% 将gb_sets结构S2中的元素从gb_sets结构中删除掉
gb_sets_difference(S1, S2) ->
	gb_sets:fold(fun gb_sets:delete_any/2, S1, S2).
