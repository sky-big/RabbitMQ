%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2001-2011. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
%% =====================================================================
%% General Balanced Trees - highly efficient dictionaries.
%%
%% Copyright (C) 1999-2001 Sven-Olof Nystr�m, Richard Carlsson
%%
%% An efficient implementation of Prof. Arne Andersson's General
%% Balanced Trees. These have no storage overhead compared to plain
%% unbalanced binary trees, and their performance is in general better
%% than AVL trees.
%% ---------------------------------------------------------------------
%% Operations:
%%
%% - empty(): returns empty tree.
%%
%% - is_empty(T): returns 'true' if T is an empty tree, and 'false'
%%   otherwise.
%%
%% - size(T): returns the number of nodes in the tree as an integer.
%%   Returns 0 (zero) if the tree is empty.
%%
%% - lookup(X, T): looks up key X in tree T; returns {value, V}, or
%%   `none' if the key is not present.
%%
%% - get(X, T): retreives the value stored with key X in tree T. Assumes
%%   that the key is present in the tree.
%%
%% - insert(X, V, T): inserts key X with value V into tree T; returns
%%   the new tree. Assumes that the key is *not* present in the tree.
%%
%% - update(X, V, T): updates key X to value V in tree T; returns the
%%   new tree. Assumes that the key is present in the tree.
%%
%% - enter(X, V, T): inserts key X with value V into tree T if the key
%%   is not present in the tree, otherwise updates key X to value V in
%%   T. Returns the new tree.
%%
%% - delete(X, T): removes key X from tree T; returns new tree. Assumes
%%   that the key is present in the tree.
%%
%% - delete_any(X, T): removes key X from tree T if the key is present
%%   in the tree, otherwise does nothing; returns new tree.
%%
%% - balance(T): rebalances tree T. Note that this is rarely necessary,
%%   but may be motivated when a large number of entries have been
%%   deleted from the tree without further insertions. Rebalancing could
%%   then be forced in order to minimise lookup times, since deletion
%%   only does not rebalance the tree.
%%
%% - is_defined(X, T): returns `true' if key X is present in tree T, and
%%   `false' otherwise.
%%
%% - keys(T): returns an ordered list of all keys in tree T.
%%
%% - values(T): returns the list of values for all keys in tree T,
%%   sorted by their corresponding keys. Duplicates are not removed.
%%
%% - to_list(T): returns an ordered list of {Key, Value} pairs for all
%%   keys in tree T.
%%
%% - from_orddict(L): turns an ordered list L of {Key, Value} pairs into
%%   a tree. The list must not contain duplicate keys.
%%
%% - smallest(T): returns {X, V}, where X is the smallest key in tree T,
%%   and V is the value associated with X in T. Assumes that the tree T
%%   is nonempty.
%%
%% - largest(T): returns {X, V}, where X is the largest key in tree T,
%%   and V is the value associated with X in T. Assumes that the tree T
%%   is nonempty.
%%
%% - take_smallest(T): returns {X, V, T1}, where X is the smallest key
%%   in tree T, V is the value associated with X in T, and T1 is the
%%   tree T with key X deleted. Assumes that the tree T is nonempty.
%%
%% - take_largest(T): returns {X, V, T1}, where X is the largest key
%%   in tree T, V is the value associated with X in T, and T1 is the
%%   tree T with key X deleted. Assumes that the tree T is nonempty.
%%
%% - iterator(T): returns an iterator that can be used for traversing
%%   the entries of tree T; see `next'. The implementation of this is
%%   very efficient; traversing the whole tree using `next' is only
%%   slightly slower than getting the list of all elements using
%%   `to_list' and traversing that. The main advantage of the iterator
%%   approach is that it does not require the complete list of all
%%   elements to be built in memory at one time.
%%
%% - next(S): returns {X, V, S1} where X is the smallest key referred to
%%   by the iterator S, and S1 is the new iterator to be used for
%%   traversing the remaining entries, or the atom `none' if no entries
%%   remain.
%%
%% - map(F, T): maps the function F(K, V) -> V' to all key-value pairs
%%   of the tree T and returns a new tree T' with the same set of keys
%%   as T and the new set of values V'.

-module(gb_trees_test).

-export([empty/0, is_empty/ 1 , size/1 , lookup/2, get/2, insert/ 3 ,
       update/ 3 , enter/3 , delete/2, delete_any/ 2 , balance/1 ,
       is_defined/ 2 , keys/1 , values/1, to_list/ 1 , from_orddict/1 ,
       smallest/ 1 , largest/1 , take_smallest/1, take_largest/ 1 ,
       iterator/ 1 , next/1 , map/2]).

-export([
             test_insert/ 0
             ]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Data structure:
%% - {Size, Tree}, where `Tree' is composed of nodes of the form:
%%   - {Key, Value, Smaller, Bigger}, and the "empty tree" node:
%%   - nil.
%%
%% I make no attempt to balance trees after deletions. Since deletions
%% don't increase the height of a tree, I figure this is OK.
%%
%% Original balance condition h(T) <= ceil(c * log(|T|)) has been
%% changed to the similar (but not quite equivalent) condition 2 ^ h(T)
%% <= |T| ^ c. I figure this should also be OK.
%%
%% Performance is comparable to the AVL trees in the Erlang book (and
%% faster in general due to less overhead); the difference is that
%% deletion works for my trees, but not for the book's trees. Behaviour
%% is logaritmic (as it should be).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Some macros.

-define(p, 2). % It seems that p = 2 is optimal for sorted keys

-define(pow(A, _), A * A). % correct with exponent as defined above.

-define(div2(X), X bsr 1 ).                       %% 对X值除以2

-define(mul2(X), X bsl 1 ).                       %% 对X值乘以2

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Some types.

-type gb_tree_node() :: 'nil' | {_, _, _, _}.
-opaque iter() :: [gb_tree_node()].

%% A declaration equivalent to the following is currently hard-coded
%% in erl_types.erl
%%
%% -opaque gb_tree() :: {non_neg_integer(), gb_tree_node()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec empty() -> gb_tree().
%% gb_trees平衡二叉树的初始化
empty() ->
      { 0 , nil}.

-spec is_empty( Tree ) -> boolean() when
      Tree :: gb_tree().
%% 判断gb_trees平衡二叉树是否为空
is_empty({0, nil}) ->
      true;

is_empty(_) ->
      false.

-spec size( Tree ) -> non_neg_integer() when
      Tree :: gb_tree().
%% 得到gb_trees平衡二叉树的大小
size({Size, _}) when is_integer( Size ), Size >= 0 ->
       Size .

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lookup( Key , Tree ) -> 'none' | {'value', Val } when
      Key :: term(),
      Val :: term(),
      Tree :: gb_tree().
%% 通过Key查询平衡二叉树
lookup(Key, {_, T}) ->
      lookup_1( Key , T ).

%% The term order is an arithmetic total order, so we should not
%% test exact equality for the keys. (If we do, then it becomes
%% possible that neither `>', ` <', nor `=:=' matches.) Testing '<'
%% and '>' first is statistically better than testing for
%% equality, and also allows us to skip the test completely in the
%% remaining case.
%% 通过Key查询平衡二叉树对应的value值的实际操作函数
lookup_1(Key, {Key1, _, Smaller, _ }) when Key < Key1 ->
      lookup_1( Key , Smaller );

lookup_1(Key, {Key1, _, _, Bigger}) when Key > Key1 ->
      lookup_1( Key , Bigger );

lookup_1(_, {_, Value, _, _}) ->
      {value, Value };

lookup_1(_, nil) ->
      none.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% This is a specialized version of `lookup'.

-spec is_defined( Key , Tree ) -> boolean() when
      Key :: term(),
      Tree :: gb_tree().
%% 判断传入的Key是否是平衡二叉树中的Key
is_defined(Key, {_, T}) ->
      is_defined_1( Key , T ).

is_defined_1(Key, {Key1, _, Smaller, _ }) when Key < Key1 ->
      is_defined_1( Key , Smaller );

is_defined_1(Key, {Key1, _, _, Bigger}) when Key > Key1 ->
      is_defined_1( Key , Bigger );

is_defined_1(_, {_, _, _, _}) ->
      true;

is_defined_1(_, nil) ->
      false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% This is a specialized version of `lookup'.

-spec get( Key , Tree ) -> Val when
      Key :: term(),
      Tree :: gb_tree(),
      Val :: term().
%% 通过Key从平衡二叉树中读取对应的value值
get(Key, {_, T}) ->
      get_1( Key , T ).

get_1(Key, {Key1, _, Smaller, _ }) when Key < Key1 ->
      get_1( Key , Smaller );

get_1(Key, {Key1, _, _, Bigger}) when Key > Key1 ->
      get_1( Key , Bigger );

get_1(_, {_, Value, _, _}) ->
       Value .

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec update( Key , Val , Tree1) -> Tree2 when
      Key :: term(),
      Val :: term(),
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree().
%% 更新平衡二叉树中Key对应的value值
update(Key, Val, {S, T}) ->
       T1 = update_1(Key , Val, T),
      { S , T1 }.

%% See `lookup' for notes on the term comparison order.

update_1(Key, Value, { Key1 , V , Smaller, Bigger }) when Key < Key1 ->
      { Key1 , V , update_1(Key, Value, Smaller), Bigger };

update_1(Key, Value, { Key1 , V , Smaller, Bigger }) when Key > Key1 ->
      { Key1 , V , Smaller, update_1( Key , Value , Bigger)};

update_1(Key, Value, { _ , _ , Smaller, Bigger }) ->
      { Key , Value , Smaller, Bigger }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert( Key , Val , Tree1) -> Tree2 when
      Key :: term(),
      Val :: term(),
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree().
%% 平衡二叉树的数据插入的操作
insert(Key, Val, {S, T}) when is_integer( S ) ->
       %% 先增加平衡二叉树的大小
       S1 = S + 1,
       %% 传入的值为节点数 * 节点数
      { S1 , insert_1(Key , Val, T, ?pow(S1, ?p))}.


%% 此处是要插入的Key小于当前节点的Key1
insert_1(Key, Value, { Key1 , V , Smaller, Bigger }, S ) when Key < Key1 ->
       case insert_1(Key , Value, Smaller, ?div2 (S )) of
            { T1 , H1 , S1} ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的左子树下，组装该节点对应的平衡二叉树
                   T = {Key1 , V, T1, Bigger},
                   %% 得到右子树的高度和节点数
                  { H2 , S2 } = count(Bigger),
                   %% 将得到的左子树的高度和右子树高度的最大值乘以二
                   H = ?mul2 (erlang:max(H1, H2)),
                   %% 得到当前节点对应的二叉树对应的节点数
                   SS = S1 + S2 + 1,
                   %% 得到当前节点的节点数二次方值
                   P = ?pow (SS, ?p),
                   if
                         %% 如果当前节点对应的二叉树的2^高度 * 2 大于 当前节点数的二次方值则对该节点进行重新平衡操作
                         H > P ->
                               %% 平衡Key1该节点的左右子树
                              balance( T , SS );
                        true ->
                               %% 否则返回当前节点对应的二叉树以及当前二叉树的2^高度以及节点数
                              { T , H , SS}
                   end ;
             T1 ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的左子树下
                  { Key1 , V , T1, Bigger}
       end ;

%% 此处是要插入的Key大于当前节点的Key1
insert_1(Key, Value, { Key1 , V , Smaller, Bigger }, S ) when Key > Key1 ->
       case insert_1(Key , Value, Bigger, ?div2 (S )) of
            { T1 , H1 , S1} ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的左子树下，组装该节点对应的平衡二叉树 
                   T = {Key1 , V, Smaller, T1 },
                   %% 得到左子树的高度和节点数
                  { H2 , S2 } = count(Smaller),
                   %% 将得到的左子树的高度和右子树高度的最大值乘以二
                   H = ?mul2 (erlang:max(H1, H2)),
                   %% 得到当前节点对应的二叉树对应的节点数
                   SS = S1 + S2 + 1,
                   %% 得到当前节点的节点数二次方值
                   P = ?pow (SS, ?p),
                   if
                         %% 如果当前节点对应的二叉树的2^高度 * 2 大于 当前节点数的二次方值则对该节点进行重新平衡操作
                         H > P ->
                               %% 平衡Key1该节点的左右子树
                              balance( T , SS );
                        true ->
                               %% 否则返回当前节点对应的二叉树以及当前二叉树的2^高度以及节点数
                              { T , H , SS}
                   end ;
             T1 ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的右子树下
                  { Key1 , V , Smaller, T1 }
       end ;

%% 如果S为0同时要插入的二叉树为nil，则直接返回该节点值以及高度和节点数
insert_1(Key, Value, nil, S ) when S =:= 0 ->
      {{ Key , Value , nil, nil}, 1, 1};

%% S不为0则不进行重新平衡操作，直接返回节点二叉树
insert_1(Key, Value, nil, _S ) ->
      { Key , Value , nil, nil};

insert_1(Key, _, _, _) ->
      erlang:error({key_exists, Key }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec enter( Key , Val , Tree1) -> Tree2 when
      Key :: term(),
      Val :: term(),
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree().
%% 如果Key没有存在gb_trees平衡二叉树中，则将该键值对插入平衡二叉树，否则更新平衡二叉树
enter(Key, Val, T) ->
       case is_defined(Key , T) of
            true ->
                  update( Key , Val , T);
            false ->
                  insert( Key , Val , T)
       end .

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 得到gb_trees平衡二叉树的2^平衡二叉树的高度以及平衡二叉树的key-value键值对的数量
%% 例如平衡二叉树的高度是5，键值对数为7，则返回{2^5 = 32, 7}
count({_, _, nil, nil}) ->
      { 1 , 1 };

count({_, _, Sm, Bi}) ->
      { H1 , S1 } = count(Sm),
      { H2 , S2 } = count(Bi),
      { ?mul2 (erlang:max(H1 , H2)), S1 + S2 + 1};

count(nil) ->
      { 1 , 0 }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec balance( Tree1 ) -> Tree2 when
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree().
%% 重新平衡平衡二叉树
balance({S, T}) ->
      { S , balance(T , S)}.


balance(T, S) ->
      balance_list(to_list_1( T ), S ).


balance_list(L, S) ->
      { T , []} = balance_list_1(L , S),
       T .


balance_list_1(L, S) when S > 1 ->
       %% 减一表示父节点
       Sm = S - 1,
       %% 整个节点数减一后整除2后得到右子树的节点数
       S2 = Sm div 2 ,
       %% 剩下的节点表示左子树的节点数
       S1 = Sm - S2,
       %% 首先根据左子树的节点数S1创建左子树，返回后得到左子树，同时拿到当前节点的key-value键值对
      { T1 , [{K , V} | L1]} = balance_list_1( L , S1 ),
       %% 除去该节点的key-value键值对后，然后根据右子树的节点数和剩下的节点数创建该节点的右子树
      { T2 , L2 } = balance_list_1(L1, S2),
       T = {K , V, T1, T2},
      { T , L2 };

%% 在平分划分后的节点数为一的时候则直接创建节点(即表示该节点只有一个节点，则当前节点没有左右子节点，则直接同过该节点创建二叉树，虽然该二叉树只有一个节点)
balance_list_1([{Key, Val} | L], 1) ->
      {{ Key , Val , nil, nil}, L};

%% 在平分划分后的节点数为零的时候表示已经没有节点(表示该节点没有一个节点，所以直接返回nil)
balance_list_1(L, 0) ->
      {nil, L }.

-spec from_orddict( List ) -> Tree when
                                                   List :: [{Key :: term(), Val :: term()}],
                                                   Tree :: gb_tree().
%% 通过orddict数据结构的数据组装成一个gb_trees平衡二叉树
from_orddict(L) ->
       S = length(L ),
      { S , balance_list(L , S)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec delete_any( Key , Tree1 ) -> Tree2 when
      Key :: term(),
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree().
%% 删除gb_trees平衡二叉树节点中Key对应的键值对节点(如果Key不存在与gb_trees平衡二叉树节点中则会直接将要操作的平衡二叉树返回去)
delete_any(Key, T) ->
       %% 判断Key是否存在于平衡二叉树中
       case is_defined(Key , T) of
            true ->
                   %% 删除gb_trees平衡二叉树Key对应的键值对
                  delete( Key , T );
            false ->
                   T
       end .

%%% delete. Assumes that key is present.

-spec delete( Key , Tree1 ) -> Tree2 when
      Key :: term(),
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree().
%% 删除gb_trees平衡二叉树Key对应的键值对(如果Key没有存在gb_trees平衡二叉树中则会报错)
delete(Key, {S, T}) when is_integer( S ), S >= 0 ->
      { S - 1 , delete_1(Key, T)}.

%% See `lookup' for notes on the term comparison order.

delete_1(Key, {Key1, Value, Smaller, Larger }) when Key < Key1 ->
       %% 删除Key1节点左子树中Key对应的key-value键值对
       Smaller1 = delete_1(Key , Smaller),
      { Key1 , Value , Smaller1, Larger };

delete_1(Key, {Key1, Value, Smaller, Bigger }) when Key > Key1 ->
       %% 删除Key1节点右子树中Key对应的key-value键值对
       Bigger1 = delete_1(Key , Bigger),
      { Key1 , Value , Smaller, Bigger1 };

delete_1(_, {_, _, Smaller, Larger }) ->
      merge( Smaller , Larger ).


%% gb_trees平衡二叉树的节点删除后，需要根据它的左右子树组装成一个新的平衡二叉树(此处的方法是得到右子树的最小节点做为该左右子树的父节点)
merge(Smaller, nil) ->
       Smaller ;

merge(nil, Larger) ->
       Larger ;

merge(Smaller, Larger ) ->
      { Key , Value , Larger1} = take_smallest1( Larger ),
      { Key , Value , Smaller, Larger1 }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec take_smallest( Tree1 ) -> {Key, Val, Tree2} when
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree(),
      Key :: term(),
      Val :: term().
%% 从gb_trees平衡二叉树中取得key最小的键值对，然后将该节点的右子树作为上一层节点的左子树
take_smallest({Size, Tree}) when is_integer( Size ), Size >= 0 ->
      { Key , Value , Larger} = take_smallest1( Tree ),
      { Key , Value , {Size - 1, Larger}}.

take_smallest1({Key, Value, nil, Larger }) ->
      { Key , Value , Larger};

take_smallest1({Key, Value, Smaller, Larger }) ->
      { Key1 , Value1 , Smaller1} = take_smallest1( Smaller ),
      { Key1 , Value1 , {Key, Value, Smaller1, Larger }}.

-spec smallest( Tree ) -> {Key, Val} when
      Tree :: gb_tree(),
      Key :: term(),
      Val :: term().
%% 取得gb_trees平衡二叉树最小key对应的key-value键值对
smallest({_, Tree}) ->
      smallest_1( Tree ).


smallest_1({Key, Value, nil, _Larger }) ->
      { Key , Value };

smallest_1({_Key, _Value, Smaller , _Larger }) ->
      smallest_1( Smaller ).

-spec take_largest( Tree1 ) -> {Key, Val, Tree2} when
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree(),
      Key :: term(),
      Val :: term().
%% 取出gb_trees平衡二叉树中value最大的值
take_largest({Size, Tree}) when is_integer( Size ), Size >= 0 ->
      { Key , Value , Smaller} = take_largest1( Tree ),
      { Key , Value , {Size - 1, Smaller}}.


%% 拿到最大Key对应的平衡二叉树节点，然后将该节点拿掉，新的平衡二叉树是将最大节点的左子树作为上一层节点的右子树
take_largest1({Key, Value, Smaller, nil}) ->
      { Key , Value , Smaller};

take_largest1({Key, Value, Smaller, Larger }) ->
      { Key1 , Value1 , Larger1} = take_largest1( Larger ),
      { Key1 , Value1 , {Key, Value, Smaller, Larger1 }}.

-spec largest( Tree ) -> {Key, Val} when
      Tree :: gb_tree(),
      Key :: term(),
      Val :: term().
%% 查找gb_trees平衡二叉树中Key最大的key-value键值对
largest({_, Tree}) ->
      largest_1( Tree ).


largest_1({Key, Value, _Smaller, nil}) ->
      { Key , Value };

largest_1({_Key, _Value, _Smaller , Larger }) ->
      largest_1( Larger ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec to_list( Tree ) -> [{Key, Val}] when
      Tree :: gb_tree(),
      Key :: term(),
      Val :: term().
%% 将平衡二叉树gb_trees中的元素转化为列表形式
to_list({_, T}) ->
      to_list( T , []).


%% 将平衡二叉树gb_trees中的元素转化为列表形式
to_list_1(T) -> to_list( T , []).


to_list({Key, Value, Small, Big}, L) ->
      to_list( Small , [{Key , Value} | to_list( Big , L )]);

to_list(nil, L) -> L.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec keys( Tree ) -> [Key] when
      Tree :: gb_tree(),
      Key :: term().
%% 拿到gb_trees平衡二叉树中所有的key
keys({_, T}) ->
      keys( T , []).


keys({Key, _Value, Small , Big }, L) ->
      keys( Small , [Key | keys(Big, L)]);

keys(nil, L) -> L.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec values( Tree ) -> [Val] when
      Tree :: gb_tree(),
      Val :: term().
%% 拿到gb_trees平衡二叉树中所有的value
values({_, T}) ->
      values( T , []).


values({_Key, Value, Small, Big}, L) ->
      values( Small , [Value | values(Big, L)]);

values(nil, L) -> L.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec iterator( Tree ) -> Iter when
      Tree :: gb_tree(),
      Iter :: iter().
%% 得到gb_trees平衡二叉树的迭代器(通过迭代器遍历平衡二叉树，实际上是通过先序遍历二叉树的方式遍历gb_trees)
iterator({_, T}) ->
      iterator_1( T ).

iterator_1(T) ->
      iterator( T , []).

%% The iterator structure is really just a list corresponding to
%% the call stack of an in-order traversal. This is quite fast.

iterator({_, _, nil, _ } = T , As) ->
      [ T | As ];

iterator({_, _, L, _} = T, As) ->
      iterator( L , [T | As]);

iterator(nil, As) ->
       As .

-spec next( Iter1 ) -> 'none' | {Key , Val, Iter2} when
      Iter1 :: iter(),
      Iter2 :: iter(),
      Key :: term(),
      Val :: term().
%% 使用iterator接口得到的迭代器进行遍历平衡二叉树，该接口是得到平衡二叉树的下一个元素
next([{X, V, _, T} | As]) ->
      { X , V , iterator(T, As)};

next([]) ->
      none.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec map( Function , Tree1 ) -> Tree2 when
      Function :: fun ((K :: term(), V1 :: term()) -> V2 :: term()),
      Tree1 :: gb_tree(),
      Tree2 :: gb_tree().
%% 对gb_trees平衡二叉树中的key-value结构执行map操作，得到新的平衡二叉树
map(F, {Size, Tree}) when is_function( F , 2 ) ->
      { Size , map_1(F , Tree)}.


map_1(_, nil) -> nil;

map_1(F, {K, V, Smaller, Larger }) ->
      { K , F (K, V), map_1( F , Smaller ), map_1(F, Larger)}.


%% 平衡二叉树插入测试用例
test_insert() ->
       G = empty(),
       G1 = insert(1 , xxw1, G),
      io:format( "11111111111: ~p~n", [ G1 ]),
       G2 = insert(2 , xxw2, G1),
      io:format( "22222222222: ~p~n", [ G2 ]),
       G3 = insert(4 , xxw4, G2),
      io:format( "33333333333: ~p~n", [ G3 ]),
       G4 = insert(3 , xxw3, G3),
      io:format( "44444444444: ~p~n", [ G4 ]),
       G5 = insert(5 , xxw5, G4),
      io:format( "55555555555: ~p~n", [ G5 ]),
       G6 = insert(6 , xxw6, G5),
      io:format( "66666666666: ~p~n", [ G6 ]),
       G7 = insert(7 , xxw7, G6),
      io:format( "77777777777: ~p~n", [ G7 ]),
      { _ , G8 } = G7,
      io:format( "88888888888: ~p~n", [count( G8 )]),
       G9 = balance(G7 ),
      { _ , G10 } = G9,
      io:format( "99999999999: ~p~n", [ G9 ]),
      io:format( "10010101010: ~p~n", [count( G10 )]).

