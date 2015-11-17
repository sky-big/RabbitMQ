%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2007-2011. All Rights Reserved.
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
%% @author Richard Carlsson <richardc@it.uu.se>
%% @author Dan Gudmundsson <dgud@erix.ericsson.se>
%% @version 1.0

%% @doc Functional, extendible arrays. Arrays can have fixed size, or
%% can grow automatically as needed. A default value is used for entries
%% that have not been explicitly set.
%%
%% Arrays uses <b>zero </b> based indexing. This is a deliberate design
%% choice and differs from other erlang datastructures, e.g. tuples.
%%
%% Unless specified by the user when the array is created, the default
%% value is the atom `undefined'. There is no difference between an
%% unset entry and an entry which has been explicitly set to the same
%% value as the default one (cf. { @link reset/2}). If you need to
%% differentiate between unset and set entries, you must make sure that
%% the default value cannot be confused with the values of set entries.
%%
%% The array never shrinks automatically; if an index `I' has been used
%% successfully to set an entry, all indices in the range [0,`I'] will
%% stay accessible unless the array size is explicitly changed by
%% calling {@link resize/2}.
%%
%% Examples:
%% ```
%% %% Create a fixed-size array with entries 0-9 set to 'undefined'
%% A0 = array:new(10).
%% 10 = array:size(A0).
%%
%% %% Create an extendible array and set entry 17 to 'true',
%% %% causing the array to grow automatically
%% A1 = array:set(17, true, array:new()).
%% 18 = array:size(A1).
%%
%% %% Read back a stored value
%% true = array:get(17, A1).
%%
%% %% Accessing an unset entry returns the default value
%% undefined = array:get(3, A1).
%%
%% %% Accessing an entry beyond the last set entry also returns the
%% %% default value, if the array does not have fixed size
%% undefined = array:get(18, A1).
%%
%% %% "sparse" functions ignore default-valued entries
%% A2 = array:set(4, false, A1).
%% [{4, false}, {17, true}] = array:sparse_to_orddict(A2).
%%
%% %% An extendible array can be made fixed-size later
%% A3 = array:fix(A2).
%%
%% %% A fixed-size array does not grow automatically and does not
%% %% allow accesses beyond the last set entry
%% {'EXIT',{badarg,_}} = (catch array:set(18, true, A3)).
%% {'EXIT',{badarg,_}} = (catch array:get(18, A3)).
%% '''

%% @type array(). A functional, extendible array. The representation is
%% not documented and is subject to change without notice. Note that
%% arrays cannot be directly compared for equality.

-module(array_test).

-export([new/0, new/1, new/2, is_array/ 1 , set/3 , get/2, size/ 1 ,
       sparse_size/ 1 , default/1 , reset/2, to_list/ 1 , sparse_to_list/1 ,
       from_list/ 1 , from_list/2 , to_orddict/1, sparse_to_orddict/ 1 ,
       from_orddict/ 1 , from_orddict/2 , map/2, sparse_map/ 2 , foldl/3 ,
       foldr/ 3 , sparse_foldl/3 , sparse_foldr/3, fix/1, relax/ 1 , is_fix/1 ,
       resize/ 1 , resize/2 ]).

%%-define(TEST,1).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% Developers:
%%
%% For OTP devs: Both tests and documentation is extracted from this
%% file, keep and update this file,
%% test are extracted with array_SUITE:extract_tests().
%% Doc with docb_gen array.erl
%% 
%% The key to speed is to minimize the number of tests, on
%% large input. Always make the most probable path as short as possible.
%% In particular, keep in mind that for large trees, the probability of
%% a leaf node is small relative to that of an internal node.
%%
%% If you try to tweak the set_1 and get_1 loops: Measure, look at the
%% generated Beam code, and measure again! The argument order matters!


%% Representation:
%%
%% A tree is either a leaf, with LEAFSIZE elements (the "base"), an
%% internal node with LEAFSIZE+1 elements, or an unexpanded tree,
%% represented by a single integer: the number of elements that may be
%% stored in the tree when it is expanded. The last element of an
%% internal node caches the number of elements that may be stored in
%% each of its subtrees.
%%
%% Note that to update an entry in a tree of height h = log[b] n, the
%% total number of written words is (b+1)+(h-1)*(b+2), since tuples use
%% a header word on the heap. 4 is the optimal base for minimizing the
%% number of words written, but causes higher trees, which takes time.
%% The best compromise between speed and memory usage seems to lie
%% around 8-10. Measurements indicate that the optimum base for speed is
%% 24 - above that, it gets slower again due to the high memory usage.
%% Base 10 is a good choice, giving 2/3 of the possible speedup from
%% base 4, but only using 1/3 more memory. (Base 24 uses 65% more memory
%% per write than base 10, but the speedup is only 21%.)

-define(DEFAULT, undefined).                                                                                  %% 数组中的默认值
-define(LEAFSIZE, 10 ).         % the "base"
-define(NODESIZE, ?LEAFSIZE ).   % (no reason to have a different size)                        %% 节点的大小
-define(NODEPATTERN( S ), {_ ,_,_,_,_,_,_,_,_,_,S}). % NODESIZE+1 elements!                  %% 得到当前子节点能存储的元素个数
-define(NEW_NODE( S ),  % beware of argument duplication!
      setelement(( ?NODESIZE + 1 ), erlang:make_tuple((?NODESIZE + 1 ), (S )), (S ))).            %% 创建新的节点的操作(节点包含11个子节点元素)
-define(NEW_LEAF( D ), erlang:make_tuple(?LEAFSIZE , (D))).                                         %% 使用D生成10个元素的tuple
-define(NODELEAFS, ?NODESIZE * ?LEAFSIZE ).

%% These make the code a little easier to experiment with.
%% It turned out that using shifts (when NODESIZE=2^n) was not faster.
-define(reduce(X), ((X) div ( ?NODESIZE ))).                                                                   %% 对当前数组缩小十倍
-define(extend(X), ((X) * (?NODESIZE))).                                                                %% 对当前数组进行扩张十倍

%%--------------------------------------------------------------------------
%% 数组array组成的数据结构
-record(array, {
                        size :: non_neg_integer(),     %% number of defined entries               %% 数组目前的最大大小
                        max  :: non_neg_integer(),     %% maximum number of entries              %% 当前数组的最大大小
                         %% in current tree
                        default,     %% the default value (usually 'undefined')                        %% 数组中未使用的数组元素的默认值
                        elements     %% the tuple tree                                                       %% 元素实际的存储位置，是tuple树
                     }).
%% A declaration equivalent to the following one is hard-coded in erl_types.
%% That declaration contains hard-coded information about the #array{}
%% structure and the types of its fields.  So, please make sure that any
%% changes to its structure are also propagated to erl_types.erl.
%%
%% -opaque array() :: #array{}.

%%
%% Types
%%

-type array_indx() :: non_neg_integer().

-type array_opt()  :: {'fixed', boolean()} | 'fixed'
                    | {'default', Value :: term()}
                    | {'size', N :: non_neg_integer()}
                    | ( N :: non_neg_integer()).
-type array_opts() :: array_opt() | [array_opt()].

-type indx_pair()  :: {Index :: array_indx(), Value :: term()}.
-type indx_pairs() :: [indx_pair()].

%%--------------------------------------------------------------------------

%% @doc Create a new, extendible array with initial size zero.
%% @equiv new([])
%%
%% @see new/1
%% @see new/2

-spec new() -> array().
%% 创建一个新的array数组
new() ->
      new([]).

%% @doc Create a new array according to the given options. By default,
%% the array is extendible and has initial size zero. Array indices
%% start at 0.
%%
%% `Options' is a single term or a list of terms, selected from the
%% following:
%% <dl>
%%   <dt>`N::integer()' or `{size, N::integer()}' </dt>
%%   <dd>Specifies the initial size of the array; this also implies
%%   `{fixed, true}'. If `N' is not a nonnegative integer, the call
%%   fails with reason `badarg'. </dd>
%%   <dt>`fixed' or `{fixed, true}' </dt>
%%   <dd>Creates a fixed-size array; see also { @link fix/1}.</dd>
%%   <dt>`{fixed, false}' </dt>
%%   <dd>Creates an extendible (non fixed-size) array. </dd>
%%   <dt>`{default, Value}' </dt>
%%   <dd>Sets the default value for the array to `Value'. </dd>
%% </dl>
%% Options are processed in the order they occur in the list, i.e.,
%% later options have higher precedence.
%%
%% The default value is used as the value of uninitialized entries, and
%% cannot be changed once the array has been created.
%%
%% Examples:
%% ```array:new(100)''' creates a fixed-size array of size 100.
%% ```array:new({default,0})''' creates an empty, extendible array
%% whose default value is 0.
%% ```array:new([{size,10},{fixed,false},{default,-1}])''' creates an
%% extendible array with initial size 10 whose default value is -1.
%%
%% @see new/0
%% @see new/2
%% @see set/3
%% @see get/2
%% @see from_list/2
%% @see fix/1

-spec new( Options :: array_opts()) -> array().
%% 根据传入的参数配置Options生成一个新的array数组
new(Options) ->
      new_0( Options , 0 , false).

%% @doc Create a new array according to the given size and options. If
%% `Size' is not a nonnegative integer, the call fails with reason
%% `badarg'. By default, the array has fixed size. Note that any size
%% specifications in `Options' will override the `Size' parameter.
%%
%% If `Options' is a list, this is simply equivalent to `new([{size,
%% Size} | Options]', otherwise it is equivalent to `new([{size, Size} |
%% [Options]]'. However, using this function directly is more efficient.
%%
%% Example:
%% ```array:new(100, {default,0})''' creates a fixed-size array of size
%% 100, whose default value is 0.
%%
%% @see new/1

-spec new( Size :: non_neg_integer(), Options :: array_opts()) -> array().
%% 根据数组大小和配置参数生成一个新的array数组
new(Size, Options) when is_integer(Size ), Size >= 0 ->
      new_0( Options , Size , true);

new(_, _) ->
      erlang:error(badarg).


%% 数组元素使用自己定义的默认值创建数组array的接口
new_0(Options, Size , Fixed ) when is_list( Options ) ->
      new_1( Options , Size , Fixed, ?DEFAULT);

new_0(Options, Size , Fixed ) ->
      new_1([ Options ], Size , Fixed, ?DEFAULT).


%% 根据参数来创建数组array
new_1([fixed | Options], Size , _ , Default) ->
      new_1( Options , Size , true, Default);

new_1([{fixed, Fixed} | Options ], Size , _, Default)
  when is_boolean( Fixed ) ->
      new_1( Options , Size , Fixed, Default);

new_1([{default, Default} | Options ], Size , Fixed, _) ->
      new_1( Options , Size , Fixed, Default);

new_1([{size, Size} | Options ], _ , _, Default)
  when is_integer( Size ), Size >= 0 ->
      new_1( Options , Size , true, Default);

new_1([Size | Options], _ , _ , Default)
  when is_integer( Size ), Size >= 0 ->
      new_1( Options , Size , true, Default);

new_1([], Size, Fixed, Default) ->
      new( Size , Fixed , Default);

new_1(_Options, _Size , _Fixed , _Default) ->
      erlang:error(badarg).


%% 实际的根据数组实际大小，是否修正的参数，数组元素默认值进行创建数组array数据结构
new(0, false, undefined) ->
       %% Constant empty array
      #array{size = 0 , max = ?LEAFSIZE , elements = ?LEAFSIZE};

new(Size, Fixed, Default) ->
       %% 根据数组的实际大小得到数组的最大元素大小
       E = find_max(Size - 1, ?LEAFSIZE),
       M = if Fixed -> 0;
               true -> E
             end ,
      #array{size = Size , max = M , default = Default, elements = E }.

-spec find_max(integer(), integer()) -> integer().
%% 根据数组的实际大小得到数组的最大元素大小
find_max(I, M) when I >= M ->
    find_max(I, ?extend( M ));

find_max(_I, M) ->
    M.


%% @doc Returns `true' if `X' appears to be an array, otherwise `false'.
%% Note that the check is only shallow; there is no guarantee that `X'
%% is a well-formed array representation even if this function returns
%% `true'.

-spec is_array( X :: term()) -> boolean().
%% 判断传入的参数是否是数组结构
is_array(#array{size = Size, max = Max })
  when is_integer( Size ), is_integer(Max ) ->
      true;

is_array(_) ->
      false.


%% @doc Get the number of entries in the array. Entries are numbered
%% from 0 to `size(Array)-1'; hence, this is also the index of the first
%% entry that is guaranteed to not have been previously set.
%% @see set/3
%% @see sparse_size/1

-spec size( Array :: array()) -> non_neg_integer().
%% 拿到当前数组的大小
size(#array{size = N}) -> N;

size(_) -> erlang:error(badarg).


%% @doc Get the value used for uninitialized entries.
%%
%% @see new/2

-spec default( Array :: array()) -> term().
%% 拿到当前数组元素的没有值的时候的默认值
default(#array{default = D}) -> D;

default(_) -> erlang:error(badarg).


%% @doc Fix the size of the array. This prevents it from growing
%% automatically upon insertion; see also { @link set/3}.
%% @see relax/1

-spec fix( Array :: array()) -> array().
%% 将当前数组固定大小，不能对数组进行扩展
fix(#array{}=A) ->
       A #array{max = 0 }.


%% @doc Check if the array has fixed size.
%% Returns `true' if the array is fixed, otherwise `false'.
%% @see fix/1

-spec is_fix( Array :: array()) -> boolean().
%% 判断当前数组是否是固定大小
is_fix(#array{max = 0}) -> true;

is_fix(#array{}) -> false.


%% @doc Make the array resizable. (Reverses the effects of { @link
%% fix/1}.)
%% @see fix/1

-spec relax( Array :: array()) -> array().
%% 解除数组固定大小的限制
relax(#array{size = N} = A) ->
    A#array{max = find_max( N - 1 , ?LEAFSIZE)}.


%% @doc Change the size of the array. If `Size' is not a nonnegative
%% integer, the call fails with reason `badarg'. If the given array has
%% fixed size, the resulting array will also have fixed size.

-spec resize( Size :: non_neg_integer(), Array :: array()) -> array().
%% 重新设置当前数组的大小
resize(Size, #array{size = N , max = M , elements = E} = A)
  when is_integer( Size ), Size >= 0 ->
       if Size > N ->
               %% 如果设置的数组大小超过当前的数组的实际大小，则将数组进行扩展
               { E1 , M1 } = grow(Size - 1, E,
                                       if M > 0 -> M;
                                            true -> find_max(N - 1, ?LEAFSIZE )
                                       end ),
               A #array{size = Size ,
                           max = if M > 0 -> M1;
                                          true -> M
                                     end ,
                           elements = E1 };
         Size < N ->
               %% TODO: shrink physical representation when shrinking the array
               A #array{size = Size };
         true ->
               A
       end ;

resize(_Size, _) ->
      erlang:error(badarg).


%% @doc Change the size of the array to that reported by { @link
%% sparse_size/1}. If the given array has fixed size, the resulting
%% array will also have fixed size.
%% @equiv resize(sparse_size(Array), Array)
%% @see resize/2
%% @see sparse_size/1

-spec resize( Array :: array()) -> array().

resize(Array) ->
    resize(sparse_size( Array ), Array ).

%% @doc Set entry `I' of the array to `Value'. If `I' is not a
%% nonnegative integer, or if the array has fixed size and `I' is larger
%% than the maximum index, the call fails with reason `badarg'.
%%
%% If the array does not have fixed size, and `I' is greater than
%% `size(Array)-1', the array will grow to size `I+1'.
%%
%% @see get/2
%% @see reset/2

-spec set( I :: array_indx(), Value :: term(), Array :: array()) -> array().
%% 向数组中插入元素
set(I, Value, #array{size = N , max = M , default = D, elements = E } = A )
  when is_integer( I ), I >= 0 ->
       if
             %% 小于当前Size，则直接将数据插入到array数组中去
             I < N ->
                   A #array{elements = set_1(I , E, Value, D)};
             %% 超过当前的Size但是小于MAX点，则只需要将当前数组的实际最大元素个数加一
             I < M ->
                   %% (note that this cannot happen if M == 0, since N >= 0)
                   A #array{size = I + 1, elements = set_1( I , E , Value, D )};
             %% 如果没有固定大小，则会进行扩展数组的大小
             M > 0 ->
                   %% 对当前array数组最大上限元素扩大10倍
                  { E1 , M1 } = grow(I, E, M),
                   A #array{size = I + 1, max = M1 ,
                               %% 将要插入的元素插入到已经扩展过的array数组中
                              elements = set_1( I , E1 , Value, D)};
            true ->
                  erlang:error(badarg)
       end ;

set(_I, _V, _A) ->
      erlang:error(badarg).

%% See get_1/3 for details about switching and the NODEPATTERN macro.
%% 此情况下，当前数组中已经有元素(此时的array结构已经是tuple tree数据结构了)
set_1(I, E = ?NODEPATTERN( S ), X , D) ->
       %% 得到要插入元素在当前节点下的子节点中的位置
       I1 = I div S + 1 ,
      setelement( I1 , E , set_1(I rem S , element(I1 , E), X, D));

%% 当前数组中还没有元素，元素字段中还是当前数组实际的最大元素个数
set_1(I, E, X, D) when is_integer( E ) ->
      expand( I , E , X, D);

%% 当到达最底层的tuple，直接将要插入的元素放入到tuple指定的位置
set_1(I, E, X, _D) ->
      setelement( I + 1 , E, X).


%% Enlarging the array upwards to accommodate an index `I'
%% 对数组进行增长操作
%% 如果当前数组中还没有元素，则得到数组的最大元素个数
grow(I, E, _M) when is_integer( E ) ->
       M1 = find_max(I , E),
      { M1 , M1 };

%% 数组中已经存在元素的情况
grow(I, E, M) ->
      grow_1( I , E , M).

grow_1(I, E, M) when I >= M ->
       %% 将当前array数组扩展十倍，则以前的array结构应该存储在tuple中的第一个位置
      grow( I , setelement(1 , ?NEW_NODE( M ), E ), ?extend( M ));

grow_1(_I, E, M) ->
       %% 返回最新的array结构中的elements字段以及最大元素个数上限
      { E , M }.


%% Insert an element in an unexpanded node, expanding it as necessary.
%% 对数组进行扩展初始化(当前数组的实际的最大元素个数大于10)(次函数的实际作用是将还是整数没有元素的array数据结构转化为实际的tuple tree数据结构，同时将X插入数组中)
expand(I, S, X, D) when S > ?LEAFSIZE ->
       %% 缩小十倍后的数字
       S1 = ?reduce (S),
       %% 创建S节点下的10个子节点
      setelement( I div S1 + 1, ?NEW_NODE( S1 ),
                     expand( I rem S1, S1, X, D));

%% 当前数组的实际的最大元素个数小于等于10的情况
expand(I, _S, X, D) ->
      setelement( I + 1 , ?NEW_LEAF( D ), X ).


%% @doc Get the value of entry `I'. If `I' is not a nonnegative
%% integer, or if the array has fixed size and `I' is larger than the
%% maximum index, the call fails with reason `badarg'. 
%%
%% If the array does not have fixed size, this function will return the
%% default value for any index `I' greater than `size(Array)-1'.
 
%% @see set/3

-spec get( I :: array_indx(), Array :: array()) -> term().
%% 从数组中得到元素
get(I, #array{size = N , max = M , elements = E, default = D })
  when is_integer( I ), I >= 0 ->
       if
             %% 要取的元素不超过当前数组的最大元素个数，则直接从数组中读取元素
             I < N ->
               get_1( I , E , D);
         %% 要取的元素超过当前数组中的最大元素个数，同时数组的大小是非固定的，则返回给默认值
         M > 0 ->
               D ;
         %% 要取的元素超过当前数组中的最大元素个数，同时数组的大小是固定的，则直接报错
         true ->
               erlang:error(badarg)
       end ;

get(_I, _A) ->
      erlang:error(badarg).

%% The use of NODEPATTERN(S) to select the right clause is just a hack,
%% but it is the only way to get the maximum speed out of this loop
%% (using the Beam compiler in OTP 11).
%% 实际的从数组中读取元素的接口
get_1(I, E = ?NODEPATTERN( S ), D ) ->
      get_1( I rem S, element( I div S + 1, E), D);

get_1(_I, E, D) when is_integer( E ) ->
       D ;

get_1(I, E, _D) ->
       %% 从tuple中拿去元素
      element( I + 1 , E).


%% @doc Reset entry `I' to the default value for the array.
%% If the value of entry `I' is the default value the array will be
%% returned unchanged. Reset will never change size of the array.
%% Shrinking can be done explicitly by calling { @link resize/2}.
%%
%% If `I' is not a nonnegative integer, or if the array has fixed size
%% and `I' is larger than the maximum index, the call fails with reason
%% `badarg'; cf. { @link set/3}
%%
%% @see new/2
%% @see set/3

%% TODO: a reset_range function

-spec reset( I :: array_indx(), Array :: array()) -> array().
%% 将数组中I这个位置的数组元素重置为默认值
reset(I, #array{size = N , max = M , default = D, elements = E }=A )
  when is_integer( I ), I >= 0 ->
       if
             %% 只有当前I这个索引位置小于当前数组实际的最大元素个数才能进行重置操作
             I < N ->
               try A #array{elements = reset_1(I, E, D)}
               catch throw:default -> A
               end ;
         M > 0 ->
               A ;
         true ->
               erlang:error(badarg)
       end ;

reset(_I, _A) ->
      erlang:error(badarg).


%% 将数组中I这个位置的数组元素重置为默认值的实际操作函数
reset_1(I, E = ?NODEPATTERN( S ), D ) ->
       I1 = I div S + 1 ,
      setelement( I1 , E , reset_1(I rem S , element(I1 , E), D));

reset_1(_I, E, _D) when is_integer( E ) ->
      throw(default);

reset_1(I, E, D) ->
       Indx = I + 1,
       case element(Indx , E) of
             D -> throw(default);
             _ -> setelement(I + 1, E, D)
       end .

%% @doc Converts the array to a list.
%%
%% @see from_list/2
%% @see sparse_to_list/1

-spec to_list( Array :: array()) -> list().
%% 将数组中的元素组装成列表形式
to_list(#array{size = 0}) ->
      [];

to_list(#array{size = N, elements = E , default = D }) ->
      to_list_1( E , D , N - 1);

to_list(_) ->
      erlang:error(badarg).

%% this part handles the rightmost subtrees
%% 将array结构转化为列表的实际操作函数
to_list_1(E = ?NODEPATTERN( S ), D , I) ->
       N = I div S ,
      to_list_3( N , D , to_list_1(element(N + 1, E), D, I rem S ), E );

%% 将array数组中最后一个10个未展开的元素放到列表中
to_list_1(E, D, I) when is_integer( E ) ->
      push( I + 1 , D, []);

%% 将array数组中最后一个10已经展开的元素放到列表中
to_list_1(E, _D, I) ->
      push_tuple( I + 1 , E, []).

%% this part handles full trees only
%% 将array数组中单个节点中的元素转化到列表中
to_list_2(E = ?NODEPATTERN( _S ), D , L) ->
      to_list_3( ?NODESIZE , D , L, E);

%% 将数组array中还没有展开的元素，将默认值放入到列表中
to_list_2(E, D, L) when is_integer( E ) ->
      push( E , D , L);

%% 将array结构最底层节点中的tuple中的元素按照顺序放入到列表中
to_list_2(E, _D, L) ->
      push_tuple( ?LEAFSIZE , E , L).


%% 将array数组树中同一高度的节点中的元素放入到列表中去(是通过后序遍历的方式实现元素转化到列表中)
to_list_3(0, _D, L, _E) ->
       L ;

to_list_3(N, D, L, E) ->
      to_list_3( N - 1 , D, to_list_2(element( N , E ), D, L), E).


%% 将数组array中还没有展开的元素，将默认值放入到列表中
push(0, _E, L) ->
       L ;

push(N, E, L) ->
      push( N - 1 , E, [E | L]).


%% 将array结构最底层节点中的tuple中的元素按照顺序放入到列表中
push_tuple(0, _T, L) ->
       L ;

push_tuple(N, T, L) ->
      push_tuple( N - 1 , T, [element( N , T ) | L]).

%% @doc Converts the array to a list, skipping default-valued entries.
%%
%% @see to_list/1

-spec sparse_to_list( Array :: array()) -> list().
%% 将array数组转化为列表，但是忽略掉默认值
sparse_to_list(#array{size = 0}) ->
      [];

sparse_to_list(#array{size = N, elements = E , default = D }) ->
      sparse_to_list_1( E , D , N - 1);

sparse_to_list(_) ->
      erlang:error(badarg).

%% see to_list/1 for details
%% 实际将array数组转化为列表忽略掉默认值的操作函数
sparse_to_list_1(E=?NODEPATTERN( S ), D , I) ->
       N = I div S ,
      sparse_to_list_3( N , D ,
                               sparse_to_list_1(element( N + 1 , E), D, I rem S ),
                               E );

sparse_to_list_1(E, _D, _I) when is_integer( E ) ->
      [];

sparse_to_list_1(E, D, I) ->
      sparse_push_tuple( I + 1 , D, E, []).


%% 将array数组中单个节点中的元素转化到列表中
sparse_to_list_2(E = ?NODEPATTERN( _S ), D , L) ->
      sparse_to_list_3( ?NODESIZE , D , L, E);

sparse_to_list_2(E, _D, L) when is_integer( E ) ->
       L ;

%% 将array结构最底层节点中的tuple中的元素按照顺序放入到列表中
sparse_to_list_2(E, D, L) ->
      sparse_push_tuple( ?LEAFSIZE , D , E, L).


%% 将array数组树中同一高度的节点中的元素放入到列表中去(是通过后序遍历的方式实现元素转化到列表中)
sparse_to_list_3(0, _D, L, _E) ->
       L ;

sparse_to_list_3(N, D, L, E) ->
      sparse_to_list_3( N - 1 , D, sparse_to_list_2(element( N , E ), D, L), E ).


%% 将最底层节点中的tuple中的元素放入到列表中
sparse_push_tuple(0, _D, _T, L) ->
       L ;

sparse_push_tuple(N, D, T, L) ->
       case element(N , T) of
             D -> sparse_push_tuple(N - 1, D, T, L);
             E -> sparse_push_tuple(N - 1, D, T, [E | L])
       end .

%% @equiv from_list(List, undefined)

-spec from_list( List :: list()) -> array().
%% 通过传入的列表中的元素组成array数组
from_list(List) ->
    from_list(List, undefined).

%% @doc Convert a list to an extendible(可扩展的) array. `Default' is used as the value
%% for uninitialized(未初始化的) entries(条目) of the array. If `List' is not a proper list,
%% the call fails with reason `badarg'.
%%
%% @see new/2
%% @see to_list/1

-spec from_list( List :: list(), Default :: term()) -> array().
%% 实际的将列表中的元素转化为array数组的操作函数
from_list([], Default) ->
      new({default, Default });

from_list(List, Default) when is_list(List ) ->
      { E , N , M} = from_list_1( ?LEAFSIZE , List , Default, 0 , [], []),
      #array{size = N , max = M , default = Default, elements = E };

from_list(_, _) ->
      erlang:error(badarg).

%% Note: A cleaner but slower algorithm is to first take the length of
%% the list and compute the max size of the final tree, and then
%% decompose(分解) the list. The below algorithm(算法) is almost twice as fast,
%% however.

%% Building the leaf(叶子) nodes (padding the last one as necessary) and
%% counting the total number of elements.
from_list_1(0, Xs, D, N, As, Es) ->
       %% 得到节点
       E = list_to_tuple(lists:reverse(As )),
       case Xs of
            [] ->
                   case Es of
                        [] ->
                               %% 传入的列表元素不超过10个元素
                              { E , N , ?LEAFSIZE};
                         _ ->
                               %% 将得到的所有的最底层节点去组装array数组树
                              from_list_2_0( N , [E | Es], ?LEAFSIZE)
                   end ;
            [ _ | _ ] ->
                  from_list_1( ?LEAFSIZE , Xs , D, N, [], [ E | Es ]);
             _ ->
                  erlang:error(badarg)
       end ;

%% 此处是组装array数组中最底层节点，传入的列表元素凑不足10个元素后，则用默认值填充
from_list_1(I, Xs, D, N, As, Es) ->
       case Xs of
            [ X | Xs1 ] ->
                  from_list_1( I - 1 , Xs1, D, N + 1, [X | As], Es);
             _ ->
                   %% 凑不足10个元素后，需要将默认值放入列表
                  from_list_1( I - 1 , Xs, D, N, [D | As], Es)
       end .

%% Building the internal nodes (note that the input is reversed).
%% 将得到的所有的最底层节点去组装array数组树
from_list_2_0(N, Es, S) ->
      from_list_2( ?NODESIZE , pad((N - 1) div S + 1 , ?NODESIZE, S , Es ),
                         S , N , [S], []).


%% 组装array树中的每一层节点
from_list_2(0, Xs, S, N, As, Es) ->
       %% 组装单个节点
       E = list_to_tuple(As ),
       case Xs of
            [] ->
                   case Es of
                        [] ->
                               %% array数组构造完毕
                              { E , N , ?extend( S )};
                         _ ->
                               %% 从当前层的树进入上一层的树进行构造array数组树
                              from_list_2_0( N , lists:reverse([E | Es]),
                                                  %% 将元素扩展10倍
                                                  ?extend (S ))
                   end ;
             _ ->
                   %% 继续组装在自己左边的节点
                  from_list_2( ?NODESIZE , Xs , S, N, [S], [E | Es])
       end ;

from_list_2(I, [X | Xs], S, N, As, Es) ->
      from_list_2( I - 1 , Xs, S, N, [X | As], Es).


%% left-padding a list Es with elements P to the nearest multiple of K
%% elements from N (adding 0 to K-1 elements).
%% 将树当前层不足的元素补足
pad(N, K, P, Es) ->
    push((K - (N rem K )) rem K, P, Es).

%% @doc Convert the array to an ordered list of pairs `{Index, Value}'.
%%
%% @see from_orddict/2
%% @see sparse_to_orddict/1

-spec to_orddict( Array :: array()) -> indx_pairs().
%% 将array数组中的元素转化为orddict数据结构(orddict结构中是{Index, Value}数据结构)
to_orddict(#array{size = 0}) ->
      [];

to_orddict(#array{size = N, elements = E , default = D }) ->
       I = N - 1,
      to_orddict_1( E , I , D, I);

to_orddict(_) ->
      erlang:error(badarg).

%% see to_list/1 for comparison
%% 实际的将array数组中的元素转化为orddict结构中的元素
to_orddict_1(E = ?NODEPATTERN( S ), R , D, I) ->
       N = I div S ,
       I1 = I rem S ,
       %% 后续遍历到array结构先访问到最后一个节点上去
      to_orddict_3( N , R - I1 - 1, D,
                         to_orddict_1(element( N + 1 , E), R, D, I1),
                         E , S );

%% 将array数组中的最后一个节点中的10个未展开的元素转化为orddict结构中的元素
to_orddict_1(E, R, D, I) when is_integer( E ) ->
      push_pairs( I + 1 , R, D, []);

%% 将array数组中的最后一个节点中的10个已经展开的元素转化为orddict结构中的元素
to_orddict_1(E, R, _D, I) ->
      push_tuple_pairs( I + 1 , R, E, []).


%% 将array数组中某一节点中的元素转化为orddict结构中的元素
to_orddict_2(E = ?NODEPATTERN( S ), R , D, L) ->
      to_orddict_3( ?NODESIZE , R , D, L, E, S);

%% 该节点已经是最底层节点且元素没有展开，则将默认值组成index-value键值对放入到orddict结构中
to_orddict_2(E, R, D, L) when is_integer( E ) ->
      push_pairs( E , R , D, L);

%% 该节点已经是最底层节点但是元素已经展开，则将元素组成index-value键值对放入到orddict结构中
to_orddict_2(E, R, _D, L) ->
      push_tuple_pairs( ?LEAFSIZE , R , E, L).


%% 将array数组中树中某一层节点中的元素转化为orddict结构中的元素(按照先序的顺序遍历整个树) 
to_orddict_3(0, _R, _D, L, _E, _S) -> %% when is_integer(R) ->
       L ;

to_orddict_3(N, R, D, L, E, S) ->
       %% 遍历完一个节点中的所有数据后需要将总的数据量减去当前节点的数据量
      to_orddict_3( N - 1 , R - S, D,
                         to_orddict_2(element( N , E ), R, D, L),
                         E , S ).

-spec push_pairs(non_neg_integer(), array_indx(), term(), indx_pairs()) ->
        indx_pairs().
%% 第一个参数表示元素个数，第二个参数作为key，将第三个参数做为value组装key-value键值对存入列表中
push_pairs(0, _I, _E, L) ->
       L ;

push_pairs(N, I, E, L) ->
      push_pairs( N - 1 , I - 1, E, [{I, E} | L]).

-spec push_tuple_pairs(non_neg_integer(), array_indx(), term(), indx_pairs()) ->
        indx_pairs().
%% 第一个参数表示元素个数，第二个参数作为key，将第一个参数对应在第三个参数tuple中的元素作为value组装key-value键值对存入列表中
push_tuple_pairs(0, _I, _T, L) ->
       L ;

push_tuple_pairs(N, I, T, L) ->
      push_tuple_pairs( N - 1 , I - 1, T, [{I, element( N , T )} | L]).

%% @doc Convert the array to an ordered list of pairs `{Index, Value}',
%% skipping default-valued entries.
%%
%% @see to_orddict/1

-spec sparse_to_orddict( Array :: array()) -> indx_pairs().
%% 将array数组中的非默认值的元素转化为orddict结构中的元素(组装为index-value结构)
sparse_to_orddict(#array{size = 0}) ->
      [];

sparse_to_orddict(#array{size = N, elements = E , default = D }) ->
       I = N - 1,
      sparse_to_orddict_1( E , I , D, I);

sparse_to_orddict(_) ->
      erlang:error(badarg).

%% see to_orddict/1 for details
%% 实际的将array数组中的元素转化为orddict结构中的元素的操作
sparse_to_orddict_1(E = ?NODEPATTERN( S ), R , D, I) ->
       N = I div S ,
       I1 = I rem S ,
      sparse_to_orddict_3( N , R - I1 - 1, D,
                                    sparse_to_orddict_1(element( N + 1 , E ), R , D, I1),
                                     E , S );

sparse_to_orddict_1(E, _R, _D, _I) when is_integer( E ) ->
      [];

%% 将array数组中的最后一个节点中的10个已经展开的元素转化为orddict结构中的元素
sparse_to_orddict_1(E, R, D, I) ->
      sparse_push_tuple_pairs( I + 1 , R, D, E, []).


%% 将array数组中某一节点中的元素转化为orddict结构中的元素
sparse_to_orddict_2(E = ?NODEPATTERN( S ), R , D, L) ->
      sparse_to_orddict_3( ?NODESIZE , R , D, L, E, S);

%% 不会将默认值放入到orddict结构中
sparse_to_orddict_2(E, _R, _D, L) when is_integer( E ) ->
       L ;

sparse_to_orddict_2(E, R, D, L) ->
      sparse_push_tuple_pairs( ?LEAFSIZE , R , D, E, L).


%% 将array数组中树中某一层节点中的元素转化为orddict结构中的元素(按照先序的顺序遍历整个树)
sparse_to_orddict_3(0, _R, _D, L, _E, _S) -> % when is_integer(R) ->
       L ;

sparse_to_orddict_3(N, R, D, L, E, S) ->
       %% 遍历完一个节点中的所有数据后需要将总的数据量减去当前节点的数据量
      sparse_to_orddict_3( N - 1 , R - S, D,
                                    sparse_to_orddict_2(element( N , E ), R , D , L),
                                     E , S ).

-spec sparse_push_tuple_pairs(non_neg_integer(), array_indx(),
                        _ , _ , indx_pairs()) -> indx_pairs().
%% 将不是默认值的元素index-value键值对放入到orddict结构中
sparse_push_tuple_pairs(0, _I, _D, _T, L) ->
       L ;

sparse_push_tuple_pairs(N, I, D, T, L) ->
       case element(N , T) of
             %% 如果是默认值则不将该index-value键值对放入到orddict结构中
             D -> sparse_push_tuple_pairs(N - 1, I - 1, D, T, L);
             %% 如果不是默认值则将该index-value键值对放入到orddict结构中
             E -> sparse_push_tuple_pairs(N - 1, I - 1, D, T, [{I, E} | L ])
       end .

%% @equiv from_orddict(Orddict, undefined)

-spec from_orddict( Orddict :: indx_pairs()) -> array().
%% 通过orddict结构生成array数组结构
from_orddict(Orddict) ->
      from_orddict( Orddict , undefined).

%% @doc Convert an ordered list of pairs `{Index, Value}' to a
%% corresponding(相应的) extendible(可扩展的) array. `Default' is used as the value for
%% uninitialized(未初始化的) entries of the array. If `List' is not a proper,
%% ordered list of pairs whose first elements are nonnegative
%% integers, the call fails with reason `badarg'.
%%
%% @see new/2
%% @see to_orddict/1

-spec from_orddict( Orddict :: indx_pairs(), Default :: term()) -> array().
%% 实际通过orddict结构生成array数组结构函数
from_orddict([], Default) ->
      new({default, Default });

from_orddict(List, Default) when is_list(List ) ->
      { E , N , M} = from_orddict_0( List , 0 , ?LEAFSIZE, Default , []),
      #array{size = N , max = M , default = Default, elements = E };

from_orddict(_, _) ->
      erlang:error(badarg).

%% 2 pass implementation(实现), first pass builds the needed leaf nodes
%% and adds hole sizes.(from_orddict_0函数用来创建叶子节点，补充默认元素)
%% (inserts default elements for missing list entries in the leafs
%%  and pads the last tuple if necessary).
%% Second pass builds the tree from the leafs and the holes.
%%
%% Doesn't build/expand unnecessary leaf nodes which costs memory
%% and time for sparse arrays.
%% 此处已经将所有的orddict结构中的元素转化为array数组树中的叶子节点
from_orddict_0([], N, _Max, _D, Es) ->
       %% Finished, build the resulting tree
       case Es of
             %% 如果不超过10个元素，则直接返回一个叶子节点
            [ E ] ->
                  { E , N , ?LEAFSIZE};
             _ ->
                  collect_leafs( N , Es , ?LEAFSIZE)
       end ;

%% orddict结构中的第一个元素的索引大于当前叶子节点的元素的最大索引，因此中间会出现空的叶子节点
from_orddict_0(Xs = [{Ix1, _} | _], Ix, Max0, D, Es0)
  when Ix1 > Max0 , is_integer(Ix1) ->
       %% We have a hole larger than a leaf
       Hole = Ix1 - Ix,
       Step = Hole - (Hole rem ?LEAFSIZE ),
       Next = Ix + Step,
      from_orddict_0( Xs , Next , Next + ?LEAFSIZE, D , [Step | Es0]);

%% orddict结构中的第一个元素的索引小于等于当前叶子节点的元素的最大索引
from_orddict_0(Xs0 = [{ _ , _ } | _], Ix0, Max, D, Es) ->
       %% Fill a leaf(填充叶子节点)
       %% 返回剩余的orddict结构中的元素，以及组装的叶子节点
      { Xs , E , Ix} = from_orddict_1( Ix0 , Max , Xs0, Ix0, D, []),
      from_orddict_0( Xs , Ix , Ix + ?LEAFSIZE, D , [E | Es]);

from_orddict_0(Xs, _, _, _, _) ->
      erlang:error({badarg, Xs }).


%% 组装单个叶子节点(根据orddict结构中的元素，以及当前叶子节点的最小索引和最大索引组装当前叶子节点的元素结构)
from_orddict_1(Ix, Ix, Xs, N, _D, As) ->
       %% Leaf is full(组装成叶子节点)
       E = list_to_tuple(lists:reverse(As )),
      { Xs , E , N};

from_orddict_1(Ix, Max, Xs, N0, D, As) ->
       case Xs of
             %% orddict结构中的索引正好等于当前索引，因此将orddict结构中的元素放入到叶子节点中
            [{ Ix , Val } | Xs1] ->
                   N = Ix + 1,
                  from_orddict_1( N , Max , Xs1, N, D, [Val | As]);
             %% orddict结构中的索引大于当前索引，因此中间空余的元素用默认值填充
            [{ Ix1 , _ } | _] when is_integer( Ix1 ), Ix1 > Ix ->
                   N = Ix + 1,
                  from_orddict_1( N , Max , Xs, N, D, [D | As]);
            [ _ | _ ] ->
                  erlang:error({badarg, Xs });
             _ ->
                  from_orddict_1( Ix + 1 , Max, Xs, N0, D, [D | As])
       end .

%% Es is reversed i.e. starting from the largest leafs
%% 收集叶子节点
collect_leafs(N, Es, S) ->
       I = (N - 1) div S + 1 ,
       Pad = ((?NODESIZE - (I rem ?NODESIZE )) rem ?NODESIZE) * S ,
       case Pad of
             0 ->
                  collect_leafs( ?NODESIZE , Es , S, N, [S], []);
             _ ->   %% Pad the end
                  collect_leafs( ?NODESIZE , [Pad | Es], S, N, [S], [])
       end .



%% 实际执行收集叶子节点的接口(该接口是用来生成array数组中的每一层节点)
collect_leafs(0, Xs, S, N, As, Es) ->
       %% 组装当前层的一个节点
       E = list_to_tuple(As ),
       case Xs of
            [] ->
                   case Es of
                        [] ->
                              { E , N , ?extend( S )};
                         _ ->
                               %% 当前层的所有节点已经组装完毕，直接进入array数组树的上一层
                              collect_leafs( N , lists:reverse([E | Es]),
                                                  %% 将当前层扩展到上层
                                                  ?extend (S ))
                   end ;
             _ ->
                   %% 组装已经组装好的节点的左边节点
                  collect_leafs( ?NODESIZE , Xs , S, N, [S], [E | Es])            
       end ;

%% 如果当前节点尚未展开，还是整数的情形
collect_leafs(I, [X | Xs], S, N, As0, Es0)
  when is_integer( X ) ->
       %% A hole, pad accordingly.
       %% 得到当前跨过的步数
       Step0 = (X div S ),
       if
             Step0 < I ->
                   %% 填充Step0个整数元素
                   As = push(Step0 , S, As0),
                  collect_leafs( I - Step0 , Xs, S, N, As, Es0);
             I =:= ?NODESIZE ->
                   Step = Step0 rem ?NODESIZE ,
                   %% 填充Step个元素
                   As = push(Step , S, As0),
                  collect_leafs( I - Step , Xs, S, N, As, [X | Es0]);
             I =:= Step0 ->
                   %% 填充Step0个元素
                   As = push(I , S, As0),
                  collect_leafs( 0 , Xs , S, N, As, Es0);
            true ->
                   %% 填充完元素
                   As = push(I , S, As0),
                   Step = Step0 - I,
                   %% 将拆分出来的元素重新放入到orddict数据元素列表中，继续收集叶子节点
                  collect_leafs( 0 , [Step * S | Xs], S, N, As, Es0)
       end ;

%% orddict节点中当前节点已经展开
collect_leafs(I, [X | Xs], S, N, As, Es) ->
      collect_leafs( I - 1 , Xs, S, N, [X | As], Es);

%% orddict结构中的所有元素已经全部放入到array数组中
collect_leafs(?NODESIZE, [], S , N , [_], Es) ->
      collect_leafs( N , lists:reverse(Es ), ?extend( S )).

%%    Function = (Index::integer(), Value::term()) -> term()
%% @doc Map the given function onto each element of the array. The
%% elements are visited in order from the lowest index to the highest.
%% If `Function' is not a function, the call fails with reason `badarg'.
%%
%% @see foldl/3
%% @see foldr/3
%% @see sparse_map/2

-spec map( Function , Array :: array()) -> array() when
      Function :: fun ((Index :: array_indx(), Value :: _ ) -> _).
%% 对数组元素进行map操作
map(Function, Array = #array{size = N , elements = E, default = D })
  when is_function( Function , 2 ) ->
       if
             N > 0 ->
                   A = Array #array{elements = []}, % kill reference, for GC
                   A #array{elements = map_1(N - 1, E, 0, Function, D )};
            true ->
                   Array
       end ;

map(_, _) ->
      erlang:error(badarg).

%% It might be simpler to traverse(遍历) the array right-to-left, as done e.g.
%% in the to_orddict/1 function, but it is better to guarantee(保证)
%% left-to-right application over the elements - that is more likely to
%% be a generally(通常) useful property.
%% 实际的对array数组元素执行map操作，对单个节点进行map操作
map_1(N, E = ?NODEPATTERN( S ), Ix , F, D) ->
      list_to_tuple(lists:reverse([ S | map_2(1 , E, Ix, F, D, [],
                                                               N div S + 1 , N rem S , S )]));

%% 对array数组中未展开的节点进行map操作
map_1(N, E, Ix, F, D) when is_integer( E ) ->
       %% 将默认值展开，然后执行map操作
      map_1( N , unfold(E , D), Ix, F, D);

%% 进入array数组树底层的第一个已经展开的10个元素
map_1(N, E, Ix, F, D) ->
      list_to_tuple(lists:reverse(map_3( 1 , E , Ix, F, D, N + 1, []))).


%% 遍历单个节点下的所有子树(I表示当前遍历到的子树的索引，Ix表示元素在数组中的索引)
map_2(I, E, Ix, F, D, L, I, R, _S) ->
      map_2_1( I + 1 , E, [map_1( R , element(I , E), Ix, F, D) | L]);

map_2(I, E, Ix, F, D, L, N, R, S) ->
      map_2( I + 1 , E, Ix + S, F, D,
              [map_1( S - 1 , element(I, E), Ix, F, D) | L],
              N , R , S).


%% 此处如果tuple中的元素没有遍历完，则将后面的值放入到列表中
map_2_1(I, E, L) when I =< ?NODESIZE ->
      map_2_1( I + 1 , E, [element( I , E ) | L]);

map_2_1(_I, _E, L) ->
       L .

-spec map_3(pos_integer(), _ , array_indx(),
          fun ((array_indx(),_ ) -> _), _, non_neg_integer(), [ X ]) -> [X ].
%% 对最小的array节点进行map操作
map_3(I, E, Ix, F, D, N, L) when I =< N ->
      map_3( I + 1 , E, Ix + 1, F, D, N, [F(Ix, element( I , E )) | L]);

map_3(I, E, Ix, F, D, N, L) when I =< ?LEAFSIZE ->
      map_3( I + 1 , E, Ix + 1, F, D, N, [D | L]);

map_3(_I, _E, _Ix, _F, _D, _N, L) ->
       L .


%% 将S个元素(S > 10)元素展开成array数组的树结构，将S对应的子节点创建出来
unfold(S, _D) when S > ?LEAFSIZE ->
    ?NEW_NODE( ?reduce (S ));

%% 升程10个D的tuple结构
unfold(_S, D) ->
    ?NEW_LEAF( D ).

%%    Function = (Index::integer(), Value::term()) -> term()
%% @doc Map the given function onto each element of the array, skipping
%% default-valued entries. The elements are visited in order from the
%% lowest index to the highest. If `Function' is not a function, the
%% call fails with reason `badarg'.
%%
%% @see map/2

-spec sparse_map( Function , Array :: array()) -> array() when
      Function :: fun ((Index :: array_indx(), Value :: _ ) -> _).
%% 对array数组进行map操作，但是忽略掉默认值
sparse_map(Function, Array =#array{size = N , elements = E, default = D })
  when is_function( Function , 2 ) ->
       if N > 0 ->
               A = Array #array{elements = []}, % kill reference, for GC
               A #array{elements = sparse_map_1(N - 1, E, 0, Function, D )};
         true ->
               Array
       end ;

sparse_map(_, _) ->
      erlang:error(badarg).

%% see map/2 for details
%% TODO: we can probably optimize away the use of div/rem here
%% 实际的对array数组元素执行map操作，对单个节点进行map操作
sparse_map_1(N, E=?NODEPATTERN( S ), Ix , F, D) ->
      list_to_tuple(lists:reverse([ S | sparse_map_2(1 , E, Ix, F, D, [],
                                                                          N div S + 1,
                                                                          N rem S, S)]));

%% 如果是还未展开的节点，则直接返回节点数字
sparse_map_1(_N, E, _Ix, _F, _D) when is_integer( E ) ->
       E ;

%% 进入array数组树底层的第一个已经展开的10个元素
sparse_map_1(_N, E, Ix, F, D) ->
      list_to_tuple(lists:reverse(sparse_map_3( 1 , E , Ix, F, D, []))).


%% 遍历单个节点下的所有子树(I表示当前遍历到的子树的索引，Ix表示元素在数组中的索引)
sparse_map_2(I, E, Ix, F, D, L, I, R, _S) ->
      sparse_map_2_1( I + 1 , E,
                           [sparse_map_1( R , element(I , E), Ix, F, D) | L ]);

sparse_map_2(I, E, Ix, F, D, L, N, R, S) ->
      sparse_map_2( I + 1 , E, Ix + S, F, D,
                         [sparse_map_1( S - 1 , element(I, E), Ix, F, D) | L ],
                         N , R , S).


%% 此处如果tuple中的元素没有遍历完，则将后面的值放入到列表中
sparse_map_2_1(I, E, L) when I =< ?NODESIZE ->
      sparse_map_2_1( I + 1 , E, [element( I , E ) | L]);

sparse_map_2_1(_I, _E, L) ->
       L .

-spec sparse_map_3(pos_integer(), _ , array_indx(),
               fun ((array_indx(),_ ) -> _), _, [X]) -> [X].
%% 对10个元素的tuple进行map操作，如果是默认元素，则直接将元素放入列表
sparse_map_3(I, T, Ix, F, D, L) when I =< ?LEAFSIZE ->
       case element(I , T) of
             D -> sparse_map_3(I + 1, T, Ix + 1, F, D, [D | L]);
             E -> sparse_map_3(I + 1, T, Ix + 1, F, D, [F(Ix, E) | L])
       end ;

sparse_map_3(_I, _E, _Ix, _F, _D, L) ->
       L .

%% @doc Fold the elements of the array using the given function and
%% initial accumulator value. The elements are visited in order from the
%% lowest index to the highest. If `Function' is not a function, the
%% call fails with reason `badarg'.
%%
%% @see foldr/3
%% @see map/2
%% @see sparse_foldl/3

-spec foldl( Function , InitialAcc :: A, Array :: array()) -> B when
      Function :: fun ((Index :: array_indx(), Value :: _ , Acc :: A) -> B ).
%% 对array数组从左边开始进行foldl操作
foldl(Function, A , #array{size = N , elements = E, default = D })
  when is_function( Function , 3 ) ->
       if N > 0 ->
               foldl_1( N - 1 , E, A, 0, Function, D );
         true ->
               A
       end ;

foldl(_, _, _) ->
      erlang:error(badarg).


%% 对array数组中单个节点进行foldl操作
foldl_1(N, E = ?NODEPATTERN( S ), A , Ix, F, D) ->
      foldl_2( 1 , E , A, Ix, F, D, N div S + 1 , N rem S , S );

%% 对array数组中尚未展开的节点进行foldl操作
foldl_1(N, E, A, Ix, F, D) when is_integer( E ) ->
      foldl_1( N , unfold(E , D), A, Ix, F, D);

%% 对10个元素的tuple进行foldl操作
foldl_1(N, E, A, Ix, F, _D) ->
      foldl_3( 1 , E , A, Ix, F, N + 1).


%% 对array数组中的某个节点下的所有子树进行foldl操作
%% 该条件下的接口是已经遍历到最后一个子树
foldl_2(I, E, A, Ix, F, D, I, R, _S) ->
      foldl_1( R , element(I , E), A, Ix, F, D);

foldl_2(I, E, A, Ix, F, D, N, R, S) ->
      foldl_2( I + 1 , E, foldl_1( S - 1 , element(I, E), A, Ix, F, D),
                   Ix + S , F, D, N, R, S).

-spec foldl_3(pos_integer(), _ , A , array_indx(),
            fun ((array_indx, _ , A) -> B), integer()) -> B .
%% 实际对10个元素的tuple进行foldl操作的接口
foldl_3(I, E, A, Ix, F, N) when I =< N ->
      foldl_3( I + 1 , E, F(Ix, element( I , E ), A), Ix + 1, F, N);

foldl_3(_I, _E, A, _Ix, _F, _N) ->
       A .

%% @doc Fold the elements of the array using the given function and
%% initial accumulator value, skipping default-valued entries. The
%% elements are visited in order from the lowest index to the highest.
%% If `Function' is not a function, the call fails with reason `badarg'.
%%
%% @see foldl/3
%% @see sparse_foldr/3

-spec sparse_foldl( Function , InitialAcc :: A, Array :: array()) -> B when
      Function :: fun ((Index :: array_indx(), Value :: _ , Acc :: A) -> B).
%% 对array数组从左边进行foldl操作，但是要忽略掉默认元素
sparse_foldl(Function, A , #array{size = N , elements = E, default = D })
  when is_function( Function , 3 ) ->
       if N > 0 ->
               sparse_foldl_1( N -1 , E, A, 0, Function, D );
         true ->
               A
       end ;

sparse_foldl(_, _, _) ->
      erlang:error(badarg).

%% see foldl/3 for details
%% TODO: this can be optimized
%% 对array数组的单个节点进行foldl操作，忽略掉默认元素
sparse_foldl_1(N, E = ?NODEPATTERN( S ), A , Ix, F, D) ->
      sparse_foldl_2( 1 , E , A, Ix, F, D, N div S + 1 , N rem S , S );

%% 如果该节点尚未展开，则直接返回该节点数字
sparse_foldl_1(_N, E, A, _Ix, _F, _D) when is_integer( E ) ->
       A ;

%% 对10个元素的tuple节点进行foldl操作，实际就是最底层的节点
sparse_foldl_1(N, E, A, Ix, F, D) ->
      sparse_foldl_3( 1 , E , A, Ix, F, D, N + 1).


%% 对array数组某个节点下的所有子树进行foldl操作
sparse_foldl_2(I, E, A, Ix, F, D, I, R, _S) ->
      sparse_foldl_1( R , element(I , E), A, Ix, F, D);

sparse_foldl_2(I, E, A, Ix, F, D, N, R, S) ->
      sparse_foldl_2( I + 1 , E, sparse_foldl_1( S - 1 , element(I, E), A, Ix , F , D),
                           Ix + S , F, D, N, R, S).


%% 实际的对10个元素的tuple进行foldl操作的接口
sparse_foldl_3(I, T, A, Ix, F, D, N) when I =< N ->
       case element(I , T) of
             D -> sparse_foldl_3(I + 1, T, A, Ix + 1, F, D, N);
             E -> sparse_foldl_3(I + 1, T, F(Ix, E, A), Ix + 1, F, D, N)
       end ;

sparse_foldl_3(_I, _T, A, _Ix, _F, _D, _N) ->
       A .

%% @doc Fold the elements of the array right-to-left using the given
%% function and initial accumulator value. The elements are visited in
%% order from the highest index to the lowest. If `Function' is not a
%% function, the call fails with reason `badarg'.
%%
%% @see foldl/3
%% @see map/2

-spec foldr( Function , InitialAcc :: A, Array :: array()) -> B when
      Function :: fun ((Index :: array_indx(), Value :: _ , Acc :: A) -> B ).
%% 对array数组从右边进行foldr操作
foldr(Function, A , #array{size = N , elements = E, default = D })
  when is_function( Function , 3 ) ->
       if N > 0 ->
               I = N - 1,
               foldr_1( I , E , I, A, Function, D );
         true ->
               A
       end ;

foldr(_, _, _) ->
      erlang:error(badarg).

%% this is based on to_orddict/1
%% 对array数组的单个节点进行foldr操作
foldr_1(I, E = ?NODEPATTERN( S ), Ix , A, F, D) ->
      foldr_2( I div S + 1, E, Ix, A, F, D, I rem S , S - 1);

%% 该节点的元素还未展开，则展开后进行foldr操作
foldr_1(I, E, Ix, A, F, D) when is_integer( E ) ->
      foldr_1( I , unfold(E , D), Ix, A, F, D );

%% 对10个元素的tuple进行foldr操作
foldr_1(I, E, Ix, A, F, _D) ->
       I1 = I + 1,
      foldr_3( I1 , E , Ix - I1, A, F).


%% 对某个节点所有的子树进行foldr操作
foldr_2(0, _E, _Ix, A, _F, _D, _R, _R0) ->
       A ;

foldr_2(I, E, Ix, A, F, D, R, R0) ->
      foldr_2( I - 1 , E, Ix - R - 1,
                  foldr_1( R , element(I , E), Ix, A, F, D),
                   F , D , R0, R0).

-spec foldr_3(array_indx(), term(), integer(), A ,
            fun ((array_indx(), _ , A) -> B)) -> B.
%% 实际的对10个tuple进行foldr操作的接口
foldr_3(0, _E, _Ix, A, _F) ->
       A ;

foldr_3(I, E, Ix, A, F) ->
      foldr_3( I - 1 , E, Ix, F(Ix + I, element( I , E ), A), F).

%% @doc Fold the elements of the array right-to-left using the given
%% function and initial accumulator value, skipping default-valued
%% entries. The elements are visited in order from the highest index to
%% the lowest. If `Function' is not a function, the call fails with
%% reason `badarg'.
%%
%% @see foldr/3
%% @see sparse_foldl/3

-spec sparse_foldr( Function , InitialAcc :: A, Array :: array()) -> B when
      Function :: fun ((Index :: array_indx(), Value :: _ , Acc :: A) -> B ).
%% 对array数组的元素执行foldr操作，但是忽略掉默认值
sparse_foldr(Function, A , #array{size = N , elements = E, default = D })
  when is_function( Function , 3 ) ->
       if N > 0 ->
               I = N - 1,
               sparse_foldr_1( I , E , I, A, Function, D );
         true ->
               A
       end ;

sparse_foldr(_, _, _) ->
      erlang:error(badarg).

%% see foldr/3 for details
%% TODO: this can be optimized
%% 对array数组的单个节点进行foldr操作，忽略掉默认值
sparse_foldr_1(I, E = ?NODEPATTERN( S ), Ix , A, F, D) ->
      sparse_foldr_2( I div S + 1, E, Ix, A, F, D, I rem S , S - 1);

%% 尚未展开的节点属于默认值，因此直接忽略掉
sparse_foldr_1(_I, E, _Ix, A, _F, _D) when is_integer( E ) ->
       A ;

%% 对array数组10个元素的tuple，即最底层的节点进行foldr操作
sparse_foldr_1(I, E, Ix, A, F, D) ->
       I1 = I + 1,
      sparse_foldr_3( I1 , E , Ix - I1, A, F, D).


%% 对array数组某个节点下的所有子树进行foldr操作，需要忽略掉默认值
sparse_foldr_2(0, _E, _Ix, A, _F, _D, _R, _R0) ->
       A ;

sparse_foldr_2(I, E, Ix, A, F, D, R, R0) ->
      sparse_foldr_2( I - 1 , E, Ix - R - 1,
                           sparse_foldr_1( R , element(I , E), Ix, A, F, D ),
                           F , D , R0, R0).

-spec sparse_foldr_3(array_indx(), _ , array_indx(), A ,
                 fun ((array_indx(), _ , A) -> B), _) -> B.
%% 实际的对最底层的节点进行foldr操作，忽略掉默认值
sparse_foldr_3(0, _T, _Ix, A, _F, _D) ->
       A ;

sparse_foldr_3(I, T, Ix, A, F, D) ->
       case element(I , T) of
             D -> sparse_foldr_3(I - 1, T, Ix, A, F, D);
             E -> sparse_foldr_3(I - 1, T, Ix, F(Ix + I, E, A), F, D)
       end .


%% @doc Get the number of entries in the array up until the last
%% non-default valued entry. In other words, returns `I+1' if `I' is the
%% last non-default valued entry in the array, or zero if no such entry
%% exists.
%% @see size/1
%% @see resize/1

-spec sparse_size( Array :: array()) -> non_neg_integer().
%% 得到array数组中的实际元素个数，忽略掉默认值
sparse_size(A) ->
       F = fun (I, _V, _A) -> throw({value, I }) end ,
       try sparse_foldr(F , [], A) of
            [] -> 0
       catch
            {value, I } ->
                   I + 1
       end .

