%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2000-2011. All Rights Reserved.
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

%% We use the dynamic hashing techniques by Per-�ke Larsson as
%% described in "The Design and Implementation of Dynamic Hashing for
%% Sets and Tables in Icon" by Griswold and Townsend.  Much of the
%% terminology comes from that paper as well.
%%
%% The segments are all of the same fixed size and we just keep
%% increasing the size of the top tuple as the table grows.  At the
%% end of the segments tuple we keep an empty segment which we use
%% when we expand the segments.  The segments are expanded by doubling
%% every time n reaches maxn instead of increasing the tuple one
%% element at a time.  It is easier and does not seem detrimental to
%% speed.  The same applies when contracting the segments.
%%
%% Note that as the order of the keys is undefined we may freely
%% reorder keys within a bucket.

-module(dict_test).

%% Standard interface.
-export([new/0,is_key/ 2 ,to_list/1 ,from_list/1,size/1]).
-export([fetch/2,find/2,fetch_keys/ 1 ,erase/2 ]).
-export([store/3,append/ 3 ,append_list/3 ,update/3,update/ 4 ,update_counter/3 ]).
-export([fold/3,map/2,filter/ 2 ,merge/3 ]).

%% Low-level interface.
%%-export([get_slot/2,get_bucket/2,on_bucket/3,fold_dict/3,
%%     maybe_expand/2,maybe_contract/2]).

%% Note: mk_seg/1 must be changed too if seg_size is changed.
-define(seg_size, 16).                                                                     %% slot激活的初始化个数
-define(max_seg, 32).
-define(expand_load, 5).
-define(contract_load, 3).
-define(exp_size, (?seg_size * ?expand_load )).                                 %% 扩张阈值 初始值为16*5=80
-define(con_size, (?seg_size * ?contract_load )).                               %% 收缩阈值 初始值为16*3=48

%% Define a hashtable.  The default values are the standard ones.
-record(dict,
            {size= 0                  :: non_neg_integer(),         % Number of elements(元素的数量)
             n= ?seg_size             :: non_neg_integer(),         % Number of active slots(已经激活的的slot数量)
             maxn= ?seg_size          :: non_neg_integer(),         % Maximum slots(最大slots数)
             bso= ?seg_size div 2     :: non_neg_integer(),         % Buddy slot offset(最大bucket数散列表中当前允许的最大bucket数量，扩张操作需要据此判断是否要增加新的bucket区段，初始为8)
             exp_size= ?exp_size      :: non_neg_integer(),          % Size to expand at(扩张阈值 初始值为16*5=80，当字典中元素个数超过这个值时，字典需要扩展)
             con_size= ?con_size      :: non_neg_integer(),          % Size to contract at(收缩阈值 初始值为16*3=48，当字典中元素个数少于这个值时，字典需要压缩(减少slots的数量))
             empty                    :: tuple(),                    % Empty segment(作为扩展segs时的初始化的默认值)
             segs                     :: tuple()                     % Segments(Segments 所有的数据存放的地方，真正储存数量的地方,
                                                                     % 初始结构为{seg}，每经过一次扩展,seg的数量翻倍,seg的结构为{[],[],...},元组中列表的个数为?seg_size定义的大小, 这里的列表叫做bucket)
            }).
%% A declaration equivalent to the following one is hard-coded in erl_types.
%% That declaration contains hard-coded information about the #dict{}
%% structure and the types of its fields.  So, please make sure that any
%% changes to its structure are also propagated to erl_types.erl.
%%
%% -opaque dict() :: #dict{}.

-define(kv(K, V), [K | V]).                % Key-Value pair format
%%-define(kv(K,V), {K,V}).                % Key-Value pair format

-spec new() -> dict().
%% 创建新的dict数据结构
new() ->
       Empty = mk_seg(?seg_size ),
      #dict{empty = Empty , segs = {Empty }}.

-spec is_key( Key , Dict ) -> boolean() when
      Key :: term(),
      Dict :: dict().
%% 判断Key是否在字典D中有对应的key-value键值对
is_key(Key, D) ->
       %% 计算slot，主要是根据hash值来计算
       Slot = get_slot(D , Key),
       %% 根据槽位Slot得到对应的bucket数据
       Bkt = get_bucket(D , Slot),
       %% 判断Key是否存在Bucket中
      find_key( Key , Bkt ).


%% 判断Key是否存在Bucket中
find_key(K, [?kv(K, _Val) | _ ]) -> true;

find_key(K, [_ | Bkt]) -> find_key( K , Bkt );

find_key(_, []) -> false.

-spec to_list( Dict ) -> List when
      Dict :: dict(),
      List :: [{ Key :: term(), Value :: term()}].
%% 将字典D中的key-value键值对转化为列表
to_list(D) ->
      fold( fun (Key , Val, List) -> [{ Key , Val } | List] end, [], D ).

-spec from_list( List ) -> Dict when
      List :: [{ Key :: term(), Value :: term()}],
      Dict :: dict().
%% 通过L列表中的key-value键值对创建一个新的dict数据结构
from_list(L) ->
      lists:foldl( fun ({K , V}, D) -> store( K , V , D) end, new(), L ).

-spec size( Dict ) -> non_neg_integer() when
      Dict :: dict().

size(#dict{size=N}) when is_integer( N ), N >= 0 -> N.

-spec fetch( Key , Dict ) -> Value when
      Key :: term(),
      Dict :: dict(),
      Value :: term().
%% 通过Key查找对应的value
fetch(Key, D) ->
       %% 计算slot，主要是根据hash值来计算
       Slot = get_slot(D , Key),
       %% 根据槽位Slot得到对应的bucket数据
       Bkt = get_bucket(D , Slot),
       %% 从bucket中查找K对应的值，如果没有查找到直接报错
       try fetch_val(Key , Bkt)
       catch
            badarg -> erlang:error(badarg, [Key , D])
       end .


%% 从bucket中查找K对应的值，如果没有查找到直接报错
fetch_val(K, [?kv(K, Val) | _]) -> Val;

fetch_val(K, [_ | Bkt]) -> fetch_val( K , Bkt );

fetch_val(_, []) -> throw(badarg).

-spec find( Key , Dict ) -> {'ok', Value } | 'error' when
      Key :: term(),
      Dict :: dict(),
      Value :: term().
%% 根据Key查找对应的值
find(Key, D) ->
       %% 计算slot，主要是根据hash值来计算
       Slot = get_slot(D , Key),
       %% 根据槽位Slot得到对应的bucket数据
       Bkt = get_bucket(D , Slot),
       %% 根据K的key查找对应的值，如果没有查找到则返回error
      find_val( Key , Bkt ).


%% 根据K的key查找对应的值，如果没有查找到则返回error
find_val(K, [?kv(K, Val)|_]) -> {ok, Val };

find_val(K, [_ | Bkt]) -> find_val( K , Bkt );

find_val(_, []) -> error.

-spec fetch_keys( Dict ) -> Keys when
      Dict :: dict(),
      Keys :: [term()].
%% 遍历dict字典，拿到所有的key的列表
fetch_keys(D) ->
      fold( fun (Key , _Val, Keys) -> [Key | Keys] end, [], D ).

-spec erase( Key , Dict1 ) -> Dict2 when
      Key :: term(),
      Dict1 :: dict(),
      Dict2 :: dict().
%%  Erase all elements with key Key.
%% 删除Key对应的键值对
erase(Key, D0) ->
       %% 计算slot，主要是根据hash值来计算
       Slot = get_slot(D0 , Key),
       %% 根据对应的槽位删除对应槽位里面数据
      { D1 , Dc } = on_bucket(fun ( B0 ) -> erase_key(Key, B0) end,
                                     D0 , Slot ),
       %% 判断是否需要将字典进行收缩，如果字典中的数据数量少于了收缩的大小，则进行收缩
      maybe_contract( D1 , Dc ).


%% 删除Key对应的键值对的实际操作的函数接口
erase_key(Key, [?kv(Key, _Val) | Bkt ]) -> {Bkt, 1};

erase_key(Key, [E | Bkt0]) ->
      { Bkt1 , Dc } = erase_key(Key, Bkt0),
      {[ E | Bkt1 ], Dc};

erase_key(_, []) -> {[], 0 }.

-spec store( Key , Value , Dict1) -> Dict2 when
      Key :: term(),
      Value :: term(),
      Dict1 :: dict(),
      Dict2 :: dict().
%% dict数据结构存储数据
store(Key, Val, D0) ->
       %% 计算slot，主要是根据hash值来计算
       Slot = get_slot(D0 , Key),
       %% dict数据结构中实际的数据存储操作接口
      { D1 , Ic } = on_bucket(fun ( B0 ) -> store_bkt_val(Key, Val, B0) end,
                                     D0 , Slot ),
       %% 完成扩展字典的功能
      maybe_expand( D1 , Ic ).

%% store_bkt_val(Key, Val, Bucket) -> {NewBucket,PutCount}.
%% 将key-value键值对存储到bucket中(存储数据的实际操作)
store_bkt_val(Key, New, [?kv(Key, _Old) | Bkt ]) -> {[?kv(Key, New) | Bkt ], 0 };

store_bkt_val(Key, New, [Other | Bkt0 ]) ->
      { Bkt1 , Ic } = store_bkt_val(Key, New, Bkt0),
      {[ Other | Bkt1 ], Ic};

store_bkt_val(Key, New, []) -> {[?kv (Key, New)], 1}.

-spec append( Key , Value , Dict1) -> Dict2 when
      Key :: term(),
      Value :: term(),
      Dict1 :: dict(),
      Dict2 :: dict().
%% 给key对应的值后面继续添加Val值，如果没有找到key对应的值，相当于在当前字典中添加新的key-value键值对
append(Key, Val, D0) ->
       %% 计算slot，主要是根据hash值来计算
       Slot = get_slot(D0 , Key),
       %% dict数据结构中实际的数据存储操作接口
      { D1 , Ic } = on_bucket(fun ( B0 ) -> append_bkt(Key, Val, B0) end,
                                     D0 , Slot ),
       %% 完成扩展字典的功能
      maybe_expand( D1 , Ic ).

%% append_bkt(Key, Val, Bucket) -> {NewBucket,PutCount}.
%% 将Key对应的值后面继续添加值
append_bkt(Key, Val, [?kv(Key, Bag) | Bkt]) -> {[ ?kv (Key , Bag ++ [ Val ]) | Bkt ], 0 };

append_bkt(Key, Val, [Other | Bkt0 ]) ->
      { Bkt1 , Ic } = append_bkt(Key, Val, Bkt0),
      {[ Other | Bkt1 ], Ic};

%% 如果没有找到对应的值，则相当于字典中添加一个新的key-value对
append_bkt(Key, Val, []) -> {[?kv (Key, [Val])], 1 }.

-spec append_list( Key , ValList , Dict1) -> Dict2 when
      Key :: term(),
      ValList :: [ Value :: term()],
      Dict1 :: dict(),
      Dict2 :: dict().
%% 在Key对应的值后面添加列表
append_list(Key, L, D0) ->
       %% 计算slot，主要是根据hash值来计算
       Slot = get_slot(D0 , Key),
       %% dict数据结构中实际的数据操作接口
      { D1 ,Ic } = on_bucket(fun ( B0 ) -> app_list_bkt(Key, L, B0) end,
                                     D0 , Slot ),
       %% 完成扩展字典的功能
      maybe_expand( D1 , Ic ).

%% app_list_bkt(Key, L, Bucket) -> {NewBucket,PutCount}.
%% 在Key对应的值后面添加列表实际操作的函数接口
app_list_bkt(Key, L, [?kv(Key, Bag) | Bkt]) -> {[ ?kv (Key , Bag ++ L) | Bkt ],0 };

app_list_bkt(Key, L, [Other | Bkt0 ]) ->
      { Bkt1 , Ic } = app_list_bkt(Key, L, Bkt0),
      {[ Other | Bkt1 ], Ic};

app_list_bkt(Key, L, []) -> {[ ?kv (Key , L)], 1}.

-spec update( Key , Fun , Dict1) -> Dict2 when
      Key :: term(),
      Fun :: fun(( Value1 :: term()) -> Value2 :: term()),
      Dict1 :: dict(),
      Dict2 :: dict().
%% 更新字典中Key对应的value值(对Key对应的值执行F函数)
update(Key, F, D0) ->
       %% 计算slot，主要是根据hash值来计算
    Slot = get_slot( D0 , Key ),
       %% dict数据结构中实际的数据操作接口
    try on_bucket( fun (B0 ) -> update_bkt( Key , F , B0) end, D0 , Slot ) of
      { D1 , _Uv } -> D1
    catch
      badarg -> erlang:error(badarg, [Key , F, D0])
    end.


%% 字典实际的更新操作
update_bkt(Key, F, [?kv(Key, Val) | Bkt]) ->
       Upd = F (Val),
      {[ ?kv (Key , Upd) | Bkt], Upd};

update_bkt(Key, F, [Other | Bkt0 ]) ->
      { Bkt1 , Upd } = update_bkt(Key, F, Bkt0),
      {[ Other | Bkt1 ], Upd};

update_bkt(_Key, _F, []) ->
      throw(badarg).

-spec update( Key , Fun , Initial, Dict1 ) -> Dict2 when
      Key :: term(),
      Initial :: term(),
      Fun :: fun(( Value1 :: term()) -> Value2 :: term()),
      Dict1 :: dict(),
      Dict2 :: dict().
%% 对Key对应的value执行F函数，如果没有找到该Key对应的value，则将Init作为value值插入dict字典中
update(Key, F, Init, D0) ->
       %% 计算slot，主要是根据hash值来计算
    Slot = get_slot( D0 , Key ),
       %% dict数据结构中实际的数据操作接口
    {D1,Ic} = on_bucket( fun (B0 ) -> update_bkt( Key , F , Init, B0) end,
                   D0 , Slot ),
       %% 完成扩展字典的功能
    maybe_expand(D1, Ic).


%% 实际更新的操作函数
update_bkt(Key, F, _, [?kv(Key, Val) | Bkt]) ->
      {[ ?kv (Key , F(Val)) | Bkt ], 0 };

update_bkt(Key, F, I, [Other | Bkt0 ]) ->
      { Bkt1 , Ic } = update_bkt(Key, F, I, Bkt0),
      {[ Other | Bkt1 ], Ic};

update_bkt(Key, F, I, []) when is_function( F , 1 ) -> {[ ?kv (Key , I)], 1}.

-spec update_counter( Key , Increment , Dict1) -> Dict2 when
      Key :: term(),
      Increment :: number(),
      Dict1 :: dict(),
      Dict2 :: dict().
%% 对Key对应的value值加上Incr，如果Key对应的value值不存在，则将Incr当做value值插入dict字典中
update_counter(Key, Incr, D0) when is_number( Incr ) ->
       %% 计算slot，主要是根据hash值来计算
    Slot = get_slot( D0 , Key ),
    {D1,Ic} = on_bucket( fun (B0 ) -> counter_bkt( Key , Incr , B0) end,
                   D0 , Slot ),
       %% 完成扩展字典的功能
    maybe_expand(D1, Ic).


%% 实际更新值的函数
counter_bkt(Key, I, [?kv(Key, Val) | Bkt]) ->
      {[ ?kv (Key , Val + I) | Bkt], 0};

counter_bkt(Key, I, [Other | Bkt0 ]) ->
      { Bkt1 , Ic } = counter_bkt(Key, I, Bkt0),
      {[ Other | Bkt1 ], Ic};

counter_bkt(Key, I, []) -> {[ ?kv (Key , I)], 1}.

-spec fold( Fun , Acc0 , Dict) -> Acc1 when
      Fun :: fun(( Key , Value , AccIn) -> AccOut),
      Key :: term(),
      Value :: term(),
      Acc0 :: term(),
      Acc1 :: term(),
      AccIn :: term(),
      AccOut :: term(),
      Dict :: dict().
%%  Fold function Fun over all "bags" in Table and return Accumulator.
%% 对字典D进行foldl操作，对每个键值对执行F函数
fold(F, Acc, D) -> fold_dict( F , Acc , D).

-spec map( Fun , Dict1 ) -> Dict2 when
      Fun :: fun(( Key :: term(), Value1 :: term()) -> Value2 :: term()),
      Dict1 :: dict(),
      Dict2 :: dict().
%% 对dicti字典进行map操作，对每个key-value执行F函数
map(F, D) -> map_dict( F , D ).

-spec filter( Pred , Dict1 ) -> Dict2 when
      Pred :: fun ((Key :: term(), Value :: term()) -> boolean()),
      Dict1 :: dict(),
      Dict2 :: dict().
%% 对dict字典进行过滤操作
filter(F, D) -> filter_dict( F , D ).

-spec merge( Fun , Dict1 , Dict2) -> Dict3 when
      Fun :: fun(( Key :: term(), Value1 :: term(), Value2 :: term()) -> Value :: term()),
      Dict1 :: dict(),
      Dict2 :: dict(),
      Dict3 :: dict().
%% 合并dict字典的操作(如果D2字典中的数据数量大于D1字典，则将D1字典中的数据合并到D2中)
merge(F, D1, D2) when D1 #dict.size < D2 #dict.size ->
      fold_dict( fun (K , V1, D) ->
                                 update( K , fun (V2) -> F(K, V1, V2) end , V1 , D)
                    end , D2 , D1);

%% 合并dict字典的操作(如果D1字典中的数据数量大于D2字典，则将D2字典中的数据合并到D1中)
merge(F, D1, D2) ->
      fold_dict( fun (K , V2, D) ->
                                 update( K , fun (V1) -> F(K, V1, V2) end , V2 , D)
                    end , D1 , D2).


%% get_slot(Hashdb, Key) -> Slot.
%%  Get the slot.  First hash on the new range, if we hit a bucket
%%  which has not been split use the unsplit buddy bucket.
%% 计算slot，主要是根据hash值来计算
get_slot(T, Key) ->
       H = erlang:phash(Key , T#dict.maxn),
       if
             H > T #dict.n -> H - T#dict.bso;
            true -> H
       end .

%% get_bucket(Hashdb, Slot) -> Bucket.
%% 根据槽位Slot得到对应的bucket数据
get_bucket(T, Slot) -> get_bucket_s( T #dict.segs, Slot ).

%% on_bucket(Fun, Hashdb, Slot) -> {NewHashDb,Result}.
%%  Apply Fun to the bucket in Slot and replace the returned bucket.
%% dict数据结构中实际的数据存储操作接口
on_bucket(F, T, Slot) ->
       SegI = ((Slot - 1) div ?seg_size ) + 1 ,
       BktI = ((Slot - 1) rem ?seg_size ) + 1 ,
       Segs = T #dict.segs,
       Seg = element(SegI , Segs),
       B0 = element(BktI , Seg),
       %% 将key-value键值对存储到bucket中
      { B1 , Res } = F(B0),                         %Op on the bucket.
       %% 将最新的数据存储到segs字段中
      { T #dict{segs = setelement(SegI , Segs, setelement( BktI , Seg , B1))}, Res }.

%% fold_dict(Fun, Acc, Dictionary) -> Acc.
%% map_dict(Fun, Dictionary) -> Dictionary.
%% filter_dict(Fun, Dictionary) -> Dictionary.
%%
%%  Work functions for fold, map and filter operations.  These
%%  traverse the hash structure rebuilding as necessary.  Note we
%%  could have implemented map and filter using fold but these are
%%  faster.  We hope!
%% 对字典D进行foldl操作，对每个键值对执行F函数
fold_dict(F, Acc, D) ->
       Segs = D #dict.segs,
      fold_segs( F , Acc , Segs, tuple_size( Segs )).


%% 对segs执行foldl操作
fold_segs(F, Acc, Segs, I) when I >= 1 ->
       Seg = element(I , Segs),
      fold_segs( F , fold_seg(F , Acc, Seg, tuple_size( Seg )), Segs , I - 1);

fold_segs(F, Acc, _, 0) when is_function( F , 3 ) -> Acc.


%% 对seg执行foldl操作
fold_seg(F, Acc, Seg, I) when I >= 1 ->
      fold_seg( F , fold_bucket(F , Acc, element( I , Seg )), Seg, I - 1);

fold_seg(F, Acc, _, 0) when is_function( F , 3 ) -> Acc.


%% 对bucket执行foldl操作
fold_bucket(F, Acc, [?kv(Key, Val) | Bkt]) ->
      fold_bucket( F , F (Key, Val, Acc), Bkt);

fold_bucket(F, Acc, []) when is_function(F , 3) -> Acc.


%% 对dicti字典进行map操作，对每个key-value执行F函数
map_dict(F, D) ->
       Segs0 = tuple_to_list(D #dict.segs),
       Segs1 = map_seg_list(F , Segs0),
       D #dict{segs = list_to_tuple(Segs1 )}.


%% 对segs进行map操作
map_seg_list(F, [Seg | Segs]) ->
       Bkts0 = tuple_to_list(Seg ),
       Bkts1 = map_bkt_list(F , Bkts0),
      [list_to_tuple( Bkts1 ) | map_seg_list(F , Segs)];

map_seg_list(F, []) when is_function( F , 2 ) -> [].


%% 对seg进行map操作
map_bkt_list(F, [Bkt0 | Bkts]) ->
      [map_bucket( F , Bkt0 ) | map_bkt_list(F, Bkts)];

map_bkt_list(F, []) when is_function( F , 2 ) -> [].


%% 对bucket执行map操作
map_bucket(F, [?kv(Key, Val) | Bkt]) ->
      [ ?kv (Key , F(Key, Val)) | map_bucket( F , Bkt )];

map_bucket(F, []) when is_function( F , 2 ) -> [].


%% 对dict字典进行过滤操作
filter_dict(F, D) ->
       Segs0 = tuple_to_list(D #dict.segs),
       %% 对segs进行过滤操作
      { Segs1 , Fc } = filter_seg_list(F, Segs0, [], 0 ),
       %% 减去掉删除的key-value个数后达到了需要收缩字典的大小
      maybe_contract( D #dict{segs = list_to_tuple(Segs1 )}, Fc).


%% 对segs进行过滤操作
filter_seg_list(F, [Seg | Segs], Fss, Fc0) ->
       Bkts0 = tuple_to_list(Seg ),
       %% 对seg进行过滤操作
      { Bkts1 , Fc1 } = filter_bkt_list(F, Bkts0, [], Fc0 ),
      filter_seg_list( F , Segs , [list_to_tuple(Bkts1) | Fss ], Fc1 );

filter_seg_list(F, [], Fss, Fc) when is_function( F , 2 ) ->
      {lists:reverse( Fss , []),Fc }.


%% 对seg进行过滤操作
filter_bkt_list(F, [Bkt0 | Bkts], Fbs, Fc0) ->
       %% 对bucket进行过滤操作
      { Bkt1 , Fc1 } = filter_bucket(F, Bkt0, [], Fc0 ),
      filter_bkt_list( F , Bkts , [Bkt1 | Fbs], Fc1);

filter_bkt_list(F, [], Fbs, Fc) when is_function( F , 2 ) ->
      {lists:reverse( Fbs ),Fc }.


%% 对bucket进行过滤操作
filter_bucket(F, [?kv(Key, Val) = E | Bkt], Fb, Fc) ->
       case F (Key, Val) of
            true -> filter_bucket(F , Bkt, [E | Fb], Fc);
            false -> filter_bucket(F , Bkt, Fb, Fc + 1)
       end ;

filter_bucket(F, [], Fb, Fc) when is_function( F , 2 ) ->
      {lists:reverse( Fb ), Fc }.

%% get_bucket_s(Segments, Slot) -> Bucket.
%% put_bucket_s(Segments, Slot, Bucket) -> NewSegments.
%% 根据Slot槽位号拿到对应位置的元素
get_bucket_s(Segs, Slot) ->
       SegI = ((Slot - 1) div ?seg_size ) + 1 ,
       BktI = ((Slot - 1) rem ?seg_size ) + 1 ,
      element( BktI , element(SegI , Segs)).


%% 将Bkt列表中的key-value数据存储到Slot槽位
put_bucket_s(Segs, Slot, Bkt) ->
       SegI = ((Slot - 1) div ?seg_size ) + 1 ,
       BktI = ((Slot - 1) rem ?seg_size ) + 1 ,
       Seg = setelement(BktI , element(SegI, Segs), Bkt),
      setelement( SegI , Segs , Seg).

%% In maybe_expand(), the variable Ic only takes the values 0 or 1,
%% but type inference is not strong enough to infer this. Thus, the
%% use of explicit pattern matching and an auxiliary function.

%% 完成扩展字典的功能
maybe_expand(T, 0) -> maybe_expand_aux( T , 0 );

maybe_expand(T, 1) -> maybe_expand_aux( T , 1 ).


%% 进程扩展的验证，如果需要扩展则进行扩展
maybe_expand_aux(T0, Ic) when T0 #dict.size + Ic > T0#dict.exp_size ->
       %% 增加双倍的segments(如果当前激活的slot和最大槽位数相等)
       T = maybe_expand_segs(T0 ),                 %Do we need more segments.
       N = T #dict.n + 1,                          %Next slot to expand into
       Segs0 = T #dict.segs,
       Slot1 = N - T#dict.bso,
       B = get_bucket_s(Segs0 , Slot1),
       Slot2 = N ,
       %% 对B中的key-value进行重新哈希操作
      [ B1 | B2 ] = rehash(B, Slot1, Slot2, T#dict.maxn),
       %% 将B1列表中的key-value数据存储到Slot1槽位
       Segs1 = put_bucket_s(Segs0 , Slot1, B1),
       %% 将B2列表中的key-value数据存储到Slot2槽位
       Segs2 = put_bucket_s(Segs1 , Slot2, B2),
       %% 增加字典中的数据数量，已经激活的slot，dict扩展上限，dict缩小的下限，以及最新的Segments
       T #dict{size = T #dict.size + Ic,
               n = N ,
               exp_size = N * ?expand_load ,
               con_size = N * ?contract_load ,
               segs = Segs2 };

%% 没有达到需要扩展的大小，则直接更新当前字典的大小
maybe_expand_aux(T, Ic) -> T#dict{size= T #dict.size + Ic }.


%% 增加双倍的segments
maybe_expand_segs(T) when T #dict.n =:= T #dict.maxn ->
       T #dict{maxn = 2 * T#dict.maxn,
               bso = 2 * T #dict.bso,
               %% 同时将seg扩展两倍
               segs = expand_segs( T #dict.segs, T #dict.empty)};

maybe_expand_segs(T) -> T.


%% 减去掉删除的key-value个数后达到了需要收缩字典的大小
maybe_contract(T, Dc) when T #dict.size - Dc < T#dict.con_size,
                                       T #dict.n > ?seg_size ->
       N = T #dict.n,
       Slot1 = N - T#dict.bso,
       Segs0 = T #dict.segs,
       %% 根据Slot1槽位号拿到对应位置的元素
       B1 = get_bucket_s(Segs0 , Slot1),
       Slot2 = N ,
       %% 根据Slot2槽位号拿到对应位置的元素
       B2 = get_bucket_s(Segs0 , Slot2),
       %% 将Slot1和Slot2槽位上的数据全部存入到Slot1槽位上
       Segs1 = put_bucket_s(Segs0 , Slot1, B1 ++ B2),
       %% 将Slot2槽位上的数据清空
       Segs2 = put_bucket_s(Segs1 , Slot2, []),    %Clear the upper bucket
       %% 将当前激活的槽位号减一
       N1 = N - 1,
       %% 收缩segs，每次减少两倍的segments(更新当前字典的数据个数，当前激活的槽位数量，以及扩张上限，收缩下限，以及最新的字典数据)
      maybe_contract_segs( T #dict{size = T #dict.size - Dc,
                                             n = N1 ,
                                             exp_size = N1 * ?expand_load ,
                                             con_size = N1 * ?contract_load ,
                                             segs = Segs2 });

%% 减去掉删除的key-value个数后达不到要收缩的大小，则只更新当前字典的最新大小
maybe_contract(T, Dc) -> T#dict{size= T #dict.size - Dc }.


%% 字典实际的收缩操作接口
maybe_contract_segs(T) when T #dict.n =:= T #dict.bso ->
       T #dict{maxn = T #dict.maxn div 2 ,
               bso = T #dict.bso div 2,
               segs = contract_segs( T #dict.segs)};

maybe_contract_segs(T) -> T.

%% rehash(Bucket, Slot1, Slot2, MaxN) -> [Bucket1|Bucket2].
%%  Yes, we should return a tuple, but this is more fun.
%% 对bucket中的key-value进行重新哈希操作
rehash([?kv(Key, _Bag) = KeyBag | T ], Slot1, Slot2, MaxN) ->
      [ L1 | L2 ] = rehash(T, Slot1, Slot2, MaxN),
       case erlang:phash(Key , MaxN) of
             Slot1 -> [[KeyBag | L1 ] | L2 ];
             Slot2 -> [L1 | [KeyBag | L2 ]]
       end ;

rehash([], _Slot1, _Slot2 , _MaxN ) -> [[] | []].

%% mk_seg(Size) -> Segment.

mk_seg(16) -> {[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []}.

%% expand_segs(Segs, EmptySeg) -> NewSegs.
%% contract_segs(Segs) -> NewSegs.
%%  Expand/contract the segment tuple by doubling/halving the number
%%  of segments.  We special case the powers of 2 upto 32, this should
%%  catch most case.  N.B. the last element in the segments tuple is
%%  an extra element containing a default empty segment.
%% 扩展segs，每次扩展两倍的segments
expand_segs({B1}, Empty) ->
      { B1 , Empty };

expand_segs({B1, B2}, Empty) ->
      { B1 , B2 , Empty, Empty};

expand_segs({B1, B2, B3, B4}, Empty) ->
      { B1 , B2 , B3, B4, Empty, Empty, Empty, Empty};

expand_segs({B1, B2, B3, B4, B5, B6, B7, B8}, Empty) ->
      { B1 , B2 , B3, B4, B5, B6, B7, B8,
       Empty , Empty , Empty, Empty, Empty, Empty, Empty, Empty};

expand_segs({B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13, B14 , B15 , B16}, Empty) ->
      { B1 , B2 , B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13, B14, B15, B16 ,
       Empty , Empty , Empty, Empty, Empty, Empty, Empty, Empty,
       Empty , Empty , Empty, Empty, Empty, Empty, Empty, Empty};

expand_segs(Segs, Empty) ->
      list_to_tuple(tuple_to_list( Segs )
                                ++ lists:duplicate(tuple_size( Segs ), Empty )).


%% 收缩segs，每次减少两倍的segments
contract_segs({B1, _}) ->
      { B1 };

contract_segs({B1, B2, _, _}) ->
      { B1 , B2 };

contract_segs({B1, B2, B3, B4, _, _, _, _}) ->
      { B1 , B2 , B3, B4};

contract_segs({B1, B2, B3, B4, B5, B6, B7, B8, _, _, _, _, _, _, _, _}) ->
      { B1 , B2 , B3, B4, B5, B6, B7 , B8 };

contract_segs({B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13, B14, B15, B16,
                     _ , _ , _, _, _, _, _, _, _, _, _, _, _, _ , _ , _}) ->
      { B1 , B2 , B3, B4, B5, B6, B7 , B8 , B9, B10, B11, B12, B13, B14, B15, B16};

contract_segs(Segs) ->
       Ss = tuple_size(Segs ) div 2 ,
      list_to_tuple(lists:sublist(tuple_to_list( Segs ), 1 , Ss)).

