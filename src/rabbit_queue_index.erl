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

-module(rabbit_queue_index).

-export([erase/1, init/3, recover/6,
         terminate/2, delete_and_terminate/1,
         publish/6, deliver/2, ack/2, sync/1, needs_sync/1, flush/1,
         read/3, next_segment_boundary/1, bounds/1, start/1, stop/0]).

-export([add_queue_ttl/0, avoid_zeroes/0, store_msg_size/0, store_msg/0]).

-define(CLEAN_FILENAME, "clean.dot").

%%----------------------------------------------------------------------------

%% The queue index is responsible(有责任) for recording the order of messages
%% within a queue on disk. As such(因此) it contains records of messages
%% being published, delivered and acknowledged. The publish record
%% includes the sequence ID, message ID and a small quantity of
%% metadata about the message; the delivery and acknowledgement
%% records just contain the sequence ID. A publish record may also
%% contain the complete message if provided to publish/5; this allows
%% the message store to be avoided altogether for small messages. In
%% either case the publish record is stored in memory in the same
%% serialised format it will take on disk.
%%
%% Because of the fact that the queue can decide at any point to send
%% a queue entry to disk, you can not rely on publishes appearing in
%% order. The only thing you can rely on is a message being published,
%% then delivered, then ack'd.
%%
%% In order to be able to clean up ack'd messages, we write to segment
%% files. These files have a fixed number of entries: ?SEGMENT_ENTRY_COUNT
%% publishes, delivers and acknowledgements. They are numbered, and so
%% it is known that the 0th segment contains messages 0 ->
%% ?SEGMENT_ENTRY_COUNT - 1, the 1st segment contains messages
%% ?SEGMENT_ENTRY_COUNT -> 2*?SEGMENT_ENTRY_COUNT - 1 and so on. As
%% such, in the segment files, we only refer to message sequence ids
%% by the LSBs as SeqId rem ?SEGMENT_ENTRY_COUNT. This gives them a
%% fixed size.
%%
%% However, transient(短暂的) messages which are not sent to disk at any point
%% will cause gaps to appear in segment files. Therefore, we delete a
%% segment file whenever the number of publishes == number of acks
%% (note that although it is not fully enforced, it is assumed that a
%% message will never be ackd before it is delivered(交付), thus this test
%% also implies == number of delivers). In practise, this does not
%% cause disk churn in the pathological case because of the journal
%% and caching (see below).
%%
%% Because of the fact that publishes, delivers and acks can occur all
%% over, we wish to avoid lots of seeking. Therefore we have a fixed
%% sized journal to which all actions are appended. When the number of
%% entries in this journal reaches max_journal_entries, the journal
%% entries are scattered out to their relevant files, and the journal
%% is truncated to zero size. Note that entries in the journal must
%% carry the full sequence id, thus the format of entries in the
%% journal is different to that in the segments.
%%
%% The journal(日志) is also kept fully in memory, pre-segmented: the state
%% contains a mapping from segment numbers to state-per-segment (this
%% state is held for all segments which have been "seen": thus a
%% segment which has been read but has no pending entries in the
%% journal is still held in this mapping. Also note that a dict is
%% used for this mapping, not an array because with an array, you will
%% always have entries from 0). Actions are stored directly in this
%% state. Thus at the point of flushing the journal, firstly no
%% reading from disk is necessary, but secondly if the known number of
%% acks and publishes in a segment are equal, given the known state of
%% the segment file combined with the journal, no writing needs to be
%% done to the segment file either (in fact it is deleted if it exists
%% at all). This is safe given that the set of acks is a subset of the
%% set of publishes. When it is necessary to sync messages, it is
%% sufficient to fsync on the journal: when entries are distributed
%% from the journal to segment files, those segments appended to are
%% fsync'd prior to the journal being truncated.
%%
%% This module is also responsible for scanning the queue index files
%% and seeding the message store on start up.
%%
%% Note that in general, the representation of a message's state as
%% the tuple: {('no_pub'|{IsPersistent, Bin, MsgBin}),
%% ('del'|'no_del'), ('ack'|'no_ack')} is richer than strictly
%% necessary for most operations. However, for startup, and to ensure
%% the safe and correct combination of journal entries with entries
%% read from the segment on disk, this richer representation vastly
%% simplifies and clarifies the code.
%%
%% For notes on Clean Shutdown and startup, see documentation in
%% variable_queue.
%%
%%----------------------------------------------------------------------------

%% ---- Journal details ----

-define(JOURNAL_FILENAME, "journal.jif").

-define(PUB_PERSIST_JPREFIX, 2#00).									%% 消息发布到索引模块中，该标志表示消息为持久化消息
-define(PUB_TRANS_JPREFIX,   2#01).									%% 消息发布到索引模块中，该标志表示消息为非持久化消息
-define(DEL_JPREFIX,         2#10).									%% 表示消息已经被发送给消费者，但是还没有得到消费者ack操作
-define(ACK_JPREFIX,         2#11).									%% 表示消息已经被发送给消费者，同时消息得到消费者的ack操作
-define(JPREFIX_BITS, 2).
-define(SEQ_BYTES, 8).
-define(SEQ_BITS, ((?SEQ_BYTES * 8) - ?JPREFIX_BITS)).

%% ---- Segment details ----

-define(SEGMENT_EXTENSION, ".idx").

%% TODO: The segment size would be configurable, but deriving all the
%% other values is quite hairy and quite possibly noticably less
%% efficient, depending on how clever the compiler is when it comes to
%% binary generation/matching with constant vs variable lengths.

-define(REL_SEQ_BITS, 14).
-define(SEGMENT_ENTRY_COUNT, 16384). %% trunc(math:pow(2,?REL_SEQ_BITS))).

%% seq only is binary 01 followed by 14 bits of rel seq id
%% (range: 0 - 16383)
-define(REL_SEQ_ONLY_PREFIX, 01).
-define(REL_SEQ_ONLY_PREFIX_BITS, 2).
-define(REL_SEQ_ONLY_RECORD_BYTES, 2).

%% publish record is binary 1 followed by a bit for is_persistent,
%% then 14 bits of rel seq id, 64 bits for message expiry, 32 bits of
%% size and then 128 bits of md5sum msg id.
-define(PUB_PREFIX, 1).
-define(PUB_PREFIX_BITS, 1).

%% 消息过期时间相关
-define(EXPIRY_BYTES, 8).
-define(EXPIRY_BITS, (?EXPIRY_BYTES * 8)).										%% 消息过期时间占据的长度
-define(NO_EXPIRY, 0).

%% 消息ID占用的长度设置
-define(MSG_ID_BYTES, 16). %% md5sum is 128 bit or 16 bytes
-define(MSG_ID_BITS, (?MSG_ID_BYTES * 8)).

%% This is the size of the message body content, for stats
-define(SIZE_BYTES, 4).
-define(SIZE_BITS, (?SIZE_BYTES * 8)).

%% This is the size of the message record embedded in the queue
%% index. If 0, the message can be found in the message store.
-define(EMBEDDED_SIZE_BYTES, 4).
-define(EMBEDDED_SIZE_BITS, (?EMBEDDED_SIZE_BYTES * 8)).

%% 16 bytes for md5sum + 8 for expiry
-define(PUB_RECORD_BODY_BYTES, (?MSG_ID_BYTES + ?EXPIRY_BYTES + ?SIZE_BYTES)).			%% 消息ID的长度占用的16字节 + 消息额外信息占用的8字节 + 消息额外信息表示长度的4字节
%% + 4 for size
-define(PUB_RECORD_SIZE_BYTES, (?PUB_RECORD_BODY_BYTES + ?EMBEDDED_SIZE_BYTES)).

%% + 2 for seq, bits and prefix
-define(PUB_RECORD_PREFIX_BYTES, 2).

%% ---- misc ----

-define(PUB, {_, _, _}). %% {IsPersistent, Bin, MsgBin}

-define(READ_MODE, [binary, raw, read]).
-define(WRITE_MODE, [write | ?READ_MODE]).

%%----------------------------------------------------------------------------
%% 队列索引数据结构
-record(qistate,
		{
		 dir,										%% 当前队列存储操作项的路径
		 segments,									%% 所有磁盘文件对应的segment的信息
		 journal_handle,							%% 当前队列的日志文件句柄
		 dirty_count,								%% 当前队列中已经保存的操作项数，该操作项数是还没有写入操作项磁盘文件的数量(用该值跟max_journal_entries比较，如果大于则该写磁盘文件)
		 max_journal_entries,						%% 配置文件中的最大日志记录数，当数目操作该数字的时候需要将操作项存入磁盘文件一次
		 on_sync,									%% 消息内容没有跟索引存储在一起的消息ID confirm的操作函数
		 on_sync_msg,								%% 消息内容和索引存储在一起的消息ID confirm 的操作函数
		 unconfirmed,								%% 消息内容没有跟索引存储在一起同时还没有confirm的消息ID gb_sets结构
		 unconfirmed_msg							%% 消息内容跟索引存储在一起的同时还没有confirm的消息ID gb_sets结构
		}).

%% 单个队列索引存储磁盘文件数据结构
-record(segment, {
				  num,									%% 存储消息在队列中的索引的磁盘文件的名字
				  path,									%% 当前磁盘文件的路径
				  journal_entries,						%% 操作日志项
				  unacked								%% 该索引磁盘文件中还没有ack的消息数量
				 }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-rabbit_upgrade({add_queue_ttl,  local, []}).
-rabbit_upgrade({avoid_zeroes,   local, [add_queue_ttl]}).
-rabbit_upgrade({store_msg_size, local, [avoid_zeroes]}).
-rabbit_upgrade({store_msg,      local, [store_msg_size]}).

-ifdef(use_specs).

-type(hdl() :: ('undefined' | any())).
-type(segment() :: ('undefined' |
                    #segment { num             :: non_neg_integer(),
                               path            :: file:filename(),
                               journal_entries :: array:array(),
                               unacked         :: non_neg_integer()
                             })).
-type(seq_id() :: integer()).
-type(seg_dict() :: {dict:dict(), [segment()]}).
-type(on_sync_fun() :: fun ((gb_sets:set()) -> ok)).
-type(qistate() :: #qistate { dir                 :: file:filename(),
                              segments            :: 'undefined' | seg_dict(),
                              journal_handle      :: hdl(),
                              dirty_count         :: integer(),
                              max_journal_entries :: non_neg_integer(),
                              on_sync             :: on_sync_fun(),
                              on_sync_msg         :: on_sync_fun(),
                              unconfirmed         :: gb_sets:set(),
                              unconfirmed_msg     :: gb_sets:set()
                            }).
-type(contains_predicate() :: fun ((rabbit_types:msg_id()) -> boolean())).
-type(walker(A) :: fun ((A) -> 'finished' |
                               {rabbit_types:msg_id(), non_neg_integer(), A})).
-type(shutdown_terms() :: [term()] | 'non_clean_shutdown').

-spec(erase/1 :: (rabbit_amqqueue:name()) -> 'ok').
-spec(init/3 :: (rabbit_amqqueue:name(),
                 on_sync_fun(), on_sync_fun()) -> qistate()).
-spec(recover/6 :: (rabbit_amqqueue:name(), shutdown_terms(), boolean(),
                    contains_predicate(),
                    on_sync_fun(), on_sync_fun()) ->
                        {'undefined' | non_neg_integer(),
                         'undefined' | non_neg_integer(), qistate()}).
-spec(terminate/2 :: ([any()], qistate()) -> qistate()).
-spec(delete_and_terminate/1 :: (qistate()) -> qistate()).
-spec(publish/6 :: (rabbit_types:msg_id(), seq_id(),
                    rabbit_types:message_properties(), boolean(),
                    non_neg_integer(), qistate()) -> qistate()).
-spec(deliver/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(ack/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(sync/1 :: (qistate()) -> qistate()).
-spec(needs_sync/1 :: (qistate()) -> 'confirms' | 'other' | 'false').
-spec(flush/1 :: (qistate()) -> qistate()).
-spec(read/3 :: (seq_id(), seq_id(), qistate()) ->
                     {[{rabbit_types:msg_id(), seq_id(),
                        rabbit_types:message_properties(),
                        boolean(), boolean()}], qistate()}).
-spec(next_segment_boundary/1 :: (seq_id()) -> seq_id()).
-spec(bounds/1 :: (qistate()) ->
                       {non_neg_integer(), non_neg_integer(), qistate()}).
-spec(start/1 :: ([rabbit_amqqueue:name()]) -> {[[any()]], {walker(A), A}}).

-spec(add_queue_ttl/0 :: () -> 'ok').

-endif.


%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------
%% 删除消息队列名字为Name对应的目录下面所有的消息索引磁盘文件
erase(Name) ->
	#qistate { dir = Dir } = blank_state(Name),
	case rabbit_file:is_dir(Dir) of
		true  -> rabbit_file:recursive_delete([Dir]);
		false -> ok
	end.


%% 新的消息队列的队列索引初始化
init(Name, OnSyncFun, OnSyncMsgFun) ->
	State = #qistate { dir = Dir } = blank_state(Name),
	false = rabbit_file:is_file(Dir), %% is_file == is file or dir
	State#qistate{on_sync     = OnSyncFun,
				  on_sync_msg = OnSyncMsgFun}.


%% 老的消息队列启动后，如果有需要恢复的信息，则恢复该消息队列的消息索引信息
recover(Name, Terms, MsgStoreRecovered, ContainsCheckFun,
		OnSyncFun, OnSyncMsgFun) ->
	%% 根据消息队列的名字得到队列索引的信息初始化结构
	State = blank_state(Name),
	%% 初始化消息索引中消息进行confirm的函数
	State1 = State #qistate{on_sync     = OnSyncFun,
							on_sync_msg = OnSyncMsgFun},
	CleanShutdown = Terms /= non_clean_shutdown,
	%% MsgStoreRecovered：表示持久化消息存储服务器进程是否恢复成功
	case CleanShutdown andalso MsgStoreRecovered of
		%% 此处是RabbitMQ系统正常停止，则进行正常的恢复
		true  -> RecoveredCounts = proplists:get_value(segments, Terms, []),
				 %% 先将日志文件中的操作项信息读入内存，然后根据恢复信息恢复操作项信息(主要是恢复unacked字段数据)
				 init_clean(RecoveredCounts, State1);
		%% 此操作是系统突然中断停止恢复持久化消息队列的索引信息(出现的情况是突然断电，服务器爆炸等等)
		false -> init_dirty(CleanShutdown, ContainsCheckFun, State1)
	end.


%% 队列进程中断的时候的回调函数会调用到消息索引处理模块
terminate(Terms, State = #qistate { dir = Dir }) ->
	{SegmentCounts, State1} = terminate(State),
	%% 将需要恢复的信息存储到恢复文件中(recovery.dets DETS的ETS类型)，等到RabbitMQ系统重启的时候，每个持久化的消息队列会从这个DETS中获取数据恢复队列
	rabbit_recovery_terms:store(filename:basename(Dir),
								[{segments, SegmentCounts} | Terms]),
	State1.


%% 队列进程中断的时候同时删除该队列对应的消息索引磁盘文件
delete_and_terminate(State) ->
	{_SegmentCounts, State1 = #qistate { dir = Dir }} = terminate(State),
	%% 删除该队列下所有的操作项磁盘文件和日志文件
	ok = rabbit_file:recursive_delete([Dir]),
	State1.


%% 消息的发布，需要将消息和消息在队列中的索引存储在一起
publish(MsgOrId, SeqId, MsgProps, IsPersistent, JournalSizeHint,
        State = #qistate{unconfirmed     = UC,
                         unconfirmed_msg = UCM}) ->
    MsgId = case MsgOrId of
                #basic_message{id = Id} -> Id;
                Id when is_binary(Id)   -> Id
            end,
	%% 消息的ID必须是16个字节
    ?MSG_ID_BYTES = size(MsgId),
	%% 先如果消息是需要进行confirm的消息，则更新confirm相关的字段，然后得到日志文件句柄
    {JournalHdl, State1} =
        get_journal_handle(
          case {MsgProps#message_properties.needs_confirming, MsgOrId} of
			  %% 如果消息需要confirm且存入到索引中的是消息ID，则将消息ID放入到unconfirmed字段的gb_sets结构中
              {true,  MsgId} -> UC1  = gb_sets:add_element(MsgId, UC),
                                State#qistate{unconfirmed     = UC1};
			  %% 如果消息需要confirm且存入到索引中的是消息结构，则将消息结构放入到unconfirmed_msg字段的gb_sets结构中
              {true,  _}     -> UCM1 = gb_sets:add_element(MsgId, UCM),
                                State#qistate{unconfirmed_msg = UCM1};
              {false, _}     -> State
          end),
    file_handle_cache_stats:update(queue_index_journal_write),
	%% 将消息ID或者消息体转化为二进制,为将消息存储在磁盘做准备
    {Bin, MsgBin} = create_pub_record_body(MsgOrId, MsgProps),
	%% 将操作项直接加载到日志文件的最后面
    ok = file_handle_cache:append(
           JournalHdl, [<<(case IsPersistent of
                               true  -> ?PUB_PERSIST_JPREFIX;
                               false -> ?PUB_TRANS_JPREFIX
                           end):?JPREFIX_BITS,
                          SeqId:?SEQ_BITS, Bin/binary,
                          (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin]),
	%% 先将操作项加到对应的磁盘文件信息结构中，然后判断是否需要刷新日志文件，如果需要刷新，刷新后，将磁盘文件数据结构中的操作项写入对应的磁盘文件，同时将日志文件中的日志操作项清空
    maybe_flush_journal(
      JournalSizeHint,
      add_to_journal(SeqId, {IsPersistent, Bin, MsgBin}, State1)).


%% 消息投送的接口
deliver(SeqIds, State) ->
    deliver_or_ack(del, SeqIds, State).


%% 处理确认的接口
ack(SeqIds, State) ->
    deliver_or_ack(ack, SeqIds, State).

%% This is called when there are outstanding confirms or when the
%% queue is idle and the journal needs syncing (see needs_sync/1).
%% 同步操作，将日志文件进程同步，同时通知同步的操作，将尚未confirm的消息ID或者尚未confirm的消息体通知消息队列进程该消息已经存储到队列索引的磁盘文件中
sync(State = #qistate { journal_handle = undefined }) ->
	State;

sync(State = #qistate { journal_handle = JournalHdl }) ->
	ok = file_handle_cache:sync(JournalHdl),
	%% 通知同步的操作，将尚未confirm的消息ID或者尚未confirm的消息体通知消息队列进程该消息已经存储到队列索引的磁盘文件中
	notify_sync(State).


%% 判断是否需要同步confirm
needs_sync(#qistate{journal_handle = undefined}) ->
	false;

needs_sync(#qistate{journal_handle  = JournalHdl,
					unconfirmed     = UC,
					unconfirmed_msg = UCM}) ->
	case gb_sets:is_empty(UC) andalso gb_sets:is_empty(UCM) of
		true  -> case file_handle_cache:needs_sync(JournalHdl) of
					 true  -> other;
					 false -> false
				 end;
		false -> confirms
	end.


%% 刷新日志文件，将日志文件中的操作项存入对应的操作项的磁盘文件(队列进程从休眠状态接收到一个消息后，则会调用该接口进行一次日志文件的刷新)
flush(State = #qistate { dirty_count = 0 }) -> State;
flush(State)                                -> flush_journal(State).


%% 从消息索引结构中读取出操作项
read(StartEnd, StartEnd, State) ->
	{[], State};

read(Start, End, State = #qistate { segments = Segments,
									dir = Dir }) when Start =< End ->
	%% Start is inclusive, End is exclusive.
	LowerB = {StartSeg, _StartRelSeq} = seq_id_to_seg_and_rel_seq_id(Start),
	UpperB = {EndSeg,   _EndRelSeq}   = seq_id_to_seg_and_rel_seq_id(End - 1),
	{Messages, Segments1} =
		lists:foldr(fun (Seg, Acc) ->
							 read_bounded_segment(Seg, LowerB, UpperB, Acc, Dir)
					end, {[], Segments}, lists:seq(StartSeg, EndSeg)),
	{Messages, State #qistate { segments = Segments1 }}.


%% 拿到SeqId对应的磁盘文件的下一个磁盘文件的第一个SeqId
next_segment_boundary(SeqId) ->
	{Seg, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
	reconstruct_seq_id(Seg + 1, 0).


%% 该接口只有在队列进程初始化的时候会被调用(该接口得到最小的队列索引ID和当前最大的队列索引ID)
bounds(State = #qistate { segments = Segments }) ->
	%% This is not particularly(异常) efficient, but only gets invoked(调用) on
	%% queue initialisation(初始化).
	SegNums = lists:sort(segment_nums(Segments)),
	%% Don't bother trying to figure out(弄清楚) the lowest seq_id, merely(仅仅) the
	%% seq_id of the start of the lowest segment. That seq_id may not
	%% actually exist, but that's fine. The important thing is that
	%% the segment exists and the seq_id reported is on a segment
	%% boundary.
	%%
	%% We also don't really care about the max seq_id. Just start the
	%% next segment: it makes life much easier.
	%%
	%% SegNums is sorted, ascending(上升的).
	%% SeqNums 是升序的
	%% reconstruct(重建)
	{LowSeqId, NextSeqId} =
		case SegNums of
			[]         -> {0, 0};
			[MinSeg | _] -> {reconstruct_seq_id(MinSeg, 0),
							 %% 根据最后一个文件的文件数字名字，得到最大的操作项索引最大值
						    reconstruct_seq_id(1 + lists:last(SegNums), 0)}
		end,
	{LowSeqId, NextSeqId, State}.


%% RabbitMQ系统启动恢复持久化队列
start(DurableQueueNames) ->
	%% 启动rabbit_recovery_terms进程(如果mnesia存储目录下有recovery.dets这个文件则创建rabbit_recovery_terms的dets表，将文件中的所有文件导入该dets表)
	ok = rabbit_recovery_terms:start(),
	{DurableTerms, DurableDirectories} =
		lists:foldl(
		  fun(QName, {RecoveryTerms, ValidDirectories}) ->
				  %% 队列名转换为队列目录名(该目录下面存储消息索引文件)
				  DirName = queue_name_to_dir_name(QName),
				  RecoveryInfo = case rabbit_recovery_terms:read(DirName) of
									 {error, _}  -> non_clean_shutdown;
									 {ok, Terms} -> Terms
								 end,
				  {[RecoveryInfo | RecoveryTerms],
				   sets:add_element(DirName, ValidDirectories)}
		  end, {[], sets:new()}, DurableQueueNames),
	
	%% Any queue directory we've not been asked to recover is considered garbage
	%% 得到消息索引存储目录的文件绝对路径(该路径是：mnesia数据库存储目录 ++ "/" ++ "queues")
	QueuesDir = queues_dir(),
	%% 删除持久化队列中mnesia数据库表已经没有该队列的信息，但是持久化队列的数据存储目录还存在，则将这些目录直接删除掉
	rabbit_file:recursive_delete(
	  [filename:join(QueuesDir, DirName) ||
		 DirName <- all_queue_directory_names(QueuesDir),
		 not sets:is_element(DirName, DurableDirectories)]),
	
	%% 清除rabbit_recovery_term这个dets表
	rabbit_recovery_terms:clear(),
	
	%% The backing queue interface requires that the queue recovery terms
	%% which come back from start/1 are in the same order as DurableQueueNames
	OrderedTerms = lists:reverse(DurableTerms),
	{OrderedTerms, {fun queue_index_walker/1, {start, DurableQueueNames}}}.


stop() -> rabbit_recovery_terms:stop().


%% 拿到所有持久化队列存储文件的绝对路径
all_queue_directory_names(Dir) ->
	case rabbit_file:list_dir(Dir) of
		{ok, Entries}   -> [E || E <- Entries,
								 rabbit_file:is_dir(filename:join(Dir, E))];
		{error, enoent} -> []
	end.

%%----------------------------------------------------------------------------
%% startup and shutdown
%%----------------------------------------------------------------------------
%% 得到空白的索引状态数据结构信息
blank_state(QueueName) ->
	blank_state_dir(
	  filename:join(queues_dir(), queue_name_to_dir_name(QueueName))).


%% 得到单个消息队列的索引数据结构
blank_state_dir(Dir) ->
	%% 从配置文件中拿到最大日志项
	{ok, MaxJournal} =
		application:get_env(rabbit, queue_index_max_journal_entries),
	%% 组装消息索引相关的数据结构
	#qistate { dir                 = Dir,
			   segments            = segments_new(),
			   journal_handle      = undefined,
			   dirty_count         = 0,
			   max_journal_entries = MaxJournal,
			   on_sync             = fun (_) -> ok end,
			   on_sync_msg         = fun (_) -> ok end,
			   unconfirmed         = gb_sets:new(),
			   unconfirmed_msg     = gb_sets:new() }.


%% 先将日志文件中的操作项信息读入内存，然后根据恢复信息恢复操作项信息(主要是恢复unacked字段数据)
init_clean(RecoveredCounts, State) ->
	%% Load the journal. Since this is a clean recovery this (almost)
	%% gets us back to where we were on shutdown.
	%% 从日志文件中读取出所有的操作项数据到队列索引结构中
	State1 = #qistate { dir = Dir, segments = Segments } = load_journal(State),
	%% The journal loading only creates records for segments touched
	%% by the journal, and the counts are based on the journal entries
	%% only. We need *complete* counts for *all* segments. By an
	%% amazing coincidence(巧合) we stored that information on shutdown.
	%% 将恢复信息中的数据放入到对应的磁盘文件信息中(不将索引信息从磁盘读取出来，在segment结构中保留当前磁盘文件还没有ack的消息数量)
	%% 拿到恢复信息中的所有索引磁盘文件名字对应的还没有ack的消息数量，然后根据日志中的信息恢复segment数据结构
	Segments1 =
		lists:foldl(
		  fun ({Seg, UnackedCount}, SegmentsN) ->
				   Segment = segment_find_or_new(Seg, Dir, SegmentsN),
				   segment_store(Segment #segment { unacked = UnackedCount },
												  SegmentsN)
		  end, Segments, RecoveredCounts),
	%% the counts above include transient messages, which would be the
	%% wrong thing to return
	{undefined, undefined, State1 # qistate { segments = Segments1 }}.


%% 此操作是系统突然中断停止恢复持久化消息队列的索引信息(出现的情况是突然断电，服务器爆炸等等)
init_dirty(CleanShutdown, ContainsCheckFun, State) ->
	%% 完全恢复消息队列索引日志。同时加载索引磁盘文件，并消除重复操作项
	%% Recover the journal completely. This will also load segments
	%% which have entries in the journal and remove duplicates. The
	%% counts will correctly reflect the combination(合并) of the segment
	%% and the journal.
	%% 恢复出日志文件中的操作项信息(先加载日志文件中的操作项数据，然后根据读取出来的所有segment磁盘文件信息读取对应的磁盘文件所有的操作项信息)
	State1 = #qistate { dir = Dir, segments = Segments } =
						  recover_journal(State),
	{Segments1, Count, Bytes, DirtyCount} =
		%% Load each segment in turn and filter out messages that are
		%% not in the msg_store, by adding acks to the journal. These
		%% acks only go to the RAM journal as it doesn't matter if we
		%% lose them. Also mark delivered if not clean shutdown. Also
		%% find the number of unacked messages. Also accumulate the
		%% dirty count here, so we can call maybe_flush_journal below
		%% and avoid unnecessary file system operations.
		%% all_segment_nums : 将Dir该队列目录下的所有操作项磁盘文件数字名全部罗列出来
		lists:foldl(
		  fun (Seg, {Segments2, CountAcc, BytesAcc, DirtyCount}) ->
				   {{Segment = #segment { unacked = UnackedCount }, Dirty},
					UnackedBytes} =
					   %% 恢复操作项磁盘文件
					   recover_segment(ContainsCheckFun, CleanShutdown,
									   segment_find_or_new(Seg, Dir, Segments2)),
				   {segment_store(Segment, Segments2),
					CountAcc + UnackedCount,
					BytesAcc + UnackedBytes, DirtyCount + Dirty}
		  end, {Segments, 0, 0, 0}, all_segment_nums(State1)),
	%% 判断当前文件的消息索引数量是否超过配置文件中配置的单个磁盘文件存储的上限，如果超过上限需要创建新的磁盘文件
	State2 = maybe_flush_journal(State1 #qistate { segments = Segments1,
												   dirty_count = DirtyCount }),
	{Count, Bytes, State2}.


%% 队列进程中断的处理操作函数，返回当前消息队列的索引磁盘文件对应的还未ack的消息数量
terminate(State = #qistate { journal_handle = JournalHdl,
							 segments = Segments }) ->
	%% 先关闭日志文件的文件句柄
	ok = case JournalHdl of
			 undefined -> ok;
			 _         -> file_handle_cache:close(JournalHdl)
		 end,
	SegmentCounts =
		segment_fold(
		  fun (#segment { num = Seg, unacked = UnackedCount }, Acc) ->
				   [{Seg, UnackedCount} | Acc]
		  end, [], Segments),
	{SegmentCounts, State #qistate { journal_handle = undefined,
									 segments = undefined }}.


%% 恢复操作项磁盘文件(拿到索引磁盘文件中还没有ack的数量)
recover_segment(ContainsCheckFun, CleanShutdown,
				Segment = #segment { journal_entries = JEntries }) ->
	%% 加载磁盘文件中的操作项
	{SegEntries, UnackedCount} = load_segment(false, Segment),
	%% 将日志文件中的操作项和磁盘文件中的操作项合并
	{SegEntries1, UnackedCountDelta} =
		segment_plus_journal(SegEntries, JEntries),
	array:sparse_foldl(
	  %% 只关注还没有得到ack的操作项
	  fun (RelSeq, {{IsPersistent, Bin, MsgBin}, Del, no_ack},
		   {SegmentAndDirtyCount, Bytes}) ->
			   %% 将磁盘里面的二进制转化为消息在内存中的basic_message的数据结构
			   {MsgOrId, MsgProps} = parse_pub_record_body(Bin, MsgBin),
			   %% ContainsCheckFun该函数主要用来判断该消息是否是持久化的消息
			   %% 将磁盘文件中的操作项恢复为内存中的操作项
			   {recover_message(ContainsCheckFun(MsgOrId), CleanShutdown,
								Del, RelSeq, SegmentAndDirtyCount),
				Bytes + case IsPersistent of
							true  -> MsgProps#message_properties.size;
							false -> 0
						end}
	  end,
	  %% 更新segment最新的还没有ack的操作项数量
	  {{Segment #segment { unacked = UnackedCount + UnackedCountDelta }, 0}, 0},
	  SegEntries1).


%% 该情况是该消息存在于消息存储服务器进程，同时RabbitMQ系统正常关闭后已经正常恢复
recover_message( true,  true,   _Del, _RelSeq, SegmentAndDirtyCount) ->
	SegmentAndDirtyCount;

%% 该情况是该消息存在于消息存储服务器进程，同时RabbitMQ系统正常关闭后没有正常恢复，操作为del，则不做操作
recover_message( true, false,    del, _RelSeq, SegmentAndDirtyCount) ->
	SegmentAndDirtyCount;

%% 该情况是该消息存在于消息存储服务器进程，同时RabbitMQ系统正常关闭后没有正常恢复，操作为no_del，则将该消息进行del操作
recover_message( true, false, no_del,  RelSeq, {Segment, DirtyCount}) ->
	{add_to_journal(RelSeq, del, Segment), DirtyCount + 1};

%% 如果该消息不是持久化消息，同时已经为del操作，则将该操作项加为ack，好让该操作项删除掉
recover_message(false,     _,    del,  RelSeq, {Segment, DirtyCount}) ->
	{add_to_journal(RelSeq, ack, Segment), DirtyCount + 1};

%% 如果该消息不是持久化消息，同时为no_del，则将该操作项设置为del和ack，好让该操作项删除掉
recover_message(false,     _, no_del,  RelSeq, {Segment, DirtyCount}) ->
	{add_to_journal(RelSeq, ack,
					add_to_journal(RelSeq, del, Segment)),
	 DirtyCount + 2}.


%% 队列名转换为队列目录名(该目录下面存储消息索引文件)
queue_name_to_dir_name(Name = #resource { kind = queue }) ->
	<<Num:128>> = erlang:md5(term_to_binary(Name)),
	rabbit_misc:format("~.36B", [Num]).

%% 得到消息索引存储目录的文件绝对路径(该路径是：mnesia数据库存储目录 ++ "/" ++ "queues")
queues_dir() ->
	filename:join(rabbit_mnesia:dir(), "queues").

%%----------------------------------------------------------------------------
%% msg store startup delta function
%%----------------------------------------------------------------------------

queue_index_walker({start, DurableQueues}) when is_list(DurableQueues) ->
	%% 启动一个收集进程
	{ok, Gatherer} = gatherer:start_link(),
	[begin
		 ok = gatherer:fork(Gatherer),
		 %% 交给进程池工作进程执行函数
		 ok = worker_pool:submit_async(
				fun () -> link(Gatherer),
						  ok = queue_index_walker_reader(QueueName, Gatherer),
						  unlink(Gatherer),
						  ok
				end)
	 end || QueueName <- DurableQueues],
	queue_index_walker({next, Gatherer});

queue_index_walker({next, Gatherer}) when is_pid(Gatherer) ->
	case gatherer:out(Gatherer) of
		empty ->
			unlink(Gatherer),
			ok = gatherer:stop(Gatherer),
			finished;
		{value, {MsgId, Count}} ->
			{MsgId, Count, {next, Gatherer}}
	end.


%% 根据队列名字读取该队列下的所有操作项数据
queue_index_walker_reader(QueueName, Gatherer) ->
	%% 得到QueueName队列名字对应的队列索引结构
	State = blank_state(QueueName),
	ok = scan_segments(
		   fun (_SeqId, MsgId, _MsgProps, true, _IsDelivered, no_ack, ok)
				 %% 只有消息内容存在消息存储服务器进程才会进行进行消息索引的恢复
				 when is_binary(MsgId) ->
					gatherer:sync_in(Gatherer, {MsgId, 1});
			  (_SeqId, _MsgId, _MsgProps, _IsPersistent, _IsDelivered,
			   _IsAcked, Acc) ->
				   Acc
		   end, ok, State),
	ok = gatherer:finish(Gatherer).


%% 扫描所有的操作项磁盘文件
scan_segments(Fun, Acc, State) ->
	State1 = #qistate { segments = Segments, dir = Dir } =
						  recover_journal(State),
	Result = lists:foldr(
			   fun (Seg, AccN) ->
						segment_entries_foldr(
						  fun (RelSeq, {{MsgOrId, MsgProps, IsPersistent},
										IsDelivered, IsAcked}, AccM) ->
								   Fun(reconstruct_seq_id(Seg, RelSeq), MsgOrId, MsgProps,
									   IsPersistent, IsDelivered, IsAcked, AccM)
						  end, AccN, segment_find_or_new(Seg, Dir, Segments))
			   %% 将Dir该队列目录下的所有操作项磁盘文件数字名全部罗列出来
			   end, Acc, all_segment_nums(State1)),
	{_SegmentCounts, _State} = terminate(State1),
	Result.

%%----------------------------------------------------------------------------
%% expiry/binary manipulation(操作)
%%----------------------------------------------------------------------------
%% 将消息ID或者消息体转化为二进制,为将消息存储在磁盘做准备
%% (如果有消息体的话，则将消息体存储在后面，出现这种情况是消息体的长度小于配置文件中配置的数字，该数字表示消息体大于该数字则将消息体和消息索引分开存储，否则存储在一起)
create_pub_record_body(MsgOrId, #message_properties { expiry = Expiry,
                                                      size   = Size }) ->
	%% 得到消息过期时间的二进制数据
    ExpiryBin = expiry_to_binary(Expiry),
    case MsgOrId of
        MsgId when is_binary(MsgId) ->
            {<<MsgId/binary, ExpiryBin/binary, Size:?SIZE_BITS>>, <<>>};
        #basic_message{id = MsgId} ->
            MsgBin = term_to_binary(MsgOrId),
            {<<MsgId/binary, ExpiryBin/binary, Size:?SIZE_BITS>>, MsgBin}
    end.

%% 将消息的过期时间转化为二进制
expiry_to_binary(undefined) -> <<?NO_EXPIRY:?EXPIRY_BITS>>;
expiry_to_binary(Expiry)    -> <<Expiry:?EXPIRY_BITS>>.


%% 将磁盘里面的二进制转化为消息在内存中的basic_message的数据结构
%% 如果没有存储消息体，则得到消息ID即可
parse_pub_record_body(<<MsgIdNum:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                        Size:?SIZE_BITS>>, MsgBin) ->
    %% work around for binary data fragmentation. See
    %% rabbit_msg_file:read_next/2
    <<MsgId:?MSG_ID_BYTES/binary>> = <<MsgIdNum:?MSG_ID_BITS>>,
    Props = #message_properties{expiry = case Expiry of
                                             ?NO_EXPIRY -> undefined;
                                             X          -> X
                                         end,
                                size   = Size},
    case MsgBin of
        <<>> -> {MsgId, Props};
        _    -> Msg = #basic_message{id = MsgId} = binary_to_term(MsgBin),
                {Msg, Props}
    end.

%%----------------------------------------------------------------------------
%% journal manipulation(消息索引日志操作相关)
%%----------------------------------------------------------------------------
%% 增加消息索引日志的操作
add_to_journal(SeqId, Action, State = #qistate { dirty_count = DCount,
												 segments = Segments,
												 dir = Dir }) ->
	%% 根据消息实际的队列索引得到存储改索引的文件名字(数字)以及该索引在该文件中的位置
	{Seg, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
	%% 查找该SeqId对应的磁盘文件信息，如果没有改文件信息，则创建一个新的磁盘文件信息
	Segment = segment_find_or_new(Seg, Dir, Segments),
	%% 在当前磁盘文件信息中插入消息索引信息
	Segment1 = add_to_journal(RelSeq, Action, Segment),
	%% 将最新的segment磁盘文件信息保存在索引结构里
	State #qistate { dirty_count = DCount + 1,
					 segments = segment_store(Segment1, Segments) };

add_to_journal(RelSeq, Action,
			   Segment = #segment { journal_entries = JEntries,
									unacked = UnackedCount }) ->
	%% 将操作项存入seqment结构中journal_entries的数组中，同时更新unacked结构
	Segment #segment {
					  journal_entries = add_to_journal(RelSeq, Action, JEntries),
					  unacked = UnackedCount + case Action of
												   %% 消息的发布，该segment还没有ack的数量加一
												   ?PUB -> +1;
												   %% 消息发送给消费者，不做处理
												   del  ->  0;
												   %% 消息已经得到ack，则将当前segment还没有ack的数量减一
												   ack  -> -1
											   end};

%% 在当前磁盘文件信息中插入消息索引信息
add_to_journal(RelSeq, Action, JEntries) ->
	case array:get(RelSeq, JEntries) of
		undefined ->
			%% 在磁盘文件信息结构中插入消息索引项
			array:set(RelSeq,
					  case Action of
						  %% 如果操作是插入操作项实体，则初始化操作项
						  ?PUB -> {Action, no_del, no_ack};
						  %% 如果操作项实体存在磁盘中，没有在内存中，直接做一次del操作
						  del  -> {no_pub,    del, no_ack};
						  %% 如果操作项实体存在磁盘中，没有在内存中，直接做一次ack操作
						  ack  -> {no_pub, no_del,    ack}
					  end, JEntries);
		%% 如果是消息发布给消费者的操作，则不管操作项实体是否在内存中，则记录当前消息
		({Pub,    no_del, no_ack}) when Action == del ->
			array:set(RelSeq, {Pub,    del, no_ack}, JEntries);
		%% 如果当前操作项实体不在内存中，并且已经del，且当前消息已经得到ack，则还是记录该消息ack，然后将操作项放入到数组中
		({no_pub,    del, no_ack}) when Action == ack ->
			array:set(RelSeq, {no_pub, del,    ack}, JEntries);
		%% 如果当前操作项的实体还在内存中，并且已经del，且当前消息已经得到ack，则直接将该操作项直接删除掉
		({?PUB,      del, no_ack}) when Action == ack ->
			array:reset(RelSeq, JEntries)
	end.


maybe_flush_journal(State) ->
	maybe_flush_journal(infinity, State).


%% 判断当前文件的消息索引数量是否超过配置文件中配置的单个磁盘文件存储的上限，如果超过上限需要创建新的磁盘文件
maybe_flush_journal(Hint, State = #qistate { dirty_count = DCount,
											 max_journal_entries = MaxJournal })
  when DCount > MaxJournal orelse (Hint =/= infinity andalso DCount > Hint) ->
	flush_journal(State);
maybe_flush_journal(_Hint, State) ->
	State.


%% 如果操作的次数大于配置文件中的次数，则将操作项append到对应的磁盘文件
flush_journal(State = #qistate { segments = Segments }) ->
	Segments1 =
		segment_fold(
		  fun (#segment { unacked = 0, path = Path }, SegmentsN) ->
				   %% 当该磁盘文件信息中的未确认的次数为0的时候则直接将该磁盘文件删除掉
				   case rabbit_file:is_file(Path) of
					   %% 表示当前索引磁盘中的所有消息已经全部得到ack，则直接删除磁盘文件
					   true  -> ok = rabbit_file:delete(Path);
					   false -> ok
				   end,
				   SegmentsN;
			 (#segment {} = Segment, SegmentsN) ->
				  %% 将操作项磁盘文件信息中保存的磁盘项存入磁盘文件的末尾，然后将新的操作项磁盘文件信息存储到索引结构中
				  segment_store(append_journal_to_segment(Segment), SegmentsN)
		  end, segments_new(), Segments),
	{JournalHdl, State1} =
		get_journal_handle(State #qistate { segments = Segments1 }),
	%% 将日志文件中的数据清空(因为已经将所有的操作项写入到对应的操作项磁盘文件中)
	ok = file_handle_cache:clear(JournalHdl),
	%% 更新脏的操作为0
	notify_sync(State1 #qistate { dirty_count = 0 }).


%% 将操作项磁盘文件信息中保存的磁盘项存入磁盘文件的末尾
append_journal_to_segment(#segment { journal_entries = JEntries,
									 path = Path } = Segment) ->
	case array:sparse_size(JEntries) of
		0 -> Segment;
		_ -> %% 将每次的操作项转化为磁盘文件存储的二进制
			 Seg = array:sparse_foldr(
					 fun entry_to_segment/3, [], JEntries),
			 file_handle_cache_stats:update(queue_index_write),
			 
			 %% 打开当前操作的磁盘文件句柄
			 {ok, Hdl} = file_handle_cache:open(Path, ?WRITE_MODE,
												[{write_buffer, infinity}]),
			 %% 将所有的操作项append到当前磁盘文件的最后面
			 file_handle_cache:append(Hdl, Seg),
			 %% 关闭磁盘文件句柄
			 ok = file_handle_cache:close(Hdl),
			 %% 创建新的日志操作项保存数组，将当前segment结构中的操作项数组置空
			 Segment #segment { journal_entries = array_new() }
	end.


%% 得到日志文件句柄
get_journal_handle(State = #qistate { journal_handle = undefined,
									  dir = Dir }) ->
	%% 得到日志文件的路径
	Path = filename:join(Dir, ?JOURNAL_FILENAME),
	%% 确保该路径上的目录的存在
	ok = rabbit_file:ensure_dir(Path),
	{ok, Hdl} = file_handle_cache:open(Path, ?WRITE_MODE,
									   [{write_buffer, infinity}]),
	{Hdl, State #qistate { journal_handle = Hdl }};
get_journal_handle(State = #qistate { journal_handle = Hdl }) ->
	{Hdl, State}.

%% Loading Journal. This isn't idempotent and will mess up the counts
%% if you call it more than once on the same state. Assumes the counts
%% are 0 to start with.
%% 从日志文件中读取出所有的操作项数据到队列索引结构中
load_journal(State = #qistate { dir = Dir }) ->
	Path = filename:join(Dir, ?JOURNAL_FILENAME),
	case rabbit_file:is_file(Path) of
		true  -> {JournalHdl, State1} = get_journal_handle(State),
				 Size = rabbit_file:file_size(Path),
				 {ok, 0} = file_handle_cache:position(JournalHdl, 0),
				 {ok, JournalBin} = file_handle_cache:read(JournalHdl, Size),
				 parse_journal_entries(JournalBin, State1);
		false -> State
	end.


%% ditto
%% 恢复出日志文件中的操作项信息(先加载日志文件中的操作项数据，然后根据日志文件中的操作项和磁盘文件中的所有操作项去除掉重复的操作项)
recover_journal(State) ->
	%% 加载日志文件
	State1 = #qistate { segments = Segments } = load_journal(State),
	Segments1 =
		segment_map(
		  fun (Segment = #segment { journal_entries = JEntries,
									unacked = UnackedCountInJournal }) ->
				   %% We want to keep ack'd entries in so that we can
				   %% remove them if duplicates are in the journal. The
				   %% counts here are purely(纯粹) from the segment itself.
				   %% 加载对应的磁盘文件中的所有操作项
				   {SegEntries, UnackedCountInSeg} = load_segment(true, Segment),
				   %% 如果日志文件和磁盘文件中都有改操作项，则需要去除掉重复操作项
				   {JEntries1, UnackedCountDuplicates} =
					   journal_minus_segment(JEntries, SegEntries),
				   Segment #segment { journal_entries = JEntries1,
									  %% unacked是日志文件中的unacked加上磁盘文件中的unacked，然后减去重复的操作项unacked
									  unacked = (UnackedCountInJournal +
													 UnackedCountInSeg -
													 UnackedCountDuplicates) }
		  end, Segments),
	State1 #qistate { segments = Segments1 }.


%% 解析日志文件中的消息索引信息，将它转化为entries中的内存结构
%% del的操作类型
parse_journal_entries(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>, State) ->
    parse_journal_entries(Rest, add_to_journal(SeqId, del, State));

%% ack的操作类型
parse_journal_entries(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>, State) ->
    parse_journal_entries(Rest, add_to_journal(SeqId, ack, State));

parse_journal_entries(<<0:?JPREFIX_BITS, 0:?SEQ_BITS,
                        0:?PUB_RECORD_SIZE_BYTES/unit:8, _/binary>>, State) ->
    %% Journal entry composed only of zeroes was probably
    %% produced during a dirty shutdown so stop reading
    State;

%% 解析出操作项主体
parse_journal_entries(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Bin:?PUB_RECORD_BODY_BYTES/binary,
                        MsgSize:?EMBEDDED_SIZE_BITS, MsgBin:MsgSize/binary,
                        Rest/binary>>, State) ->
    IsPersistent = case Prefix of
                       ?PUB_PERSIST_JPREFIX -> true;
                       ?PUB_TRANS_JPREFIX   -> false
                   end,
    parse_journal_entries(
      Rest, add_to_journal(SeqId, {IsPersistent, Bin, MsgBin}, State));

parse_journal_entries(_ErrOrEoF, State) ->
    State.


%% 消息转发成功或者确认成功的操作
deliver_or_ack(_Kind, [], State) ->
    State;
deliver_or_ack(Kind, SeqIds, State) ->
    JPrefix = case Kind of ack -> ?ACK_JPREFIX; del -> ?DEL_JPREFIX end,
    {JournalHdl, State1} = get_journal_handle(State),
    file_handle_cache_stats:update(queue_index_journal_write),
	%% 直接将操作项写入日志文件
    ok = file_handle_cache:append(
           JournalHdl,
           [<<JPrefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>> || SeqId <- SeqIds]),
	%% 判断是否需要将日志文件中的操作项信息写入到对应的操作项磁盘文件中
    maybe_flush_journal(lists:foldl(fun (SeqId, StateN) ->
                                            add_to_journal(SeqId, Kind, StateN)
                                    end, State1, SeqIds)).


%% 通知同步的操作，将尚未confirm的消息ID或者尚未confirm的消息体通知消息队列进程该消息已经存储到队列索引的磁盘文件中
notify_sync(State = #qistate{unconfirmed     = UC,
							 unconfirmed_msg = UCM,
							 on_sync         = OnSyncFun,
							 on_sync_msg     = OnSyncMsgFun}) ->
	State1 = case gb_sets:is_empty(UC) of
				 true  -> State;
				 false -> OnSyncFun(UC),
						  %% 创建新的gb_sets结构到unconfirmed字段中
						  State#qistate{unconfirmed = gb_sets:new()}
			 end,
	case gb_sets:is_empty(UCM) of
		true  -> State1;
		false -> OnSyncMsgFun(UCM),
				 %% 创建新的gb_sets结构到unconfirmed_msg字段中
				 State1#qistate{unconfirmed_msg = gb_sets:new()}
	end.

%%----------------------------------------------------------------------------
%% segment manipulation
%%----------------------------------------------------------------------------
%% 根据消息实际的队列索引得到存储改索引的文件名字(数字)以及该索引在该文件中的位置
seq_id_to_seg_and_rel_seq_id(SeqId) ->
	{ SeqId div ?SEGMENT_ENTRY_COUNT, SeqId rem ?SEGMENT_ENTRY_COUNT }.


%% 重建队列ID
reconstruct_seq_id(Seg, RelSeq) ->
	(Seg * ?SEGMENT_ENTRY_COUNT) + RelSeq.


%% 将Dir该队列目录下的所有操作项磁盘文件数字名全部罗列出来
all_segment_nums(#qistate { dir = Dir, segments = Segments }) ->
	lists:sort(
	  sets:to_list(
		lists:foldl(
		  fun (SegName, Set) ->
				   sets:add_element(
					 %% 将字符串转化为整形
					 list_to_integer(
					   %% 将当前操作项的磁盘文件中的文件名数字取出来
					   lists:takewhile(fun (C) -> $0 =< C andalso C =< $9 end,
									   SegName)), Set)
		  end, sets:from_list(segment_nums(Segments)),
		  rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir)))).


%% 从Segments结构中读取Seg这个磁盘文件信息，如果没有找到，则创建一个该Seg磁盘文件对应的信息结构segment
segment_find_or_new(Seg, Dir, Segments) ->
	case segment_find(Seg, Segments) of
		{ok, Segment} -> Segment;
		error         -> SegName = integer_to_list(Seg)  ++ ?SEGMENT_EXTENSION,
						 Path = filename:join(Dir, SegName),
						 %% 创建一个新的消息队列索引磁盘存储文件的信息结构
						 #segment { num             = Seg,
									path            = Path,
									journal_entries = array_new(),
									unacked         = 0 }
	end.


%% 消息索引磁盘文件信息的查找(会有两个最新使用过的磁盘文件信息没有存储到字典里，而是存储在列表中，减少对字典的操作)
segment_find(Seg, {_Segments, [Segment = #segment { num = Seg } | _]}) ->
	{ok, Segment}; %% 1 or (2, matches head)
segment_find(Seg, {_Segments, [_, Segment = #segment { num = Seg }]}) ->
	{ok, Segment}; %% 2, matches tail
segment_find(Seg, {Segments, _}) -> %% no match
	dict:find(Seg, Segments).


%% 消息索引磁盘文件信息的存储(会有两个最新使用过的磁盘文件信息没有存储到字典里，而是存储在列表中，减少对字典的操作)
%% 会将最近操作的segment结构放在列表的第一个位置，同时在第二个参数列表中缓存两个segment，这两个segment不存储在字典中
segment_store(Segment = #segment { num = Seg }, %% 1 or (2, matches head)
			  {Segments, [#segment { num = Seg } | Tail]}) ->
	{Segments, [Segment | Tail]};
segment_store(Segment = #segment { num = Seg }, %% 2, matches tail
			  {Segments, [SegmentA, #segment { num = Seg }]}) ->
	{Segments, [Segment, SegmentA]};
segment_store(Segment = #segment { num = Seg }, {Segments, []}) ->
	{dict:erase(Seg, Segments), [Segment]};
segment_store(Segment = #segment { num = Seg }, {Segments, [SegmentA]}) ->
	{dict:erase(Seg, Segments), [Segment, SegmentA]};
segment_store(Segment = #segment { num = Seg },
			  {Segments, [SegmentA, SegmentB]}) ->
	{dict:store(SegmentB#segment.num, SegmentB, dict:erase(Seg, Segments)),
	 [Segment, SegmentA]}.


%% 对所有的磁盘文件进行遍历，然后执行Fun函数
segment_fold(Fun, Acc, {Segments, CachedSegments}) ->
	dict:fold(fun (_Seg, Segment, Acc1) -> Fun(Segment, Acc1) end,
			  lists:foldl(Fun, Acc, CachedSegments), Segments).


segment_map(Fun, {Segments, CachedSegments}) ->
	{dict:map(fun (_Seg, Segment) -> Fun(Segment) end, Segments),
	 lists:map(Fun, CachedSegments)}.


%% 得到所有的存储消息在队列中的索引的存储文件的文件数字列表
segment_nums({Segments, CachedSegments}) ->
	lists:map(fun (#segment { num = Num }) -> Num end, CachedSegments) ++
		dict:fetch_keys(Segments).


%% 得到一个新的索引存储片段数据结构
segments_new() ->
	{dict:new(), []}.


%% 将每次的操作项转化为磁盘文件存储的二进制
entry_to_segment(_RelSeq, {?PUB, del, ack}, Buf) ->
    Buf;
entry_to_segment(RelSeq, {Pub, Del, Ack}, Buf) ->
    %% NB: we are assembling the segment in reverse order here, so
    %% del/ack comes first.
    Buf1 = case {Del, Ack} of
               {no_del, no_ack} ->
                   Buf;
               _ ->
                   Binary = <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                              RelSeq:?REL_SEQ_BITS>>,
                   case {Del, Ack} of
						%% 如果消息传递给消费者，也进行ack操作则向磁盘文件中存储两个同样的数据
                       {del, ack} -> [[Binary, Binary] | Buf];
                       _          -> [Binary | Buf]
                   end
           end,
    case Pub of
        no_pub ->
            Buf1;
        {IsPersistent, Bin, MsgBin} ->
			%% 组装操作项为操作项磁盘文件的二进制数据
            [[<<?PUB_PREFIX:?PUB_PREFIX_BITS,
                (bool_to_int(IsPersistent)):1,
                RelSeq:?REL_SEQ_BITS, Bin/binary,
                (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin] | Buf1]
    end.


%% 从队列索引中读取出还没有ack的操作项
read_bounded_segment(Seg, {StartSeg, StartRelSeq}, {EndSeg, EndRelSeq},
					 {Messages, Segments}, Dir) ->
	%% 找到Seg对应的segment信息结构
	Segment = segment_find_or_new(Seg, Dir, Segments),
	%% 循环单个操作项信息结构，得到该结构所有的no_ack的操作项
	{segment_entries_foldr(
	   fun (RelSeq, {{MsgOrId, MsgProps, IsPersistent}, IsDelivered, no_ack},
			Acc)
			 when (Seg > StartSeg orelse StartRelSeq =< RelSeq) andalso
					  (Seg < EndSeg   orelse EndRelSeq   >= RelSeq) ->
				[{MsgOrId, reconstruct_seq_id(StartSeg, RelSeq), MsgProps,
				  IsPersistent, IsDelivered == del} | Acc];
		  (_RelSeq, _Value, Acc) ->
			   Acc
	   end, Messages, Segment),
	 segment_store(Segment, Segments)}.


%% 先将单个索引磁盘文件中的操作项读取出来，然后对单个索引磁盘文件中的每个项执行Fun函数
%% 该函数会将Segment对应的磁盘文件中的操作项信息和内存中的日志项磁盘信息合并起来执行Fun函数
segment_entries_foldr(Fun, Init,
					  Segment = #segment { journal_entries = JEntries }) ->
	%% 从磁盘文件中加载Segment这个索引磁盘文件中的操作项
	{SegEntries, _UnackedCount} = load_segment(false, Segment),
	{SegEntries1, _UnackedCountD} = segment_plus_journal(SegEntries, JEntries),
	array:sparse_foldr(
	  fun (RelSeq, {{IsPersistent, Bin, MsgBin}, Del, Ack}, Acc) ->
			   {MsgOrId, MsgProps} = parse_pub_record_body(Bin, MsgBin),
			   Fun(RelSeq, {{MsgOrId, MsgProps, IsPersistent}, Del, Ack}, Acc)
	  end, Init, SegEntries1).

%% Loading segments
%%
%% Does not do any combining with the journal at all.
%% 加载磁盘文件中的操作项
load_segment(KeepAcked, #segment { path = Path }) ->
	Empty = {array_new(), 0},
	case rabbit_file:is_file(Path) of
		false -> Empty;
		true  -> Size = rabbit_file:file_size(Path),
				 file_handle_cache_stats:update(queue_index_read),
				 %% 打开Path对应的操作项磁盘文件句柄
				 {ok, Hdl} = file_handle_cache:open(Path, ?READ_MODE, []),
				 %% 将文件句柄的偏移已到第一个位置
				 {ok, 0} = file_handle_cache:position(Hdl, bof),
				 %% 从Hdl句柄读取Size个字节的信息
				 {ok, SegBin} = file_handle_cache:read(Hdl, Size),
				 %% 关闭磁盘文件句柄
				 ok = file_handle_cache:close(Hdl),
				 %% 解析从操作项磁盘文件中读取的二进制文件，将二进制文件转化为内存中的数据结构
				 Res = parse_segment_entries(SegBin, KeepAcked, Empty),
				 Res
	end.


%% 解析从操作项磁盘文件中读取的二进制文件，将二进制文件转化为内存中的数据结构
%% 操作项实体的解析
parse_segment_entries(<<?PUB_PREFIX:?PUB_PREFIX_BITS,
						IsPersistNum:1, RelSeq:?REL_SEQ_BITS, Rest/binary>>,
						KeepAcked, Acc) ->
	parse_segment_publish_entry(
							Rest, 1 == IsPersistNum, RelSeq, KeepAcked, Acc);

%% del和ack操作的解析
parse_segment_entries(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
						RelSeq:?REL_SEQ_BITS, Rest/binary>>, KeepAcked, Acc) ->
	parse_segment_entries(
						Rest, KeepAcked, add_segment_relseq_entry(KeepAcked, RelSeq, Acc));

parse_segment_entries(<<>>, _KeepAcked, Acc) ->
	Acc.


%% 从二进制流中解析出操作项数据
parse_segment_publish_entry(<<Bin:?PUB_RECORD_BODY_BYTES/binary,
                              MsgSize:?EMBEDDED_SIZE_BITS,
                              MsgBin:MsgSize/binary, Rest/binary>>,
                            IsPersistent, RelSeq, KeepAcked,
                            {SegEntries, Unacked}) ->
	%% 组装好内存中的操作项信息
    Obj = {{IsPersistent, Bin, MsgBin}, no_del, no_ack},
    SegEntries1 = array:set(RelSeq, Obj, SegEntries),
    parse_segment_entries(Rest, KeepAcked, {SegEntries1, Unacked + 1});

parse_segment_publish_entry(Rest, _IsPersistent, _RelSeq, KeepAcked, Acc) ->
    parse_segment_entries(Rest, KeepAcked, Acc).


%% 组装删除和确认的操作项(该信息都是放在操作项实际信息的后面)
add_segment_relseq_entry(KeepAcked, RelSeq, {SegEntries, Unacked}) ->
	case array:get(RelSeq, SegEntries) of
		{Pub, no_del, no_ack} ->
			{array:set(RelSeq, {Pub, del, no_ack}, SegEntries), Unacked};
		{Pub, del, no_ack} when KeepAcked ->
			{array:set(RelSeq, {Pub, del, ack},    SegEntries), Unacked - 1};
		{_Pub, del, no_ack} ->
			{array:reset(RelSeq,                   SegEntries), Unacked - 1}
	end.


%% 创建一个新的数组(fixed参数：避免插入时自动成长)
array_new() ->
	array:new([{default, undefined}, fixed, {size, ?SEGMENT_ENTRY_COUNT}]).


%% 将bool值转化为整形值
bool_to_int(true ) -> 1;
bool_to_int(false) -> 0.

%%----------------------------------------------------------------------------
%% journal & segment combination(日志文件中的操作项和segment磁盘文件中的操作项合并)
%%----------------------------------------------------------------------------

%% Combine what we have just read from a segment file with what we're
%% holding for that segment in memory. There must be no duplicates.
%% 根据磁盘文件中的SegEntries操作项和日志文件中的JEntries操作项，将两者相加(主要以日志中的数据为准)
segment_plus_journal(SegEntries, JEntries) ->
	array:sparse_foldl(
	  fun (RelSeq, JObj, {SegEntriesOut, AdditionalUnacked}) ->
			   SegEntry = array:get(RelSeq, SegEntriesOut),
			   {Obj, AdditionalUnackedDelta} =
				   segment_plus_journal1(SegEntry, JObj),
			   {case Obj of
					undefined -> array:reset(RelSeq, SegEntriesOut);
					_         -> array:set(RelSeq, Obj, SegEntriesOut)
				end,
				AdditionalUnacked + AdditionalUnackedDelta}
	  end, {SegEntries, 0}, JEntries).

%% Here, the result is a tuple with the first element containing the
%% item which we may be adding to (for items only in the journal),
%% modifying in (bits in both), or, when returning 'undefined',
%% erasing from (ack in journal, not segment) the segment array. The
%% other element of the tuple is the delta for AdditionalUnacked.
%% segment_plus_journal1函数第一个参数为索引磁盘文件中的操作项，第二个参数为日志中的操作项
%% 日志文件中有该操作项，但是该操作项为no_del和no_ack
segment_plus_journal1(undefined, {?PUB, no_del, no_ack} = Obj) ->
	{Obj, 1};
%% 日志文件中有该操作项，但是该操作项为del和no_ack
segment_plus_journal1(undefined, {?PUB, del, no_ack} = Obj) ->
	{Obj, 1};
%% 日志文件中有该操作项，但是该操作项为del和ack
segment_plus_journal1(undefined, {?PUB, del, ack}) ->
	{undefined, 0};

%% 日志文件中没有该操作项，但是磁盘文件中有该操作项，但是主要以日志文件中的信息为主
segment_plus_journal1({?PUB = Pub, no_del, no_ack}, {no_pub, del, no_ack}) ->
	{{Pub, del, no_ack}, 0};
segment_plus_journal1({?PUB, no_del, no_ack},       {no_pub, del, ack}) ->
	{undefined, -1};
segment_plus_journal1({?PUB, del, no_ack},          {no_pub, no_del, ack}) ->
	{undefined, -1}.

%% Remove from the journal entries for a segment, items that are
%% duplicates of entries found in the segment itself. Used on start up
%% to clean up the journal.
%% JEntries为日志中的操作项数组，SegEntries为操作项磁盘文件中的操作项数组
journal_minus_segment(JEntries, SegEntries) ->
	array:sparse_foldl(
	  fun (RelSeq, JObj, {JEntriesOut, UnackedRemoved}) ->
			   SegEntry = array:get(RelSeq, SegEntries),
			   %% 根据日志中的操作项和索引磁盘文件中的操作项信息回复日志中的操作项
			   {Obj, UnackedRemovedDelta} =
				   journal_minus_segment1(JObj, SegEntry),
			   {case Obj of
					keep      -> JEntriesOut;
					undefined -> array:reset(RelSeq, JEntriesOut);
					_         -> array:set(RelSeq, Obj, JEntriesOut)
				end,
				UnackedRemoved + UnackedRemovedDelta}
	  end, {JEntries, 0}, JEntries).

%% Here, the result is a tuple with the first element containing the
%% item we are adding to or modifying in the (initially fresh) journal
%% array. If the item is 'undefined' we leave the journal array
%% alone. The other element of the tuple is the deltas for
%% UnackedRemoved.

%% Both the same. Must be at least the publish
%% journal_minus_segment1函数第一个参数为日志文件中的操作项信息，第二个参数是索引磁盘文件中的操作项信息
%% 日志和索引磁盘中的两个操作项相等(操作项磁盘文件中有该操作项, 则将日志文件中的该操作项删除掉)
journal_minus_segment1({?PUB, _Del, no_ack} = Obj, Obj) ->
	{undefined, 1};
journal_minus_segment1({?PUB, _Del, ack} = Obj,    Obj) ->
	{undefined, 0};

%% Just publish in journal(在日志文件中只是publish)
%% 只有日志中将消息发布(该操作项还没有写入磁盘，则将该操作项保存)
journal_minus_segment1({?PUB, no_del, no_ack},     undefined) ->
	{keep, 0};

%% Publish and deliver in journal(在日志文件中publish和deliver)(在日志中该消息已经发布和发送给消费者)
%% 在日志文件中已经发送给消费者，但是索引磁盘文件中没有改操作项，则保留该日志
journal_minus_segment1({?PUB, del, no_ack},        undefined) ->
	{keep, 0};
%% 在日志和索引磁盘文件中都已经发布，在日志中已经将消息发布给消费者，磁盘索引中没有，则返回{no_pub, del, no_ack}
journal_minus_segment1({?PUB = Pub, del, no_ack},  {Pub, no_del, no_ack}) ->
	{{no_pub, del, no_ack}, 1};

%% Publish, deliver and ack in journal(日志文件中已经publish，deliver和ack)
%% 在日志中该消息已经被发布，发送给消费者，ack操作，如果索引磁盘中没有，则保留该操作项
journal_minus_segment1({?PUB, del, ack},           undefined) ->
	{keep, 0};
%% 在日志中该消息已经被发布，发送给消费者，ack操作，如果索引磁盘中没有发送给消费者和ack，则保留日志中的信息
journal_minus_segment1({?PUB = Pub, del, ack},     {Pub, no_del, no_ack}) ->
	{{no_pub, del, ack}, 1};
%% 在日志中该消息已经被发布，发送给消费者，ack操作，如果索引磁盘中没有ack，则保留日志中的信息
journal_minus_segment1({?PUB = Pub, del, ack},     {Pub, del, no_ack}) ->
	{{no_pub, no_del, ack}, 1};

%% Just deliver in journal(在日志文件只是deliver)
%% 在日志中只进行过发送给消费者
journal_minus_segment1({no_pub, del, no_ack},      {?PUB, no_del, no_ack}) ->
	{keep, 0};
journal_minus_segment1({no_pub, del, no_ack},      {?PUB, del, no_ack}) ->
	{undefined, 0};

%% Just ack in journal(在日志文件中只是ack)
%% 在日志中只进行过ack
journal_minus_segment1({no_pub, no_del, ack},      {?PUB, del, no_ack}) ->
	{keep, 0};
journal_minus_segment1({no_pub, no_del, ack},      {?PUB, del, ack}) ->
	{undefined, -1};

%% Deliver and ack in journal(在日志文件中deliver和ack)
%% 在日志中只发送给消费者和ack操作
journal_minus_segment1({no_pub, del, ack},         {?PUB, no_del, no_ack}) ->
	{keep, 0};
journal_minus_segment1({no_pub, del, ack},         {?PUB, del, no_ack}) ->
	{{no_pub, no_del, ack}, 0};
journal_minus_segment1({no_pub, del, ack},         {?PUB, del, ack}) ->
	{undefined, -1};

%% Missing segment. If flush_journal/1 is interrupted after deleting
%% the segment but before truncating the journal we can get these
%% cases: a delivery and an acknowledgement in the journal, or just an
%% acknowledgement in the journal, but with no segment. In both cases
%% we have really forgotten the message; so ignore what's in the
%% journal.
journal_minus_segment1({no_pub, no_del, ack},      undefined) ->
	{undefined, 0};
journal_minus_segment1({no_pub, del, ack},         undefined) ->
	{undefined, 0}.

%%----------------------------------------------------------------------------
%% upgrade
%%----------------------------------------------------------------------------
add_queue_ttl() ->
    foreach_queue_index({fun add_queue_ttl_journal/1,
                         fun add_queue_ttl_segment/1}).


add_queue_ttl_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        MsgId:?MSG_ID_BYTES/binary, Rest/binary>>) ->
    {[<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, MsgId,
      expiry_to_binary(undefined)], Rest};
add_queue_ttl_journal(_) ->
    stop.


add_queue_ttl_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                        RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BYTES/binary,
                        Rest/binary>>) ->
    {[<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS>>,
      MsgId, expiry_to_binary(undefined)], Rest};
add_queue_ttl_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
add_queue_ttl_segment(_) ->
    stop.


avoid_zeroes() ->
    foreach_queue_index({none, fun avoid_zeroes_segment/1}).

avoid_zeroes_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS,  IsPersistentNum:1,
                       RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                       Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS>>, Rest};
avoid_zeroes_segment(<<0:?REL_SEQ_ONLY_PREFIX_BITS,
                       RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
avoid_zeroes_segment(_) ->
    stop.

%% At upgrade time we just define every message's size as 0 - that
%% will save us a load of faff with the message store, and means we
%% can actually use the clean recovery terms in VQ. It does mean we
%% don't count message bodies from before the migration, but we can
%% live with that.
store_msg_size() ->
	foreach_queue_index({fun store_msg_size_journal/1,
						 fun store_msg_size_segment/1}).


store_msg_size_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                         MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                         Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_journal(_) ->
    stop.


store_msg_size_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                         RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                         Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_size_segment(_) ->
    stop.


store_msg() ->
    foreach_queue_index({fun store_msg_journal/1,
                         fun store_msg_segment/1}).


store_msg_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
                    Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_journal(_) ->
    stop.


store_msg_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                    RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                    Expiry:?EXPIRY_BITS, Size:?SIZE_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                    RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_segment(_) ->
    stop.

%%----------------------------------------------------------------------------

foreach_queue_index(Funs) ->
	QueuesDir = queues_dir(),
	QueueDirNames = all_queue_directory_names(QueuesDir),
	{ok, Gatherer} = gatherer:start_link(),
	[begin
		 ok = gatherer:fork(Gatherer),
		 ok = worker_pool:submit_async(
				fun () ->
						 transform_queue(filename:join(QueuesDir, QueueDirName),
										 Gatherer, Funs)
				end)
	 end || QueueDirName <- QueueDirNames],
	empty = gatherer:out(Gatherer),
	unlink(Gatherer),
	ok = gatherer:stop(Gatherer).


transform_queue(Dir, Gatherer, {JournalFun, SegmentFun}) ->
	ok = transform_file(filename:join(Dir, ?JOURNAL_FILENAME), JournalFun),
	[ok = transform_file(filename:join(Dir, Seg), SegmentFun)
			|| Seg <- rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir)],
	ok = gatherer:finish(Gatherer).


transform_file(_Path, none) ->
	ok;
transform_file(Path, Fun) when is_function(Fun)->
	PathTmp = Path ++ ".upgrade",
	case rabbit_file:file_size(Path) of
		0    -> ok;
		Size -> {ok, PathTmpHdl} =
					file_handle_cache:open(PathTmp, ?WRITE_MODE,
										   [{write_buffer, infinity}]),
				
				{ok, PathHdl} = file_handle_cache:open(
								  Path, ?READ_MODE, [{read_buffer, Size}]),
				{ok, Content} = file_handle_cache:read(PathHdl, Size),
				ok = file_handle_cache:close(PathHdl),
				
				ok = drive_transform_fun(Fun, PathTmpHdl, Content),
				
				ok = file_handle_cache:close(PathTmpHdl),
				ok = rabbit_file:rename(PathTmp, Path)
	end.


drive_transform_fun(Fun, Hdl, Contents) ->
	case Fun(Contents) of
		stop                -> ok;
		{Output, Contents1} -> ok = file_handle_cache:append(Hdl, Output),
							   drive_transform_fun(Fun, Hdl, Contents1)
	end.
