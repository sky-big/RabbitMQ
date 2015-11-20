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

-module(rabbit_msg_store).

-behaviour(gen_server2).

-export([start_link/4, successfully_recovered_state/1,
         client_init/4, client_terminate/1, client_delete_and_terminate/1,
         client_ref/1, close_all_indicated/1,
         write/3, write_flow/3, read/2, contains/2, remove/2]).

-export([set_maximum_since_use/2, has_readers/2, combine_files/3,
         delete_file/2]). %% internal

-export([transform_dir/3, force_recovery/2]). %% upgrade

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, prioritise_call/4, prioritise_cast/3,
         prioritise_info/3, format_message_queue/2]).

%%----------------------------------------------------------------------------

-include("rabbit_msg_store.hrl").

-define(SYNC_INTERVAL,  25).   %% milliseconds
-define(CLEAN_FILENAME, "clean.dot").
-define(FILE_SUMMARY_FILENAME, "file_summary.ets").
-define(TRANSFORM_TMP, "transform_tmp").

-define(BINARY_MODE,     [raw, binary]).
-define(READ_MODE,       [read]).
-define(READ_AHEAD_MODE, [read_ahead | ?READ_MODE]).
-define(WRITE_MODE,      [write]).

-define(FILE_EXTENSION,        ".rdq").				%% 实际的磁盘文件后缀
-define(FILE_EXTENSION_TMP,    ".rdt").				%% 合并文件的时候，如果目的文件中的有效消息是不连续的，则将不是从0开始的后续有消息消息拷贝到后缀为rdt的文件里面，该文件只是个临时文件

-define(HANDLE_CACHE_BUFFER_SIZE, 1048576). %% 1MB

 %% i.e. two pairs, so GC does not go idle when busy
-define(MAXIMUM_SIMULTANEOUS_GC_FILES, 4).

%%----------------------------------------------------------------------------
%% 存储服务器进程保存状态的数据结构
-record(msstate,
		{ dir,                    		%% store directory(当前存储服务器进程存储磁盘文件的路径)
		  index_module,           		%% the module for index ops(处理消息索引的模块，消息索引的结构为msg_location)
		  index_state,            		%% where are messages?(消息索引相关的状态数据结构)
		  current_file,           		%% current file name as number(当前操作的存储文件名字，是数字，存储消息的文件名字是数字)
		  current_file_handle,    		%% current file handle since the last fsync?(当前操作的文件的文件句柄)
		  file_handle_cache,      		%% file handle cache(存储服务器进程中自己的文件句柄缓存字典)
		  sync_timer_ref,         		%% TRef for our interval timer
		  sum_valid_data,         		%% sum of valid data in all files(所有磁盘文件的所有有效数据之和)
		  sum_file_size,          		%% sum of file sizes(所有磁盘文件的所有数据之和)
		  pending_gc_completion,  		%% things to do once GC completes(等待垃圾回收的等待队列)
		  gc_pid,                 		%% pid of our GC(垃圾回收操作的进程Pid)
		  file_handles_ets,       		%% tid of the shared file handles table(磁盘文件的句柄缓存ETS名字)
		  file_summary_ets,       		%% tid of the file summary table(单个磁盘文件信息存储的ETS)
		  cur_file_cache_ets,     		%% tid of current file cache table(当前文件消息缓存ETS, 保存写入到当前文件中的消息，每个元素的结构为{MsgId, Msg, CacheRefCount})
		  flying_ets,             		%% tid of writes/removes in flight(保存每个队列中正在等待执行的操作：读消息或者删除消息，每个元素的结构为{{MsgId, CRef}, Diff})
		  dying_clients,          		%% set of dying clients(死亡的客户端队列进程标识字典)
		  clients,                		%% map of references of all registered clients(存储客户端标识和一些相关信息的字典)
		  %% to callbacks
		  successfully_recovered, 		%% boolean: did we recover state?(如果上次RabbitMQ系统崩溃，这次启动后是否恢复成功的标志)
		  file_size_limit,        		%% how big are our files allowed to get?(单个磁盘文件存储的最大上限)
		  cref_to_msg_ids         		%% client ref to synced(已同步) messages mapping
		}).

%% 存储服务器客户端进程保存的存储信息
-record(client_msstate,
		{ server,						%% 存储服务器进程的名字
		  client_ref,					%% 客户端队列进程的ref
		  file_handle_cache,			%% 客户端队列进程中文件句柄的缓存文件
		  index_state,					%% 消息索引相关的状态数据结构
		  index_module,					%% 处理消息索引的模块，消息索引的结构为msg_location
		  dir,							%% 当前存储服务器进程存储磁盘文件的路径
		  gc_pid,						%% 垃圾回收操作的进程Pid
		  file_handles_ets,				%% 磁盘文件的句柄缓存ETS名字
		  file_summary_ets,				%% 单个磁盘文	件信息存储的ETS
		  cur_file_cache_ets,			%% 当前文件消息缓存ETS
		  flying_ets					%% flying_ets表的表名
		}).

%% 说明持久化文件的信息(summary：总结)
-record(file_summary,
		{file,							%% 持久化文件名称（数字）
		 valid_total_size,				%% 文件中的有效消息大小
		 left,							%% Left代表该文件左右的文件名
		 right,							%% 该文件右边的文件（Left与Right的存在主要与文件的合并有关，前面提到文件以数字命名，并连续递增，但文件合并会引起文件名称不连续）
		 file_size,						%% 文件当前大小
		 locked,						%% 当前文件是否被锁定，当有GC在操作该文件时，文件被锁定
		 readers						%% 该文件当前的读者数量
		}).

-record(gc_state,
		{ dir,							%% 当前存储服务器进程存储磁盘文件的路径
		  index_module,					%% 处理消息索引的模块，消息索引的结构为msg_location
		  index_state,					%% 消息索引相关的状态数据结构
		  file_summary_ets,				%% 单个磁盘文	件信息存储的ETS
		  file_handles_ets,				%% 打开的文件句柄ETS
		  msg_store						%% 存储服务器进程的Pid
		}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([gc_state/0, file_num/0]).

-type(gc_state() :: #gc_state { dir              :: file:filename(),
                                index_module     :: atom(),
                                index_state      :: any(),
                                file_summary_ets :: ets:tid(),
                                file_handles_ets :: ets:tid(),
                                msg_store        :: server()
                              }).

-type(server() :: pid() | atom()).
-type(client_ref() :: binary()).
-type(file_num() :: non_neg_integer()).
-type(client_msstate() :: #client_msstate {
                      server             :: server(),
                      client_ref         :: client_ref(),
                      file_handle_cache  :: dict:dict(),
                      index_state        :: any(),
                      index_module       :: atom(),
                      dir                :: file:filename(),
                      gc_pid             :: pid(),
                      file_handles_ets   :: ets:tid(),
                      file_summary_ets   :: ets:tid(),
                      cur_file_cache_ets :: ets:tid(),
                      flying_ets         :: ets:tid()}).
-type(msg_ref_delta_gen(A) ::
        fun ((A) -> 'finished' |
                    {rabbit_types:msg_id(), non_neg_integer(), A})).
-type(maybe_msg_id_fun() ::
        'undefined' | fun ((gb_sets:set(), 'written' | 'ignored') -> any())).
-type(maybe_close_fds_fun() :: 'undefined' | fun (() -> 'ok')).
-type(deletion_thunk() :: fun (() -> boolean())).

-spec(start_link/4 ::
        (atom(), file:filename(), [binary()] | 'undefined',
         {msg_ref_delta_gen(A), A}) -> rabbit_types:ok_pid_or_error()).
-spec(successfully_recovered_state/1 :: (server()) -> boolean()).
-spec(client_init/4 :: (server(), client_ref(), maybe_msg_id_fun(),
                        maybe_close_fds_fun()) -> client_msstate()).
-spec(client_terminate/1 :: (client_msstate()) -> 'ok').
-spec(client_delete_and_terminate/1 :: (client_msstate()) -> 'ok').
-spec(client_ref/1 :: (client_msstate()) -> client_ref()).
-spec(close_all_indicated/1 ::
        (client_msstate()) -> rabbit_types:ok(client_msstate())).
-spec(write/3 :: (rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok').
-spec(write_flow/3 :: (rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok').
-spec(read/2 :: (rabbit_types:msg_id(), client_msstate()) ->
                     {rabbit_types:ok(msg()) | 'not_found', client_msstate()}).
-spec(contains/2 :: (rabbit_types:msg_id(), client_msstate()) -> boolean()).
-spec(remove/2 :: ([rabbit_types:msg_id()], client_msstate()) -> 'ok').

-spec(set_maximum_since_use/2 :: (server(), non_neg_integer()) -> 'ok').
-spec(has_readers/2 :: (non_neg_integer(), gc_state()) -> boolean()).
-spec(combine_files/3 :: (non_neg_integer(), non_neg_integer(), gc_state()) ->
                              deletion_thunk()).
-spec(delete_file/2 :: (non_neg_integer(), gc_state()) -> deletion_thunk()).
-spec(force_recovery/2 :: (file:filename(), server()) -> 'ok').
-spec(transform_dir/3 :: (file:filename(), server(),
        fun ((any()) -> (rabbit_types:ok_or_error2(msg(), any())))) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

%% We run GC whenever (garbage / sum_file_size) > ?GARBAGE_FRACTION
%% It is not recommended to set this to < 0.5
-define(GARBAGE_FRACTION,      0.5).

%% The components:
%%
%% Index: this is a mapping from MsgId to #msg_location{}:
%%        {MsgId, RefCount, File, Offset, TotalSize}
%%        By default, it's in ets, but it's also pluggable.
%% FileSummary: this is an ets table which maps File to #file_summary{}:
%%        {File, ValidTotalSize, Left, Right, FileSize, Locked, Readers}
%%
%% The basic idea is that messages are appended to the current file up
%% until that file becomes too big (> file_size_limit). At that point,
%% the file is closed and a new file is created on the _right_ of the
%% old file which is used for new messages. Files are named
%% numerically ascending, thus the file with the lowest name is the
%% eldest file.
%%
%% We need to keep track of which messages are in which files (this is
%% the Index); how much useful data is in each file and which files
%% are on the left and right of each other. This is the purpose of the
%% FileSummary ets table.
%%
%% As messages are removed from files, holes appear in these
%% files. The field ValidTotalSize contains the total amount of useful
%% data left in the file. This is needed for garbage collection.
%%
%% When we discover that a file is now empty, we delete it. When we
%% discover that it can be combined with the useful data in either its
%% left or right neighbour, and overall, across all the files, we have
%% ((the amount of garbage) / (the sum of all file sizes)) >
%% ?GARBAGE_FRACTION, we start a garbage collection run concurrently,
%% which will compact the two files together. This keeps disk
%% utilisation high and aids performance. We deliberately do this
%% lazily in order to prevent doing GC on files which are soon to be
%% emptied (and hence deleted) soon.
%%
%% Given the compaction between two files, the left file (i.e. elder
%% file) is considered the ultimate destination for the good data in
%% the right file. If necessary, the good data in the left file which
%% is fragmented throughout the file is written out to a temporary
%% file, then read back in to form a contiguous chunk of good data at
%% the start of the left file. Thus the left file is garbage collected
%% and compacted. Then the good data from the right file is copied
%% onto the end of the left file. Index and FileSummary tables are
%% updated.
%%
%% On non-clean startup, we scan the files we discover, dealing with
%% the possibilites of a crash having occured during a compaction
%% (this consists of tidyup - the compaction is deliberately designed
%% such that data is duplicated on disk rather than risking it being
%% lost), and rebuild the FileSummary ets table and Index.
%%
%% So, with this design, messages move to the left. Eventually, they
%% should end up in a contiguous block on the left and are then never
%% rewritten. But this isn't quite the case. If in a file there is one
%% message that is being ignored, for some reason, and messages in the
%% file to the right and in the current block are being read all the
%% time then it will repeatedly be the case that the good data from
%% both files can be combined and will be written out to a new
%% file. Whenever this happens, our shunned message will be rewritten.
%%
%% So, provided that we combine messages in the right order,
%% (i.e. left file, bottom to top, right file, bottom to top),
%% eventually our shunned message will end up at the bottom of the
%% left file. The compaction/combining algorithm is smart enough to
%% read in good data from the left file that is scattered throughout
%% (i.e. C and D in the below diagram), then truncate the file to just
%% above B (i.e. truncate to the limit of the good contiguous region
%% at the start of the file), then write C and D on top and then write
%% E, F and G from the right file on top. Thus contiguous blocks of
%% good data at the bottom of files are not rewritten.
%%
%% +-------+    +-------+         +-------+
%% |   X   |    |   G   |         |   G   |
%% +-------+    +-------+         +-------+
%% |   D   |    |   X   |         |   F   |
%% +-------+    +-------+         +-------+
%% |   X   |    |   X   |         |   E   |
%% +-------+    +-------+         +-------+
%% |   C   |    |   F   |   ===>  |   D   |
%% +-------+    +-------+         +-------+
%% |   X   |    |   X   |         |   C   |
%% +-------+    +-------+         +-------+
%% |   B   |    |   X   |         |   B   |
%% +-------+    +-------+         +-------+
%% |   A   |    |   E   |         |   A   |
%% +-------+    +-------+         +-------+
%%   left         right             left
%%
%% From this reasoning, we do have a bound on the number of times the
%% message is rewritten. From when it is inserted, there can be no
%% files inserted between it and the head of the queue, and the worst
%% case is that everytime it is rewritten, it moves one position lower
%% in the file (for it to stay at the same position requires that
%% there are no holes beneath it, which means truncate would be used
%% and so it would not be rewritten at all). Thus this seems to
%% suggest the limit is the number of messages ahead of it in the
%% queue, though it's likely that that's pessimistic, given the
%% requirements for compaction/combination of files.
%%
%% The other property is that we have is the bound on the lowest
%% utilisation, which should be 50% - worst case is that all files are
%% fractionally over half full and can't be combined (equivalent is
%% alternating full files and files with only one tiny message in
%% them).
%%
%% Messages are reference-counted. When a message with the same msg id
%% is written several times we only store it once, and only remove it
%% from the store when it has been removed the same number of times.
%%
%% The reference counts do not persist. Therefore the initialisation
%% function must be provided with a generator that produces ref count
%% deltas for all recovered messages. This is only used on startup
%% when the shutdown was non-clean.
%%
%% Read messages with a reference count greater than one are entered
%% into a message cache. The purpose of the cache is not especially
%% performance, though it can help there too, but prevention of memory
%% explosion. It ensures that as messages with a high reference count
%% are read from several processes they are read back as the same
%% binary object rather than multiples of identical binary
%% objects.
%%
%% Reads can be performed directly by clients without calling to the
%% server. This is safe because multiple file handles can be used to
%% read files. However, locking is used by the concurrent GC to make
%% sure that reads are not attempted from files which are in the
%% process of being garbage collected.
%%
%% When a message is removed, its reference count is decremented. Even
%% if the reference count becomes 0, its entry is not removed. This is
%% because in the event of the same message being sent to several
%% different queues, there is the possibility of one queue writing and
%% removing the message before other queues write it at all. Thus
%% accomodating 0-reference counts allows us to avoid unnecessary
%% writes here. Of course, there are complications: the file to which
%% the message has already been written could be locked pending
%% deletion or GC, which means we have to rewrite the message as the
%% original copy will now be lost.
%%
%% The server automatically defers reads, removes and contains calls
%% that occur which refer to files which are currently being
%% GC'd. Contains calls are only deferred in order to ensure they do
%% not overtake removes.
%%
%% The current file to which messages are being written has a
%% write-back cache. This is written to immediately by clients and can
%% be read from by clients too. This means that there are only ever
%% writes made to the current file, thus eliminating delays due to
%% flushing write buffers in order to be able to safely read from the
%% current file. The one exception to this is that on start up, the
%% cache is not populated with msgs found in the current file, and
%% thus in this case only, reads may have to come from the file
%% itself. The effect of this is that even if the msg_store process is
%% heavily overloaded, clients can still write and read messages with
%% very low latency and not block at all.
%%
%% Clients of the msg_store are required to register before using the
%% msg_store. This provides them with the necessary client-side state
%% to allow them to directly access the various caches and files. When
%% they terminate, they should deregister. They can do this by calling
%% either client_terminate/1 or client_delete_and_terminate/1. The
%% differences are: (a) client_terminate is synchronous. As a result,
%% if the msg_store is badly overloaded and has lots of in-flight
%% writes and removes to process, this will take some time to
%% return. However, once it does return, you can be sure that all the
%% actions you've issued to the msg_store have been processed. (b) Not
%% only is client_delete_and_terminate/1 asynchronous, but it also
%% permits writes and subsequent removes from the current
%% (terminating) client which are still in flight to be safely
%% ignored. Thus from the point of view of the msg_store itself, and
%% all from the same client:
%%
%% (T) = termination; (WN) = write of msg N; (RN) = remove of msg N
%% --> W1, W2, W1, R1, T, W3, R2, W2, R1, R2, R3, W4 -->
%%
%% The client obviously sent T after all the other messages (up to
%% W4), but because the msg_store prioritises messages, the T can be
%% promoted and thus received early.
%%
%% Thus at the point of the msg_store receiving T, we have messages 1
%% and 2 with a refcount of 1. After T, W3 will be ignored because
%% it's an unknown message, as will R3, and W4. W2, R1 and R2 won't be
%% ignored because the messages that they refer to were already known
%% to the msg_store prior to T. However, it can be a little more
%% complex: after the first R2, the refcount of msg 2 is 0. At that
%% point, if a GC occurs or file deletion, msg 2 could vanish, which
%% would then mean that the subsequent W2 and R2 are then ignored.
%%
%% The use case then for client_delete_and_terminate/1 is if the
%% client wishes to remove everything it's written to the msg_store:
%% it issues removes for all messages it's written and not removed,
%% and then calls client_delete_and_terminate/1. At that point, any
%% in-flight writes (and subsequent removes) can be ignored, but
%% removes and writes for messages the msg_store already knows about
%% will continue to be processed normally (which will normally just
%% involve modifying the reference count, which is fast). Thus we save
%% disk bandwidth for writes which are going to be immediately removed
%% again by the the terminating client.
%%
%% We use a separate set to keep track of the dying clients in order
%% to keep that set, which is inspected on every write and remove, as
%% small as possible. Inspecting the set of all clients would degrade
%% performance with many healthy clients and few, if any, dying
%% clients, which is the typical case.
%%
%% When the msg_store has a backlog (i.e. it has unprocessed messages
%% in its mailbox / gen_server priority queue), a further optimisation
%% opportunity arises: we can eliminate pairs of 'write' and 'remove'
%% from the same client for the same message. A typical occurrence of
%% these is when an empty durable queue delivers persistent messages
%% to ack'ing consumers. The queue will asynchronously ask the
%% msg_store to 'write' such messages, and when they are acknowledged
%% it will issue a 'remove'. That 'remove' may be issued before the
%% msg_store has processed the 'write'. There is then no point going
%% ahead with the processing of that 'write'.
%%
%% To detect this situation a 'flying_ets' table is shared between the
%% clients and the server. The table is keyed on the combination of
%% client (reference) and msg id, and the value represents an
%% integration of all the writes and removes currently "in flight" for
%% that message between the client and server - '+1' means all the
%% writes/removes add up to a single 'write', '-1' to a 'remove', and
%% '0' to nothing. (NB: the integration can never add up to more than
%% one 'write' or 'read' since clients must not write/remove a message
%% more than once without first removing/writing it).
%%
%% Maintaining this table poses two challenges: 1) both the clients
%% and the server access and update the table, which causes
%% concurrency issues, 2) we must ensure that entries do not stay in
%% the table forever, since that would constitute a memory leak. We
%% address the former by carefully modelling all operations as
%% sequences of atomic actions that produce valid results in all
%% possible interleavings. We address the latter by deleting table
%% entries whenever the server finds a 0-valued entry during the
%% processing of a write/remove. 0 is essentially equivalent to "no
%% entry". If, OTOH, the value is non-zero we know there is at least
%% one other 'write' or 'remove' in flight, so we get an opportunity
%% later to delete the table entry when processing these.
%%
%% There are two further complications. We need to ensure that 1)
%% eliminated writes still get confirmed, and 2) the write-back cache
%% doesn't grow unbounded. These are quite straightforward to
%% address. See the comments in the code.
%%
%% For notes on Clean Shutdown and startup, see documentation in
%% variable_queue.

%%----------------------------------------------------------------------------
%% public API(存储服务器进程提供给外部的通用接口)
%%----------------------------------------------------------------------------
%% 存储服务器进程的启动入口函数
start_link(Server, Dir, ClientRefs, StartupFunState) ->
	gen_server2:start_link({local, Server}, ?MODULE,
						   [Server, Dir, ClientRefs, StartupFunState],
						   [{timeout, infinity}]).


%% 请求存储服务器进程是否成功恢复过消息信息(就是RabbitMQ系统崩溃的时候，再次重启，存储服务器进程是否恢复成功)
successfully_recovered_state(Server) ->
	gen_server2:call(Server, successfully_recovered_state, infinity).


%% 消息存储客户端的初始化(队列进程在此处得到客户端信息的数据结构)
client_init(Server, Ref, MsgOnDiskFun, CloseFDsFun) ->
	{IState, IModule, Dir, GCPid,
	 FileHandlesEts, FileSummaryEts, CurFileCacheEts, FlyingEts} =
		gen_server2:call(
		  Server, {new_client_state, Ref, self(), MsgOnDiskFun, CloseFDsFun},
		  infinity),
	%% 组装客户端存储相关的数据结构信息
	#client_msstate { server             = Server,
					  client_ref         = Ref,
					  file_handle_cache  = dict:new(),
					  index_state        = IState,
					  index_module       = IModule,
					  dir                = Dir,
					  gc_pid             = GCPid,
					  file_handles_ets   = FileHandlesEts,
					  file_summary_ets   = FileSummaryEts,
					  cur_file_cache_ets = CurFileCacheEts,
					  flying_ets         = FlyingEts }.


%% 队列进程中断的回调接口
client_terminate(CState = #client_msstate { client_ref = Ref }) ->
	%% 客户端进程关掉所有的文件句柄
	close_all_handles(CState),
	ok = server_call(CState, {client_terminate, Ref}).


%% 队列进程删除并中断的回调接口
client_delete_and_terminate(CState = #client_msstate { client_ref = Ref }) ->
	%% 客户端进程关掉所有的文件句柄
    close_all_handles(CState),
    ok = server_cast(CState, {client_dying, Ref}),
    ok = server_cast(CState, {client_delete, Ref}).


%% 拿到客户端进程的标志号
client_ref(#client_msstate { client_ref = Ref }) -> Ref.


%% 进行消息数据存储的接口(有进程间消息流控制的接口)
write_flow(MsgId, Msg, CState = #client_msstate { server = Server }) ->
	%% 首先执行消息流的操作
	credit_flow:send(whereis(Server), ?CREDIT_DISC_BOUND),
	%% 然后通过客户端数据结构写数据
	client_write(MsgId, Msg, flow, CState).


%% 进行消息数据存储的接口
write(MsgId, Msg, CState) -> client_write(MsgId, Msg, noflow, CState).


%% 读取消息的接口
read(MsgId,
	 CState = #client_msstate { cur_file_cache_ets = CurFileCacheEts }) ->
	file_handle_cache_stats:update(msg_store_read),
	%% Check the cur file cache
	%% 从当前文件缓存表中查找，如果找到则返回
	case ets:lookup(CurFileCacheEts, MsgId) of
		[] ->
			Defer = fun() -> {server_call(CState, {read, MsgId}), CState} end,
			%% 从Index中查找，如果找到一个引用数大于0的消息，则调用client_read1/3读取实际的消息
			%% 否则认为消息没找到，通过上面的Defer函数，调用服务端read操作（消息引用数为0时，消息有可能会处于GC处理状态） 
			case index_lookup_positive_ref_count(MsgId, CState) of
				%% 在消息索引中没有找到，则直接到消息存储服务器进程中读取
				not_found   -> Defer();
				MsgLocation -> client_read1(MsgLocation, Defer, CState)
			end;
		[{MsgId, Msg, _CacheRefCount}] ->
			{{ok, Msg}, CState}
	end.


%% 查看消息存储服务器进程是否有MsgId这个消息
contains(MsgId, CState) -> server_call(CState, {contains, MsgId}).


%% 消息的删除(消息队列进行消息的ack)
remove([],    _CState) -> ok;
remove(MsgIds, CState = #client_msstate { client_ref = CRef }) ->
	%% 删除操作，fly_ets表中的Diff减1
	[client_update_flying(-1, MsgId, CState) || MsgId <- MsgIds],
	%% 异步通知存储服务器进程进行消息的删除
	server_cast(CState, {remove, CRef, MsgIds}).


set_maximum_since_use(Server, Age) ->
	gen_server2:cast(Server, {set_maximum_since_use, Age}).

%%----------------------------------------------------------------------------
%% Client-side-only helpers(存储服务器进程的客户端进程的辅助函数接口)
%%----------------------------------------------------------------------------
%% 向存储服务器进程发送同步消息
server_call(#client_msstate { server = Server }, Msg) ->
	gen_server2:call(Server, Msg, infinity).


%% 向存储服务器进程发送异步消息
server_cast(#client_msstate { server = Server }, Msg) ->
	gen_server2:cast(Server, Msg).


%% 客户端写入消息的入口(所有收到该消息的消息队列都会调用此处接口)
client_write(MsgId, Msg, Flow,
			 CState = #client_msstate { cur_file_cache_ets = CurFileCacheEts,
										client_ref         = CRef }) ->
	file_handle_cache_stats:update(msg_store_write),
	%% 更新flying_ets表中对应{MsgId, CRef}的Diff，因为是写操作，所以加1
	ok = client_update_flying(+1, MsgId, CState),
	%% 更新cur_file_cache_ets表中对应MsgId的记录，CacheRefCount加1
	ok = update_msg_cache(CurFileCacheEts, MsgId, Msg),
	%% 调用服务端write操作
	ok = server_cast(CState, {write, CRef, MsgId, Flow}).


%% 根据消息的索引信息读取消息
client_read1(#msg_location { msg_id = MsgId, file = File } = MsgLocation, Defer,
			 CState = #client_msstate { file_summary_ets = FileSummaryEts }) ->
	case ets:lookup(FileSummaryEts, File) of
		[] -> %% File has been GC'd and no longer exists. Go around again.
			%% 索引磁盘文件在垃圾回收后导致不存在，则从头执行
			read(MsgId, CState);
		[#file_summary { locked = Locked, right = Right }] ->
			client_read2(Locked, Right, MsgLocation, Defer, CState)
	end.


%% 要读取得消息所在的磁盘文件没有进行垃圾回收，且右边没有文件(两层缓存中没有数据，且该消息明显在当前操作的磁盘文件中，到此我们只能到存储服务器进程中去取数据)
%% 右边没有文件表明当前要读取的消息就是在当前操作文件中，，如果缓存中没有，有可能该消息在消息存储服务器进程中还没有将消息写入磁盘，写的消息在消息存储服务器进程中还没有执行，
%% 则只能向消息存储服务器进程发送读取消息的消息来获取读取消息
client_read2(false, undefined, _MsgLocation, Defer, _CState) ->
	%% Although we've already checked both caches and not found the
	%% message there, the message is apparently(显然的) in the
	%% current_file. We can only arrive here if we are trying to read
	%% a message which we have not written, which is very odd, so just
	%% defer.
	%%
	%% OR, on startup, the cur_file_cache is not populated with the
	%% contents of the current file, thus reads from the current file
	%% will end up here and will need to be deferred.
	Defer();

%% 当前文件正在进行垃圾回收或者正在进行合并操作，已经被锁住，则直接前往存储服务器进程进行读取消息
client_read2(true, _Right, _MsgLocation, Defer, _CState) ->
	%% Of course, in the mean time, the GC could have run and our msg
	%% is actually in a different file, unlocked. However, defering is
	%% the safest and simplest thing to do.
	Defer();

%% 此处的情况是消息索引对应的磁盘文件没有被锁住，同时该索引对应的磁盘文件不是当前操作的磁盘文件
client_read2(false, _Right,
			 MsgLocation = #msg_location { msg_id = MsgId, file = File },
			 Defer,
			 CState = #client_msstate { file_summary_ets = FileSummaryEts }) ->
	%% It's entirely(完全) possible that everything we're doing from here on
	%% is for the wrong file, or a non-existent file, as a GC may have
	%% finished.
	%% 将该消息索引对应的磁盘文件信息中的读取者曾加一
	safe_ets_update_counter(
	  FileSummaryEts, File, {#file_summary.readers, +1},
	  fun (_) -> client_read3(MsgLocation, Defer, CState) end,
	  fun () -> read(MsgId, CState) end).


client_read3(#msg_location { msg_id = MsgId, file = File }, Defer,
			 CState = #client_msstate { file_handles_ets = FileHandlesEts,
										file_summary_ets = FileSummaryEts,
										gc_pid           = GCPid,
										client_ref       = Ref }) ->
	%% 该函数是如果需要读取的磁盘文件依然是锁住状态，当将阅读者减一后为0，则通知存储服务器进程的垃圾回收进程要操作的磁盘文件已经没有阅读者，让垃圾回收进程立刻进行垃圾回收
	Release =
		fun() -> ok = case ets:update_counter(FileSummaryEts, File,
											  {#file_summary.readers, -1}) of
						  0 -> case ets:lookup(FileSummaryEts, File) of
								   [#file_summary { locked = true }] ->
									   %% 通知垃圾回收进程File文件已经没有读取者
									   rabbit_msg_store_gc:no_readers(
										 GCPid, File);
								   _ -> ok
							   end;
						  _ -> ok
					  end
		end,
	%% If a GC involving(涉及) the file hasn't already started, it won't
	%% start now. Need to check again to see if we've been locked in
	%% the meantime, between lookup and update_counter (thus GC
	%% started before our +1. In fact, it could have finished by now
	%% too).
	case ets:lookup(FileSummaryEts, File) of
		[] -> %% GC has deleted our file, just go round again.
			%% 该文件已经被垃圾回收
			read(MsgId, CState);
		%% 在此处得到的消息索引磁盘信息仍然处于锁住状态，则将读取者减一后，直接向消息存储服务器进程读取消息
		[#file_summary { locked = true }] ->
			%% If we get a badarg here, then the GC has finished and
			%% deleted our file. Try going around again. Otherwise,
			%% just defer.
			%%
			%% badarg scenario: we lookup, msg_store locks, GC starts,
			%% GC ends, we +1 readers, msg_store ets:deletes (and
			%% unlocks the dest)
			try Release(),
				%% 再前往存储服务器进程读取该消息
				Defer()
			catch error:badarg -> read(MsgId, CState)
			end;
		%% 当前磁盘文件没有被锁住的情况
		[#file_summary { locked = false }] ->
			%% Ok, we're definitely(明确地) safe to continue - a GC involving
			%% the file cannot start up now, and isn't running, so
			%% nothing will tell us from now on to close the handle if
			%% it's already open.
			%%
			%% Finally, we need to recheck that the msg is still at
			%% the same place - it's possible an entire GC ran between
			%% us doing the lookup and the +1 on the readers. (Same as
			%% badarg scenario above, but we don't have a missing file
			%% - we just have the /wrong/ file).
			%% 为了避免一个完整的垃圾回收发生在最开始的索引查找和阅读者加一的操作之间，因此在此刻再查找一次索引
			case index_lookup(MsgId, CState) of
				%% 当前读出来的消息索引和之前读取的消息索引在同一个文件
				#msg_location { file = File } = MsgLocation ->
					%% Still the same file.
					%% 将客户端Ref对应的关闭的句柄从句柄缓存中删除掉
					{ok, CState1} = close_all_indicated(CState),
					%% We are now guaranteed(保证) that the mark_handle_open
					%% call will either insert_new correctly, or will
					%% fail, but find the value is open, not close.
					%% 标记文件句柄的打开
					mark_handle_open(FileHandlesEts, File, Ref),
					%% Could the msg_store now mark the file to be
					%% closed? No: marks for closing are issued only
					%% when the msg_store has locked the file.
					%% This will never be the current file
					%% 从磁盘文件中读取消息，根据消息索引
					{Msg, CState2} = read_from_disk(MsgLocation, CState1),
					%% 释放读取者，让读取者标志减一
					Release(), %% this MUST NOT fail with badarg
					%% 返回读取到的消息
					{{ok, Msg}, CState2};
				#msg_location {} = MsgLocation -> %% different file!
					%% 在此时找到的消息索引跟之前找到的消息索引所在的磁盘文件不一样，则将读取者数量减一
					Release(), %% this MUST NOT fail with badarg
					%% 去client_read1继续读取消息
					client_read1(MsgLocation, Defer, CState);
				not_found -> %% it seems not to exist. Defer, just to be sure.
					try Release() %% this can badarg, same as locked case, above
					catch error:badarg -> ok
					end,
					Defer()
			end
	end.


%% 更新flying_ets表中对应{MsgId, CRef}的Diff
client_update_flying(Diff, MsgId, #client_msstate { flying_ets = FlyingEts,
													client_ref = CRef }) ->
	Key = {MsgId, CRef},
	case ets:insert_new(FlyingEts, {Key, Diff}) of
		true  -> ok;
		false -> try ets:update_counter(FlyingEts, Key, {2, Diff}) of
					 0    -> ok;
					 Diff -> ok;
					 Err  -> throw({bad_flying_ets_update, Diff, Err, Key})
				 catch error:badarg ->
						   %% this is guaranteed to succeed since the
						   %% server only removes and updates flying_ets
						   %% entries; it never inserts them
						   true = ets:insert_new(FlyingEts, {Key, Diff})
				 end,
				 ok
	end.


%% 清理掉客户端队列进程在消息存储服务进程中的信息
clear_client(CRef, State = #msstate { cref_to_msg_ids = CTM,
									  dying_clients = DyingClients }) ->
	State #msstate { cref_to_msg_ids = dict:erase(CRef, CTM),
					 dying_clients = sets:del_element(CRef, DyingClients) }.


%%----------------------------------------------------------------------------
%% gen_server callbacks(存储服务器进程的回调函数接口)
%%----------------------------------------------------------------------------
%% 消息存储服务器进程的回调初始化函数
init([Server, BaseDir, ClientRefs, StartupFunState]) ->
	%% 接收EXIT信息
	process_flag(trap_exit, true),
	
	%% 向file_handle_cache进行注册
	ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
											 [self()]),
	
	%% 拿到自己存储服务器进程的存储路径(mnesia数据库存储目录 + "/msg_store_persistent"或者"/msg_store_transient")
	Dir = filename:join(BaseDir, atom_to_list(Server)),
	
	%% rabbit应用msg_store_index_module参数配置为rabbit_msg_store_ets_index
	{ok, IndexModule} = application:get_env(msg_store_index_module),
	%% 打印消息存储服务器进程使用的索引模块
	rabbit_log:info("~w: using ~p to provide index~n", [Server, IndexModule]),
	
	AttemptFileSummaryRecovery =
		case ClientRefs of
			%% 如果ClientRefs为空，则当前消息存储进程为临时消息存储进程，将当前目录下的所有磁盘文件全部删除
			undefined -> ok = rabbit_file:recursive_delete([Dir]),					%% 递归删除该目录下的所有文件(该目录下的都是临时消息在内存不足的时候存入磁盘的)
						 ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
						 false;
			_         -> ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
						 %% 将临时的存储文件放在正式的存储文件中的最后面(临时文件的后缀为rdt，正式存储文件的后缀为rdq)
						 recover_crashed_compactions(Dir)
		end,
	
	%% if we found crashed compactions we trust neither the
	%% file_summary nor the location index. Note the file_summary is
	%% left empty here if it can't be recovered.
	%% 创建rabbit_msg_store_file_summary ETS表(该ETS表示该存储服务器磁盘文件对应的信息数据结构)
	%% 如果存在file_summary.ets文件(该文件是RabbitMQ系统崩溃的时候根据rabbit_msg_store_file_summary ETS创建的)，则将该文件恢复成rabbit_msg_store_file_summary ETS表
	{FileSummaryRecovered, FileSummaryEts} =
		recover_file_summary(AttemptFileSummaryRecovery, Dir),
	
	%% 将保持在clean.dot文件中的信息取出来(clean.dot文件为RabbitMQ系统崩溃的时候产生)
	%% 将msg_store_index.ets文件中的消息索引信息恢复到rabbit_msg_store_ets_index ETS表(msg_store_index.ets为RabbitMQ系统崩溃的时候将消息的索引信息直接保持到该文件中)
	%% 如果消息存储服务器自己存的客户端标识和所有消息队列传入的客户端标识相等，同时消息索引处理模块相等，则通过消息索引模块进行恢复操作
	{CleanShutdown, IndexState, ClientRefs1} =
		recover_index_and_client_refs(IndexModule, FileSummaryRecovered,
									  ClientRefs, Dir, Server),
	%% 得到最新的客户端信息数据结构
	Clients = dict:from_list(
				[{CRef, {undefined, undefined, undefined}} ||
				 CRef <- ClientRefs1]),
	%% CleanShutdown => msg location index and file_summary both
	%% recovered correctly.
	%% 如果消息索引和队列进程客户端恢复失败，而磁盘文件信息恢复成功，则将磁盘文件信息的ETS清空
	true = case {FileSummaryRecovered, CleanShutdown} of
			   {true, false} -> ets:delete_all_objects(FileSummaryEts);
			   _             -> true
		   end,
	%% CleanShutdown <=> msg location index and file_summary both
	%% recovered correctly.
	
	%% 创建rabbit_msg_store_shared_file_handles ETS表(创建文件句柄缓存的ETS)
	FileHandlesEts  = ets:new(rabbit_msg_store_shared_file_handles,
							  [ordered_set, public]),
	%% 创建rabbit_msg_store_cur_file ETS表
	%% 保存写入到当前文件中的消息，每个元素的结构为{MsgId, Msg, CacheRefCount}
	%% 由客户端写入，其它客户端可访问。对于写入到当前文件中消息的访问都是从该缓存结构中读取的。每当新的持久化文件生成时，缓存中CacheRefCount为0的消息会被清除
	CurFileCacheEts = ets:new(rabbit_msg_store_cur_file, [set, public]),
	%% 创建rabbit_msg_store_flying ETS表
	%% 保存每个队列中正在等待执行的操作：读消息或者删除消息，每个元素的结构为{{MsgId, CRef}, Diff}
	%% 其中CRef代表队列进程，Diff代表一个差值：+1代表所有的操作可以合并成一个写操作，-1代表所有的操作可以合并成一个删除操作，0代表什么也不用做。
	%% 这个数据结构的主要作用是尽量减少对磁盘文件的写入和删除
	%% 考虑这样一个场景，一个队列需要持久化消息，在收到生产者的消息后，队列首先需要把消息写入磁盘，然后把消息投递到消费者
	%% 收到消费者的ack后，要从队列中删除该消息，假如ack回来时，消息的写入操作还未被执行，则这时候可以直接略过写入和读取操作
	FlyingEts       = ets:new(rabbit_msg_store_flying, [set, public]),
	
	%% 得到配置文件中配置的单个存储文件的最大存储量
	{ok, FileSizeLimit} = application:get_env(msg_store_file_size_limit),
	
	%% 在当前存储服务器进程下启动rabbit_msg_store_gc进程(用于垃圾回收的进程)
	{ok, GCPid} = rabbit_msg_store_gc:start_link(
					#gc_state { dir              = Dir,
								index_module     = IndexModule,
								index_state      = IndexState,
								file_summary_ets = FileSummaryEts,
								file_handles_ets = FileHandlesEts,
								msg_store        = self()
							  }),
	
	%% 组装存储服务器的存储状态的数据结构
	State = #msstate { dir                    = 		Dir,
					   index_module           = 		IndexModule,
					   index_state            = 		IndexState,
					   current_file           = 		0,
					   current_file_handle    = 		undefined,
					   file_handle_cache      = 		dict:new(),
					   sync_timer_ref         = 		undefined,
					   sum_valid_data         = 		0,
					   sum_file_size          = 		0,
					   pending_gc_completion  = 		orddict:new(),
					   gc_pid                 = 		GCPid,
					   file_handles_ets       = 		FileHandlesEts,
					   file_summary_ets       = 		FileSummaryEts,
					   cur_file_cache_ets     = 		CurFileCacheEts,
					   flying_ets             = 		FlyingEts,
					   dying_clients          = 		sets:new(),
					   clients                = 		Clients,
					   successfully_recovered = 		CleanShutdown,
					   file_size_limit        = 		FileSizeLimit,
					   cref_to_msg_ids        = 		dict:new()
					 },
	
	%% If we didn't recover the msg location index then we need to
	%% rebuild it now.
	%% 如果我们没有恢复消息 msg_location消息索引，则需要我们自己创建消息索引
	%% 该操作主要是通过持久化目录下的磁盘文件信息，将磁盘文件中的消息信息转化为内存中的消息索引(msg_location)，消息文件结构信息(file_summary_ets)
	%% 1.如果RabbitMQ系统恢复成功，则只需要更新当前持久化的总大小等信息
	%% 2.如果RabbitMQ系统是特殊停止，比如断电，服务器爆炸等异常情况，则需要根据磁盘中的信息进行恢复消息索引信息，单个文件信息的数据结构
	{Offset, State1 = #msstate { current_file = CurFile }} =
		build_index(CleanShutdown, StartupFunState, State),
	
	%% read is only needed so that we can seek
	%% 打开当前要操作的磁盘文件句柄
	{ok, CurHdl} = open_file(Dir, filenum_to_name(CurFile),
							 [read | ?WRITE_MODE]),
	%% 将当前操作的文件句柄的位置偏移量移到对应的偏移位置
	{ok, Offset} = file_handle_cache:position(CurHdl, Offset),
	%% 截断该磁盘文件的信息，后面的磁盘文件信息已经是无用的垃圾信息
	ok = file_handle_cache:truncate(CurHdl),
	
	%% 更新完当前操作的磁盘文件句柄后，然后立刻查看当前目录下面是否存在能够合并的两个文件，如果有的话，则立刻进行磁盘文件合并的操作
	{ok, maybe_compact(State1 #msstate { current_file_handle = CurHdl }),
	 hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


%% 得到call类型的消息优先级
prioritise_call(Msg, _From, _Len, _State) ->
	case Msg of
		successfully_recovered_state                        -> 7;
		{new_client_state, _Ref, _Pid, _MODC, _CloseFDsFun} -> 7;
		{read, _MsgId}                                      -> 2;
		_                                                   -> 0
	end.


%% 得到cast类型的消息优先级
prioritise_cast(Msg, _Len, _State) ->
	case Msg of
		{combine_files, _Source, _Destination, _Reclaimed} -> 8;
		{delete_file, _File, _Reclaimed}                   -> 8;
		{set_maximum_since_use, _Age}                      -> 8;
		{client_dying, _Pid}                               -> 7;
		_                                                  -> 0
	end.


%% 得到info类型的消息优先级
prioritise_info(Msg, _Len, _State) ->
	case Msg of
		sync                                               -> 8;
		_                                                  -> 0
	end.


%% 处理客户端队列进程过来询问是否恢复成功的消息
handle_call(successfully_recovered_state, _From, State) ->
	reply(State #msstate.successfully_recovered, State);

%% 队列客户端进程call过来向存储服务器进程进行注册
handle_call({new_client_state, CRef, CPid, MsgOnDiskFun, CloseFDsFun}, _From,
			State = #msstate { dir                = Dir,
							   index_state        = IndexState,
							   index_module       = IndexModule,
							   file_handles_ets   = FileHandlesEts,
							   file_summary_ets   = FileSummaryEts,
							   cur_file_cache_ets = CurFileCacheEts,
							   flying_ets         = FlyingEts,
							   clients            = Clients,
							   gc_pid             = GCPid }) ->
	%% 将客户端信息存储到clients的字典里
	Clients1 = dict:store(CRef, {CPid, MsgOnDiskFun, CloseFDsFun}, Clients),
	%% 然后立刻开始监视该队列进程
	erlang:monitor(process, CPid),
	%% 返回存储进程索引信息和索引模块以及存储目录，存储服务器垃圾回收的进程Pid，打开的文件句柄缓存ETS表名，当前操作的磁盘文件中消息缓存的ETS表名，flying_ets ETS表名
	reply({IndexState, IndexModule, Dir, GCPid, FileHandlesEts, FileSummaryEts,
		   CurFileCacheEts, FlyingEts},
		  State #msstate { clients = Clients1 });

%% 处理客户端队列进程终止的消息
handle_call({client_terminate, CRef}, _From, State) ->
	reply(ok, clear_client(CRef, State));

%% call存储服务器进程根据消息ID进行读取消息
handle_call({read, MsgId}, From, State) ->
	State1 = read_message(MsgId, From, State),
	noreply(State1);

%% 处理查看消息MsgId对应的消息是否存在
handle_call({contains, MsgId}, From, State) ->
	State1 = contains_message(MsgId, From, State),
	noreply(State1).


%% 存储服务器进程dying_clients增加死亡的客户端进程
handle_cast({client_dying, CRef},
			State = #msstate { dying_clients = DyingClients }) ->
	DyingClients1 = sets:add_element(CRef, DyingClients),
	noreply(write_message(CRef, <<>>,
						  State #msstate { dying_clients = DyingClients1 }));

%% 客户端进程删除的消息处理
handle_cast({client_delete, CRef},
			State = #msstate { clients = Clients }) ->
	State1 = State #msstate { clients = dict:erase(CRef, Clients) },
	noreply(remove_message(CRef, CRef, clear_client(CRef, State1)));

%% 服务端write操作（异步操作）
handle_cast({write, CRef, MsgId, Flow},
			State = #msstate { cur_file_cache_ets = CurFileCacheEts,
							   clients            = Clients }) ->
	%% 进程间消息流控制相关处理
	case Flow of
		flow   -> {CPid, _, _} = dict:fetch(CRef, Clients),
				  credit_flow:ack(CPid, ?CREDIT_DISC_BOUND);
		noflow -> ok
	end,
	%% cur_file_cache_ets对应MsgId记录，CacheRefCount减1，一般情况下，前面的client_write/4会对CachedRefCount加1，这里减1后，CacheRefCount为0，
	%% 在下面write_message/4操作完成后，如果要创建新的持久化文件，CacheRefCount为0的所有记录都会被清除（cur_file_cache_ets一般只保存当前文件里的消息）  
	true = 0 =< ets:update_counter(CurFileCacheEts, MsgId, {3, -1}),
	%% 根据flying_ets表中Diff的值来确定是否要执行实际的写入操作，如果update_flying/4返回ignore，则表示不需要写入
	%% 如果在收到该消息之前，消息队列已经对消息进行ack操作，则将不进行写磁盘的操作
	case update_flying(-1, MsgId, CRef, State) of
		process ->
			[{MsgId, Msg, _PWC}] = ets:lookup(CurFileCacheEts, MsgId),
			noreply(write_message(MsgId, Msg, CRef, State));
		ignore ->
			%% 消息队列进行remove操作已经操作，则淘汰了write写操作
			%% A 'remove' has already been issued(发行) and eliminated(淘汰) the
			%% 'write'.
			%% 消息已经得到ack后进行confirm操作接口
			State1 = blind_confirm(CRef, gb_sets:singleton(MsgId),
								   ignored, State),
			%% If all writes get eliminated(消除), cur_file_cache_ets could
			%% grow unbounded. To prevent that we delete the cache
			%% entry here, but only if the message isn't in the
			%% current file. That way reads of the message can
			%% continue to be done client side, from either the cache
			%% or the non-current files. If the message *is* in the
			%% current file then the cache entry will be removed by
			%% the normal logic for that in write_message/4 and
			%% maybe_roll_to_new_file/2.
			%% 如果要处理的消息不在当前文件，则清除cur_file_cache_ets中关于该消息的记录
			case index_lookup(MsgId, State1) of
				[#msg_location { file = File }]
				  when File == State1 #msstate.current_file ->
					ok;
				_ ->
					%% 将当前文件缓存ETS中CacheRefCount为0的记录删除
					true = ets:match_delete(CurFileCacheEts, {MsgId, '_', 0})
			end,
			noreply(State1)
	end;

%% 存储服务器进程异步处理删除的消息
handle_cast({remove, CRef, MsgIds}, State) ->
	{RemovedMsgIds, State1} =
		lists:foldl(
		  fun (MsgId, {Removed, State2}) ->
				   %% 根据flying_ets中的Diff值决定是否需要执行删除操作
				   case update_flying(+1, MsgId, CRef, State2) of
					   process -> {[MsgId | Removed],
								   remove_message(MsgId, CRef, State2)};
					   ignore  -> {Removed, State2}
				   end
		  end, {[], State}, MsgIds),
	noreply(maybe_compact(client_confirm(CRef, gb_sets:from_list(RemovedMsgIds),
										 ignored, State1)));

%% 存储服务器进程处理合并存储文件的消息
handle_cast({combine_files, Source, Destination, Reclaimed},
			State = #msstate { sum_file_size    = SumFileSize,
							   file_handles_ets = FileHandlesEts,
							   file_summary_ets = FileSummaryEts,
							   clients          = Clients }) ->
	%% 存储文件删除后进行相关的操作
	ok = cleanup_after_file_deletion(Source, State),
	%% see comment in cleanup_after_file_deletion, and client_read3
	true = mark_handle_to_close(Clients, FileHandlesEts, Destination, false),
	%% 将目的存储文件解锁
	true = ets:update_element(FileSummaryEts, Destination,
							  {#file_summary.locked, false}),
	%% 得到总的磁盘文件的实际大小(减掉合并文件中两个文件中的垃圾消息的大小)
	State1 = State #msstate { sum_file_size = SumFileSize - Reclaimed },
	%% 继续看是否有能够合并的磁盘文件进行合并
	noreply(maybe_compact(run_pending([Source, Destination], State1)));

%% 磁盘文件的删除
handle_cast({delete_file, File, Reclaimed},
			State = #msstate { sum_file_size = SumFileSize }) ->
	%% 存储文件删除后进行相关的操作
	ok = cleanup_after_file_deletion(File, State),
	%% 设置当前最新的总的文件大小
	State1 = State #msstate { sum_file_size = SumFileSize - Reclaimed },
	noreply(maybe_compact(run_pending([File], State1)));

handle_cast({set_maximum_since_use, Age}, State) ->
	ok = file_handle_cache:set_maximum_since_use(Age),
	noreply(State).


%% 处理同步的消息
handle_info(sync, State) ->
	noreply(internal_sync(State));

%% 处理超时的消息
handle_info(timeout, State) ->
	noreply(internal_sync(State));

%% 处理客户端队列进程死掉的消息
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
	credit_flow:peer_down(Pid),
	noreply(State);

handle_info({'EXIT', _Pid, Reason}, State) ->
	{stop, Reason, State}.


%% 存储服务器进程崩溃的处理
terminate(_Reason, State = #msstate { index_state         = IndexState,
									  index_module        = IndexModule,
									  current_file_handle = CurHdl,
									  gc_pid              = GCPid,
									  file_handles_ets    = FileHandlesEts,
									  file_summary_ets    = FileSummaryEts,
									  cur_file_cache_ets  = CurFileCacheEts,
									  flying_ets          = FlyingEts,
									  clients             = Clients,
									  dir                 = Dir }) ->
	%% stop the gc first, otherwise it could be working and we pull
	%% out the ets tables from under it.
	%% 停止垃圾回收进程
	ok = rabbit_msg_store_gc:stop(GCPid),
	%% 关闭掉当前磁盘文件的句柄
	State1 = case CurHdl of
				 undefined -> State;
				 %% 这里是将所有需要被confirm的消息按所属的queue进行分组，然后通知对应的消息队列已经得到confirm的消息
				 _         -> State2 = internal_sync(State),
							  %% 然后关闭当前操作的文件句柄
							  ok = file_handle_cache:close(CurHdl),
							  State2
			 end,
	%% 存储服务器进程关闭所有缓存的文件句柄
	State3 = close_all_handles(State1),
	%% 将file_summary的ETS表中的所有数据存储到file_summary.ets这个文件中，等到RabbitMQ系统重启的时候会将该文件中的数据直接读取到file_summary ETS表
	%% 即将当前存储服务器对应的磁盘文件ETS表转化为磁盘文件存储到当前存储服务器对应的磁盘目录下，等RabbitMQ系统恢复重启，则将数据直接从当前磁盘文件恢复为file_summary ETS表
	ok = store_file_summary(FileSummaryEts, Dir),
	%% file_summary_ets file_handles_ets cur_file_cache_ets flying_ets这四个ETS表
	[true = ets:delete(T) || T <- [FileSummaryEts, FileHandlesEts,
								   CurFileCacheEts, FlyingEts]],
	%% RabbitMQ系统崩溃的时候，将消息索引信息存储到msg_store_index.ets这个文件中(将所有消息索引对应的ETS转化为磁盘文件，RabbitMQ系统恢复重启，则将磁盘文件恢复成消息索引ETS)
	IndexModule:terminate(IndexState),
	%% 将客户端标识和索引处理的模块名字信息存储到clean.dot文件中(当RabbitMQ系统崩溃的时候)
	ok = store_recovery_terms([{client_refs, dict:fetch_keys(Clients)},
							   {index_module, IndexModule}], Dir),
	State3 #msstate { index_state         = undefined,
					  current_file_handle = undefined }.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%----------------------------------------------------------------------------
%% general helper functions(通用的辅助函数)
%%----------------------------------------------------------------------------
%% 没有信息返回给客户端进程
noreply(State) ->
	{State1, Timeout} = next_state(State),
	{noreply, State1, Timeout}.


%% 返回给客户端仅此Reply信息
reply(Reply, State) ->
	{State1, Timeout} = next_state(State),
	{reply, Reply, State1, Timeout}.


next_state(State = #msstate { sync_timer_ref  = undefined,
							  cref_to_msg_ids = CTM }) ->
	case dict:size(CTM) of
		0 -> {State, hibernate};
		_ -> {start_sync_timer(State), 0}
	end;

next_state(State = #msstate { cref_to_msg_ids = CTM }) ->
	case dict:size(CTM) of
		0 -> {stop_sync_timer(State), hibernate};
		_ -> {State, 0}
	end.


%% 启动同步定时器
start_sync_timer(State) ->
	rabbit_misc:ensure_timer(State, #msstate.sync_timer_ref,
							 ?SYNC_INTERVAL, sync).


%% 停止同步定时器
stop_sync_timer(State) ->
	rabbit_misc:stop_timer(State, #msstate.sync_timer_ref).


%% 这里是将所有需要被confirm的消息按所属的queue进行分组，然后通知对应的消息队列已经得到confirm的消息
internal_sync(State = #msstate { current_file_handle = CurHdl,
								 cref_to_msg_ids     = CTM }) ->
	%% 先停止同步的定时器
	State1 = stop_sync_timer(State),
	%% 拿到所有队列客户端需要confirm的消息ID列表
	CGs = dict:fold(fun (CRef, MsgIds, NS) ->
							 case gb_sets:is_empty(MsgIds) of
								 true  -> NS;
								 false -> [{CRef, MsgIds} | NS]
							 end
					end, [], CTM),
	ok = case CGs of
			 [] -> ok;
			 _  -> file_handle_cache:sync(CurHdl)
		 end,
	%% 实际的通知队列客户端进程需要confirm的消息ID列表的动作
	lists:foldl(fun ({CRef, MsgIds}, StateN) ->
						 client_confirm(CRef, MsgIds, written, StateN)
				end, State1, CGs).


%% 更新flying_ets这个ETS表
update_flying(Diff, MsgId, CRef, #msstate { flying_ets = FlyingEts }) ->
	Key = {MsgId, CRef},
	NDiff = -Diff,
	case ets:lookup(FlyingEts, Key) of
		[]           -> ignore;
		[{_,  Diff}] -> ignore; %% [1]
		%% 此处是CRef处理写消息的时候，CRef消息队列没有将消息删除，则将flying ETS中的信息删除，然后进行消息写操作
		[{_, NDiff}] -> ets:update_counter(FlyingEts, Key, {2, Diff}),
						true = ets:delete_object(FlyingEts, {Key, 0}),
						process;
		%% 当执行CRef客户端消息队列写消息的时候，发现CRef已经将消息删除，则只需要立刻将该CRef对应的信息从flying ETS中删除，不用进行消息写操作
		[{_, 0}]     -> true = ets:delete_object(FlyingEts, {Key, 0}),
						ignore;
		[{_, Err}]   -> throw({bad_flying_ets_record, Diff, Err, Key})
	end.
%% [1] We can get here, for example, in the following scenario: There
%% is a write followed by a remove in flight. The counter will be 0,
%% so on processing the write the server attempts to delete the
%% entry. If at that point the client injects another write it will
%% either insert a new entry, containing +1, or increment the existing
%% entry to +1, thus preventing its removal. Either way therefore when
%% the server processes the read, the counter will be +1.

%% 当前客户端已经死亡，同时该消息ID对应的消息索引没有发现
write_action({true, not_found}, _MsgId, State) ->
	{ignore, undefined, State};

%% 当前客户端已经死亡，返回该消息索引的磁盘文件名字
write_action({true, #msg_location { file = File }}, _MsgId, State) ->
	{ignore, File, State};

%% 当前客户端没有死亡同时该消息的索引没有读取到，则需要直接写入磁盘(这种情况是消息第一次写入消息存储服务器进程，需要创建一个新的消息索引)
write_action({false, not_found}, _MsgId, State) ->
	{write, State};

%% 此处是客户端没有死亡，同时消息索引的引用计数为0的情况
write_action({Mask, #msg_location { ref_count = 0, file = File,
									total_size = TotalSize }},
			 MsgId, State = #msstate { file_summary_ets = FileSummaryEts }) ->
	case {Mask, ets:lookup(FileSummaryEts, File)} of
		{false, [#file_summary { locked = true }]} ->
			ok = index_delete(MsgId, State),
			{write, State};
		{false_if_increment, [#file_summary { locked = true }]} ->
			%% The msg for MsgId is older than the client death
			%% message, but as it is being GC'd currently we'll have
			%% to write a new copy, which will then be younger, so
			%% ignore this write.
			{ignore, File, State};
		{_Mask, [#file_summary {}]} ->
			%% 将消息索引引用增加一
			ok = index_update_ref_count(MsgId, 1, State),
			%% 更新当前存储文件的文件大小以及总的数据量的大小
			State1 = adjust_valid_total_size(File, TotalSize, State),
			{confirm, File, State1}
	end;

%% 此处的情况是消息队列没有挂掉，同时消息索引存在且引用不为0，则当前写入只需要将该消息的引用加一，然后可以通知客户端可以对该消息进行confirm操作
write_action({_Mask, #msg_location { ref_count = RefCount, file = File }},
			 MsgId, State) ->
	ok = index_update_ref_count(MsgId, RefCount + 1, State),
	%% We already know about it, just update counter. Only update
	%% field otherwise bad interaction with concurrent GC
	{confirm, File, State}.


%% 完成实际的消息写入入口
write_message(MsgId, Msg, CRef,
			  State = #msstate { cur_file_cache_ets = CurFileCacheEts }) ->
	case write_action(should_mask_action(CRef, MsgId, State), MsgId, State) of
		%% 当前存储服务器进程中没有该消息，需要将该消息写入磁盘(该消息是第一次写入该消息存储服务器进程)
		{write, State1} ->
			write_message(MsgId, Msg,
						  %% CRef对应的消息队列新增可以confirm的消息MsgId
						  record_pending_confirm(CRef, MsgId, State1));
		%% 忽略掉该消息(出现这种情况有可能是CRef对应的客户端已经挂掉)
		{ignore, CurFile, State1 = #msstate { current_file = CurFile }} ->
			State1;
		%% 忽略掉该消息(出现这种情况有可能是CRef对应的客户端已经挂掉)
		{ignore, _File, State1} ->
			%% 从消息缓存中将该消息的引用为0的信息删除掉
			true = ets:delete_object(CurFileCacheEts, {MsgId, Msg, 0}),
			State1;
		%% 如果是当前操作文件confirm的消息，则先将要confirm的消息记录下来
		{confirm, CurFile, State1 = #msstate { current_file = CurFile }}->
			record_pending_confirm(CRef, MsgId, State1);
		%% 如果是不同操作文件confirm的消息，则将当前消息缓存中引用为0的删除掉，然后立刻通知客户端进行消息confirm
		{confirm, _File, State1} ->
			true = ets:delete_object(CurFileCacheEts, {MsgId, Msg, 0}),
			%% 立刻通知客户端进行消息confirm
			update_pending_confirms(
			  fun (MsgOnDiskFun, CTM) ->
					   MsgOnDiskFun(gb_sets:singleton(MsgId), written),
					   CTM
			  end, CRef, State1)
	end.


%% 消息存储服务器进程消息删除操作
remove_message(MsgId, CRef,
			   State = #msstate { file_summary_ets = FileSummaryEts }) ->
	case should_mask_action(CRef, MsgId, State) of
		{true, _Location} ->
			State;
		{false_if_increment, #msg_location { ref_count = 0 }} ->
			%% CRef has tried to both write and remove this msg whilst
			%% it's being GC'd.
			%%
			%% ASSERTION: [#file_summary { locked = true }] =
			%% ets:lookup(FileSummaryEts, File),
			State;
		{_Mask, #msg_location { ref_count = RefCount, file = File,
								total_size = TotalSize }}
		  when RefCount > 0 ->
			%% only update field, otherwise bad interaction(相互影响) with
			%% concurrent GC
			%% 将消息MsgId对应的索引信息中引用字段减一的函数
			Dec = fun () -> index_update_ref_count(
							  MsgId, RefCount - 1, State) end,
			case RefCount of
				%% don't remove from cur_file_cache_ets here because
				%% there may be further writes in the mailbox for the
				%% same msg.
				%% 消息索引引用为一，则减去后，引用为0
				1 -> case ets:lookup(FileSummaryEts, File) of
						 [#file_summary { locked = true }] ->
							 %% 将自己加入等待垃圾回收等待队列
							 add_to_pending_gc_completion(
							   {remove, MsgId, CRef}, File, State);
						 %% 当前索引磁盘没有被锁住，则立刻执行引用减一，然后判断该磁盘索引文件是否需要删除
						 [#file_summary {}] ->
							 ok = Dec(),
							 %% 在删除掉该消息后，看存储该消息的磁盘文件的有效消息是否为0，如果为0的话则直接将该磁盘文件删除掉
							 delete_file_if_empty(
							   File, adjust_valid_total_size(
								 File, -TotalSize, State))
					 end;
				%% 如果当前索引的引用大于一，则只将消息的索引减一
				_ -> ok = Dec(),
					 State
			end
	end.


%% 存储服务器进程内部写消息到磁盘的接口(消息第一次写入消息存储服务器进程会走到此处，将消息存入到磁盘，然后新建消息索引)
write_message(MsgId, Msg,
			  State = #msstate { current_file_handle = CurHdl,
								 current_file        = CurFile,
								 sum_valid_data      = SumValid,
								 sum_file_size       = SumFileSize,
								 file_summary_ets    = FileSummaryEts }) ->
	%% 拿到CurHdl句柄对应的最新偏移量
	{ok, CurOffset} = file_handle_cache:current_virtual_offset(CurHdl),
	%% 将消息内容添加到当前文件后面
	{ok, TotalSize} = rabbit_msg_file:append(CurHdl, MsgId, Msg),
	%% 生成新的消息索引结构，然后插入索引ETS表中
	ok = index_insert(
		   #msg_location { msg_id = MsgId, ref_count = 1, file = CurFile,
						   offset = CurOffset, total_size = TotalSize }, State),
	[#file_summary { right = undefined, locked = false }] =
		ets:lookup(FileSummaryEts, CurFile),
	%% 更新当前文件大小的统计信息
	[_, _] = ets:update_counter(FileSummaryEts, CurFile,
								[{#file_summary.valid_total_size, TotalSize},
								 {#file_summary.file_size,        TotalSize}]),
	%% 更新所有文件的统计信息，并判断是否需要创建一个新的持久化文件
	maybe_roll_to_new_file(CurOffset + TotalSize,
						   State #msstate {
										   sum_valid_data = SumValid    + TotalSize,
										   sum_file_size  = SumFileSize + TotalSize }).


%% 在存储服务器进程里进行读取消息
read_message(MsgId, From, State) ->
	%% 从索引存储ETS查找Key对应的数据(消息的引用计数为0认为消息不存在)
	case index_lookup_positive_ref_count(MsgId, State) of
		not_found   -> gen_server2:reply(From, not_found),
					   State;
		MsgLocation -> read_message1(From, MsgLocation, State)
	end.


%% 存储服务器进程中实际根据消息索引读取消息实体的接口
read_message1(From, #msg_location { msg_id = MsgId, file = File,
									offset = Offset } = MsgLoc,
			  State = #msstate { current_file        = CurFile,
								 current_file_handle = CurHdl,
								 file_summary_ets    = FileSummaryEts,
								 cur_file_cache_ets  = CurFileCacheEts }) ->
	%% 要读取的消息存储的文件跟当前存储服务器打开的文件相等
	case File =:= CurFile of
		true  -> {Msg, State1} =
					 %% can return [] if msg in file existed on startup
					 %% 从当前操作的磁盘文件消息缓存中读取消息
					 case ets:lookup(CurFileCacheEts, MsgId) of
						 [] ->
							 {ok, RawOffSet} =
								 file_handle_cache:current_raw_offset(CurHdl),
							 ok = case Offset >= RawOffSet of
									  %% 如果要读取的文件的偏移量大于当前文件的偏移量，则需要将缓存中的文件写入磁盘文件，然后从磁盘文件中读取自己需要的消息
									  true  -> file_handle_cache:flush(CurHdl);
									  false -> ok
								  end,
							 %% 从磁盘文件中读取消息，根据消息索引
							 read_from_disk(MsgLoc, State);
						 [{MsgId, Msg1, _CacheRefCount}] ->
							 {Msg1, State}
					 end,
				 %% 直接将结果返回给客户端进程
				 gen_server2:reply(From, {ok, Msg}),
				 State1;
		%% 要读取的消息存储的文件跟当前存储服务器打开的文件不相等
		false -> [#file_summary { locked = Locked }] =
					 ets:lookup(FileSummaryEts, File),
				 case Locked of
					 %% 将操作加入需要等待垃圾回收完成的dict字典中(客户端进程同步等待中，自己的存储进程则继续做其他事情)
					 true  -> add_to_pending_gc_completion({read, MsgId, From},
														   File, State);
					 %% 如果文件没有被锁住，则直接从磁盘文件中读取
					 false -> {Msg, State1} = read_from_disk(MsgLoc, State),
							  gen_server2:reply(From, {ok, Msg}),
							  State1
				 end
	end.


%% 从磁盘文件中读取消息，根据消息索引
read_from_disk(#msg_location { msg_id = MsgId, file = File, offset = Offset,
							   total_size = TotalSize }, State) ->
	{Hdl, State1} = get_read_handle(File, State),
	%% 定位到磁盘文件存储改消息的地方
	{ok, Offset} = file_handle_cache:position(Hdl, Offset),
	{ok, {MsgId, Msg}} =
		case rabbit_msg_file:read(Hdl, TotalSize) of
			{ok, {MsgId, _}} = Obj ->
				Obj;
			Rest ->
				{error, {misread, [{old_state, State},
								   {file_num,  File},
								   {offset,    Offset},
								   {msg_id,    MsgId},
								   {read,      Rest},
								   {proc_dict, get()}
								  ]}}
		end,
	{Msg, State1}.


contains_message(MsgId, From,
				 State = #msstate { pending_gc_completion = Pending }) ->
	%% 从索引存储ETS查找Key对应的数据(消息的引用计数为0也认为不存在)
	case index_lookup_positive_ref_count(MsgId, State) of
		not_found ->
			gen_server2:reply(From, false),
			State;
		#msg_location { file = File } ->
			case orddict:is_key(File, Pending) of
				true  -> %% 将操作加入需要等待垃圾回收完成的dict字典中
						 add_to_pending_gc_completion(
						   {contains, MsgId, From}, File, State);
				false -> gen_server2:reply(From, true),
						 State
			end
	end.


%% 将操作加入需要等待垃圾回收完成的dict字典中
add_to_pending_gc_completion(
  Op, File, State = #msstate { pending_gc_completion = Pending }) ->
	State #msstate { pending_gc_completion =
						 rabbit_misc:orddict_cons(File, Op, Pending) }.


%% 执行正在等待文件垃圾回收完成后的后续操作
run_pending(Files, State) ->
	lists:foldl(
	  fun (File, State1 = #msstate { pending_gc_completion = Pending }) ->
			   Pending1 = orddict:erase(File, Pending),
			   lists:foldl(
				 fun run_pending_action/2,
				 State1 #msstate { pending_gc_completion = Pending1 },
				 lists:reverse(orddict:fetch(File, Pending)))
	  end, State, Files).


%% 垃圾回收完毕后，执行等待该垃圾回收的相关操作
run_pending_action({read, MsgId, From}, State) ->
	read_message(MsgId, From, State);
run_pending_action({contains, MsgId, From}, State) ->
	contains_message(MsgId, From, State);
run_pending_action({remove, MsgId, CRef}, State) ->
	remove_message(MsgId, CRef, State).


%% 安全的更新消息读取者的数量
safe_ets_update_counter(Tab, Key, UpdateOp, SuccessFun, FailThunk) ->
	try
		SuccessFun(ets:update_counter(Tab, Key, UpdateOp))
	catch error:badarg -> FailThunk()
	end.


%% 更新cur_file_cache_ets表中对应MsgId的记录，CacheRefCount加1
update_msg_cache(CacheEts, MsgId, Msg) ->
	case ets:insert_new(CacheEts, {MsgId, Msg, 1}) of
		true  -> ok;
		%% 如果该消息已经存在于消息缓存中，则将读取字段加一
		false -> safe_ets_update_counter(
				   CacheEts, MsgId, {3, +1}, fun (_) -> ok end,
				   fun () -> update_msg_cache(CacheEts, MsgId, Msg) end)
	end.


%% 更新当前存储文件的文件大小以及总的数据量的大小
adjust_valid_total_size(File, Delta, State = #msstate {
													   sum_valid_data   = SumValid,
													   file_summary_ets = FileSummaryEts }) ->
	[_] = ets:update_counter(FileSummaryEts, File,
							 [{#file_summary.valid_total_size, Delta}]),
	State #msstate { sum_valid_data = SumValid + Delta }.


%% 字典存储接口
orddict_store(Key, Val, Dict) ->
	false = orddict:is_key(Key, Dict),
	orddict:store(Key, Val, Dict).


%% 根据Fun函数更新消息存储服务器进程中等待confirm的字典数据结构
update_pending_confirms(Fun, CRef,
						State = #msstate { clients         = Clients,
										   cref_to_msg_ids = CTM }) ->
	case dict:fetch(CRef, Clients) of
		{_CPid, undefined,    _CloseFDsFun} -> State;
		{_CPid, MsgOnDiskFun, _CloseFDsFun} -> CTM1 = Fun(MsgOnDiskFun, CTM),
											   State #msstate {
															   cref_to_msg_ids = CTM1 }
	end.


%% CRef对应的消息队列新增可以confirm的消息MsgId
record_pending_confirm(CRef, MsgId, State) ->
	update_pending_confirms(
	  fun (_MsgOnDiskFun, CTM) ->
			   %% 更新队列进程对应的需要confirm的消息ID
			   dict:update(CRef, fun (MsgIds) -> gb_sets:add(MsgId, MsgIds) end,
						   gb_sets:singleton(MsgId), CTM)
	  end, CRef, State).


%% CRef对应的消息队列MsgIds的消息ID列表进程confirm操作
client_confirm(CRef, MsgIds, ActionTaken, State) ->
	update_pending_confirms(
	  fun (MsgOnDiskFun, CTM) ->
			   case dict:find(CRef, CTM) of
				   {ok, Gs} -> %% 将要confirm的MsgIds和CRef对应的需要confirm的消息ID求交集，将这些交集通知队列进程消息confirm
					   		   MsgOnDiskFun(gb_sets:intersection(Gs, MsgIds),
											ActionTaken),
							   %% 将已经confirm的MsgIds从CRef对应的confirm结构gb_sets中删除掉，得到CRef剩余的还未confirm的消息ID列表
							   MsgIds1 = rabbit_misc:gb_sets_difference(
										   Gs, MsgIds),
							   case gb_sets:is_empty(MsgIds1) of
								   %% 如果剩余的等待confirm的消息ID为空，则将CRef从字典中删除掉
								   true  -> dict:erase(CRef, CTM);
								   false -> dict:store(CRef, MsgIds1, CTM)
							   end;
				   error    -> CTM
			   end
	  end, CRef, State).


%% 消息已经得到ack后进行confirm操作接口
blind_confirm(CRef, MsgIds, ActionTaken, State) ->
	update_pending_confirms(
	  fun (MsgOnDiskFun, CTM) -> MsgOnDiskFun(MsgIds, ActionTaken), CTM end,
	  CRef, State).

%% Detect(检测) whether the MsgId is older or younger than the client's death
%% msg (if there is one). If the msg is older than the client death
%% msg, and it has a 0 ref_count we must only alter(改变) the ref_count, not
%% rewrite the msg - rewriting it would make it younger than the death
%% msg and thus should be ignored. Note that this (correctly) returns
%% false when testing to remove the death msg itself.
should_mask_action(CRef, MsgId,
				   State = #msstate { dying_clients = DyingClients }) ->
	case {sets:is_element(CRef, DyingClients), index_lookup(MsgId, State)} of
		{false, Location} ->
			{false, Location};
		{true, not_found} ->
			{true, not_found};
		%% 对应的消息队列已经挂掉，则取出
		{true, #msg_location { file = File, offset = Offset,
							   ref_count = RefCount } = Location} ->
			#msg_location { file = DeathFile, offset = DeathOffset } =
							  index_lookup(CRef, State),
			%% 用客户端死亡写入磁盘文件中的偏移位置和当前得到的消息索引在磁盘文件中的偏移量进行比较
			%% 如果消息索引进入该消息存储服务器的时间小于客户端消息队列的死亡，则认为该消息对应的客户端没有死亡，可以继续将消息写入
			{case {{DeathFile, DeathOffset} < {File, Offset}, RefCount} of
				 {true,  _} -> true;
				 {false, 0} -> false_if_increment;
				 {false, _} -> false
			 end, Location}
	end.

%%----------------------------------------------------------------------------
%% file helper functions(磁盘文件操作辅助函数)
%%----------------------------------------------------------------------------
%% 打开存储文件接口
open_file(Dir, FileName, Mode) ->
	file_handle_cache:open(form_filename(Dir, FileName), ?BINARY_MODE ++ Mode,
						   [{write_buffer, ?HANDLE_CACHE_BUFFER_SIZE},
							{read_buffer,  ?HANDLE_CACHE_BUFFER_SIZE}]).


%% 客户端进程关闭缓存中的的文件句柄
close_handle(Key, CState = #client_msstate { file_handle_cache = FHC }) ->
	CState #client_msstate { file_handle_cache = close_handle(Key, FHC) };


%% 存储服务器进程关闭缓存中的文件句柄
close_handle(Key, State = #msstate { file_handle_cache = FHC }) ->
	State #msstate { file_handle_cache = close_handle(Key, FHC) };


%% 实际关闭文件句柄的函数接口(FHC为file_handle_cache文件句柄缓存文件)
close_handle(Key, FHC) ->
	case dict:find(Key, FHC) of
		{ok, Hdl} -> ok = file_handle_cache:close(Hdl),
					 dict:erase(Key, FHC);
		error     -> FHC
	end.


%% 标记文件句柄的打开
mark_handle_open(FileHandlesEts, File, Ref) ->
	%% This is fine to fail (already exists). Note it could fail with
	%% the value being close, and not have it updated to open.
	ets:insert_new(FileHandlesEts, {{Ref, File}, open}),
	true.


%% See comment in client_read3 - only call this when the file is locked
%% File这个磁盘文件被删除后调用该函数
mark_handle_to_close(ClientRefs, FileHandlesEts, File, Invoke) ->
	[ begin
		  case (ets:update_element(FileHandlesEts, Key, {2, close})
					andalso Invoke) of
			  true  -> case dict:fetch(Ref, ClientRefs) of
						   {_CPid, _MsgOnDiskFun, undefined} ->
							   ok;
						   {_CPid, _MsgOnDiskFun, CloseFDsFun} ->
							   ok = CloseFDsFun()
					   end;
			  false -> ok
		  end
	  end || {{Ref, _File} = Key, open} <-
				 ets:match_object(FileHandlesEts, {{'_', File}, open}) ],
	true.


%% 当File文件在文件句柄ETS对应的数据不存在，则立刻将该消息磁盘文件删除
safe_file_delete_fun(File, Dir, FileHandlesEts) ->
	fun () -> safe_file_delete(File, Dir, FileHandlesEts) end.


safe_file_delete(File, Dir, FileHandlesEts) ->
	%% do not match on any value - it's the absence of the row that
	%% indicates the client has really closed the file.
	case ets:match_object(FileHandlesEts, {{'_', File}, '_'}, 1) of
		{[_ | _], _Cont} -> false;
		_                -> ok = file:delete(
								   form_filename(Dir, filenum_to_name(File))),
							true
	end.


%% 将客户端Ref对应的关闭的句柄从句柄缓存中删除掉
close_all_indicated(#client_msstate { file_handles_ets = FileHandlesEts,
									  client_ref       = Ref } =
										CState) ->
	Objs = ets:match_object(FileHandlesEts, {{Ref, '_'}, close}),
	{ok, lists:foldl(fun ({Key = {_Ref, File}, close}, CStateM) ->
							  true = ets:delete(FileHandlesEts, Key),
							  close_handle(File, CStateM)
					 end, CState, Objs)}.


%% 客户端进程关掉所有的文件句柄
close_all_handles(CState = #client_msstate { file_handles_ets  = FileHandlesEts,
											 file_handle_cache = FHC,
											 client_ref        = Ref }) ->
	ok = dict:fold(fun (File, Hdl, ok) ->
							%% 删除文件句柄ETS中的数据
							true = ets:delete(FileHandlesEts, {Ref, File}),
							%% 关闭句柄
							file_handle_cache:close(Hdl)
				   end, ok, FHC),
	CState #client_msstate { file_handle_cache = dict:new() };


%% 存储服务器进程关闭所有缓存的文件句柄
close_all_handles(State = #msstate { file_handle_cache = FHC }) ->
	ok = dict:fold(fun (_Key, Hdl, ok) -> file_handle_cache:close(Hdl) end,
				   ok, FHC),
	State #msstate { file_handle_cache = dict:new() }.


%% 客户端队列进程得到要读取的磁盘文件的句柄(如果dict中缓存的有，则直接拿去，如果没有则打开新的句柄，同时将新的句柄存入dict字典中)
get_read_handle(FileNum, CState = #client_msstate { file_handle_cache = FHC,
													dir = Dir }) ->
	{Hdl, FHC2} = get_read_handle(FileNum, FHC, Dir),
	{Hdl, CState #client_msstate { file_handle_cache = FHC2 }};


%% 消息存储服务器进程得到要读取的磁盘文件的句柄(如果dict中缓存的有，则直接拿去，如果没有则打开新的句柄，同时将新的句柄存入dict字典中)
get_read_handle(FileNum, State = #msstate { file_handle_cache = FHC,
											dir = Dir }) ->
	{Hdl, FHC2} = get_read_handle(FileNum, FHC, Dir),
	{Hdl, State #msstate { file_handle_cache = FHC2 }}.


%% 得到要读取的磁盘文件的句柄(如果dict中缓存的有，则直接拿去，如果没有则打开新的句柄)
get_read_handle(FileNum, FHC, Dir) ->
	case dict:find(FileNum, FHC) of
		{ok, Hdl} -> {Hdl, FHC};
		error     -> {ok, Hdl} = open_file(Dir, filenum_to_name(FileNum),
										   ?READ_MODE),
					 {Hdl, dict:store(FileNum, Hdl, FHC)}
	end.


%% preallocate：预分配
preallocate(Hdl, FileSizeLimit, FinalPos) ->
	{ok, FileSizeLimit} = file_handle_cache:position(Hdl, FileSizeLimit),
	ok = file_handle_cache:truncate(Hdl),
	{ok, FinalPos} = file_handle_cache:position(Hdl, FinalPos),
	ok.


%% 截断并且扩张文件
truncate_and_extend_file(Hdl, Lowpoint, Highpoint) ->
	{ok, Lowpoint} = file_handle_cache:position(Hdl, Lowpoint),
	ok = file_handle_cache:truncate(Hdl),
	ok = preallocate(Hdl, Highpoint, Lowpoint).


%% 生成存储文件的绝对路径
form_filename(Dir, Name) -> filename:join(Dir, Name).


%% 根据文件名组装磁盘存储文件名
filenum_to_name(File) -> integer_to_list(File) ++ ?FILE_EXTENSION.


%% 去掉文件的后缀，得到文件的数字
filename_to_num(FileName) -> list_to_integer(filename:rootname(FileName)).


%% 列出Dir目录下的所有文件，并对所有的文件名进行排序
list_sorted_filenames(Dir, Ext) ->
	lists:sort(fun (A, B) -> filename_to_num(A) < filename_to_num(B) end,
			   filelib:wildcard("*" ++ Ext, Dir)).

%%----------------------------------------------------------------------------
%% index(消息索引处理接口相关)
%%----------------------------------------------------------------------------
%% 从索引存储ETS查找Key对应的数据(消息的引用计数为0也认为不存在)
index_lookup_positive_ref_count(Key, State) ->
	case index_lookup(Key, State) of
		not_found                       -> not_found;
		%% 消息的引用计数为0也认为不存在
		#msg_location { ref_count = 0 } -> not_found;
		#msg_location {} = MsgLocation  -> MsgLocation
	end.


%% 更新消息索引的引用计数
index_update_ref_count(Key, RefCount, State) ->
	index_update_fields(Key, {#msg_location.ref_count, RefCount}, State).


%% 从索引存储ETS根据Key查找对应的数据
index_lookup(Key, #client_msstate { index_module = Index,
									index_state  = State }) ->
	Index:lookup(Key, State);


%% 消息索引ETS的查找
index_lookup(Key, #msstate { index_module = Index, index_state = State }) ->
	Index:lookup(Key, State).


%% 将msg_location结构的索引信息存入rabbit_msg_store_ets_index ETS表
index_insert(Obj, #msstate { index_module = Index, index_state = State }) ->
	Index:insert(Obj, State).


%% 消息索引ETS的更新
index_update(Obj, #msstate { index_module = Index, index_state = State }) ->
	Index:update(Obj, State).


%% 消息索引ETS字段的更新
index_update_fields(Key, Updates, #msstate { index_module = Index,
											 index_state  = State }) ->
	Index:update_fields(Key, Updates, State).


%% 消息索引删除接口
index_delete(Key, #msstate { index_module = Index, index_state = State }) ->
	Index:delete(Key, State).


%% 根据磁盘文件名删除消息索引
index_delete_by_file(File, #msstate { index_module = Index,
									  index_state  = State }) ->
	Index:delete_by_file(File, State).

%%----------------------------------------------------------------------------
%% shutdown and recovery(RabbitMQ服务器关闭和恢复相关的接口)
%%----------------------------------------------------------------------------
%% 如果消息存储服务器自己存的客户端标识和所有消息队列传入的客户端标识相等，同时消息索引处理模块相等，则通过消息索引模块进行恢复操作
recover_index_and_client_refs(IndexModule, _Recover, undefined, Dir, _Server) ->
	{false, IndexModule:new(Dir), []};

recover_index_and_client_refs(IndexModule, false, _ClientRefs, Dir, Server) ->
	rabbit_log:warning("~w: rebuilding indices from scratch~n", [Server]),
	{false, IndexModule:new(Dir), []};

recover_index_and_client_refs(IndexModule, true, ClientRefs, Dir, Server) ->
	Fresh = fun (ErrorMsg, ErrorArgs) ->
					 rabbit_log:warning("~w: " ++ ErrorMsg ++ "~n"
											"rebuilding indices from scratch~n",
										[Server | ErrorArgs]),
					 {false, IndexModule:new(Dir), []}
			end,
	%% 将保持在clean.dot文件中的信息取出来(clean.dot文件为RabbitMQ系统崩溃的时候产生)
	case read_recovery_terms(Dir) of
		{false, Error} ->
			Fresh("failed to read recovery terms: ~p", [Error]);
		{true, Terms} ->
			%% 拿到RabbitMQ崩溃的时候，存储的所有客户端标识
			RecClientRefs  = proplists:get_value(client_refs, Terms, []),
			%% 拿到RabbitMQ崩溃的时候，存储的消息索引的处理模块名字
			RecIndexModule = proplists:get_value(index_module, Terms),
			%% 如果消息存储服务器自己存的客户端标识和所有消息队列传入的客户端标识相等，同时消息索引处理模块相等，则通过消息索引模块进行恢复操作
			case (lists:sort(ClientRefs) =:= lists:sort(RecClientRefs)
					  andalso IndexModule =:= RecIndexModule) of
				%% 将msg_store_index.ets文件中的消息索引信息恢复到rabbit_msg_store_ets_index ETS表(msg_store_index.ets为RabbitMQ系统崩溃的时候将消息的索引信息直接保持到该文件中)
				true  -> case IndexModule:recover(Dir) of
							 {ok, IndexState1} ->
								 {true, IndexState1, ClientRefs};
							 {error, Error} ->
								 Fresh("failed to recover index: ~p", [Error])
						 end;
				false -> Fresh("recovery terms differ from present", [])
			end
	end.


%% 将tuple的列表信息存储到clean.dot文件中(当RabbitMQ系统崩溃的时候)
store_recovery_terms(Terms, Dir) ->
	rabbit_file:write_term_file(filename:join(Dir, ?CLEAN_FILENAME), Terms).


%% 将保持在clean.dot文件中的信息取出来(clean.dot文件为RabbitMQ系统崩溃的时候产生)
read_recovery_terms(Dir) ->
	Path = filename:join(Dir, ?CLEAN_FILENAME),
	case rabbit_file:read_term_file(Path) of
		{ok, Terms}    -> case file:delete(Path) of
							  ok             -> {true,  Terms};
							  {error, Error} -> {false, Error}
						  end;
		{error, Error} -> {false, Error}
	end.


%% 将file_summary的ETS表中的所有数据存储到file_summary.ets这个文件中，等到RabbitMQ系统重启的时候会将该文件中的数据直接读取到file_summary ETS表
%% 即将当前存储服务器对应的磁盘文件ETS表转化为磁盘文件存储到当前存储服务器对应的磁盘目录下，等RabbitMQ系统恢复重启，则将数据直接从当前磁盘文件恢复为file_summary ETS表
store_file_summary(Tid, Dir) ->
	ok = ets:tab2file(Tid, filename:join(Dir, ?FILE_SUMMARY_FILENAME),
					  [{extended_info, [object_count]}]).


%% 如果没有tmp结尾的临时文件进行恢复数据，则只是创建rabbit_msg_store_file_summary ETS表
recover_file_summary(false, _Dir) ->
	%% TODO: the only reason for this to be an *ordered*_set is so
	%% that a) maybe_compact can start a traversal(遍历) from the eldest
	%% file, and b) build_index in fast recovery mode can easily
	%% identify the current file. It's awkward(尴尬的) to have both that
	%% odering and the left/right pointers in the entries - replacing
	%% the former with some additional bit of state would be easy, but
	%% ditching the latter would be neater.
	%% 创建rabbit_msg_store_file_summary ETS表
	{false, ets:new(rabbit_msg_store_file_summary,
					[ordered_set, public, {keypos, #file_summary.file}])};

%% 如果存在file_summary.ets文件(该文件是RabbitMQ系统崩溃的时候根据rabbit_msg_store_file_summary ETS创建的)，则将该文件恢复成rabbit_msg_store_file_summary ETS表
recover_file_summary(true, Dir) ->
	Path = filename:join(Dir, ?FILE_SUMMARY_FILENAME),
	case ets:file2tab(Path) of
		{ok, Tid}       -> ok = file:delete(Path),
						   {true, Tid};
		{error, _Error} -> recover_file_summary(false, Dir)
	end.


%% 根据消息队列中的操作项索引信息恢复存储服务器进程中的消息索引
count_msg_refs(Gen, Seed, State) ->
	case Gen(Seed) of
		finished ->
			ok;
		{_MsgId, 0, Next} ->
			count_msg_refs(Gen, Next, State);
		{MsgId, Delta, Next} ->
			%% 从消息操作项索引得到的消息ID如果不在消息服务器存储进程中的索引中，则将消息插入消息索引
			ok = case index_lookup(MsgId, State) of
					 not_found ->
						 index_insert(#msg_location { msg_id = MsgId,
													  file = undefined,
													  ref_count = Delta },
									  State);
					 #msg_location { ref_count = RefCount } = StoreEntry ->
						 NewRefCount = RefCount + Delta,
						 case NewRefCount of
							 %% 如果当前消息的引用变为0，则立刻将该消息索引删除掉
							 0 -> index_delete(MsgId, State);
							 %% 如果最新的消息索引引用不为0，则更新最新的消息索引信息
							 _ -> index_update(StoreEntry #msg_location {
																		 ref_count = NewRefCount },
																		State)
						 end
				 end,
			count_msg_refs(Gen, Next, State)
	end.


%% 将临时的存储文件放在正式的存储文件中的最后面(临时文件的后缀为rdt，正式存储文件的后缀为rdq)
recover_crashed_compactions(Dir) ->
	FileNames =    list_sorted_filenames(Dir, ?FILE_EXTENSION),
	TmpFileNames = list_sorted_filenames(Dir, ?FILE_EXTENSION_TMP),
	lists:foreach(
	  fun (TmpFileName) ->
			   NonTmpRelatedFileName =
				   filename:rootname(TmpFileName) ++ ?FILE_EXTENSION,
			   true = lists:member(NonTmpRelatedFileName, FileNames),
			   ok = recover_crashed_compaction(
					  Dir, TmpFileName, NonTmpRelatedFileName)
	  end, TmpFileNames),
	TmpFileNames == [].


%% 直接将临时文件的消息数据拷贝到实际的存储文件的最后面
recover_crashed_compaction(Dir, TmpFileName, NonTmpRelatedFileName) ->
	%% Because a msg can legitimately(合理的) appear multiple times(多次) in the
	%% same file, identifying(确定) the contents of the tmp file and where
	%% they came from is non-trivial. If we are recovering a crashed
	%% compaction then we will be rebuilding the index, which can cope(应付)
	%% with duplicates appearing. Thus the simplest and safest thing
	%% to do is to append the contents of the tmp file to its main
	%% file.
	{ok, TmpHdl}  = open_file(Dir, TmpFileName, ?READ_MODE),
	{ok, MainHdl} = open_file(Dir, NonTmpRelatedFileName,
							  ?READ_MODE ++ ?WRITE_MODE),
	{ok, _End} = file_handle_cache:position(MainHdl, eof),
	Size = filelib:file_size(form_filename(Dir, TmpFileName)),
	%% 直接将临时文件的消息数据拷贝到实际的存储文件的最后面
	{ok, Size} = file_handle_cache:copy(TmpHdl, MainHdl, Size),
	%% 关闭正确的消息存储文件句柄
	ok = file_handle_cache:close(MainHdl),
	%% 删除临时文件
	ok = file_handle_cache:delete(TmpHdl),
	ok.


%% 扫描消息存储文件的信息，将存储在磁盘文件中的二进制消息信息按照存储的规则读取出来
scan_file_for_valid_messages(Dir, FileName) ->
	case open_file(Dir, FileName, ?READ_MODE) of
		{ok, Hdl}       -> Valid = rabbit_msg_file:scan(
									 Hdl, filelib:file_size(
									   form_filename(Dir, FileName)),
									 fun scan_fun/2, []),
						   ok = file_handle_cache:close(Hdl),
						   Valid;
		{error, enoent} -> {ok, [], 0};
		{error, Reason} -> {error, {unable_to_scan_file, FileName, Reason}}
	end.


scan_fun({MsgId, TotalSize, Offset, _Msg}, Acc) ->
	[{MsgId, TotalSize, Offset} | Acc].

%% Takes the list in *ascending* order (i.e. eldest message
%% first). This is the opposite of what scan_file_for_valid_messages
%% produces. The list of msgs that is produced is youngest first.
%% 找到L列表中消息不连续的地方
drop_contiguous_block_prefix(L) -> drop_contiguous_block_prefix(L, 0).


drop_contiguous_block_prefix([], ExpectedOffset) ->
	{ExpectedOffset, []};

drop_contiguous_block_prefix([#msg_location { offset = ExpectedOffset,
											  total_size = TotalSize } | Tail],
							 ExpectedOffset) ->
	ExpectedOffset1 = ExpectedOffset + TotalSize,
	drop_contiguous_block_prefix(Tail, ExpectedOffset1);

drop_contiguous_block_prefix(MsgsAfterGap, ExpectedOffset) ->
	{ExpectedOffset, MsgsAfterGap}.


%% 创建索引信息
%% 如果通过中断信息恢复成功，则只统计所有消息的大小
build_index(true, _StartupFunState,
			State = #msstate { file_summary_ets = FileSummaryEts }) ->
	ets:foldl(
	  fun (#file_summary { valid_total_size = ValidTotalSize,
						   file_size        = FileSize,
						   file             = File },
		   {_Offset, State1 = #msstate { sum_valid_data = SumValid,
										 sum_file_size  = SumFileSize }}) ->
			   %% 更新当前消息存储服务器自己存储的消息的大小(最大的消息文件名数字是当前存储服务器进程操作的文件)
			   {FileSize, State1 #msstate {
										   sum_valid_data = SumValid + ValidTotalSize,
										   sum_file_size  = SumFileSize + FileSize,
										   current_file   = File }}
	  end, {0, State}, FileSummaryEts);

%% 没有恢复过消息，则需要创建一次索引信息(执行到此处是因为RabbitMQ出现特殊中断情况，比如说断电，服务器爆炸等情况)
build_index(false, {MsgRefDeltaGen, MsgRefDeltaGenInit},
			State = #msstate { dir = Dir }) ->
	%% 根据消息队列中的操作项索引信息恢复存储服务器进程中的消息索引
	ok = count_msg_refs(MsgRefDeltaGen, MsgRefDeltaGenInit, State),
	%% 启动收集进程
	{ok, Pid} = gatherer:start_link(),
	%% 得到所有的rdq文件名(通过文件名的数字进行排序)
	case [filename_to_num(FileName) ||
			FileName <- list_sorted_filenames(Dir, ?FILE_EXTENSION)] of
		[]     -> build_index(Pid, undefined, [State #msstate.current_file],
							  State);
		%% 将所有存储在磁盘中的消息磁盘文件从磁盘文件中把消息信息读取出来，然后存储在内存中
		Files  -> {Offset, State1} = build_index(Pid, undefined, Files, State),
				  {Offset, lists:foldl(fun delete_file_if_empty/2,
									   State1, Files)}
	end.


%% 持久化文件目录下面没有rdq后缀的文件类型
build_index(Gatherer, Left, [],
			State = #msstate { file_summary_ets = FileSummaryEts,
							   sum_valid_data   = SumValid,
							   sum_file_size    = SumFileSize }) ->
	case gatherer:out(Gatherer) of
		%% 如果收集进程中的数据为空
		empty ->
			%% 取消关联搜集进程
			unlink(Gatherer),
			%% 停止搜集进程
			ok = gatherer:stop(Gatherer),
			%% 根据磁盘文件名删除消息索引(该过程主要是删除那些存在于消息队列索引中，但是不存在持久化文件中的消息，将这些消息的索引从ETS中删除)
			ok = index_delete_by_file(undefined, State),
			%% 得到最后一个索引磁盘文件的偏移大小
			Offset = case ets:lookup(FileSummaryEts, Left) of
						 []                                       -> 0;
						 [#file_summary { file_size = FileSize }] -> FileSize
					 end,
			%% 磁盘文件中的最后一个文件作为当前操作的磁盘文件名
			{Offset, State #msstate { current_file = Left }};
		{value, #file_summary { valid_total_size = ValidTotalSize,
								file_size = FileSize } = FileSummary} ->
			%% 将从磁盘文件的信息插入ETS表
			true = ets:insert_new(FileSummaryEts, FileSummary),
			%% 继续从搜集进程中读取磁盘文件的信息，知道搜集进程为空
			build_index(Gatherer, Left, [],
						State #msstate {
										sum_valid_data = SumValid + ValidTotalSize,
										sum_file_size  = SumFileSize + FileSize })
	end;

%% 持久化文件目录下面有rdq后缀的文件类型，然后恢复这些磁盘文件的消息
build_index(Gatherer, Left, [File | Files], State) ->
	ok = gatherer:fork(Gatherer),
	%% 将创建索引信息的任务提交给工作池进程
	ok = worker_pool:submit_async(
		   fun () -> build_index_worker(Gatherer, State,
										Left, File, Files)
		   end),
	%% 扫描下一个消息存储的磁盘文件
	build_index(Gatherer, File, Files, State).


%% 该函数是通过工作池进程来执行，该函数的作用是扫描单个磁盘文件中的消息信息(单个磁盘文件的恢复操作接口)
build_index_worker(Gatherer, State = #msstate { dir = Dir },
				   Left, File, Files) ->
	%% 扫描消息存储文件的信息，将存储在磁盘文件中的二进制消息信息按照存储的规则读取出来
	{ok, Messages, FileSize} =
		scan_file_for_valid_messages(Dir, filenum_to_name(File)),
	{ValidMessages, ValidTotalSize} =
		lists:foldl(
		  fun (Obj = {MsgId, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
				   %% 消息索引已经通过上面消息在消息队列中的位置，然后创建过消息索引msg_location
				   case index_lookup(MsgId, State) of
					   #msg_location { file = undefined } = StoreEntry ->
						   %% 把从磁盘文件中的读取出来的消息信息更新到消息索引中(更新消息索引的文件名字，在索引磁盘文件中的偏移开始位置，当前消息占用的大小信息)
						   ok = index_update(StoreEntry #msg_location {
																	   file = File, offset = Offset,
																	   total_size = TotalSize },
																	  State),
						   {[Obj | VMAcc], VTSAcc + TotalSize};
					   _ ->
						   {VMAcc, VTSAcc}
				   end
		  end, {[], 0}, Messages),
	{Right, FileSize1} =
		case Files of
			%% if it's the last file, we'll truncate to remove any
			%% rubbish above the last valid message. This affects the
			%% file size.
			[]    -> {undefined, case ValidMessages of
									 [] -> 0;
									 %% 得到最后一个文件的磁盘文件的大小
									 _  -> {_MsgId, TotalSize, Offset} =
											   lists:last(ValidMessages),
										   Offset + TotalSize
								 end};
			[F | _] -> {F, FileSize}
		end,
	%% 向收集进程发送磁盘文件信息，然后本消息存储文件进程做后续相关的处理(异步向搜集进程发送磁盘文件信息)
	ok = gatherer:in(Gatherer, #file_summary {
											  file             = File,
											  valid_total_size = ValidTotalSize,
											  left             = Left,
											  right            = Right,
											  file_size        = FileSize1,
											  locked           = false,
											  readers          = 0 }),
	%% 通知搜集进程该单个磁盘文件搜集完毕
	ok = gatherer:finish(Gatherer).

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation -- internal(内部垃圾回收，文件合并，聚合相关接口)
%%----------------------------------------------------------------------------
%% 更新所有文件的统计信息，并判断是否需要创建一个新的持久化文件
maybe_roll_to_new_file(
  Offset,
  State = #msstate { dir                 = Dir,
					 current_file_handle = CurHdl,
					 current_file        = CurFile,
					 file_summary_ets    = FileSummaryEts,
					 cur_file_cache_ets  = CurFileCacheEts,
					 file_size_limit     = FileSizeLimit })
  %% 当前文件的大小已经超过单个存储文件的上限，因此需要创建新的文件来存储消息
  when Offset >= FileSizeLimit ->
	%% 这里是将所有需要被confirm的消息按所属的queue进行分组，然后通知对应的消息队列已经得到confirm的消息
	State1 = internal_sync(State),
	%% 关闭当前文件的句柄
	ok = file_handle_cache:close(CurHdl),
	%% 得到新的文件的文件名
	NextFile = CurFile + 1,
	%% 打开新的新的存储文件的文件句柄
	{ok, NextHdl} = open_file(Dir, filenum_to_name(NextFile), ?WRITE_MODE),
	%% 向file_summary ETS表插入新的存储文件的信息
	true = ets:insert_new(FileSummaryEts, #file_summary {
														 file             = NextFile,
														 valid_total_size = 0,
														 left             = CurFile,
														 right            = undefined,
														 file_size        = 0,
														 locked           = false,
														 readers          = 0 }),
	%% 更新就的存储的文件信息file_summary中的右文件是新打开的文件名
	true = ets:update_element(FileSummaryEts, CurFile,
							  {#file_summary.right, NextFile}),
	%% 删除当前文件的缓存ETS中CacheRefCount为0的数据，因为这些消息已经存储到老的磁盘文件中
	true = ets:match_delete(CurFileCacheEts, {'_', '_', 0}),
	%% 磁盘存储文件中有可能在消息被消费后，需要将存储的磁盘文件进行合并
	maybe_compact(State1 #msstate { current_file_handle = NextHdl,
									current_file        = NextFile });

maybe_roll_to_new_file(_, State) ->
	State.


%% 磁盘存储文件中有可能在消息被消费后，需要将存储的磁盘文件进行合并
maybe_compact(State = #msstate { sum_valid_data        = SumValid,
								 sum_file_size         = SumFileSize,
								 gc_pid                = GCPid,
								 pending_gc_completion = Pending,
								 file_summary_ets      = FileSummaryEts,
								 file_size_limit       = FileSizeLimit })
  %% 当所有文件中的垃圾消息（已经被删除的消息）比例大于阈值（GARBAGE_FRACTION = 0.5）时，会触发文件合并操作（至少有三个文件存在的情况下），以提高磁盘利用率。
  when SumFileSize > 2 * FileSizeLimit andalso
		   (SumFileSize - SumValid) / SumFileSize > ?GARBAGE_FRACTION ->
	%% TODO: the algorithm here is sub-optimal - it may result in a
	%% complete traversal(遍历) of FileSummaryEts.
	First = ets:first(FileSummaryEts),
	%% 如果磁盘文件信息为空，或者垃圾回收大小操作4个，表示当前垃圾回收进程很繁忙，则先不进行文件的合并
	case First =:= '$end_of_table' orelse
			 orddict:size(Pending) >= ?MAXIMUM_SIMULTANEOUS_GC_FILES of
		true ->
			State;
		false ->
			%% 查找能够合并的文件
			case find_files_to_combine(FileSummaryEts, FileSizeLimit,
									   ets:lookup(FileSummaryEts, First)) of
				not_found ->
					State;
				{Src, Dst} ->
					%% 将目的文件和源文件等待垃圾回收队列初始化
					Pending1 = orddict_store(Dst, [],
											 orddict_store(Src, [], Pending)),
					%% 将目的文件和源文件在进程中缓存中的句柄关闭
					State1 = close_handle(Src, close_handle(Dst, State)),
					%% 将Src文件锁定
					true = ets:update_element(FileSummaryEts, Src,
											  {#file_summary.locked, true}),
					%% 将Dst文件锁定
					true = ets:update_element(FileSummaryEts, Dst,
											  {#file_summary.locked, true}),
					%% 通知垃圾回收进程进行磁盘文件的合并
					ok = rabbit_msg_store_gc:combine(GCPid, Src, Dst),
					%% 更新存储服务器进程等待垃圾回收队列字段
					State1 #msstate { pending_gc_completion = Pending1 }
			end
	end;

maybe_compact(State) ->
	State.


%% 查找能够合并的两个文件
find_files_to_combine(FileSummaryEts, FileSizeLimit,
					  [#file_summary { file             = Dst,
									   valid_total_size = DstValid,
									   right            = Src,
									   locked           = DstLocked }]) ->
	case Src of
		undefined ->
			not_found;
		_   ->
			%% 两个文件必须相连，但并不意味着文件名连续，且B必须在A的右边
			[#file_summary { file             = Src,
							 valid_total_size = SrcValid,
							 left             = Dst,
							 right            = SrcRight,
							 locked           = SrcLocked }] = Next =
																   ets:lookup(FileSummaryEts, Src),
			%% B的右边必须还有文件存在，也就是必须有三个文件存在
			case SrcRight of
				undefined -> not_found;
				%% A、B的有效数据之和必须小于一个文件的最大容量，也就是说两个文件可以合并成一个文件
				%% 两个文件中都有有效数据
				%% 两个文件都没有处于锁定状态（没有进行GC）
				_         -> case (DstValid + SrcValid =< FileSizeLimit) andalso
									  (DstValid > 0) andalso (SrcValid > 0) andalso
									  not (DstLocked orelse SrcLocked) of
								 true  -> {Src, Dst};
								 false -> find_files_to_combine(
											FileSummaryEts, FileSizeLimit, Next)
							 end
			end
	end.


%% 当索引磁盘中的有效数据为0，则立刻将该磁盘索引文件删除
delete_file_if_empty(File, State = #msstate { current_file = File }) ->
	State;
delete_file_if_empty(File, State = #msstate {
											 gc_pid                = GCPid,
											 file_summary_ets      = FileSummaryEts,
											 pending_gc_completion = Pending }) ->
	[#file_summary { valid_total_size = ValidData,
					 locked           = false }] =
		ets:lookup(FileSummaryEts, File),
	case ValidData of
		%% don't delete the file_summary_ets entry for File here
		%% because we could have readers which need to be able to
		%% decrement the readers count.
		%% 将当前磁盘文件锁住
		%% 如果File这个磁盘文件中有效的消息数据为0，则将当前文件删除掉
		0 -> true = ets:update_element(FileSummaryEts, File,
									   {#file_summary.locked, true}),
			 %% 通知垃圾回收进程进行删除操作
			 ok = rabbit_msg_store_gc:delete(GCPid, File),
			 %% 将等待该文件操作的字典清空
			 Pending1 = orddict_store(File, [], Pending),
			 %% 关闭掉存储服务器进程中缓存的该文件的文件句柄
			 close_handle(File,
						  State #msstate { pending_gc_completion = Pending1 });
		%% 如果当前磁盘文件在删除掉消息后有效的消息数据不为0，则什么都不做
		_ -> State
	end.


%% 存储文件删除后进行相关的操作
cleanup_after_file_deletion(File,
							#msstate { file_handles_ets = FileHandlesEts,
									   file_summary_ets = FileSummaryEts,
									   clients          = Clients }) ->
	%% Ensure that any clients that have open fhs to the file close
	%% them before using them again. This has to be done here (given
	%% it's done in the msg_store, and not the gc), and not when
	%% starting up the GC, because if done when starting up the GC,
	%% the client could find the close, and close and reopen the fh,
	%% whilst the GC is waiting for readers to disappear, before it's
	%% actually done the GC.
	true = mark_handle_to_close(Clients, FileHandlesEts, File, true),
	[#file_summary { left    = Left,
					 right   = Right,
					 locked  = true,
					 readers = 0 }] = ets:lookup(FileSummaryEts, File),
	%% We'll never delete the current file, so right is never undefined
	true = Right =/= undefined, %% ASSERTION
	%% 将该文件从自己的左右文件中删除掉，更新自己右边的存储文件的左边文件为改删除文件的左边文件
	true = ets:update_element(FileSummaryEts, Right,
							  {#file_summary.left, Left}),
	%% ensure the double linked list is maintained
	%% 将该文件从自己的左右文件中删除掉，更新自己左边的存储文件的右边文件为改删除文件的右边文件
	true = case Left of
			   undefined -> true; %% File is the eldest file (left-most)
			   _         -> ets:update_element(FileSummaryEts, Left,
											   {#file_summary.right, Right})
		   end,
	%% 将当前文件的信息删除掉
	true = ets:delete(FileSummaryEts, File),
	ok.

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation -- external
%%----------------------------------------------------------------------------
%% 判断当前存储的磁盘文件File是否有读取用户
has_readers(File, #gc_state { file_summary_ets = FileSummaryEts }) ->
	[#file_summary { locked = true, readers = Count }] =
		ets:lookup(FileSummaryEts, File),
	Count /= 0.


%% 合并存储消息的磁盘文件的实际操作函数(通过垃圾回收进程执行的实际的合并磁盘文件的操作)(文件的合并是大数字的文件合并到小数字的文件中)
combine_files(Source, Destination,
			  State = #gc_state { file_summary_ets = FileSummaryEts,
								  file_handles_ets = FileHandlesEts,
								  dir              = Dir,
								  msg_store        = Server }) ->
	%% 得到源文件的信息
	[#file_summary {
					readers          = 0,
					left             = Destination,
					valid_total_size = SourceValid,
					file_size        = SourceFileSize,
					locked           = true }] = ets:lookup(FileSummaryEts, Source),
	%% 得到目的文件的信息
	[#file_summary {
					readers          = 0,
					right            = Source,
					valid_total_size = DestinationValid,
					file_size        = DestinationFileSize,
					locked           = true }] = ets:lookup(FileSummaryEts, Destination),
	
	%% 根据源文件名组装磁盘存储文件名
	SourceName           = filenum_to_name(Source),
	%% 根据目的文件名组装磁盘存储文件名
	DestinationName      = filenum_to_name(Destination),
	%% 打开源文件句柄(以读的方式打开文件)
	{ok, SourceHdl}      = open_file(Dir, SourceName,
									 ?READ_AHEAD_MODE),
	%% 打开目的文件句柄(以读和写的方式打开文件)
	{ok, DestinationHdl} = open_file(Dir, DestinationName,
									 ?READ_AHEAD_MODE ++ ?WRITE_MODE),
	%% 得到两个文件总的有效数据长度
	TotalValidData = SourceValid + DestinationValid,
	%% if DestinationValid =:= DestinationContiguousTop then we don't
	%% need a tmp file
	%% if they're not equal, then we need to write out everything past
	%%   the DestinationContiguousTop to a tmp file then truncate(截断),
	%%   copy back in, and then copy over from Source
	%% otherwise we just truncate straight away and copy over from Source
	{DestinationWorkList, DestinationValid} =
		%% 加载目的消息存储的磁盘文件，将里面的消息解析出来和总的大小(从目标文件读取有效消息数据)
		load_and_vacuum_message_file(Destination, State),
	{DestinationContiguousTop, DestinationWorkListTail} =
		%% 检查从目标文件开始的连续有效消息区间
		drop_contiguous_block_prefix(DestinationWorkList),
	%% DestinationWorkListTail表示目的文件中后续不连续的消息队列
	case DestinationWorkListTail of
		%% 如果目标文件中的所有有效消息数据都是连续，只要把目标文件扩展到合并后的大小，并将写入位置定位到目标文件的最后
		[] -> ok = truncate_and_extend_file(
					 DestinationHdl, DestinationContiguousTop, TotalValidData);
		%% 如果目标文件中有空洞，则：1）将除了第一个连续区间内的消息以外的所有有效消息先读到一个临时文件；2）把目标文件扩展到合并后的大小；3）把临时文件中的有效数据拷贝到目标文件
		_  -> %% 得到临时文件的名字,临时文件后缀为rdt
			  Tmp = filename:rootname(DestinationName) ++ ?FILE_EXTENSION_TMP,
			  %% 打开临时文件，得到临时文件的句柄
			  {ok, TmpHdl} = open_file(Dir, Tmp, ?READ_AHEAD_MODE ++ ?WRITE_MODE),
			  %% 将目的文件中后面不连续的消息拷贝到同名后缀rdt的文件里
			  ok = copy_messages(
					 DestinationWorkListTail, DestinationContiguousTop,
					 DestinationValid, DestinationHdl, TmpHdl, Destination,
					 State),
			  %% 得到tmp文件中的实际消息的大小
			  TmpSize = DestinationValid - DestinationContiguousTop,
			  %% so now Tmp contains everything we need to salvage(抢救)
			  %% from Destination, and index_state has been updated to
			  %% reflect the compaction of Destination so truncate
			  %% Destination and copy from Tmp back to the end
			  {ok, 0} = file_handle_cache:position(TmpHdl, 0),
			  %% 截断并且扩张文件
			  ok = truncate_and_extend_file(
					 DestinationHdl, DestinationContiguousTop, TotalValidData),
			  %% 将tmp文件中的后续消息再拷贝到目的文件中，实现了截取掉后续无效的消息
			  {ok, TmpSize} =
				  file_handle_cache:copy(TmpHdl, DestinationHdl, TmpSize),
			  %% position in DestinationHdl should now be DestinationValid
			  %% 同步目的文件
			  ok = file_handle_cache:sync(DestinationHdl),
			  ok = file_handle_cache:delete(TmpHdl)
	end,
	%% 加载源消息存储的磁盘文件，将里面的消息解析出来和总的大小(从源文件中加载所有有效消息数据)
	{SourceWorkList, SourceValid} = load_and_vacuum_message_file(Source, State),
	%% 将源文件中的所有有效数据拷贝到目标文件
	ok = copy_messages(SourceWorkList, DestinationValid, TotalValidData,
					   SourceHdl, DestinationHdl, Destination, State),
	%% tidy up
	%% 关闭目的文件的句柄
	ok = file_handle_cache:close(DestinationHdl),
	%% 关闭源文件的句柄
	ok = file_handle_cache:close(SourceHdl),
	
	%% don't update dest.right, because it could be changing at the
	%% same time
	%% 更新目的文件的总的文件大小信息
	true = ets:update_element(
			 FileSummaryEts, Destination,
			 [{#file_summary.valid_total_size, TotalValidData},
			  {#file_summary.file_size,        TotalValidData}]),
	
	%% 得到两个文件中无效的数据大小
	Reclaimed = SourceFileSize + DestinationFileSize - TotalValidData,
	%% 再次循环通知存储服务器进程看是否有能够继续合并的文件
	gen_server2:cast(Server, {combine_files, Source, Destination, Reclaimed}),
	safe_file_delete_fun(Source, Dir, FileHandlesEts).


%% 磁盘文件的删除
delete_file(File, State = #gc_state { file_summary_ets = FileSummaryEts,
									  file_handles_ets = FileHandlesEts,
									  dir              = Dir,
									  msg_store        = Server }) ->
	[#file_summary { valid_total_size = 0,
					 locked           = true,
					 file_size        = FileSize,
					 readers          = 0 }] = ets:lookup(FileSummaryEts, File),
	%% 判断加载该文件上的消息为空
	{[], 0} = load_and_vacuum_message_file(File, State),
	gen_server2:cast(Server, {delete_file, File, FileSize}),
	safe_file_delete_fun(File, Dir, FileHandlesEts).


%% 加载消息存储的磁盘文件，将里面的消息解析出来和总的大小(从目标文件读取有效消息数据)
load_and_vacuum_message_file(File, #gc_state { dir          = Dir,
											   index_module = Index,
											   index_state  = IndexState }) ->
	%% Messages here will be end-of-file at start-of-list
	%% 从磁盘文件中将所有的消息读取出来
	{ok, Messages, _FileSize} =
		scan_file_for_valid_messages(Dir, filenum_to_name(File)),
	%% foldl will reverse so will end up with msgs in ascending offset order
	lists:foldl(
	  fun ({MsgId, TotalSize, Offset}, Acc = {List, Size}) ->
			   case Index:lookup(MsgId, IndexState) of
				   %% 如果索引计数为0则将消息的索引删除
				   #msg_location { file = File, total_size = TotalSize,
								   offset = Offset, ref_count = 0 } = Entry ->
					   ok = Index:delete_object(Entry, IndexState),
					   Acc;
				   %% 如果引用不为0的索引则表示该消息还没有被删除
				   #msg_location { file = File, total_size = TotalSize,
								   offset = Offset } = Entry ->
					   {[ Entry | List ], TotalSize + Size};
				   _ ->
					   Acc
			   end
	  end, {[], 0}, Messages).


%% 实际的拷贝函数
copy_messages(WorkList, InitOffset, FinalOffset, SourceHdl, DestinationHdl,
			  Destination, #gc_state { index_module = Index,
									   index_state  = IndexState }) ->
	%% 将SourceHdl句柄文件中BlockStart开始的字节到BlockEnd的字节拷贝到DestinationHdl句柄文件的后面
	Copy = fun ({BlockStart, BlockEnd}) ->
					BSize = BlockEnd - BlockStart,
					{ok, BlockStart} =
						file_handle_cache:position(SourceHdl, BlockStart),
					{ok, BSize} =
						file_handle_cache:copy(SourceHdl, DestinationHdl, BSize)
		   end,
	%% 将WorkList列表中的消息复制到DestinationHdl文件句柄中
	case
		lists:foldl(
		  fun (#msg_location { msg_id = MsgId, offset = Offset,
							   total_size = TotalSize },
			   {CurOffset, Block = {BlockStart, BlockEnd}}) ->
				   %% CurOffset is in the DestinationFile.
				   %% Offset, BlockStart and BlockEnd are in the SourceFile
				   %% update MsgLocation to reflect(反映) change of file and offset
				   %% 更新消息的索引信息
				   ok = Index:update_fields(MsgId,
											[{#msg_location.file, Destination},
											 {#msg_location.offset, CurOffset}],
											IndexState),
				   {CurOffset + TotalSize,
					case BlockEnd of
						undefined ->
							%% base case, called only for the first list elem
							{Offset, Offset + TotalSize};
						Offset ->
							%% 源消息存储文件没有跳过消息(直到找到不连续的消息，则将之前连续的消息拷贝到目的文件，然后再继续查找)
							%% extend the current block because the
							%% next msg follows straight on
							{BlockStart, BlockEnd + TotalSize};
						_ ->
							%% found a gap(缺口), so actually do the work for
							%% the previous block
							Copy(Block),
							{Offset, Offset + TotalSize}
					end}
		  end, {InitOffset, {undefined, undefined}}, WorkList) of
		{FinalOffset, Block} ->
			case WorkList of
				[] -> ok;
				_  -> Copy(Block), %% do the last remaining block
					  ok = file_handle_cache:sync(DestinationHdl)
			end;
		{FinalOffsetZ, _Block} ->
			{gc_error, [{expected, FinalOffset},
						{got, FinalOffsetZ},
						{destination, Destination}]}
	end.


force_recovery(BaseDir, Store) ->
	Dir = filename:join(BaseDir, atom_to_list(Store)),
	case file:delete(filename:join(Dir, ?CLEAN_FILENAME)) of
		ok              -> ok;
		{error, enoent} -> ok
	end,
	recover_crashed_compactions(BaseDir),
	ok.


foreach_file(D, Fun, Files) ->
	[ok = Fun(filename:join(D, File)) || File <- Files].


foreach_file(D1, D2, Fun, Files) ->
	[ok = Fun(filename:join(D1, File), filename:join(D2, File)) || File <- Files].


transform_dir(BaseDir, Store, TransformFun) ->
	Dir = filename:join(BaseDir, atom_to_list(Store)),
	TmpDir = filename:join(Dir, ?TRANSFORM_TMP),
	TransformFile = fun (A, B) -> transform_msg_file(A, B, TransformFun) end,
	CopyFile = fun (Src, Dst) -> {ok, _Bytes} = file:copy(Src, Dst), ok end,
	case filelib:is_dir(TmpDir) of
		true  -> throw({error, transform_failed_previously});
		false -> FileList = list_sorted_filenames(Dir, ?FILE_EXTENSION),
				 foreach_file(Dir, TmpDir, TransformFile,     FileList),
				 foreach_file(Dir,         fun file:delete/1, FileList),
				 foreach_file(TmpDir, Dir, CopyFile,          FileList),
				 foreach_file(TmpDir,      fun file:delete/1, FileList),
				 ok = file:del_dir(TmpDir)
	end.


transform_msg_file(FileOld, FileNew, TransformFun) ->
	ok = rabbit_file:ensure_parent_dirs_exist(FileNew),
	{ok, RefOld} = file_handle_cache:open(FileOld, [raw, binary, read], []),
	{ok, RefNew} = file_handle_cache:open(FileNew, [raw, binary, write],
										  [{write_buffer,
											?HANDLE_CACHE_BUFFER_SIZE}]),
	{ok, _Acc, _IgnoreSize} =
		rabbit_msg_file:scan(
		  RefOld, filelib:file_size(FileOld),
		  fun({MsgId, _Size, _Offset, BinMsg}, ok) ->
				  {ok, MsgNew} = case binary_to_term(BinMsg) of
									 <<>> -> {ok, <<>>};  %% dying client marker
									 Msg  -> TransformFun(Msg)
								 end,
				  {ok, _} = rabbit_msg_file:append(RefNew, MsgId, MsgNew),
				  ok
		  end, ok),
	ok = file_handle_cache:close(RefOld),
	ok = file_handle_cache:close(RefNew),
	ok.
