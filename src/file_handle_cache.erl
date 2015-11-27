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

-module(file_handle_cache).

%% 主要作用是管理每个进程拥有的文件描述符数量，确保当前RabbitMQ系统的文件描述符不超过上限

%% A File Handle Cache
%%
%% This extends a subset of the functionality of the Erlang file
%% module. In the below, we use "file handle" to specifically refer to
%% file handles, and "file descriptor" to refer to descriptors which
%% are not file handles, e.g. sockets.
%%
%% Some constraints
%% 1) This supports one writer, multiple readers per file. Nothing
%% else.
%% 2) Do not open the same file from different processes. Bad things
%% may happen, especially for writes.
%% 3) Writes are all appends. You cannot write to the middle of a
%% file, although you can truncate and then append if you want.
%% 4) There are read and write buffers. Feel free to use the read_ahead
%% mode, but beware of the interaction between that buffer and the write
%% buffer.
%%
%% Some benefits
%% 1) You do not have to remember to call sync before close
%% 2) Buffering is much more flexible than with the plain file module,
%% and you can control when the buffer gets flushed out. This means
%% that you can rely on reads-after-writes working, without having to
%% call the expensive sync.
%% 3) Unnecessary calls to position and sync get optimised out.
%% 4) You can find out what your 'real' offset is, and what your
%% 'virtual' offset is (i.e. where the hdl really is, and where it
%% would be after the write buffer is written out).
%%
%% There is also a server component which serves to limit the number
%% of open file descriptors. This is a hard limit: the server
%% component will ensure that clients do not have more file
%% descriptors open than it's configured to allow.
%%
%% On open, the client requests permission from the server to open the
%% required number of file handles. The server may ask the client to
%% close other file handles that it has open, or it may queue the
%% request and ask other clients to close file handles they have open
%% in order to satisfy the request. Requests are always satisfied in
%% the order they arrive, even if a latter request (for a small number
%% of file handles) can be satisfied before an earlier request (for a
%% larger number of file handles). On close, the client sends a
%% message to the server. These messages allow the server to keep
%% track of the number of open handles. The client also keeps a
%% gb_tree which is updated on every use of a file handle, mapping the
%% time at which the file handle was last used (timestamp) to the
%% handle. Thus the smallest key in this tree maps to the file handle
%% that has not been used for the longest amount of time. This
%% smallest key is included in the messages to the server. As such,
%% the server keeps track of when the least recently used file handle
%% was used *at the point of the most recent open or close* by each
%% client.
%%
%% Note that this data can go very out of date, by the client using
%% the least recently used handle.
%%
%% When the limit is exceeded (i.e. the number of open file handles is
%% at the limit and there are pending 'open' requests), the server
%% calculates the average age of the last reported least recently used
%% file handle of all the clients. It then tells all the clients to
%% close any handles not used for longer than this average, by
%% invoking the callback the client registered. The client should
%% receive this message and pass it into
%% set_maximum_since_use/1. However, it is highly possible this age
%% will be greater than the ages of all the handles the client knows
%% of because the client has used its file handles in the mean
%% time. Thus at this point the client reports to the server the
%% current timestamp at which its least recently used file handle was
%% last used. The server will check two seconds later that either it
%% is back under the limit, in which case all is well again, or if
%% not, it will calculate a new average age. Its data will be much
%% more recent now, and so it is very likely that when this is
%% communicated to the clients, the clients will close file handles.
%% (In extreme cases, where it's very likely that all clients have
%% used their open handles since they last sent in an update, which
%% would mean that the average will never cause any file handles to
%% be closed, the server can send out an average age of 0, resulting
%% in all available clients closing all their file handles.)
%%
%% Care is taken to ensure that (a) processes which are blocked
%% waiting for file descriptors to become available are not sent
%% requests to close file handles; and (b) given it is known how many
%% file handles a process has open, when the average age is forced to
%% 0, close messages are only sent to enough processes to release the
%% correct number of file handles and the list of processes is
%% randomly shuffled. This ensures we don't cause processes to
%% needlessly close file handles, and ensures that we don't always
%% make such requests of the same processes.
%%
%% The advantage of this scheme is that there is only communication
%% from the client to the server on open, close, and when in the
%% process of trying to reduce file handle usage. There is no
%% communication from the client to the server on normal file handle
%% operations. This scheme forms a feed-back loop - the server does
%% not care which file handles are closed, just that some are, and it
%% checks this repeatedly when over the limit.
%%
%% Handles which are closed as a result of the server are put into a
%% "soft-closed" state in which the handle is closed (data flushed out
%% and sync'd first) but the state is maintained. The handle will be
%% fully reopened again as soon as needed, thus users of this library
%% do not need to worry about their handles being closed by the server
%% - reopening them when necessary is handled transparently.
%%
%% The server also supports obtain, release and transfer. obtain/{0,1}
%% blocks until a file descriptor is available, at which point the
%% requesting process is considered to 'own' more descriptor(s).
%% release/{0,1} is the inverse operation and releases previously obtained
%% descriptor(s). transfer/{1,2} transfers ownership of file descriptor(s)
%% between processes. It is non-blocking. Obtain has a
%% lower limit, set by the ?OBTAIN_LIMIT/1 macro. File handles can use
%% the entire limit, but will be evicted by obtain calls up to the
%% point at which no more obtain calls can be satisfied by the obtains
%% limit. Thus there will always be some capacity available for file
%% handles. Processes that use obtain are never asked to return them,
%% and they are not managed in any way by the server. It is simply a
%% mechanism to ensure that processes that need file descriptors such
%% as sockets can do so in such a way that the overall number of open
%% file descriptors is managed.
%%
%% The callers of register_callback/3, obtain, and the argument of
%% transfer are monitored, reducing the count of handles in use
%% appropriately when the processes terminate.

-behaviour(gen_server2).

-export([register_callback/3]).
-export([open/3, close/1, read/2, append/2, needs_sync/1, sync/1, position/2,
		 truncate/1, current_virtual_offset/1, current_raw_offset/1, flush/1,
		 copy/3, set_maximum_since_use/1, delete/1, clear/1]).
-export([obtain/0, obtain/1, release/0, release/1, transfer/1, transfer/2,
		 set_limit/1, get_limit/0, info_keys/0, with_handle/1, with_handle/2,
		 info/0, info/1]).
-export([ulimit/0]).

-export([start_link/0, start_link/2, init/1, handle_call/3, handle_cast/2,
		 handle_info/2, terminate/2, code_change/3, prioritise_cast/3]).

-define(SERVER, ?MODULE).
-define(RESERVED_FOR_OTHERS, 100).

-define(FILE_HANDLES_LIMIT_OTHER, 1024).
-define(FILE_HANDLES_CHECK_INTERVAL, 2000).

-define(OBTAIN_LIMIT(LIMIT), trunc((LIMIT * 0.9) - 2)).				%% 根据LIMIT个能够打开的文件句柄上限得到当前RabbitMQ系统中能够obtain上限
-define(CLIENT_ETS_TABLE, file_handle_cache_client).				%% 保存客户端状态的ETS表名
-define(ELDERS_ETS_TABLE, file_handle_cache_elders).

%%----------------------------------------------------------------------------
%% 一个文件的读写状态数据结构
-record(file,
		{ reader_count,					%% 文件的读者数量
		  has_writer					%% 判断是否有写数据者
		}).

%% 一个文件的处理句柄数据结构
-record(handle,
		{ hdl,							%% prim_file打开文件返回的文件句柄
		  offset,						%% 当前实际的读取到文件的字符偏移量，实际就是当前读取到的文件字符位置
		  is_dirty,
		  write_buffer_size,			%% 
		  write_buffer_size_limit,		%% 当前句柄写缓存的最大限制
		  write_buffer,					%% 当前句柄要写的缓存
		  read_buffer,					%% 上次读取的字符串缓存
		  read_buffer_pos,				%% 上次读取的时候实际读取到的字符串pos位置
		  read_buffer_rem,        		%% Num of bytes from pos to end(保存的是num个字节从pos到文件结束)(上次读取的字符串数量 - read_buffer_pos，即上次读取的数据还剩余的数据大小)
		  read_buffer_size,       		%% Next size of read buffer to use(下一个要读取的缓存大小)
		  read_buffer_size_limit, 		%% Max size of read buffer to use(最大读取缓存的大小，根据参数设置的)
		  read_buffer_usage,      		%% Bytes we have read from it, for tuning(上次读取的数据中已经读取的数据大小)
		  at_eof,						%% 是否达到文件最后
		  path,							%% 当前操作文件的绝对路径
		  mode,							%% 当前操作文件的操作类型列表
		  options,						%% 当前句柄操作配置的参数
		  is_write,						%% 当前句柄是否是写操作
		  is_read,						%% 当前句柄是否是读操作
		  last_used_at					%% 最后一次使用句柄的时间
		}).

%% file_handle_cache进程的状态数据结构
-record(fhc_state,
		{ elders,						%% 记录客户端对应最老的句柄最后一次的使用时间的ETS表名
		  limit,						%% 总体文件描述符的上限
		  open_count,					%% 当前RabbitMQ系统文件句柄打开的个数
		  open_pending,					%% 当前RabbitMQ系统等待打开文件句柄的等待队列
		  obtain_limit, %%socket		%% obtain类型的描述符上限
		  obtain_count_socket,			%% 当前RabbitMQ系统打开的Socket的数量
		  obtain_count_file,			%% 当前RabbitMQ系统操作文件的数量
		  obtain_pending_socket,		%% 当前RabbitMQ系统等待打开socket的等待队列
		  obtain_pending_file,			%% 当前RabbitMQ等待打开文件的等待队列
		  clients,						%% 存储客户端信息的ETS表名
		  timer_ref,					%% 较少文件描述符后下次检查的定时器，用来判断是否还需要减少
		  alarm_set,					%% 文件描述符超过上限的报警函数
		  alarm_clear					%% 文件描述符从超过上限到为超过上限的警报清除函数
		}).

%% client客户端状态数据结构
-record(cstate,
		{ pid,							%% 客户端进程Pid
		  callback,						%% 客户端进程注册的回调函数
		  opened,						%% 客户端打开的文件句柄数量
		  obtained_socket,				%% 客户端打开的socket数量
		  obtained_file,				%% 客户端操作文件的数量
		  blocked,						%% 客户端当前是否阻塞的字段
		  pending_closes				%% 客户端在等待关闭的数量
		}).

%% 等待的状态数据结构
-record(pending,
		{ kind,							%% 操作类型(open，file，socket)
		  pid,							%% 客户端进程Pid
		  requested,					%% 当前请求要打开的数量
		  from							%% 接收返回结果的进程Pid
		}).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ref() :: any()).
-type(ok_or_error() :: 'ok' | {'error', any()}).
-type(val_or_error(T) :: {'ok', T} | {'error', any()}).
-type(position() :: ('bof' | 'eof' | non_neg_integer() |
                     {('bof' |'eof'), non_neg_integer()} |
                     {'cur', integer()})).
-type(offset() :: non_neg_integer()).

-spec(register_callback/3 :: (atom(), atom(), [any()]) -> 'ok').
-spec(open/3 ::
        (file:filename(), [any()],
         [{'write_buffer', (non_neg_integer() | 'infinity' | 'unbuffered')} |
          {'read_buffer', (non_neg_integer() | 'unbuffered')}])
        -> val_or_error(ref())).
-spec(close/1 :: (ref()) -> ok_or_error()).
-spec(read/2 :: (ref(), non_neg_integer()) ->
                     val_or_error([char()] | binary()) | 'eof').
-spec(append/2 :: (ref(), iodata()) -> ok_or_error()).
-spec(sync/1 :: (ref()) ->  ok_or_error()).
-spec(position/2 :: (ref(), position()) -> val_or_error(offset())).
-spec(truncate/1 :: (ref()) -> ok_or_error()).
-spec(current_virtual_offset/1 :: (ref()) -> val_or_error(offset())).
-spec(current_raw_offset/1     :: (ref()) -> val_or_error(offset())).
-spec(flush/1 :: (ref()) -> ok_or_error()).
-spec(copy/3 :: (ref(), ref(), non_neg_integer()) ->
                     val_or_error(non_neg_integer())).
-spec(delete/1 :: (ref()) -> ok_or_error()).
-spec(clear/1 :: (ref()) -> ok_or_error()).
-spec(set_maximum_since_use/1 :: (non_neg_integer()) -> 'ok').
-spec(obtain/0 :: () -> 'ok').
-spec(obtain/1 :: (non_neg_integer()) -> 'ok').
-spec(release/0 :: () -> 'ok').
-spec(release/1 :: (non_neg_integer()) -> 'ok').
-spec(transfer/1 :: (pid()) -> 'ok').
-spec(transfer/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(with_handle/1 :: (fun(() -> A)) -> A).
-spec(with_handle/2 :: (non_neg_integer(), fun(() -> A)) -> A).
-spec(set_limit/1 :: (non_neg_integer()) -> 'ok').
-spec(get_limit/0 :: () -> non_neg_integer()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/0 :: () -> rabbit_types:infos()).
-spec(info/1 :: ([atom()]) -> rabbit_types:infos()).
-spec(ulimit/0 :: () -> 'unknown' | non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------
-define(INFO_KEYS, [total_limit, total_used, sockets_limit, sockets_used]).

%%----------------------------------------------------------------------------
%% Public(公共) API
%%----------------------------------------------------------------------------
%% file_handle_cache进程的启动API接口
start_link() ->
	start_link(fun alarm_handler:set_alarm/1, fun alarm_handler:clear_alarm/1).


%% file_handle_cache进程的启动API接口
start_link(AlarmSet, AlarmClear) ->
	gen_server2:start_link({local, ?SERVER}, ?MODULE, [AlarmSet, AlarmClear],
						   [{timeout, infinity}]).


%% 打开文件的客户端进程注册的回调清理函数的接口
register_callback(M, F, A)
  when is_atom(M) andalso is_atom(F) andalso is_list(A) ->
	gen_server2:cast(?SERVER, {register_callback, self(), {M, F, A}}).


%% 打开一个文件的API接口
open(Path, Mode, Options) ->
	%% 得到文件名字对应的绝对路径(如果传入的是单个文件名字则返回当前工作目录加上文件名，如果传入的是绝对路径则直接返回绝对路径)
	Path1 = filename:absname(Path),
	%% 得到当前文件的file文件数据结构
	File1 = #file { reader_count = RCount, has_writer = HasWriter } =
					  case get({Path1, fhc_file}) of
						  File = #file {} -> File;
						  undefined       -> #file { reader_count = 0,
													 has_writer = false }
					  end,
	%% 将打开文件的append mode直接转化为write
	Mode1 = append_to_write(Mode),
	%% 判断当前文件操作模式是否是写模式
	IsWriter = is_writer(Mode1),
	case IsWriter andalso HasWriter of
		true  -> {error, writer_exists};
		false -> %% 新增当前文件关闭的句柄数据结构
				 {ok, Ref} = new_closed_handle(Path1, Mode1, Options),
				 case get_or_reopen([{Ref, new}]) of
					 {ok, [_Handle1]} ->
						 RCount1 = case is_reader(Mode1) of
									   true  -> RCount + 1;
									   false -> RCount
								   end,
						 HasWriter1 = HasWriter orelse IsWriter,
						 %% 更新打开的磁盘文件的file结构到调用该接口的进程中的进程字典中
						 put({Path1, fhc_file},
							 File1 #file { reader_count = RCount1,
										   has_writer = HasWriter1 }),
						 {ok, Ref};
					 Error ->
						 erase({Ref, fhc_handle}),
						 Error
				 end
	end.


%% 关闭一个文件的接口
close(Ref) ->
	case erase({Ref, fhc_handle}) of
		undefined -> ok;
		Handle    -> case hard_close(Handle) of
						 ok               -> ok;
						 {Error, Handle1} -> put_handle(Ref, Handle1),
											 Error
					 end
	end.


%% 读取文件的接口
read(Ref, Count) ->
	%% 先将Ref句柄的写缓存清空，然后对Ref句柄进行Fun函数的操作，操作成功后更新Ref句柄最后使用时间
	with_flushed_handles(
	  [Ref], keep,
	  fun ([#handle { is_read = false }]) ->
			   {error, not_open_for_reading};
		 ([Handle = #handle{read_buffer       = Buf,
							read_buffer_pos   = BufPos,
							read_buffer_rem   = BufRem,
							read_buffer_usage = BufUsg,
							offset            = Offset}])
		   when BufRem >= Count ->
			  %% 当前读取过还未引用的数据大于等于需要阅读的字符数量Count则直接从缓存中读取
			  <<_:BufPos/binary, Res:Count/binary, _/binary>> = Buf,
			  {{ok, Res}, [Handle#handle{offset            = Offset + Count,
										 read_buffer_pos   = BufPos + Count,
										 read_buffer_rem   = BufRem - Count,
										 read_buffer_usage = BufUsg + Count }]};
		 ([Handle0]) ->
			  %% 当内存的使用量超过上限有可能需要减少读取的缓存
			  maybe_reduce_read_cache([Ref]),
			  Handle = #handle{read_buffer      = Buf,
							   read_buffer_pos  = BufPos,
							   read_buffer_rem  = BufRem,
							   read_buffer_size = BufSz,
							   hdl              = Hdl,
							   offset           = Offset}
								  = tune_read_buffer_limit(Handle0, Count),
			  %% 读缓存中上次读中剩余的字符串数量小于本次需要读取的字符数量
			  WantedCount = Count - BufRem,
			  %% 实际读取文件的地方(WantedCount:实际要从磁盘读取的数据大小)
			  case prim_file_read(Hdl, lists:max([BufSz, WantedCount])) of
				  {ok, Data} ->
					  <<_:BufPos/binary, BufTl/binary>> = Buf,
					  ReadCount = size(Data),
					  case ReadCount < WantedCount of
						  %% 到这种情况是要读取的数据大于读到的数据，此时读取已经到达磁盘文件的末尾
						  true ->
							  OffSet1 = Offset + BufRem + ReadCount,
							  {{ok, <<BufTl/binary, Data/binary>>},
							   %% 则重置句柄的读缓存，然后更新文件句柄的最新偏移位置
							   [reset_read_buffer(
								  Handle#handle{offset = OffSet1})]};
						  false ->
							  <<Hd:WantedCount/binary, _/binary>> = Data,
							  OffSet1 = Offset + BufRem + WantedCount,
							  BufRem1 = ReadCount - WantedCount,
							  {{ok, <<BufTl/binary, Hd/binary>>},
							   [Handle#handle{offset            = OffSet1,
											  read_buffer       = Data,
											  read_buffer_pos   = WantedCount,
											  read_buffer_rem   = BufRem1,
											  read_buffer_usage = WantedCount}]}
					  end;
				  %% 已经读到文件的尾部
				  eof ->
					  {eof, [Handle #handle { at_eof = true }]};
				  %% 读取数据出现错误，则将该句柄的读缓存清空
				  Error ->
					  {Error, [reset_read_buffer(Handle)]}
			  end
	  end).


%% file_handle_cache中的文件写操作只能是写在文件的后面
append(Ref, Data) ->
	%% 对Refs所有的句柄执行Fun函数，在执行Fun函数之前reset表示会将Refs所有句柄的读缓存清除掉
	with_handles(
	  [Ref],
	  fun ([#handle { is_write = false }]) ->
			   {error, not_open_for_writing};
		 ([Handle]) ->
			  case maybe_seek(eof, Handle) of
				  %% 没有写缓存的设置，则直接将数据写入文件
				  {{ok, _Offset}, #handle { hdl = Hdl, offset = Offset,
											write_buffer_size_limit = 0,
											at_eof = true } = Handle1} ->
					  Offset1 = Offset + iolist_size(Data),
					  %% prim_file模块写Hdl句柄Bytes长度的数据，然后更新io_write字段对应的count，size，time信息
					  {prim_file_write(Hdl, Data),
					   %% 将is_dirty字段设置为true，然后更新最新的文件偏移量
					   [Handle1 #handle { is_dirty = true, offset = Offset1 }]};
				  {{ok, _Offset}, #handle { write_buffer = WriteBuffer,
											write_buffer_size = Size,
											write_buffer_size_limit = Limit,
											at_eof = true } = Handle1} ->
					  WriteBuffer1 = [Data | WriteBuffer],
					  Size1 = Size + iolist_size(Data),
					  Handle2 = Handle1 #handle { write_buffer = WriteBuffer1,
												  write_buffer_size = Size1 },
					  %% 如果不是永久缓存且当前缓存的大小超过写缓存的大小限制则将当前的写缓存写入文件
					  case Limit =/= infinity andalso Size1 > Limit of
						  %% 将句柄中缓存的数据写入磁盘文件
						  true  -> {Result, Handle3} = write_buffer(Handle2),
								   {Result, [Handle3]};
						  false -> {ok, [Handle2]}
					  end;
				  {{error, _} = Error, Handle1} ->
					  {Error, [Handle1]}
			  end
	  end).


%% 文件的同步
sync(Ref) ->
	%% 先将Refs所有句柄的写缓存清空，然后对Refs列表中的句柄进行Fun函数的操作，操作成功后更新Refs列表中的所有最后使用时间
	with_flushed_handles(
	  [Ref], keep,
	  fun ([#handle { is_dirty = false, write_buffer = [] }]) ->
			   ok;
		 ([Handle = #handle { hdl = Hdl,
							  is_dirty = true, write_buffer = [] }]) ->
			  %% prim_file模块同步磁盘文件，然后更新io_sync字段的数量和同步花费的时间
			  case prim_file_sync(Hdl) of
				  ok    -> {ok, [Handle #handle { is_dirty = false }]};
				  Error -> {Error, [Handle]}
			  end
	  end).


%% 判断是否需要同步
needs_sync(Ref) ->
	%% This must *not* use with_handles/2; see bug 25052
	case get({Ref, fhc_handle}) of
		#handle { is_dirty = false, write_buffer = [] } -> false;
		#handle {}                                      -> true
	end.


position(Ref, NewOffset) ->
	%% 先将Refs所有句柄的写缓存清空，然后对Refs列表中的句柄进行Fun函数的操作，操作成功后更新Refs列表中的所有最后使用时间
	with_flushed_handles(
	  [Ref], keep,
	  %% 判断当前文件句柄是否需要重新设置偏移位置，如果需要，则立刻对文件句柄重新设置偏移位置
	  fun ([Handle]) -> {Result, Handle1} = maybe_seek(NewOffset, Handle),
						{Result, [Handle1]}
	  end).

%% truncate:截短
%% 截断Ref对应的磁盘文件
truncate(Ref) ->
	%% 先将Refs所有句柄的写缓存清空，然后对Refs列表中的句柄进行Fun函数的操作，操作成功后更新Refs列表中的所有最后使用时间
	with_flushed_handles(
	  [Ref],
	  fun ([Handle1 = #handle { hdl = Hdl }]) ->
			   case prim_file:truncate(Hdl) of
				   %% 更新句柄已经处于磁盘文件尾部
				   ok    -> {ok, [Handle1 #handle { at_eof = true }]};
				   Error -> {Error, [Handle1]}
			   end
	  end).


%% 拿到Ref句柄对应的最新偏移量
current_virtual_offset(Ref) ->
	with_handles([Ref], fun ([#handle { at_eof = true, is_write = true,
										offset = Offset,
										write_buffer_size = Size }]) ->
								 {ok, Offset + Size};
						   ([#handle { offset = Offset }]) ->
								{ok, Offset}
				 end).


%% 拿到Ref句柄对应的偏移量，直接获取offset字段，不需用加上write_buffer_size字段的偏移量
current_raw_offset(Ref) ->
	with_handles([Ref], fun ([Handle]) -> {ok, Handle #handle.offset} end).


%% 将Ref对应的句柄中的写数据缓存写入到磁盘中
flush(Ref) ->
	with_flushed_handles([Ref], fun ([Handle]) -> {ok, [Handle]} end).


%% 将Src句柄中Count大小的数据拷贝到Dest句柄对应的磁盘文件
copy(Src, Dest, Count) ->
	%% 对Refs所有的句柄执行Fun函数，在执行Fun函数之前reset表示会将Refs所有句柄的读缓存清除掉，同时将Refs所有句柄的写缓存清空
	with_flushed_handles(
	  [Src, Dest],
	  fun ([SHandle = #handle { is_read  = true, hdl = SHdl, offset = SOffset },
			DHandle = #handle { is_write = true, hdl = DHdl, offset = DOffset }]
		  ) ->
			   case prim_file:copy(SHdl, DHdl, Count) of
				   {ok, Count1} = Result1 ->
					   {Result1,
						%% 调整目的和源句柄对应的磁盘文件的最新偏移量
						[SHandle #handle { offset = SOffset + Count1 },
										 DHandle #handle { offset = DOffset + Count1,
														   is_dirty = true }]};
				   Error ->
					   {Error, [SHandle, DHandle]}
			   end;
		 (_Handles) ->
			  {error, incorrect_handle_modes}
	  end).


%% 强制删除句柄，写缓存数据会丢失
delete(Ref) ->
	%% 先清除句柄进程字典
	case erase({Ref, fhc_handle}) of
		undefined ->
			ok;
		Handle = #handle { path = Path } ->
			%% 硬关闭(先进行软关闭，先将写缓存中的数据写入磁盘，然后关闭句柄，然后如果当前读个数不为0或者是写操作则更新文件最新的读取者数量和has_writer字段，否则直接将文件信息全部清除掉)
			case hard_close(Handle #handle { is_dirty = false,
											 write_buffer = [] }) of
				ok               -> prim_file:delete(Path);
				{Error, Handle1} -> put_handle(Ref, Handle1),
									Error
			end
	end.


%% 清理Ref对应的句柄
clear(Ref) ->
	with_handles(
	  [Ref],
	  fun ([#handle { at_eof = true, write_buffer_size = 0, offset = 0 }]) ->
			   ok;
		 ([Handle]) ->
			  case maybe_seek(bof, Handle#handle{write_buffer      = [],
												 write_buffer_size = 0}) of
				  {{ok, 0}, Handle1 = #handle { hdl = Hdl }} ->
					  %% 直接将句柄对应的磁盘文件截断
					  case prim_file:truncate(Hdl) of
						  ok    -> {ok, [Handle1 #handle { at_eof = true }]};
						  Error -> {Error, [Handle1]}
					  end;
				  {{error, _} = Error, Handle1} ->
					  {Error, [Handle1]}
			  end
	  end).


%% 将当前进程客户端中文件句柄打开时间超过MaximumAge的句柄软关闭
set_maximum_since_use(MaximumAge) ->
	Now = now(),
	case lists:foldl(
		   fun ({{Ref, fhc_handle},
				 Handle = #handle { hdl = Hdl, last_used_at = Then }}, Rep) ->
					case Hdl =/= closed andalso
							 timer:now_diff(Now, Then) >= MaximumAge of
						true  -> soft_close(Ref, Handle) orelse Rep;
						false -> Rep
					end;
			  (_KeyValuePair, Rep) ->
				   Rep
		   end, false, get()) of
		false -> age_tree_change(), ok;
		true  -> ok
	end.


%% obtain/0方法增加拥有的文件描述符数量
obtain()      -> obtain(1).


%% release/0方法减少拥有的文件描述符数量
release()     -> release(1).


%% transfer/1将文件描述符的拥有权转移到另外一个进程（原进程拥有的打开描述符数量减1，转移目标进程拥有的文件描述符数量加1）
transfer(Pid) -> transfer(Pid, 1).


%% obtain/0方法增加拥有的文件描述符数量
obtain(Count)        -> obtain(Count, socket).


%% release/0方法减少拥有的文件描述符数量
release(Count)       -> release(Count, socket).


%% 操作单个整体文件的接口
with_handle(Fun) ->
	with_handle(1, Fun).


%% 操作单个整体文件的接口
with_handle(N, Fun) ->
	%% 将当前客户端进程在file_handle_cache进程中file对应的数量增加N
	ok = obtain(N, file),
	try Fun()
	%% 执行完Fun函数后，再将当前客户端进程在file_handle_cache进程中file对应的数量减去N
	after ok = release(N, file)
	end.


%% 增加Type对应obtain的数量
obtain(Count, Type) when Count > 0 ->
	%% If the FHC isn't running, obtains succeed immediately.
	case whereis(?SERVER) of
		undefined -> ok;
		_         -> gen_server2:call(
					   ?SERVER, {obtain, Count, Type, self()}, infinity)
	end.


%% release/0方法减少拥有的文件描述符数量
release(Count, Type) when Count > 0 ->
	gen_server2:cast(?SERVER, {release, Count, Type, self()}).


%% transfer/1将文件描述符的拥有权转移到另外一个进程（原进程拥有的打开描述符数量减Count，转移目标进程拥有的文件描述符数量加Count）
transfer(Pid, Count) when Count > 0 ->
	gen_server2:cast(?SERVER, {transfer, Count, self(), Pid}).


%% 设置file_handle_cache进程中文件描述符的上限
set_limit(Limit) ->
	gen_server2:call(?SERVER, {set_limit, Limit}, infinity).


%% 获取file_handle_cache进程上的文件描述符上限
get_limit() ->
	gen_server2:call(?SERVER, get_limit, infinity).


%% 获得file_handle_cache关键信息key列表
info_keys() -> ?INFO_KEYS.


%% 获取file_handle_cache进程INFO_KEYS对应的所有关键信息
info() -> info(?INFO_KEYS).


%% 获取file_handle_cache进程Items对应的所有关键信息
info(Items) -> gen_server2:call(?SERVER, {info, Items}, infinity).

%%----------------------------------------------------------------------------
%% Internal(内部) functions
%%----------------------------------------------------------------------------
%% prim_file模块读Hdl句柄Size大小的磁盘数据，然后更新io_read字段对应的count，size，time信息
prim_file_read(Hdl, Size) ->
	file_handle_cache_stats:update(
	  io_read, Size, fun() -> prim_file:read(Hdl, Size) end).


%% prim_file模块写Hdl句柄Bytes长度的数据，然后更新io_write字段对应的count，size，time信息
prim_file_write(Hdl, Bytes) ->
	file_handle_cache_stats:update(
	  io_write, iolist_size(Bytes), fun() -> prim_file:write(Hdl, Bytes) end).


%% prim_file模块同步磁盘文件，然后更新io_sync字段的数量和同步花费的时间
prim_file_sync(Hdl) ->
	file_handle_cache_stats:update(io_sync, fun() -> prim_file:sync(Hdl) end).


%% prim_file模块更新文件句柄最新的偏移位置，然后更新io_seek字段的数量和确定位置的执行时间
prim_file_position(Hdl, NewOffset) ->
	file_handle_cache_stats:update(
	  io_seek, fun() -> prim_file:position(Hdl, NewOffset) end).


%% 判断Mode模式是否是读模式
is_reader(Mode) -> lists:member(read, Mode).


%% 判断Mode模式是否是写模式
is_writer(Mode) -> lists:member(write, Mode).


%% 将打开文件的append mode直接转化为write
append_to_write(Mode) ->
	case lists:member(append, Mode) of
		true  -> [write | Mode -- [append, write]];
		false -> Mode
	end.


%% 对Refs所有的句柄执行Fun函数，在执行Fun函数之前reset表示会将Refs所有句柄的读缓存清除掉
with_handles(Refs, Fun) ->
	with_handles(Refs, reset, Fun).


%% 对Refs列表中的句柄进行Fun函数的操作，操作成功后更新Refs列表中的所有最后使用时间
with_handles(Refs, ReadBuffer, Fun) ->
	%% 如果Refs列表中的句柄没有打开则重新打开
	case get_or_reopen([{Ref, reopen} || Ref <- Refs]) of
		{ok, Handles0} ->
			Handles = case ReadBuffer of
						  %% 重置句柄的读缓存
						  reset -> [reset_read_buffer(H) || H <- Handles0];
						  %% keep表示保持读缓存
						  keep  -> Handles0
					  end,
			case Fun(Handles) of
				{Result, Handles1} when is_list(Handles1) ->
					%% 更新Refs的最后使用时间
					lists:zipwith(fun put_handle/2, Refs, Handles1),
					Result;
				Result ->
					Result
			end;
		Error ->
			Error
	end.


%% 对Refs所有的句柄执行Fun函数，在执行Fun函数之前reset表示会将Refs所有句柄的读缓存清除掉，同时将Refs所有句柄的写缓存清空
with_flushed_handles(Refs, Fun) ->
	with_flushed_handles(Refs, reset, Fun).


%% 先将Refs所有句柄的写缓存清空，然后对Refs列表中的句柄进行Fun函数的操作，操作成功后更新Refs列表中的所有最后使用时间
with_flushed_handles(Refs, ReadBuffer, Fun) ->
	with_handles(
	  Refs, ReadBuffer,
	  fun (Handles) ->
			   %% 如果有写缓存，则先将写缓存数据写入文件
			   case lists:foldl(
					  fun (Handle, {ok, HandlesAcc}) ->
							   %% 将句柄的缓存写入磁盘
							   {Res, Handle1} = write_buffer(Handle),
							   {Res, [Handle1 | HandlesAcc]};
						 (Handle, {Error, HandlesAcc}) ->
							  {Error, [Handle | HandlesAcc]}
					  end, {ok, []}, Handles) of
				   {ok, Handles1} ->
					   Fun(lists:reverse(Handles1));
				   {Error, Handles1} ->
					   {Error, lists:reverse(Handles1)}
			   end
	  end).


%% 根据标志拿到对应的handle或者重新打开该文件
get_or_reopen(RefNewOrReopens) ->
	%% 将已经打开的句柄和关闭的句柄分开
	case partition_handles(RefNewOrReopens) of
		{OpenHdls, []} ->
			{ok, [Handle || {_Ref, Handle} <- OpenHdls]};
		{OpenHdls, ClosedHdls} ->
			%% 得到年龄最老的句柄创建时间
			Oldest = oldest(get_age_tree(), fun () -> now() end),
			case gen_server2:call(?SERVER, {open, self(), length(ClosedHdls),
											Oldest}, infinity) of
				ok ->
					case reopen(ClosedHdls) of
						{ok, RefHdls}  -> sort_handles(RefNewOrReopens,
													   OpenHdls, RefHdls, []);
						Error          -> Error
					end;
				%% file_handle_cache进程中的文件描述符已经超过上限，且当前客户端有打开的文件句柄，则将当前客户端的所有文件句柄进行软关闭
				close ->
					%% 将当前客户端所有没有关闭的句柄执行软关闭
					[soft_close(Ref, Handle) ||
					   {{Ref, fhc_handle}, Handle = #handle { hdl = Hdl }} <-
						   get(),
					   Hdl =/= closed],
					%% 将当前客户端的所有句柄关闭后，再重新打开要打开的句柄
					get_or_reopen(RefNewOrReopens)
			end
	end.


%% 重新打开文件句柄的函数接口
reopen(ClosedHdls) -> reopen(ClosedHdls, get_age_tree(), []).


reopen([], Tree, RefHdls) ->
	%% 更新最新的时间平衡二叉树
	put_age_tree(Tree),
	{ok, lists:reverse(RefHdls)};
reopen([{Ref, NewOrReopen, Handle = #handle { hdl          = closed,
											  path         = Path,
											  mode         = Mode0,
											  offset       = Offset,
											  last_used_at = undefined }} |
			RefNewOrReopenHdls] = ToOpen, Tree, RefHdls) ->
	Mode = case NewOrReopen of
			   %% new表示当前文件是要新打开的文件句柄
			   new    -> Mode0;
			   %% reopen表示当前文件是重新打开句柄，同时更新IO中重新打开文件的数量
			   reopen -> file_handle_cache_stats:update(io_reopen),
						 [read | Mode0]
		   end,
	%% 调用prim_file:open()接口打开文件句柄
	case prim_file:open(Path, Mode) of
		{ok, Hdl} ->
			Now = now(),
			{{ok, _Offset}, Handle1} =
				maybe_seek(Offset, reset_read_buffer(
							 Handle#handle{hdl              = Hdl,
										   offset           = 0,
										   last_used_at     = Now})),
			%% 将当前文件句柄数据结构存入进程字典
			put({Ref, fhc_handle}, Handle1),
			%% 将当前句柄的打开时间写入时间二叉树，然后继续处理重新打开的剩余列表
			reopen(RefNewOrReopenHdls, gb_trees:insert(Now, Ref, Tree),
				   [{Ref, Handle1} | RefHdls]);
		Error ->
			%% NB: none of the handles in ToOpen are in the age tree
			Oldest = oldest(Tree, fun () -> undefined end),
			[gen_server2:cast(?SERVER, {close, self(), Oldest}) || _ <- ToOpen],
			%% 更新最新的时间平衡二叉树
			put_age_tree(Tree),
			Error
	end.


%% 将已经打开的句柄和关闭的句柄分开
partition_handles(RefNewOrReopens) ->
	lists:foldr(
	  fun ({Ref, NewOrReopen}, {Open, Closed}) ->
			   case get({Ref, fhc_handle}) of
				   #handle { hdl = closed } = Handle ->
					   {Open, [{Ref, NewOrReopen, Handle} | Closed]};
				   #handle {} = Handle ->
					   {[{Ref, Handle} | Open], Closed}
			   end
	  end, {[], []}, RefNewOrReopens).


%% 给句柄排序
sort_handles([], [], [], Acc) ->
	{ok, lists:reverse(Acc)};
sort_handles([{Ref, _} | RefHdls], [{Ref, Handle} | RefHdlsA], RefHdlsB, Acc) ->
	sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]);
sort_handles([{Ref, _} | RefHdls], RefHdlsA, [{Ref, Handle} | RefHdlsB], Acc) ->
	sort_handles(RefHdls, RefHdlsA, RefHdlsB, [Handle | Acc]).


%% 更新文件句柄最新的使用时间
put_handle(Ref, Handle = #handle { last_used_at = Then }) ->
	Now = now(),
	age_tree_update(Then, Now, Ref),
	put({Ref, fhc_handle}, Handle #handle { last_used_at = Now }).


%% 对时间二叉树执行Fun函数
with_age_tree(Fun) -> put_age_tree(Fun(get_age_tree())).


%% 得到当前年龄二叉树的数据结构
get_age_tree() ->
	case get(fhc_age_tree) of
		undefined -> gb_trees:empty();
		AgeTree   -> AgeTree
	end.


%% 更新最新的时间平衡二叉树
put_age_tree(Tree) -> put(fhc_age_tree, Tree).


%% 将Then老的时间对应的数据删除掉，然后插入Now对应Ref的数据
age_tree_update(Then, Now, Ref) ->
	with_age_tree(
	  fun (Tree) ->
			   gb_trees:insert(Now, Ref, gb_trees:delete_any(Then, Tree))
	  end).


%% 将Then这个时间点对应的打开句柄数据从时间二叉树中删除
age_tree_delete(Then) ->
	with_age_tree(
	  fun (Tree) ->
			   %% 将Then时间对应的数据删除
			   Tree1 = gb_trees:delete_any(Then, Tree),
			   Oldest = oldest(Tree1, fun () -> undefined end),
			   %% 通知file_handle_cache进程关闭一个句柄
			   gen_server2:cast(?SERVER, {close, self(), Oldest}),
			   Tree1
	  end).


%% 年龄数的改变接口
age_tree_change() ->
	with_age_tree(
	  fun (Tree) ->
			   case gb_trees:is_empty(Tree) of
				   true  -> Tree;
				   false -> {Oldest, _Ref} = gb_trees:smallest(Tree),
							%% 通知file_handle_cache进程更新
							gen_server2:cast(?SERVER, {update, self(), Oldest})
			   end,
			   Tree
	  end).


%% 拿到年龄二叉树中年龄最先的数据
oldest(Tree, DefaultFun) ->
	case gb_trees:is_empty(Tree) of
		true  -> DefaultFun();
		false -> {Oldest, _Ref} = gb_trees:smallest(Tree),
				 Oldest
	end.


%% 新增一个文件handle
new_closed_handle(Path, Mode, Options) ->
	%% 拿到当前文件写的缓存大小
	WriteBufferSize =
		case proplists:get_value(write_buffer, Options, unbuffered) of
			unbuffered           -> 0;
			infinity             -> infinity;
			N when is_integer(N) -> N
		end,
	%% 拿到当前文件读的缓存大小
	ReadBufferSize =
		case proplists:get_value(read_buffer, Options, unbuffered) of
			unbuffered             -> 0;
			N2 when is_integer(N2) -> N2
		end,
	%% 给当前文件生成一个唯一的标识
	Ref = make_ref(),
	%% 将文件句柄信息放入到自己进程的进程字典中
	put({Ref, fhc_handle}, #handle { hdl                     = closed,
									 offset                  = 0,
									 is_dirty                = false,
									 write_buffer_size       = 0,
									 write_buffer_size_limit = WriteBufferSize,
									 write_buffer            = [],
									 read_buffer             = <<>>,
									 read_buffer_pos         = 0,
									 read_buffer_rem         = 0,
									 read_buffer_size        = ReadBufferSize,
									 read_buffer_size_limit  = ReadBufferSize,
									 read_buffer_usage       = 0,
									 at_eof                  = false,
									 path                    = Path,
									 mode                    = Mode,
									 options                 = Options,
									 is_write                = is_writer(Mode),
									 is_read                 = is_reader(Mode),
									 last_used_at            = undefined }),
	{ok, Ref}.


%% 软关闭文件句柄Handle(所谓软关闭，就是将Handle对应文件buffer写完，sync后再关闭)
soft_close(Ref, Handle) ->
	{Res, Handle1} = soft_close(Handle),
	case Res of
		%% 删除句柄成功，则将最新的句柄写入到进程字典
		ok -> put({Ref, fhc_handle}, Handle1),
			  true;
		%% 更新文件句柄最新的使用时间
		_  -> put_handle(Ref, Handle1),
			  false
	end.


soft_close(Handle = #handle { hdl = closed }) ->
	{ok, Handle};
soft_close(Handle) ->
	%% 将句柄中缓存的数据写入磁盘文件
	case write_buffer(Handle) of
		{ok, #handle { hdl         = Hdl,
					   is_dirty    = IsDirty,
					   last_used_at = Then } = Handle1 } ->
			ok = case IsDirty of
					 %% prim_file模块同步磁盘文件，然后更新io_sync字段的数量和同步花费的时间
					 true  -> prim_file_sync(Hdl);
					 false -> ok
				 end,
			%% 将文件句柄关闭
			ok = prim_file:close(Hdl),
			%% 删除平衡二叉树中的打开时间数据
			age_tree_delete(Then),
			{ok, Handle1 #handle { hdl            = closed,
								   is_dirty       = false,
								   last_used_at   = undefined }};
		{_Error, _Handle} = Result ->
			Result
	end.


%% 硬关闭(先进行软关闭，先将写缓存中的数据写入磁盘，然后关闭句柄，然后如果当前读个数不为0或者是写操作则更新文件最新的读取者数量和has_writer字段，否则直接将文件信息全部清除掉)
hard_close(Handle) ->
	%% 先进行软关闭
	case soft_close(Handle) of
		{ok, #handle { path = Path,
					   is_read = IsReader, is_write = IsWriter }} ->
			#file { reader_count = RCount, has_writer = HasWriter } = File =
																		  get({Path, fhc_file}),
			RCount1 = case IsReader of
						  true  -> RCount - 1;
						  false -> RCount
					  end,
			HasWriter1 = HasWriter andalso not IsWriter,
			case RCount1 =:= 0 andalso not HasWriter1 of
				%% 如果当前读的个数为0且不是写操作则将file的进程字典擦掉
				true  -> erase({Path, fhc_file});
				%% 如果当前读个数不为0或者是写操作则更新文件最新的读取者数量和has_writer字段
				false -> put({Path, fhc_file},
							 File #file { reader_count = RCount1,
										  has_writer = HasWriter1 })
			end,
			ok;
		{_Error, _Handle} = Result ->
			Result
	end.


%% 判断当前文件句柄是否需要重新设置偏移位置，如果需要，则立刻对文件句柄重新设置偏移位置
maybe_seek(New, Handle = #handle{hdl              = Hdl,
								 offset           = Old,
								 read_buffer_pos  = BufPos,
								 read_buffer_rem  = BufRem,
								 at_eof           = AtEoF}) ->
	%% 根据文件是否处在文件结尾，最新的偏移位置和老的偏移位置得到文件偏移是否在文件尾部和是否需要进行重新设置偏移位置
	{AtEoF1, NeedsSeek} = needs_seek(AtEoF, Old, New),
	case NeedsSeek of
		%% 需要调整read_buffer_pos和read_buffer_rem字段
		true when is_number(New) andalso
					  ((New >= Old andalso New =< BufRem + Old)
						   orelse (New < Old andalso Old - New =< BufPos)) ->
			Diff = New - Old,
			{{ok, New}, Handle#handle{offset          = New,
									  at_eof          = AtEoF1,
									  read_buffer_pos = BufPos + Diff,
									  read_buffer_rem = BufRem - Diff}};
		true ->
			%% prim_file模块更新文件句柄最新的偏移位置，然后更新io_seek字段的数量和确定位置的执行时间
			case prim_file_position(Hdl, New) of
				{ok, Offset1} = Result ->
					%% 重置读缓存
					{Result, reset_read_buffer(Handle#handle{offset = Offset1,
															 at_eof = AtEoF1})};
				{error, _} = Error ->
					{Error, Handle}
			end;
		false ->
			{{ok, Old}, Handle}
	end.


%% 根据文件是否处在文件结尾，最新的偏移位置和老的偏移位置得到文件偏移是否在文件尾部和是否需要进行重新设置偏移位置
needs_seek( AtEoF, _CurOffset,  cur     ) -> {AtEoF, false};
needs_seek( AtEoF, _CurOffset,  {cur, 0}) -> {AtEoF, false};
needs_seek(  true, _CurOffset,  eof     ) -> {true , false};
needs_seek(  true, _CurOffset,  {eof, 0}) -> {true , false};
needs_seek( false, _CurOffset,  eof     ) -> {true , true };
needs_seek( false, _CurOffset,  {eof, 0}) -> {true , true };
needs_seek( AtEoF,          0,  bof     ) -> {AtEoF, false};
needs_seek( AtEoF,          0,  {bof, 0}) -> {AtEoF, false};
needs_seek( AtEoF,  CurOffset, CurOffset) -> {AtEoF, false};
needs_seek(  true,  CurOffset, {bof, DesiredOffset})
  when DesiredOffset >= CurOffset ->
	{true, true};
needs_seek(  true, _CurOffset, {cur, DesiredOffset})
  when DesiredOffset > 0 ->
	{true, true};
needs_seek(  true,  CurOffset, DesiredOffset) %% same as {bof, DO}
  when is_integer(DesiredOffset) andalso DesiredOffset >= CurOffset ->
	{true, true};
%% because we can't really track size, we could well end up at EoF and not know
needs_seek(_AtEoF, _CurOffset, _DesiredOffset) ->
	{false, true}.


%% 将句柄中缓存的数据写入磁盘文件
write_buffer(Handle = #handle { write_buffer = [] }) ->
	{ok, Handle};
write_buffer(Handle = #handle { hdl = Hdl, offset = Offset,
								write_buffer = WriteBuffer,
								write_buffer_size = DataSize,
								at_eof = true }) ->
	%% prim_file模块写Hdl句柄Bytes长度的数据，然后更新io_write字段对应的count，size，time信息
	case prim_file_write(Hdl, lists:reverse(WriteBuffer)) of
		ok ->
			Offset1 = Offset + DataSize,
			%% 更新当前句柄最新的句柄偏移位置，同时将缓存写的数据字段重置为初始化，is_dirty字段设置为true
			{ok, Handle #handle { offset = Offset1, is_dirty = true,
								  write_buffer = [], write_buffer_size = 0 }};
		{error, _} = Error ->
			{Error, Handle}
	end.


%% 重置句柄的读缓存
reset_read_buffer(Handle) ->
	Handle#handle{read_buffer     = <<>>,
				  read_buffer_pos = 0,
				  read_buffer_rem = 0}.

%% We come into this function whenever there's been a miss while
%% reading from the buffer - but note that when we first start with a
%% new handle the usage will be 0.  Therefore in that case don't take
%% it as meaning the buffer was useless, we just haven't done anything
%% yet!
%% 调整read_buffer_size的大小，此处是第一次读取磁盘文件，则直接返回
tune_read_buffer_limit(Handle = #handle{read_buffer_usage = 0}, _Count) ->
	Handle;
%% In this head we have been using the buffer but now tried to read
%% outside it. So how did we do? If we used less than the size of the
%% buffer, make the new buffer the size of what we used before, but
%% add one byte (so that next time we can distinguish(区分) between getting
%% the buffer size exactly right and actually wanting more). If we
%% read 100% of what we had, then double it for next time, up to the
%% limit that was set when we were created.
%% 调整read_buffer_size的大小
tune_read_buffer_limit(Handle = #handle{read_buffer            = Buf,
										read_buffer_usage      = Usg,
										read_buffer_size       = Sz,
										read_buffer_size_limit = Lim}, Count) ->
	%% If the buffer is <<>> then we are in the first read after a
	%% reset, the read_buffer_usage is the total usage from before the
	%% reset. But otherwise we are in a read which read off the end of
	%% the buffer, so really the size of this read should be included
	%% in the usage.
	TotalUsg = case Buf of
				   <<>> -> Usg;
				   _    -> Usg + Count
			   end,
	Handle#handle{read_buffer_usage = 0,
				  read_buffer_size  = erlang:min(case TotalUsg < Sz of
													 true  -> Usg + 1;
													 false -> Usg * 2
												 end, Lim)}.


%% 当内存的使用量超过上限有可能需要减少读取的缓存
maybe_reduce_read_cache(SparedRefs) ->
	case rabbit_memory_monitor:memory_use(bytes) of
		{_, infinity}                             -> ok;
		{MemUse, MemLimit} when MemUse < MemLimit -> ok;
		{MemUse, MemLimit}                        -> reduce_read_cache(
													   (MemUse - MemLimit) * 2,
													   SparedRefs)
	end.


%% 拿到当前进程中所有的除去SparedRefs同时read_buffer大于0的所有句柄，将这些句柄中的读缓存清空直到清空的内存大小超过MemToFree大小
reduce_read_cache(MemToFree, SparedRefs) ->
	%% 拿到当前进程中所有的除去SparedRefs同时read_buffer大于0的所有句柄
	Handles = lists:sort(
				fun({_, H1}, {_, H2}) -> H1 < H2 end,
				[{R, H} || {{R, fhc_handle}, H} <- get(),
						   not lists:member(R, SparedRefs)
							   andalso size(H#handle.read_buffer) > 0]),
	FreedMem = lists:foldl(
				 fun
					%% 如果句柄中释放的内存大小超过MemToFree，则停止将后面的句柄的读缓存清空
					(_, Freed) when Freed >= MemToFree ->
						 Freed;
					({Ref, #handle{read_buffer = Buf} = Handle}, Freed) ->
						 %% 重置句柄的读缓存
						 Handle1 = reset_read_buffer(Handle),
						 %% 将最新的句柄存储起来
						 put({Ref, fhc_handle}, Handle1),
						 %% 增加获得释放的内存大小
						 Freed + size(Buf)
				 end, 0, Handles),
	if
		FreedMem < MemToFree andalso SparedRefs =/= [] ->
			reduce_read_cache(MemToFree - FreedMem, []);
		true ->
			ok
	end.


%% 获取file_handle_cache进程中Items对应的关键信息
infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].


%% 获取file_handle_cache进程总的文件描述上限
i(total_limit,   #fhc_state{limit               = Limit}) -> Limit;

%% 获得file_handle_cache进程总的使用的文件描述数量
i(total_used,    State)                                   -> used(State);

%% 获取file_handle_cache进程中记录的socket的上限
i(sockets_limit, #fhc_state{obtain_limit        = Limit}) -> Limit;

%% 获取file_handle_cache进程中记录的socket使用的数量
i(sockets_used,  #fhc_state{obtain_count_socket = Count}) -> Count;

%% 不能识别的Item关键key
i(Item, _) -> throw({bad_argument, Item}).


%% 得到当前使用的句柄数
used(#fhc_state{open_count          = C1,
				obtain_count_socket = C2,
				obtain_count_file   = C3}) -> C1 + C2 + C3.

%%----------------------------------------------------------------------------
%% gen_server2 callbacks
%%----------------------------------------------------------------------------
%% file_handle_cache行为为gen_server2进程的回调初始化函数
init([AlarmSet, AlarmClear]) ->
	%% 统计信息的初始化
	file_handle_cache_stats:init(),
	Limit = case application:get_env(file_handles_high_watermark) of
				{ok, Watermark} when (is_integer(Watermark) andalso
										  Watermark > 0) ->
					Watermark;
				_ ->
					%% 得到当前操作系统上的能够打开的最大文件句柄数
					case ulimit() of
						unknown  -> ?FILE_HANDLES_LIMIT_OTHER;
						Lim      -> lists:max([2, Lim - ?RESERVED_FOR_OTHERS])
					end
			end,
	%% 根据Limit个能够打开的文件句柄上限得到当前RabbitMQ系统中能够obtain上限
	ObtainLimit = obtain_limit(Limit),
	%% 向error_logger日志服务器进程打印当前RabbitMQ系统能够打开的最大文件句柄数的日志
	error_logger:info_msg("Limiting to approx ~p file handles (~p sockets)~n",
						  [Limit, ObtainLimit]),
	%% 创建保存客户端状态的ETS表(存储每个client的信息，key是pid)
	Clients = ets:new(?CLIENT_ETS_TABLE, [set, private, {keypos, #cstate.pid}]),
	%% file_handle_cache_elders ETS {Pid,EldestUnusedSince}，存储每个client的pid和其最老的Handle打开时间，没有key(elders：长老)
	Elders = ets:new(?ELDERS_ETS_TABLE, [set, private]),
	{ok, #fhc_state { elders                = Elders,
					  limit                 = Limit,
					  open_count            = 0,
					  open_pending          = pending_new(),
					  obtain_limit          = ObtainLimit,
					  obtain_count_file     = 0,
					  obtain_pending_file   = pending_new(),
					  obtain_count_socket   = 0,
					  obtain_pending_socket = pending_new(),
					  clients               = Clients,
					  timer_ref             = undefined,
					  alarm_set             = AlarmSet,
					  alarm_clear           = AlarmClear }}.


prioritise_cast(Msg, _Len, _State) ->
	case Msg of
		{release, _, _, _}           -> 5;
		_                            -> 0
	end.


%% 处理打开文件的同步消息
handle_call({open, Pid, Requested, EldestUnusedSince}, From,
			State = #fhc_state { open_count   = Count,
								 open_pending = Pending,
								 elders       = Elders,
								 clients      = Clients })
  when EldestUnusedSince =/= undefined ->
	%% 更新客户端进程Pid现在对应的最老的句柄打开时间
	true = ets:insert(Elders, {Pid, EldestUnusedSince}),
	%% 组装等待结构
	Item = #pending { kind      = open,
					  pid       = Pid,
					  requested = Requested,
					  from      = From },
	%% 如果Pid对应的客户端是第一次打开文件句柄，则创建新的客户端数据结构
	ok = track_client(Pid, Clients),
	%% 判断是否需要减少打开的文件数
	case needs_reduce(State #fhc_state { open_count = Count + Requested }) of
		true  -> case ets:lookup(Clients, Pid) of
					 [#cstate { opened = 0 }] ->
						 %% 将当前客户端阻塞掉
						 true = ets:update_element(
								  Clients, Pid, {#cstate.blocked, true}),
						 {noreply,
						  %% 减少文件操作描述符
						  reduce(State #fhc_state {
												   %% 将操作项放入到open_pending等待队列中
												   open_pending = pending_in(Item, Pending) })};
					 %% 如果这个pid已经打开了很多文件，就返回close, client接到close后就soft_close所有之前打开的Handle
					 %% 所谓软关闭，就是将Handle对应文件buffer写完，sync后再关闭。软关闭所有之前打开的Handle后再重新试图open这个Handle
					 [#cstate { opened = Opened }] ->
						 %% 更新客户端pending_closes字段的数量，即Pid对应的客户端等待关闭的数量增加Opened(即将当前客户端的所有句柄全部关闭)
						 true = ets:update_element(
								  Clients, Pid,
								  {#cstate.pending_closes, Opened}),
						 {reply, close, State}
				 end;
		%% 如果Pid进程打开的总的文件数少于限制，则立刻执行等待操作的结构
		false -> {noreply, run_pending_item(Item, State)}
	end;


%% 处理客户端进程Pid对应的Obtain消息
handle_call({obtain, N, Type, Pid}, From,
			State = #fhc_state { clients = Clients }) ->
	%% 获得Type类型对应的总的数量
	Count = obtain_state(Type, count, State),
	%% 获得Type类型对应的等待队列
	Pending = obtain_state(Type, pending, State),
	%% 如果Pid对应的客户端是新客户端，则添加新的客户端数据
	ok = track_client(Pid, Clients),
	%% 组装等待操作结构
	Item = #pending { kind      = {obtain, Type}, pid  = Pid,
					  requested = N,              from = From },
	%% 如果obtain对应的Type超过上限则执行该函数
	Enqueue = fun () ->
					   %% 更新Pid对应的客户端状态为阻塞
					   true = ets:update_element(Clients, Pid,
												 {#cstate.blocked, true}),
					   %% 设置Type对应的最新阻塞队列
					   set_obtain_state(Type, pending,
										%% 往阻塞队列中添加阻塞结构
										pending_in(Item, Pending), State)
			  end,
	{noreply,
	 %% 判断obtain类型是否超过上限
	 case obtain_limit_reached(Type, State) of
		 %% Type对应的类型超过上限，则将操作项放入到等待队列中
		 true  -> Enqueue();
		 false -> case needs_reduce(
						 %% 设置Type对应的数量
						 set_obtain_state(Type, count, Count + 1, State)) of
					  true  -> reduce(Enqueue());
					  %% 根据新老状态来判断是否进行报警或者取消报警
					  false -> adjust_alarm(
								 %% 运行等待操作的操作项
								 State, run_pending_item(Item, State))
				  end
	 end};


%% 设置file_handle_cache进程中文件描述符的上限
handle_call({set_limit, Limit}, _From, State) ->
	%% 根据新老状态来判断是否进行报警或者取消报警
	{reply, ok, adjust_alarm(
	   %% 判断是否需要减少文件描述符，如果需要则进行描述符减少操作
	   State, maybe_reduce(
		 %% 处理等待队列
		 process_pending(
		   State #fhc_state {
							 limit        = Limit,
							 %% 根据总体上限limit得到obtain对应的上限
							 obtain_limit = obtain_limit(Limit) })))};


%% 获取file_handle_cache进程上的文件描述符上限
handle_call(get_limit, _From, State = #fhc_state { limit = Limit }) ->
	{reply, Limit, State};


%% 处理获取file_handle_cache进程中Items对应的关键信息
handle_call({info, Items}, _From, State) ->
	{reply, infos(Items, State), State}.


%% 注册回调函数的同时新创建一个客户端数据
handle_cast({register_callback, Pid, MFA},
			State = #fhc_state { clients = Clients }) ->
	%% 创建一个新的客户端信息到ETS表
	ok = track_client(Pid, Clients),
	%% 更新客户端结构中回调函数字段
	true = ets:update_element(Clients, Pid, {#cstate.callback, MFA}),
	{noreply, State};


%% 更新Pid客户端对应的最老的句柄打开时间
handle_cast({update, Pid, EldestUnusedSince},
			State = #fhc_state { elders = Elders })
  when EldestUnusedSince =/= undefined ->
	true = ets:insert(Elders, {Pid, EldestUnusedSince}),
	%% don't call maybe_reduce from here otherwise we can create a
	%% storm of messages
	{noreply, State};


%% 处理将Pid对应的客户端的obtain数量减去N
handle_cast({release, N, Type, Pid}, State) ->
	%% 先将Type对应的数量减去N，然后处理等待队列
	State1 = process_pending(update_counts({obtain, Type}, Pid, -N, State)),
	%% 根据新老状态来判断是否进行报警或者取消报警
	{noreply, adjust_alarm(State, State1)};


%% 关闭句柄，需要通知file_handle_cache进程减去文件描述符数量
handle_cast({close, Pid, EldestUnusedSince},
			State = #fhc_state { elders = Elders, clients = Clients }) ->
	%% 更新Pid进程最老的句柄打开时间
	true = case EldestUnusedSince of
			   undefined -> ets:delete(Elders, Pid);
			   _         -> ets:insert(Elders, {Pid, EldestUnusedSince})
		   end,
	%% 将Pid客户端对应的等待关闭的数量减一
	ets:update_counter(Clients, Pid, {#cstate.pending_closes, -1, 0, 0}),
	%% 更新open对应的文件描述符数量，然后处理等待队列，然后根据新老状态来判断是否需要报警或者解除报警
	{noreply, adjust_alarm(State, process_pending(
							 %% 更新open对应的文件描述符数量
							 update_counts(open, Pid, -1, State)))};


%% 处理将FromPid对应的客户端转化为ToPid对应的客户端
handle_cast({transfer, N, FromPid, ToPid}, State) ->
	%% 如果ToPid是一个新的客户端，则创建一个新的ToPid对应的客户端信息
	ok = track_client(ToPid, State#fhc_state.clients),
	{noreply, process_pending(
	   %% 将ToPid对应的obtain的个数增加N
	   update_counts({obtain, socket}, ToPid, +N,
					 %% 将FromPid对应的obtain的个数减去N
					 update_counts({obtain, socket}, FromPid, -N,
								   State)))}.


%% 处理通过定时器发送给自己的check_counts消息，用来检测file_handle_cache进程是否需要继续减少文件描述符
handle_info(check_counts, State) ->
	%% 判断是否需要减少文件描述符，如果需要则进行描述符减少操作
	{noreply, maybe_reduce(State #fhc_state { timer_ref = undefined })};


%% 处理客户端进程挂掉的消息
handle_info({'DOWN', _MRef, process, Pid, _Reason},
			State = #fhc_state { elders                = Elders,
								 open_count            = OpenCount,
								 open_pending          = OpenPending,
								 obtain_count_file     = ObtainCountF,
								 obtain_count_socket   = ObtainCountS,
								 obtain_pending_file   = ObtainPendingF,
								 obtain_pending_socket = ObtainPendingS,
								 clients               = Clients }) ->
	%% 取得Pid客户端进程对应的信息数据结构
	[#cstate { opened          = Opened,
			   obtained_file   = ObtainedFile,
			   obtained_socket = ObtainedSocket}] =
		ets:lookup(Clients, Pid),
	%% 从客户端ETS中删除掉Pid对应的信息
	true = ets:delete(Clients, Pid),
	%% 从elders ETS中删除掉Pid对应的信息
	true = ets:delete(Elders, Pid),
	%% 从等待队列中过滤出Pid对应的等待信息
	Fun = fun (#pending { pid = Pid1 }) -> Pid1 =/= Pid end,
	State1 = process_pending(
			   State #fhc_state {
								 open_count            = OpenCount - Opened,
								 %% 过滤掉open_pending字段中等待的结构
								 open_pending          = filter_pending(Fun, OpenPending),
								 obtain_count_file     = ObtainCountF - ObtainedFile,
								 obtain_count_socket   = ObtainCountS - ObtainedSocket,
								 %% 过滤掉obtain_pending_file字段中等待的结构
								 obtain_pending_file   = filter_pending(Fun, ObtainPendingF),
								 %% 过滤掉obtain_pending_socket字段中等待的结构
								 obtain_pending_socket = filter_pending(Fun, ObtainPendingS) }),
	{noreply, adjust_alarm(State, State1)}.


%% file_handle_cache进程中断糊掉函数
terminate(_Reason, State = #fhc_state { clients = Clients,
										elders  = Elders }) ->
	%% 将客户端的ETS表删除掉
	ets:delete(Clients),
	%% 将elders ETS表删除
	ets:delete(Elders),
	State.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------------
%% pending(等待) queue abstraction helpers
%%----------------------------------------------------------------------------
%% 对队列执行foldl操作
queue_fold(Fun, Init, Q) ->
    case queue:out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> queue_fold(Fun, Fun(V, Init), Q1)
    end.

%% 过滤等待队列中的数据
filter_pending(Fun, {Count, Queue}) ->
    {Delta, Queue1} =
        queue_fold(
          fun (Item = #pending { requested = Requested }, {DeltaN, QueueN}) ->
                  case Fun(Item) of
                      true  -> {DeltaN, queue:in(Item, QueueN)};
                      false -> {DeltaN - Requested, QueueN}
                  end
          end, {0, queue:new()}, Queue),
    {Count + Delta, Queue1}.

%% 创建新的等待数据结构
pending_new() ->
    {0, queue:new()}.

%% 往阻塞队列中添加阻塞结构
pending_in(Item = #pending { requested = Requested }, {Count, Queue}) ->
    {Count + Requested, queue:in(Item, Queue)}.

%% 从阻塞队列取出队列的头部元素
pending_out({0, _Queue} = Pending) ->
    {empty, Pending};
pending_out({N, Queue}) ->
    {{value, #pending { requested = Requested }} = Result, Queue1} =
        queue:out(Queue),
    {Result, {N - Requested, Queue1}}.

%% 获取阻塞队列的数量
pending_count({Count, _Queue}) ->
    Count.

%% 判断阻塞队列是否为空
pending_is_empty({0, _Queue}) ->
    true;
pending_is_empty({_N, _Queue}) ->
    false.

%%----------------------------------------------------------------------------
%% server helpers
%%----------------------------------------------------------------------------
%% 根据Limit个能够打开的文件句柄上限得到当前RabbitMQ系统中能够obtain上限
obtain_limit(infinity) -> infinity;
obtain_limit(Limit)    -> case ?OBTAIN_LIMIT(Limit) of
							  OLimit when OLimit < 0 -> 0;
							  OLimit                 -> OLimit
						  end.


%% 判断obtain类型是否超过上限
obtain_limit_reached(socket, State) -> obtain_limit_reached(State);
obtain_limit_reached(file,   State) -> needs_reduce(State).


obtain_limit_reached(#fhc_state{obtain_limit        = Limit,
								obtain_count_socket = Count}) ->
	Limit =/= infinity andalso Count >= Limit.


%% 获得obtain相关的结构
obtain_state(file,   count,   #fhc_state{obtain_count_file     = N}) -> N;
obtain_state(socket, count,   #fhc_state{obtain_count_socket   = N}) -> N;
obtain_state(file,   pending, #fhc_state{obtain_pending_file   = N}) -> N;
obtain_state(socket, pending, #fhc_state{obtain_pending_socket = N}) -> N.


%% 设置obtain相关的结构
set_obtain_state(file,   count,   N, S) -> S#fhc_state{obtain_count_file = N};
set_obtain_state(socket, count,   N, S) -> S#fhc_state{obtain_count_socket = N};
set_obtain_state(file,   pending, N, S) -> S#fhc_state{obtain_pending_file = N};
set_obtain_state(socket, pending, N, S) -> S#fhc_state{obtain_pending_socket = N}.


%% 根据新老状态来判断是否进行报警或者取消报警
adjust_alarm(OldState = #fhc_state { alarm_set   = AlarmSet,
									 alarm_clear = AlarmClear }, NewState) ->
	case {obtain_limit_reached(OldState), obtain_limit_reached(NewState)} of
		%% 如果老的状态为非超过上限，新的状态超过上限，则调用AlarmSet函数进行报警
		{false, true} -> AlarmSet({file_descriptor_limit, []});
		%% 如果老的状态为超过上限，新的状态非超过上限，则调用AlarmClear函数清除报警
		{true, false} -> AlarmClear(file_descriptor_limit);
		_             -> ok
	end,
	NewState.


%% 处理等待队列
process_pending(State = #fhc_state { limit = infinity }) ->
	State;
process_pending(State) ->
	process_open(process_obtain(socket, process_obtain(file, State))).


%% 处理open对应的等待队列
process_open(State = #fhc_state { limit        = Limit,
								  open_pending = Pending}) ->
	{Pending1, State1} = process_pending(Pending, Limit - used(State), State),
	State1 #fhc_state { open_pending = Pending1 }.


process_obtain(Type, State = #fhc_state { limit        = Limit,
										  obtain_limit = ObtainLimit }) ->
	%% 拿到Type对应的数量
	ObtainCount = obtain_state(Type, count, State),
	%% 拿到Type对应的等待队列
	Pending = obtain_state(Type, pending, State),
	%% 获得当前总的剩余可以增加的文件数
	Quota = case Type of
				file   -> Limit - (used(State));
				socket -> lists:min([ObtainLimit - ObtainCount,
									 Limit - (used(State))])
			end,
	{Pending1, State1} = process_pending(Pending, Quota, State),
	%% 设置Type对应的obtain数据信息
	set_obtain_state(Type, pending, Pending1, State1).


%% 处理Pending等待队列
process_pending(Pending, Quota, State) when Quota =< 0 ->
	{Pending, State};
process_pending(Pending, Quota, State) ->
	case pending_out(Pending) of
		{empty, _Pending} ->
			{Pending, State};
		{{value, #pending { requested = Requested }}, _Pending1}
		  when Requested > Quota ->
			{Pending, State};
		{{value, #pending { requested = Requested } = Item}, Pending1} ->
			%% 继续后续的处理等待队列
			process_pending(Pending1, Quota - Requested,
							%% 运行等待操作的操作项
							run_pending_item(Item, State))
	end.


%% 运行等待操作的操作项
run_pending_item(#pending { kind      = Kind,
							pid       = Pid,
							requested = Requested,
							from      = From },
				 State = #fhc_state { clients = Clients }) ->
	%% 在此处向客户端进程发送成功的消息
	gen_server2:reply(From, ok),
	%% 更新客户端的状态为非阻塞状态
	true = ets:update_element(Clients, Pid, {#cstate.blocked, false}),
	%% 根据Kind类型更新统计数据信息
	update_counts(Kind, Pid, Requested, State).


%% 更新file_handle_cache进程中的打开数量信息以及客户端信息中的打开数量信息
update_counts(Kind, Pid, Delta,
			  State = #fhc_state { open_count          = OpenCount,
								   obtain_count_file   = ObtainCountF,
								   obtain_count_socket = ObtainCountS,
								   clients             = Clients }) ->
	%% 更Pid进程新客户端的打开数量
	{OpenDelta, ObtainDeltaF, ObtainDeltaS} =
		update_counts1(Kind, Pid, Delta, Clients),
	%% 更新file_handle_cache进程总的文件或者socket等的打开数
	State #fhc_state { open_count          = OpenCount    + OpenDelta,
					   obtain_count_file   = ObtainCountF + ObtainDeltaF,
					   obtain_count_socket = ObtainCountS + ObtainDeltaS }.


%% 更Pid进程新客户端的打开数量
update_counts1(open, Pid, Delta, Clients) ->
	ets:update_counter(Clients, Pid, {#cstate.opened, Delta}),
	{Delta, 0, 0};
update_counts1({obtain, file}, Pid, Delta, Clients) ->
	ets:update_counter(Clients, Pid, {#cstate.obtained_file, Delta}),
	{0, Delta, 0};
update_counts1({obtain, socket}, Pid, Delta, Clients) ->
	ets:update_counter(Clients, Pid, {#cstate.obtained_socket, Delta}),
	{0, 0, Delta}.


%% 判断是否需要减少文件描述符，如果需要则进行描述符减少操作
maybe_reduce(State) ->
	case needs_reduce(State) of
		true  -> reduce(State);
		false -> State
	end.


%% 判断是否需要减少打开的文件数
needs_reduce(State = #fhc_state { limit                 = Limit,
								  open_pending          = OpenPending,
								  obtain_limit          = ObtainLimit,
								  obtain_count_socket   = ObtainCountS,
								  obtain_pending_file   = ObtainPendingF,
								  obtain_pending_socket = ObtainPendingS }) ->
	Limit =/= infinity
		andalso ((used(State) > Limit)
					 orelse (not pending_is_empty(OpenPending))
					 orelse (not pending_is_empty(ObtainPendingF))
					 orelse (ObtainCountS < ObtainLimit
								 andalso not pending_is_empty(ObtainPendingS))).


%% file_handle_cache进程中文件描述超过上限需要减少文件描述符
reduce(State = #fhc_state { open_pending          = OpenPending,
							obtain_pending_file   = ObtainPendingFile,
							obtain_pending_socket = ObtainPendingSocket,
							elders                = Elders,
							clients               = Clients,
							timer_ref             = TRef }) ->
	Now = now(),
	%% CStates		：			表示所有打开的文件句柄的客户端结构列表
	%% Sum			：			表示所有客户端最老的句柄打开时间到现在的时间差的和
	%% ClientCount	：			表示所有的客户端数量
	{CStates, Sum, ClientCount} =
		ets:foldl(fun ({Pid, Eldest}, {CStatesAcc, SumAcc, CountAcc} = Accs) ->
						   [#cstate { pending_closes = PendingCloses,
									  opened         = Opened,
									  blocked        = Blocked } = CState] =
							   ets:lookup(Clients, Pid),
						   case Blocked orelse PendingCloses =:= Opened of
							   true  -> Accs;
							   false -> {[CState | CStatesAcc],
										 SumAcc + timer:now_diff(Now, Eldest),
										 CountAcc + 1}
						   end
				  end, {[], 0, 0}, Elders),
	case CStates of
		[] -> ok;
		_  -> case (Sum / ClientCount) -
					   (1000 * ?FILE_HANDLES_CHECK_INTERVAL) of
				  %% 如果平均值大于两秒，说明有很多老文件还未关闭, 就对每个client call 清理函数。参数是最老时间的平均值。如果没有注册清理函数就不管。
				  %% 将CStates列表中的所有客户端中的所有句柄打开时间超过AverageAge的句柄关闭掉
				  AverageAge when AverageAge > 0 ->
					  notify_age(CStates, AverageAge);
				  %% 如果平均值小于两秒，说明没有很多未关闭的老文件。找到所有注册清理函数的pid, 假设有N个Open被Block，就用其中N个清理函数去清理N个已打开的Handle
				  _ ->
					  notify_age0(Clients, CStates,
								  %% 得到所有等待队列数量之和
								  pending_count(OpenPending) +
									  pending_count(ObtainPendingFile) +
									  pending_count(ObtainPendingSocket))
			  end
	end,
	case TRef of
		%% 如果没有定时器，则启动定时器，间隔2秒向自己发送check_counts消息
		undefined -> TRef1 = erlang:send_after(
							   ?FILE_HANDLES_CHECK_INTERVAL, ?SERVER,
							   check_counts),
					 State #fhc_state { timer_ref = TRef1 };
		_         -> State
	end.


%% 将CStates列表中的所有客户端中的所有句柄打开时间超过AverageAge的句柄关闭掉
notify_age(CStates, AverageAge) ->
	lists:foreach(
	  fun (#cstate { callback = undefined }) -> ok;
		 (#cstate { callback = {M, F, A} }) -> apply(M, F, A ++ [AverageAge])
	  end, CStates).


%% 将有回调的客户端列表中减去所有等待句柄的数量后为0之前的客户端所有的文件句柄全部删除掉
notify_age0(Clients, CStates, Required) ->
	case [CState || CState <- CStates, CState#cstate.callback =/= undefined] of
		[]            -> ok;
		Notifications -> S = random:uniform(length(Notifications)),
						 {L1, L2} = lists:split(S, Notifications),
						 notify(Clients, Required, L2 ++ L1)
	end.


%% 将_Required文件句柄删除掉
notify(_Clients, _Required, []) ->
	ok;
notify(_Clients, Required, _Notifications) when Required =< 0 ->
	ok;
notify(Clients, Required, [#cstate{ pid      = Pid,
									callback = {M, F, A},
									opened   = Opened } | Notifications]) ->
	%% 通知该客户端将自己所有的句柄关闭掉
	apply(M, F, A ++ [0]),
	%% 更新当前客户端等待关闭的文件句柄数量
	ets:update_element(Clients, Pid, {#cstate.pending_closes, Opened}),
	notify(Clients, Required - Opened, Notifications).


%% track:追踪
%% 创建一个新的客户端信息到ETS表
track_client(Pid, Clients) ->
	case ets:insert_new(Clients, #cstate { pid             = Pid,
										   callback        = undefined,
										   opened          = 0,
										   obtained_file   = 0,
										   obtained_socket = 0,
										   blocked         = false,
										   pending_closes  = 0 }) of
		true  -> %% 监视客户端进程
				_MRef = erlang:monitor(process, Pid),
				ok;
		false -> ok
	end.


%% To increase the number of file descriptors: on Windows set ERL_MAX_PORTS
%% environment variable, on Linux set `ulimit -n`.
%% 得到当前RabbitMQ系统的能够打开的最大文件句柄数
ulimit() ->
	case proplists:get_value(max_fds, erlang:system_info(check_io)) of
		MaxFds when is_integer(MaxFds) andalso MaxFds > 1 ->
			case os:type() of
				{win32, _OsName} ->
					%% On Windows max_fds is twice the number of open files:
					%%   https://github.com/yrashk/erlang/blob/e1282325ed75e52a98d5/erts/emulator/sys/win32/sys.c#L2459-2466
					MaxFds div 2;
				_Any ->
					%% For other operating systems trust Erlang.
					MaxFds
			end;
		_ ->
			unknown
	end.
