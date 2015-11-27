%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_db).

-include("rabbit_mgmt.hrl").
-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0]).

-export([augment_exchanges/3, augment_queues/3,
         augment_nodes/2, augment_vhosts/2,
         get_channel/2, get_connection/2,
         get_all_channels/1, get_all_connections/1,
         get_all_consumers/0, get_all_consumers/1,
         get_overview/2, get_overview/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1,
         prioritise_cast/3, prioritise_call/4, format_message_queue/2]).

%% For testing
-export([override_lookups/1, reset_lookups/0]).

-import(rabbit_misc, [pget/3, pset/3]).

%% The management database listens to events broadcast via the
%% rabbit_event mechanism, and responds to queries from the various
%% rabbit_mgmt_wm_* modules. It handles several kinds of events, and
%% slices and dices them in various ways.
%%
%% There are three types of events coming in: created (when an object
%% is created, containing immutable facts about it), stats (emitted on
%% a timer, with mutable facts about the object), and deleted (just
%% containing the object's ID). In this context "objects" means
%% connections, channels, exchanges, queues, consumers, vhosts and
%% nodes. Note that we do not care about users, permissions, bindings,
%% parameters or policies.
%%
%% Connections and channels are identified by pids. Queues and
%% exchanges are identified by names (which are #resource{}s). VHosts
%% and nodes are identified by names which are binaries. And consumers
%% are identified by {ChPid, QName, CTag}.
%%
%% The management database records the "created" events for
%% connections, channels and consumers, and can thus be authoritative
%% about those objects. For queues, exchanges and nodes we go to
%% Mnesia to find out the immutable details of the objects.
%%
%% For everything other than consumers, the database can then augment
%% these immutable details with stats, as the object changes. (We
%% never emit anything very interesting about consumers).
%%
%% Stats on the inbound side are refered to as coarse- and
%% fine-grained. Fine grained statistics are the message rates
%% maintained by channels and associated with tuples: {publishing
%% channel, exchange}, {publishing channel, exchange, queue} and
%% {queue, consuming channel}. Coarse grained stats are everything
%% else and are associated with only one object, not a tuple.
%%
%% Within the management database though we rearrange things a bit: we
%% refer to basic stats, simple stats and detail stats.
%%
%% Basic stats are those coarse grained stats for which we do not
%% retain a history and do not perform any calculations -
%% e.g. connection.state or channel.prefetch_count.
%%
%% Simple stats are those for which we do history / calculations which
%% are associated with one object *after aggregation* - so these might
%% originate with coarse grained stats - e.g. connection.send_oct or
%% queue.messages_ready. But they might also originate from fine
%% grained stats which have been aggregated - e.g. the message rates
%% for a vhost or queue.
%%
%% Finally, detailed stats are those for which we do history /
%% calculations which are associated with two objects. These
%% have to have originated as fine grained stats, but can still have
%% been aggregated.
%%
%% Created events and basic stats are stored in ETS tables by object,
%% looked up in an orddict in #state.tables. Simple and detailed stats
%% (which only differ depending on how they're keyed) are stored in
%% #state.aggregated_stats.
%%
%% For detailed stats we also store an index for each object referencing
%% all the other objects that form a detailed stats key with it. This is
%% so that we can always avoid table scanning while deleting stats and
%% thus make sure that handling deleted events is O(n)-ish.
%%
%% For each key for simple and detailed stats we maintain a #stats{}
%% record, essentially a base counter for everything that happened
%% before the samples we have kept, and a gb_tree of {timestamp,
%% sample} values.
%%
%% We also have #state.old_stats to let us calculate instantaneous
%% rates, in order to apportion simple / detailed stats into time
%% slices as they come in. These instantaneous rates are not returned
%% in response to any query, the rates shown in the API are calculated
%% at query time. old_stats contains both coarse and fine
%% entries. Coarse entries are pruned when the corresponding object is
%% deleted, and fine entries are pruned when the emitting channel is
%% closed, and whenever we receive new fine stats from a channel. So
%% it's quite close to being a cache of "the previous stats we
%% received".
%%
%% We also keep a timer going, in order to prune old samples from
%% #state.aggregated_stats.
%%
%% Overall the object is to do all the aggregation when events come
%% in, and make queries be simple lookups as much as possible. One
%% area where this does not happen is the global overview - which is
%% aggregated from vhost stats at query time since we do not want to
%% reveal anything about other vhosts to unprivileged users.

-record(state, {
				%% "stats" for which no calculations are required
				tables,										%% 所有统计新的ETS表名的orddict数据结构
				%% database of aggregated samples
				aggregated_stats,							%% 存储统计信息的ETS
				%% index for detailed aggregated_stats that have 2-tuple keys
				aggregated_stats_index,						%% 存储统计信息索引的ETS
				%% What the previous info item was for any given
				%% {queue/channel/connection}
				old_stats,									%% 老数据ETS，服务于统计信息
				gc_timer,
				gc_next_key,
				lookups,									%% 查询函数列表
				interval,									%% 统计信息刷新时间间隔
				event_refresh_ref,
				rates_mode									%% 当前模式
			   }).

-define(FINE_STATS_TYPES, [channel_queue_stats, channel_exchange_stats,
						   channel_queue_exchange_stats]).

-define(TABLES, [queue_stats, connection_stats, channel_stats,
				 consumers_by_queue, consumers_by_channel,
				 node_stats, node_node_stats]).

-define(DELIVER_GET, [deliver, deliver_no_ack, get, get_no_ack]).

-define(FINE_STATS, [publish, publish_in, publish_out,
					 ack, deliver_get, confirm, return_unroutable, redeliver] ++
			?DELIVER_GET).

%% Most come from channels as fine stats, but queues emit these directly.
%% 队列中消息速度相关的关键key列表
-define(QUEUE_MSG_RATES, [disk_reads, disk_writes]).

-define(MSG_RATES, ?FINE_STATS ++ ?QUEUE_MSG_RATES).

%% 队列消息数量相关的关键key列表
-define(QUEUE_MSG_COUNTS, [messages, messages_ready, messages_unacknowledged]).

-define(COARSE_NODE_STATS,
		[mem_used, fd_used, sockets_used, proc_used, disk_free,
		 io_read_count,  io_read_bytes,  io_read_avg_time,
		 io_write_count, io_write_bytes, io_write_avg_time,
		 io_sync_count,  io_sync_avg_time,
		 io_seek_count,  io_seek_avg_time,
		 io_reopen_count, mnesia_ram_tx_count,  mnesia_disk_tx_count,
		 msg_store_read_count, msg_store_write_count,
		 queue_index_journal_write_count,
		 queue_index_write_count, queue_index_read_count]).

-define(COARSE_NODE_NODE_STATS, [send_bytes, recv_bytes]).

%% Normally 0 and no history means "has never happened, don't
%% report". But for these things we do want to report even at 0 with
%% no history.
%% 每次都需要报告的关键key列表
-define(ALWAYS_REPORT_STATS,
		[io_read_avg_time, io_write_avg_time,
		 io_sync_avg_time | ?QUEUE_MSG_COUNTS]).

-define(COARSE_CONN_STATS, [recv_oct, send_oct]).

-define(GC_INTERVAL, 5000).
-define(GC_MIN_ROWS, 100).
-define(GC_MIN_RATIO, 0.01).

-define(DROP_LENGTH, 1000).

prioritise_cast({event, #event{type  = Type,
							   props = Props}}, Len, _State)
  when (Type =:= channel_stats orelse
			Type =:= queue_stats) andalso Len > ?DROP_LENGTH ->
	case pget(idle_since, Props) of
		unknown -> drop;
		_       -> 0
	end;
prioritise_cast(_Msg, _Len, _State) ->
	0.

%% We want timely replies to queries even when overloaded, so return 5
%% as priority. Also we only have access to the queue length here, not
%% in handle_call/3, so stash it in the dictionary. This is a bit ugly
%% but better than fiddling with gen_server2 even more.
prioritise_call(_Msg, _From, Len, _State) ->
	put(last_queue_length, Len),
	5.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------
%% rabbit_mgmt_db进程启动入口函数
start_link() ->
	Ref = make_ref(),
	case gen_server2:start_link({global, ?MODULE}, ?MODULE, [Ref], []) of
		{ok, Pid} -> register(?MODULE, Pid), %% [1]
					 rabbit:force_event_refresh(Ref),
					 {ok, Pid};
		Else      -> Else
	end.
%% [1] For debugging it's helpful to locally register the name too
%% since that shows up in places global names don't.

%% R = Ranges, M = Mode
%% 根据M模式列出Xs列表中所有的exchange信息
augment_exchanges(Xs, R, M) -> safe_call({augment_exchanges, Xs, R, M}, Xs).


%% 根据M模式列出Qs列表中队列的信息
augment_queues(Qs, R, M)    -> safe_call({augment_queues, Qs, R, M}, Qs).


%% 列出VHosts列表中的所有VHost相关信息
augment_vhosts(VHosts, R)   -> safe_call({augment_vhosts, VHosts, R}, VHosts).


%% 列出Nodes列表中所有节点相关信息
augment_nodes(Nodes, R)     -> safe_call({augment_nodes, Nodes, R}, Nodes).


get_channel(Name, R)        -> safe_call({get_channel, Name, R}, not_found).
get_connection(Name, R)     -> safe_call({get_connection, Name, R}, not_found).

get_all_channels(R)         -> safe_call({get_all_channels, R}).
get_all_connections(R)      -> safe_call({get_all_connections, R}).

get_all_consumers()         -> safe_call({get_all_consumers, all}).
get_all_consumers(V)        -> safe_call({get_all_consumers, V}).

get_overview(User, R)       -> safe_call({get_overview, User, R}).
get_overview(R)             -> safe_call({get_overview, all, R}).

override_lookups(Lookups)   -> safe_call({override_lookups, Lookups}).
reset_lookups()             -> safe_call(reset_lookups).


%% call到rabbit_mgmt_db进程获取信息的统一接口
safe_call(Term)          -> safe_call(Term, []).
safe_call(Term, Default) -> safe_call(Term, Default, 1).

%% See rabbit_mgmt_sup_sup for a discussion of the retry logic.
%% 向rabbit_mgmt_db进程进行call操作的统一接口
safe_call(Term, Default, Retries) ->
    rabbit_misc:with_exit_handler(
      fun () ->
              case Retries of
                  0 -> Default;
                  _ -> rabbit_mgmt_sup_sup:start_child(),
                       safe_call(Term, Default, Retries - 1)
              end
      end,
      fun () -> gen_server2:call({global, ?MODULE}, Term, infinity) end).

%%----------------------------------------------------------------------------
%% Internal, gen_server2 callbacks
%%----------------------------------------------------------------------------
%% rabbit_mgmt_db进程启动回调初始化函数
init([Ref]) ->
	%% When Rabbit is overloaded, it's usually especially important
	%% that the management plugin work.
	process_flag(priority, high),
	%% 得到rabbit应用中统计数据刷新时间
	{ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
	{ok, RatesMode} = application:get_env(rabbitmq_management, rates_mode),
	%% 向rabbit_node_monitor进程订阅信息
	rabbit_node_monitor:subscribe(self()),
	%% 打印rabbit_mgmt_db进程统计数据库启动
	rabbit_log:info("Statistics database started.~n"),
	%% 创建rabbit_mgmt_db进程中名字为rabbit_mgmt_db的ETS函数
	Table = fun () -> ets:new(rabbit_mgmt_db, [ordered_set]) end,
	Tables = orddict:from_list([{Key, Table()} || Key <- ?TABLES]),
	%% 启动一个5秒的垃圾回收定时器
	{ok, set_gc_timer(
	   %% 设置交换机exchange和队列queue的查询函数
	   reset_lookups(
		 #state{interval               = Interval,
				tables                 = Tables,
				old_stats              = Table(),
				aggregated_stats       = Table(),
				aggregated_stats_index = Table(),
				event_refresh_ref      = Ref,
				rates_mode             = RatesMode})), hibernate,
	 {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


handle_call({augment_exchanges, Xs, Ranges, basic}, _From, State) ->
    reply(list_exchange_stats(Ranges, Xs, State), State);

handle_call({augment_exchanges, Xs, Ranges, full}, _From, State) ->
    reply(detail_exchange_stats(Ranges, Xs, State), State);

handle_call({augment_queues, Qs, Ranges, basic}, _From, State) ->
    reply(list_queue_stats(Ranges, Qs, State), State);

handle_call({augment_queues, Qs, Ranges, full}, _From, State) ->
    reply(detail_queue_stats(Ranges, Qs, State), State);

handle_call({augment_vhosts, VHosts, Ranges}, _From, State) ->
    reply(vhost_stats(Ranges, VHosts, State), State);

handle_call({augment_nodes, Nodes, Ranges}, _From, State) ->
    {reply, node_stats(Ranges, Nodes, State), State};

handle_call({get_channel, Name, Ranges}, _From,
            State = #state{tables = Tables}) ->
    case created_event(Name, channel_stats, Tables) of
        not_found -> reply(not_found, State);
        Ch        -> [Result] = detail_channel_stats(Ranges, [Ch], State),
                     reply(Result, State)
    end;

handle_call({get_connection, Name, Ranges}, _From,
            State = #state{tables = Tables}) ->
    case created_event(Name, connection_stats, Tables) of
        not_found -> reply(not_found, State);
        Conn      -> [Result] = connection_stats(Ranges, [Conn], State),
                     reply(Result, State)
    end;

handle_call({get_all_channels, Ranges}, _From,
            State = #state{tables = Tables}) ->
    Chans = created_events(channel_stats, Tables),
    reply(list_channel_stats(Ranges, Chans, State), State);

handle_call({get_all_connections, Ranges}, _From,
            State = #state{tables = Tables}) ->
    Conns = created_events(connection_stats, Tables),
    reply(connection_stats(Ranges, Conns, State), State);

handle_call({get_all_consumers, VHost},
            _From, State = #state{tables = Tables}) ->
    All = ets:tab2list(orddict:fetch(consumers_by_queue, Tables)),
    {reply, [augment_msg_stats(
               augment_consumer(Obj), State) ||
                {{#resource{virtual_host = VHostC}, _Ch, _CTag}, Obj} <- All,
                VHost =:= all orelse VHost =:= VHostC], State};

handle_call({get_overview, User, Ranges}, _From,
            State = #state{tables = Tables}) ->
    VHosts = case User of
                 all -> rabbit_vhost:list();
                 _   -> rabbit_mgmt_util:list_visible_vhosts(User)
             end,
    %% TODO: there's no reason we can't do an overview of send_oct and
    %% recv_oct now!
    VStats = [read_simple_stats(vhost_stats, VHost, State) ||
                 VHost <- VHosts],
    MessageStats = [overview_sum(Type, VStats) || Type <- ?MSG_RATES],
    QueueStats = [overview_sum(Type, VStats) || Type <- ?QUEUE_MSG_COUNTS],
    F = case User of
            all -> fun (L) -> length(L) end;
            _   -> fun (L) -> length(rabbit_mgmt_util:filter_user(L, User)) end
        end,
    %% Filtering out the user's consumers would be rather expensive so let's
    %% just not show it
    Consumers = case User of
                    all -> Table = orddict:fetch(consumers_by_queue, Tables),
                           [{consumers, ets:info(Table, size)}];
                    _   -> []
                end,
    ObjectTotals = Consumers ++
        [{queues,      length([Q || V <- VHosts,
                                    Q <- rabbit_amqqueue:list(V)])},
         {exchanges,   length([X || V <- VHosts,
                                    X <- rabbit_exchange:list(V)])},
         {connections, F(created_events(connection_stats, Tables))},
         {channels,    F(created_events(channel_stats, Tables))}],
    reply([{message_stats, format_samples(Ranges, MessageStats, State)},
           {queue_totals,  format_samples(Ranges, QueueStats, State)},
           {object_totals, ObjectTotals},
           {statistics_db_event_queue, get(last_queue_length)}], State);

handle_call({override_lookups, Lookups}, _From, State) ->
    reply(ok, State#state{lookups = Lookups});

handle_call(reset_lookups, _From, State) ->
    reply(ok, reset_lookups(State));

%% Used in rabbit_mgmt_test_db where we need guarantees events have
%% been handled before querying
handle_call({event, Event = #event{reference = none}}, _From, State) ->
    handle_event(Event, State),
    reply(ok, State);

handle_call(_Request, _From, State) ->
    reply(not_understood, State).

%% Only handle events that are real, or pertain to a force-refresh
%% that we instigated.
handle_cast({event, Event = #event{reference = none}}, State) ->
    handle_event(Event, State),
    noreply(State);

handle_cast({event, Event = #event{reference = Ref}},
            State = #state{event_refresh_ref = Ref}) ->
    handle_event(Event, State),
    noreply(State);

handle_cast(_Request, State) ->
    noreply(State).

handle_info(gc, State) ->
    noreply(set_gc_timer(gc_batch(State)));

handle_info({node_down, Node}, State = #state{tables = Tables}) ->
    Conns = created_events(connection_stats, Tables),
    Chs = created_events(channel_stats, Tables),
    delete_all_from_node(connection_closed, Node, Conns, State),
    delete_all_from_node(channel_closed, Node, Chs, State),
    noreply(State);

handle_info(_Info, State) ->
    noreply(State).

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

reply(Reply, NewState) -> {reply, Reply, NewState, hibernate}.
noreply(NewState) -> {noreply, NewState, hibernate}.


%% 启动一个5秒的垃圾回收定时器
set_gc_timer(State) ->
	TRef = erlang:send_after(?GC_INTERVAL, self(), gc),
	State#state{gc_timer = TRef}.


%% 设置交换机exchange和队列queue的查询函数
reset_lookups(State) ->
	State#state{lookups = [{exchange, fun rabbit_exchange:lookup/1},
						   {queue,    fun rabbit_amqqueue:lookup/1}]}.


handle_pre_hibernate(State) ->
	%% rabbit_event can end up holding on to some memory after a busy
	%% workout, but it's not a gen_server so we can't make it
	%% hibernate. The best we can do is forcibly GC it here (if
	%% rabbit_mgmt_db is hibernating the odds are rabbit_event is
	%% quiescing in some way too).
	rpc:multicall(
	  rabbit_mnesia:cluster_nodes(running), rabbit_mgmt_db_handler, gc, []),
	{hibernate, State}.


format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).


delete_all_from_node(Type, Node, Items, State) ->
	[case node(Pid) of
		 Node -> handle_event(#event{type = Type, props = [{pid, Pid}]}, State);
		 _    -> ok
	 end || Item <- Items, Pid <- [pget(pid, Item)]].

%%----------------------------------------------------------------------------
%% Internal, utilities
%%----------------------------------------------------------------------------

pget(Key, List) -> pget(Key, List, unknown).

%% id_name() and id() are for use when handling events, id_lookup()
%% for when augmenting. The difference is that when handling events a
%% queue name will be a resource, but when augmenting we will be
%% passed a queue proplist that will already have been formatted -
%% i.e. it will have name and vhost keys.
id_name(node_stats)       -> name;
id_name(node_node_stats)  -> route;
id_name(vhost_stats)      -> name;
id_name(queue_stats)      -> name;
id_name(exchange_stats)   -> name;
id_name(channel_stats)    -> pid;
id_name(connection_stats) -> pid.


%% 根据Type类型拿到对应的关键key对应的值(即拿到数据在ETS里面的ID)
id(Type, List) -> pget(id_name(Type), List).


%% 组装队列或者交换机的资源数据结构
id_lookup(queue_stats, List) ->
	rabbit_misc:r(pget(vhost, List), queue, pget(name, List));
id_lookup(exchange_stats, List) ->
	rabbit_misc:r(pget(vhost, List), exchange, pget(name, List));
id_lookup(Type, List) ->
	id(Type, List).


%% 从Table这个ETS表里查询Key对应的数据信息
lookup_element(Table, Key) -> lookup_element(Table, Key, 2).


%% 返回Table表中id为key的第Pos位置的元素
lookup_element(Table, Key, Pos) ->
	try ets:lookup_element(Table, Key, Pos)
	catch error:badarg -> []
	end.


fine_stats_id(ChPid, {Q, X}) -> {ChPid, Q, X};
fine_stats_id(ChPid, QorX)   -> {ChPid, QorX}.


%% 将时间TS根据单位Interval进行向下取整
floor(TS, #state{interval = Interval}) ->
	rabbit_mgmt_util:floor(rabbit_mgmt_format:now_to_ms(TS), Interval).


%% 将时间TS根据单位Interval进行向上取整
ceil(TS, #state{interval = Interval}) ->
	rabbit_mgmt_util:ceil (rabbit_mgmt_format:now_to_ms(TS), Interval).


%% 组装detail关键Key
details_key(Key) -> list_to_atom(atom_to_list(Key) ++ "_details").

%%----------------------------------------------------------------------------
%% Internal, event-receiving side
%%----------------------------------------------------------------------------
%% 处理队列状态事件
handle_event(#event{type = queue_stats, props = Stats, timestamp = Timestamp},
			 State) ->
	%% 进行状态的统计
	handle_stats(queue_stats, Stats, Timestamp,
				 [{fun rabbit_mgmt_format:properties/1, [backing_queue_status]},
				  {fun rabbit_mgmt_format:now_to_str/1, [idle_since]},
				  {fun rabbit_mgmt_format:queue_state/1, [state]}],
				 %% 消息队列的消息数量相关的需要单独特殊统计(且要统计到对应的VHost下)，而写和读磁盘的数量只需要统计到对应的队列下
				 ?QUEUE_MSG_COUNTS, ?QUEUE_MSG_RATES, State);

%% 处理队列被删除的事件
handle_event(Event = #event{type = queue_deleted,
							props = [{name, Name}],
							timestamp = Timestamp},
			 State = #state{old_stats = OldTable}) ->
	%% 删除队列对应的消费者
	delete_consumers(Name, consumers_by_queue, consumers_by_channel, State),
	%% This is fiddly. Unlike for connections and channels, we need to
	%% decrease any amalgamated coarse stats for [messages,
	%% messages_ready, messages_unacknowledged] for this queue - since
	%% the queue's deletion means we have really got rid of messages!
	Id = {coarse, {queue_stats, Name}},
	%% This ceil must correspond to the ceil in append_samples/5
	TS = ceil(Timestamp, State),
	OldStats = lookup_element(OldTable, Id),
	%% 将消息队列的消息直接全部减去
	[record_sample(Id, {Key, -pget(Key, OldStats, 0), TS, State}, true, State)
	   || Key <- ?QUEUE_MSG_COUNTS],
	%% 删除channel对queue相关的统计数据
	delete_samples(channel_queue_stats,  {'_', Name}, State),
	%% 删除queue对exchange相关的统计数据
	delete_samples(queue_exchange_stats, {Name, '_'}, State),
	%% 将队列状态的各个数据从aggregated_stats这个ETS表里删除掉
	delete_samples(queue_stats,          Name,        State),
	%% 删除队列对象在对应ETS表里的数据
	handle_deleted(queue_stats, Event, State);

%% 处理exchange交换机被删除的事件
handle_event(Event = #event{type = exchange_deleted,
							props = [{name, Name}]}, State) ->
	%% 删除channel对exchange相关统计数据
	delete_samples(channel_exchange_stats,  {'_', Name}, State),
	%% 删除queue对exchange相关的统计数据
	delete_samples(queue_exchange_stats,    {'_', Name}, State),
	%% 删除exchange状态数据
	delete_samples(exchange_stats,          Name,        State),
	%% 删除exchange对象数据
	handle_deleted(exchange_stats, Event, State);

%% 处理vhost被删除的事件
handle_event(#event{type = vhost_deleted,
					props = [{name, Name}]}, State) ->
	%% 删除Vhost对应的相关统计信息
	delete_samples(vhost_stats, Name, State);

%% 处理connection连接被创建的事件
handle_event(#event{type = connection_created, props = Stats}, State) ->
	handle_created(
	  connection_stats, Stats,
	  [%% 打印IP地址
	   {fun rabbit_mgmt_format:addr/1,         [host, peer_host]},
	   %% 打印端口
	   {fun rabbit_mgmt_format:port/1,         [port, peer_port]},
	   %% 打印当前RabbitMQ系统使用的AMQP协议版本号
	   {fun rabbit_mgmt_format:protocol/1,     [protocol]},
	   %% 打印客户端特性
	   {fun rabbit_mgmt_format:amqp_table/1,   [client_properties]}], State);

%% 处理connection连接状态的事件
handle_event(#event{type = connection_stats, props = Stats,
					timestamp = Timestamp},
			 State) ->
	%% 处理连接进程状态信息
	handle_stats(connection_stats, Stats, Timestamp, [], ?COARSE_CONN_STATS,
				 State);

%% 处理connection连接被关闭的事件
handle_event(Event = #event{type  = connection_closed,
							props = [{pid, Pid}]}, State) ->
	%% 删除连接进程相同的统计信息
	delete_samples(connection_stats, Pid, State),
	%% 删除连接进程对象
	handle_deleted(connection_stats, Event, State);

%% 处理channel被创建的事件
handle_event(#event{type = channel_created, props = Stats}, State) ->
	%% 处理channel被创建的统计信息
	handle_created(channel_stats, Stats, [], State);

%% 处理channel状态的事件
handle_event(#event{type = channel_stats, props = Stats, timestamp = Timestamp},
			 State = #state{old_stats = OldTable}) ->
	%% 记录channel的状态到对应的ETS表里
	handle_stats(channel_stats, Stats, Timestamp,
				 [{fun rabbit_mgmt_format:now_to_str/1, [idle_since]}],
				 [], State),
	%% 拿到当前channel进程的Pid
	ChPid = id(channel_stats, Stats),
	%% 得到老的fine状态列表
	AllStats = [old_fine_stats(Type, Stats, State)
				  || Type <- ?FINE_STATS_TYPES],
	%% 将老的fine数据删除掉
	ets:match_delete(OldTable, {{fine, {ChPid, '_'}},      '_'}),
	%% 将老的fine数据删除掉
	ets:match_delete(OldTable, {{fine, {ChPid, '_', '_'}}, '_'}),
	[handle_fine_stats(Timestamp, AllStatsElem, State)
	   || AllStatsElem <- AllStats];

%% 处理channel被关闭的事件
handle_event(Event = #event{type = channel_closed,
							props = [{pid, Pid}]},
			 State = #state{old_stats = Old}) ->
	%% 删除消费者
	delete_consumers(Pid, consumers_by_channel, consumers_by_queue, State),
	%% 删除channel对queue的统计信息
	delete_samples(channel_queue_stats,    {Pid, '_'}, State),
	%% 删除channel对exchange的统计信息
	delete_samples(channel_exchange_stats, {Pid, '_'}, State),
	%% 删除channel_stats统计信息
	delete_samples(channel_stats,          Pid,        State),
	%% 处理channel对象被删除的事件
	handle_deleted(channel_stats, Event, State),
	%% 将Old ETS表里对应的channel信息删除掉
	ets:match_delete(Old, {{fine, {Pid, '_'}},      '_'}),
	%% 将Old ETS表里对应的channel信息删除掉
	ets:match_delete(Old, {{fine, {Pid, '_', '_'}}, '_'});

%% 处理consumer消费者被创建的事件
handle_event(#event{type = consumer_created, props = Props}, State) ->
	Fmt = [{fun rabbit_mgmt_format:amqp_table/1, [arguments]}],
	%% 处理消费者的增加事件
	handle_consumer(fun(Table, Id, P0) ->
							P = rabbit_mgmt_format:format(P0, Fmt),
							ets:insert(Table, {Id, P})
					end,
					Props, State);

%% 处理consumer消费者被删除的事件
handle_event(#event{type = consumer_deleted, props = Props}, State) ->
	%% 处理消费者删除事件
	handle_consumer(fun(Table, Id, _P) -> ets:delete(Table, Id) end,
					Props, State);

%% TODO: we don't clear up after dead nodes here - this is a very tiny
%% leak every time a node is permanently removed from the cluster. Do
%% we care?
%% 处理节点状态事件
handle_event(#event{type = node_stats, props = Stats0, timestamp = Timestamp},
			 State) ->
	%% 将句柄平均操作时间解析出来
	Stats = proplists:delete(persister_stats, Stats0) ++
				pget(persister_stats, Stats0),
	%% 处理节点状态信息
	handle_stats(node_stats, Stats, Timestamp, [], ?COARSE_NODE_STATS, State);

%% 处理集群中节点和节点之间的状态
handle_event(#event{type = node_node_stats, props = Stats,
					timestamp = Timestamp}, State) ->
	%% 处理节点和节点之间的状态信息
	handle_stats(node_node_stats, Stats, Timestamp, [], ?COARSE_NODE_NODE_STATS,
				 State);

%% 处理集群中节点被删除的事件
handle_event(Event = #event{type  = node_node_deleted,
							props = [{route, Route}]}, State) ->
	%% 将节点的统计信息删除掉
	delete_samples(node_node_stats, Route, State),
	%% 删除集群中节点的删除
	handle_deleted(node_node_stats, Event, State);

handle_event(_Event, _State) ->
	ok.


%% 处理对象被创建的事件
handle_created(TName, Stats, Funs, State = #state{tables = Tables}) ->
	%% 将状态信息Stats列表标准化
	Formatted = rabbit_mgmt_format:format(Stats, Funs),
	%% 将数据写入TName对应的ETS表里
	ets:insert(orddict:fetch(TName, Tables), {{id(TName, Stats), create},
											  Formatted,
											  pget(name, Stats)}),
	{ok, State}.


%% 处理对象的状态信息事件
handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, State) ->
	handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, [], State).


%% 处理对象的状态信息事件
handle_stats(TName, Stats, Timestamp, Funs, RatesKeys, NoAggRatesKeys,
			 State = #state{tables = Tables, old_stats = OldTable}) ->
	%% 获取在ETS里面的ID
	Id = id(TName, Stats),
	IdSamples = {coarse, {TName, Id}},
	OldStats = lookup_element(OldTable, IdSamples),
	%% 会更新VHost对应的信息
	append_samples(
	  Stats, Timestamp, OldStats, IdSamples, RatesKeys, true, State),
	%% 不会更新VHost对应的信息
	append_samples(
	  Stats, Timestamp, OldStats, IdSamples, NoAggRatesKeys, false, State),
	StripKeys = [id_name(TName)] ++ RatesKeys ++ ?FINE_STATS_TYPES,
	Stats1 = [{K, V} || {K, V} <- Stats, not lists:member(K, StripKeys)],
	%% 对Stats1列表中的信息使用Funs函数进行打印出标准数据
	Stats2 = rabbit_mgmt_format:format(Stats1, Funs),
	%% 将最新的数据插入到TName对应的ETS表里，以Id为作为该数据的ID
	ets:insert(orddict:fetch(TName, Tables), {{Id, stats}, Stats2, Timestamp}),
	{ok, State}.


%% 处理对象的删除操作
handle_deleted(TName, #event{props = Props}, State = #state{tables    = Tables,
															old_stats = Old}) ->
	%% 拿到对象的ID
	Id = id(TName, Props),
	case orddict:find(TName, Tables) of
		%% 如果查找到TName对应的ETS表，则根据该对象的ID将create和stats数据从ETS表里删除掉
		{ok, Table} -> ets:delete(Table, {Id, create}),
					   ets:delete(Table, {Id, stats});
		error       -> ok
	end,
	%% 根据ID将数据从老ETS表里删除掉
	ets:delete(Old, {coarse, {TName, Id}}),
	{ok, State}.


%% 处理消费者的事件
handle_consumer(Fun, Props, State = #state{tables = Tables}) ->
	%% 先将消费者信息整理
	P = rabbit_mgmt_format:format(Props, []),
	%% 获得消费者标识
	CTag = pget(consumer_tag, P),
	%% 获得消费者所在的消息队列名字
	Q    = pget(queue,        P),
	%% 获得消费者所在的channel Pid
	Ch   = pget(channel,      P),
	%% 获得队列上消费者统计ETS
	QTable  = orddict:fetch(consumers_by_queue,   Tables),
	%% 获得channel上消费者统计ETS
	ChTable = orddict:fetch(consumers_by_channel, Tables),
	%% 将消费者信息插入队列上消费者统计ETS
	Fun(QTable,  {Q, Ch, CTag}, P),
	%% 将消费者信息插入channel上消费者统计ETS
	Fun(ChTable, {Ch, Q, CTag}, P),
	{ok, State}.

%% The consumer_deleted event is emitted by queues themselves -
%% therefore in the event that a queue dies suddenly we may not get
%% it. The best way to handle this is to make sure we also clean up
%% consumers when we hear about any queue going down.
%% 删除消费者信息
delete_consumers(PrimId, PrimTableName, SecTableName,
				 #state{tables = Tables}) ->
	Table1 = orddict:fetch(PrimTableName, Tables),
	Table2 = orddict:fetch(SecTableName, Tables),
	SecIdCTags = ets:match(Table1, {{PrimId, '$1', '$2'}, '_'}),
	ets:match_delete(Table1, {{PrimId, '_', '_'}, '_'}),
	[ets:delete(Table2, {SecId, PrimId, CTag}) || [SecId, CTag] <- SecIdCTags].


%% 得到老的fine状态列表
old_fine_stats(Type, Props, #state{old_stats = Old}) ->
	case pget(Type, Props) of
		unknown       -> ignore;
		AllFineStats0 -> ChPid = id(channel_stats, Props),
						 [begin
							  %% 得到Fine对应的ID
							  Id = fine_stats_id(ChPid, Ids),
							  {Id, Stats, lookup_element(Old, {fine, Id})}
						  end || {Ids, Stats} <- AllFineStats0]
	end.


%% 处理fine对应的状态数据
handle_fine_stats(_Timestamp, ignore, _State) ->
	ok;

handle_fine_stats(Timestamp, AllStats, State) ->
	[handle_fine_stat(Id, Stats, Timestamp, OldStats, State) ||
	   {Id, Stats, OldStats} <- AllStats].


handle_fine_stat(Id, Stats, Timestamp, OldStats, State) ->
	%% 获得deliver_get字段对应的数量
	Total = lists:sum([V || {K, V} <- Stats, lists:member(K, ?DELIVER_GET)]),
	Stats1 = case Total of
				 0 -> Stats;
				 _ -> [{deliver_get, Total} | Stats]
			 end,
	append_samples(Stats1, Timestamp, OldStats, {fine, Id}, all, true, State).


%% 删除samples中的数据
delete_samples(Type, {Id, '_'}, State) ->
	delete_samples_with_index(Type, Id, fun forward/2, State);
delete_samples(Type, {'_', Id}, State) ->
	delete_samples_with_index(Type, Id, fun reverse/2, State);
delete_samples(Type, Id, #state{aggregated_stats = ETS}) ->
	ets:match_delete(ETS, delete_match(Type, Id)).


delete_samples_with_index(Type, Id, Order,
						  #state{aggregated_stats       = ETS,
								 aggregated_stats_index = ETSi}) ->
	Ids2 = lists:append(ets:match(ETSi, {{Type, Id, '$1'}})),
	ets:match_delete(ETSi, {{Type, Id, '_'}}),
	[begin
		 ets:match_delete(ETS, delete_match(Type, Order(Id, Id2))),
		 ets:match_delete(ETSi, {{Type, Id2, Id}})
	 end || Id2 <- Ids2].


forward(A, B) -> {A, B}.
reverse(A, B) -> {B, A}.


delete_match(Type, Id) -> {{{Type, Id}, '_'}, '_'}.


%% 在老的ETS表里插入整体数据，然后在aggregated_stats这个ETS表里分开插入时间对应的数据
append_samples(Stats, TS, OldStats, Id, Keys, Agg,
			   State = #state{old_stats = OldTable}) ->
	case ignore_coarse_sample(Id, State) of
		false ->
			%% This ceil must correspond(符合) to the ceil in handle_event
			%% queue_deleted
			NewMS = ceil(TS, State),
			case Keys of
				all -> [append_sample(K, V, NewMS, OldStats, Id, Agg, State)
						  || {K, V} <- Stats];
				_   -> [append_sample(K, V, NewMS, OldStats, Id, Agg, State)
						  || K <- Keys,
							 V <- [pget(K, Stats)],
							 V =/= 0 orelse lists:member(K, ?ALWAYS_REPORT_STATS)]
			end,
			ets:insert(OldTable, {Id, Stats});
		true ->
			ok
	end.


append_sample(Key, Val, NewMS, OldStats, Id, Agg, State) when is_number(Val) ->
	OldVal = case pget(Key, OldStats, 0) of
				 N when is_number(N) -> N;
				 _                   -> 0
			 end,
	record_sample(Id, {Key, Val - OldVal, NewMS, State}, Agg, State),
	ok;
append_sample(_Key, _Value, _NewMS, _OldStats, _Id, _Agg, _State) ->
	ok.


%% 判断对象是否存在
ignore_coarse_sample({coarse, {queue_stats, Q}}, State) ->
	not object_exists(Q, State);
ignore_coarse_sample(_, _) ->
	false.

%% Node stats do not have a vhost of course
record_sample({coarse, {node_stats, _Node} = Id}, Args, true, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, {node_node_stats, _Names} = Id}, Args, true, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, Id}, Args, false, _State) ->
    record_sample0(Id, Args);

record_sample({coarse, Id}, Args, true, State) ->
    record_sample0(Id, Args),
    record_sample0({vhost_stats, vhost(Id, State)}, Args);

%% Deliveries / acks (Q -> Ch)(在此处统计消息队列送走的以及ack的消息，消息的发送和进行ack操作都是对消息队列进行)
record_sample({fine, {Ch, Q = #resource{kind = queue}}}, Args, true, State) ->
    case object_exists(Q, State) of
		%% 更新channel对queue上的统计信息
        true  -> record_sample0({channel_queue_stats, {Ch, Q}}, Args),
				 %% 统计消息队列送出的消息数量和ack的数量
                 record_sample0({queue_stats,         Q},       Args);
        false -> ok
    end,
    record_sample0({channel_stats, Ch},       Args),
    record_sample0({vhost_stats,   vhost(Q)}, Args);

%% Publishes / confirms (Ch -> X)(消息的publish和confirm操作都是对exchange交换机进行)
record_sample({fine, {Ch, X = #resource{kind = exchange}}}, Args, true, State) ->
    case object_exists(X, State) of
        true  -> record_sample0({channel_exchange_stats, {Ch, X}}, Args),
                 record_sampleX(publish_in,              X,        Args);
        false -> ok
    end,
    record_sample0({channel_stats, Ch},       Args),
    record_sample0({vhost_stats,   vhost(X)}, Args);

%% Publishes (but not confirms) (Ch -> X -> Q)
record_sample({fine, {_Ch,
                      Q = #resource{kind = queue},
                      X = #resource{kind = exchange}}}, Args, true, State) ->
    %% TODO This one logically feels like it should be here. It would
    %% correspond to "publishing channel message rates to queue" -
    %% which would be nice to handle - except we don't. And just
    %% uncommenting this means it gets merged in with "consuming
    %% channel delivery from queue" - which is not very helpful.
    %% record_sample0({channel_queue_stats, {Ch, Q}}, Args),
    QExists = object_exists(Q, State),
    XExists = object_exists(X, State),
    case QExists of
		%% 统计消息队列publish的大小
        true  -> record_sample0({queue_stats,          Q},       Args);
        false -> ok
    end,
    case QExists andalso XExists of
        true  -> record_sample0({queue_exchange_stats, {Q,  X}}, Args);
        false -> ok
    end,
    case XExists of
        true  -> record_sampleX(publish_out,           X,        Args);
        false -> ok
    end.

%% We have to check the queue and exchange objects still exist since
%% their deleted event could be overtaken by a channel stats event
%% which contains fine stats referencing them. That's also why we
%% don't need to check the channels exist - their deleted event can't
%% be overtaken by their own last stats event.
%%
%% Also, sometimes the queue_deleted event is not emitted by the queue
%% (in the nodedown case) - so it can overtake the final queue_stats
%% event (which is not *guaranteed* to be lost). So we make a similar
%% check for coarse queue stats.
%%
%% We can be sure that mnesia will be up to date by the time we receive
%% the event (even though we dirty read) since the deletions are
%% synchronous and we do not emit the deleted event until after the
%% deletion has occurred.
%% 判断Name对象在当前RabbitMQ系统中是否存在
object_exists(Name = #resource{kind = Kind}, #state{lookups = Lookups}) ->
	case (pget(Kind, Lookups))(Name) of
		{ok, _} -> true;
		_       -> false
	end.


%% 获取对象VHost
vhost(#resource{virtual_host = VHost}) -> VHost.


%% 获取对象VHost
vhost({queue_stats, #resource{virtual_host = VHost}}, _State) ->
	VHost;
vhost({TName, Pid}, #state{tables = Tables}) ->
	Table = orddict:fetch(TName, Tables),
	pget(vhost, lookup_element(Table, {Pid, create})).

%% exchanges have two sets of "publish" stats, so rearrange things a touch
record_sampleX(RenamePublishTo, X, {publish, Diff, TS, State}) ->
	record_sample0({exchange_stats, X}, {RenamePublishTo, Diff, TS, State});
record_sampleX(_RenamePublishTo, X, {Type, Diff, TS, State}) ->
	record_sample0({exchange_stats, X}, {Type, Diff, TS, State}).

%% Ignore case where ID1 and ID2 are in a tuple, i.e. detailed stats,
%% when in basic mode
record_sample0({Type, {_ID1, _ID2}}, {_, _, _, #state{rates_mode = basic}})
  when Type =/= node_node_stats ->
	ok;
%% aggregated：汇总
record_sample0(Id0, {Key, Diff, TS, #state{aggregated_stats       = ETS,
										   aggregated_stats_index = ETSi}}) ->
	Id = {Id0, Key},
	Old = case lookup_element(ETS, Id) of
			  [] -> case Id0 of
						{Type, {Id1, Id2}} ->
							ets:insert(ETSi, {{Type, Id2, Id1}}),
							ets:insert(ETSi, {{Type, Id1, Id2}});
						_ ->
							ok
					end,
					rabbit_mgmt_stats:blank();
			  E  -> E
		  end,
	ets:insert(ETS, {Id, rabbit_mgmt_stats:record(TS, Diff, Old)}).

%%----------------------------------------------------------------------------
%% Internal, querying side
%%----------------------------------------------------------------------------
%% 队列详细信息
-define(QUEUE_DETAILS,
		{queue_stats, [{incoming,   queue_exchange_stats, fun first/1},
					   {deliveries, channel_queue_stats,  fun second/1}]}).

-define(EXCHANGE_DETAILS,
		{exchange_stats, [{incoming, channel_exchange_stats, fun second/1},
						  {outgoing, queue_exchange_stats,   fun second/1}]}).

-define(CHANNEL_DETAILS,
		{channel_stats, [{publishes,  channel_exchange_stats, fun first/1},
						 {deliveries, channel_queue_stats,    fun first/1}]}).

-define(NODE_DETAILS,
		{node_stats, [{cluster_links, node_node_stats, fun first/1}]}).

first(Id)  -> {Id, '$1'}.
second(Id) -> {'$1', Id}.


%% 列出Objs列表中所有队列的信息
list_queue_stats(Ranges, Objs, State) ->
	%% 直接call到队列进程获得队列进程的内存使用情况，获得最新的队列进程内存使用情况
	adjust_hibernated_memory_use(
	  merge_stats(Objs, queue_funs(Ranges, State))).


%% 列出Objs列表中所有队列的详细信息
detail_queue_stats(Ranges, Objs, State) ->
	%% 直接call到队列进程获得队列进程的内存使用情况，获得最新的队列进程内存使用情况
	adjust_hibernated_memory_use(
	  merge_stats(Objs, [%% 获取该队列消费者详细信息的函数
						 consumer_details_fun(
						   fun (Props) -> id_lookup(queue_stats, Props) end,
						   consumers_by_queue, State),
						 %% 获得队列详细统计信息的列表
						 detail_stats_fun(Ranges, ?QUEUE_DETAILS, State)
						%% 获得消息队列普通信息列表
							 | queue_funs(Ranges, State)])).


%% 获取队列基本信息的函数列表
queue_funs(Ranges, State) ->
	[%% 获得队列对象的基本信息函数
	 basic_stats_fun(queue_stats, State),
	 simple_stats_fun(Ranges, queue_stats, State),
	 augment_msg_stats_fun(State)].


%% 获取exchange基本信息
list_exchange_stats(Ranges, Objs, State) ->
	merge_stats(Objs, [%% 根据范围列出基本详细信息
					   simple_stats_fun(Ranges, exchange_stats, State),
					   %% 增加消息状态的函数(增加Props里面对应的Pid相关信息)
					   augment_msg_stats_fun(State)]).


%% 列出exchange的详细信息
detail_exchange_stats(Ranges, Objs, State) ->
	merge_stats(Objs, [%% 根据范围列出基本详细信息
					   simple_stats_fun(Ranges, exchange_stats, State),
					   %% 列出exchange上进出的详细信息
					   detail_stats_fun(Ranges, ?EXCHANGE_DETAILS, State),
					   %% 增加消息状态的函数(增加Props里面对应的Pid相关信息)
					   augment_msg_stats_fun(State)]).


%% 列出连接的基本信息
connection_stats(Ranges, Objs, State) ->
	merge_stats(Objs, [%% 列出连接的基础信息
					   basic_stats_fun(connection_stats, State),
					   %% 列出连接进程收取发送数据的详细信息
					   simple_stats_fun(Ranges, connection_stats, State),
					   %% 增加消息状态的函数(增加Props里面对应的Pid相关信息)
					   augment_msg_stats_fun(State)]).


%% 列出channel的基本信息
list_channel_stats(Ranges, Objs, State) ->
	merge_stats(Objs, [%% 列出channel进程的基础信息
					   basic_stats_fun(channel_stats, State),
					   %% 列出channel进程的详细信息
					   simple_stats_fun(Ranges, channel_stats, State),
					   %% 增加消息状态的函数(增加Props里面对应的Pid相关信息)
					   augment_msg_stats_fun(State)]).


%% 列出channel完整详细信息
detail_channel_stats(Ranges, Objs, State) ->
	merge_stats(Objs, [%% 列出channel进程的基础信息
					   basic_stats_fun(channel_stats, State),
					   %% 列出channel进程的详细信息
					   simple_stats_fun(Ranges, channel_stats, State),
					   %% 列出channel上的消费者信息
					   consumer_details_fun(
						 fun (Props) -> pget(pid, Props) end,
						 consumers_by_channel, State),
					   %% 列出channel进程上publish和deliver的详细信息
					   detail_stats_fun(Ranges, ?CHANNEL_DETAILS, State),
					   %% 增加消息状态的函数(增加Props里面对应的Pid相关信息)
					   augment_msg_stats_fun(State)]).


%% 列出vhost的信息
vhost_stats(Ranges, Objs, State) ->
	merge_stats(Objs, [simple_stats_fun(Ranges, vhost_stats, State)]).


%% 列出node的信息
node_stats(Ranges, Objs, State) ->
	merge_stats(Objs, [%% 获得节点的基础信息
					   basic_stats_fun(node_stats, State),
					   simple_stats_fun(Ranges, node_stats, State),
					   detail_and_basic_stats_fun(
						 node_node_stats, Ranges, ?NODE_DETAILS, State)]).


%% 根据Funs函数列表得到队列所有的相关信息
merge_stats(Objs, Funs) ->
	[lists:foldl(fun (Fun, Props) -> combine(Fun(Props), Props) end, Obj, Funs)
	   || Obj <- Objs].


%% 合并新老信息
combine(New, Old) ->
	case pget(state, Old) of
		unknown -> New ++ Old;
		live    -> New ++ proplists:delete(state, Old);
		_       -> proplists:delete(state, New) ++ Old
	end.

%% i.e. the non-calculated stats
%% 获得Type类型对象的基本信息函数
basic_stats_fun(Type, #state{tables = Tables}) ->
	Table = orddict:fetch(Type, Tables),
	fun (Props) ->
			 Id = id_lookup(Type, Props),
			 lookup_element(Table, {Id, stats})
	end.

%% i.e. coarse stats, and fine stats aggregated up to a single number per thing
%% 获得Type对应的对象简单的统计信息
simple_stats_fun(Ranges, Type, State) ->
	fun (Props) ->
			 %% 组装队列或者交换机的资源数据结构
			 Id = id_lookup(Type, Props),
			 %% 从Stats列表中提取出感兴趣的信息
			 extract_msg_stats(
			   %% 根据Ranges范围标准打印样品信息
			   format_samples(Ranges, read_simple_stats(Type, Id, State), State))
	end.

%% i.e. fine stats that are broken out per sub-thing
%% 获得IdType对象详细统计信息
detail_stats_fun(Ranges, {IdType, FineSpecs}, State) ->
	fun (Props) ->
			 Id = id_lookup(IdType, Props),
			 [detail_stats(Ranges, Name, AggregatedStatsType, IdFun(Id), State)
				|| {Name, AggregatedStatsType, IdFun} <- FineSpecs]
	end.

%% This does not quite do the same as detail_stats_fun +
%% basic_stats_fun; the basic part here assumes compound keys (like
%% detail stats) but non-calculated (like basic stats). Currently the
%% only user of that is node-node stats.
%%
%% We also assume that FineSpecs is single length here (at [1]).
detail_and_basic_stats_fun(Type, Ranges, {IdType, FineSpecs},
						   State = #state{tables = Tables}) ->
	Table = orddict:fetch(Type, Tables),
	F = detail_stats_fun(Ranges, {IdType, FineSpecs}, State),
	fun (Props) ->
			 Id = id_lookup(IdType, Props),
			 BasicStatsRaw = ets:match(Table, {{{Id, '$1'}, stats}, '$2', '_'}),
			 BasicStatsDict = dict:from_list([{K, V} || [K,V] <- BasicStatsRaw]),
			 [{K, Items}] = F(Props), %% [1]
			 Items2 = [case dict:find(id_lookup(IdType, Item), BasicStatsDict) of
						   {ok, BasicStats} -> BasicStats ++ Item;
						   error            -> Item
					   end || Item <- Items],
			 [{K, Items2}]
	end.


%% 读取简单状态函数，获得Type和ID对应的相关的统计信息
read_simple_stats(Type, Id, #state{aggregated_stats = ETS}) ->
	FromETS = ets:match(ETS, {{{Type, Id}, '$1'}, '$2'}),
	[{K, V} || [K, V] <- FromETS].


%% 读取详细的统计信息
read_detail_stats(Type, Id, #state{aggregated_stats = ETS}) ->
	%% Id must contain '$1'
	FromETS = ets:match(ETS, {{{Type, Id}, '$2'}, '$3'}),
	%% [[G, K, V]] -> [{G, [{K, V}]}] where G is Q/X/Ch, K is from
	%% ?FINE_STATS and V is a stats tree
	%% TODO does this need to be optimised?
	lists:foldl(
	  fun ([G, K, V], L) ->
			   case lists:keyfind(G, 1, L) of
				   false    -> [{G, [{K, V}]} | L];
				   {G, KVs} -> lists:keyreplace(G, 1, L, {G, [{K, V} | KVs]})
			   end
	  end, [], FromETS).


%% 从Stats列表中提取出感兴趣的信息
extract_msg_stats(Stats) ->
	FineStats = lists:append([[K, details_key(K)] || K <- ?MSG_RATES]),
	{MsgStats, Other} =
		lists:partition(fun({K, _}) -> lists:member(K, FineStats) end, Stats),
	case MsgStats of
		[] -> Other;
		_  -> [{message_stats, MsgStats} | Other]
	end.


%% 获取详细状态信息
detail_stats(Ranges, Name, AggregatedStatsType, Id, State) ->
	{Name,
	 [[{stats, format_samples(Ranges, KVs, State)} | format_detail_id(G, State)]
	  || {G, KVs} <- read_detail_stats(AggregatedStatsType, Id, State)]}.


%% 标准化详细信息的ID
format_detail_id(ChPid, State) when is_pid(ChPid) ->
	augment_msg_stats([{channel, ChPid}], State);
format_detail_id(#resource{name = Name, virtual_host = Vhost, kind = Kind},
				 _State) ->
	[{Kind, [{name, Name}, {vhost, Vhost}]}];
format_detail_id(Node, _State) when is_atom(Node) ->
	[{name, Node}].


%% 根据Ranges范围标准打印样品信息
format_samples(Ranges, ManyStats, #state{interval = Interval}) ->
	lists:append(
	  [case rabbit_mgmt_stats:is_blank(Stats) andalso
				not lists:member(K, ?ALWAYS_REPORT_STATS) of
		   true  -> [];
		   false -> {Details, Counter} = rabbit_mgmt_stats:format(
										   pick_range(K, Ranges),
										   Stats, Interval),
					%% 返回K对应的总的数量，已经K组装的详细信息key对应的详细信息
					[{K,              Counter},
					 {details_key(K), Details}]
	   end || {K, Stats} <- ManyStats]).


%% 选择范围
pick_range(K, {RangeL, RangeM, RangeD, RangeN}) ->
	case {lists:member(K, ?QUEUE_MSG_COUNTS),
		  lists:member(K, ?MSG_RATES),
		  lists:member(K, ?COARSE_CONN_STATS),
		  lists:member(K, ?COARSE_NODE_STATS)
			  orelse lists:member(K, ?COARSE_NODE_NODE_STATS)} of
		{true, false, false, false} -> RangeL;
		{false, true, false, false} -> RangeM;
		{false, false, true, false} -> RangeD;
		{false, false, false, true} -> RangeN
	end.

%% We do this when retrieving the queue record rather than when
%% storing it since the memory use will drop *after* we find out about
%% hibernation, so to do it when we receive a queue stats event would
%% be fiddly and racy. This should be quite cheap though.
%% 直接call到队列进程获得队列进程的内存使用情况，获得最新的队列进程内存使用情况
adjust_hibernated_memory_use(Qs) ->
	%% 获得所有队列的Pid列表
	Pids = [pget(pid, Q) ||
			  Q <- Qs, pget(idle_since, Q, not_idle) =/= not_idle],
	%% We use delegate here not for ordering reasons but because we
	%% want to get the right amount of parallelism and minimise
	%% cross-cluster communication.
	%% 直接通过代理进程向队列进程取得队列进程使用的内存大小
	{Mem, _BadNodes} = delegate:invoke(Pids, {erlang, process_info, [memory]}),
	MemDict = dict:from_list([{P, M} || {P, M = {memory, _}} <- Mem]),
	%% 将最新的队列内存使用情况放入到状态列表中
	[case dict:find(pget(pid, Q), MemDict) of
		 error        -> Q;
		 {ok, Memory} -> [Memory|proplists:delete(memory, Q)]
	 end || Q <- Qs].


created_event(Name, Type, Tables) ->
	Table = orddict:fetch(Type, Tables),
	case ets:match(Table, {{'$1', create}, '_', Name}) of
		[]     -> not_found;
		[[Id]] -> lookup_element(Table, {Id, create})
	end.


created_events(Type, Tables) ->
	[Facts || {{_, create}, Facts, _Name}
				  <- ets:tab2list(orddict:fetch(Type, Tables))].


%% 获取KeyFun过滤下来的消费者详细信息的函数
consumer_details_fun(KeyFun, TableName, State = #state{tables = Tables}) ->
	Table = orddict:fetch(TableName, Tables),
	fun ([])    -> [];
	   (Props) -> Pattern = {KeyFun(Props), '_', '_'},
				  [{consumer_details,
					%% 获得Pattern模式消费者对应的详细信息
					[augment_msg_stats(augment_consumer(Obj), State)
					   || Obj <- lists:append(
							ets:match(Table, {Pattern, '$1'}))]}]
	end.


%% 增加消费者(获得队列名字键值对)
augment_consumer(Obj) ->
	[{queue, rabbit_mgmt_format:resource(pget(queue, Obj))} |
		 proplists:delete(queue, Obj)].

%%----------------------------------------------------------------------------
%% Internal, query-time summing for overview
%%----------------------------------------------------------------------------

overview_sum(Type, VHostStats) ->
	Stats = [pget(Type, VHost, rabbit_mgmt_stats:blank())
			   || VHost <- VHostStats],
	{Type, rabbit_mgmt_stats:sum(Stats)}.

%%----------------------------------------------------------------------------
%% Internal, query-time augmentation
%%----------------------------------------------------------------------------
%% augment：增加
augment_msg_stats(Props, State) ->
	rabbit_mgmt_format:strip_pids(
	  (augment_msg_stats_fun(State))(Props) ++ Props).


%% 增加消息状态的函数(增加Props里面对应的Pid相关信息)
augment_msg_stats_fun(State) ->
	Funs = [%% 将连接进程的详细信息打印出来
			{connection, fun augment_connection_pid/2},
			%% 将channel对应的详细信息打印出来
			{channel,    fun augment_channel_pid/2},
			%% 如果该队列有owner_pid，则将该队列对应的连接进程的信息打印出来
			{owner_pid,  fun augment_connection_pid/2}],
	fun (Props) -> augment(Props, Funs, State) end.


augment(Items, Funs, State) ->
	Augmented = [augment(K, Items, Fun, State) || {K, Fun} <- Funs],
	[{K, V} || {K, V} <- Augmented, V =/= unknown].


augment(K, Items, Fun, State) ->
	Key = details_key(K),
	case pget(K, Items) of
		none    -> {Key, unknown};
		unknown -> {Key, unknown};
		Id      -> {Key, Fun(Id, State)}
	end.


%% 获取channel进程的名字，编号，用户结构，对应连接进程的名字，远端端口，远端host
augment_channel_pid(Pid, #state{tables = Tables}) ->
	Ch = lookup_element(orddict:fetch(channel_stats, Tables),
						{Pid, create}),
	Conn = lookup_element(orddict:fetch(connection_stats, Tables),
						  {pget(connection, Ch), create}),
	[{name,            pget(name,   Ch)},
	 {number,          pget(number, Ch)},
	 {user,            pget(user,   Ch)},
	 {connection_name, pget(name,         Conn)},
	 {peer_port,       pget(peer_port,    Conn)},
	 {peer_host,       pget(peer_host,    Conn)}].


%% 获取连接进程的名字，远端端口，远端host
augment_connection_pid(Pid, #state{tables = Tables}) ->
	Conn = lookup_element(orddict:fetch(connection_stats, Tables),
						  {Pid, create}),
	[{name,         pget(name,         Conn)},
	 {peer_port,    pget(peer_port,    Conn)},
	 {peer_host,    pget(peer_host,    Conn)}].

%%----------------------------------------------------------------------------
%% Internal, event-GCing
%%----------------------------------------------------------------------------

gc_batch(State = #state{aggregated_stats = ETS}) ->
	{ok, Policies} = application:get_env(
					   rabbitmq_management, sample_retention_policies),
	Rows = erlang:max(?GC_MIN_ROWS,
					  round(?GC_MIN_RATIO * ets:info(ETS, size))),
	gc_batch(Rows, Policies, State).


gc_batch(0, _Policies, State) ->
	State;
gc_batch(Rows, Policies, State = #state{aggregated_stats = ETS,
										gc_next_key      = Key0}) ->
	Key = case Key0 of
			  undefined -> ets:first(ETS);
			  _         -> ets:next(ETS, Key0)
		  end,
	Key1 = case Key of
			   '$end_of_table' -> undefined;
			   _               -> Now = floor(os:timestamp(), State),
								  Stats = ets:lookup_element(ETS, Key, 2),
								  gc(Key, Stats, Policies, Now, ETS),
								  Key
		   end,
	gc_batch(Rows - 1, Policies, State#state{gc_next_key = Key1}).


gc({{Type, Id}, Key}, Stats, Policies, Now, ETS) ->
	Policy = pget(retention_policy(Type), Policies),
	case rabbit_mgmt_stats:gc({Policy, Now}, Stats) of
		Stats  -> ok;
		Stats2 -> ets:insert(ETS, {{{Type, Id}, Key}, Stats2})
	end.


retention_policy(node_stats)             -> global;
retention_policy(node_node_stats)        -> global;
retention_policy(vhost_stats)            -> global;
retention_policy(queue_stats)            -> basic;
retention_policy(exchange_stats)         -> basic;
retention_policy(connection_stats)       -> basic;
retention_policy(channel_stats)          -> basic;
retention_policy(queue_exchange_stats)   -> detailed;
retention_policy(channel_exchange_stats) -> detailed;
retention_policy(channel_queue_stats)    -> detailed.
