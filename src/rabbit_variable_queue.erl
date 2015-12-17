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

-module(rabbit_variable_queue).

-export([init/3, terminate/2, delete_and_terminate/2, delete_crashed/1,
         purge/1, purge_acks/1,
         publish/6, publish_delivered/5, discard/4, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/2,
         ackfold/4, fold/3, len/1, is_empty/1, depth/1,
         set_ram_duration_target/2, ram_duration/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         info/2, invoke/3, is_duplicate/2, multiple_routing_keys/0]).

-export([start/1, stop/0]).

%% exported for testing only
-export([start_msg_store/2, stop_msg_store/0, init/6]).

%%----------------------------------------------------------------------------
%% Messages, and their position in the queue, can be in memory or on
%% disk, or both. Persistent messages will have both message and
%% position pushed to disk as soon as they arrive; transient messages
%% can be written to disk (and thus both types can be evicted from
%% memory) under memory pressure. The question of whether a message is
%% in RAM and whether it is persistent are orthogonal.
%% (持久化消息当它一到达RabbitMQ系统则会立刻将消息和消息索引位置写入磁盘)
%% (短暂消息在内存中有压力的时候则将该类型的消息写入磁盘)
%% (持久性和短暂消息这两种类型的可从存储器逐出，即两者都可以写入磁盘)
%%
%% Messages are persisted using the queue index and the message
%% store. Normally the queue index holds the position of the message
%% *within this queue* along with a couple of small bits of metadata,
%% while the message store holds the message itself (including headers
%% and other properties.
%% persisted(坚持) metadata(元数据) headers(消息头) properties(消息特性)
%% 所有的消息坚持使用队列索引和消息结构存储，通常消息索引包括这个消息在队列中的位置，
%% 它包括一系列的小的元数据，同时消息结构存储自己消息本身，还包括消息的头和消息的特性等
%%
%% However, as an optimisation(优化), small messages can be embedded(嵌入式)
%% directly in the queue index and bypass(省略) the message store
%% altogether.
%% 然而作为一种优化，小的消息能够直接嵌入到消息队列索引中忽略掉消息结构存储消息体，即将消息本身和消息位置存储到消息索引中
%%
%% Definitions:
%%
%% alpha: this is a message where both the message itself, and its
%%        position within the queue are held in RAM(消息本身和消息位置索引都只在内存中)
%%
%% beta:  this is a message where the message itself is only held on
%%        disk (if persisted to the message store) but its position
%%        within the queue is held in RAM.(消息本身存储在磁盘中，但是消息的位置索引存在内存中)
%%
%% gamma: this is a message where the message itself is only held on
%%        disk, but its position is both in RAM and on disk.(消息本身存储在磁盘中，但是消息的位置索引存在内存中和磁盘中)
%%
%% delta: this is a collection of messages, represented by a single
%%        term, where the messages and their position are only held on
%%        disk.(消息本身和消息的位置索引都值存储在磁盘中)
%%
%% Note that(注意) for persistent messages, the message and its position
%% within the queue are always held on disk, *in addition* to being in
%% one of the above classifications.
%% 注意对于持久化消息，消息体结构和消息索引总是存储在磁盘上
%%
%% Also note that within this code, the term gamma seldom
%% appears. It's frequently the case that gammas are defined by betas
%% who have had their queue position recorded on disk.
%% gamma类型在当前代码中很少出现，但是很频繁的是gammas由betas定义，而betas的位置索引记录在磁盘
%%
%% In general, messages move q1 -> q2 -> delta -> q3 -> q4, though
%% many of these steps are frequently(频繁) skipped. q1 and q4 only hold
%% alphas, q2 and q3 hold both betas and gammas. When a message
%% arrives, its classification(分类) is determined. It is then added to the
%% rightmost appropriate(适当) queue.(q1,q4只存储消息本身和消息位置索引在内存中的消息(存储类型为alpha),q2,q3存储类型beta，gamma)
%% 通常，消息是q1 -> q2 -> delta -> q3 -> q4的方式转化，然而许多的步骤被频繁的忽略掉
%% q1,q4只存储alphas类型的消息,q2,q3存储betas和gammas类型,当一个消息到来，它的分类已经被定义，然后被加入到适当的队列
%%
%% If a new message is determined to be a beta or gamma, q1 is
%% empty. If a new message is determined to be a delta, q1 and q2 are
%% empty (and actually q4 too).
%% 如果一个新消息被定义为beta或者gamma，则q1是为空，如果一个新消息被定义为delta，q1和q2为空(同时其实q4也为空)
%%
%% When removing messages from a queue, if q4 is empty then q3 is read
%% directly. If q3 becomes empty then the next segment's worth of
%% messages from delta are read into q3, reducing the size of
%% delta. If the queue is non empty, either q4 or q3 contain
%% entries. It is never permitted for delta to hold all the messages
%% in the queue.
%% 当移除一个消息从队列中，如果q4为空然后q3将被直接读取。如果q3变为空然后下一段有价值的消息来自delta，将读取的磁盘消息存入q3
%% 减少delta磁盘队列消息的数量
%% 如果队列为非空，q4或者q3包括项。
%% 决不允许delta类型持有队列中的所有消息
%%
%% The duration indicated(说明) to us by the memory_monitor is used to
%% calculate, given our current ingress(进入) and egress(出去) rates, how many
%% messages we should hold in RAM (i.e. as alphas). We track the
%% ingress and egress rates for both messages and pending acks and
%% rates for both are considered when calculating the number of
%% messages to hold in RAM. When we need to push alphas to betas or
%% betas to gammas, we favour writing out messages that are further
%% from the head of the queue. This minimises(最小化) writes to disk, as the
%% messages closer to the tail of the queue stay in the queue for
%% longer, thus do not need to be replaced as quickly by sending other
%% messages to disk.
%% memory_monitor进程持续的说明给我们是用来计算，给我们当前进入和出去的速率，这样让我们知道我们应该保留多少消息在内存中
%%
%% Whilst(虽然) messages are pushed to disk and forgotten from RAM as soon
%% as requested by a new setting of the queue RAM duration, the
%% inverse is not true: we only load messages back into RAM as
%% demanded as(如要求为) the queue is read from. Thus only publishes to the
%% queue will take up available spare capacity.
%%
%% When we report our duration to the memory monitor, we calculate
%% average ingress and egress rates over the last two samples, and
%% then calculate our duration based on the sum of the ingress and
%% egress rates. More than two samples could be used, but it's a
%% balance between responding quickly enough to changes in
%% producers/consumers versus ignoring temporary blips. The problem
%% with temporary blips is that with just a few queues, they can have
%% substantial impact on the calculation of the average duration and
%% hence cause unnecessary I/O. Another alternative is to increase the
%% amqqueue_process:RAM_DURATION_UPDATE_PERIOD to beyond 5
%% seconds. However, that then runs the risk of being too slow to
%% inform the memory monitor of changes. Thus a 5 second interval,
%% plus a rolling average over the last two samples seems to work
%% well in practice.
%%
%% The sum of the ingress and egress rates is used because the egress
%% rate alone is not sufficient. Adding in the ingress rate means that
%% queues which are being flooded by messages are given more memory,
%% resulting in them being able to process the messages faster (by
%% doing less I/O, or at least deferring it) and thus helping keep
%% their mailboxes empty and thus the queue as a whole is more
%% responsive. If such a queue also has fast but previously idle
%% consumers, the consumer can then start to be driven as fast as it
%% can go, whereas if only egress rate was being used, the incoming
%% messages may have to be written to disk and then read back in,
%% resulting in the hard disk being a bottleneck in driving the
%% consumers. Generally, we want to give Rabbit every chance of
%% getting rid of messages as fast as possible and remaining
%% responsive, and using only the egress rate impacts that goal.
%%
%% Once the queue has more alphas than the target_ram_count, the
%% surplus(剩下的) must be converted to betas, if not gammas, if not rolled
%% into delta. The conditions under which these transitions occur
%% reflect the conflicting goals of minimising RAM cost per msg, and
%% minimising CPU cost per msg. Once the msg has become a beta, its
%% payload is no longer in RAM, thus a read from the msg_store must
%% occur before the msg can be delivered, but the RAM cost of a beta
%% is the same as a gamma, so converting a beta to gamma will not free
%% up any further RAM. To reduce the RAM cost further, the gamma must
%% be rolled into delta. Whilst recovering a beta or a gamma to an
%% alpha requires only one disk read (from the msg_store), recovering
%% a msg from within delta will require two reads (queue_index and
%% then msg_store). But delta has a near-0 per-msg RAM cost. So the
%% conflict is between using delta more, which will free up more
%% memory, but require additional CPU and disk ops, versus using delta
%% less and gammas and betas more, which will cost more memory, but
%% require fewer disk ops and less CPU overhead.
%%
%% In the case of(如果是) a persistent msg published to a durable queue, the
%% msg is immediately written to the msg_store and queue_index. If
%% then additionally converted from an alpha, it'll immediately go to
%% a gamma (as it's already in queue_index), and cannot exist as a
%% beta. Thus a durable queue with a mixture of persistent and
%% transient msgs in it which has more messages than permitted by the
%% target_ram_count may contain an interspersed mixture of betas and
%% gammas in q2 and q3.
%%
%% There is then a ratio that controls how many betas and gammas there
%% can be. This is based on the target_ram_count and thus expresses
%% the fact that as the number of permitted alphas in the queue falls,
%% so should the number of betas and gammas fall (i.e. delta
%% grows). If q2 and q3 contain more than the permitted number of
%% betas and gammas, then the surplus are forcibly converted to gammas
%% (as necessary) and then rolled into delta. The ratio is that
%% delta/(betas+gammas+delta) equals
%% (betas+gammas+delta)/(target_ram_count+betas+gammas+delta). I.e. as
%% the target_ram_count shrinks to 0, so must betas and gammas.
%%
%% The conversion of betas to gammas is done in batches of at least
%% ?IO_BATCH_SIZE. This value should not be too small, otherwise the
%% frequent operations on the queues of q2 and q3 will not be
%% effectively amortised (switching the direction of queue access
%% defeats amortisation). Note that there is a natural upper bound due
%% to credit_flow limits on the alpha to beta conversion.
%%
%% The conversion from alphas to betas is chunked due to the
%% credit_flow limits of the msg_store. This further smooths the
%% effects of changes to the target_ram_count and ensures the queue
%% remains responsive even when there is a large amount of IO work to
%% do. The 'resume' callback is utilised to ensure that conversions
%% are done as promptly as possible whilst ensuring the queue remains
%% responsive.
%%
%% In the queue we keep track of both messages that are pending
%% delivery and messages that are pending acks. In the event of a
%% queue purge, we only need to load qi segments if the queue has
%% elements in deltas (i.e. it came under significant memory
%% pressure). In the event of a queue deletion, in addition to the
%% preceding, by keeping track of pending acks in RAM, we do not need
%% to search through qi segments looking for messages that are yet to
%% be acknowledged.
%%
%% Pending acks are recorded in memory by storing the message itself.
%% If the message has been sent to disk, we do not store the message
%% content. During memory reduction, pending acks containing message
%% content have that content removed and the corresponding messages
%% are pushed out to disk.
%%
%% Messages from pending acks are returned to q4, q3 and delta during
%% requeue, based on the limits of seq_id contained in each. Requeued
%% messages retain their original seq_id, maintaining order
%% when requeued.
%%
%% The order in which alphas are pushed to betas and pending acks
%% are pushed to disk is determined dynamically. We always prefer to
%% push messages for the source (alphas or acks) that is growing the
%% fastest (with growth measured as avg. ingress - avg. egress).
%%
%% Notes on Clean Shutdown
%% (This documents behaviour in variable_queue, queue_index and
%% msg_store.)
%%
%% In order to try to achieve as fast a start-up as possible, if a
%% clean shutdown occurs, we try to save out state to disk to reduce
%% work on startup. In the msg_store this takes the form of the
%% index_module's state, plus the file_summary ets table, and client
%% refs. In the VQ, this takes the form of the count of persistent
%% messages in the queue and references into the msg_stores. The
%% queue_index adds to these terms the details of its segments and
%% stores the terms in the queue directory.
%%
%% Two message stores are used. One is created for persistent messages
%% to durable queues that must survive restarts, and the other is used
%% for all other messages that just happen to need to be written to
%% disk. On start up we can therefore nuke the transient message
%% store, and be sure that the messages in the persistent store are
%% all that we need.
%%
%% The references to the msg_stores are there so that the msg_store
%% knows to only trust its saved state if all of the queues it was
%% previously talking to come up cleanly. Likewise, the queues
%% themselves (esp queue_index) skips work in init if all the queues
%% and msg_store were shutdown cleanly. This gives both good speed
%% improvements and also robustness so that if anything possibly went
%% wrong in shutdown (or there was subsequent manual tampering), all
%% messages and queues that can be recovered are recovered, safely.
%%
%% To delete transient messages lazily, the variable_queue, on
%% startup, stores the next_seq_id reported by the queue_index as the
%% transient_threshold. From that point on, whenever it's reading a
%% message off disk via the queue_index, if the seq_id is below this
%% threshold and the message is transient then it drops the message
%% (the message itself won't exist on disk because it would have been
%% stored in the transient msg_store which would have had its saved
%% state nuked on startup). This avoids the expensive operation of
%% scanning the entire queue on startup in order to delete transient
%% messages that were only pushed to disk to save memory.
%%
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

%% 整个队列的状态
-record(vqstate,
		{ 
		 q1,						%% 消息队列一，保存alpha类型的消息
		 q2,						%% 消息队列二，保存beta和gamma类型的消息
		 delta,						%% 消息磁盘信息结构
		 q3,						%% 消息队列三，保存beta和gamma类型的消息
		 q4,						%% 消息队列四，保存alpha类型的消息
		 next_seq_id,				%% 下一个消息在队列中的索引ID
		 ram_pending_ack,    		%% msgs using store, still in RAM(消息内容在内存中，消息内容存储在消息存储服务器进程，同时等待ack的消息队列二叉树)
		 disk_pending_ack,   		%% msgs in store, paged out(消息内容不在内存中，同时等待ack的消息队列二叉树)
		 qi_pending_ack,     		%% msgs using qi, *can't* be paged out(消息内容在内存中，消息内容在队列索引中，同时等待ack的消息队列二叉树)
		 index_state,				%% 保存该队列的索引信息结构
		 msg_store_clients,			%% 消息内容存储客户端进程保留的相关信息
		 durable,					%% 该队列是否是持久化队列的标志
		 transient_threshold,		%% threshold：阀值
		 
		 len,                		%% w/o unacked(当前队列的长度，即当前队列中消息的数量)
		 bytes,              		%% w/o unacked(当前队列中消息的总大小)
		 unacked_bytes,				%% 当前队列中消息还未ack的总的大小
		 persistent_count,   		%% w   unacked(当前队列消息持久化的个数)
		 persistent_bytes,   		%% w   unacked(当前队列中持久化消息总的大小)
		 
		 target_ram_count,			%% 该队列中允许在内存中保存的最大消息个数(如果为infinity，则表示当前系统内存足够，不需要进行较少内存使用的操作)
		 ram_msg_count,      		%% w/o unacked(该队列中，当前消息在内存中的数量)
		 ram_msg_count_prev,		%% 消息速率计算相关字段
		 ram_ack_count_prev,		%% 消息速率计算相关字段
		 ram_bytes,          		%% w   unacked(在内存中该队列保存的消息的大小)
		 out_counter,				%% 出队列的消息数量
		 in_counter,				%% 进入队列中的消息数量
		 rates,						%% 队列中进入和出队列的速率结构保存字段
		 msgs_on_disk,				%% 消息存储服务器进程通知backing_queue消息已经存在磁盘中的消息gb_sets结构
		 msg_indices_on_disk,		%% 消息队列索引模块通知backing_queue消息已经存储在队列索引对应的磁盘文件中的消息gb_sets结构
		 unconfirmed,				%% 还没有confirm的消息的集合
		 confirmed,					%% 已经confirm的消息的集合
		 ack_out_counter,			%% 退出队列的消息等待ack的消息的数量
		 ack_in_counter,			%% 进入队列的消息且等待ack的消息的数量
		 %% Unlike the other counters these two do not feed into
		 %% #rates{} and get reset
		 disk_read_count,			%% 从磁盘中读取消息的数量(即该队列从磁盘读数据的数量)
		 disk_write_count			%% 该队列中消息或者消息索引写入磁盘的数量(即该队列写磁盘的数量)
		}).

-record(rates, {
				in,					%% 消息进入队列的速率
				out,				%% 消息出队列的速率
				ack_in,				%% 消息ack进入队列的速率
				ack_out,			%% 消息ack出队列的速率
				timestamp
			   }).

%% 消息保存在队列中内存中的数据结构
-record(msg_status,
		{
		 seq_id,					%% 消息在队列中的索引ID
		 msg_id,					%% 消息ID
		 msg,						%% 消息实体
		 is_persistent,				%% 是否是持久化消息
		 is_delivered,				%% 该消息是否已经发送给消费者
		 msg_in_store,				%% 标识该进程是否已经存储到消息磁盘文件中
		 index_on_disk,				%% 标识该进程是否已经存储到队列磁盘索引中
		 persist_to,				%% 该消息是需要存储到消息存储服务器(msg_store)进程，还是只存储在队列索引(queue_index)中的标志
		 msg_props					%% 该消息的特性结构
		}).

%% 磁盘消息数据结构
-record(delta,
		{
		 start_seq_id, 			%% start_seq_id is inclusive(消息在当前队列中最小的索引号)
		 count,					%% 存储在磁盘中的消息数
		 end_seq_id    			%% end_seq_id is exclusive(消息在当前队列中最大的索引号)
		}).

%% When we discover that we should write some indices(指数) to disk for some
%% betas, the IO_BATCH_SIZE sets the number of betas that we must be
%% due to write indices for before we do any work at all.
-define(IO_BATCH_SIZE, 2048). %% next power-of-2 after ?CREDIT_DISC_BOUND
-define(HEADER_GUESS_SIZE, 100). %% see determine_persist_to/2
%% 持久性消息存储
-define(PERSISTENT_MSG_STORE, msg_store_persistent).
%% 短暂性消息存储
-define(TRANSIENT_MSG_STORE,  msg_store_transient).
%% RabbitMQ系统封装queue后的lqueue队列模块名
-define(QUEUE, lqueue).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

%%----------------------------------------------------------------------------

-rabbit_upgrade({multiple_routing_keys, local, []}).

-ifdef(use_specs).

-type(timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}).
-type(seq_id()  :: non_neg_integer()).

-type(rates() :: #rates { in        :: float(),
                          out       :: float(),
                          ack_in    :: float(),
                          ack_out   :: float(),
                          timestamp :: timestamp()}).

-type(delta() :: #delta { start_seq_id :: non_neg_integer(),
                          count        :: non_neg_integer(),
                          end_seq_id   :: non_neg_integer() }).

%% The compiler (rightfully) complains that ack() and state() are
%% unused. For this reason we duplicate a -spec from
%% rabbit_backing_queue with the only intent being to remove
%% warnings. The problem here is that we can't parameterise the BQ
%% behaviour by these two types as we would like to. We still leave
%% these here for documentation purposes.
-type(ack() :: seq_id()).
-type(state() :: #vqstate {
             q1                    :: ?QUEUE:?QUEUE(),
             q2                    :: ?QUEUE:?QUEUE(),
             delta                 :: delta(),
             q3                    :: ?QUEUE:?QUEUE(),
             q4                    :: ?QUEUE:?QUEUE(),
             next_seq_id           :: seq_id(),
             ram_pending_ack       :: gb_trees:tree(),
             disk_pending_ack      :: gb_trees:tree(),
             qi_pending_ack        :: gb_trees:tree(),
             index_state           :: any(),
             msg_store_clients     :: 'undefined' | {{any(), binary()},
                                                    {any(), binary()}},
             durable               :: boolean(),
             transient_threshold   :: non_neg_integer(),

             len                   :: non_neg_integer(),
             bytes                 :: non_neg_integer(),
             unacked_bytes         :: non_neg_integer(),

             persistent_count      :: non_neg_integer(),
             persistent_bytes      :: non_neg_integer(),

             target_ram_count      :: non_neg_integer() | 'infinity',
             ram_msg_count         :: non_neg_integer(),
             ram_msg_count_prev    :: non_neg_integer(),
             ram_ack_count_prev    :: non_neg_integer(),
             ram_bytes             :: non_neg_integer(),
             out_counter           :: non_neg_integer(),
             in_counter            :: non_neg_integer(),
             rates                 :: rates(),
             msgs_on_disk          :: gb_sets:set(),
             msg_indices_on_disk   :: gb_sets:set(),
             unconfirmed           :: gb_sets:set(),
             confirmed             :: gb_sets:set(),
             ack_out_counter       :: non_neg_integer(),
             ack_in_counter        :: non_neg_integer(),
             disk_read_count       :: non_neg_integer(),
             disk_write_count      :: non_neg_integer() }).
%% Duplicated from rabbit_backing_queue
-spec(ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

-spec(multiple_routing_keys/0 :: () -> 'ok').

-endif.
%% delta结构的默认初始化结构
-define(BLANK_DELTA, #delta { start_seq_id = undefined,
                              count        = 0,
                              end_seq_id   = undefined }).
-define(BLANK_DELTA_PATTERN(Z), #delta { start_seq_id = Z,
                                         count        = 0,
                                         end_seq_id   = Z }).

-define(MICROS_PER_SECOND, 1000000.0).

%% We're sampling every 5s for RAM duration; a half life that is of
%% the same order of magnitude is probably about right.
-define(RATE_AVG_HALF_LIFE, 5.0).

%% We will recalculate the #rates{} every time we get asked for our
%% RAM duration, or every N messages published, whichever is
%% sooner. We do this since the priority calculations in
%% rabbit_amqqueue_process need fairly fresh rates.
-define(MSGS_PER_RATE_CALC, 100).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
%% rabbit:recover()函数启动过程调用过来
start(DurableQueues) ->
	{AllTerms, StartFunState} = rabbit_queue_index:start(DurableQueues),
	%% 在rabbit_sup监督进程下启动msg_store_persistent负责带持久化属性的消息的持久化（重启后消息不会丢失）进程
	%% 在rabbit_sup监督进程下启动msg_store_transient负责不带持久化属性的消息的持久化（因为内存吃紧引起，重启后消息会丢失）进程
	start_msg_store(
	  [Ref || Terms <- AllTerms,
			  Terms /= non_clean_shutdown,
			  %% 拿到所有队列的标识(该标识是队列进程创建的时候通过rabbit_guid:gen()函数创建的唯一标识)
			  begin
				  Ref = proplists:get_value(persistent_ref, Terms),
				  Ref =/= undefined
			  end],
	  StartFunState),
	{ok, AllTerms}.


stop() ->
	%% 停止RabbitMQ系统的存储服务器进程(包括持久化存储进程和非持久化的存储进程)
	ok = stop_msg_store(),
	ok = rabbit_queue_index:stop().


%% 启动RabbitMQ系统中的消息存储服务器进程(包括持久化存储进程和非持久化的存储进程)
start_msg_store(Refs, StartFunState) ->
	%% 在rabbit_sup监督进程下启动msg_store_transient负责不带持久化属性的消息的持久化（因为内存吃紧引起，重启后消息会丢失）进程
	ok = rabbit_sup:start_child(?TRANSIENT_MSG_STORE, rabbit_msg_store,
								[?TRANSIENT_MSG_STORE, rabbit_mnesia:dir(),
								 undefined,  {fun (ok) -> finished end, ok}]),
	%% 在rabbit_sup监督进程下启动msg_store_persistent负责带持久化属性的消息的持久化（重启后消息不会丢失）进程
	ok = rabbit_sup:start_child(?PERSISTENT_MSG_STORE, rabbit_msg_store,
								[?PERSISTENT_MSG_STORE, rabbit_mnesia:dir(),
								 Refs, StartFunState]).


%% 停止RabbitMQ系统的存储服务器进程(包括持久化存储进程和非持久化的存储进程)
stop_msg_store() ->
	ok = rabbit_sup:stop_child(?PERSISTENT_MSG_STORE),
	ok = rabbit_sup:stop_child(?TRANSIENT_MSG_STORE).


%% 队列的初始化
init(Queue, Recover, Callback) ->
	init(
	  Queue, Recover, Callback,
	  %% 消息存储服务器进程通知队列进程已经消息存储到磁盘，消息存储服务器进程向队列进程进行confirm操作
	  fun (MsgIds, ActionTaken) ->
			   msgs_written_to_disk(Callback, MsgIds, ActionTaken)
	  end,
	  %% 队列索引模块将消息索引存入到磁盘后回调通知队列进程索引可以进行confirm操作(该回调的消息内容存储在消息存储服务器进程，没有跟消息索引存储在一起)
	  fun (MsgIds) -> msg_indices_written_to_disk(Callback, MsgIds) end,
	  %% 队列索引模块将消息索引存入到磁盘后回调通知队列进程可以进行confirm操作(该回调的消息内容同消息索引是存储在一起的，没有存储到消息存储服务器进程中)
	  fun (MsgIds) -> msgs_and_indices_written_to_disk(Callback, MsgIds) end).


%% 新创建的队列的初始化接口
init(#amqqueue { name = QueueName, durable = IsDurable }, new,
	 AsyncCallback, MsgOnDiskFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun) ->
	%% 队列消息索引的初始化,返回消息索引队列初始化后的状态数据结构
	IndexState = rabbit_queue_index:init(QueueName,
										 MsgIdxOnDiskFun, MsgAndIdxOnDiskFun),
	init(IsDurable, IndexState, 0, 0, [],
		 case IsDurable of
			 true  -> msg_store_client_init(?PERSISTENT_MSG_STORE,
											MsgOnDiskFun, AsyncCallback);
			 false -> undefined
		 end,
		 msg_store_client_init(?TRANSIENT_MSG_STORE, undefined, AsyncCallback));


%% We can be recovering(恢复) a transient(短暂的) queue if it crashed
%% 持久化队列恢复初始化过程初始化接口
init(#amqqueue { name = QueueName, durable = IsDurable }, Terms,
	 AsyncCallback, MsgOnDiskFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun) ->
	%% 如果恢复数据里有队列进程的标识，则拿出来，否则通过rabbit_guid:gen()给队列生成一个新的标识
	{PRef, RecoveryTerms} = process_recovery_terms(Terms),
	{PersistentClient, ContainsCheckFun} =
		case IsDurable of
			true  -> C = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef,
											   MsgOnDiskFun, AsyncCallback),
					 {C, fun (MsgId) when is_binary(MsgId) ->
								  %% 查看消息存储服务器进程是否有MsgId这个消息
								  rabbit_msg_store:contains(MsgId, C);
							(#basic_message{is_persistent = Persistent}) ->
								 Persistent
					  end};
			false -> {undefined, fun(_MsgId) -> false end}
		end,
	%% 消息存储的客户端的初始化(得到队列进程在在消息存储服务器进程中的数据存储结构)
	TransientClient  = msg_store_client_init(?TRANSIENT_MSG_STORE,
											 undefined, AsyncCallback),
	%% 队列是恢复类型，则在此处去恢复队列索引相关信息
	{DeltaCount, DeltaBytes, IndexState} =
		rabbit_queue_index:recover(
		  QueueName, RecoveryTerms,
		  %% 去消息存储服务器进程取存储服务器进程是否成功的恢复数据(当RabbitMQ系统崩溃的时候，会将崩溃信息写入不同的磁盘文件)
		  rabbit_msg_store:successfully_recovered_state(?PERSISTENT_MSG_STORE),
		  ContainsCheckFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun),
	init(IsDurable, IndexState, DeltaCount, DeltaBytes, RecoveryTerms,
		 PersistentClient, TransientClient).


%% 如果恢复数据里有队列进程的标识，则拿出来，否则通过rabbit_guid:gen()给队列生成一个新的标识
process_recovery_terms(Terms = non_clean_shutdown) ->
	{rabbit_guid:gen(), Terms};

process_recovery_terms(Terms) ->
	case proplists:get_value(persistent_ref, Terms) of
		undefined -> {rabbit_guid:gen(), []};
		PRef      -> {PRef, Terms}
	end.


%% 消息队列进程异常中断的时候的回调函数
terminate(_Reason, State) ->
	State1 = #vqstate { persistent_count  = PCount,
						persistent_bytes  = PBytes,
						index_state       = IndexState,
						msg_store_clients = {MSCStateP, MSCStateT} } =
						  %% 将当前队列中等待ack的非持久化消息从消息存储进程中删除掉
						  purge_pending_ack(true, State),
	%% 通知持久化存储服务器进程自己异常中断
	PRef = case MSCStateP of
			   undefined -> undefined;
			   %% 通知持久化消息存储进程当前消息队列停止，同时取得当前消息队列在持久化消息存储进程中的唯一标识
			   _         -> ok = rabbit_msg_store:client_terminate(MSCStateP),
							rabbit_msg_store:client_ref(MSCStateP)
		   end,
	ok = rabbit_msg_store:client_delete_and_terminate(MSCStateT),
	%% 得到rabbit_variable_queue这个backing_queue消息队列停止的时候需要保存的信息
	Terms = [{persistent_ref,   PRef},
			 {persistent_count, PCount},
			 {persistent_bytes, PBytes}],
	a(State1 #vqstate { index_state       = rabbit_queue_index:terminate(
											  Terms, IndexState),
						msg_store_clients = undefined }).

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.
%% 停止该消息队列，同时将将该消息队列相关信息删除掉
delete_and_terminate(_Reason, State) ->
	%% TODO: there is no need to interact with qi at all - which we do
	%% as part of 'purge' and 'purge_pending_ack', other than
	%% deleting it.
	%% 清除该队列中所有的消息
	{_PurgeCount, State1} = purge(State),
	%% 将当前队列中所有等待ack的消息从消息索引和消息存储服务器中删除掉
	State2 = #vqstate { index_state         = IndexState,
						msg_store_clients   = {MSCStateP, MSCStateT} } =
						  purge_pending_ack(false, State1),
	%% 队列进程中断的时候同时删除该队列对应的消息索引磁盘文件
	IndexState1 = rabbit_queue_index:delete_and_terminate(IndexState),
	%% 如果是持久化队列，则立刻通知持久化消息存储进程删除自己
	case MSCStateP of
		undefined -> ok;
		_         -> rabbit_msg_store:client_delete_and_terminate(MSCStateP)
	end,
	%% 通知非持久化的存储进程删除自己
	rabbit_msg_store:client_delete_and_terminate(MSCStateT),
	a(State2 #vqstate { index_state       = IndexState1,
						msg_store_clients = undefined }).


%% 队列异常崩溃，直接删除掉该队列对应的操作项磁盘文件
delete_crashed(#amqqueue{name = QName}) ->
	%% 删除消息队列名字为Name对应的目录下面所有的消息索引磁盘文件
	ok = rabbit_queue_index:erase(QName).


%% 清除该队列中所有的消息
purge(State = #vqstate { q4  = Q4,
						 len = Len }) ->
	%% TODO: when there are no pending acks, which is a common case,
	%% we could simply wipe the qi instead of issuing delivers and
	%% acks for all the messages.
	%% 将Q4队列中的所有消息删除掉
	State1 = remove_queue_entries(Q4, State),
	
	%% 清除beta(消息内容只在磁盘上，消息索引只在内存中)和delta(消息的内容和索引都在磁盘上)类型的消息
	%% 此操作会将磁盘中的消息，以及Q2，Q3队列中的消息删除掉
	State2 = #vqstate { q1 = Q1 } =
						  purge_betas_and_deltas(State1 #vqstate { q4 = ?QUEUE:new() }),
	
	%% 将Q1队列中的所有消息删除掉
	State3 = remove_queue_entries(Q1, State2),
	
	%% 返回删除掉的消息个数
	{Len, a(State3 #vqstate { q1 = ?QUEUE:new() })}.


%% 将当前队列中所有等待ack的消息从消息索引和消息存储服务器中删除掉
purge_acks(State) -> a(purge_pending_ack(false, State)).


%% 消息的发布接口
publish(Msg = #basic_message { is_persistent = IsPersistent, id = MsgId },
		MsgProps = #message_properties { needs_confirming = NeedsConfirming },
		IsDelivered, _ChPid, _Flow,
		State = #vqstate { q1 = Q1, q3 = Q3, q4 = Q4,
						   next_seq_id      = SeqId,
						   in_counter       = InCount,
						   durable          = IsDurable,
						   unconfirmed      = UC }) ->
	%% 只有持久化队列和消息持久化才会对消息进行持久化
	IsPersistent1 = IsDurable andalso IsPersistent,
	%% 组装消息状态(该数据结构是实际存储在队列中的数据)
	MsgStatus = msg_status(IsPersistent1, IsDelivered, SeqId, Msg, MsgProps),
	%% 如果队列和消息都是持久化类型，则将消息内容和消息在队列中的索引写入磁盘
	{MsgStatus1, State1} = maybe_write_to_disk(false, false, MsgStatus, State),
	%% 将消息状态数据结构存入内存(如果Q3队列不为空，则将新消息存入Q1队列，如果为空则将新消息存入Q4队列)
	State2 = case ?QUEUE:is_empty(Q3) of
				 %% 如果Q3队列不为空，则将当前的消息写入Q1队列
				 false -> State1 #vqstate { q1 = ?QUEUE:in(m(MsgStatus1), Q1) };
				 %% 如果Q3队列为空，则将当前的消息写入Q4队列
				 true  -> State1 #vqstate { q4 = ?QUEUE:in(m(MsgStatus1), Q4) }
			 end,
	%% 进入队列中的消息数量加一
	InCount1 = InCount + 1,
	%% 如果消息需要确认，将该消息加入unconfirmed字段
	UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
	%% 更新队列进程中的状态信息
	State3 = stats({1, 0}, {none, MsgStatus1},
				   %% 更新下一个消息在消息中的位置
				   State2#vqstate{ next_seq_id = SeqId + 1,
								   in_counter  = InCount1,
								   unconfirmed = UC1 }),
	%% RabbitMQ系统中使用的内存过多，此操作是将内存中的队列数据写入到磁盘中
	a(reduce_memory_use(maybe_update_rates(State3))).


%% 该消息已经被消费者取走，等待消费者的恢复确认，因此该消息是不会放入队列中，但是如果消息为持久化消息，需要将消息存入磁盘文件中
publish_delivered(Msg = #basic_message { is_persistent = IsPersistent,
										 id = MsgId },
				  MsgProps = #message_properties {
												  needs_confirming = NeedsConfirming },
				  _ChPid, _Flow,
				  State = #vqstate { next_seq_id      = SeqId,
									 out_counter      = OutCount,
									 in_counter       = InCount,
									 durable          = IsDurable,
									 unconfirmed      = UC }) ->
	%% 只有持久化队列和消息持久化才会对消息进行持久化
	IsPersistent1 = IsDurable andalso IsPersistent,
	%% 组装消息状态(该数据结构是实际存储在队列中的数据，该消息表示已经被传递过)
	MsgStatus = msg_status(IsPersistent1, true, SeqId, Msg, MsgProps),
	%% 如果队列和消息都是持久化类型，则将消息实体和消息在队列中的索引写入磁盘
	{MsgStatus1, State1} = maybe_write_to_disk(false, false, MsgStatus, State),
	%% 记录该消息到等待ack的二叉树里
	State2 = record_pending_ack(m(MsgStatus1), State1),
	%% 如果消息需要确认，将该消息加入unconfirmed字段
	UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
	%% 更新进入队列的消息字段in_counter加一，退出队列的消息字段out_counter加一
	State3 = stats({0, 1}, {none, MsgStatus1},
				   State2 #vqstate { next_seq_id      = SeqId    + 1,
									 out_counter      = OutCount + 1,
									 in_counter       = InCount  + 1,
									 unconfirmed      = UC1 }),
	%% RabbitMQ系统中使用的内存过多，此操作是将内存中的队列数据写入到磁盘中
	{SeqId, a(reduce_memory_use(maybe_update_rates(State3)))}.


%% discard：丢弃
%% 消息队列丢弃消息的接口
%% 丢弃的情况一：消息发送给消费者，但是不需要进行ack，则立刻丢弃
%% 丢弃的情况二：消息没有发送给消费者，但是当前消息队列消息存活时间设置为0，同时没有设置死亡消息转发的交换机exchange
discard(_MsgId, _ChPid, _Flow, State) -> State.


%% 得到backing_queue中已经得到confirm的消息列表
drain_confirmed(State = #vqstate { confirmed = C }) ->
	case gb_sets:is_empty(C) of
		true  -> {[], State}; %% common case
		false -> {gb_sets:to_list(C), State #vqstate {
													  confirmed = gb_sets:new() }}
	end.


%% 通过消息message_properties的特性字段去执行Pred函数，如果条件满足，则将该消息从backing_queue中删除掉
%% 直到找到一个不满足的消息，则停止操作(该操作主要是将过期的消息删除掉)
dropwhile(Pred, State) ->
	case queue_out(State) of
		{empty, State1} ->
			{undefined, a(State1)};
		{{value, MsgStatus = #msg_status { msg_props = MsgProps }}, State1} ->
			case Pred(MsgProps) of
				%% 如果满足条件，则将该消息从backing_queue中删除掉
				true  -> {_, State2} = remove(false, MsgStatus, State1),
						 %% 继续操作队列中的下一个消息
						 dropwhile(Pred, State2);
				%% 将取出来的没有删除的消息存入到队列的头部
				false -> {MsgProps, a(in_r(MsgStatus, State1))}
			end
	end.


%% 通过消息message_properties的特性字段去执行Pred函数，如果条件满足，将消息执行Fun函数，然后将结果放入到Acc列表中
%% 直到找到一个不满足的消息，则停止操作(该操作主要是将过期的消息删除掉)
fetchwhile(Pred, Fun, Acc, State) ->
	case queue_out(State) of
		{empty, State1} ->
			{undefined, Acc, a(State1)};
		{{value, MsgStatus = #msg_status { msg_props = MsgProps }}, State1} ->
			case Pred(MsgProps) of
				%% 如果满足条件，且消息的内容存储在消息存储服务器进程中，则将内容从磁盘中读取出来
				true  -> {Msg, State2} = read_msg(MsgStatus, State1),
						 %% 再删除消息的同时，将该消息设置为等待消费者ack的消息(执行fetchwhile函数后，必须对这些消息进行ack操作才能将这些消息彻底删除)
						 {AckTag, State3} = remove(true, MsgStatus, State2),
						 %% 继续向队列中的下一个消息执行该操作
						 fetchwhile(Pred, Fun, Fun(Msg, AckTag, Acc), State3);
				%% 将取出来的没有删除的消息存入到队列的头部
				false -> {MsgProps, Acc, a(in_r(MsgStatus, State1))}
			end
	end.


%% 从消息队列中读取消息
fetch(AckRequired, State) ->
	case queue_out(State) of
		{empty, State1} ->
			{empty, a(State1)};
		{{value, MsgStatus}, State1} ->
			%% it is possible that the message wasn't read from disk
			%% at this point, so read it in.
			%% 读取消息(如果该消息结构里面没有消息实体则需要从消息存储服务器进程中读取)
			{Msg, State2} = read_msg(MsgStatus, State1),
			%% 删除消息(包括从存储服务器进程中和消息队列索引中删除消息)
			{AckTag, State3} = remove(AckRequired, MsgStatus, State2),
			{{Msg, MsgStatus#msg_status.is_delivered, AckTag}, a(State3)}
	end.


%% 将消息队列中头部的消息删除掉
drop(AckRequired, State) ->
	case queue_out(State) of
		{empty, State1} ->
			{empty, a(State1)};
		{{value, MsgStatus}, State1} ->
			{AckTag, State2} = remove(AckRequired, MsgStatus, State1),
			{{MsgStatus#msg_status.msg_id, AckTag}, a(State2)}
	end.


%% ack接口
ack([], State) ->
	{[], State};
%% optimisation: this head is essentially a partial evaluation of the
%% general case below, for the single-ack case.
%% 传入的是消息在队列索引中的序列号
ack([SeqId], State) ->
	{#msg_status { msg_id        = MsgId,
				   is_persistent = IsPersistent,
				   msg_in_store  = MsgInStore,
				   index_on_disk = IndexOnDisk },
	 %% 根据SeqID删除等待ack二叉树中的数据
	 State1 = #vqstate { index_state       = IndexState,
						 msg_store_clients = MSCState,
						 ack_out_counter   = AckOutCount }} =
		remove_pending_ack(true, SeqId, State),
	%% 如果该消息存在于队列索引中，则去队列索引中进行ack操作
	IndexState1 = case IndexOnDisk of
					  true  -> rabbit_queue_index:ack([SeqId], IndexState);
					  false -> IndexState
				  end,
	%% 如果消息体保存在消息存储服务器进程中，则去该进程将该消息体删除掉
	case MsgInStore of
		true  -> ok = msg_store_remove(MSCState, IsPersistent, [MsgId]);
		false -> ok
	end,
	%% 更新从队列中出去的消息个数加一
	{[MsgId],
	 a(State1 #vqstate { index_state      = IndexState1,
						 ack_out_counter  = AckOutCount + 1 })};

ack(AckTags, State) ->
	%% 根据AckTags列表中的SeqId得到索引存储在队列索引中的SeqId，消息体保存在消息存储服务器进程中的SeqId列表
	{{IndexOnDiskSeqIds, MsgIdsByStore, AllMsgIds},
	 State1 = #vqstate { index_state       = IndexState,
						 msg_store_clients = MSCState,
						 ack_out_counter   = AckOutCount }} =
		lists:foldl(
		  fun (SeqId, {Acc, State2}) ->
				   %% 如果该消息存在于队列索引中，则去队列索引中进行ack操作
				   {MsgStatus, State3} = remove_pending_ack(true, SeqId, State2),
				   %% 拿到消息在队列索引中的ID，以及区分出msg_in_store为true，但是IsPersistent为false的消息ID列表，以及IsPersistent为true的消息ID列表
				   {accumulate_ack(MsgStatus, Acc), State3}
		  end, {accumulate_ack_init(), State}, AckTags),
	%% 根据得到的在存储在队列索引中的SeqId，去队列索引中将这些SeqId删除掉
	IndexState1 = rabbit_queue_index:ack(IndexOnDiskSeqIds, IndexState),
	%% 根据得到的存储在消息存储服务器进程中的SeqId列表，从消息存储服务器中将这些消息的消息体删除掉
	[ok = msg_store_remove(MSCState, IsPersistent, MsgIds)
			|| {IsPersistent, MsgIds} <- orddict:to_list(MsgIdsByStore)],
	%% 更新从队列中出去的消息个数加上传进来的SeqiId列表个数
	{lists:reverse(AllMsgIds),
	 a(State1 #vqstate { index_state      = IndexState1,
						 ack_out_counter  = AckOutCount + length(AckTags) })}.


%% requeue：重新排队
%% 将AckTags列表中的消息重新放入队列，然后将队列重新排序
requeue(AckTags, #vqstate { delta      = Delta,
							q3         = Q3,
							q4         = Q4,
							in_counter = InCounter,
							len        = Len } = State) ->
	%% 将AckTags能够合并到Q4队列中的消息合并到Q4队列
	{SeqIds,  Q4a, MsgIds,  State1} = queue_merge(lists:sort(AckTags), Q4, [],
												  %% 拿到队列Q3的最小队列索引
												  beta_limit(Q3),
												  fun publish_alpha/2, State),
	{SeqIds1, Q3a, MsgIds1, State2} = queue_merge(SeqIds, Q3, MsgIds,
												  %% 拿到索引磁盘中的最小队列索引
												  delta_limit(Delta),
												  fun publish_beta/2, State1),
	%% 将AckTags中处于磁盘的消息重新写入磁盘
	{Delta1, MsgIds2, State3}       = delta_merge(SeqIds1, Delta, MsgIds1,
												  State2),
	MsgCount = length(MsgIds2),
	{MsgIds2, a(reduce_memory_use(
				  maybe_update_rates(
					State3 #vqstate { delta      = Delta1,
									  q3         = Q3a,
									  q4         = Q4a,
									  in_counter = InCounter + MsgCount,
									  len        = Len + MsgCount })))}.


%% 对等待ack的每个消息进行MsgFun函数操作
ackfold(MsgFun, Acc, State, AckTags) ->
	{AccN, StateN} =
		lists:foldl(fun(SeqId, {Acc0, State0}) ->
							%% 根据SeqId查找等待确认的消息
							MsgStatus = lookup_pending_ack(SeqId, State0),
							%% 如果消息内容在磁盘中，则将该消息内容从磁盘中读取出来
							{Msg, State1} = read_msg(MsgStatus, State0),
							{MsgFun(Msg, SeqId, Acc0), State1}
					end, {Acc, State}, AckTags),
	{AccN, a(StateN)}.


fold(Fun, Acc, State = #vqstate{index_state = IndexState}) ->
	{Its, IndexState1} = lists:foldl(fun inext/2, {[], IndexState},
									 [msg_iterator(State),
									  disk_ack_iterator(State),
									  ram_ack_iterator(State),
									  qi_ack_iterator(State)]),
	ifold(Fun, Acc, Its, State#vqstate{index_state = IndexState1}).


%% 得到当前队列中的消息个数
len(#vqstate { len = Len }) -> Len.


%% 判断当前队列是否为空
is_empty(State) -> 0 == len(State).


%% 得到队列中所有的消息长度(包括等待ack的所有消息)
depth(State = #vqstate { ram_pending_ack  = RPA,
						 disk_pending_ack = DPA,
						 qi_pending_ack   = QPA }) ->
	len(State) + gb_trees:size(RPA) + gb_trees:size(DPA) + gb_trees:size(QPA).


%% rabbit_memory_monitor进程通知消息队列最新的内存持续时间
set_ram_duration_target(
  DurationTarget, State = #vqstate {
									rates = #rates { in      = AvgIngressRate,
													 out     = AvgEgressRate,
													 ack_in  = AvgAckIngressRate,
													 ack_out = AvgAckEgressRate },
									target_ram_count = TargetRamCount }) ->
	%% 获得所有的速率之和
	Rate =
		AvgEgressRate + AvgIngressRate + AvgAckEgressRate + AvgAckIngressRate,
	%% 计算最新的内存中最多的消息数量
	TargetRamCount1 =
		case DurationTarget of
			infinity  -> infinity;
			_         -> trunc(DurationTarget * Rate) %% msgs = sec * msgs/sec
		end,
	%% 得到最新的内存中最多的消息数
	State1 = State #vqstate { target_ram_count = TargetRamCount1 },
	a(case TargetRamCount1 == infinity orelse
			   (TargetRamCount =/= infinity andalso
					TargetRamCount1 >= TargetRamCount) of
		  true  -> State1;
		  %% 如果最新的内存中的消息数量小于老的内存中的消息数量，则立刻进行减少内存中的消息的操作
		  false -> reduce_memory_use(State1)
	  end).


%% 判断是否需要更新速率
maybe_update_rates(State = #vqstate{ in_counter  = InCount,
									 out_counter = OutCount })
  when InCount + OutCount > ?MSGS_PER_RATE_CALC ->
	update_rates(State);
maybe_update_rates(State) ->
	State.


%% 更新当前消息队列的速率
update_rates(State = #vqstate{ in_counter      =     InCount,
							   out_counter     =    OutCount,
							   ack_in_counter  =  AckInCount,
							   ack_out_counter = AckOutCount,
							   rates = #rates{ in        =     InRate,
											   out       =    OutRate,
											   ack_in    =  AckInRate,
											   ack_out   = AckOutRate,
											   timestamp = TS }}) ->
	Now = erlang:now(),
	
	Rates = #rates { in        = update_rate(Now, TS,     InCount,     InRate),
					 out       = update_rate(Now, TS,    OutCount,    OutRate),
					 ack_in    = update_rate(Now, TS,  AckInCount,  AckInRate),
					 ack_out   = update_rate(Now, TS, AckOutCount, AckOutRate),
					 timestamp = Now },
	
	State#vqstate{ in_counter      = 0,
				   out_counter     = 0,
				   ack_in_counter  = 0,
				   ack_out_counter = 0,
				   rates           = Rates }.


%% 实际更新速率的接口
update_rate(Now, TS, Count, Rate) ->
	%% 得到秒数
	Time = timer:now_diff(Now, TS) / ?MICROS_PER_SECOND,
	rabbit_misc:moving_average(Time, ?RATE_AVG_HALF_LIFE, Count / Time, Rate).


%% 获得当前消息队列内存中消息速率中分母持续时间大小
ram_duration(State) ->
	State1 = #vqstate { rates = #rates { in      = AvgIngressRate,
										 out     = AvgEgressRate,
										 ack_in  = AvgAckIngressRate,
										 ack_out = AvgAckEgressRate },
						ram_msg_count      = RamMsgCount,
						ram_msg_count_prev = RamMsgCountPrev,
						ram_pending_ack    = RPA,
						qi_pending_ack     = QPA,
						ram_ack_count_prev = RamAckCountPrev } =
						  update_rates(State),
	
	%% 获得内存中等待ack的消息数量
	RamAckCount = gb_trees:size(RPA) + gb_trees:size(QPA),
	
	Duration = %% (msgs+acks) / ((msgs+acks)/sec) == sec
		case lists:all(fun (X) -> X < 0.01 end,
					   [AvgEgressRate, AvgIngressRate,
						AvgAckEgressRate, AvgAckIngressRate]) of
			true  -> infinity;
			%% (内存中的消息数量 + 内存中等待ack的消息数量) / (4 * (消息进入速率 + 消息出的速率 + 消息进入ack速率 + 消息出ack速率)) = 平均的持续时间
			false -> (RamMsgCountPrev + RamMsgCount +
						  RamAckCount + RamAckCountPrev) /
						 (4 * (AvgEgressRate + AvgIngressRate +
								   AvgAckEgressRate + AvgAckIngressRate))
		end,
	
	{Duration, State1}.


%% 判断是否需要进行同步confirm操作
needs_timeout(#vqstate { index_state = IndexState }) ->
	case rabbit_queue_index:needs_sync(IndexState) of
		confirms -> timed;
		other    -> idle;
		false    -> false
	end.


%% confirm同步的操作
timeout(State = #vqstate { index_state = IndexState }) ->
	State #vqstate { index_state = rabbit_queue_index:sync(IndexState) }.


%% 刷新日志文件，将日志文件中的操作项存入对应的操作项的磁盘文件(队列进程从休眠状态接收到一个消息后，则会调用该接口进行一次日志文件的刷新)
handle_pre_hibernate(State = #vqstate { index_state = IndexState }) ->
	State #vqstate { index_state = rabbit_queue_index:flush(IndexState) }.


%% 睡眠接口，RabbitMQ系统中使用的内存过多，此操作是将内存中的队列数据写入到磁盘中
resume(State) -> a(reduce_memory_use(State)).


%% 获取消息队列中消息进入和出队列的速率大小
msg_rates(#vqstate { rates = #rates { in  = AvgIngressRate,
									  out = AvgEgressRate } }) ->
	{AvgIngressRate, AvgEgressRate}.


%% 拿到队列中的信息
%% 拿到队列中已经准备好同时在内存中的消息数量
info(messages_ready_ram, #vqstate{ram_msg_count = RamMsgCount}) ->
	RamMsgCount;

%% 拿到在内存中等待ack的消息数量
info(messages_unacknowledged_ram, #vqstate{ram_pending_ack = RPA,
										   qi_pending_ack  = QPA}) ->
	gb_trees:size(RPA) + gb_trees:size(QPA);

%% 拿到队列中消息在内存中的数量
info(messages_ram, State) ->
	info(messages_ready_ram, State) + info(messages_unacknowledged_ram, State);

%% 拿到队列中持久化的消息数量
info(messages_persistent, #vqstate{persistent_count = PersistentCount}) ->
	PersistentCount;

%% 拿到队列中消息内容的总大小
info(message_bytes, #vqstate{bytes         = Bytes,
							 unacked_bytes = UBytes}) ->
	Bytes + UBytes;

%% 拿到队列中已经准备好的消息内容的大小
info(message_bytes_ready, #vqstate{bytes = Bytes}) ->
	Bytes;

%% 拿到队列中还未ack的消息内容占的总大小
info(message_bytes_unacknowledged, #vqstate{unacked_bytes = UBytes}) ->
	UBytes;

%% 拿到队列中消息内容在内存中的总大小
info(message_bytes_ram, #vqstate{ram_bytes = RamBytes}) ->
	RamBytes;

%% 拿到队列中持久化消息的总大小
info(message_bytes_persistent, #vqstate{persistent_bytes = PersistentBytes}) ->
	PersistentBytes;

%% 拿到队列从磁盘读取的次数(包括从消息存储服务器读取消息内容，还有从队列索引读取消息在队列中的索引)
info(disk_reads, #vqstate{disk_read_count = Count}) ->
	Count;

%% 拿到队列中写磁盘的次数(包括向消息存储服务器进程存储消息内容，还有向消息索引中存储消息在队列中的索引)
info(disk_writes, #vqstate{disk_write_count = Count}) ->
	Count;

%% 拿到队列的基本信息
info(backing_queue_status, #vqstate {
									 q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
									 len              = Len,
									 target_ram_count = TargetRamCount,
									 next_seq_id      = NextSeqId,
									 rates            = #rates { in      = AvgIngressRate,
																 out     = AvgEgressRate,
																 ack_in  = AvgAckIngressRate,
																 ack_out = AvgAckEgressRate }}) ->
	[ {q1                  , ?QUEUE:len(Q1)},
	  {q2                  , ?QUEUE:len(Q2)},
	  {delta               , Delta},
	  {q3                  , ?QUEUE:len(Q3)},
	  {q4                  , ?QUEUE:len(Q4)},
	  {len                 , Len},
	  {target_ram_count    , TargetRamCount},
	  {next_seq_id         , NextSeqId},
	  {avg_ingress_rate    , AvgIngressRate},
	  {avg_egress_rate     , AvgEgressRate},
	  {avg_ack_ingress_rate, AvgAckIngressRate},
	  {avg_ack_egress_rate , AvgAckEgressRate} ];

info(Item, _) ->
	throw({bad_argument, Item}).


invoke(?MODULE, Fun, State) -> Fun(?MODULE, State);
invoke(      _,   _, State) -> State.


is_duplicate(_Msg, State) -> {false, State}.

%%----------------------------------------------------------------------------
%% Minor helpers(次要的辅助函数)
%%----------------------------------------------------------------------------
%% backing_queue模块中做当前消息队列的断言
a(State = #vqstate { q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
					 len              = Len,
					 bytes            = Bytes,
					 unacked_bytes    = UnackedBytes,
					 persistent_count = PersistentCount,
					 persistent_bytes = PersistentBytes,
					 ram_msg_count    = RamMsgCount,
					 ram_bytes        = RamBytes}) ->
	E1 = ?QUEUE:is_empty(Q1),
	E2 = ?QUEUE:is_empty(Q2),
	ED = Delta#delta.count == 0,
	E3 = ?QUEUE:is_empty(Q3),
	E4 = ?QUEUE:is_empty(Q4),
	LZ = Len == 0,
	
	%% Q1队列不为空，则Q3队列必须不为空
	true = E1 or not E3,
	%% Q2队列不为空，则delta磁盘中的消息必须不为空
	true = E2 or not ED,
	%% delta磁盘中的消息不为空，则Q3队列中必须不为空
	true = ED or not E3,
	%% 如果消息队列中没有消息，则Q3和Q4队列必须同时为空
	true = LZ == (E3 and E4),
	
	%% 整数值必须是大于0的值
	true = Len             >= 0,
	true = Bytes           >= 0,
	true = UnackedBytes    >= 0,
	true = PersistentCount >= 0,
	true = PersistentBytes >= 0,
	true = RamMsgCount     >= 0,
	true = RamMsgCount     =< Len,
	true = RamBytes        >= 0,
	true = RamBytes        =< Bytes + UnackedBytes,
	
	State.


%% 判断当前消息队列中在磁盘上的消息加上索引开始的数字小于消息索引当前结束数字(即判断磁盘上的消息数量的正确性)
d(Delta = #delta { start_seq_id = Start, count = Count, end_seq_id = End })
  when Start + Count =< End ->
	Delta.


%% 判断消息的合法性，即只有持久化的消息才能存储到磁盘和索引中
m(MsgStatus = #msg_status { is_persistent = IsPersistent,
							msg_in_store  = MsgInStore,
							index_on_disk = IndexOnDisk }) ->
	%% 不能出现消息是持久化消息，但是没有存储在消息索引中
	true = (not IsPersistent) or IndexOnDisk,
	%% 消息内容必须存储在内存中或者存储在磁盘中
	true = msg_in_ram(MsgStatus) or MsgInStore,
	MsgStatus.


%% 将bool类型转换为整形数字
one_if(true ) -> 1;
one_if(false) -> 0.


%% 如果第一个参数为true，则将E加入到L这个列表中
cons_if(true,   E, L) -> [E | L];
cons_if(false, _E, L) -> L.


%% 如果消息需要确认，将该消息加入unconfirmed字段
gb_sets_maybe_insert(false, _Val, Set) -> Set;
gb_sets_maybe_insert(true,   Val, Set) -> gb_sets:add(Val, Set).


%% 组装消息状态
%% (该数据结构是实际存储在队列中的数据，初始化的时候msg_in_store为false表示没有将消息内容存储在磁盘，index_on_disk为false表示消息在队列中的索引没有存储在队列索引队列的磁盘上)
msg_status(IsPersistent, IsDelivered, SeqId,
		   Msg = #basic_message {id = MsgId}, MsgProps) ->
	#msg_status{seq_id        = SeqId,
				msg_id        = MsgId,
				msg           = Msg,
				is_persistent = IsPersistent,
				is_delivered  = IsDelivered,
				msg_in_store  = false,
				index_on_disk = false,
				%% 决定消息实体的存储位置，如果当前消息实体的实际大小大于queue_index_embed_msgs_below配置的字段，则存储到消息存储进程，否则跟消息索引存储在一起
				persist_to    = determine_persist_to(Msg, MsgProps),
				msg_props     = MsgProps}.


%% 将磁盘文件中的信息转化为内存中的msg_status结构(有消息内容)
beta_msg_status({Msg = #basic_message{id = MsgId},
				 SeqId, MsgProps, IsPersistent, IsDelivered}) ->
	MS0 = beta_msg_status0(SeqId, MsgProps, IsPersistent, IsDelivered),
	MS0#msg_status{msg_id       = MsgId,
				   msg          = Msg,
				   persist_to   = queue_index,
				   msg_in_store = false};

%% 将磁盘文件中的信息转化为内存中的msg_status结构(没有消息内容)
beta_msg_status({MsgId, SeqId, MsgProps, IsPersistent, IsDelivered}) ->
	MS0 = beta_msg_status0(SeqId, MsgProps, IsPersistent, IsDelivered),
	MS0#msg_status{msg_id       = MsgId,
				   msg          = undefined,
				   persist_to   = msg_store,
				   msg_in_store = true}.


%% 组装没有消息内容的msg_status结构
beta_msg_status0(SeqId, MsgProps, IsPersistent, IsDelivered) ->
	#msg_status{seq_id        = SeqId,
				msg           = undefined,
				is_persistent = IsPersistent,
				is_delivered  = IsDelivered,
				index_on_disk = true,
				msg_props     = MsgProps}.


%% 将内存中msg_status中的消息体字段msg置空，即将消息从内存中清掉
trim_msg_status(MsgStatus) ->
	case persist_to(MsgStatus) of
		msg_store   -> MsgStatus#msg_status{msg = undefined};
		queue_index -> MsgStatus
	end.


%% 根据消息是否持久化的标志找到不同的消息存储客户端进行相关的消息存储
with_msg_store_state({MSCStateP, MSCStateT},  true, Fun) ->
	{Result, MSCStateP1} = Fun(MSCStateP),
	{Result, {MSCStateP1, MSCStateT}};
with_msg_store_state({MSCStateP, MSCStateT}, false, Fun) ->
	{Result, MSCStateT1} = Fun(MSCStateT),
	{Result, {MSCStateP, MSCStateT1}}.


with_immutable_msg_store_state(MSCState, IsPersistent, Fun) ->
	{Res, MSCState} = with_msg_store_state(MSCState, IsPersistent,
										   fun (MSCState1) ->
													{Fun(MSCState1), MSCState1}
										   end),
	Res.


%% 消息存储的客户端的初始化(自己生成一个客户端Ref)
msg_store_client_init(MsgStore, MsgOnDiskFun, Callback) ->
	msg_store_client_init(MsgStore, rabbit_guid:gen(), MsgOnDiskFun,
						  Callback).


%% 消息存储的客户端的初始化
msg_store_client_init(MsgStore, Ref, MsgOnDiskFun, Callback) ->
	%% 客户端Ref是rabbit_guid:gen()生成的唯一ID标识
	CloseFDsFun = msg_store_close_fds_fun(MsgStore =:= ?PERSISTENT_MSG_STORE),
	%% 向MsgStore消息存储服务器进程进行注册初始化
	rabbit_msg_store:client_init(MsgStore, Ref, MsgOnDiskFun,
								 fun () -> Callback(?MODULE, CloseFDsFun) end).


%% 从消息存储服务器进程中写消息
msg_store_write(MSCState, IsPersistent, MsgId, Msg) ->
	with_immutable_msg_store_state(
	  MSCState, IsPersistent,
	  fun (MSCState1) ->
			   rabbit_msg_store:write_flow(MsgId, Msg, MSCState1)
	  end).


%% 从消息存储服务器进程读取消息
msg_store_read(MSCState, IsPersistent, MsgId) ->
	with_msg_store_state(
	  MSCState, IsPersistent,
	  fun (MSCState1) ->
			   rabbit_msg_store:read(MsgId, MSCState1)
	  end).


%% 从消息存储服务器进程删除消息
msg_store_remove(MSCState, IsPersistent, MsgIds) ->
	with_immutable_msg_store_state(
	  MSCState, IsPersistent,
	  fun (MCSState1) ->
			   rabbit_msg_store:remove(MsgIds, MCSState1)
	  end).


msg_store_close_fds(MSCState, IsPersistent) ->
	with_msg_store_state(
	  MSCState, IsPersistent,
	  fun (MSCState1) -> rabbit_msg_store:close_all_indicated(MSCState1) end).


msg_store_close_fds_fun(IsPersistent) ->
	fun (?MODULE, State = #vqstate { msg_store_clients = MSCState }) ->
			 {ok, MSCState1} = msg_store_close_fds(MSCState, IsPersistent),
			 State #vqstate { msg_store_clients = MSCState1 }
	end.


%% 如果该消息已经传递给消费者，则需要通知队列索引给该消息做一个标记
maybe_write_delivered(false, _SeqId, IndexState) ->
	IndexState;
maybe_write_delivered(true, SeqId, IndexState) ->
	rabbit_queue_index:deliver([SeqId], IndexState).


%% 过滤掉从rabbit_queue_index中读取过来的消息队列索引(如果该消息不是持久化的则需要删除掉)，最后得到当前内存中准备好的消息个数以及内存中的消息的总的大小
betas_from_index_entries(List, TransientThreshold, RPA, DPA, QPA, IndexState) ->
	{Filtered, Delivers, Acks, RamReadyCount, RamBytes} =
		lists:foldr(
		  fun ({_MsgOrId, SeqId, _MsgProps, IsPersistent, IsDelivered} = M,
			   {Filtered1, Delivers1, Acks1, RRC, RB} = Acc) ->
				   %% 如果当前的消息索引SeqId小于当前最大的队列索引且不是持久化的消息则将该队列索引删除掉
				   %% 将RabbitMQ系统上次关闭系统时候非持久化写入索引磁盘的消息从队列索引中删除
				   case SeqId < TransientThreshold andalso not IsPersistent of
					   %% 此处的情况是RabbitMQ系统关闭以前非持久化消息存储到磁盘中的索引信息再从磁盘读取出来的时候必须将他们彻底从RabbitMQ系统中删除
					   true  -> {Filtered1,
								 cons_if(not IsDelivered, SeqId, Delivers1),
								 [SeqId | Acks1], RRC, RB};
					   false -> %% 将磁盘文件中的信息转化为内存中的msg_status结构
						   MsgStatus = m(beta_msg_status(M)),
						   %% 判断消息实体是否在内存中
						   HaveMsg = msg_in_ram(MsgStatus),
						   %% 拿到消息的大小
						   Size = msg_size(MsgStatus),
						   case (gb_trees:is_defined(SeqId, RPA) orelse
									 gb_trees:is_defined(SeqId, DPA) orelse
									 gb_trees:is_defined(SeqId, QPA)) of
							   false -> {?QUEUE:in_r(MsgStatus, Filtered1),
										 Delivers1, Acks1,
										 RRC + one_if(HaveMsg),
										 RB + one_if(HaveMsg) * Size};
							   true  -> Acc %% [0]
						   end
				   end
		  end, {?QUEUE:new(), [], [], 0, 0}, List),
	{Filtered, RamReadyCount, RamBytes,
	 %% 将RabbitMQ系统上次关闭系统时候非持久化写入索引磁盘的消息从队列索引中删除
	 rabbit_queue_index:ack(
	   Acks, rabbit_queue_index:deliver(Delivers, IndexState))}.

%% [0] We don't increase RamBytes here, even though it pertains to
%% unacked messages too, since if HaveMsg then the message must have
%% been stored in the QI, thus the message must have been in
%% qi_pending_ack, thus it must already have been in RAM.

%% 在将一个SeqId的消息写入索引文件后，扩展delta数据结构
%% 该情况是delta为默认值的情况
expand_delta(SeqId, ?BLANK_DELTA_PATTERN(X)) ->
	d(#delta { start_seq_id = SeqId, count = 1, end_seq_id = SeqId + 1 });

%% 新增加的SeqId小于delta的最小SeqId
expand_delta(SeqId, #delta { start_seq_id = StartSeqId,
							 count        = Count } = Delta)
  when SeqId < StartSeqId ->
	d(Delta #delta { start_seq_id = SeqId, count = Count + 1 });

%% 新增加的SeqId大于delta的最大值
expand_delta(SeqId, #delta { count        = Count,
							 end_seq_id   = EndSeqId } = Delta)
  when SeqId >= EndSeqId ->
	d(Delta #delta { count = Count + 1, end_seq_id = SeqId + 1 });

%% 新增加的SeqId在当前delta的中间
expand_delta(_SeqId, #delta { count       = Count } = Delta) ->
	d(Delta #delta { count = Count + 1 }).

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API(内部主要的辅助函数)
%%----------------------------------------------------------------------------
%% 最终队列的初始化函数
init(IsDurable, IndexState, DeltaCount, DeltaBytes, Terms,
	 PersistentClient, TransientClient) ->
	%% 该接口只有在队列进程初始化的时候会被调用(该接口得到最小的队列索引ID和当前最大的队列索引ID)
	{LowSeqId, NextSeqId, IndexState1} = rabbit_queue_index:bounds(IndexState),
	
	{DeltaCount1, DeltaBytes1} =
		case Terms of
			non_clean_shutdown -> {DeltaCount, DeltaBytes};
			%% 从恢复信息中拿到当前队列的持久化消息数量和持久化消息的大小
			_                  -> {proplists:get_value(persistent_count,
													   Terms, DeltaCount),
								   proplists:get_value(persistent_bytes,
													   Terms, DeltaBytes)}
		end,
	Delta = case DeltaCount1 == 0 andalso DeltaCount /= undefined of
				%% 返回delta结构的默认初始化结构
				true  -> ?BLANK_DELTA;
				%% 得到最新的delta数据结构(该结构表示当前消息队列在磁盘上的消息信息)
				false -> d(#delta { start_seq_id = LowSeqId,
									count        = DeltaCount1,
									end_seq_id   = NextSeqId })
			end,
	Now = now(),
	State = #vqstate {
					  q1                  = ?QUEUE:new(),
					  q2                  = ?QUEUE:new(),
					  delta               = Delta,
					  q3                  = ?QUEUE:new(),
					  q4                  = ?QUEUE:new(),
					  next_seq_id         = NextSeqId,
					  ram_pending_ack     = gb_trees:empty(),
					  disk_pending_ack    = gb_trees:empty(),
					  qi_pending_ack      = gb_trees:empty(),
					  index_state         = IndexState1,
					  msg_store_clients   = {PersistentClient, TransientClient},
					  durable             = IsDurable,
					  transient_threshold = NextSeqId,
					  
					  len                 = DeltaCount1,
					  persistent_count    = DeltaCount1,
					  bytes               = DeltaBytes1,
					  persistent_bytes    = DeltaBytes1,
					  
					  target_ram_count    = infinity,
					  ram_msg_count       = 0,
					  ram_msg_count_prev  = 0,
					  ram_ack_count_prev  = 0,
					  ram_bytes           = 0,
					  unacked_bytes       = 0,
					  out_counter         = 0,
					  in_counter          = 0,
					  rates               = blank_rates(Now),
					  msgs_on_disk        = gb_sets:new(),
					  msg_indices_on_disk = gb_sets:new(),
					  unconfirmed         = gb_sets:new(),
					  confirmed           = gb_sets:new(),
					  ack_out_counter     = 0,
					  ack_in_counter      = 0,
					  disk_read_count     = 0,
					  disk_write_count    = 0 },
	a(maybe_deltas_to_betas(State)).


%% 初始化队列速率相关的数据结构
blank_rates(Now) ->
	#rates { in        = 0.0,
			 out       = 0.0,
			 ack_in    = 0.0,
			 ack_out   = 0.0,
			 timestamp = Now}.


%% 将参数的第一个消息放入到当前队列的头部
in_r(MsgStatus = #msg_status { msg = undefined },
	 State = #vqstate { q3 = Q3, q4 = Q4 }) ->
	case ?QUEUE:is_empty(Q4) of
		%% 如果Q4为空，而要插入的消息类型beta和gamma类型，则将要插入的消息直接插入到Q3队列的头部
		true  -> State #vqstate { q3 = ?QUEUE:in_r(MsgStatus, Q3) };
		%% 如果当前队列Q4中有alpha类型的消息，则需要将插入的消息的消息内容从消息存储服务器进程中读取出来，将要插入的消息做成alpha类型的消息，然后插入到Q4队列的头部
		false -> {Msg, State1 = #vqstate { q4 = Q4a }} =
					 %% 从消息存储服务器进程读取消息内容
					 read_msg(MsgStatus, State),
				 MsgStatus1 = MsgStatus#msg_status{msg = Msg},
				 stats(ready0, {MsgStatus, MsgStatus1},
					   %% 将要插入的消息组装成alpha类型的消息，然后将该消息插入到Q4队列的头部
					   State1 #vqstate { q4 = ?QUEUE:in_r(MsgStatus1, Q4a) })
	end;

in_r(MsgStatus, State = #vqstate { q4 = Q4 }) ->
	State #vqstate { q4 = ?QUEUE:in_r(MsgStatus, Q4) }.


%% 从队列中获取消息
queue_out(State = #vqstate { q4 = Q4 }) ->
	%% 首先尝试从Q4队列中取得元素(Q4队列中的消息类型为alpha)
	case ?QUEUE:out(Q4) of
		{empty, _Q4} ->
			%% 如果Q4队列为空则从Q3队列中取得元素(如果Q3也为空，则直接返回空)
			case fetch_from_q3(State) of
				{empty, _State1} = Result     -> Result;
				{loaded, {MsgStatus, State1}} -> {{value, MsgStatus}, State1}
			end;
		{{value, MsgStatus}, Q4a} ->
			{{value, MsgStatus}, State #vqstate { q4 = Q4a }}
	end.


%% 读取消息(如果该消息结构里面没有消息内容则需要从消息存储服务器进程中读取)
read_msg(#msg_status{msg           = undefined,
					 msg_id        = MsgId,
					 is_persistent = IsPersistent}, State) ->
	%% 如果消息内容没有存在，则需要向消息存储服务器进程去取得消息内容
	read_msg(MsgId, IsPersistent, State);
read_msg(#msg_status{msg = Msg}, State) ->
	{Msg, State}.


%% 从消息存储服务器进程中读取
read_msg(MsgId, IsPersistent, State = #vqstate{msg_store_clients = MSCState,
											   disk_read_count   = Count}) ->
	{{ok, Msg = #basic_message {}}, MSCState1} =
		%% 从消息存储服务器进程读取消息
		msg_store_read(MSCState, IsPersistent, MsgId),
	{Msg, State #vqstate {msg_store_clients = MSCState1,
						  disk_read_count   = Count + 1}}.


%% 更新队列状态数据结构
stats(Signs, Statuses, State) ->
	stats0(expand_signs(Signs), expand_statuses(Statuses), State).


expand_signs(ready0)   -> {0, 0, true};
expand_signs({A, B})   -> {A, B, false}.


expand_statuses({none, A})    -> {false,         msg_in_ram(A), A};
expand_statuses({B,    none}) -> {msg_in_ram(B), false,         B};
expand_statuses({B,    A})    -> {msg_in_ram(B), msg_in_ram(A), B}.

%% In this function at least, we are religious: the variable name
%% contains "Ready" or "Unacked" iff that is what it counts. If
%% neither is present it counts both.
%% 更新队列数据结构中的数据
%% DeltaUnacked为1表示该消息已经被消费者取走，只是等待消费者的ack确认，因此该消息是不会计算在队列中
stats0({DeltaReady, DeltaUnacked, ReadyMsgPaged},
	   {InRamBefore, InRamAfter, MsgStatus},
	   State = #vqstate{len              = ReadyCount,
						bytes            = ReadyBytes,
						ram_msg_count    = RamReadyCount,
						persistent_count = PersistentCount,
						unacked_bytes    = UnackedBytes,
						ram_bytes        = RamBytes,
						persistent_bytes = PersistentBytes}) ->
	%% 拿到消息的大小
	S = msg_size(MsgStatus),
	DeltaTotal = DeltaReady + DeltaUnacked,
	DeltaRam = case {InRamBefore, InRamAfter} of
				   {false, false} ->  0;
				   {false, true}  ->  1;
				   {true,  false} -> -1;
				   {true,  true}  ->  0
			   end,
	DeltaRamReady = case DeltaReady of
						1                    -> one_if(InRamAfter);
						-1                   -> -one_if(InRamBefore);
						0 when ReadyMsgPaged -> DeltaRam;
						0                    -> 0
					end,
	DeltaPersistent = DeltaTotal * one_if(MsgStatus#msg_status.is_persistent),
	State#vqstate{%% 当前队列的长度，即当前队列中消息的数量
				  len               = ReadyCount      + DeltaReady,
				  %% 该队列中，当前消息在内存中的数量
				  ram_msg_count     = RamReadyCount   + DeltaRamReady,
				  %% 当前队列消息持久化的个数
				  persistent_count  = PersistentCount + DeltaPersistent,
				  %% 当前队列中消息的总大小
				  bytes             = ReadyBytes      + DeltaReady       * S,
				  %% 当前队列中消息还未ack的总的大小
				  unacked_bytes     = UnackedBytes    + DeltaUnacked     * S,
				  %% 在内存中该队列保存的消息内容的大小(包括消息内容在内存中的等待ack和没有等待ack的消息总大小)
				  ram_bytes         = RamBytes        + DeltaRam         * S,
				  %% 当前队列中持久化消息总的大小
				  persistent_bytes  = PersistentBytes + DeltaPersistent  * S}.


%% 拿到消息的大小
msg_size(#msg_status{msg_props = #message_properties{size = Size}}) -> Size.


%% 判断消息实体是否在内存中
msg_in_ram(#msg_status{msg = Msg}) -> Msg =/= undefined.


%% 删除消息(包括从存储服务器进程中和消息队列索引中删除消息)
remove(AckRequired, MsgStatus = #msg_status {
											 seq_id        = SeqId,
											 msg_id        = MsgId,
											 is_persistent = IsPersistent,
											 is_delivered  = IsDelivered,
											 msg_in_store  = MsgInStore,
											 index_on_disk = IndexOnDisk },
	   State = #vqstate {out_counter       = OutCount,
						 index_state       = IndexState,
						 msg_store_clients = MSCState}) ->
	%% 1. Mark it delivered if necessary
	%% 1. 如果该消息已经传递给消费者，则需要通知队列索引给该消息做一个标记
	IndexState1 = maybe_write_delivered(
					IndexOnDisk andalso not IsDelivered,
					SeqId, IndexState),
	
	%% 2. Remove from msg_store and queue index, if necessary
	%% 2. 如果必要的话，则将该消息从消息队列索引和消息存储服务器进程中删除消息
	%% 该函数是删除消息存储进程中的消息体
	Rem = fun () ->
				   ok = msg_store_remove(MSCState, IsPersistent, [MsgId])
		  end,
	%% 该函数是向消息队列索引中ack消息
	Ack = fun () -> rabbit_queue_index:ack([SeqId], IndexState1) end,
	%% 根据不同的状态进行不同的删除动作(如果消息需要进行消费者进行ack，则消息索引和消息存储服务器进程都先保留该消息)
	IndexState2 = case {AckRequired, MsgInStore, IndexOnDisk} of
					  %% 如果消息不需要消费者的ack，如果消息不存在消息索引磁盘文件中，但是消息存储在消息存储服务器进程中，则将该消息从消息存储服务器进程中删除掉
					  {false, true,  false} -> Rem(), IndexState1;
					  %% 如果消息不需要消费者的ack，如果消息存储在消息索引磁盘文件中，同时消息存储在消息存储服务器进程中，则将该消息从消息存储服务器进程中删除掉，同时自动直接进行ack操作
					  {false, true,   true} -> Rem(), Ack();
					  %% 如果消息不需要消费者的ack，如果消息存储在消息索引磁盘文件中，消息没有存储在消息存储服务器进程中，则自动只对该消息进行ack操作
					  {false, false,  true} -> Ack();
					  _                     -> IndexState1
				  end,
	
	%% 3. If an ack is required, add something sensible to PA
	%% 3. 根据消息存储不在同的地方，将等待ack的消息插入不同的二叉树
	{AckTag, State1} = case AckRequired of
						   true  -> %% 记录消息等待ack
							   StateN = record_pending_ack(
										  MsgStatus #msg_status {
																 is_delivered = true }, State),
							   {SeqId, StateN};
						   false -> {undefined, State}
					   end,
	State2       = case AckRequired of
					   false -> stats({-1, 0}, {MsgStatus, none},     State1);
					   true  -> stats({-1, 1}, {MsgStatus, MsgStatus}, State1)
				   end,
	%% 判断是否需要更新速率
	{AckTag, maybe_update_rates(
	   State2 #vqstate {out_counter = OutCount + 1,
						index_state = IndexState2})}.


%% 清除beta(消息内容只在磁盘上，消息索引只在内存中)和delta(消息的内容和索引都在磁盘上)类型的消息
purge_betas_and_deltas(State = #vqstate { q3 = Q3 }) ->
	case ?QUEUE:is_empty(Q3) of
		true  -> State;
		false -> %% 删除Q3队列中的所有消息
				 State1 = remove_queue_entries(Q3, State),
				 %% 将队列索引中的消息读入队列Q3，继续删除这些消息，知道从队列索引中读取不到消息数据
				 %% 先将deltas类型的消息转化为betas类型的消息存储到Q3队列，然后将该Q3消息删除掉
				 %% 如果磁盘中已经没有消息，则会将Q2队列中的消息放入Q3队列
				 purge_betas_and_deltas(maybe_deltas_to_betas(
										  State1#vqstate{q3 = ?QUEUE:new()}))
	end.


%% 删除Q队列中的消息
remove_queue_entries(Q, State = #vqstate{index_state       = IndexState,
										 msg_store_clients = MSCState}) ->
	{MsgIdsByStore, Delivers, Acks, State1} =
		?QUEUE:foldl(fun remove_queue_entries1/2,
					 {orddict:new(), [], [], State}, Q),
	%% 将得到的该队列中所有的持久化消息从消息存储服务器进程中删除掉(IsPersistent是决定从哪个消息存储服务器进程进行操作)
	ok = orddict:fold(fun (IsPersistent, MsgIds, ok) ->
							   %% 从消息存储服务器进程删除消息
							   msg_store_remove(MSCState, IsPersistent, MsgIds)
					  end, ok, MsgIdsByStore),
	%% 将需要传递和ack的消息从队列索引中去掉
	IndexState1 = rabbit_queue_index:ack(
					Acks, rabbit_queue_index:deliver(Delivers, IndexState)),
	%% 得到最新的消息队列索引状态
	State1#vqstate{index_state = IndexState1}.


%% 根据消息中的状态，得到是否需要从磁盘中删除掉，是否需要传递，ack确认消息，同时得到最新的队列信息数据结构
remove_queue_entries1(
  #msg_status { msg_id = MsgId, seq_id = SeqId, is_delivered = IsDelivered,
				msg_in_store = MsgInStore, index_on_disk = IndexOnDisk,
				is_persistent = IsPersistent} = MsgStatus,
  {MsgIdsByStore, Delivers, Acks, State}) ->
	{%% 如果该消息是持久化消息，则将该消息加入字典，然后将该消息从消息存储服务器进程中删除消息
	 case MsgInStore of
		 true  -> rabbit_misc:orddict_cons(IsPersistent, MsgId, MsgIdsByStore);
		 false -> MsgIdsByStore
	 end,
	 %% 如果该消息在队列索引中，同时该消息没有被传递，则将该消息放入需要被传递的列表中
	 cons_if(IndexOnDisk andalso not IsDelivered, SeqId, Delivers),
	 %% 如果该消息在队列索引中，同时该消息没有被ack，则将该消息放入需要被ack的列表中
	 cons_if(IndexOnDisk, SeqId, Acks),
	 %% 将队列中的消息数量减一，同时更新队列中内存消息的数量，大小等信息
	 stats({-1, 0}, {MsgStatus, none}, State)}.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------
%% 如果队列和消息都是持久化类型或者强制性存磁盘，则将新到的消息实体存入磁盘(且该消息没有持久化过)
maybe_write_msg_to_disk(_Force, MsgStatus = #msg_status {
														 msg_in_store = true }, State) ->
	{MsgStatus, State};

%% Force:是否强制将消息写入磁盘，只有强制将消息写入磁盘或者该消息需要持久化才会将消息写入磁盘
maybe_write_msg_to_disk(Force, MsgStatus = #msg_status {
														msg = Msg, msg_id = MsgId,
														is_persistent = IsPersistent },
						State = #vqstate{ msg_store_clients = MSCState,
										  disk_write_count  = Count})
  %% Force是用来内存不足的时候强制将消息存储到磁盘
  when Force orelse IsPersistent ->
	case persist_to(MsgStatus) of
		msg_store   -> ok = msg_store_write(MSCState, IsPersistent, MsgId,
											%% 在存储在磁盘中的提前准备操作(清除内容结构中的二进制特性结构数据，只保留已经解析过的特性结构)
											prepare_to_store(Msg)),
					   %% 得到最新的msg_status数据结构，和最新的队列状态数据结构
					   %% 更新消息体存储到磁盘的字段msg_in_store为true
					   %% 更新写入磁盘的数量disk_write_count加一
					   {MsgStatus#msg_status{msg_in_store = true},
											State#vqstate{disk_write_count = Count + 1}};
		queue_index -> {MsgStatus, State}
	end;

maybe_write_msg_to_disk(_Force, MsgStatus, State) ->
	{MsgStatus, State}.


%% 将消息在队列中的索引写入磁盘(如果消息是持久化类型)
maybe_write_index_to_disk(_Force, MsgStatus = #msg_status {
														   index_on_disk = true }, State) ->
	{MsgStatus, State};

%% Force:是否强制将消息写入磁盘，出现这种情况是系统内存不足，需要将消息存储到磁盘中保存
maybe_write_index_to_disk(Force, MsgStatus = #msg_status {
														  msg           = Msg,
														  msg_id        = MsgId,
														  seq_id        = SeqId,
														  is_persistent = IsPersistent,
														  is_delivered  = IsDelivered,
														  msg_props     = MsgProps},
						  State = #vqstate{target_ram_count = TargetRamCount,
										   disk_write_count = DiskWriteCount,
										   index_state      = IndexState})
  %% Force是用来内存不足的时候强制将消息存储到磁盘
  when Force orelse IsPersistent ->
	{MsgOrId, DiskWriteCount1} =
		case persist_to(MsgStatus) of
			msg_store   -> {MsgId, DiskWriteCount};
			%% 在存储在磁盘中的提前准备操作(清除内容结构中的二进制特性结构数据，只保留已经解析过的特性结构)
			queue_index -> {prepare_to_store(Msg), DiskWriteCount + 1}
		end,
	%% 将该消息和索引发布到rabbit_queue_index模块去做相关的处理
	IndexState1 = rabbit_queue_index:publish(
					MsgOrId, SeqId, MsgProps, IsPersistent, TargetRamCount,
					IndexState),
	%% 如果该消息已经被传送到了消费者，则通知消息索引该消息已经发送给消费者
	IndexState2 = maybe_write_delivered(IsDelivered, SeqId, IndexState1),
	%% 得到最新的消息状态数据结构，更新了新的索引信息和写入磁盘的数量
	%% 更新消息索引在磁盘中的字段index_on_disk为true
	%% 更新写入磁盘的条数disk_write_count加一
	{MsgStatus#msg_status{index_on_disk = true},
						 State#vqstate{index_state      = IndexState2,
									   disk_write_count = DiskWriteCount1}};

maybe_write_index_to_disk(_Force, MsgStatus, State) ->
	{MsgStatus, State}.


%% 如果队列和消息都是持久化类型，则将消息实体和消息在队列中的索引写入磁盘
%% ForceMsg如果为true，表示不管消息是否为持久化都会将消息内容写入磁盘文件
%% ForceIndex如果为true，表示不管小时是否为持久化都会将消息写入队列索引对应的磁盘文件中
maybe_write_to_disk(ForceMsg, ForceIndex, MsgStatus, State) ->
	%% 将消息实体写入磁盘(如果消息是持久化类型)
	{MsgStatus1, State1} = maybe_write_msg_to_disk(ForceMsg, MsgStatus, State),
	%% 将消息在队列中的索引写入磁盘(如果消息是持久化类型)
	maybe_write_index_to_disk(ForceIndex, MsgStatus1, State1).


%% 决定消息实体的存储位置，如果当前消息实体的实际大小大于queue_index_embed_msgs_below配置的字段，则存储到消息存储进程，否则跟消息索引存储在一起
determine_persist_to(#basic_message{
									content = #content{properties     = Props,
													   properties_bin = PropsBin}},
					 #message_properties{size = BodySize}) ->
	%% 得到queue_index_embed_msgs_below对应配置的值，该值表示消息的内容长度超过该值则将消息和索引单独分开存储，小于该值则将消息体和消息在队列中的索引存储在一起
	{ok, IndexMaxSize} = application:get_env(
						   rabbit, queue_index_embed_msgs_below),
	%% The >= is so that you can set the env to 0 and never persist
	%% to the index.
	%%
	%% We want this to be fast, so we avoid size(term_to_binary())
	%% here, or using the term size estimation from truncate.erl, both
	%% of which are too slow. So instead, if the message body size
	%% goes over the limit then we avoid any other checks.
	%%
	%% If it doesn't we need to decide if the properties will push
	%% it past the limit. If we have the encoded properties (usual
	%% case) we can just check their size. If we don't (message came
	%% via the direct client), we make a guess based on the number of
	%% headers.
	%% 如果消息体的长度超过配置上限则将消息和消息索引分开存储
	case BodySize >= IndexMaxSize of
		true  -> msg_store;
		false -> Est = case is_binary(PropsBin) of
						   true  -> BodySize + size(PropsBin);
						   false -> #'P_basic'{headers = Hs} = Props,
									case Hs of
										undefined -> 0;
										_         -> length(Hs)
									end * ?HEADER_GUESS_SIZE + BodySize
					   end,
				 case Est >= IndexMaxSize of
					 true  -> msg_store;
					 false -> queue_index
				 end
	end.


persist_to(#msg_status{persist_to = To}) -> To.


%% 在存储在磁盘中的提前准备操作(清除内容结构中的二进制特性结构数据，只保留已经解析过的特性结构)
prepare_to_store(Msg) ->
	Msg#basic_message{
					  %% don't persist any recoverable(可重获的) decoded properties
					  %% 清除内容结构中的二进制特性结构数据，只保留已经解析过的特性结构
					  content = rabbit_binary_parser:clear_decoded_content(
								  Msg #basic_message.content)}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks(队列中消息ack相关接口)
%%----------------------------------------------------------------------------
%% 记录该消息到等待ack的二叉树里
record_pending_ack(#msg_status { seq_id = SeqId } = MsgStatus,
				   State = #vqstate { ram_pending_ack  = RPA,
									  disk_pending_ack = DPA,
									  qi_pending_ack   = QPA,
									  ack_in_counter   = AckInCount}) ->
	Insert = fun (Tree) -> gb_trees:insert(SeqId, MsgStatus, Tree) end,
	%% 根据消息存储不在同的地方，将等待ack的消息插入不同的二叉树
	{RPA1, DPA1, QPA1} =
		case {msg_in_ram(MsgStatus), persist_to(MsgStatus)} of
			%% 如果不在内存中，则将等待ack的消息直接写入disk_pending_ack字段
			{false, _}           -> {RPA, Insert(DPA), QPA};
			%% 如果消息内容是写入队列索引，则将等待ack的消息直接入qi_pending_ack字段
			{_,     queue_index} -> {RPA, DPA, Insert(QPA)};
			%% 如果消息内容是写入磁盘文件，则将等待ack的消息直接写入ram_pending_ack字段
			{_,     msg_store}   -> {Insert(RPA), DPA, QPA}
		end,
	%% 更新三个等待ack二叉树，同时将进入队列的等待ack的消息数量加一
	State #vqstate { ram_pending_ack  = RPA1,
					 disk_pending_ack = DPA1,
					 qi_pending_ack   = QPA1,
					 %% 等待ack的消息数量加一
					 ack_in_counter   = AckInCount + 1}.


%% 根据SeqId查找等待确认的消息
lookup_pending_ack(SeqId, #vqstate { ram_pending_ack  = RPA,
									 disk_pending_ack = DPA,
									 qi_pending_ack   = QPA}) ->
	case gb_trees:lookup(SeqId, RPA) of
		{value, V} -> V;
		none       -> case gb_trees:lookup(SeqId, DPA) of
						  {value, V} -> V;
						  none       -> gb_trees:get(SeqId, QPA)
					  end
	end.


%% 根据SeqID删除等待ack二叉树中的数据
%% First parameter = UpdateStats
remove_pending_ack(true, SeqId, State) ->
	{MsgStatus, State1} = remove_pending_ack(false, SeqId, State),
	%% 更新队列数据结构中保存的相关信息
	{MsgStatus, stats({0, -1}, {MsgStatus, none}, State1)};

remove_pending_ack(false, SeqId, State = #vqstate{ram_pending_ack  = RPA,
												  disk_pending_ack = DPA,
												  qi_pending_ack   = QPA}) ->
	case gb_trees:lookup(SeqId, RPA) of
		{value, V} -> RPA1 = gb_trees:delete(SeqId, RPA),
					  {V, State #vqstate { ram_pending_ack = RPA1 }};
		none       -> case gb_trees:lookup(SeqId, DPA) of
						  {value, V} ->
							  DPA1 = gb_trees:delete(SeqId, DPA),
							  {V, State#vqstate{disk_pending_ack = DPA1}};
						  none ->
							  QPA1 = gb_trees:delete(SeqId, QPA),
							  {gb_trees:get(SeqId, QPA),
							   State#vqstate{qi_pending_ack = QPA1}}
					  end
	end.


%% 清除掉当前队列中等待ack的消息，如果KeepPersistent为true，表示只将等待ack但是非持久化的消息从消息存储进程中删除，
%% 如果KeepPersistent为false，则将当前所有等待ack的消息从消息索引和消息存储进程中删除掉
purge_pending_ack(KeepPersistent,
				  State = #vqstate { ram_pending_ack   = RPA,
									 disk_pending_ack  = DPA,
									 qi_pending_ack    = QPA,
									 index_state       = IndexState,
									 msg_store_clients = MSCState }) ->
	F = fun (_SeqId, MsgStatus, Acc) -> accumulate_ack(MsgStatus, Acc) end,
	%% 拿到消息在队列索引中的ID，以及区分出msg_in_store为true，但是IsPersistent为false的消息ID列表，以及IsPersistent为true的消息ID列表
	{IndexOnDiskSeqIds, MsgIdsByStore, _AllMsgIds} =
		rabbit_misc:gb_trees_fold(
		F, rabbit_misc:gb_trees_fold(
		F, rabbit_misc:gb_trees_fold(
		F, accumulate_ack_init(), RPA), DPA), QPA),
	%% 将内存等待ack的消息字段，磁盘等待ack的消息字段，队列索引等待ack的消息字段重置为初始化状态
	State1 = State #vqstate { ram_pending_ack  = gb_trees:empty(),
							  disk_pending_ack = gb_trees:empty(),
							  qi_pending_ack   = gb_trees:empty()},
	
	%% KeepPersistent如果为true，表示会保存持久化消息，则将非持久化的消息内容从消息存储服务器进程中删除掉该消息
	%% KeepPersistent为false，则表示将所有等待ack的消息全部删除掉
	case KeepPersistent of
		true  -> case orddict:find(false, MsgIdsByStore) of
					 error        -> State1;
					 %% 将不是持久化的消息但是该消息已经存储到了消息服务器中的磁盘文件中，则将这些消息全部从消息存储服务器进程中删除掉
					 {ok, MsgIds} -> %% 从消息存储服务器进程删除消息
						 			 ok = msg_store_remove(MSCState, false,
														   MsgIds),
									 State1
				 end;
		false -> %% 将当前消息队列中所有等待ack的消息同时存储在消息索引磁盘文件中的消息进行ack操作，用来将该消息队列中在消息索引中的消息全部删除掉
				 IndexState1 =
					 rabbit_queue_index:ack(IndexOnDiskSeqIds, IndexState),
				 %% 将该消息队列中等待ack的消息同时存储在磁盘中的消息全部删除掉
				 [ok = msg_store_remove(MSCState, IsPersistent, MsgIds)
						 || {IsPersistent, MsgIds} <- orddict:to_list(MsgIdsByStore)],
				 State1 #vqstate { index_state = IndexState1 }
	end.


%% 累计ack初始化函数
accumulate_ack_init() -> {[], orddict:new(), []}.


%% 拿到消息在队列索引中的ID，以及区分出msg_in_store为true，但是IsPersistent为false的消息ID列表，以及IsPersistent为true的消息ID列表
accumulate_ack(#msg_status { seq_id        = SeqId,
							 msg_id        = MsgId,
							 is_persistent = IsPersistent,
							 msg_in_store  = MsgInStore,
							 index_on_disk = IndexOnDisk },
			   {IndexOnDiskSeqIdsAcc, MsgIdsByStore, AllMsgIds}) ->
	{cons_if(IndexOnDisk, SeqId, IndexOnDiskSeqIdsAcc),
	 case MsgInStore of
		 true  -> rabbit_misc:orddict_cons(IsPersistent, MsgId, MsgIdsByStore);
		 false -> MsgIdsByStore
	 end,
	 [MsgId | AllMsgIds]}.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------
%% backing_queue进行消息confirm的实际操作
record_confirms(MsgIdSet, State = #vqstate { msgs_on_disk        = MOD,
											 msg_indices_on_disk = MIOD,
											 unconfirmed         = UC,
											 confirmed           = C }) ->
	State #vqstate {
					%% 将confirm的消息ID从msgs_on_disk字段中删除掉
					msgs_on_disk        = rabbit_misc:gb_sets_difference(MOD,  MsgIdSet),
					%% 将confirm的消息ID从msg_indices_on_disk字段中删除掉
					msg_indices_on_disk = rabbit_misc:gb_sets_difference(MIOD, MsgIdSet),
					%% 将confirm的消息ID从unconfirmed字段中删除掉
					unconfirmed         = rabbit_misc:gb_sets_difference(UC,   MsgIdSet),
					%% 将已经confirm的消息更新到confirmed字段
					confirmed           = gb_sets:union(C, MsgIdSet) }.


%% 消息存储服务器进程通知队列进程已经消息存储到磁盘，消息存储服务器进程向队列进程进行confirm操作
%% ignored：表示该消息已经得到ack，而到现在才进行confirm操作
msgs_written_to_disk(Callback, MsgIdSet, ignored) ->
	Callback(?MODULE,
			 fun (?MODULE, State) -> record_confirms(MsgIdSet, State) end);

msgs_written_to_disk(Callback, MsgIdSet, written) ->
	Callback(?MODULE,
			 fun (?MODULE, State = #vqstate { msgs_on_disk        = MOD,
											  msg_indices_on_disk = MIOD,
											  unconfirmed         = UC }) ->
					  %% 跟队列中没有confirm的消息ID求交集得到可以confirm的消息ID列表
					  Confirmed = gb_sets:intersection(UC, MsgIdSet),
					  %% 消息内容和消息索引分开存储的时候，则需要消息存储服务器进程和消息队列索引同时confirm，才能确认该消息可以confirm
					  record_confirms(gb_sets:intersection(MsgIdSet, MIOD),
									  State #vqstate {
													  %% 将已经confirm的消息更新到msgs_on_disk字段
													  msgs_on_disk =
														  gb_sets:union(MOD, Confirmed) })
			 end).


%% 队列索引模块将消息索引存入到磁盘后回调通知队列进程索引可以进行confirm操作(该回调的消息内容存储在消息存储服务器进程，没有跟消息索引存储在一起)
msg_indices_written_to_disk(Callback, MsgIdSet) ->
	Callback(?MODULE,
			 fun (?MODULE, State = #vqstate { msgs_on_disk        = MOD,
											  msg_indices_on_disk = MIOD,
											  unconfirmed         = UC }) ->
					  %% 跟队列中没有confirm的消息ID求交集得到正在confirm的消息ID列表
					  Confirmed = gb_sets:intersection(UC, MsgIdSet),
					  %% 消息内容和消息索引分开存储的时候，则需要消息存储服务器进程和消息队列索引同时confirm，才能确认该消息可以confirm
					  record_confirms(gb_sets:intersection(MsgIdSet, MOD),
									  State #vqstate {
													  %% 将已经confirm的消息更新到msg_indices_on_disk字段
													  msg_indices_on_disk =
														  gb_sets:union(MIOD, Confirmed) })
			 end).


%% 队列索引模块将消息索引存入到磁盘后回调通知队列进程可以进行confirm操作(该回调的消息内容同消息索引是存储在一起的，没有存储到消息存储服务器进程中)
msgs_and_indices_written_to_disk(Callback, MsgIdSet) ->
	Callback(?MODULE,
			 fun (?MODULE, State) -> record_confirms(MsgIdSet, State) end).

%%----------------------------------------------------------------------------
%% Internal plumbing for requeue
%%----------------------------------------------------------------------------
%% 将消息的消息体从消息存储服务器进程中读取出来(将消息转化为alpha类型)
publish_alpha(#msg_status { msg = undefined } = MsgStatus, State) ->
	%% 将消息内容从磁盘文件中读取出来
	{Msg, State1} = read_msg(MsgStatus, State),
	MsgStatus1 = MsgStatus#msg_status { msg = Msg },
	{MsgStatus1, stats({1, -1}, {MsgStatus, MsgStatus1}, State1)};
publish_alpha(MsgStatus, State) ->
	{MsgStatus, stats({1, -1}, {MsgStatus, MsgStatus}, State)}.


%% 将消息转化为beta类型
publish_beta(MsgStatus, State) ->
	%% 强制将消息内容写入磁盘
	{MsgStatus1, State1} = maybe_write_to_disk(true, false, MsgStatus, State),
	%% 将内存中msg_status中的消息内容字段msg置空，即将消息从内存中清掉
	MsgStatus2 = m(trim_msg_status(MsgStatus1)),
	{MsgStatus2, stats({1, -1}, {MsgStatus, MsgStatus2}, State1)}.


%% Rebuild queue, inserting sequence ids to maintain ordering
%% 将队列中的消息和MsgIds列表中的消息进行合并
queue_merge(SeqIds, Q, MsgIds, Limit, PubFun, State) ->
	queue_merge(SeqIds, Q, ?QUEUE:new(), MsgIds,
				Limit, PubFun, State).


queue_merge([SeqId | Rest] = SeqIds, Q, Front, MsgIds,
			Limit, PubFun, State)
  %% 当只有Q队列中没有消息或者当前要合并的消息小于给定的Limit SeqId才能进行合并
  when Limit == undefined orelse SeqId < Limit ->
	case ?QUEUE:out(Q) of
		{{value, #msg_status { seq_id = SeqIdQ } = MsgStatus}, Q1}
		  when SeqIdQ < SeqId ->
			%% enqueue from the remaining queue
			%% 当当前SeqId大于后面的SeqIdQ，即SeqId在两者之间的话，就表示可以插入，则将该SeqId插入队列
			queue_merge(SeqIds, Q1, ?QUEUE:in(MsgStatus, Front), MsgIds,
						Limit, PubFun, State);
		{_, _Q1} ->
			%% enqueue from the remaining list of sequence ids
			%% 从等待ack的数据结构中将消息拿出来
			{MsgStatus, State1} = msg_from_pending_ack(SeqId, State),
			{#msg_status { msg_id = MsgId } = MsgStatus1, State2} =
				PubFun(MsgStatus, State1),
			queue_merge(Rest, Q, ?QUEUE:in(MsgStatus1, Front), [MsgId | MsgIds],
						Limit, PubFun, State2)
	end;

queue_merge(SeqIds, Q, Front, MsgIds,
			_Limit, _PubFun, State) ->
	{SeqIds, ?QUEUE:join(Front, Q), MsgIds, State}.


%% 将等待ack的消息重新放入到磁盘中
delta_merge([], Delta, MsgIds, State) ->
	{Delta, MsgIds, State};
delta_merge(SeqIds, Delta, MsgIds, State) ->
	lists:foldl(fun (SeqId, {Delta0, MsgIds0, State0}) ->
						 %% 从等待ack的数据结构中将消息拿出来
						 {#msg_status { msg_id = MsgId } = MsgStatus, State1} =
							 msg_from_pending_ack(SeqId, State0),
						 %% 将消息重新写入磁盘中
						 {_MsgStatus, State2} =
							 maybe_write_to_disk(true, true, MsgStatus, State1),
						 {expand_delta(SeqId, Delta0), [MsgId | MsgIds0],
						  stats({1, -1}, {MsgStatus, none}, State2)}
				end, {Delta, MsgIds, State}, SeqIds).


%% Mostly opposite of record_pending_ack/2
%% 从等待ack的数据结构中将消息拿出来
msg_from_pending_ack(SeqId, State) ->
	%% 将SeqId从等待ack的二叉树结构中删除
	{#msg_status { msg_props = MsgProps } = MsgStatus, State1} =
		remove_pending_ack(false, SeqId, State),
	{MsgStatus #msg_status {
							msg_props = MsgProps #message_properties { needs_confirming = false } },
						   State1}.


%% 拿到队列Q的最小队列索引
beta_limit(Q) ->
	case ?QUEUE:peek(Q) of
		{value, #msg_status { seq_id = SeqId }} -> SeqId;
		empty                                   -> undefined
	end.


%% 拿到索引磁盘中的最小队列索引
delta_limit(?BLANK_DELTA_PATTERN(_X))             -> undefined;
delta_limit(#delta { start_seq_id = StartSeqId }) -> StartSeqId.

%%----------------------------------------------------------------------------
%% Iterator
%%----------------------------------------------------------------------------

ram_ack_iterator(State) ->
	{ack, gb_trees:iterator(State#vqstate.ram_pending_ack)}.


disk_ack_iterator(State) ->
	{ack, gb_trees:iterator(State#vqstate.disk_pending_ack)}.


qi_ack_iterator(State) ->
	{ack, gb_trees:iterator(State#vqstate.qi_pending_ack)}.


msg_iterator(State) -> istate(start, State).


istate(start, State) -> {q4,    State#vqstate.q4,    State};
istate(q4,    State) -> {q3,    State#vqstate.q3,    State};
istate(q3,    State) -> {delta, State#vqstate.delta, State};
istate(delta, State) -> {q2,    State#vqstate.q2,    State};
istate(q2,    State) -> {q1,    State#vqstate.q1,    State};
istate(q1,   _State) -> done.


next({ack, It}, IndexState) ->
	case gb_trees:next(It) of
		none                     -> {empty, IndexState};
		{_SeqId, MsgStatus, It1} -> Next = {ack, It1},
									{value, MsgStatus, true, Next, IndexState}
	end;
next(done, IndexState) -> {empty, IndexState};
next({delta, #delta{start_seq_id = SeqId,
					end_seq_id   = SeqId}, State}, IndexState) ->
	next(istate(delta, State), IndexState);
next({delta, #delta{start_seq_id = SeqId,
					end_seq_id   = SeqIdEnd} = Delta, State}, IndexState) ->
	SeqIdB = rabbit_queue_index:next_segment_boundary(SeqId),
	SeqId1 = lists:min([SeqIdB, SeqIdEnd]),
	{List, IndexState1} = rabbit_queue_index:read(SeqId, SeqId1, IndexState),
	next({delta, Delta#delta{start_seq_id = SeqId1}, List, State}, IndexState1);
next({delta, Delta, [], State}, IndexState) ->
	next({delta, Delta, State}, IndexState);
next({delta, Delta, [{_, SeqId, _, _, _} = M | Rest], State}, IndexState) ->
	case (gb_trees:is_defined(SeqId, State#vqstate.ram_pending_ack) orelse
			  gb_trees:is_defined(SeqId, State#vqstate.disk_pending_ack) orelse
			  gb_trees:is_defined(SeqId, State#vqstate.qi_pending_ack)) of
		false -> Next = {delta, Delta, Rest, State},
				 {value, beta_msg_status(M), false, Next, IndexState};
		true  -> next({delta, Delta, Rest, State}, IndexState)
	end;
next({Key, Q, State}, IndexState) ->
	case ?QUEUE:out(Q) of
		{empty, _Q}              -> next(istate(Key, State), IndexState);
		{{value, MsgStatus}, QN} -> Next = {Key, QN, State},
									{value, MsgStatus, false, Next, IndexState}
	end.


inext(It, {Its, IndexState}) ->
	case next(It, IndexState) of
		{empty, IndexState1} ->
			{Its, IndexState1};
		{value, MsgStatus1, Unacked, It1, IndexState1} ->
			{[{MsgStatus1, Unacked, It1} | Its], IndexState1}
	end.


ifold(_Fun, Acc, [], State) ->
	{Acc, State};
ifold(Fun, Acc, Its, State) ->
	[{MsgStatus, Unacked, It} | Rest] =
		lists:sort(fun ({#msg_status{seq_id = SeqId1}, _, _},
						{#msg_status{seq_id = SeqId2}, _, _}) ->
							SeqId1 =< SeqId2
				   end, Its),
	{Msg, State1} = read_msg(MsgStatus, State),
	case Fun(Msg, MsgStatus#msg_status.msg_props, Unacked, Acc) of
		{stop, Acc1} ->
			{Acc1, State};
		{cont, Acc1} ->
			{Its1, IndexState1} = inext(It, {Rest, State1#vqstate.index_state}),
			ifold(Fun, Acc1, Its1, State1#vqstate{index_state = IndexState1})
	end.

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------
%% RabbitMQ系统中使用的内存过多，此操作是将内存中的队列数据写入到磁盘中
reduce_memory_use(State = #vqstate { target_ram_count = infinity }) ->
	State;

reduce_memory_use(State = #vqstate {
									ram_pending_ack  = RPA,
									ram_msg_count    = RamMsgCount,
									target_ram_count = TargetRamCount,
									rates            = #rates { in      = AvgIngress,
																out     = AvgEgress,
																ack_in  = AvgAckIngress,
																ack_out = AvgAckEgress } }) ->
	State1 = #vqstate { q2 = Q2, q3 = Q3 } =
						  %% 得到当前在内存中的数量超过允许在内存中的最大数量的个数
						  case chunk_size(RamMsgCount + gb_trees:size(RPA), TargetRamCount) of
							  0  -> State;
							  %% Reduce memory of pending acks and alphas. The order is
							  %% determined based on which is growing faster. Whichever
							  %% comes second may very well get a quota of 0 if the
							  %% first manages to push out the max number of messages.
							  S1 -> Funs = case ((AvgAckIngress - AvgAckEgress) >
													 (AvgIngress - AvgEgress)) of
											   %% ack操作进入的流量大于消息进入的流量，则优先将等待ack的消息写入磁盘文件
											   true  -> [
														 %% 限制内存中的等待ack的消息(将消息内容在内存中的等待ack的消息的消息内容写入磁盘文件)
														 fun limit_ram_acks/2,
														 %% 将Quota个alphas类型的消息转化为betas类型的消息(Q1和Q4队列都是alphas类型的消息)
														 fun push_alphas_to_betas/2
														];
											   %% 消息进入的流量大于ack操作进入的消息流量，则优先将非等待ack的消息写入磁盘文件
											   false -> [
														 %% 将Quota个alphas类型的消息转化为betas类型的消息(Q1和Q4队列都是alphas类型的消息)
														 fun push_alphas_to_betas/2,
														 %% 限制内存中的等待ack的消息(将消息内容在内存中的等待ack的消息的消息内容写入磁盘文件)
														 fun limit_ram_acks/2
														]
										   end,
									%% 真正执行转化的函数
									{_, State2} = lists:foldl(fun (ReduceFun, {QuotaN, StateN}) ->
																	   ReduceFun(QuotaN, StateN)
															  end, {S1, State}, Funs),
									State2
						  end,
	%% 当前beta类型的消息大于允许的beta消息的最大值，则将beta类型多余的消息转化为deltas类型的消息
	case chunk_size(?QUEUE:len(Q2) + ?QUEUE:len(Q3),
					permitted_beta_count(State1)) of
		S2 when S2 >= ?IO_BATCH_SIZE ->
			%% There is an implicit(含蓄), but subtle(微妙), upper bound here. We
			%% may shuffle a lot of messages from Q2/3 into delta, but
			%% the number of these that require any disk operation,
			%% namely index writing, i.e. messages that are genuine
			%% betas and not gammas, is bounded by the credit_flow
			%% limiting of the alpha->beta conversion above.
			%% 将S2个betas类型的消息转化为deltas类型的消息
			push_betas_to_deltas(S2, State1);
		_  ->
			State1
	end.


%% 限制内存中的等待ack的消息(将消息内容在内存中的等待ack的消息的消息内容写入磁盘文件)
limit_ram_acks(0, State) ->
	{0, State};

limit_ram_acks(Quota, State = #vqstate { ram_pending_ack  = RPA,
										 disk_pending_ack = DPA }) ->
	case gb_trees:is_empty(RPA) of
		true ->
			{Quota, State};
		false ->
			%% 拿到队列索引最大的消息
			{SeqId, MsgStatus, RPA1} = gb_trees:take_largest(RPA),
			%% 内存不足，强制性的将等待ack的SeqId消息内容写入磁盘
			{MsgStatus1, State1} =
				maybe_write_to_disk(true, false, MsgStatus, State),
			%% 如果成功的将消息写入磁盘，则将内存中的消息体字段清空
			MsgStatus2 = m(trim_msg_status(MsgStatus1)),
			%% 更新存储在磁盘中等待ack的消息字段disk_pending_ack，将刚才从存储在内存中等待ack的消息字段ram_pending_ack中的SeqId存储到disk_pending_ack字段中
			DPA1 = gb_trees:insert(SeqId, MsgStatus2, DPA),
			%% 更新队列状态，同时更新最新的ram_pending_ack和disk_pending_ack字段
			limit_ram_acks(Quota - 1,
						   %% 主要是更新内存中保存的消息大小(ram_bytes减去当前写入磁盘的消息的大小)
						   stats({0, 0}, {MsgStatus, MsgStatus2},
								 State1 #vqstate { ram_pending_ack  = RPA1,
												   disk_pending_ack = DPA1 }))
	end.


%% 拿到当前队列允许的beta类型的消息上限
permitted_beta_count(#vqstate { len = 0 }) ->
	infinity;

permitted_beta_count(#vqstate { target_ram_count = 0, q3 = Q3 }) ->
	%% rabbit_queue_index:next_segment_boundary(0)拿到0对应的磁盘文件的下一个磁盘文件的第一个SeqId
	lists:min([?QUEUE:len(Q3), rabbit_queue_index:next_segment_boundary(0)]);

permitted_beta_count(#vqstate { q1               = Q1,
								q4               = Q4,
								target_ram_count = TargetRamCount,
								len              = Len }) ->
	BetaDelta = Len - ?QUEUE:len(Q1) - ?QUEUE:len(Q4),
	lists:max([rabbit_queue_index:next_segment_boundary(0),
			   BetaDelta - ((BetaDelta * BetaDelta) div
								(BetaDelta + TargetRamCount))]).


%% 得到Current超过Permitted的个数
chunk_size(Current, Permitted)
  when Permitted =:= infinity orelse Permitted >= Current ->
	0;

chunk_size(Current, Permitted) ->
	Current - Permitted.


%% 从队列Q3中读取消息
fetch_from_q3(State = #vqstate { q1    = Q1,
								 q2    = Q2,
								 delta = #delta { count = DeltaCount },
								 q3    = Q3,
								 q4    = Q4 }) ->
	%% 先从Q3队列中取元素(如果为空，则直接返回为空)
	case ?QUEUE:out(Q3) of
		{empty, _Q3} ->
			{empty, State};
		{{value, MsgStatus}, Q3a} ->
			State1 = State #vqstate { q3 = Q3a },
			State2 = case {?QUEUE:is_empty(Q3a), 0 == DeltaCount} of
						 {true, true} ->
							 %% 当这两个队列都为空时，可以确认q2也为空，也就是这时候，q2，q3，delta，q4都为空，那么，q1队列的消息可以直接转移到q4，下次获取消息时就可以直接从q4获取
							 %% q3 is now empty, it wasn't before;
							 %% delta is still empty. So q2 must be
							 %% empty, and we know q4 is empty
							 %% otherwise we wouldn't be loading from
							 %% q3. As such, we can just set q4 to Q1.
							 %% 当Q3队列为空，且磁盘中的消息数量为空，则断言Q2队列为空
							 true = ?QUEUE:is_empty(Q2), %% ASSERTION
							 %% 当Q3队列为空，且磁盘中的消息数量为空，则断言Q4队列为空
							 true = ?QUEUE:is_empty(Q4), %% ASSERTION
							 %% 从Q3队列中取走消息后发现Q3队列为空，同时磁盘中没有消息，则将Q1队列中的消息放入Q4队列
							 State1 #vqstate { q1 = ?QUEUE:new(), q4 = Q1 };
						 {true, false} ->
							 %% 从Q3队列中取走消息后发现Q3队列为空，q3空，delta非空，这时候就需要从delta队列（内容与索引都在磁盘上，通过maybe_deltas_to_betas/1调用）读取消息，并转移到q3队列
							 maybe_deltas_to_betas(State1);
						 {false, _} ->
							 %% q3非空，直接返回，下次获取消息还可以从q3获取
							 %% q3 still isn't empty, we've not
							 %% touched delta, so the invariants
							 %% between q1, q2, delta and q3 are
							 %% maintained
							 State1
					 end,
			{loaded, {MsgStatus, State2}}
	end.


%% 从磁盘中读取队列数据到内存中来(从队列消息中最小索引ID读取出一个索引磁盘文件大小的消息索引信息)
%% 从队列索引的磁盘文件将单个磁盘文件中的消息索引读取出来
%% 该操作是将单个队列索引磁盘文件中的deltas类型消息转换为beta类型的消息
maybe_deltas_to_betas(State = #vqstate { delta = ?BLANK_DELTA_PATTERN(X) }) ->
	State;

maybe_deltas_to_betas(State = #vqstate {
										q2                   = Q2,
										delta                = Delta,
										q3                   = Q3,
										index_state          = IndexState,
										ram_msg_count        = RamMsgCount,
										ram_bytes            = RamBytes,
										ram_pending_ack      = RPA,
										disk_pending_ack     = DPA,
										qi_pending_ack       = QPA,
										disk_read_count      = DiskReadCount,
										transient_threshold  = TransientThreshold }) ->
	#delta { start_seq_id = DeltaSeqId,
			 count        = DeltaCount,
			 end_seq_id   = DeltaSeqIdEnd } = Delta,
	%% 根据delta中的开始DeltaSeqId得到存在索引磁盘的最小的磁盘索引号
	DeltaSeqId1 =
		lists:min([rabbit_queue_index:next_segment_boundary(DeltaSeqId),
				   DeltaSeqIdEnd]),
	%% 从队列索引中读取消息索引(从队列索引的磁盘文件将单个磁盘文件中的消息索引读取出来)
	{List, IndexState1} = rabbit_queue_index:read(DeltaSeqId, DeltaSeqId1,
												  IndexState),
	%% 过滤掉从rabbit_queue_index中读取过来的消息队列索引(如果该消息不是持久化的则需要删除掉)，最后得到当前内存中准备好的消息个数以及内存中的消息的总的大小
	{Q3a, RamCountsInc, RamBytesInc, IndexState2} =
		%% RabbitMQ系统关闭以前非持久化消息存储到磁盘中的索引信息再从磁盘读取出来的时候必须将他们彻底从RabbitMQ系统中删除
		betas_from_index_entries(List, TransientThreshold,
								 RPA, DPA, QPA, IndexState1),
	%% 更新队列消息索引结构，内存中队列中的消息个数，队列内存中消息占的大小，以及从磁盘文件读取的次数
	State1 = State #vqstate { index_state       = IndexState2,
							  ram_msg_count     = RamMsgCount   + RamCountsInc,
							  ram_bytes         = RamBytes      + RamBytesInc,
							  disk_read_count   = DiskReadCount + RamCountsInc},
	case ?QUEUE:len(Q3a) of
		0 ->
			%% we ignored every message in the segment due to it being
			%% transient and below the threshold
			%% 如果读取的当前消息队列索引磁盘文件中的操作项为空，则继续读下一个消息索引磁盘文件中的操作项
			maybe_deltas_to_betas(
			  State1 #vqstate {
							   delta = d(Delta #delta { start_seq_id = DeltaSeqId1 })});
		Q3aLen ->
			%% 将从索引中读取出来的消息索引存储到Q3队列(将新从磁盘中读取的消息队列添加到老的Q3队列的后面)
			Q3b = ?QUEUE:join(Q3, Q3a),
			case DeltaCount - Q3aLen of
				0 ->
					%% 如果读取出来的长度和队列索引的总长度相等，则delta信息被重置为消息个数为0，同时q2中的消息转移到q3队列
					%% delta is now empty, but it wasn't before, so
					%% can now join q2 onto q3
					State1 #vqstate { q2    = ?QUEUE:new(),
									  delta = ?BLANK_DELTA,
									  %% 如果磁盘中已经没有消息，则将Q2队列中的消息放入Q3队列
									  q3    = ?QUEUE:join(Q3b, Q2) };
				N when N > 0 ->
					%% 得到最新的队列消息磁盘中的信息
					Delta1 = d(#delta { start_seq_id = DeltaSeqId1,
										count        = N,
										end_seq_id   = DeltaSeqIdEnd }),
					%% 更新最新的q3队列和磁盘信息结构
					State1 #vqstate { delta = Delta1,
									  q3    = Q3b }
			end
	end.


%% 将Quota个alphas类型的消息转化为betas类型的消息(Q1和Q4队列都是alphas类型的消息)
push_alphas_to_betas(Quota, State) ->
	%% 将Q1队列中消息转化为betas类型的消息
	%% 如果磁盘中没有消息，则将Q1中的消息存储到Q3队列，如果磁盘中有消息则将Q3队列中的消息存储到Q2队列(将Q1队列头部的元素放入到Q2或者Q3队列的尾部)
	{Quota1, State1} =
		push_alphas_to_betas(
		  fun ?QUEUE:out/1,
		  fun (MsgStatus, Q1a,
			   %% 如果delta类型的消息的个数为0，则将该消息存入存入Q3队列
			   State0 = #vqstate { q3 = Q3, delta = #delta { count = 0 } }) ->
				   State0 #vqstate { q1 = Q1a, q3 = ?QUEUE:in(MsgStatus, Q3) };
			 %% 如果delta类型的消息个数不为0，则将该消息存入Q2队列
			 (MsgStatus, Q1a, State0 = #vqstate { q2 = Q2 }) ->
				  State0 #vqstate { q1 = Q1a, q2 = ?QUEUE:in(MsgStatus, Q2) }
		  end,
		  Quota, State #vqstate.q1, State),
	%% 将Q4队列中消息转化为betas类型的消息(Q4 -> Q3)(将Q4队列尾部的元素不断的放入到Q3队列的头部)
	{Quota2, State2} =
		push_alphas_to_betas(
		  fun ?QUEUE:out_r/1,
		  fun (MsgStatus, Q4a, State0 = #vqstate { q3 = Q3 }) ->
				   State0 #vqstate { q3 = ?QUEUE:in_r(MsgStatus, Q3), q4 = Q4a }
		  end,
		  Quota1, State1 #vqstate.q4, State1),
	{Quota2, State2}.


%% 将Quota个alphas类型的消息转化为betas类型的消息(Q1和Q4队列都是alphas类型的消息)
push_alphas_to_betas(_Generator, _Consumer, Quota, _Q,
					 State = #vqstate { ram_msg_count    = RamMsgCount,
										target_ram_count = TargetRamCount })
  when Quota =:= 0 orelse
		   TargetRamCount =:= infinity orelse
		   TargetRamCount >= RamMsgCount ->
	{Quota, State};

push_alphas_to_betas(Generator, Consumer, Quota, Q, State) ->
	%% 判断当前进程是否处在阻塞状态
	case credit_flow:blocked() of
		true  -> {Quota, State};
		false -> case Generator(Q) of
					 {empty, _Q} ->
						 {Quota, State};
					 {{value, MsgStatus}, Qa} ->
						 %% 强制将消息内容写入磁盘文件(如果该消息为持久化消息则不会写磁盘文件)
						 {MsgStatus1, State1} =
							 maybe_write_to_disk(true, false, MsgStatus, State),
						 %% 将内存中msg_status中的消息体字段msg置空，即将消息从内存中清掉
						 MsgStatus2 = m(trim_msg_status(MsgStatus1)),
						 %% 将ram_msg_count字段减一，ram_bytes字段减去该消息内容的大小，ram_bytes字段减去该消息的大小
						 State2 = stats(
									ready0, {MsgStatus, MsgStatus2}, State1),
						 %% 将取得的消息存入对应的队列(将消息从Q1或者Q4队列放入到Q2或者Q3消息队列中)
						 State3 = Consumer(MsgStatus2, Qa, State2),
						 push_alphas_to_betas(Generator, Consumer, Quota - 1,
											  Qa, State3)
				 end
	end.


%% 将Quota个betas类型的消息转化为deltas类型的消息
push_betas_to_deltas(Quota, State = #vqstate { q2    = Q2,
											   delta = Delta,
											   q3    = Q3}) ->
	PushState = {Quota, Delta, State},
	%% 优先将Q3队列中的消息转化为deltas类型的消息(会将Q3队列中第一个索引磁盘中的消息保存下来不写入磁盘索引中)
	{Q3a, PushState1} = push_betas_to_deltas(
						  fun ?QUEUE:out_r/1,
						  fun rabbit_queue_index:next_segment_boundary/1,
						  Q3, PushState),
	%% 优先将Q2队列中的消息转化为deltas类型的消息(Q2队列则没有限制，可以将该队列中所有消息写入队列索引磁盘中)
	{Q2a, PushState2} = push_betas_to_deltas(
						  fun ?QUEUE:out/1,
						  fun (Q2MinSeqId) -> Q2MinSeqId end,
						  Q2, PushState1),
	{_, Delta1, State1} = PushState2,
	State1 #vqstate { q2    = Q2a,
					  delta = Delta1,
					  q3    = Q3a }.


%% 将Quota个betas类型的消息转化为deltas类型的消息
push_betas_to_deltas(Generator, LimitFun, Q, PushState) ->
	case ?QUEUE:is_empty(Q) of
		true ->
			{Q, PushState};
		false ->
			{value, #msg_status { seq_id = MinSeqId }} = ?QUEUE:peek(Q),
			{value, #msg_status { seq_id = MaxSeqId }} = ?QUEUE:peek_r(Q),
			Limit = LimitFun(MinSeqId),
			%% MaxSeqId必须大于等于MinSeqId这个索引对应的磁盘文件中最大的索引ID
			case MaxSeqId < Limit of
				%% 如果MaxSeqId小于Limit则表明Q这个队列中的所有消息是属于同一个索引磁盘文件中
				true  -> {Q, PushState};
				false -> push_betas_to_deltas1(Generator, Limit, Q, PushState)
			end
	end.


%% 将Quota个betas类型的消息转化为deltas类型的消息
push_betas_to_deltas1(_Generator, _Limit, Q, {0, _Delta, _State} = PushState) ->
	{Q, PushState};

push_betas_to_deltas1(Generator, Limit, Q, {Quota, Delta, State} = PushState) ->
	case Generator(Q) of
		{empty, _Q} ->
			{Q, PushState};
		{{value, #msg_status { seq_id = SeqId }}, _Qa}
		  when SeqId < Limit ->
			{Q, PushState};
		{{value, MsgStatus = #msg_status { seq_id = SeqId }}, Qa} ->
			%% 将消息索引信息强制写入磁盘文件
			{#msg_status { index_on_disk = true }, State1} =
				maybe_write_index_to_disk(true, MsgStatus, State),
			%% 将ram_msg_count字段减一，ram_bytes字段减去该消息内容的大小
			State2 = stats(ready0, {MsgStatus, none}, State1),
			%% 在将一个SeqId的消息写入索引文件后，扩展delta数据结构
			Delta1 = expand_delta(SeqId, Delta),
			%% 继续转换Qa队列中的消息
			push_betas_to_deltas1(Generator, Limit, Qa,
								  {Quota - 1, Delta1, State2})
	end.

%%----------------------------------------------------------------------------
%% Upgrading
%%----------------------------------------------------------------------------

multiple_routing_keys() ->
	transform_storage(
	  fun ({basic_message, ExchangeName, Routing_Key, Content,
			MsgId, Persistent}) ->
			   {ok, {basic_message, ExchangeName, [Routing_Key], Content,
					 MsgId, Persistent}};
		 (_) -> {error, corrupt_message}
	  end),
	ok.


%% Assumes message store is not running
transform_storage(TransformFun) ->
	transform_store(?PERSISTENT_MSG_STORE, TransformFun),
	transform_store(?TRANSIENT_MSG_STORE, TransformFun).


transform_store(Store, TransformFun) ->
	rabbit_msg_store:force_recovery(rabbit_mnesia:dir(), Store),
	rabbit_msg_store:transform_dir(rabbit_mnesia:dir(), Store, TransformFun).
