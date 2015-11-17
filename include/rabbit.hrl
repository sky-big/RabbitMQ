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

%% Passed around most places
-record(user, {username,
               tags,
               authz_backends}). %% List of {Module, AuthUserImpl} pairs

%% Passed to auth backends
-record(auth_user, {username,
                    tags,
                    impl}).

%% Implementation for the internal auth backend
%% 内部的玩家结构
-record(internal_user, {username, password_hash, tags}).

%% 用户的权限结构
-record(permission, {configure, write, read}).

%% 用户对应的host
-record(user_vhost, {username, virtual_host}).

%% 用户对应的权限
-record(user_permission, {user_vhost, permission}).

%% Host结构
-record(vhost, {virtual_host, dummy}).

%% 消息内容结构
-record(content,
        {class_id,
         properties, %% either 'none', or a decoded record/tuple
         properties_bin, %% either 'none', or an encoded properties binary
         %% Note: at most one of properties and properties_bin can be
         %% 'none' at once.
         protocol, %% The protocol under which properties_bin was encoded
         payload_fragments_rev %% list of binaries, in reverse order (!)
         }).

%% 资源结构名字
-record(resource, {virtual_host, kind, name}).

%% fields described as 'transient' here are cleared when writing to
%% rabbit_durable_<thing>
%% exchange数据结构
-record(exchange, {
				   name,						%% resource资源结构，该结构中包含VHost和exchange的名字
				   type,						%% exchange交换机的类型
				   durable,						%% exchange交换机的持久化标志
				   auto_delete,					%% 是否自动删除
				   internal,					%% 是否是内部的标志
				   arguments, %% immutable		%% 参数
				   scratches,    %% durable, explicitly(显式的) updated via update_scratch/3
				   policy,       %% durable, implicitly(暗中的) updated when policy changes
				   decorators}). %% transient, recalculated in store/1 (i.e. recovery)

%% 队列的数据结构
-record(amqqueue, {
				   name,						%% resource资源结构，该结构中包含VHost和队列的名字
				   durable,						%% 队列是否可持久化的标志
				   auto_delete,					%% 队列是否在消费者为0的时候自动删除掉的标志
				   exclusive_owner = none, %% immutable(exclusive：独有的，仅创建者可以使用的私有队列，断开后自动删除(当前连接不在时，队列是否自动删除))
				   arguments,                   %% immutable(配置参数)
				   pid,                         %% durable (just so we know home node)(队列进程的Pid)
				   slave_pids,					%% 该队列的镜像队列的进程Pid列表
				   sync_slave_pids, %% transient(同步的镜像队列的进程Pid列表)
				   recoverable_slaves,          %% durable(recoverable:可恢复的)
				   policy,                      %% durable, implicit update as above
				   gm_pids,                     %% transient
				   decorators,                  %% transient, recalculated(重新计算) as above
				   state}).                     %% durable (have we crashed?)(当前队列的状态)

-record(exchange_serial, {name, next}).

%% mnesia doesn't like unary records, so we add a dummy 'value' field
%% 路由信息存储结构
-record(route, {binding, value = const}).
-record(reverse_route, {reverse_binding, value = const}).

%% 绑定信息结构
-record(binding, {source, key, destination, args = []}).
-record(reverse_binding, {destination, key, source, args = []}).

%% exchange交换机topic类型匹配相关的数据结构
-record(topic_trie_node, {trie_node, edge_count, binding_count}).
-record(topic_trie_edge, {trie_edge, node_id}).
-record(topic_trie_binding, {trie_binding, value = const}).

%% topic绑定信息中的节点小数据结构
-record(trie_node, {exchange_name, node_id}).
%% topic绑定信息中的边小数据结构
-record(trie_edge, {exchange_name, node_id, word}).
%% topic绑定信息中的绑定小数据结构
-record(trie_binding, {exchange_name, node_id, destination, arguments}).

-record(listener, {node, protocol, host, ip_address, port}).

-record(runtime_parameters, {key, value}).

%% 消息内容的最基础结构
-record(basic_message, {exchange_name, routing_keys = [], content, id,
                        is_persistent}).

-record(ssl_socket, {tcp, ssl}).
-record(delivery, {mandatory, confirm, sender, message, msg_seq_no, flow}).
-record(amqp_error, {name, explanation = "", method = none}).

%% 事件数据结构
-record(event, {type, props, reference = undefined, timestamp}).

%% 单个消息的属性数据结构
-record(message_properties, {expiry, needs_confirming = false, size}).

%% 插件结构
-record(plugin, {
				 name,          %% atom()						%% 当前插件的名字
				 version,       %% string()						%% 当前插件的版本号
				 description,   %% string()						%% 当前插件的描述信息
				 type,          %% 'ez' or 'dir'				%% 当前插件的类型
				 dependencies,  %% [{atom(), string()}]			%% 当前插件的依赖app列表
				 location		%% string()						%% 当前插件所处的位置
				}).

%%----------------------------------------------------------------------------

-define(COPYRIGHT_MESSAGE, "Copyright (C) 2007-2014 GoPivotal, Inc.").
-define(INFORMATION_MESSAGE, "Licensed under the MPL.  See http://www.rabbitmq.com/").
-define(ERTS_MINIMUM, "5.6.3").

%% EMPTY_FRAME_SIZE, 8 = 1 + 2 + 4 + 1
%%  - 1 byte of frame type
%%  - 2 bytes of channel number
%%  - 4 bytes of frame payload length
%%  - 1 byte of payload trailer FRAME_END byte
%% See rabbit_binary_generator:check_empty_frame_size/0, an assertion
%% called at startup.
-define(EMPTY_FRAME_SIZE, 8).

-define(MAX_WAIT, 16#ffffffff).

-define(HIBERNATE_AFTER_MIN,        1000).
-define(DESIRED_HIBERNATE,         10000).
-define(CREDIT_DISC_BOUND,   {2000, 500}).

-define(INVALID_HEADERS_KEY, <<"x-invalid-headers">>).
-define(ROUTING_HEADERS, [<<"CC">>, <<"BCC">>]).
-define(DELETED_HEADER, <<"BCC">>).

%% Trying to send a term across a cluster larger than 2^31 bytes will
%% cause the VM to exit with "Absurdly large distribution output data
%% buffer". So we limit the max message size to 2^31 - 10^6 bytes (1MB
%% to allow plenty of leeway for the #basic_message{} and #content{}
%% wrapping the message body).
-define(MAX_MSG_SIZE, 2147383648).

%% First number is maximum size in bytes before we start to
%% truncate. The following 4-tuple is:
%%
%% 1) Maximum size of printable lists and binaries.
%% 2) Maximum size of any structural term.
%% 3) Amount to decrease 1) every time we descend while truncating.
%% 4) Amount to decrease 2) every time we descend while truncating.
%%
%% Whole thing feeds into truncate:log_event/2.
%% 100000表示最大字节，如果超过则需要进行截取
%% 2000表示可打印字符串和二进制数据的最大字节
%% 100表示任何结构性数据的最大字节，如果超过该数量则需要进行截取
-define(LOG_TRUNC, {100000, {2000, 100, 50, 5}}).

%% 存储进程的名字的操作
-define(store_proc_name(N), rabbit_misc:store_proc_name(?MODULE, N)).
