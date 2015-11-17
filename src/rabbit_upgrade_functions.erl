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

-module(rabbit_upgrade_functions).

%% If you are tempted to add include("rabbit.hrl"). here, don't. Using record
%% defs here leads to pain later.

-compile([export_all]).

-rabbit_upgrade({remove_user_scope,     mnesia, []}).
-rabbit_upgrade({hash_passwords,        mnesia, []}).
-rabbit_upgrade({add_ip_to_listener,    mnesia, []}).
-rabbit_upgrade({internal_exchanges,    mnesia, []}).
-rabbit_upgrade({user_to_internal_user, mnesia, [hash_passwords]}).
-rabbit_upgrade({topic_trie,            mnesia, []}).
-rabbit_upgrade({semi_durable_route,    mnesia, []}).
-rabbit_upgrade({exchange_event_serial, mnesia, []}).
-rabbit_upgrade({trace_exchanges,       mnesia, [internal_exchanges]}).
-rabbit_upgrade({user_admin_to_tags,    mnesia, [user_to_internal_user]}).
-rabbit_upgrade({ha_mirrors,            mnesia, []}).
-rabbit_upgrade({gm,                    mnesia, []}).
-rabbit_upgrade({exchange_scratch,      mnesia, [trace_exchanges]}).
-rabbit_upgrade({mirrored_supervisor,   mnesia, []}).
-rabbit_upgrade({topic_trie_node,       mnesia, []}).
-rabbit_upgrade({runtime_parameters,    mnesia, []}).
-rabbit_upgrade({exchange_scratches,    mnesia, [exchange_scratch]}).
-rabbit_upgrade({policy,                mnesia,
                 [exchange_scratches, ha_mirrors]}).
-rabbit_upgrade({sync_slave_pids,       mnesia, [policy]}).
-rabbit_upgrade({no_mirror_nodes,       mnesia, [sync_slave_pids]}).
-rabbit_upgrade({gm_pids,               mnesia, [no_mirror_nodes]}).
-rabbit_upgrade({exchange_decorators,   mnesia, [policy]}).
-rabbit_upgrade({policy_apply_to,       mnesia, [runtime_parameters]}).
-rabbit_upgrade({queue_decorators,      mnesia, [gm_pids]}).
-rabbit_upgrade({internal_system_x,     mnesia, [exchange_decorators]}).
-rabbit_upgrade({cluster_name,          mnesia, [runtime_parameters]}).
-rabbit_upgrade({down_slave_nodes,      mnesia, [queue_decorators]}).
-rabbit_upgrade({queue_state,           mnesia, [down_slave_nodes]}).
-rabbit_upgrade({recoverable_slaves,    mnesia, [queue_state]}).

%% -------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_user_scope/0     :: () -> 'ok').
-spec(hash_passwords/0        :: () -> 'ok').
-spec(add_ip_to_listener/0    :: () -> 'ok').
-spec(internal_exchanges/0    :: () -> 'ok').
-spec(user_to_internal_user/0 :: () -> 'ok').
-spec(topic_trie/0            :: () -> 'ok').
-spec(semi_durable_route/0    :: () -> 'ok').
-spec(exchange_event_serial/0 :: () -> 'ok').
-spec(trace_exchanges/0       :: () -> 'ok').
-spec(user_admin_to_tags/0    :: () -> 'ok').
-spec(ha_mirrors/0            :: () -> 'ok').
-spec(gm/0                    :: () -> 'ok').
-spec(exchange_scratch/0      :: () -> 'ok').
-spec(mirrored_supervisor/0   :: () -> 'ok').
-spec(topic_trie_node/0       :: () -> 'ok').
-spec(runtime_parameters/0    :: () -> 'ok').
-spec(policy/0                :: () -> 'ok').
-spec(sync_slave_pids/0       :: () -> 'ok').
-spec(no_mirror_nodes/0       :: () -> 'ok').
-spec(gm_pids/0               :: () -> 'ok').
-spec(exchange_decorators/0   :: () -> 'ok').
-spec(policy_apply_to/0       :: () -> 'ok').
-spec(queue_decorators/0      :: () -> 'ok').
-spec(internal_system_x/0     :: () -> 'ok').
-spec(cluster_name/0          :: () -> 'ok').
-spec(down_slave_nodes/0      :: () -> 'ok').
-spec(queue_state/0           :: () -> 'ok').
-spec(recoverable_slaves/0    :: () -> 'ok').

-endif.

%%--------------------------------------------------------------------

%% It's a bad idea to use records or record_info here, even for the
%% destination form. Because in the future, the destination form of
%% your current transform may not match the record any more, and it
%% would be messy to have to go back and fix old transforms at that
%% point.
%% 将rabbit_user_permission表中permission字段中的第一个字段删除掉(表结构中的字段有删除)
remove_user_scope() ->
	transform(
	  rabbit_user_permission,
	  fun ({user_permission, UV, {permission, _Scope, Conf, Write, Read}}) ->
			   {user_permission, UV, {permission, Conf, Write, Read}}
	  end,
	  [user_vhost, permission]).


%% 对rabbit_user表中用户的密码进行hash操作(老版本的密码没有进行hash操作)
hash_passwords() ->
	transform(
	  rabbit_user,
	  fun ({user, Username, Password, IsAdmin}) ->
			   Hash = rabbit_auth_backend_internal:hash_password(Password),
			   {user, Username, Hash, IsAdmin}
	  end,
	  [username, password_hash, is_admin]).


%% rabbit_listener表中增加ip_address字段(rabbit_listener表中的字段新版本中有增加)
add_ip_to_listener() ->
	transform(
	  rabbit_listener,
	  fun ({listener, Node, Protocol, Host, Port}) ->
			   {listener, Node, Protocol, Host, {0, 0, 0, 0}, Port}
	  end,
	  [node, protocol, host, ip_address, port]).


%% 在交换机表rabbit_exchange和rabbit_durable_exchange表中添加字段
internal_exchanges() ->
	Tables = [rabbit_exchange, rabbit_durable_exchange],
	AddInternalFun =
		fun ({exchange, Name, Type, Durable, AutoDelete, Args}) ->
				 {exchange, Name, Type, Durable, AutoDelete, false, Args}
		end,
	[ ok = transform(T,
					 AddInternalFun,
					 [name, type, durable, auto_delete, internal, arguments])
			 || T <- Tables ],
	ok.


%% 将以前的用户表user结构的名字替换为internal_user
user_to_internal_user() ->
	transform(
	  rabbit_user,
	  fun({user, Username, PasswordHash, IsAdmin}) ->
			  {internal_user, Username, PasswordHash, IsAdmin}
	  end,
	  [username, password_hash, is_admin], internal_user).


%% 创建exchange交换机topic类型寻找对应消息队列的数据表
topic_trie() ->
	create(rabbit_topic_trie_edge, [{record_name, topic_trie_edge},
									{attributes, [trie_edge, node_id]},
									{type, ordered_set}]),
	create(rabbit_topic_trie_binding, [{record_name, topic_trie_binding},
									   {attributes, [trie_binding, value]},
									   {type, ordered_set}]).


%% 升级创建rabbit_semi_durable_route表
semi_durable_route() ->
	create(rabbit_semi_durable_route, [{record_name, route},
									   {attributes, [binding, value]}]).


%% 升级创建rabbit_exchange_serial表
exchange_event_serial() ->
	create(rabbit_exchange_serial, [{record_name, exchange_serial},
									{attributes, [name, next]}]).


%% 向rabbit_durable_exchange表插入amq.rabbitmq.trace交换机
trace_exchanges() ->
	[declare_exchange(
	   rabbit_misc:r(VHost, exchange, <<"amq.rabbitmq.trace">>), topic) ||
	   VHost <- rabbit_vhost:list()],
	ok.


%% 向rabbit_user表中调整tags字段
user_admin_to_tags() ->
	transform(
	  rabbit_user,
	  fun({internal_user, Username, PasswordHash, true}) ->
			  {internal_user, Username, PasswordHash, [administrator]};
		 ({internal_user, Username, PasswordHash, false}) ->
			  {internal_user, Username, PasswordHash, [management]}
	  end,
	  [username, password_hash, tags], internal_user).


%% 高可用队列相关
ha_mirrors() ->
	Tables = [rabbit_queue, rabbit_durable_queue],
	AddMirrorPidsFun =
		fun ({amqqueue, Name, Durable, AutoDelete, Owner, Arguments, Pid}) ->
				 {amqqueue, Name, Durable, AutoDelete, Owner, Arguments, Pid,
				  [], undefined}
		end,
	[ ok = transform(T,
					 AddMirrorPidsFun,
					 [name, durable, auto_delete, exclusive_owner, arguments,
					  pid, slave_pids, mirror_nodes])
			 || T <- Tables ],
	ok.


%% 高可用队列相关
gm() ->
	create(gm_group, [{record_name, gm_group},
					  {attributes, [name, version, members]}]).


%% 向rabbit_exchange和rabbit_durable_exchange表中添加scratch字段
exchange_scratch() ->
	ok = exchange_scratch(rabbit_exchange),
	ok = exchange_scratch(rabbit_durable_exchange).


exchange_scratch(Table) ->
	transform(
	  Table,
	  fun ({exchange, Name, Type, Dur, AutoDel, Int, Args}) ->
			   {exchange, Name, Type, Dur, AutoDel, Int, Args, undefined}
	  end,
	  [name, type, durable, auto_delete, internal, arguments, scratch]).


%% 创建mirrored_sup_childspec表
mirrored_supervisor() ->
	create(mirrored_sup_childspec,
		   [{record_name, mirrored_sup_childspec},
			{attributes, [key, mirroring_pid, childspec]}]).


%% 创建rabbit_topic_trie_node表，该表示交换机exchange topic类型路由消息队列相关的表
topic_trie_node() ->
	create(rabbit_topic_trie_node,
		   [{record_name, topic_trie_node},
			{attributes, [trie_node, edge_count, binding_count]},
			{type, ordered_set}]).


%% 创建rabbit_runtime_parameters表
runtime_parameters() ->
	create(rabbit_runtime_parameters,
		   [{record_name, runtime_parameters},
			{attributes, [key, value]},
			{disc_copies, [node()]}]).


%% 将rabbit_exchange表和rabbit_durable_exchange表中字段scratch改为scratches
exchange_scratches() ->
	ok = exchange_scratches(rabbit_exchange),
	ok = exchange_scratches(rabbit_durable_exchange).


exchange_scratches(Table) ->
	transform(
	  Table,
	  fun ({exchange, Name, Type = <<"x-federation">>, Dur, AutoDel, Int, Args,
			Scratch}) ->
			   Scratches = orddict:store(federation, Scratch, orddict:new()),
			   {exchange, Name, Type, Dur, AutoDel, Int, Args, Scratches};
		 %% We assert here that nothing else uses the scratch mechanism ATM
		 ({exchange, Name, Type, Dur, AutoDel, Int, Args, undefined}) ->
			  {exchange, Name, Type, Dur, AutoDel, Int, Args, undefined}
	  end,
	  [name, type, durable, auto_delete, internal, arguments, scratches]).


%% 向队列和交换机的表结构中添加policy策略字段
policy() ->
	ok = exchange_policy(rabbit_exchange),
	ok = exchange_policy(rabbit_durable_exchange),
	ok = queue_policy(rabbit_queue),
	ok = queue_policy(rabbit_durable_queue).


exchange_policy(Table) ->
	transform(
	  Table,
	  fun ({exchange, Name, Type, Dur, AutoDel, Int, Args, Scratches}) ->
			   {exchange, Name, Type, Dur, AutoDel, Int, Args, Scratches,
				undefined}
	  end,
	  [name, type, durable, auto_delete, internal, arguments, scratches,
	   policy]).


queue_policy(Table) ->
	transform(
	  Table,
	  fun ({amqqueue, Name, Dur, AutoDel, Excl, Args, Pid, SPids, MNodes}) ->
			   {amqqueue, Name, Dur, AutoDel, Excl, Args, Pid, SPids, MNodes,
				undefined}
	  end,
	  [name, durable, auto_delete, exclusive_owner, arguments, pid,
	   slave_pids, mirror_nodes, policy]).


%% 高可用队列相关
sync_slave_pids() ->
	Tables = [rabbit_queue, rabbit_durable_queue],
	AddSyncSlavesFun =
		fun ({amqqueue, N, D, AD, Excl, Args, Pid, SPids, MNodes, Pol}) ->
				 {amqqueue, N, D, AD, Excl, Args, Pid, SPids, [], MNodes, Pol}
		end,
	[ok = transform(T, AddSyncSlavesFun,
					[name, durable, auto_delete, exclusive_owner, arguments,
					 pid, slave_pids, sync_slave_pids, mirror_nodes, policy])
			|| T <- Tables],
	ok.


%% 高可用队列相关
no_mirror_nodes() ->
	Tables = [rabbit_queue, rabbit_durable_queue],
	RemoveMirrorNodesFun =
		fun ({amqqueue, N, D, AD, O, A, Pid, SPids, SSPids, _MNodes, Pol}) ->
				 {amqqueue, N, D, AD, O, A, Pid, SPids, SSPids, Pol}
		end,
	[ok = transform(T, RemoveMirrorNodesFun,
					[name, durable, auto_delete, exclusive_owner, arguments,
					 pid, slave_pids, sync_slave_pids, policy])
			|| T <- Tables],
	ok.


%% 高可用队列相关
gm_pids() ->
	Tables = [rabbit_queue, rabbit_durable_queue],
	AddGMPidsFun =
		fun ({amqqueue, N, D, AD, O, A, Pid, SPids, SSPids, Pol}) ->
				 {amqqueue, N, D, AD, O, A, Pid, SPids, SSPids, Pol, []}
		end,
	[ok = transform(T, AddGMPidsFun,
					[name, durable, auto_delete, exclusive_owner, arguments,
					 pid, slave_pids, sync_slave_pids, policy, gm_pids])
			|| T <- Tables],
	ok.


%% 向交换机表中添加decorators字段
exchange_decorators() ->
	ok = exchange_decorators(rabbit_exchange),
	ok = exchange_decorators(rabbit_durable_exchange).


exchange_decorators(Table) ->
	transform(
	  Table,
	  fun ({exchange, Name, Type, Dur, AutoDel, Int, Args, Scratches,
			Policy}) ->
			   {exchange, Name, Type, Dur, AutoDel, Int, Args, Scratches, Policy,
				{[], []}}
	  end,
	  [name, type, durable, auto_delete, internal, arguments, scratches, policy,
	   decorators]).


%% 策略升级apply_to字段
policy_apply_to() ->
	transform(
	  rabbit_runtime_parameters,
	  fun ({runtime_parameters, Key = {_VHost, <<"policy">>, _Name}, Value}) ->
			   ApplyTo = apply_to(proplists:get_value(<<"definition">>, Value)),
			   {runtime_parameters, Key, [{<<"apply-to">>, ApplyTo} | Value]};
		 ({runtime_parameters, Key, Value}) ->
			  {runtime_parameters, Key, Value}
	  end,
	  [key, value]),
	rabbit_policy:invalidate(),
	ok.


%% 策略升级apply_to字段
apply_to(Def) ->
	case [proplists:get_value(K, Def) ||
			K <- [<<"federation-upstream-set">>, <<"ha-mode">>]] of
		[undefined, undefined] -> <<"all">>;
		[_,         undefined] -> <<"exchanges">>;
		[undefined, _]         -> <<"queues">>;
		[_,         _]         -> <<"all">>
	end.


%% 向队列表中添加decorators字段
queue_decorators() ->
	ok = queue_decorators(rabbit_queue),
	ok = queue_decorators(rabbit_durable_queue).


%% 向队列表中添加decorators字段
queue_decorators(Table) ->
	transform(
	  Table,
	  fun ({amqqueue, Name, Durable, AutoDelete, ExclusiveOwner, Arguments,
			Pid, SlavePids, SyncSlavePids, Policy, GmPids}) ->
			   {amqqueue, Name, Durable, AutoDelete, ExclusiveOwner, Arguments,
				Pid, SlavePids, SyncSlavePids, Policy, GmPids, []}
	  end,
	  [name, durable, auto_delete, exclusive_owner, arguments, pid, slave_pids,
	   sync_slave_pids, policy, gm_pids, decorators]).


internal_system_x() ->
	transform(
	  rabbit_durable_exchange,
	  fun ({exchange, Name = {resource, _, _, <<"amq.rabbitmq.", _/binary>>},
			Type, Dur, AutoDel, _Int, Args, Scratches, Policy, Decorators}) ->
			   {exchange, Name, Type, Dur, AutoDel, true, Args, Scratches,
				Policy, Decorators};
		 (X) ->
			  X
	  end,
	  [name, type, durable, auto_delete, internal, arguments, scratches, policy,
	   decorators]).


cluster_name() ->
	{atomic, ok} = mnesia:transaction(fun cluster_name_tx/0),
	ok.


cluster_name_tx() ->
	%% mnesia:transform_table/4 does not let us delete records
	T = rabbit_runtime_parameters,
	mnesia:write_lock_table(T),
	Ks = [K || {_VHost, <<"federation">>, <<"local-nodename">>} = K
																	  <- mnesia:all_keys(T)],
	case Ks of
		[]     -> ok;
		[K|Tl] -> [{runtime_parameters, _K, Name}] = mnesia:read(T, K, write),
				  R = {runtime_parameters, cluster_name, Name},
				  mnesia:write(T, R, write),
				  case Tl of
					  [] -> ok;
					  _  -> {VHost, _, _} = K,
							error_logger:warning_msg(
							  "Multiple local-nodenames found, picking '~s' "
								  "from '~s' for cluster name~n", [Name, VHost])
				  end
	end,
	[mnesia:delete(T, K, write) || K <- Ks],
	ok.


%% 高可用队列相关
down_slave_nodes() ->
	ok = down_slave_nodes(rabbit_queue),
	ok = down_slave_nodes(rabbit_durable_queue).


%% 高可用队列相关
down_slave_nodes(Table) ->
	transform(
	  Table,
	  fun ({amqqueue, Name, Durable, AutoDelete, ExclusiveOwner, Arguments,
			Pid, SlavePids, SyncSlavePids, Policy, GmPids, Decorators}) ->
			   {amqqueue, Name, Durable, AutoDelete, ExclusiveOwner, Arguments,
				Pid, SlavePids, SyncSlavePids, [], Policy, GmPids, Decorators}
	  end,
	  [name, durable, auto_delete, exclusive_owner, arguments, pid, slave_pids,
	   sync_slave_pids, down_slave_nodes, policy, gm_pids, decorators]).


%% 向队列表中添加state字段
queue_state() ->
	ok = queue_state(rabbit_queue),
	ok = queue_state(rabbit_durable_queue).


queue_state(Table) ->
	transform(
	  Table,
	  fun ({amqqueue, Name, Durable, AutoDelete, ExclusiveOwner, Arguments,
			Pid, SlavePids, SyncSlavePids, DSN, Policy, GmPids, Decorators}) ->
			   {amqqueue, Name, Durable, AutoDelete, ExclusiveOwner, Arguments,
				Pid, SlavePids, SyncSlavePids, DSN, Policy, GmPids, Decorators,
				live}
	  end,
	  [name, durable, auto_delete, exclusive_owner, arguments, pid, slave_pids,
	   sync_slave_pids, down_slave_nodes, policy, gm_pids, decorators, state]).


%% recoverable：可恢复的
recoverable_slaves() ->
	ok = recoverable_slaves(rabbit_queue),
	ok = recoverable_slaves(rabbit_durable_queue).


recoverable_slaves(Table) ->
	transform(
	  Table, fun (Q) -> Q end, %% Don't change shape of record
	  [name, durable, auto_delete, exclusive_owner, arguments, pid, slave_pids,
	   sync_slave_pids, recoverable_slaves, policy, gm_pids, decorators,
	   state]).


%%--------------------------------------------------------------------
%% 对表结构中所有的表元素执行Fun函数
transform(TableName, Fun, FieldList) ->
	rabbit_table:wait([TableName]),
	%% mnesia:transform_table该方法对Tab表的所有数据更改format，它对表里所有记录调用Fun
	{atomic, ok} = mnesia:transform_table(TableName, Fun, FieldList),
	ok.


%% 对表结构中所有的表元素执行Fun函数，同时NewRecordName是新设置的record名字
transform(TableName, Fun, FieldList, NewRecordName) ->
	rabbit_table:wait([TableName]),
	{atomic, ok} = mnesia:transform_table(TableName, Fun, FieldList,
										  NewRecordName),
	ok.


%% mnesia数据库创建Tab表
create(Tab, TabDef) ->
	{atomic, ok} = mnesia:create_table(Tab, TabDef),
	ok.

%% Dumb replacement for rabbit_exchange:declare that does not require
%% the exchange type registry or worker pool to be running by dint of
%% not validating anything and assuming the exchange type does not
%% require serialisation.
%% NB: this assumes the pre-exchange-scratch-space format
declare_exchange(XName, Type) ->
	X = {exchange, XName, Type, true, false, false, []},
	ok = mnesia:dirty_write(rabbit_durable_exchange, X).
