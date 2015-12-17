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

-module(rabbit_table).

%% schema表：Mnesia系统的配置在模式（schema）里描述。模式是一种特殊的表，它包含了诸如表名、每个
%% 表的存储类型（例如，表应该存储到RAM、硬盘或者可能是两者以及表的位置）等信息。
%% 不像数据表，模式表里包含的信息只能通过与模式相关的函数来访问和修改

-export([create/0, create_local_copy/1, wait_for_replicated/0, wait/1,
         force_load/0, is_present/0, is_empty/0, needs_default_data/0,
         check_schema_integrity/0, clear_ram_only_tables/0, wait_timeout/0]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(create/0 :: () -> 'ok').
-spec(create_local_copy/1 :: ('disc' | 'ram') -> 'ok').
-spec(wait_for_replicated/0 :: () -> 'ok').
-spec(wait/1 :: ([atom()]) -> 'ok').
-spec(wait_timeout/0 :: () -> non_neg_integer() | infinity).
-spec(force_load/0 :: () -> 'ok').
-spec(is_present/0 :: () -> boolean()).
-spec(is_empty/0 :: () -> boolean()).
-spec(needs_default_data/0 :: () -> boolean()).
-spec(check_schema_integrity/0 :: () -> rabbit_types:ok_or_error(any())).
-spec(clear_ram_only_tables/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------
%% 创建mnesia数据库表(根据当前文件定义的mnesia表)
create() ->
	lists:foreach(fun ({Tab, TabDef}) ->
						   TabDef1 = proplists:delete(match, TabDef),
						   case mnesia:create_table(Tab, TabDef1) of
							   {atomic, ok} -> ok;
							   {aborted, Reason} ->
								   throw({error, {table_creation_failed,
												  Tab, TabDef1, Reason}})
						   end
				  end, definitions()),
	ok.

%% The sequence(顺序) in which we delete the schema and then the other
%% tables is important: if we delete the schema first when moving to
%% RAM mnesia will loudly complain since it doesn't make much sense to
%% do that. But when moving to disc, we need to move the schema first.
%% 创建本地节点mnesia表的拷贝
create_local_copy(disc) ->
	create_local_copy(schema, disc_copies),
	create_local_copies(disc);

create_local_copy(ram)  ->
	create_local_copies(ram),
	create_local_copy(schema, ram_copies).


%% replicated(复制)(集群中的所有节点等待所有的表复制同步成功)(等待在当前节点上有磁盘副本(disc_copies)的表和cluster完成同步)
wait_for_replicated() ->
	wait([Tab || {Tab, TabDef} <- definitions(),
				 not lists:member({local_content, true}, TabDef)]).


%% in which case(在这种情况下)
wait(TableNames) ->
	%% We might be in ctl here for offline ops(离线操作), in which case we can't
	%% get_env() for the rabbit app.
	Timeout = wait_timeout(),
	%% 如果Mnesia推断另一个节点（远程）的拷贝比本地节点的拷贝更新时，初始化时在节点上复制
	%% 表可能会导致问题，初始化进程无法处理。在这种情况下，对mnesia:wait_for_tables/2的调用将暂
	%% 停调用进程，直到远程节点从其本地磁盘初始化表后通过网络将表复制到本地节点上
	case mnesia:wait_for_tables(TableNames, Timeout) of
		ok ->
			ok;
		{timeout, BadTabs} ->
			throw({error, {timeout_waiting_for_tables, BadTabs}});
		{error, Reason} ->
			throw({error, {failed_waiting_for_tables, Reason}})
	end.


%% 从rabbit应用中拿到等待mnesia数据库表加载的超时时间
wait_timeout() ->
	case application:get_env(rabbit, mnesia_table_loading_timeout) of
		{ok, T}   -> T;
		undefined -> 30000
	end.


%% 强制加载mnesia数据库表根据mnesia_table中定义的数据库表
force_load() -> [mnesia:force_load_table(T) || T <- names()], ok.


%% 判断当前系统中的mnesia数据库表和定义的表是否完全相同
is_present() -> names() -- mnesia:system_info(tables) =:= [].


%% 判断mnesia数据库的所有表是否为空
is_empty()           -> is_empty(names()).


%% rabbit_user，rabbit_user_permission，rabbit_vhost这三个表必须要有默认的数据(判断是否需要插入默认的数据)
needs_default_data() -> is_empty([rabbit_user, rabbit_user_permission,
								  rabbit_vhost]).


%% 判断Names列表中的mnesia表名是否为空
is_empty(Names) ->
	lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
			  Names).


%% 确保mnesia数据库表的完整 integrity(完整)
check_schema_integrity() ->
	Tables = mnesia:system_info(tables),
	case check(fun (Tab, TabDef) ->
						case lists:member(Tab, Tables) of
							false -> {error, {table_missing, Tab}};
							true  -> check_attributes(Tab, TabDef)
						end
			   end) of
		ok     -> ok = wait(names()),
				  check(fun check_content/2);
		Other  -> Other
	end.


%% 清除本节点中ram表中的数据
clear_ram_only_tables() ->
	Node = node(),
	lists:foreach(
	  fun (TabName) ->
			   case lists:member(Node, mnesia:table_info(TabName, ram_copies)) of
				   true  -> {atomic, ok} = mnesia:clear_table(TabName);
				   false -> ok
			   end
	  end, names()),
	ok.

%%--------------------------------------------------------------------
%% Internal(内部) helpers
%%--------------------------------------------------------------------
%% 创建本地节点所有表的拷贝
create_local_copies(Type) ->
	lists:foreach(
	  fun ({Tab, TabDef}) ->
			   HasDiscCopies     = has_copy_type(TabDef, disc_copies),
			   HasDiscOnlyCopies = has_copy_type(TabDef, disc_only_copies),
			   LocalTab          = proplists:get_bool(local_content, TabDef),
			   StorageType =
				   if
					   Type =:= disc orelse LocalTab ->
						   if
							   HasDiscCopies     -> disc_copies;
							   HasDiscOnlyCopies -> disc_only_copies;
							   true              -> ram_copies
						   end;
					   Type =:= ram ->
						   ram_copies
				   end,
			   ok = create_local_copy(Tab, StorageType)
	  end, definitions(Type)),
	ok.


%% 创建Tab表的本地节点的拷贝
%% mnesia:del_table_copy(Tab, Node) 该函数在节点Node上删除表的副本，如果表的最后一个副本被删除，则表被删除
%% mnesia:move_table_copy(Tab, From, To) 该函数是将Tab的副本从From节点移动到To节点, 表的{type}属性被保留, 移动之后From节点上将不存在这个表的副本
%% mnesia:add_table_copy(Tab, Node, Type) 该函数是在Node节点上创建Tab的备份, 并且可以指定新的{type}类型, 这里的type必须是ram_copies, disc_copies, disc_only_copies
create_local_copy(Tab, Type) ->
	StorageType = mnesia:table_info(Tab, storage_type),
	{atomic, ok} =
		if
			StorageType == unknown ->
				%% mnesia:add_table_copy(Tab, Node, Type) 该函数是在Node节点上创建Tab的备份, 并且可以指定新的{type}类型, 
				%% 这里的type必须是ram_copies, disc_copies, disc_only_copies
				mnesia:add_table_copy(Tab, node(), Type);
			StorageType /= Type ->
				%% 改变Tab表的存储类型
				mnesia:change_table_copy_type(Tab, node(), Type);
			true -> {atomic, ok}
		end,
	ok.


has_copy_type(TabDef, DiscType) ->
	lists:member(node(), proplists:get_value(DiscType, TabDef, [])).


%% 检查mnesia表中的字段属性是否跟定义的一致
check_attributes(Tab, TabDef) ->
	{_, ExpAttrs} = proplists:lookup(attributes, TabDef),
	case mnesia:table_info(Tab, attributes) of
		ExpAttrs -> ok;
		Attrs    -> {error, {table_attributes_mismatch, Tab, ExpAttrs, Attrs}}
	end.


%% 检查mnesia表中的字段内容是否跟定义的一致
check_content(Tab, TabDef) ->
	{_, Match} = proplists:lookup(match, TabDef),
	case mnesia:dirty_first(Tab) of
		'$end_of_table' ->
			ok;
		Key ->
			ObjList = mnesia:dirty_read(Tab, Key),
			MatchComp = ets:match_spec_compile([{Match, [], ['$_']}]),
			case ets:match_spec_run(ObjList, MatchComp) of
				ObjList -> ok;
				_       -> {error, {table_content_invalid, Tab, Match, ObjList}}
			end
	end.


%% 通用检查函数
check(Fun) ->
	case [Error || {Tab, TabDef} <- definitions(),
				   case Fun(Tab, TabDef) of
					   ok             -> Error = none, false;
					   {error, Error} -> true
				   end] of
		[]     -> ok;
		Errors -> {error, Errors}
	end.

%%--------------------------------------------------------------------
%% Table definitions
%%--------------------------------------------------------------------
%% 拿到RabbitMQ系统中所有mnesia的数据库表
names() -> [Tab || {Tab, _} <- definitions()].

%% The tables aren't supposed to be on disk on a ram node
definitions(disc) ->
	definitions();
definitions(ram) ->
	[{Tab, [{disc_copies, []}, {ram_copies, [node()]} |
				proplists:delete(
			  ram_copies, proplists:delete(disc_copies, TabDef))]} ||
	 {Tab, TabDef} <- definitions()].

%% mnesia数据表的定义的地方
definitions() ->
	[
	 {rabbit_user,
	  [{record_name, internal_user},
	   {attributes, record_info(fields, internal_user)},
	   {disc_copies, [node()]},
	   {match, #internal_user{_='_'}}]
	 },
	 {rabbit_user_permission,
	  [{record_name, user_permission},
	   {attributes, record_info(fields, user_permission)},
	   {disc_copies, [node()]},
	   {match, #user_permission{user_vhost = #user_vhost{_='_'},
								permission = #permission{_='_'},
								_='_'}}]
	 },
	 {rabbit_vhost,
	  [{record_name, vhost},
	   {attributes, record_info(fields, vhost)},
	   {disc_copies, [node()]},
	   {match, #vhost{_='_'}}]
	 },
	 {rabbit_listener,
	  [{record_name, listener},
	   {attributes, record_info(fields, listener)},
	   {type, bag},
	   {match, #listener{_='_'}}]
	 },
	 {rabbit_durable_route,
	  [{record_name, route},
	   {attributes, record_info(fields, route)},
	   {disc_copies, [node()]},
	   {match, #route{binding = binding_match(), _='_'}}]
	 },
	 {rabbit_semi_durable_route,
	  [{record_name, route},
	   {attributes, record_info(fields, route)},
	   {type, ordered_set},
	   {match, #route{binding = binding_match(), _='_'}}]
	 },
	 {rabbit_route,
	  [{record_name, route},
	   {attributes, record_info(fields, route)},
	   {type, ordered_set},
	   {match, #route{binding = binding_match(), _='_'}}]
	 },
	 {rabbit_reverse_route,
	  [{record_name, reverse_route},
	   {attributes, record_info(fields, reverse_route)},
	   {type, ordered_set},
	   {match, #reverse_route{reverse_binding = reverse_binding_match(),
							  _='_'}}]
	 },
	 {rabbit_topic_trie_node,
	  [{record_name, topic_trie_node},
	   {attributes, record_info(fields, topic_trie_node)},
	   {type, ordered_set},
	   {match, #topic_trie_node{trie_node = trie_node_match(), _='_'}}]
	 },
	 {rabbit_topic_trie_edge,
	  [{record_name, topic_trie_edge},
	   {attributes, record_info(fields, topic_trie_edge)},
	   {type, ordered_set},
	   {match, #topic_trie_edge{trie_edge = trie_edge_match(), _='_'}}]
	 },
	 {rabbit_topic_trie_binding,
	  [{record_name, topic_trie_binding},
	   {attributes, record_info(fields, topic_trie_binding)},
	   {type, ordered_set},
	   {match, #topic_trie_binding{trie_binding = trie_binding_match(),
								   _='_'}}]
	 },
	 {rabbit_durable_exchange,
	  [{record_name, exchange},
	   {attributes, record_info(fields, exchange)},
	   {disc_copies, [node()]},
	   {match, #exchange{name = exchange_name_match(), _='_'}}]
	 },
	 {rabbit_exchange,
	  [{record_name, exchange},
	   {attributes, record_info(fields, exchange)},
	   {match, #exchange{name = exchange_name_match(), _='_'}}]
	 },
	 {rabbit_exchange_serial,
	  [{record_name, exchange_serial},
	   {attributes, record_info(fields, exchange_serial)},
	   {match, #exchange_serial{name = exchange_name_match(), _='_'}}]
	 },
	 {rabbit_runtime_parameters,
	  [{record_name, runtime_parameters},
	   {attributes, record_info(fields, runtime_parameters)},
	   {disc_copies, [node()]},
	   {match, #runtime_parameters{_='_'}}]
	 },
	 {rabbit_durable_queue,
	  [{record_name, amqqueue},
	   {attributes, record_info(fields, amqqueue)},
	   {disc_copies, [node()]},
	   {match, #amqqueue{name = queue_name_match(), _='_'}}]
	 },
	 {rabbit_queue,
	  [{record_name, amqqueue},
	   {attributes, record_info(fields, amqqueue)},
	   {match, #amqqueue{name = queue_name_match(), _='_'}}]
	 }
	]
	%% 获得gm_group数据库表的定义，跟消息队列镜像队列相关的数据库表
	++ gm:table_definitions()
	++ mirrored_supervisor:table_definitions().


binding_match() ->
	#binding{source = exchange_name_match(),
			 destination = binding_destination_match(),
			 _='_'}.


reverse_binding_match() ->
	#reverse_binding{destination = binding_destination_match(),
					 source = exchange_name_match(),
					 _='_'}.


binding_destination_match() ->
	resource_match('_').


trie_node_match() ->
	#trie_node{   exchange_name = exchange_name_match(), _='_'}.
trie_edge_match() ->
	#trie_edge{   exchange_name = exchange_name_match(), _='_'}.
trie_binding_match() ->
	#trie_binding{exchange_name = exchange_name_match(), _='_'}.


exchange_name_match() ->
	resource_match(exchange).


queue_name_match() ->
	resource_match(queue).


resource_match(Kind) ->
	#resource{kind = Kind, _='_'}.
