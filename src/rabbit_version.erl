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

-module(rabbit_version).

-export([recorded/0, matches/2, desired/0, desired_for_scope/1,
         record_desired/0, record_desired_for_scope/1,
         upgrades_required/1]).

%% -------------------------------------------------------------------
-ifdef(use_specs).

-export_type([scope/0, step/0]).

-type(scope() :: atom()).
-type(scope_version() :: [atom()]).
-type(step() :: {atom(), atom()}).

-type(version() :: [atom()]).

-spec(recorded/0 :: () -> rabbit_types:ok_or_error2(version(), any())).
-spec(matches/2 :: ([A], [A]) -> boolean()).
-spec(desired/0 :: () -> version()).
-spec(desired_for_scope/1 :: (scope()) -> scope_version()).
-spec(record_desired/0 :: () -> 'ok').
-spec(record_desired_for_scope/1 ::
        (scope()) -> rabbit_types:ok_or_error(any())).
-spec(upgrades_required/1 ::
        (scope()) -> rabbit_types:ok_or_error2([step()], any())).

-endif.
%% -------------------------------------------------------------------

-define(VERSION_FILENAME, "schema_version").
-define(SCOPES, [mnesia, local]).

%% -------------------------------------------------------------------
%% 读取mnesia数据库目录下的schema_version文件的数据
recorded() -> case rabbit_file:read_term_file(schema_filename()) of
				  {ok, [V]}        -> {ok, V};
				  {error, _} = Err -> Err
			  end.


%% 更新mnesia数据库目录下的schema_version文件中的数据
record(V) -> ok = rabbit_file:write_term_file(schema_filename(), [V]).


%% 根据Scope范围从schema_version文件中读取配置的需要升级的名字
recorded_for_scope(Scope) ->
	case recorded() of
		{error, _} = Err ->
			Err;
		{ok, Version} ->
			{ok, case lists:keysearch(Scope, 1, categorise_by_scope(Version)) of
					 false                 -> [];
					 {value, {Scope, SV1}} -> SV1
				 end}
	end.


%% 更新Scope对应的属性(categorise：归类，scope：范围)
record_for_scope(Scope, ScopeVersion) ->
	case recorded() of
		{error, _} = Err ->
			Err;
		{ok, Version} ->
			Version1 = lists:keystore(Scope, 1, categorise_by_scope(Version),
									  {Scope, ScopeVersion}),
			%% 写入到schema_version文件中
			ok = record([Name || {_Scope, Names} <- Version1, Name <- Names])
	end.

%% -------------------------------------------------------------------
%% 判断是否匹配
matches(VerA, VerB) ->
	lists:usort(VerA) =:= lists:usort(VerB).

%% -------------------------------------------------------------------
%% 拿到mnesia，local有向图中的出度为0的Step列表
desired() -> [Name || Scope <- ?SCOPES, Name <- desired_for_scope(Scope)].


%% 拿到Scope范围对应的有向图出度为0的顶点
desired_for_scope(Scope) -> with_upgrade_graph(fun heads/1, Scope).


%% 磁盘节点第一次启动的时候创建mnesia数据库表的时候会调用该函数(将mnesia，local有向图中的出度为0的Step列表存储到schema_version文件中)
record_desired() -> record(desired()).


%% 将Scope中所有出度为零的顶点存入schema_version文件(即将这次的升级名字写入磁盘文件，即将Scope范围最新的出度为0的顶点写入磁盘文件)
record_desired_for_scope(Scope) ->
	record_for_scope(Scope, desired_for_scope(Scope)).


%% Scope：表示范围，得到Scope范围中需要升级的名字
upgrades_required(Scope) ->
	%% 根据Scope范围从schema_version文件中读取配置的需要升级的名字
	case recorded_for_scope(Scope) of
		{error, enoent} ->
			case filelib:is_file(rabbit_guid:filename()) of
				false -> {error, starting_from_scratch};
				true  -> {error, version_not_available}
			end;
		{ok, CurrentHeads} ->
			%% 生成升级的有向图
			with_upgrade_graph(
			  fun (G) ->
					   case unknown_heads(CurrentHeads, G) of
						   []      -> {ok, upgrades_to_apply(CurrentHeads, G)};
						   Unknown -> {error, {future_upgrades_found, Unknown}}
					   end
			  end, Scope)
	end.

%% -------------------------------------------------------------------
%% 生成升级的有向图
with_upgrade_graph(Fun, Scope) ->
	case rabbit_misc:build_acyclic_graph(
		   fun ({_App, Module, Steps}) -> vertices(Module, Steps, Scope) end,
		   fun ({_App, Module, Steps}) -> edges(Module, Steps, Scope) end,
		   rabbit_misc:all_module_attributes(rabbit_upgrade)) of
		{ok, G} -> try
					   Fun(G)
				   after
					   true = digraph:delete(G)
				   end;
		{error, {vertex, duplicate, StepName}} ->
			throw({error, {duplicate_upgrade_step, StepName}});
		{error, {edge, {bad_vertex, StepName}, _From, _To}} ->
			throw({error, {dependency_on_unknown_upgrade_step, StepName}});
		{error, {edge, {bad_edge, StepNames}, _From, _To}} ->
			throw({error, {cycle_in_upgrade_steps, StepNames}})
	end.


%% 获得有向图的顶点的函数
vertices(Module, Steps, Scope0) ->
	[{StepName, {Module, StepName}} || {StepName, Scope1, _Reqs} <- Steps,
									   Scope0 == Scope1].


%% 获得有向图边的函数
edges(_Module, Steps, Scope0) ->
	[{Require, StepName} || {StepName, Scope1, Requires} <- Steps,
							Require <- Requires,
							Scope0 == Scope1].


%% 找出Heads列表中的节点没有存在有向图G中的节点
unknown_heads(Heads, G) ->
	[H || H <- Heads, digraph:vertex(G, H) =:= false].


%% 得到实际要升级的Name列表
upgrades_to_apply(Heads, G) ->
	%% Take all the vertices which can reach the known heads. That's
	%% everything we've already applied. Subtract(减掉) that from all
	%% vertices: that's what we have to apply.
	%% 得到有向图G中的节点(该节点是没有path到Heads)
	Unsorted = sets:to_list(
				 sets:subtract(
				   %% 得到有向图G中的所有节点
				   sets:from_list(digraph:vertices(G)),
				   %% 返回有向图G中所有的有到Heads列表中节点路径的节点列表
				   sets:from_list(digraph_utils:reaching(Heads, G)))),
	%% Form a subgraph(子图) from that list and find a topological ordering(拓扑顺序)
	%% so we can invoke them in order.
	%% digraph_utils:reaching(Vertices, Digraph) -> 返回有向图Digraph中有到达Vertices列表中节点的所有顶点(包括Vertices节点中自己的本身)
	%% digraph_utils:subgraph(G, Unsorted) -> 返回Unsorted这些节点的最大子图，最大子图就是在给出的节点中，从原图中构造出一个节点最多的子图
	[element(2, digraph:vertex(G, StepName)) ||
	   StepName <- digraph_utils:topsort(digraph_utils:subgraph(G, Unsorted))].


%% 获得有向图G的终点(digraph:out_degree(G, V)是获得有向图G的顶点V的出度)
heads(G) ->
	lists:sort([V || V <- digraph:vertices(G), digraph:out_degree(G, V) =:= 0]).

%% -------------------------------------------------------------------
%% categorise:分类
%% 根据schema_version文件中配置的需要升级的名字组装成对应范围中对应需要升级的名字列表
categorise_by_scope(Version) when is_list(Version) ->
	Categorised =
		[{Scope, Name} || {_App, _Module, Attributes} <-
							  rabbit_misc:all_module_attributes(rabbit_upgrade),
						  {Name, Scope, _Requires} <- Attributes,
						  lists:member(Name, Version)],
	%% 得到根据不用的分类得到对应的名字列表
	orddict:to_list(
	  lists:foldl(fun ({Scope, Name}, CatVersion) ->
						   rabbit_misc:orddict_cons(Scope, Name, CatVersion)
				  end, orddict:new(), Categorised)).


dir() -> rabbit_mnesia:dir().


%% 得到mnesia数据库目录下的schema_version文件的绝对路径
schema_filename() -> filename:join(dir(), ?VERSION_FILENAME).
