%% Author: xxw
%% Created: 2014-3-12
%% Description: TODO: Add description to node_util
-module(node_util).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([
		 get_node_name/1,
		 get_line_mnesia_db_nodes/0,
		 get_all_nodes/0,
		 check_snode_match/2,
		 get_run_apps/1
		 ]).

-export([
		 get_cache_node/0,
		 get_cache_nodes/0,
		 get_line_node/0,
		 get_line_nodes/0,
		 get_db_node/0,
		 get_db_nodes/0,
		 get_gate_nodes/0
		 ]).

%%
%% API Functions
%%
%% 得到节点的名字的前缀
get_node_name(Node) ->
	StrNode = erlang:atom_to_list(Node),
	case string:tokens(StrNode, "@") of
		[NodeName, _Host] ->
			NodeName;
		_ -> 
			[]
	end.


%% 得到节点启动的匹配的app
get_run_apps(Node) ->
	SNodeName = get_node_name(Node),
	NodeSystemOptions = system_option:nodes_option(),
	lists:foldl(fun({App, _Nodes}, Acc) -> 
						case check_snode_match(App, erlang:list_to_atom(SNodeName)) of
							true ->
								[App | Acc];
							false ->
								Acc
						end
						end, [], NodeSystemOptions).


get_line_mnesia_db_nodes() ->
	lists:filter(fun(Node) -> 
						 NodeName = node_util:get_node_name(Node),
						 Index = string:str(NodeName, "line"),
						 case Index >= 1 of
							 true ->
								 true;
							 false ->
								 false
						 end
						 end, environment:get(pre_connect_nodes, [])).


%% 得到所有的联通的节点名
get_all_nodes() ->
	[node() | nodes()] ++ nodes(hidden).


%% 判断SNode节点和AppType能够匹配
check_snode_match(AppType, SNode) ->
	SNodeStr = erlang:atom_to_list(SNode),
	lists:foldl(fun(Node, Acc) -> 
						NodeStr = erlang:atom_to_list(Node),
						case Acc of
							true ->
								Acc;
							_ ->
								Index = string:str(SNodeStr, NodeStr),
								case Index >= 1 of
									true ->
										true;
									false ->
										false
								end
						end
						end, false, system_option:get_node_option(AppType)).


get_cache_node() ->
	case get_cache_nodes() of
		[] ->
			undefined;
		[Node | _] ->
			Node
	end.


get_line_node() ->
	case get_line_nodes() of
		[] ->
			undefined;
		[Node | _] ->
			Node
	end.


get_db_node() ->
	case get_db_nodes() of
		[] ->
			undefined;
		[Node | _] ->
			Node
	end.


get_cache_nodes() ->
	lists:filter(fun(Node) ->
						 check_snode_match(cache, Node)
						 end, get_all_nodes()).


get_line_nodes() ->
	lists:filter(fun(Node) ->
						 check_snode_match(line, Node)
						 end, get_all_nodes()).


get_db_nodes() ->
	lists:filter(fun(Node) ->
						 check_snode_match(db, Node)
						 end, get_all_nodes()).


get_gate_nodes() ->
	lists:filter(fun(Node) ->
						 check_snode_match(gate, Node)
						 end, get_all_nodes()).

%%
%% Local Functions
%%

