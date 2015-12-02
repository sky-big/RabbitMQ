%% Author: xxw
%% Created: 2014-3-11
%% Description: TODO: Add description to str_util
-module(str_util).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([
		 sprintf/2,
		 make_node/2,
		 replace/3,
		 datetime_to_short_string/1,
		 string_to_term/1,
		 term_to_string/1
		 ]).

%%
%% API Functions
%%
sprintf(Format, Data) ->
	lists:flatten(io_lib:format(Format, Data)).

%% 制作node节点的名字
make_node(NodeName, Ip) when erlang:is_list(NodeName) and erlang:is_list(Ip) ->
	erlang:list_to_atom(NodeName ++ "@" ++ Ip);
make_node(NodeName, Ip) when erlang:is_atom(NodeName) and erlang:is_atom(Ip) ->
	make_node(erlang:atom_to_list(NodeName), Ip);
make_node(NodeName, Ip) when erlang:is_list(NodeName) and erlang:is_atom(Ip) ->
	make_node(NodeName, erlang:atom_to_list(Ip));
make_node(NodeName, Ip) when erlang:is_atom(NodeName) and erlang:is_atom(Ip) ->
	make_node(erlang:atom_to_list(NodeName), erlang:atom_to_list(Ip));
make_node(_, _) ->
	''.
 
token_str(String, TokenStr) ->
	Len = string:len(TokenStr),
	case string:str(String, TokenStr) of
		0 ->
			[String];
		1 ->
			LeftString = string:sub_string(String, Len + 1),
			token_str(LeftString, TokenStr);
		Index ->
			FirstString = string:sub_string(String, 1, Index - 1),
			LeftString = string:sub_string(String, Index + Len),
			[FirstString | token_str(LeftString, TokenStr)]
	end.

replace(String, TokenStr, NewString) ->
	SpligStrings = token_str(String, TokenStr),
	string:join(SpligStrings, NewString).

datetime_to_short_string(DateTime) ->
	{{Year, Mon, Day}, {Hour, Minute, Second}} = DateTime,
	sprintf("~p~2..0w~2..0w~2..0w~2..0w~2..0w", [Year, Mon, Day, Hour, Minute, Second]).

string_to_term(String) ->
	case erl_scan:string(String ++ ".") of
		{ok, Tokens, _} ->
			case erl_parse:parse_term(Tokens) of
				{ok, Term} ->
					{ok, Term};
				Reason ->
					io:format("<<<<<<<<<<<<<<<<<<<<<< string_to_term error :~p~n", [{Reason, erlang:get_stacktrace()}])
			end;
		Reason ->
			io:format("<<<<<<<<<<<<<<<<<<<<<< string_to_term error :~p~n", [{Reason, erlang:get_stacktrace()}])
	end.

term_to_string(Term) ->
	lists:flatten(io_lib:format("~w", [Term])).

%%
%% Local Functions
%%

