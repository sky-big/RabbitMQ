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
%% Copyright (c) 2011-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(pmon).

%% Process Monitor(进程监控)
%% ================
%%
%% This module monitors processes so that every process has at most
%% 1 monitor.
%% Processes monitored can be dynamically(动态) added and removed.
%% 该模块监控进程,是每一个进程最多有一个监控者
%%
%% Unlike erlang:[de]monitor* functions, this module
%% provides basic querying capability(能力) and avoids contacting down nodes.
%%
%% It is used to monitor nodes, queue mirrors, and by
%% the queue collector, among other things.

-export([new/0, new/1, monitor/2, monitor_all/2, demonitor/2,
         is_monitored/2, erase/2, monitored/1, is_empty/1]).

-compile({no_auto_import, [monitor/2]}).

-record(state, {dict, module}).

-ifdef(use_specs).

%%----------------------------------------------------------------------------

-export_type([?MODULE/0]).

-opaque(?MODULE() :: #state{dict   :: dict:dict(),
                            module :: atom()}).

-type(item()         :: pid() | {atom(), node()}).

-spec(new/0          :: () -> ?MODULE()).
-spec(new/1          :: ('erlang' | 'delegate') -> ?MODULE()).
-spec(monitor/2      :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitor_all/2  :: ([item()], ?MODULE()) -> ?MODULE()).
-spec(demonitor/2    :: (item(), ?MODULE()) -> ?MODULE()).
-spec(is_monitored/2 :: (item(), ?MODULE()) -> boolean()).
-spec(erase/2        :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitored/1    :: (?MODULE()) -> [item()]).
-spec(is_empty/1     :: (?MODULE()) -> boolean()).

-endif.

%% 默认创建erlang作为监视的模块
new() -> new(erlang).


new(Module) -> #state{dict   = dict:new(),
					  module = Module}.


%% 通过Module模块监视Item进程
monitor(Item, S = #state{dict = M, module = Module}) ->
	case dict:is_key(Item, M) of
		true  -> S;
		false -> case node_alive_shortcut(Item) of
					 true  -> Ref = Module:monitor(process, Item),
							  S#state{dict = dict:store(Item, Ref, M)};
					 false -> self() ! {'DOWN', fake_ref, process, Item,
										nodedown},
							  S
				 end
	end.


%% 监听进程列表
monitor_all([],     S) -> S;                %% optimisation

monitor_all([Item], S) -> monitor(Item, S); %% optimisation

monitor_all(Items,  S) -> lists:foldl(fun monitor/2, S, Items).


demonitor(Item, S = #state{dict = M, module = Module}) ->
	case dict:find(Item, M) of
		{ok, MRef} -> Module:demonitor(MRef),
					  S#state{dict = dict:erase(Item, M)};
		error      -> M
	end.


is_monitored(Item, #state{dict = M}) -> dict:is_key(Item, M).


erase(Item, S = #state{dict = M}) -> S#state{dict = dict:erase(Item, M)}.


monitored(#state{dict = M}) -> dict:fetch_keys(M).


is_empty(#state{dict = M}) -> dict:size(M) == 0.

%%----------------------------------------------------------------------------

%% We check here to see if the node is alive in order to avoid trying
%% to connect to it if it isn't - this can cause substantial(大量)
%% slowdowns(减速). We can't perform(实行) this shortcut(捷径) if passed {Name, Node}
%% since we would need to convert that into a pid for the fake 'DOWN'
%% message, so we always return true here - but that's OK, it's just
%% an optimisation.
%% 我们在这里检查，看看是否节点是活着为了避免尝试连接到它，如果它不是 - 这可能会导致速度显着降低。
%% 我们不能执行此快捷方式，如果通过{名，节点}因为我们需要将其转换成一个PID为假'DOWN'的消息，所以我们一直在这里返回true - 不过没关系，这只是一种优化。
node_alive_shortcut(P) when is_pid(P) ->
	lists:member(node(P), [node() | nodes()]);

node_alive_shortcut({_Name, _Node}) ->
	true.
