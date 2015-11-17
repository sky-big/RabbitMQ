-module(sockjs_session_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/3]).
-export([init/1]).

%% --------------------------------------------------------------------------

%% -spec start_link() -> ignore | {'ok', pid()} | {'error', any()}.
start_link() ->
     supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{undefined, {sockjs_session, start_link, []},
            transient, 5000, worker, [sockjs_session]}]}}.

start_child(SessionId, Service, Info) ->
   supervisor:start_child(?MODULE, [SessionId, Service, Info]).
