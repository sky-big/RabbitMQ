-export([ping/2]).

-include_lib("wm_reqdata.hrl").

ping(ReqData, State) ->
    {pong, ReqData, State}.


