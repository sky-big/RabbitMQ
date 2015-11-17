-module(sockjs_service).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     {sockjs_init, 2},
     {sockjs_handle, 3},
     {sockjs_terminate, 2}
    ];

behaviour_info(_Other) ->
    undefined.
