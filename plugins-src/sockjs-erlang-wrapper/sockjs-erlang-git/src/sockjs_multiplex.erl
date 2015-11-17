-module(sockjs_multiplex).

-behaviour(sockjs_service).

-export([init_state/1]).
-export([sockjs_init/2, sockjs_handle/3, sockjs_terminate/2]).

-record(service, {callback, state, vconn}).

%% --------------------------------------------------------------------------

init_state(Services) ->
    L = [{Topic, #service{callback = Callback, state = State}} ||
            {Topic, Callback, State} <- Services],
    {orddict:from_list(L), orddict:new()}.



sockjs_init(_Conn, {_Services, _Channels} = S) ->
    {ok, S}.

sockjs_handle(Conn, Data, {Services, Channels}) ->
    [Type, Topic, Payload] = split($,, binary_to_list(Data), 3),
    case orddict:find(Topic, Services) of
        {ok, Service} ->
            Channels1 = action(Conn, {Type, Topic, Payload}, Service, Channels),
            {ok, {Services, Channels1}};
        _Else ->
            {ok, {Services, Channels}}
    end.

sockjs_terminate(_Conn, {Services, Channels}) ->
    _ = [ {emit(closed, Channel)} ||
            {_Topic, Channel} <- orddict:to_list(Channels) ],
    {ok, {Services, orddict:new()}}.


action(Conn, {Type, Topic, Payload}, Service, Channels) ->
    case {Type, orddict:is_key(Topic, Channels)} of
        {"sub", false} ->
            Channel = Service#service{
                         vconn = sockjs_multiplex_channel:new(
                                   Conn, Topic)
                        },
            orddict:store(Topic, emit(init, Channel), Channels);
        {"uns", true} ->
            Channel = orddict:fetch(Topic, Channels),
            emit(closed, Channel),
            orddict:erase(Topic, Channels);
        {"msg", true} ->
            Channel = orddict:fetch(Topic, Channels),
            orddict:store(Topic, emit({recv, Payload}, Channel), Channels);
        _Else ->
            %% Ignore
            Channels
    end.


emit(What, Channel = #service{callback = Callback,
                              state    = State,
                              vconn    = VConn}) ->
    case Callback(VConn, What, State) of
        {ok, State1} -> Channel#service{state = State1};
        ok           -> Channel
    end.


%% --------------------------------------------------------------------------

split(Char, Str, Limit) ->
    Acc = split(Char, Str, Limit, []),
    lists:reverse(Acc).
split(_Char, _Str, 0, Acc) -> Acc;
split(Char, Str, Limit, Acc) ->
    {L, R} = case string:chr(Str, Char) of
                 0 -> {Str, ""};
                 I -> {string:substr(Str, 1, I-1), string:substr(Str, I+1)}
             end,
    split(Char, R, Limit-1, [L | Acc]).
