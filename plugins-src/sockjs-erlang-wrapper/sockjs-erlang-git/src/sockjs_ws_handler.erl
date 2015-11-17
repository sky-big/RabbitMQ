-module(sockjs_ws_handler).

-export([received/3, reply/2, close/2]).

-include("sockjs_internal.hrl").

%% --------------------------------------------------------------------------

%% -spec received(websocket|rawwebsocket, pid(), binary()) -> ok | shutdown.
%% Ignore empty
received(_RawWebsocket, _SessionPid, <<>>) ->
    ok;
received(websocket, SessionPid, Data) ->
    case sockjs_json:decode(Data) of
        {ok, Msg} when is_binary(Msg) ->
            session_received([Msg], SessionPid);
        {ok, Messages} when is_list(Messages) ->
            session_received(Messages, SessionPid);
        _Else ->
            shutdown
    end;

received(rawwebsocket, SessionPid, Data) ->
    session_received([Data], SessionPid).

session_received(Messages, SessionPid) ->
    try sockjs_session:received(Messages, SessionPid) of
        ok         -> ok
    catch
        no_session -> shutdown
    end.

%% -spec reply(websocket|rawwebsocket, pid()) -> {close|open, binary()} | wait.
reply(websocket, SessionPid) ->
    case sockjs_session:reply(SessionPid) of
        {W, Frame} when W =:= ok orelse W =:= close->
            Frame1 = sockjs_util:encode_frame(Frame),
            {W, iolist_to_binary(Frame1)};
        wait ->
            wait
    end;
reply(rawwebsocket, SessionPid) ->
    case sockjs_session:reply(SessionPid, false) of
        {W, Frame} when W =:= ok orelse W =:= close->
            case Frame of
                {open, nil}               -> reply(rawwebsocket, SessionPid);
                {close, {_Code, _Reason}} -> {close, <<>>};
                {data, [Msg]}             -> {ok, iolist_to_binary(Msg)};
                {heartbeat, nil}          -> reply(rawwebsocket, SessionPid)
            end;
        wait ->
            wait
    end.

%% -spec close(websocket|rawwebsocket, pid()) -> ok.
close(_RawWebsocket, SessionPid) ->
    SessionPid ! force_shutdown,
    ok.
