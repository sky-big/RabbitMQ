-module(sockjs_cowboy_handler).
-behaviour(cowboy_http_handler).
-behaviour(cowboy_http_websocket_handler).

%% Cowboy http callbacks
-export([init/3, handle/2, terminate/2]).

%% Cowboy ws callbacks
-export([websocket_init/3, websocket_handle/3,
         websocket_info/3, websocket_terminate/3]).

-include("sockjs_internal.hrl").

%% --------------------------------------------------------------------------

init({_Any, http}, Req, Service) ->
    case sockjs_handler:is_valid_ws(Service, {cowboy, Req}) of
        {true, {cowboy, _Req1}, _Reason} ->
            {upgrade, protocol, cowboy_http_websocket};
        {false, {cowboy, Req1}, _Reason} ->
            {ok, Req1, Service}
    end.

handle(Req, Service) ->
    {cowboy, Req3} = sockjs_handler:handle_req(Service, {cowboy, Req}),
    {ok, Req3, Service}.

terminate(_Req, _Service) ->
    ok.

%% --------------------------------------------------------------------------

websocket_init(_TransportName, Req,
               Service = #service{logger        = Logger,
                                  subproto_pref = SubProtocolPref}) ->
    Req3 = case cowboy_http_req:header(<<"Sec-Websocket-Protocol">>, Req) of
               {undefined, Req1} ->
                   Req1;
               {SubProtocols, Req1} ->
                   SelectedSubProtocol =
                     choose_subprotocol_bin(SubProtocols, SubProtocolPref),
                   {ok, Req2} = cowboy_http_req:set_resp_header(
                                  <<"Sec-Websocket-Protocol">>,
                                  SelectedSubProtocol, Req1),
                   Req2
           end,

    Req4 = Logger(Service, {cowboy, Req3}, websocket),

    Service1 = Service#service{disconnect_delay = 5*60*1000},

    {Info, Req5} = sockjs_handler:extract_info(Req4),
    SessionPid = sockjs_session:maybe_create(undefined, Service1, Info),
    {RawWebsocket, {cowboy, Req7}} =
        case sockjs_handler:get_action(Service, Req5) of
            {{match, WS}, Req6} when WS =:= websocket orelse
                                     WS =:= rawwebsocket ->
                {WS, Req6}
        end,
    self() ! go,
    {ok, Req7, {RawWebsocket, SessionPid}}.

websocket_handle({text, Data}, Req, {RawWebsocket, SessionPid} = S) ->
    case sockjs_ws_handler:received(RawWebsocket, SessionPid, Data) of
        ok       -> {ok, Req, S};
        shutdown -> {shutdown, Req, S}
    end;
websocket_handle(_Unknown, Req, S) ->
    {shutdown, Req, S}.

websocket_info(go, Req, {RawWebsocket, SessionPid} = S) ->
    case sockjs_ws_handler:reply(RawWebsocket, SessionPid) of
        wait          -> {ok, Req, S};
        {ok, Data}    -> self() ! go,
                         {reply, {text, Data}, Req, S};
        {close, <<>>} -> {shutdown, Req, S};
        {close, Data} -> self() ! shutdown,
                         {reply, {text, Data}, Req, S}
    end;
websocket_info(shutdown, Req, S) ->
    {shutdown, Req, S}.

websocket_terminate(_Reason, _Req, {RawWebsocket, SessionPid}) ->
    sockjs_ws_handler:close(RawWebsocket, SessionPid),
    ok.

%% --------------------------------------------------------------------------

choose_subprotocol_bin(SubProtocols, Pref) ->
    choose_subprotocol(re:split(SubProtocols, ", *"), Pref).
choose_subprotocol(SubProtocols, undefined) ->
    erlang:hd(lists:reverse(lists:sort(SubProtocols)));
choose_subprotocol(SubProtocols, Pref) ->
    case lists:filter(fun (E) -> lists:member(E, SubProtocols) end, Pref) of
        [Hd | _] -> Hd;
        []       -> choose_subprotocol(SubProtocols, undefined)
    end.
