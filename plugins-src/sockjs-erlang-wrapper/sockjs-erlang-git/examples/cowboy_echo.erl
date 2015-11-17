#!/usr/bin/env escript
%%! -smp disable +A1 +K true -pa ebin deps/cowboy/ebin -input
-module(cowboy_echo).
-mode(compile).

-export([main/1]).

%% Cowboy callbacks
-export([init/3, handle/2, terminate/2]).


main(_) ->
    Port = 8081,
    application:start(sockjs),
    application:start(cowboy),

    SockjsState = sockjs_handler:init_state(
                    <<"/echo">>, fun service_echo/3, state, []),

    VhostRoutes = [{[<<"echo">>, '...'], sockjs_cowboy_handler, SockjsState},
                   {'_', ?MODULE, []}],
    Routes = [{'_',  VhostRoutes}], % any vhost

    io:format(" [*] Running at http://localhost:~p~n", [Port]),
    cowboy:start_listener(http, 100,
                          cowboy_tcp_transport, [{port,     Port}],
                          cowboy_http_protocol, [{dispatch, Routes}]),
    receive
        _ -> ok
    end.

%% --------------------------------------------------------------------------

init({_Any, http}, Req, []) ->
    {ok, Req, []}.

handle(Req, State) ->
    {ok, Data} = file:read_file("./examples/echo.html"),
    {ok, Req1} = cowboy_http_req:reply(200, [{<<"Content-Type">>, "text/html"}],
                                       Data, Req),
    {ok, Req1, State}.

terminate(_Req, _State) ->
    ok.

%% --------------------------------------------------------------------------

service_echo(_Conn, init, state)        -> {ok, state};
service_echo(Conn, {recv, Data}, state) -> Conn:send(Data);
service_echo(_Conn, closed, state)      -> {ok, state}.
