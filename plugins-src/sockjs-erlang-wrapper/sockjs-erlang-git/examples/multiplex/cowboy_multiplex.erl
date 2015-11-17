#!/usr/bin/env escript
%%! -smp disable +A1 +K true -pa ebin deps/cowboy/ebin -input
-module(cowboy_multiplex).
-mode(compile).

-export([main/1]).

%% Cowboy callbacks
-export([init/3, handle/2, terminate/2]).

main(_) ->
    Port = 8081,
    application:start(sockjs),
    application:start(cowboy),

    MultiplexState = sockjs_multiplex:init_state(
                       [{"ann",  fun service_ann/3,  []},
                        {"bob",  fun service_bob/3,  []},
                        {"carl", fun service_carl/3, []}]),

    SockjsState = sockjs_handler:init_state(
                    <<"/multiplex">>, sockjs_multiplex, MultiplexState, []),

    VhostRoutes = [{[<<"multiplex">>, '...'], sockjs_cowboy_handler, SockjsState},
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
    {Path, Req1} = cowboy_http_req:path(Req),
    {ok, Req2} = case Path of
                     [<<"multiplex.js">>] ->
                         {ok, Data} = file:read_file("./examples/multiplex/multiplex.js"),
                         cowboy_http_req:reply(200, [{<<"Content-Type">>, "application/javascript"}],
                                               Data, Req1);
                     [] ->
                         {ok, Data} = file:read_file("./examples/multiplex/index.html"),
                         cowboy_http_req:reply(200, [{<<"Content-Type">>, "text/html"}],
                                               Data, Req1);
                     _ ->
                         cowboy_http_req:reply(404, [],
                                               <<"404 - Nothing here\n">>, Req1)
                 end,
    {ok, Req2, State}.

terminate(_Req, _State) ->
    ok.

%% --------------------------------------------------------------------------

service_ann(Conn, init, State) ->
    Conn:send("Ann says hi!"),
    {ok, State};
service_ann(Conn, {recv, Data}, State) ->
    Conn:send(["Ann nods: ", Data]),
    {ok, State};
service_ann(_Conn, closed, State) ->
    {ok, State}.

service_bob(Conn, init, State) ->
    Conn:send("Bob doesn't agree."),
    {ok, State};
service_bob(Conn, {recv, Data}, State) ->
    Conn:send(["Bob says no to: ", Data]),
    {ok, State};
service_bob(_Conn, closed, State) ->
    {ok, State}.

service_carl(Conn, init, State) ->
    Conn:send("Carl says goodbye!"),
    Conn:close(),
    {ok, State};
service_carl(_Conn, _, State) ->
    {ok, State}.
