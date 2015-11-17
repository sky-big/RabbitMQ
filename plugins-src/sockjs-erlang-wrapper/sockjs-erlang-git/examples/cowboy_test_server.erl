#!/usr/bin/env escript
%%! -smp disable +A1 +K true -pa ebin deps/cowboy/ebin -input
-module(cowboy_test_server).
-mode(compile).

-export([main/1]).

%% Cowboy callbacks
-export([init/3, handle/2, terminate/2]).


main(_) ->
    Port = 8081,
    application:start(sockjs),
    application:start(cowboy),

    StateEcho = sockjs_handler:init_state(
                  <<"/echo">>, fun service_echo/3, state,
                  [{response_limit, 4096}]),
    StateClose = sockjs_handler:init_state(
                   <<"/close">>, fun service_close/3, state, []),
    StateAmplify = sockjs_handler:init_state(
                     <<"/amplify">>, fun service_amplify/3, state, []),
    StateBroadcast = sockjs_handler:init_state(
                       <<"/broadcast">>, fun service_broadcast/3, state, []),
    StateDWSEcho = sockjs_handler:init_state(
                  <<"/disabled_websocket_echo">>, fun service_echo/3, state,
                     [{websocket, false}]),
    StateCNEcho = sockjs_handler:init_state(
                    <<"/cookie_needed_echo">>, fun service_echo/3, state,
                    [{cookie_needed, true}]),

    VRoutes = [{[<<"echo">>, '...'], sockjs_cowboy_handler, StateEcho},
               {[<<"close">>, '...'], sockjs_cowboy_handler, StateClose},
               {[<<"amplify">>, '...'], sockjs_cowboy_handler, StateAmplify},
               {[<<"broadcast">>, '...'], sockjs_cowboy_handler, StateBroadcast},
               {[<<"disabled_websocket_echo">>, '...'], sockjs_cowboy_handler,
                StateDWSEcho},
               {[<<"cookie_needed_echo">>, '...'], sockjs_cowboy_handler,
                StateCNEcho},
               {'_', ?MODULE, []}],
    Routes = [{'_',  VRoutes}], % any vhost

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
    {ok, Req2} = cowboy_http_req:reply(404, [],
                 <<"404 - Nothing here (via sockjs-erlang fallback)\n">>, Req),
    {ok, Req2, State}.

terminate(_Req, _State) ->
    ok.

%% --------------------------------------------------------------------------

service_echo(_Conn, init, state)        -> {ok, state};
service_echo(Conn, {recv, Data}, state) -> Conn:send(Data);
service_echo(_Conn, closed, state)      -> {ok, state}.

service_close(Conn, _, _State) ->
    Conn:close(3000, "Go away!").

service_amplify(Conn, {recv, Data}, _State) ->
    N0 = list_to_integer(binary_to_list(Data)),
    N = if N0 > 0 andalso N0 < 19 -> N0;
           true                   -> 1
        end,
    Conn:send(list_to_binary(
                  string:copies("x", round(math:pow(2, N)))));
service_amplify(_Conn, _, _State) ->
    ok.

service_broadcast(Conn, init, _State) ->
    case ets:info(broadcast_table, memory) of
        undefined ->
            ets:new(broadcast_table, [public, named_table]);
        _Any ->
            ok
    end,
    true = ets:insert(broadcast_table, {Conn}),
    ok;
service_broadcast(Conn, closed, _State) ->
    true = ets:delete_object(broadcast_table, {Conn}),
    ok;
service_broadcast(_Conn, {recv, Data}, _State) ->
    ets:foldl(fun({Conn1}, _Acc) -> Conn1:send(Data) end,
              [], broadcast_table),
    ok.
