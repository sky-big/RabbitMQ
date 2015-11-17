-module(sockjs_http).

-export([path/1, method/1, body/1, body_qs/1, header/2, jsessionid/1,
         callback/1, peername/1, sockname/1]).
-export([reply/4, chunk_start/3, chunk/2, chunk_end/1]).
-export([hook_tcp_close/1, unhook_tcp_close/1, abruptly_kill/1]).
-include("sockjs_internal.hrl").

%% --------------------------------------------------------------------------

%% -spec path(req()) -> {string(), req()}.
path({cowboy, Req})       -> {Path, Req1} = cowboy_http_req:raw_path(Req),
                             {binary_to_list(Path), {cowboy, Req1}}.

%% -spec method(req()) -> {atom(), req()}.
method({cowboy, Req})       -> {Method, Req1} = cowboy_http_req:method(Req),
                               case is_binary(Method) of
                                   true  -> {list_to_atom(binary_to_list(Method)), {cowboy, Req1}};
                                   false -> {Method, {cowboy, Req1}}
                               end.

%% -spec body(req()) -> {binary(), req()}.
body({cowboy, Req})       -> {ok, Body, Req1} = cowboy_http_req:body(Req),
                             {Body, {cowboy, Req1}}.

%% -spec body_qs(req()) -> {binary(), req()}.
body_qs(Req) ->
    {H, Req1} =  header('Content-Type', Req),
    case H of
        H when H =:= "text/plain" orelse H =:= "" ->
            body(Req1);
        _ ->
            %% By default assume application/x-www-form-urlencoded
            body_qs2(Req1)
    end.
body_qs2({cowboy, Req}) ->
    {BodyQS, Req1} = cowboy_http_req:body_qs(Req),
    case proplists:get_value(<<"d">>, BodyQS) of
        undefined ->
            {<<>>, {cowboy, Req1}};
        V ->
            {V, {cowboy, Req1}}
    end.

%% -spec header(atom(), req()) -> {nonempty_string() | undefined, req()}.
header(K, {cowboy, Req})->
    {H, Req2} = cowboy_http_req:header(K, Req),
    {V, Req3} = case H of
                    undefined ->
                        cowboy_http_req:header(list_to_binary(atom_to_list(K)), Req2);
                    _ -> {H, Req2}
                end,
    case V of
        undefined -> {undefined, {cowboy, Req3}};
        _         -> {binary_to_list(V), {cowboy, Req3}}
    end.

%% -spec jsessionid(req()) -> {nonempty_string() | undefined, req()}.
jsessionid({cowboy, Req}) ->
    {C, Req2} = cowboy_http_req:cookie(<<"JSESSIONID">>, Req),
    case C of
        _ when is_binary(C) ->
            {binary_to_list(C), {cowboy, Req2}};
        undefined ->
            {undefined, {cowboy, Req2}}
    end.

%% -spec callback(req()) -> {nonempty_string() | undefined, req()}.
callback({cowboy, Req}) ->
    {CB, Req1} = cowboy_http_req:qs_val(<<"c">>, Req),
    case CB of
        undefined -> {undefined, {cowboy, Req1}};
        _         -> {binary_to_list(CB), {cowboy, Req1}}
    end.

%% -spec peername(req()) -> {{inet:ip_address(), non_neg_integer()}, req()}.
peername({cowboy, Req}) ->
    {P, Req1} = cowboy_http_req:peer(Req),
    {P, {cowboy, Req1}}.

%% -spec sockname(req()) -> {{inet:ip_address(), non_neg_integer()}, req()}.
sockname({cowboy, Req} = R) ->
    {ok, _T, S} = cowboy_http_req:transport(Req),
    %% Cowboy has peername(), but doesn't have sockname() equivalent.
    {ok, Addr} = case S of
                     _ when is_port(S) ->
                         inet:sockname(S);
                     _ ->
                         {ok, {{0,0,0,0}, 0}}
                 end,
    {Addr, R}.

%% --------------------------------------------------------------------------

%% -spec reply(non_neg_integer(), headers(), iodata(), req()) -> req().
reply(Code, Headers, Body, {cowboy, Req}) ->
    Body1 = iolist_to_binary(Body),
    {ok, Req1} = cowboy_http_req:reply(Code, enbinary(Headers), Body1, Req),
    {cowboy, Req1}.

%% -spec chunk_start(non_neg_integer(), headers(), req()) -> req().
chunk_start(Code, Headers, {cowboy, Req}) ->
    {ok, Req1} = cowboy_http_req:chunked_reply(Code, enbinary(Headers), Req),
    {cowboy, Req1}.

%% -spec chunk(iodata(), req()) -> {ok | error, req()}.
chunk(Chunk, {cowboy, Req} = R) ->
    case cowboy_http_req:chunk(Chunk, Req) of
        ok          -> {ok, R};
        {error, _E} -> {error, R}
                      %% This shouldn't happen too often, usually we
                      %% should catch tco socket closure before.
    end.

%% -spec chunk_end(req()) -> req().
chunk_end({cowboy, _Req} = R)  -> R.

enbinary(L) -> [{list_to_binary(K), list_to_binary(V)} || {K, V} <- L].


%% -spec hook_tcp_close(req()) -> req().
hook_tcp_close(R = {cowboy, Req}) ->
    {ok, T, S} = cowboy_http_req:transport(Req),
    T:setopts(S,[{active,once}]),
    R.

%% -spec unhook_tcp_close(req()) -> req().
unhook_tcp_close(R = {cowboy, Req}) ->
    {ok, T, S} = cowboy_http_req:transport(Req),
    T:setopts(S,[{active,false}]),
    R.

%% -spec abruptly_kill(req()) -> req().
abruptly_kill(R = {cowboy, Req}) ->
    {ok, T, S} = cowboy_http_req:transport(Req),
    T:close(S),
    R.
