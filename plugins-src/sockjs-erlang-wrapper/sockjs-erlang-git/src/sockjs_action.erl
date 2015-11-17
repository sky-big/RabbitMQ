-module(sockjs_action).

% none
-export([welcome_screen/3, options/3, iframe/3, info_test/3]).
% send
-export([xhr_polling/4, xhr_streaming/4, eventsource/4, htmlfile/4, jsonp/4]).
% recv
-export([xhr_send/4, jsonp_send/4]).
% misc
-export([websocket/3, rawwebsocket/3]).

-include("sockjs_internal.hrl").

%% --------------------------------------------------------------------------

-define(IFRAME, "<!DOCTYPE html>
<html>
<head>
  <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\" />
  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\" />
  <script>
    document.domain = document.domain;
    _sockjs_onload = function(){SockJS.bootstrap_iframe();};
  </script>
  <script src=\"~s\"></script>
</head>
<body>
  <h2>Don't panic!</h2>
  <p>This is a SockJS hidden iframe. It's used for cross domain magic.</p>
</body>
</html>").

-define(IFRAME_HTMLFILE, "<!doctype html>
<html><head>
  <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\" />
  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\" />
</head><body><h2>Don't panic!</h2>
  <script>
    document.domain = document.domain;
    var c = parent.~s;
    c.start();
    function p(d) {c.message(d);};
    window.onload = function() {c.stop();};
  </script>").

%% --------------------------------------------------------------------------

%% -spec welcome_screen(req(), headers(), service()) -> req().
welcome_screen(Req, Headers, _Service) ->
    H = [{"Content-Type", "text/plain; charset=UTF-8"}],
    sockjs_http:reply(200, H ++ Headers,
          "Welcome to SockJS!\n", Req).

%% -spec options(req(), headers(), service()) -> req().
options(Req, Headers, _Service) ->
    sockjs_http:reply(204, Headers, "", Req).

%% -spec iframe(req(), headers(), service()) -> req().
iframe(Req, Headers, #service{sockjs_url = SockjsUrl}) ->
    IFrame = io_lib:format(?IFRAME, [SockjsUrl]),
    MD5 = "\"" ++ binary_to_list(base64:encode(erlang:md5(IFrame))) ++ "\"",
    {H, Req2} = sockjs_http:header('If-None-Match', Req),
    case H of
        MD5 -> sockjs_http:reply(304, Headers, "", Req2);
        _   -> sockjs_http:reply(
                 200, [{"Content-Type", "text/html; charset=UTF-8"},
                       {"ETag",         MD5}] ++ Headers, IFrame, Req2)
    end.


%% -spec info_test(req(), headers(), service()) -> req().
info_test(Req, Headers, #service{websocket = Websocket,
                                 cookie_needed = CookieNeeded}) ->
    I = [{websocket, Websocket},
         {cookie_needed, CookieNeeded},
         {origins, [<<"*:*">>]},
         {entropy, sockjs_util:rand32()}],
    D = sockjs_json:encode({I}),
    H = [{"Content-Type", "application/json; charset=UTF-8"}],
    sockjs_http:reply(200, H ++ Headers, D, Req).

%% --------------------------------------------------------------------------

%% -spec xhr_polling(req(), headers(), service(), session()) -> req().
xhr_polling(Req, Headers, Service, Session) ->
    Req1 = chunk_start(Req, Headers),
    reply_loop(Req1, Session, 1, fun fmt_xhr/1, Service).

%% -spec xhr_streaming(req(), headers(), service(), session()) -> req().
xhr_streaming(Req, Headers, Service = #service{response_limit = ResponseLimit},
              Session) ->
    Req1 = chunk_start(Req, Headers),
    %% IE requires 2KB prefix:
    %% http://blogs.msdn.com/b/ieinternals/archive/2010/04/06/comet-streaming-in-internet-explorer-with-xmlhttprequest-and-xdomainrequest.aspx
    Req2 = chunk(Req1, list_to_binary(string:copies("h", 2048)),
                 fun fmt_xhr/1),
    reply_loop(Req2, Session, ResponseLimit, fun fmt_xhr/1, Service).

%% -spec eventsource(req(), headers(), service(), session()) -> req().
eventsource(Req, Headers, Service = #service{response_limit = ResponseLimit},
            SessionId) ->
    Req1 = chunk_start(Req, Headers, "text/event-stream; charset=UTF-8"),
    Req2 = chunk(Req1, <<$\r, $\n>>),
    reply_loop(Req2, SessionId, ResponseLimit, fun fmt_eventsource/1, Service).


%% -spec htmlfile(req(), headers(), service(), session()) -> req().
htmlfile(Req, Headers, Service = #service{response_limit = ResponseLimit},
         SessionId) ->
    S = fun (Req1, CB) ->
                Req2 = chunk_start(Req1, Headers, "text/html; charset=UTF-8"),
                IFrame = iolist_to_binary(io_lib:format(?IFRAME_HTMLFILE, [CB])),
                %% Safari needs at least 1024 bytes to parse the
                %% website. Relevant:
                %%   http://code.google.com/p/browsersec/wiki/Part2#Survey_of_content_sniffing_behaviors
                Padding = string:copies(" ", 1024 - size(IFrame)),
                Req3 = chunk(Req2, [IFrame, Padding, <<"\r\n\r\n">>]),
                reply_loop(Req3, SessionId, ResponseLimit, fun fmt_htmlfile/1, Service)
        end,
    verify_callback(Req, S).

%% -spec jsonp(req(), headers(), service(), session()) -> req().
jsonp(Req, Headers, Service, SessionId) ->
    S = fun (Req1, CB) ->
                Req2 = chunk_start(Req1, Headers),
                reply_loop(Req2, SessionId, 1,
                           fun (Body) -> fmt_jsonp(Body, CB) end, Service)
        end,
    verify_callback(Req, S).

verify_callback(Req, Success) ->
    {CB, Req1} = sockjs_http:callback(Req),
    case CB of
        undefined ->
            sockjs_http:reply(500, [], "\"callback\" parameter required", Req1);
        _ ->
            Success(Req1, CB)
    end.

%% --------------------------------------------------------------------------

%% -spec xhr_send(req(), headers(), service(), session()) -> req().
xhr_send(Req, Headers, _Service, Session) ->
    {Body, Req1} = sockjs_http:body(Req),
    case handle_recv(Req1, Body, Session) of
        {error, Req2} ->
            Req2;
        ok ->
            H = [{"content-type", "text/plain; charset=UTF-8"}],
            sockjs_http:reply(204, H ++ Headers, "", Req1)
    end.

%% -spec jsonp_send(req(), headers(), service(), session()) -> req().
jsonp_send(Req, Headers, _Service, Session) ->
    {Body, Req1} = sockjs_http:body_qs(Req),
    case handle_recv(Req1, Body, Session) of
        {error, Req2} ->
            Req2;
        ok ->
            H = [{"content-type", "text/plain; charset=UTF-8"}],
            sockjs_http:reply(200, H ++ Headers, "ok", Req1)
    end.

handle_recv(Req, Body, Session) ->
    case Body of
        _Any when Body =:= <<>> ->
            {error, sockjs_http:reply(500, [], "Payload expected.", Req)};
        _Any ->
            case sockjs_json:decode(Body) of
                {ok, Decoded} when is_list(Decoded)->
                    sockjs_session:received(Decoded, Session),
                    ok;
                {error, _} ->
                    {error, sockjs_http:reply(500, [],
                                              "Broken JSON encoding.", Req)}
            end
    end.

%% --------------------------------------------------------------------------

-define(STILL_OPEN, {2010, "Another connection still open"}).

chunk_start(Req, Headers) ->
    chunk_start(Req, Headers, "application/javascript; charset=UTF-8").
chunk_start(Req, Headers, ContentType) ->
    sockjs_http:chunk_start(200, [{"Content-Type", ContentType}] ++ Headers,
                            Req).

reply_loop(Req, SessionId, ResponseLimit, Fmt, Service) ->
    Req0 = sockjs_http:hook_tcp_close(Req),
    case sockjs_session:reply(SessionId) of
        wait           -> receive
                              %% In Cowboy we need to capture async
                              %% messages from the tcp connection -
                              %% ie: {active, once}.
                              {tcp_closed, _} ->
                                  Req0;
                              %% In Cowboy we may in theory get real
                              %% http requests, this is bad.
                              {tcp, _S, Data} ->
                                  error_logger:error_msg(
                                    "Received unexpected data on a "
                                    "long-polling http connection: ~p. "
                                    "Connection aborted.~n",
                                    [Data]),
                                  Req1 = sockjs_http:abruptly_kill(Req),
                                  Req1;
                              go ->
                                  Req1 = sockjs_http:unhook_tcp_close(Req0),
                                  reply_loop(Req1, SessionId, ResponseLimit,
                                             Fmt, Service)
                          end;
        session_in_use -> Frame = sockjs_util:encode_frame({close, ?STILL_OPEN}),
                          chunk_end(Req0, Frame, Fmt);
        {close, Frame} -> Frame1 = sockjs_util:encode_frame(Frame),
                          chunk_end(Req0, Frame1, Fmt);
        {ok, Frame}    -> Frame1 = sockjs_util:encode_frame(Frame),
                          Frame2 = iolist_to_binary(Frame1),
                          Req2 = chunk(Req0, Frame2, Fmt),
                          reply_loop0(Req2, SessionId,
                                      ResponseLimit - size(Frame2),
                                      Fmt, Service)
    end.

reply_loop0(Req, _SessionId, ResponseLimit, _Fmt, _Service) when ResponseLimit =< 0 ->
    chunk_end(Req);
reply_loop0(Req, SessionId, ResponseLimit, Fmt, Service) ->
    reply_loop(Req, SessionId, ResponseLimit, Fmt, Service).

chunk(Req, Body)      ->
    {_, Req1} = sockjs_http:chunk(Body, Req),
    Req1.
chunk(Req, Body, Fmt) -> chunk(Req, Fmt(Body)).

chunk_end(Req) -> sockjs_http:chunk_end(Req).
chunk_end(Req, Body, Fmt) -> Req1 = chunk(Req, Body, Fmt),
                             chunk_end(Req1).

%% -spec fmt_xhr(iodata()) -> iodata().
fmt_xhr(Body) -> [Body, "\n"].

%% -spec fmt_eventsource(iodata()) -> iodata().
fmt_eventsource(Body) ->
    Escaped = sockjs_util:url_escape(binary_to_list(iolist_to_binary(Body)),
                                     "%\r\n\0"), %% $% must be first!
    [<<"data: ">>, Escaped, <<"\r\n\r\n">>].

%% -spec fmt_htmlfile(iodata()) -> iodata().
fmt_htmlfile(Body) ->
    Double = sockjs_json:encode(iolist_to_binary(Body)),
    [<<"<script>\np(">>, Double, <<");\n</script>\r\n">>].

%% -spec fmt_jsonp(iodata(), iodata()) -> iodata().
fmt_jsonp(Body, Callback) ->
    %% Yes, JSONed twice, there isn't a a better way, we must pass
    %% a string back, and the script, will be evaled() by the
    %% browser.
    [Callback, "(", sockjs_json:encode(iolist_to_binary(Body)), ");\r\n"].

%% --------------------------------------------------------------------------

%% -spec websocket(req(), headers(), service()) -> req().
websocket(Req, Headers, Service) ->
    {_Any, Req1, {R1, R2}} = sockjs_handler:is_valid_ws(Service, Req),
    case {R1, R2} of
        {false, _} ->
            sockjs_http:reply(400, Headers,
                              "Can \"Upgrade\" only to \"WebSocket\".", Req1);
        {_, false} ->
            sockjs_http:reply(400, Headers,
                              "\"Connection\" must be \"Upgrade\"", Req1);
        {true, true} ->
            sockjs_http:reply(400, Headers,
                              "This WebSocket request can't be handled.", Req1)
    end.

%% -spec rawwebsocket(req(), headers(), service()) -> req().
rawwebsocket(Req, Headers, Service) ->
    websocket(Req, Headers, Service).
