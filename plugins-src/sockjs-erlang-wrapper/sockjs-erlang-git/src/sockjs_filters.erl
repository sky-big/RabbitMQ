-module(sockjs_filters).

-include("sockjs_internal.hrl").

-export([cache_for/2, h_sid/2, h_no_cache/2, xhr_cors/2,
         xhr_options_post/2, xhr_options_get/2]).

-define(YEAR, 365 * 24 * 60 * 60).

%% --------------------------------------------------------------------------

%% -spec cache_for(req(), headers()) -> {headers(), req()}.
cache_for(Req, Headers) ->
    Expires = calendar:gregorian_seconds_to_datetime(
                calendar:datetime_to_gregorian_seconds(
                  calendar:now_to_datetime(now())) + ?YEAR),
    H = [{"Cache-Control", "public, max-age=" ++ integer_to_list(?YEAR)},
         {"Expires",       httpd_util:rfc1123_date(Expires)}],
    {H ++ Headers, Req}.

%% -spec h_sid(req(), headers()) -> {headers(), req()}.
h_sid(Req, Headers) ->
    %% Some load balancers do sticky sessions, but only if there is
    %% a JSESSIONID cookie. If this cookie isn't yet set, we shall
    %% set it to a dumb value. It doesn't really matter what, as
    %% session information is usually added by the load balancer.
    {C, Req2} = sockjs_http:jsessionid(Req),
    H = case C of
        undefined -> [{"Set-Cookie", "JSESSIONID=dummy; path=/"}];
        Jsid      -> [{"Set-Cookie", "JSESSIONID=" ++ Jsid ++ "; path=/"}]
    end,
    {H ++ Headers, Req2}.

%% -spec h_no_cache(req(), headers()) -> {headers(), req()}.
h_no_cache(Req, Headers) ->
    H = [{"Cache-Control", "no-store, no-cache, must-revalidate, max-age=0"}],
    {H ++ Headers, Req}.

%% -spec xhr_cors(req(), headers()) -> {headers(), req()}.
xhr_cors(Req, Headers) ->
    {OriginH, Req1} = sockjs_http:header('Origin', Req),
     Origin = case OriginH of
                  "null"    -> "*";
                  undefined -> "*";
                  O         -> O
              end,
    {HeadersH, Req2} = sockjs_http:header(
                             'Access-Control-Request-Headers', Req1),
    AllowHeaders = case HeadersH of
                       undefined -> [];
                       V         -> [{"Access-Control-Allow-Headers", V}]
                   end,
    H = [{"Access-Control-Allow-Origin",      Origin},
         {"Access-Control-Allow-Credentials", "true"}],
    {H ++ AllowHeaders ++ Headers, Req2}.

%% -spec xhr_options_post(req(), headers()) -> {headers(), req()}.
xhr_options_post(Req, Headers) ->
    xhr_options(Req, Headers, ["OPTIONS", "POST"]).

%% -spec xhr_options_get(req(), headers()) -> {headers(), req()}.
xhr_options_get(Req, Headers) ->
    xhr_options(Req, Headers, ["OPTIONS", "GET"]).

%% -spec xhr_options(req(), headers(), list(string())) -> {headers(), req()}.
xhr_options(Req, Headers, Methods) ->
    H = [{"Access-Control-Allow-Methods", string:join(Methods, ", ")},
         {"Access-Control-Max-Age", integer_to_list(?YEAR)}],
    {H ++ Headers, Req}.
