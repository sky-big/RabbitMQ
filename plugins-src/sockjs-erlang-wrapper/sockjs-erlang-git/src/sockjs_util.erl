-module(sockjs_util).

-export([rand32/0]).
-export([encode_frame/1]).
-export([url_escape/2]).

-include("sockjs_internal.hrl").

%% --------------------------------------------------------------------------

%% -spec rand32() -> non_neg_integer().
rand32() ->
    case get(random_seeded) of
        undefined ->
            {MegaSecs, Secs, MicroSecs} = now(),
            _ = random:seed(MegaSecs, Secs, MicroSecs),
            put(random_seeded, true);
        _Else ->
            ok
    end,
    random:uniform(erlang:trunc(math:pow(2,32)))-1.


%% -spec encode_frame(frame()) -> iodata().
encode_frame({open, nil}) ->
    <<"o">>;
encode_frame({close, {Code, Reason}}) ->
    [<<"c">>,
     sockjs_json:encode([Code, list_to_binary(Reason)])];
encode_frame({data, L}) ->
    [<<"a">>,
     sockjs_json:encode([iolist_to_binary(D) || D <- L])];
encode_frame({heartbeat, nil}) ->
    <<"h">>.


%% -spec url_escape(string(), string()) -> iolist().
url_escape(Str, Chars) ->
    [case lists:member(Char, Chars) of
         true  -> hex(Char);
         false -> Char
     end || Char <- Str].

hex(C) ->
    <<High0:4, Low0:4>> = <<C>>,
    High = integer_to_list(High0),
    Low = integer_to_list(Low0),
    "%" ++ High ++ Low.
