-compile({parse_transform,pmod_pt}).

-module(sockjs_multiplex_channel, [Conn, Topic]).

-export([send/1, close/0, close/2, info/0]).

send(Data) ->
    Conn:send(iolist_to_binary(["msg", ",", Topic, ",", Data])).

close() ->
    close(1000, "Normal closure").

close(_Code, _Reason) ->
    Conn:send(iolist_to_binary(["uns", ",", Topic])).

info() ->
    Conn:info() ++ [{topic, Topic}].

