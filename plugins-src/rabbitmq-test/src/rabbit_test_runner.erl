%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_test_runner).

-include_lib("kernel/include/file.hrl").

-define(TIMEOUT, 600).

-import(rabbit_misc, [pget/2]).

-export([run_in_broker/2, run_multi/5]).

run_in_broker(Dir, Filter) ->
    add_server_test_ebin_dir(),
    io:format("~nIn-broker tests~n================~n~n", []),
    eunit:test(make_tests_single(Dir, Filter, ?TIMEOUT), []).

run_multi(ServerDir, Dir, Filter, Cover, PluginsDir) ->
    io:format("~nMulti-node tests~n================~n~n", []),
    %% Umbrella does not give us -sname
    net_kernel:start([?MODULE, shortnames]),
    inets:start(), %% Used by HTTP tests
    error_logger:tty(false),
    case Cover of
        true  -> io:format("Cover compiling..."),
                 cover:start(),
                 ok = rabbit_misc:enable_cover(["../rabbitmq-server/"]),
                 io:format(" done.~n~n");
        false -> ok
    end,
    R = eunit:test(make_tests_multi(
                     ServerDir, Dir, Filter, Cover, PluginsDir, ?TIMEOUT), []),
    case Cover of
        true  -> io:format("~nCover reporting..."),
                 ok = rabbit_misc:report_cover(),
                 io:format(" done.~n~n");
        false -> ok
    end,
    R.

make_tests_single(Dir, Filter, Timeout) ->
    {Filtered, AllCount, Width} = find_tests(Dir, Filter, "_test"),
    io:format("Running ~B of ~B tests; FILTER=~s~n~n",
              [length(Filtered), AllCount, Filter]),
    [make_test_single(M, FWith, F, ShowHeading, Timeout, Width)
     || {M, FWith, F, ShowHeading} <- annotate_show_heading(Filtered)].

make_tests_multi(ServerDir, Dir, Filter, Cover, PluginsDir, Timeout) ->
    {Filtered, AllCount, Width} = find_tests(Dir, Filter, "_with"),
    io:format("Running ~B of ~B tests; FILTER=~s; COVER=~s~n~n",
              [length(Filtered), AllCount, Filter, Cover]),
    Cfg = [{cover,   Cover},
           {base,    basedir() ++ "/nodes"},
           {server,  ServerDir},
           {plugins, PluginsDir}],
    rabbit_test_configs:enable_plugins(Cfg),
    [make_test_multi(M, FWith, F, ShowHeading, Timeout, Width, Cfg)
     || {M, FWith, F, ShowHeading} <- annotate_show_heading(Filtered)].

find_tests(Dir, Filter, Suffix) ->
    All = [{M, FWith, F} ||
              M <- modules(Dir),
              {FWith, _Arity} <- proplists:get_value(exports, M:module_info()),
              string:right(atom_to_list(FWith), length(Suffix)) =:= Suffix,
              F <- [truncate_function_name(FWith, length(Suffix))]],
    Filtered = [Test || {M, _FWith, F} = Test <- All,
                        should_run(M, F, Filter)],
    Width = case Filtered of
                [] -> 0;
                _  -> lists:max([atom_length(F) || {_, _, F} <- Filtered])
            end,
    {Filtered, length(All), Width}.

make_test_single(M, FWith, F, ShowHeading, Timeout, Width) ->
    {timeout,
     Timeout,
     fun () ->
             maybe_print_heading(M, ShowHeading),
             io:format(user, "~s [running]", [name(F, Width)]),
             M:FWith(),
             io:format(user, " [PASSED].~n", [])
     end}.

make_test_multi(M, FWith, F, ShowHeading, Timeout, Width, InitialCfg) ->
    {setup,
     fun () ->
             maybe_print_heading(M, ShowHeading),
             io:format(user, "~s [setup]", [name(F, Width)]),
             setup_error_logger(M, F, basedir()),
             recursive_delete(pget(base, InitialCfg)),
             try
                 apply_config(M:FWith(), InitialCfg)
             catch
                 error:{Type, Error, Cfg, Stack} ->
                     case Cfg of
                         InitialCfg -> ok; %% [0]
                         _          -> rabbit_test_configs:stop_nodes(Cfg)
                     end,
                     exit({Type, Error, Stack})
             end
     end,
     fun (Nodes) ->
             rabbit_test_configs:stop_nodes(Nodes),
             %% Partition tests change this, let's revert
             net_kernel:set_net_ticktime(60, 1),
             io:format(user, ".~n", [])
     end,
     fun (Nodes) ->
             [{timeout,
               Timeout,
               fun () ->
                       [link(pget(linked_pid, N)) || N <- Nodes],
                       io:format(user, " [running]", []),
                       %%try
                           M:F(Nodes),
                           io:format(user, " [PASSED]", [])
                       %% catch
                       %%     Type:Reason ->
                       %%         io:format(user, "YYY stop~n", []),
                       %%         rabbit_test_configs:stop_nodes(Nodes),
                       %%         exit({Type, Reason, erlang:get_stacktrace()})
                       %% end
               end}]
     end}.
%% [0] If we didn't get as far as starting any nodes then we only have
%% one proplist for initial config, not several per node. So avoid
%% trying to "stop" it - it won't work (and there's nothing to do
%% anyway).

maybe_print_heading(M, true) ->
    io:format(user, "~n~s~n~s~n", [M, string:chars($-, atom_length(M))]);
maybe_print_heading(_M, false) ->
    ok.

apply_config(Things, Cfg) when is_list(Things) ->
    lists:foldl(fun apply_config/2, Cfg, Things);
apply_config(F, Cfg) when is_atom(F) ->
    apply_config(fun (C) -> rabbit_test_configs:F(C) end, Cfg);
apply_config(F, Cfg) when is_function(F) ->
    try
        F(Cfg)
    catch
        Type:Error -> erlang:error({Type, Error, Cfg, erlang:get_stacktrace()})
    end.

annotate_show_heading(List) ->
    annotate_show_heading(List, undefined).

annotate_show_heading([], _) ->
    [];
annotate_show_heading([{M, FWith, F} | Rest], Current) ->
    [{M, FWith, F, M =/= Current} | annotate_show_heading(Rest, M)].

setup_error_logger(M, F, Base) ->
    case error_logger_logfile_filename() of
        {error, no_log_file} -> ok;
        _                    -> ok = error_logger:logfile(close)
    end,
    FN = rabbit_misc:format("~s/~s:~s.log", [basedir(), M, F]),
    ensure_dir(Base),
    ok = error_logger:logfile({open, FN}).

truncate_function_name(FWith, Length) ->
    FName = atom_to_list(FWith),
    list_to_atom(string:substr(FName, 1, length(FName) - Length)).

should_run(_M, _F, "all") -> true;
should_run(M, F, Filter)  -> MF = rabbit_misc:format("~s:~s", [M, F]),
                             case re:run(MF, Filter) of
                                 {match, _} -> true;
                                 nomatch    -> false
                             end.

ensure_dir(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type=regular}}   -> exit({exists_as_file, Path});
        {ok, #file_info{type=directory}} -> ok;
        _                                -> file:make_dir(Path)
    end.

modules(RelDir) ->
    {ok, Files} = file:list_dir(RelDir),
    [M || F <- Files,
          M <- case string:tokens(F, ".") of
                   [MStr, "beam"] -> [list_to_atom(MStr)];
                   _              -> []
               end].

recursive_delete(Dir) ->
    rabbit_test_configs:execute({"rm -rf ~s", [Dir]}).

name(F, Width) ->
    R = atom_to_list(F),
    R ++ ":" ++ string:chars($ , Width - length(R)).

atom_length(A) -> length(atom_to_list(A)).

basedir() -> "/tmp/rabbitmq-multi-node".

%% reimplement error_logger:logfile(filename) only using
%% gen_event:call/4 instead of gen_event:call/3 with our old friend
%% the 5 second timeout. Grr.
error_logger_logfile_filename() ->
    case gen_event:call(
           error_logger, error_logger_file_h, filename, infinity) of
	{error,_} -> {error, no_log_file};
	Val       -> Val
    end.

add_server_test_ebin_dir() ->
    %% Some tests need modules from this dir, but it's not on the path
    %% by default.
    {file, Path} = code:is_loaded(rabbit),
    Ebin = filename:dirname(Path),
    TestEbin = filename:join([Ebin, "..", "test", "ebin"]),
    code:add_path(TestEbin).
