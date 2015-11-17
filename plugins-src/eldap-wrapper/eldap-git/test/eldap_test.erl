-module(eldap_test).
%%% --------------------------------------------------------------------
%%% Created:  12 Oct 2000 by Tobbe 
%%% Function: Test code for the eldap module
%%%
%%% Copyright (C) 2000  Torbjörn Törnkvist
%%% Copyright (c) 2010 Torbjorn Tornkvist <tobbe@tornkvist.org>
%%% See MIT-LICENSE at the top dir for licensing information.
%%% 
%%% --------------------------------------------------------------------
-vc('$Id$ ').
-export([topen_bind/1,topen_bind/2,all/0,t10/0,t20/0,t21/0,t22/0,
	 t23/0,t24/0,t25/0,t26/0,t27/0,debug/1,t30/0,t31/0,
	 t40/0,t41/0,t50/0,t51/0]).
-export([crl1/0]).
-export([switch/1]).
-export([junk/0]).

-include("ELDAPv3.hrl").
-include("eldap.hrl").

junk() ->
    DN = "cn=Torbjorn Tornkvist, ou=people, o=Bluetail AB, dc=bluetail, dc=com",
    Msg = #'LDAPMessage'{messageID  = 1,
			 protocolOp = {delRequest,DN}},
    asn1rt:encode('ELDAPv3', 'LDAPMessage', Msg).

%%% --------------------------------------------------------------------
%%% TEST STUFF
%%% ----------
%%% When adding a new test case it can be useful to
%%% switch on debugging, i.e debug(t) in the call to
%%% topen_bind/2.
%%% --------------------------------------------------------------------

all() ->
    Check = "=== Check the result of the previous test case !~n",
    t10(),
    t20(),t21(),t22(),t23(),t24(),t25(),t26(),t27(),
    t30(),t26(Check),t31(),t26(Check),
    t40(),t26(Check),t41(),t26(Check),
    t50(),t26(Check),t51(),t26(Check),
    ok.

%%%
%%% Setup a connection and bind using simple authentication
%%%
t10() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 10 (connection setup + simple auth)~n"),
		line(),
		X = topen_bind("localhost", debug(f)),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Do an equality match:  sn = Tornkvist
%%%
t20() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 20 (equality match)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Filter = eldap:equalityMatch("sn","Tornkvist"),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, Filter}])),

		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Do a substring match:  sn = To*kv*st
%%%
t21() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 21 (substring match)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Filter = eldap:substrings("sn", [{initial,"To"},
						 {any,"kv"},
						 {final,"st"}]),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, Filter}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Do a substring match:  sn = *o* 
%%% and do only retrieve the cn attribute
%%%
t22() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 22 (substring match + return 'cn' only)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Filter = eldap:substrings("sn", [{any,"o"}]),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, Filter},
					  {attributes,["cn"]}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).


%%%
%%% Do a present search for the attribute 'objectclass'
%%% on the base level.
%%%
t23() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 23 (objectclass=* , base level)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, eldap:present("objectclass")},
					  {scope,eldap:baseObject()}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Do a present search for the attribute 'objectclass'
%%% on a single level.
%%%
t24() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 24 (objectclass=* , single level)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, eldap:present("objectclass")},
					  {scope,eldap:singleLevel()}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Do a present search for the attribute 'objectclass'
%%% on the whole subtree.
%%%
t25() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 25 (objectclass=* , whole subtree)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, eldap:present("objectclass")},
					  {scope,eldap:wholeSubtree()}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Do a present search for the attributes 
%%% 'objectclass' and 'sn' on the whole subtree.
%%%
t26() -> t26([]).
t26(Heading) ->
    F = fun() ->
		sleep(),
		line(),
		heading(Heading,
			"=== TEST 26 (objectclass=* and sn=*)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Filter = eldap:'and'([eldap:present("objectclass"),
				      eldap:present("sn")]),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, Filter},
					  {scope,eldap:wholeSubtree()}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Do a present search for the attributes 
%%% 'objectclass' and (not 'sn') on the whole subtree.
%%%
t27() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== TEST 27 (objectclass=* and (not sn))~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Filter = eldap:'and'([eldap:present("objectclass"),
				      eldap:'not'(eldap:present("sn"))]),
		X=(catch eldap:search(S, [{base, "dc=bluetail, dc=com"},
					  {filter, Filter},
					  {scope,eldap:wholeSubtree()}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Replace the 'telephoneNumber' attribute and
%%% add a new attribute 'description'
%%%
t30() -> t30([]).
t30(Heading) ->
    F = fun() ->
		sleep(),
		{_,_,Tno} = erlang:now(),
		Stno = integer_to_list(Tno),
		Desc = "LDAP hacker " ++ Stno,
		line(),
		heading(Heading,
			"=== TEST 30 (replace telephoneNumber/"
			  ++ Stno ++ " add description/" ++ Desc
			  ++ ")~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Obj = "cn=Torbjorn Tornkvist, ou=people, o=Bluetail AB, dc=bluetail, dc=com",
		Mod = [eldap:mod_replace("telephoneNumber", [Stno]),
		       eldap:mod_add("description", [Desc])],
		X=(catch eldap:modify(S, Obj, Mod)),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Delete attribute 'description'
%%%
t31() -> t31([]).
t31(Heading) ->
    F = fun() ->
		sleep(),
		{_,_,Tno} = erlang:now(),
		line(),
		heading(Heading,
			"=== TEST 31 (delete 'description' attribute)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Obj = "cn=Torbjorn Tornkvist, ou=people, o=Bluetail AB, dc=bluetail, dc=com",
		Mod = [eldap:mod_delete("description", [])],
		X=(catch eldap:modify(S, Obj, Mod)),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Add an entry
%%%
t40() -> t40([]).
t40(Heading) ->
    F = fun() ->
		sleep(),
		{_,_,Tno} = erlang:now(),
		line(),
		heading(Heading,
			"=== TEST 40 (add entry 'Bill Valentine')~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Entry = "cn=Bill Valentine, ou=people, o=Bluetail AB, dc=bluetail, dc=com",
		X=(catch eldap:add(S, Entry,
				   [{"objectclass", ["person"]},
				    {"cn", ["Bill Valentine"]},
				    {"sn", ["Valentine"]},
				    {"telephoneNumber", ["545 555 00"]}])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Delete an entry
%%%
t41() -> t41([]).
t41(Heading) ->
    F = fun() ->
		sleep(),
		{_,_,Tno} = erlang:now(),
		line(),
		heading(Heading,
			"=== TEST 41 (delete entry 'Bill Valentine')~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Entry = "cn=Bill Valentine, ou=people, o=Bluetail AB, dc=bluetail, dc=com",
		X=(catch eldap:delete(S, Entry)),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Modify the DN of an entry
%%%
t50() -> t50([]).
t50(Heading) ->
    F = fun() ->
		sleep(),
		{_,_,Tno} = erlang:now(),
		line(),
		heading(Heading,
			"=== TEST 50 (modify DN to: 'Torbjorn M.Tornkvist')~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Entry = "cn=Torbjorn Tornkvist, ou=people, o=Bluetail AB, dc=bluetail, dc=com",
		X=(catch eldap:modify_dn(S, Entry,
					 "cn=Torbjorn M.Tornkvist",
					 false,
					 [])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%%
%%% Modify the DN of an entry and remove the RDN attribute.
%%% NB: Must be run after: 't50' !
%%%
t51() -> t51([]).
t51(Heading) ->
    F = fun() ->
		sleep(),
		{_,_,Tno} = erlang:now(),
		line(),
		heading(Heading,
			"=== TEST 51 (modify DN, remove the RDN attribute)~n"),
		line(),
		{ok,S} = topen_bind("localhost", debug(f)),
		Entry = "cn=Torbjorn M.Tornkvist, ou=people, o=Bluetail AB, dc=bluetail, dc=com",
		X=(catch eldap:modify_dn(S, Entry,
					 "cn=Torbjorn Tornkvist",
					 true,
					 [])),
		io:format("~p~n",[X]),
		X
	end,
    go(F).

%%% --------------------------------------------------------------------
%%% Test cases for certificate revocation lists
%%% --------------------------------------------------------------------

crl1() ->
    F = fun() ->
		sleep(),
		line(),
		io:format("=== CRL-TEST 1 ~n"),
		line(),
		{ok,S} = crl_open_bind("localhost", debug(f)),
		Filter = eldap:equalityMatch("cn","Administrative CA"),
		X=(catch eldap:search(S, [{base, "o=Post Danmark, c=DK"},
					  {filter, Filter},
					  {attributes,["certificateRevocationList"]}])),
		dump_to_file("test-crl1.result",X),
		ok
	end,
    go(F).


dump_to_file(Fname,{ok,Res}) ->
    case Res#eldap_search_result.entries of
	[Entry|_] ->
	    case Entry#eldap_entry.attributes of
		[{Attribute,Value}|_] ->
		    file:write_file(Fname,list_to_binary(Value)),
		    io:format("Value of '~s' dumped to file: ~s~n",
			      [Attribute,Fname]);
		Else ->
		    io:format("ERROR(dump_to_file): no attributes found~n",[])
	    end;
	Else ->
	    io:format("ERROR(dump_to_file): no entries found~n",[])
    end.

switch(1) ->
    %%
    %% SEARCH
    %%
    F = fun() ->
		sleep(),
		line(),
		io:format("=== SWITCH-TEST 1 (short-search)~n"),
		line(),
		{ok,S} = sw_open_bind("korp", debug(t)),
		Filter = eldap:equalityMatch("cn","Administrative CA"),
		X=(catch eldap:search(S, [{base, "o=Post Danmark, c=DK"},
					  {filter, Filter},
					  {attributes,["cn"]}])),
		io:format("RESULT: ~p~n", [X]),
		%%dump_to_file("test-switch-1.result",X),
		eldap:close(S),
		ok
	end,
    go(F);
switch(2) ->
    %%
    %% ADD AN ENTRY
    %%
    F = fun() ->
		sleep(),
		line(),
		io:format("=== SWITCH-TEST 2 (add-entry)~n"),
		line(),
		{ok,S} = sw_open_bind("korp", debug(t)),
		Entry = "cn=Bill Valentine, o=Post Danmark, c=DK",
		X=(catch eldap:add(S, Entry,
				   [{"objectclass", ["person"]},
				    {"cn", ["Bill Valentine"]},
				    {"sn", ["Valentine"]}
				    ])),
		io:format("~p~n",[X]),
		eldap:close(S),
		X
	end,
    go(F);
switch(3) ->
    %%
    %% SEARCH FOR THE NEWLEY ADDED ENTRY
    %%
    F = fun() ->
		sleep(),
		line(),
		io:format("=== SWITCH-TEST 3 (search-added)~n"),
		line(),
		{ok,S} = sw_open_bind("korp", debug(t)),
		Filter = eldap:equalityMatch("cn","Bill Valentine"),
		X=(catch eldap:search(S, [{base, "o=Post Danmark, c=DK"},
					  {filter, Filter},
					  {attributes,["cn"]}])),
		io:format("RESULT: ~p~n", [X]),
		%%dump_to_file("test-switch-1.result",X),
		eldap:close(S),
		ok
	end,
    go(F);
switch(4) ->
    %%
    %% DELETE THE NEWLEY ADDED ENTRY
    %%
    F = fun() ->
		sleep(),
		line(),
		io:format("=== SWITCH-TEST 4 (delete-added)~n"),
		line(),
		{ok,S} = sw_open_bind("korp", debug(t)),
		Entry = "cn=Bill Valentine, o=Post Danmark, c=DK",
		X=(catch eldap:delete(S, Entry)),
		io:format("RESULT: ~p~n", [X]),
		%%dump_to_file("test-switch-1.result",X),
		eldap:close(S),
		ok
	end,
    go(F).



%%% ---------------
%%% Misc. functions
%%% ---------------

sw_open_bind(Host) -> 
    sw_open_bind(Host, debug(t)).

sw_open_bind(Host, Dbg) ->
    sw_open_bind(Host, Dbg, "cn=Torbjorn Tornkvist,o=Post Danmark,c=DK", "qwe123").

sw_open_bind(Host, LogFun, RootDN, Passwd) ->
    Opts = [{log,LogFun},{port,9779}],
    {ok,Handle} = eldap:open([Host], Opts),
    {eldap:simple_bind(Handle, RootDN, Passwd),
     Handle}.

crl_open_bind(Host) -> 
    crl_open_bind(Host, debug(t)).

crl_open_bind(Host, Dbg) ->
    do_open_bind(Host, Dbg, "o=Post Danmark, c=DK", "hejsan").

topen_bind(Host) -> 
    topen_bind(Host, debug(t)).

topen_bind(Host, Dbg) -> 
    do_open_bind(Host, Dbg, "dc=bluetail, dc=com", "hejsan").

do_open_bind(Host, LogFun, RootDN, Passwd) ->
    Opts = [{log,LogFun}],
    {ok,Handle} = eldap:open([Host], Opts),
    {eldap:simple_bind(Handle, RootDN, Passwd),
     Handle}.

debug(t) -> fun(L,S,A) -> io:format("--- " ++ S, A) end;
debug(1) -> fun(L,S,A) when L =< 1 -> io:format("--- " ++ S, A) end;
debug(2) -> fun(L,S,A) when L =< 2 -> io:format("--- " ++ S, A) end;
debug(f) -> false.

sleep()    -> msleep(400).
%sleep(Sec) -> msleep(Sec*1000).
msleep(T)  -> receive after T -> true end.

line() ->
    S = "==============================================================\n",
    io:format(S).

heading([], Heading) -> io:format(Heading);
heading(Heading, _ ) -> io:format(Heading).

%%%
%%% Process to run the test case
%%%
go(F) ->
    Self = self(),
    Pid = spawn(fun() -> run(F,Self) end),
    receive {Pid, X} -> ok end.

run(F, Pid) ->
    Pid ! {self(),catch F()}.
