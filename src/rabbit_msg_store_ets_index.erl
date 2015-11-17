%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_ets_index).

-include("rabbit_msg_store.hrl").

-behaviour(rabbit_msg_store_index).

-export([new/1, recover/1,
         lookup/2, insert/2, update/2, update_fields/3, delete/2,
         delete_object/2, delete_by_file/2, terminate/1]).

-define(MSG_LOC_NAME, rabbit_msg_store_ets_index).
-define(FILENAME, "msg_store_index.ets").

-record(state, { table, dir }).

%% 创建新的消息索引数据结构
new(Dir) ->
	file:delete(filename:join(Dir, ?FILENAME)),
	Tid = ets:new(?MSG_LOC_NAME, [set, public, {keypos, #msg_location.msg_id}]),
	#state { table = Tid, dir = Dir }.


%% 将msg_store_index.ets文件中的消息索引信息恢复到rabbit_msg_store_ets_index ETS表(msg_store_index.ets为RabbitMQ系统崩溃的时候将消息的索引信息直接保持到该文件中)
recover(Dir) ->
	Path = filename:join(Dir, ?FILENAME),
	case ets:file2tab(Path) of
		{ok, Tid}  -> file:delete(Path),
					  {ok, #state { table = Tid, dir = Dir }};
		Error      -> Error
	end.


%% Key为消息ID
lookup(Key, State) ->
	case ets:lookup(State #state.table, Key) of
		[]      -> not_found;
		[Entry] -> Entry
	end.


%% 插入消息的索引信息到rabbit_msg_store_ets_index ETS表
insert(Obj, State) ->
	true = ets:insert_new(State #state.table, Obj),
	ok.


%% 消息存储服务器进程中消息索引信息的更新整个索引接口
update(Obj, State) ->
	true = ets:insert(State #state.table, Obj),
	ok.


%% 消息存储服务器进程中消息索引信息的更新字段接口
update_fields(Key, Updates, State) ->
	true = ets:update_element(State #state.table, Key, Updates),
	ok.


%% 消息存储服务器进程中消息索引删除的接口
delete(Key, State) ->
	true = ets:delete(State #state.table, Key),
	ok.


%% 消息存储服务器进程中删除整个消息索引接口
delete_object(Obj, State) ->
	true = ets:delete_object(State #state.table, Obj),
	ok.


%% 根据磁盘文件名删除该文件对应的所有索引
delete_by_file(File, State) ->
	MatchHead = #msg_location { file = File, _ = '_' },
	ets:select_delete(State #state.table, [{MatchHead, [], [true]}]),
	ok.


%% RabbitMQ系统崩溃的时候，将消息索引信息存储到msg_store_index.ets这个文件中
terminate(#state { table = MsgLocations, dir = Dir }) ->
	ok = ets:tab2file(MsgLocations, filename:join(Dir, ?FILENAME),
					  [{extended_info, [object_count]}]),
	ets:delete(MsgLocations).
