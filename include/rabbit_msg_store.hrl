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

-include("rabbit.hrl").

-ifdef(use_specs).

-type(msg() :: any()).

-endif.

%% 该结构表示一个消息在持久化文件中的位置
-record(msg_location,
		{msg_id,						%% 消息ID
		 ref_count,						%% 消息的引用计数
		 file,							%% 消息保存的文件名（数字）
		 offset,						%% 消息在文件中的偏移量
		 total_size						%% 消息的大小
		}).
