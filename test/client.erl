%%% -------------------------------------------------------------------
%%% Author  : xxw
%%% Description :
%%%
%%% Created : 2015-12-1
%%% -------------------------------------------------------------------
-module(client).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("amqp_client.hrl").
%% --------------------------------------------------------------------
%% External exports
-export([
		 start_link/0
		]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {connection, channel}).

-define(SEND_INTERVAL_TIME, 5000).				%% 给RabbitMQ系统发送消息的时间间隔

%% ====================================================================
%% External functions
%% ====================================================================


%% ====================================================================
%% Server functions
%% ====================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([]) ->
	State =
		case run_option:get_options("../options/run.option") of
			[] ->
				io:format("start_cluster start find run.option is empty~n"),
				#state{};
			Options ->
				%% 统一节点的cookie
				Cookie = proplists:get_value(cookie, Options),
				erlang:set_cookie(node(), Cookie),
				%% 如果是windows则修改cmd窗口的标题等相关东西
				case os:type() of
					{win32, nt} ->
						SNodeName = node_util:get_node_name(node()),
						TitleCmdLine = "title " ++ SNodeName,
						os:cmd(TitleCmdLine);
					_ ->
						nothing
				end,
				%% 连接RabbitMQ服务器
				connect_rabbitmq_server(Options)
		end,
	{ok, State}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
%% 处理向服务器发送消息的定时消息
handle_info(send_msg_to_server, State) ->
	send_msg_to_rabbitmq_server(State),
	{noreply, State};

handle_info(Msg, State) ->
	io:format("Receieve RabbitMQ Server Data:~p~n", [Msg]),
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
%% 连接RabbitMQ服务器
connect_rabbitmq_server(Options) ->
	SelfNodeName = node_util:get_node_name(node()),
	Prefix = proplists:get_value(prefix, Options),
	RealNodeName = string:sub_string(SelfNodeName, string:len(Prefix) + 1, string:len(SelfNodeName)),
	AllClientInfo = proplists:get_value(clients, Options),
	case lists:keyfind(RealNodeName, 1, AllClientInfo) of
		false ->
			#state{};
		{_, _, _, Params, _} ->
			AMQParams = #amqp_params_network{
											 username 		=  		proplists:get_value(username, Params),
											 password 		=  		proplists:get_value(password, Params),
											 host 			=  		proplists:get_value(host, Params),
											 virtual_host 	=  		proplists:get_value(virtual_host, Params),
											 channel_max 	=  		proplists:get_value(channel_max, Params),
											 port			=		proplists:get_value(port, Params)
											},
			%% 建立 rabbitmq 队列服务器的一个连接
			{ok, Connection} = amqp_connection:start(AMQParams),
			%% 创建新的频道
			{ok, Channel} = amqp_connection:open_channel(Connection),
			%% 创建交换机和队列，同时将两者绑定在一起，同时创建该队列的消费者
			create_queue_and_exchange_and_consumer(Channel),
			%% 将消息队列和RabbitMQ系统日志交换机绑定起来
			binding_to_rabbitmq_log_exchange(Channel),
			%% 启动定时器向服务器发送消息
			erlang:send_after(?SEND_INTERVAL_TIME, self(), send_msg_to_server),
			#state{connection = Connection, channel = Channel}
	end.


%% 定时向服务器发送消息
send_msg_to_rabbitmq_server(#state{channel = Channel}) ->
	Message = #'basic.publish'{exchange = atom_to_binary(node(), latin1), routing_key = atom_to_binary(node(), latin1)},
	Content = #amqp_msg{
						payload = <<"send_msg_to_rabbitmq_server test data">>,
						props = #'P_basic'{delivery_mode = 2}
					   },
	amqp_channel:cast(Channel, Message, Content),
	%% 启动定时器向服务器发送消息
	erlang:send_after(?SEND_INTERVAL_TIME, self(), send_msg_to_server).


%% 创建交换机和队列，同时将两者绑定在一起
create_queue_and_exchange_and_consumer(Channel) ->
	ExchangeName = atom_to_binary(node(), latin1),
	ExchangeDeclare = #'exchange.declare'{
										  exchange = ExchangeName,
										  type = <<"direct">>,
										  durable = true,
										  passive = false
										 },
	%% 创建exchange
	#'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
	%% 创建队列
	QueueName = atom_to_binary(node(), latin1),
	QueueDeclare = #'queue.declare'{queue = QueueName, durable = true},
	#'queue.declare_ok'{queue = QueueName} = amqp_channel:call(Channel, QueueDeclare),
	%% 将队列和exchange绑定
	QueueBind = #'queue.bind'{queue = QueueName, exchange = ExchangeName, routing_key = atom_to_binary(node(), latin1)},
	#'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
	ConsumeMsg = #'basic.consume'{queue = atom_to_binary(node(), latin1), consumer_tag = atom_to_binary(node(), latin1), no_ack = true},
	amqp_channel:subscribe(Channel, ConsumeMsg, self()).


%% 将消息队列和RabbitMQ系统日志交换机绑定起来
binding_to_rabbitmq_log_exchange(Channel) ->
	QueueName = atom_to_binary(node(), latin1),
	QueueBind = #'queue.bind'{queue = QueueName, exchange = <<"amq.rabbitmq.log">>, routing_key = <<"#">>},
	#'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).