# RabbitMQ
RabbitMQ系统3.5.3版本中文完全注释，该目录下是RabbitMQ系统包括所有插件的源代码编译，
根据配置文件启动集群，停止集群，根据配置文件启动客户端节点连接RabbitMQ系统进行测试


一.scripts目录下是整个RabbitMQ系统包括插件源代码编译，组建集群，启动客户端节点列表的脚本目录

    1.Make.bat脚本用来编译整个RabbitMQ系统包括插件的源代码
      (但是如果beam目录修改时间大于等于源代码文件目录则不会进行重新编译,否则进行重新编译)

    2.Make_All.bat脚本则会将所有的beam文件全部删除，然后将RabbitMQ系统包括插件的所有源代码重新编译成beam文件

    3.Run_Cluster.bat脚本是根据option目录下的run.options文件中的配置启动RabbitMQ系统集群
      节点配置样本：
      {nodes, [
			{"rabbit1", "127.0.0.1", 5673, nosmp, disc, "-rabbitmq_management listener [{port,15673}]"},
			{"rabbit2", "127.0.0.1", 5674, nosmp, ram,  "-rabbitmq_management listener [{port,15674}]"},
			{"rabbit3", "127.0.0.1", 5675, nosmp, disc, "-rabbitmq_management listener [{port,15675}]"}
		]
	  }
      第一个RabbitMQ节点监听端口是5673，rabbitmq_management插件监听的端口是15673，且该RabbitMQ系统节点为磁盘节点
      第二个RabbitMQ节点监听端口是5674，rabbitmq_management插件监听的端口是15674，且该RabbitMQ系统节点为内存节点
      第三个RabbitMQ节点监听端口是5673，rabbitmq_management插件监听的端口是15675，且该RabbitMQ系统节点为磁盘节点

    4.Stop_Cluster.bat脚本是根据option目录下的run.options文件中的配置停止当前启动的RabbitMQ系统集群

    5.Run_Clients.bat脚本是根据option目录下的run.options文件中的配置启动客户端节点，用来测试连接RabbitMQ集群中的节点，
      这些客户端测试节点会定时5秒向客户端发送消息，然后会启动消费者不断的在对应的队列消费消息。
      客户端节点配置样本：
      {clients, [
			{"rabbit_client1", "127.0.0.1", nosmp,
						[
							{username,			<<"guest">>},
							{password,			<<"guest">>},
							{virtual_host,		<<"/">>},
							{channel_max,		0},
							{port,				5673},
							{host,				"localhost"}
						 ], ""
			},
			{"rabbit_client2", "127.0.0.1", nosmp,
						[
							{username,			<<"guest">>},
							{password,			<<"guest">>},
							{virtual_host,		<<"/">>},
							{channel_max,		0},
							{port,				5674},
							{host,				"localhost"}
						 ], ""
			},
			{"rabbit_client3", "127.0.0.1", nosmp,
						[
							{username,			<<"guest">>},
							{password,			<<"guest">>},
							{virtual_host,		<<"/">>},
							{channel_max,		0},
							{port,				5675},
							{host,				"localhost"}
						 ], ""
			}
		]
	  }
      第一个客户端节点连接的是端口为5673的RabbitMQ系统节点
      第二个客户端节点连接的是端口为5674的RabbitMQ系统节点
      第三个客户端节点连接的是端口为5675的RabbitMQ系统节点
    
二.version_up:up_all().会将改变的代码热更新到RabbitMQ服务器集群中
