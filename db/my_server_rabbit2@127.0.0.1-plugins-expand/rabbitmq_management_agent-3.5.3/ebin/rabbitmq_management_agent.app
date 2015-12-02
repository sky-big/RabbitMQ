{application,rabbitmq_management_agent,
             [{description,"RabbitMQ Management Agent"},
              {vsn,"3.5.3"},
              {modules,[rabbit_mgmt_agent_app,rabbit_mgmt_agent_sup,
                        rabbit_mgmt_db_handler,rabbit_mgmt_external_stats]},
              {registered,[]},
              {mod,{rabbit_mgmt_agent_app,[]}},
              {env,[]},
              {applications,[kernel,stdlib,rabbit]}]}.
