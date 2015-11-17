DEPS:=rabbitmq-erlang-client
FILTER:=all
COVER:=false
WITH_BROKER_TEST_COMMANDS:=rabbit_test_runner:run_in_broker(\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\")
STANDALONE_TEST_COMMANDS:=rabbit_test_runner:run_multi(\"$(UMBRELLA_BASE_DIR)/rabbitmq-server\",\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\",$(COVER),none)

## Require R15B to compile inet_proxy_dist since it requires includes
## introduced there.
ifeq ($(shell erl -noshell -eval 'io:format([list_to_integer(X) || X <- string:tokens(erlang:system_info(version), ".")] >= [5,9]), halt().'),true)
PACKAGE_ERLC_OPTS+=-Derlang_r15b_or_later
endif
