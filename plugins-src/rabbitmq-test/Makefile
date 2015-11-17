.PHONY: all full lite conformance16 update-qpid-testsuite run-qpid-testsuite \
	prepare restart-app stop-app start-app \
	start-secondary-app stop-secondary-app \
	restart-secondary-node cleanup force-snapshot \
	enable-ha disable-ha

include ../umbrella.mk

BROKER_DIR=../rabbitmq-server
TEST_DIR=../rabbitmq-java-client

TEST_RABBIT_PORT=5672
TEST_HARE_PORT=5673
TEST_RABBIT_SSL_PORT=5671
TEST_HARE_SSL_PORT=5670

COVER=true

ifeq ($(COVER), true)
COVER_START=start-cover
COVER_STOP=stop-cover
else
COVER_START=
COVER_STOP=
endif

# we actually want to test for ssl above 3.9 (eg >= 3.10), but this
# comparison is buggy because it doesn't believe 10 > 9, so it doesn't
# believe 3.10 > 3.9. As a result, we cheat, and use the erts version
# instead. SSL 3.10 came out with R13B, which included erts 5.7.1, so
# we require > 5.7.0.
SSL_VERIFY=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.7.0" ]; then echo "true"; else echo "false"; fi)
ifeq (true,$(SSL_VERIFY))
SSL_VERIFY_OPTION :={verify,verify_peer},{fail_if_no_peer_cert,false}
else
SSL_VERIFY_OPTION :={verify_code,1}
endif
export SSL_CERTS_DIR := $(realpath certs)
export PASSWORD := test
RABBIT_BROKER_OPTIONS := "-rabbit ssl_listeners [{\\\"0.0.0.0\\\",$(TEST_RABBIT_SSL_PORT)}] -rabbit ssl_options [{cacertfile,\\\"$(SSL_CERTS_DIR)/testca/cacert.pem\\\"},{certfile,\\\"$(SSL_CERTS_DIR)/server/cert.pem\\\"},{keyfile,\\\"$(SSL_CERTS_DIR)/server/key.pem\\\"},$(SSL_VERIFY_OPTION)] -rabbit auth_mechanisms ['PLAIN','AMQPLAIN','EXTERNAL','RABBIT-CR-DEMO']"
HARE_BROKER_OPTIONS := "-rabbit ssl_listeners [{\\\"0.0.0.0\\\",$(TEST_HARE_SSL_PORT)}] -rabbit ssl_options [{cacertfile,\\\"$(SSL_CERTS_DIR)/testca/cacert.pem\\\"},{certfile,\\\"$(SSL_CERTS_DIR)/server/cert.pem\\\"},{keyfile,\\\"$(SSL_CERTS_DIR)/server/key.pem\\\"},$(SSL_VERIFY_OPTION)] -rabbit auth_mechanisms ['PLAIN','AMQPLAIN','EXTERNAL','RABBIT-CR-DEMO']"

TESTS_FAILED := echo '\n============'\
	   	     '\nTESTS FAILED'\
		     '\n============\n'

all: full test

full:
	OK=true && \
	$(MAKE) prepare && \
	{ $(MAKE) -C $(BROKER_DIR) run-tests || { OK=false; $(TESTS_FAILED); } } && \
	{ $(MAKE) run-qpid-testsuite || { OK=false; $(TESTS_FAILED); } } && \
	{ ( cd $(TEST_DIR) && MAKE=$(MAKE) ant test-suite ) || { OK=false; $(TESTS_FAILED); } } && \
	$(MAKE) cleanup && { $$OK || $(TESTS_FAILED); } && $$OK

unit:
	OK=true && \
	$(MAKE) prepare && \
	{ $(MAKE) -C $(BROKER_DIR) run-tests || OK=false; } && \
	$(MAKE) cleanup && $$OK

lite:
	OK=true && \
	$(MAKE) prepare && \
	{ $(MAKE) -C $(BROKER_DIR) run-tests || OK=false; } && \
	{ ( cd $(TEST_DIR) && MAKE=$(MAKE) ant test-suite ) || OK=false; } && \
	$(MAKE) cleanup && $$OK

conformance16:
	OK=true && \
	$(MAKE) prepare && \
	{ $(MAKE) -C $(BROKER_DIR) run-tests || OK=false; } && \
	{ ( cd $(TEST_DIR) && MAKE=$(MAKE) ant test-suite ) || OK=false; } && \
	$(MAKE) cleanup && $$OK

qpid_testsuite:
	$(MAKE) update-qpid-testsuite

update-qpid-testsuite:
	svn co -r 906960 http://svn.apache.org/repos/asf/qpid/trunk/qpid/python qpid_testsuite
	# hg clone http://rabbit-hg.eng.vmware.com/mirrors/qpid_testsuite
	- patch -N -r - -p0 -d qpid_testsuite/ < qpid_patch

prepare-qpid-patch:
	cd qpid_testsuite && svn diff > ../qpid_patch && cd ..

run-qpid-testsuite: qpid_testsuite
	AMQP_SPEC=../rabbitmq-docs/specs/amqp0-8.xml qpid_testsuite/qpid-python-test -m tests_0-8 -I rabbit_failing.txt
	AMQP_SPEC=../rabbitmq-docs/specs/amqp0-9-1.xml qpid_testsuite/qpid-python-test -m tests_0-9 -I rabbit_failing.txt

clean:
	rm -rf qpid_testsuite

prepare: create_ssl_certs
	$(MAKE) -C $(BROKER_DIR) \
		RABBITMQ_NODENAME=hare \
		RABBITMQ_NODE_IP_ADDRESS=0.0.0.0 \
		RABBITMQ_NODE_PORT=${TEST_HARE_PORT} \
		RABBITMQ_SERVER_START_ARGS=$(HARE_BROKER_OPTIONS) \
		RABBITMQ_CONFIG_FILE=/does-not-exist \
		RABBITMQ_ENABLED_PLUGINS_FILE=/does-not-exist \
		stop-node cleandb start-background-node
	$(MAKE) -C $(BROKER_DIR) \
		RABBITMQ_NODE_IP_ADDRESS=0.0.0.0 \
		RABBITMQ_NODE_PORT=${TEST_RABBIT_PORT} \
		RABBITMQ_SERVER_START_ARGS=$(RABBIT_BROKER_OPTIONS) \
		RABBITMQ_CONFIG_FILE=/does-not-exist \
		RABBITMQ_ENABLED_PLUGINS_FILE=/does-not-exist \
		stop-node cleandb start-background-node ${COVER_START} start-rabbit-on-node
	$(MAKE) -C $(BROKER_DIR) RABBITMQ_NODENAME=hare start-rabbit-on-node

start-app:
	$(MAKE) -C $(BROKER_DIR) \
		RABBITMQ_NODE_IP_ADDRESS=0.0.0.0 \
		RABBITMQ_NODE_PORT=${TEST_RABBIT_PORT} \
		RABBITMQ_SERVER_START_ARGS=$(RABBIT_BROKER_OPTIONS) \
		RABBITMQ_CONFIG_FILE=/does-not-exist \
		RABBITMQ_ENABLED_PLUGINS_FILE=/does-not-exist \
		start-rabbit-on-node

stop-app:
	$(MAKE) -C $(BROKER_DIR) stop-rabbit-on-node

restart-app: stop-app start-app

start-secondary-app:
	$(MAKE) -C $(BROKER_DIR) RABBITMQ_NODENAME=hare start-rabbit-on-node

stop-secondary-app:
	$(MAKE) -C $(BROKER_DIR) RABBITMQ_NODENAME=hare stop-rabbit-on-node

restart-secondary-node:
	$(MAKE) -C $(BROKER_DIR) \
		RABBITMQ_NODENAME=hare \
		RABBITMQ_NODE_IP_ADDRESS=0.0.0.0 \
		RABBITMQ_NODE_PORT=${TEST_HARE_PORT} \
		RABBITMQ_SERVER_START_ARGS=$(HARE_BROKER_OPTIONS) \
		RABBITMQ_CONFIG_FILE=/does-not-exist \
		RABBITMQ_ENABLED_PLUGINS_FILE=/does-not-exist \
		stop-node start-background-node
	$(MAKE) -C $(BROKER_DIR) RABBITMQ_NODENAME=hare start-rabbit-on-node

force-snapshot:
	$(MAKE) -C $(BROKER_DIR) force-snapshot

set-resource-alarm:
	$(MAKE) -C $(BROKER_DIR) set-resource-alarm SOURCE=$(SOURCE)

clear-resource-alarm:
	$(MAKE) -C $(BROKER_DIR) clear-resource-alarm SOURCE=$(SOURCE)

enable-ha:
	$(BROKER_DIR)/scripts/rabbitmqctl set_policy HA \
		".*" '{"ha-mode": "all"}'

disable-ha:
	$(BROKER_DIR)/scripts/rabbitmqctl clear_policy HA

cleanup:
	-$(MAKE) -C $(BROKER_DIR) \
		RABBITMQ_NODENAME=hare \
		RABBITMQ_NODE_IP_ADDRESS=0.0.0.0 \
		RABBITMQ_NODE_PORT=${TEST_HARE_PORT} \
		RABBITMQ_SERVER_START_ARGS=$(HARE_BROKER_OPTIONS) \
		RABBITMQ_CONFIG_FILE=/does-not-exist \
		RABBITMQ_ENABLED_PLUGINS_FILE=/does-not-exist \
		stop-rabbit-on-node stop-node
	-$(MAKE) -C $(BROKER_DIR) \
		RABBITMQ_NODE_IP_ADDRESS=0.0.0.0 \
		RABBITMQ_NODE_PORT=${TEST_RABBIT_PORT} \
		RABBITMQ_SERVER_START_ARGS=$(RABBIT_BROKER_OPTIONS) \
		RABBITMQ_CONFIG_FILE=/does-not-exist \
		RABBITMQ_ENABLED_PLUGINS_FILE=/does-not-exist \
		stop-rabbit-on-node ${COVER_STOP} stop-node

create_ssl_certs:
	$(MAKE) -C certs DIR=$(SSL_CERTS_DIR) clean all

