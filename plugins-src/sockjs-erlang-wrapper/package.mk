APP_NAME:=sockjs
DEPS:=cowboy-wrapper

UPSTREAM_GIT:=https://github.com/rabbitmq/sockjs-erlang.git
UPSTREAM_REVISION:=3132eb920aea9abd5c5e65349331c32d8cfa961e # 0.3.4
RETAIN_ORIGINAL_VERSION:=true
WRAPPER_PATCHES:=\
        0000-remove-spec-patch.diff \
        0001-a2b-b2a.diff \
        0002-parameterised-modules-r16a.diff \
        0003-websocket-subprotocol

ORIGINAL_APP_FILE:=$(CLONE_DIR)/src/$(APP_NAME).app.src
DO_NOT_GENERATE_APP_FILE=true

ERLC_OPTS:=$(ERLC_OPTS) -D no_specs

define construct_app_commands
	cp $(CLONE_DIR)/LICENSE-* $(APP_DIR)
	rm $(APP_DIR)/ebin/pmod_pt.beam
endef

define package_rules

$(CLONE_DIR)/ebin/sockjs_multiplex_channel.beam: $(CLONE_DIR)/ebin/pmod_pt.beam

endef
