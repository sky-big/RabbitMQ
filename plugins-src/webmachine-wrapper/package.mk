APP_NAME:=webmachine
DEPS:=mochiweb-wrapper

UPSTREAM_GIT:=https://github.com/rabbitmq/webmachine.git
UPSTREAM_REVISION:=e9359c7092b228f671417abe68319913f1aebe46
RETAIN_ORIGINAL_VERSION:=true

WRAPPER_PATCHES:=10-remove-crypto-dependency.patch

ORIGINAL_APP_FILE=$(CLONE_DIR)/src/$(APP_NAME).app.src
DO_NOT_GENERATE_APP_FILE=true

define package_rules

# This rule is run *before* the one in do_package.mk
$(PLUGINS_SRC_DIST_DIR)/$(PACKAGE_DIR)/.srcdist_done::
	cp $(CLONE_DIR)/LICENSE $(PACKAGE_DIR)/LICENSE-Apache-Basho

endef
