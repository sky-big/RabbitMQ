APP_NAME:=eldap

UPSTREAM_GIT:=https://github.com/rabbitmq/eldap.git
UPSTREAM_REVISION:=e309de4db4b78d67d623
WRAPPER_PATCHES:=eldap-appify.patch remove-eldap-fsm.patch eldap-no-ssl-seed.patch remove-ietf-doc.patch

ORIGINAL_APP_FILE:=$(CLONE_DIR)/ebin/$(APP_NAME).app
DO_NOT_GENERATE_APP_FILE=true

GENERATED_DIR:=$(CLONE_DIR)/generated
PACKAGE_ERLC_OPTS+=-I $(GENERATED_DIR)
INCLUDE_HRLS+=$(GENERATED_DIR)/ELDAPv3.hrl
EBIN_BEAMS+=$(GENERATED_DIR)/ELDAPv3.beam

define package_rules

$(CLONE_DIR)/src/ELDAPv3.asn: $(CLONE_DIR)/.done

$(GENERATED_DIR)/ELDAPv3.hrl $(GENERATED_DIR)/ELDAPv3.beam: $(CLONE_DIR)/src/ELDAPv3.asn
	@mkdir -p $(GENERATED_DIR)
	$(ERLC) $(PACKAGE_ERLC_OPTS) -o $(GENERATED_DIR) $$<

$(PACKAGE_DIR)+clean::
	rm -rf $(GENERATED_DIR) $(EBIN_DIR)

# This rule is run *before* the one in do_package.mk
$(PLUGINS_SRC_DIST_DIR)/$(PACKAGE_DIR)/.srcdist_done::
	cp $(CLONE_DIR)/LICENSE $(PACKAGE_DIR)/LICENSE-MIT-eldap

endef
