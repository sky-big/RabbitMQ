# This file produces the makefile fragment associated with a package.
# It includes the package's package.mk, interprets all of the
# variables that package.mk might have set, and then visits any
# dependencies of the package that have not already been visited.
#
# PACKAGE_DIR should be set to the canonical path of the package.

# Mark that this package has been visited, so we can avoid doing it again
DONE_$(PACKAGE_DIR):=true

# Declare the standard per-package targets
.PHONY: $(PACKAGE_DIR)+dist $(PACKAGE_DIR)+clean $(PACKAGE_DIR)+clean-recursive

$(PACKAGE_DIR)+dist:: $(PACKAGE_DIR)/dist/.done

$(PACKAGE_DIR)+srcdist:: $(PACKAGE_DIR)/srcdist/.done

$(PACKAGE_DIR)+clean::

$(PACKAGE_DIR)+clean-with-deps:: $(PACKAGE_DIR)+clean

# Hook into the "all package" targets used by the main public-umbrella
# makefile
all-packages:: $(PACKAGE_DIR)/dist/.done
clean-all-packages:: $(PACKAGE_DIR)+clean

ifndef NON_INTEGRATED_$(PACKAGE_DIR)

PACKAGE_NAME=$(notdir $(abspath $(PACKAGE_DIR)))

# Set all the per-package vars to their default values

# The packages upon which this package depends
DEPS:=

# The name of the erlang application produced by the package
APP_NAME=$(call package_to_app_name,$(PACKAGE_NAME))

# The location of the .app file which is used as the basis for the
# .app file which goes into the .ez
ORIGINAL_APP_FILE=$(EBIN_DIR)/$(APP_NAME).app

# The location of the source for that file (before the modules list is
# generated). Ignored if DO_NOT_GENERATE_APP_FILE is set.
ORIGINAL_APP_SOURCE=$(PACKAGE_DIR)/src/$(APP_NAME).app.src

# Set to prevent generation of the app file.
DO_NOT_GENERATE_APP_FILE:=

# Should the .ez files for this package, its dependencies, and its
# source distribution be included in RabbitMQ releases, and should we test
# this plugin when invoking "make test" in the umbrella?
RELEASABLE:=

# The options to pass to erlc when compiling .erl files in this
# package
PACKAGE_ERLC_OPTS=$(ERLC_OPTS)

# The directories containing Erlang source files
SOURCE_DIRS:=$(PACKAGE_DIR)/src

# The Erlang source files to compile and include in the package .ez file
SOURCE_ERLS=$(strip $(foreach D,$(SOURCE_DIRS),$(wildcard $(D)/*.erl)))

# The directories containing Erlang *.hrl files to include in the
# package .ez file.
INCLUDE_DIRS:=$(PACKAGE_DIR)/include

# The Erlang .hrl files to include in the package .ez file.
INCLUDE_HRLS=$(strip $(foreach D,$(INCLUDE_DIRS),$(wildcard $(D)/*.hrl)))

# The location of the directory containing the .app file.  This is
# also where the .beam files produced by compiling SOURCE_ERLS will
# go.
EBIN_DIR:=$(PACKAGE_DIR)/ebin

# The .beam files for the application.
EBIN_BEAMS=$(patsubst %,$(EBIN_DIR)/%.beam,$(notdir $(basename $(SOURCE_ERLS))))

# Erlang expressions which will be invoked during testing (not in the
# broker).
STANDALONE_TEST_COMMANDS:=

# Erlang expressions which will be invoked within the broker during
# testing.
WITH_BROKER_TEST_COMMANDS:=

# Config file to give to the test broker.
WITH_BROKER_TEST_CONFIG:=

# Test scripts which should be invokedduring testing
STANDALONE_TEST_SCRIPTS:=

# Test scripts which should be invoked alongside a running broker
# during testing
WITH_BROKER_TEST_SCRIPTS:=

# Test scripts which should be invoked to configure the broker before testing
WITH_BROKER_SETUP_SCRIPTS:=

# When cleaning, should we also remove the cloned directory for
# wrappers?
PRESERVE_CLONE_DIR?=

# The directory within the package that contains tests
TEST_DIR=$(PACKAGE_DIR)/test

# The directories containing .erl files for tests
TEST_SOURCE_DIRS=$(TEST_DIR)/src

# The .erl files for tests
TEST_SOURCE_ERLS=$(strip $(foreach D,$(TEST_SOURCE_DIRS),$(wildcard $(D)/*.erl)))

# Where to put .beam files produced by compiling TEST_SOURCE_ERLS
TEST_EBIN_DIR=$(TEST_DIR)/ebin

# The .beam files produced by compiling TEST_SOURCE_ERLS
TEST_EBIN_BEAMS=$(patsubst %,$(TEST_EBIN_DIR)/%.beam,$(notdir $(basename $(TEST_SOURCE_ERLS))))

# Wrapper package variables

# The git URL to clone from.  Setting this variable marks the package
# as a wrapper package.
UPSTREAM_GIT:=

# The Mercurial URL to clone from.  Setting this variable marks the
# package as a wrapper package.
UPSTREAM_HG:=

UPSTREAM_TYPE=$(if $(UPSTREAM_GIT),git)$(if $(UPSTREAM_HG),hg)

# The upstream revision to clone.  Leave empty for default or master
UPSTREAM_REVISION:=

# Where to clone the upstream repository to
CLONE_DIR=$(PACKAGE_DIR)/$(patsubst %-wrapper,%,$(PACKAGE_NAME))-$(UPSTREAM_TYPE)

# The source directories contained in the cloned repositories.  These
# are appended to SOURCE_DIRS.
UPSTREAM_SOURCE_DIRS=$(CLONE_DIR)/src

# The include directories contained in the cloned repositories.  These
# are appended to INCLUDE_DIRS.
UPSTREAM_INCLUDE_DIRS=$(CLONE_DIR)/include

# Patches to apply to the upstream codebase after cloning, if any
WRAPPER_PATCHES:=

# The version number to assign to the build artifacts
PACKAGE_VERSION=$(VERSION)

# Should the app version incorporate the version from the original
# .app file?
RETAIN_ORIGINAL_VERSION:=

# The original version that should be incorporated into the package
# version if RETAIN_ORIGINAL_VERSION is set.  If empty, the original
# version will be extracted from ORIGINAL_APP_FILE.
ORIGINAL_VERSION:=

# For customising construction of the build application directory.
CONSTRUCT_APP_PREREQS:=
construct_app_commands=

package_rules=

# Now let the package makefile fragment do its stuff
include $(PACKAGE_DIR)/package.mk

# package_rules provides a convenient way to force prompt expansion
# of variables, including expansion in commands that would otherwise
# be deferred.
#
# If package_rules is defined by the package makefile, we expand it
# and eval it.  The point here is to get around the fact that make
# defers expansion of commands.  But if we use package variables in
# targets, as we naturally want to do, deferred expansion doesn't
# work: They might have been trampled on by a later package.  Because
# we expand package_rules here, references to package varialbes will
# get expanded with the values we expect.
#
# The downside is that any variable references for which expansion
# really should be deferred need to be protected by doulbing up the
# dollar.  E.g., inside package_rules, you should write $$@, not $@.
#
# We use the same trick again below.
ifdef package_rules
$(eval $(package_rules))
endif

# Some variables used for brevity below.  Packages can't set these.
APP_FILE=$(PACKAGE_DIR)/build/$(APP_NAME).app.$(PACKAGE_VERSION)
APP_DONE=$(PACKAGE_DIR)/build/app/.done.$(PACKAGE_VERSION)
APP_DIR=$(PACKAGE_DIR)/build/app/$(APP_NAME)-$(PACKAGE_VERSION)
EZ_FILE=$(PACKAGE_DIR)/dist/$(APP_NAME)-$(PACKAGE_VERSION).ez
DEPS_FILE=$(PACKAGE_DIR)/build/deps.mk


# Convert the DEPS package names to canonical paths
DEP_PATHS:=$(foreach DEP,$(DEPS),$(call package_to_path,$(DEP)))

# Handle RETAIN_ORIGINAL_VERSION / ORIGINAL_VERSION
ifdef RETAIN_ORIGINAL_VERSION

# Automatically acquire ORIGINAL_VERSION from ORIGINAL_APP_FILE
ifndef ORIGINAL_VERSION

# The generated ORIGINAL_VERSION setting goes in build/version.mk
$(eval $(call safe_include,$(PACKAGE_DIR)/build/version.mk))

$(PACKAGE_DIR)/build/version.mk: $(ORIGINAL_APP_FILE)
	sed -n -e 's|^.*{vsn, *"\([^"]*\)".*$$|ORIGINAL_VERSION:=\1|p' <$< >$@

$(APP_FILE): $(PACKAGE_DIR)/build/version.mk

endif # ifndef ORIGINAL_VERSION

PACKAGE_VERSION:=$(ORIGINAL_VERSION)-rmq$(VERSION)

endif # ifdef RETAIN_ORIGINAL_VERSION

# Handle wrapper packages
ifneq ($(UPSTREAM_TYPE),)

SOURCE_DIRS+=$(UPSTREAM_SOURCE_DIRS)
INCLUDE_DIRS+=$(UPSTREAM_INCLUDE_DIRS)

define package_rules

# We use --no-backup-if-mismatch to prevent .orig files ending up in
# source builds and causing warnings on Debian if the patches have
# fuzz.
ifdef UPSTREAM_GIT
$(CLONE_DIR)/.done:
	rm -rf $(CLONE_DIR)
	git clone $(UPSTREAM_GIT) $(CLONE_DIR)
	# Work around weird github breakage (bug 25264)
	cd $(CLONE_DIR) && git pull
	$(if $(UPSTREAM_REVISION),cd $(CLONE_DIR) && git checkout $(UPSTREAM_REVISION))
	$(if $(WRAPPER_PATCHES),$(foreach F,$(WRAPPER_PATCHES),patch -E -z .umbrella-orig -d $(CLONE_DIR) -p1 <$(PACKAGE_DIR)/$(F) &&) :)
	find $(CLONE_DIR) -name "*.umbrella-orig" -delete
	touch $$@
endif # UPSTREAM_GIT

ifdef UPSTREAM_HG
$(CLONE_DIR)/.done:
	rm -rf $(CLONE_DIR)
	hg clone -r $(or $(UPSTREAM_REVISION),default) $(UPSTREAM_HG) $(CLONE_DIR)
	$(if $(WRAPPER_PATCHES),$(foreach F,$(WRAPPER_PATCHES),patch -E -z .umbrella-orig -d $(CLONE_DIR) -p1 <$(PACKAGE_DIR)/$(F) &&) :)
	find $(CLONE_DIR) -name "*.umbrella-orig" -delete
	touch $$@
endif # UPSTREAM_HG

# When we clone, we need to remake anything derived from the app file
# (e.g. build/version.mk).
$(ORIGINAL_APP_FILE): $(CLONE_DIR)/.done

# We include the commit hash into the package version, via hash.mk
# (not in build/ because we want it to survive
#   make PRESERVE_CLONE_DIR=true clean
# for obvious reasons)
$(eval $(call safe_include,$(PACKAGE_DIR)/hash.mk))

$(PACKAGE_DIR)/hash.mk: $(CLONE_DIR)/.done
	@mkdir -p $$(@D)
ifdef UPSTREAM_GIT
	echo UPSTREAM_SHORT_HASH:=`git --git-dir=$(CLONE_DIR)/.git log -n 1 HEAD | grep commit | cut -b 8-14` >$$@
endif
ifdef UPSTREAM_HG
	echo UPSTREAM_SHORT_HASH:=`hg id -R $(CLONE_DIR) -i | cut -c -7` >$$@
endif

$(APP_FILE): $(PACKAGE_DIR)/hash.mk

PACKAGE_VERSION:=$(PACKAGE_VERSION)-$(UPSTREAM_TYPE)$(UPSTREAM_SHORT_HASH)

$(PACKAGE_DIR)+clean::
	[ "x" != "x$(PRESERVE_CLONE_DIR)" ] || rm -rf $(CLONE_DIR) hash.mk
endef # package_rules
$(eval $(package_rules))

endif # UPSTREAM_TYPE

# Generate a rule to compile .erl files from the directory $(1) into
# directory $(2), taking extra erlc options from $(3)
define package_source_dir_targets
$(2)/%.beam: $(1)/%.erl $(PACKAGE_DIR)/build/dep-apps/.done | $(DEPS_FILE)
	@mkdir -p $$(@D)
	ERL_LIBS=$(PACKAGE_DIR)/build/dep-apps $(ERLC) $(PACKAGE_ERLC_OPTS) $(foreach D,$(INCLUDE_DIRS),-I $(D)) -pa $$(@D) -o $$(@D) $(3) $$<

endef

$(eval $(foreach D,$(SOURCE_DIRS),$(call package_source_dir_targets,$(D),$(EBIN_DIR),)))
$(eval $(foreach D,$(TEST_SOURCE_DIRS),$(call package_source_dir_targets,$(D),$(TEST_EBIN_DIR),-pa $(EBIN_DIR))))

# Commands to run the broker for tests
#
# $(1): The value for RABBITMQ_SERVER_START_ARGS
# $(2): Extra env var settings when invoking the rabbitmq-server script
# $(3): Extra .ezs to copy into the plugins dir
define run_broker
	rm -rf $(TEST_TMPDIR)
	mkdir -p $(foreach D,log plugins $(NODENAME),$(TEST_TMPDIR)/$(D))
	cp -p $(PACKAGE_DIR)/dist/*.ez $(TEST_TMPDIR)/plugins
	$(call copy,$(3),$(TEST_TMPDIR)/plugins)
	rm -f $(TEST_TMPDIR)/plugins/rabbit_common*.ez
	RABBITMQ_PLUGINS_DIR=$(TEST_TMPDIR)/plugins \
	    RABBITMQ_ENABLED_PLUGINS_FILE=$(TEST_TMPDIR)/enabled_plugins \
	    $(UMBRELLA_BASE_DIR)/rabbitmq-server/scripts/rabbitmq-plugins \
	    set --offline $$$$(RABBITMQ_PLUGINS_DIR=$(TEST_TMPDIR)/plugins \
            RABBITMQ_ENABLED_PLUGINS_FILE=$(TEST_TMPDIR)/enabled_plugins \
	    $(UMBRELLA_BASE_DIR)/rabbitmq-server/scripts/rabbitmq-plugins list -m | tr '\n' ' ')
	MAKE="$(MAKE)" \
	  RABBITMQ_PLUGINS_DIR=$(TEST_TMPDIR)/plugins \
	  RABBITMQ_ENABLED_PLUGINS_FILE=$(TEST_TMPDIR)/enabled_plugins \
	  RABBITMQ_LOG_BASE=$(TEST_TMPDIR)/log \
	  RABBITMQ_MNESIA_BASE=$(TEST_TMPDIR)/$(NODENAME) \
	  RABBITMQ_PID_FILE=$(TEST_TMPDIR)/$(NODENAME).pid \
	  RABBITMQ_NODENAME=$(NODENAME) \
	  RABBITMQ_SERVER_START_ARGS=$(1) \
	  $(2) $(UMBRELLA_BASE_DIR)/rabbitmq-server/scripts/rabbitmq-server
endef

# Commands to run the package's test suite
#
# $(1): Extra .ezs to copy into the plugins dir
define run_with_broker_tests
$(if $(WITH_BROKER_TEST_COMMANDS)$(WITH_BROKER_TEST_SCRIPTS),$(call run_with_broker_tests_aux,$1))
endef

define run_with_broker_tests_aux
	$(call run_broker,'-pa $(TEST_EBIN_DIR) -coverage directories ["$(EBIN_DIR)"$(COMMA)"$(TEST_EBIN_DIR)"]',RABBITMQ_CONFIG_FILE=$(WITH_BROKER_TEST_CONFIG),$(1)) &
	$(UMBRELLA_BASE_DIR)/rabbitmq-server/scripts/rabbitmqctl -n $(NODENAME) wait $(TEST_TMPDIR)/$(NODENAME).pid
	echo > $(TEST_TMPDIR)/rabbit-test-output && \
	if $(foreach SCRIPT,$(WITH_BROKER_SETUP_SCRIPTS),$(SCRIPT) &&) \
	    $(foreach CMD,$(WITH_BROKER_TEST_COMMANDS), \
	     echo >> $(TEST_TMPDIR)/rabbit-test-output && \
	     echo "$(CMD)." \
               | tee -a $(TEST_TMPDIR)/rabbit-test-output \
               | $(ERL_CALL) $(ERL_CALL_OPTS) \
               | tee -a $(TEST_TMPDIR)/rabbit-test-output \
               | egrep "{ok, (ok|passed)}" >/dev/null &&) \
	    MAKE="$(MAKE)" \
	      $(foreach SCRIPT,$(WITH_BROKER_TEST_SCRIPTS),$(SCRIPT) &&) : ; \
        then \
	  touch $(TEST_TMPDIR)/.passed ; \
	  echo "\nPASSED\n" ; \
	else \
	  cat $(TEST_TMPDIR)/rabbit-test-output ; \
	  echo "\n\nFAILED\n" ; \
	fi
	sleep 1
	echo "rabbit_misc:report_cover(), init:stop()." | $(ERL_CALL) $(ERL_CALL_OPTS)
	sleep 1
	test -f $(TEST_TMPDIR)/.passed
endef

# The targets common to all integrated packages
define package_rules

# Put all relevant ezs into the dist dir for this package, including
# the main ez file produced by this package
#
# When the package version changes, our .ez filename will change, and
# we need to regenerate the dist directory.  So the dependency needs
# to go via a stamp file that incorporates the version in its name.
# But we need a target with a fixed name for other packages to depend
# on.  And it can't be a phony, as a phony will always get rebuilt.
# Hence the need for two stamp files here.
$(PACKAGE_DIR)/dist/.done: $(PACKAGE_DIR)/dist/.done.$(PACKAGE_VERSION)
	touch $$@

$(PACKAGE_DIR)/dist/.done.$(PACKAGE_VERSION): $(PACKAGE_DIR)/build/dep-ezs/.done $(APP_DONE)
	rm -rf $$(@D)
	mkdir -p $$(@D)
	cd $(dir $(APP_DIR)) && zip -q -r $$(abspath $(EZ_FILE)) $(notdir $(APP_DIR))
	$$(call copy,$$(wildcard $$(<D)/*.ez),$(PACKAGE_DIR)/dist)
	touch $$@

# Gather all the ezs from dependency packages
$(PACKAGE_DIR)/build/dep-ezs/.done: $(foreach P,$(DEP_PATHS),$(P)/dist/.done)
	rm -rf $$(@D)
	mkdir -p $$(@D)
	@echo [elided] copy dependent ezs
	@$(if $(DEP_PATHS),$(foreach P,$(DEP_PATHS),$$(call copy,$$(wildcard $(P)/dist/*.ez),$$(@D),&&)) :)
	touch $$@

# Put together the main app tree for this package
$(APP_DONE): $(EBIN_BEAMS) $(INCLUDE_HRLS) $(APP_FILE) $(CONSTRUCT_APP_PREREQS)
	rm -rf $$(@D)
	mkdir -p $(APP_DIR)/ebin $(APP_DIR)/include
	@echo [elided] copy beams to ebin
	@$(call copy,$(EBIN_BEAMS),$(APP_DIR)/ebin)
	cp -p $(APP_FILE) $(APP_DIR)/ebin/$(APP_NAME).app
	$(call copy,$(INCLUDE_HRLS),$(APP_DIR)/include)
	$(construct_app_commands)
	touch $$@

# Copy the .app file into place, set its version number
$(APP_FILE): $(ORIGINAL_APP_FILE)
	@mkdir -p $$(@D)
	sed -e 's|{vsn, *\"[^\"]*\"|{vsn,\"$(PACKAGE_VERSION)\"|' <$$< >$$@

ifndef DO_NOT_GENERATE_APP_FILE

# Generate the .app file. Note that this is a separate step from above
# so that the plugin still works correctly when symlinked as a directory
$(ORIGINAL_APP_FILE): $(ORIGINAL_APP_SOURCE) $(SOURCE_ERLS) $(UMBRELLA_BASE_DIR)/generate_app
	@mkdir -p $$(@D)
	escript $(UMBRELLA_BASE_DIR)/generate_app $$< $$@ $(SOURCE_DIRS)

$(PACKAGE_DIR)+clean::
	rm -f $(ORIGINAL_APP_FILE)

endif

# Unpack the ezs from dependency packages, so that their contents are
# accessible to erlc
$(PACKAGE_DIR)/build/dep-apps/.done: $(PACKAGE_DIR)/build/dep-ezs/.done
	rm -rf $$(@D)
	mkdir -p $$(@D)
	@echo [elided] unzip ezs
	@cd $$(@D) && $$(foreach EZ,$$(wildcard $(PACKAGE_DIR)/build/dep-ezs/*.ez),unzip -q $$(abspath $$(EZ)) &&) :
	touch $$@

# Dependency autogeneration.  This is complicated slightly by the need
# to generate a dependency file which is path-independent.
$(DEPS_FILE): $(SOURCE_ERLS) $(INCLUDE_HRLS) $(TEST_SOURCE_ERLS)
	@mkdir -p $$(@D)
	@echo [elided] generate deps
	@$$(if $$^,echo $$(subst : ,:,$$(foreach F,$$^,$$(abspath $$(F)):)) | escript $(abspath $(UMBRELLA_BASE_DIR)/generate_deps) $$@ '$$$$(EBIN_DIR)',echo >$$@)
	@echo [elided] fix test deps
	@$$(foreach F,$(TEST_EBIN_BEAMS),sed -e 's|^$$$$(EBIN_DIR)/$$(notdir $$(F)):|$$$$(TEST_EBIN_DIR)/$$(notdir $$(F)):|' $$@ > $$@.tmp && mv $$@.tmp $$@ && ) :
	sed -e 's|$$@|$$$$(DEPS_FILE)|' $$@ > $$@.tmp && mv $$@.tmp $$@

$(eval $(call safe_include,$(DEPS_FILE)))

$(PACKAGE_DIR)/srcdist/.done: $(PACKAGE_DIR)/srcdist/.done.$(PACKAGE_VERSION)
	touch $$@

$(PACKAGE_DIR)/srcdist/.done.$(PACKAGE_VERSION):
	mkdir -p $(PACKAGE_DIR)/build/srcdist/
	rsync -a --exclude '.hg*' --exclude '.git*' --exclude 'build' $(PACKAGE_DIR) $(PACKAGE_DIR)/build/srcdist/$(APP_NAME)-$(PACKAGE_VERSION)
	mkdir -p $(PACKAGE_DIR)/srcdist/
	tar cjf $(PACKAGE_DIR)/srcdist/$(APP_NAME)-$(PACKAGE_VERSION)-src.tar.bz2 -C $(PACKAGE_DIR)/build/srcdist/ $(APP_NAME)-$(PACKAGE_VERSION)
	touch $$@

$(PACKAGE_DIR)+clean::
	rm -rf $(EBIN_DIR)/*.beam $(TEST_EBIN_DIR)/*.beam $(PACKAGE_DIR)/dist $(PACKAGE_DIR)/srcdist $(PACKAGE_DIR)/build $(PACKAGE_DIR)/erl_crash.dump

$(PACKAGE_DIR)+clean-with-deps:: $(foreach P,$(DEP_PATHS),$(P)+clean-with-deps)

ifdef RELEASABLE
all-releasable:: $(PACKAGE_DIR)/dist/.done

copy-releasable:: $(PACKAGE_DIR)/dist/.done
	cp $(PACKAGE_DIR)/dist/*.ez $(PLUGINS_DIST_DIR)

copy-srcdist:: $(PLUGINS_SRC_DIST_DIR)/$(PACKAGE_DIR)/.srcdist_done

endif

$(PLUGINS_SRC_DIST_DIR)/$(PACKAGE_DIR)/.srcdist_done:: $(ORIGINAL_APP_FILE) $(foreach P,$(DEP_PATHS),$(PLUGINS_SRC_DIST_DIR)/$(P)/.srcdist_done)
	rsync -a --exclude '.hg*' --exclude '.git*' $(PACKAGE_DIR) $(PLUGINS_SRC_DIST_DIR)/
	[ -f $(PACKAGE_DIR)/license_info ] && cp $(PACKAGE_DIR)/license_info $(PLUGINS_SRC_DIST_DIR)/licensing/license_info_$(PACKAGE_NAME) || true
	find $(PACKAGE_DIR) -maxdepth 1 -name 'LICENSE-*' -exec cp '{}' $(PLUGINS_SRC_DIST_DIR)/licensing/ \;
	touch $(PLUGINS_SRC_DIST_DIR)/$(PACKAGE_DIR)/.srcdist_done

# A hook to allow packages to verify that prerequisites are satisfied
# before running.
.PHONY: $(PACKAGE_DIR)+pre-run
$(PACKAGE_DIR)+pre-run::

# Run erlang with the package, its tests, and all its dependencies
# available.
.PHONY: $(PACKAGE_DIR)+run
$(PACKAGE_DIR)+run: $(PACKAGE_DIR)/dist/.done $(TEST_EBIN_BEAMS) $(PACKAGE_DIR)+pre-run
	ERL_LIBS=$(PACKAGE_DIR)/dist $(ERL) $(ERL_OPTS) -pa $(TEST_EBIN_DIR)

# Run the broker with the package, its tests, and all its dependencies
# available.
.PHONY: $(PACKAGE_DIR)+run-in-broker
$(PACKAGE_DIR)+run-in-broker: $(PACKAGE_DIR)/dist/.done $(RABBITMQ_SERVER_PATH)/dist/.done $(TEST_EBIN_BEAMS)
	$(call run_broker,'-pa $(TEST_EBIN_DIR)',RABBITMQ_ALLOW_INPUT=true)

# A hook to allow packages to verify that prerequisites are satisfied
# before running tests.
.PHONY: $(PACKAGE_DIR)+pre-test
$(PACKAGE_DIR)+pre-test::

# Runs the package's tests that operate within (or in conjuction with)
# a running broker.
.PHONY: $(PACKAGE_DIR)+in-broker-test
$(PACKAGE_DIR)+in-broker-test: $(PACKAGE_DIR)/dist/.done $(RABBITMQ_SERVER_PATH)/dist/.done $(TEST_EBIN_BEAMS) $(PACKAGE_DIR)+pre-test $(PACKAGE_DIR)+standalone-test $(if $(RELEASABLE),$(call chain_test,$(PACKAGE_DIR)+in-broker-test))
	$(call run_with_broker_tests)

# Running the coverage tests requires Erlang/OTP R14. Note that
# coverage only covers the in-broker tests.
.PHONY: $(PACKAGE_DIR)+coverage
$(PACKAGE_DIR)+coverage: $(PACKAGE_DIR)/dist/.done $(COVERAGE_PATH)/dist/.done $(TEST_EBIN_BEAMS) $(PACKAGE_DIR)+pre-test
	$(call run_with_broker_tests,$(COVERAGE_PATH)/dist/*.ez)

# Runs the package's tests that don't need a running broker
.PHONY: $(PACKAGE_DIR)+standalone-test
$(PACKAGE_DIR)+standalone-test: $(PACKAGE_DIR)/dist/.done $(TEST_EBIN_BEAMS) $(PACKAGE_DIR)+pre-test $(if $(RELEASABLE),$(call chain_test,$(PACKAGE_DIR)+standalone-test))
	$$(if $(STANDALONE_TEST_COMMANDS),\
	  $$(foreach CMD,$(STANDALONE_TEST_COMMANDS),\
	    ERL_LIBS=$(PACKAGE_DIR)/dist $(ERL) -noinput $(ERL_OPTS) -pa $(TEST_EBIN_DIR) -sname standalone_test -eval "init:stop(case $$(CMD) of ok -> 0; passed -> 0; _Else -> 1 end)" &&\
	  )\
	:)
	$$(if $(STANDALONE_TEST_SCRIPTS),$$(foreach SCRIPT,$(STANDALONE_TEST_SCRIPTS),$$(SCRIPT) &&) :)

# Run all the package's tests
.PHONY: $(PACKAGE_DIR)+test
$(PACKAGE_DIR)+test:: $(PACKAGE_DIR)+standalone-test $(PACKAGE_DIR)+in-broker-test

.PHONY: $(PACKAGE_DIR)+check-xref
$(PACKAGE_DIR)+check-xref: $(PACKAGE_DIR)/dist/.done
	UNPACKDIR=$$$$(mktemp -d $(TMPDIR)/tmp.XXXXXXXXXX) && \
	for ez in $$$$(find $(PACKAGE_DIR)/dist -type f -name "*.ez"); do \
	  unzip -q $$$${ez} -d $$$${UNPACKDIR}; \
	done && \
	rm -rf $$$${UNPACKDIR}/rabbit_common-* && \
	ln -sf $$$$(pwd)/$(RABBITMQ_SERVER_PATH)/ebin $$$${UNPACKDIR} && \
	OK=true && \
	{ $(UMBRELLA_BASE_DIR)/check_xref $(PACKAGE_DIR) $$$${UNPACKDIR} || OK=false; } && \
	rm -rf $$$${UNPACKDIR} && \
	$$$${OK}

check-xref-packages:: $(PACKAGE_DIR)+check-xref

endef
$(eval $(package_rules))

# Recursing into dependency packages has to be the last thing we do
# because it will trample all over the per-package variables.

# Recurse into dependency packages
$(foreach DEP_PATH,$(DEP_PATHS),$(eval $(call do_package,$(DEP_PATH))))

else # NON_INTEGRATED_$(PACKAGE_DIR)

define package_rules

# When the package version changes, our .ez filename will change, and
# we need to regenerate the dist directory.  So the dependency needs
# to go via a stamp file that incorporates the version in its name.
# But we need a target with a fixed name for other packages to depend
# on.  And it can't be a phony, as a phony will always get rebuilt.
# Hence the need for two stamp files here.
$(PACKAGE_DIR)/dist/.done: $(PACKAGE_DIR)/dist/.done.$(VERSION)
	touch $$@

# Non-integrated packages (rabbitmq-server and rabbitmq-erlang-client)
# present a dilemma.  We could re-make the package every time we need
# it.  But that will cause a huge amount of unnecessary rebuilding.
# Or we could not worry about rebuilding non-integrated packages.
# That's good for those developing plugins, but not for those who want
# to work on the broker and erlang client in the context of the
# plugins.  So instead, we use a conservative approximation to the
# dependency structure within the package, to tell when to re-run the
# makefile.
$(PACKAGE_DIR)/dist/.done.$(VERSION): $(PACKAGE_DIR)/Makefile $(wildcard $(PACKAGE_DIR)/*.mk) $(wildcard $(PACKAGE_DIR)/src/*.erl) $(wildcard $(PACKAGE_DIR)/include/*.hrl) $(wildcard $(PACKAGE_DIR)/*.py) $(foreach DEP,$(NON_INTEGRATED_DEPS_$(PACKAGE_DIR)),$(call package_to_path,$(DEP))/dist/.done)
	rm -rf $$(@D)
	$$(MAKE) -C $(PACKAGE_DIR)
	mkdir -p $$(@D)
	touch $$@

# When building plugins-src we want to "make clean", but some
# non-integrated packages will not be there. Don't fall over in that case.
$(PACKAGE_DIR)+clean::
	if [ -d $(PACKAGE_DIR) ] ; then $$(MAKE) -C $(PACKAGE_DIR) clean ; fi
	rm -rf $(PACKAGE_DIR)/dist

endef
$(eval $(package_rules))

endif # NON_INTEGRATED_$(PACKAGE_DIR)
