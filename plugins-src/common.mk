# Various global definitions

# UMBRELLA_BASE_DIR should be set to the path of the
# rabbitmq-public-umbrella directory before this file is included.

# Make version check
REQUIRED_MAKE_VERSION:=3.81
ifneq ($(shell ( echo "$(MAKE_VERSION)" ; echo "$(REQUIRED_MAKE_VERSION)" ) | sort -t. -n | head -1),$(REQUIRED_MAKE_VERSION))
$(error GNU make version $(REQUIRED_MAKE_VERSION) required)
endif

# This is the standard trick for making pattern substitution work
# (amongst others) when the replacement needs to include a comma.
COMMA:=,

# Global settings that can be overridden on the command line

# These ones are expected to be passed down to the sub-makes invoked
# for non-integrated packages
VERSION ?= 0.0.0
ERL ?= erl
ERL_OPTS ?=
ERLC ?= erlc
ERLC_OPTS ?= -Wall +debug_info
TMPDIR ?= /tmp

NODENAME ?= rabbit-test
ERL_CALL ?= erl_call
ERL_CALL_OPTS ?= -sname $(NODENAME) -e

# Where we put all the files produced when running tests.
TEST_TMPDIR=$(TMPDIR)/rabbitmq-test

# Callable functions

# Convert a package name to the corresponding erlang app name
define package_to_app_name
$(subst -,_,$(1))
endef

# If the variable named $(1) holds a non-empty value, return it.
# Otherwise, set the variable to $(2) and return that value.
define memoize
$(if $($(1)),$($(1)),$(eval $(1):=$(2))$(2))
endef

# Return a canonical form for the path in $(1)
#
# Absolute path names can be a bit verbose.  This provides a way to
# canonicalize path names with more concise results.
define canonical_path
$(call memoize,SHORT_$(realpath $(1)),$(1))
endef

# Convert a package name to a path name
define package_to_path
$(call canonical_path,$(UMBRELLA_BASE_DIR)/$(1))
endef

# Produce a cp command to copy from $(1) to $(2), unless $(1) is
# empty, in which case do nothing.
#
# The optional $(3) gives a suffix to append to the command, if a
# command is produced.
define copy
$(if $(1),cp -r $(1) $(2)$(if $(3), $(3)))
endef

# Produce the makefile fragment for the package with path in $(1), if
# it hasn't already been visited.  The path should have been
# canonicalized via canonical_path.
define do_package
# Have we already visited this package?  If so, skip it
ifndef DONE_$(1)
PACKAGE_DIR:=$(1)
include $(UMBRELLA_BASE_DIR)/do-package.mk
endif
endef

# This is used to chain test rules, so that test-all-packages works in
# the presence of 'make -j'
define chain_test
$(if $(CHAIN_TESTS),$(CHAINED_TESTS)$(eval CHAINED_TESTS+=$(1)))
endef

# Mark the non-integrated repos
NON_INTEGRATED_$(call package_to_path,rabbitmq-server):=true
NON_INTEGRATED_$(call package_to_path,rabbitmq-erlang-client):=true
NON_INTEGRATED_$(call package_to_path,rabbitmq-java-client):=true
NON_INTEGRATED_$(call package_to_path,rabbitmq-dotnet-client):=true
NON_INTEGRATED_DEPS_$(call package_to_path,rabbitmq-erlang-client):=rabbitmq-server

# Where the coverage package lives
COVERAGE_PATH:=$(call package_to_path,coverage)

# Where the rabbitmq-server package lives
RABBITMQ_SERVER_PATH=$(call package_to_path,rabbitmq-server)

# Cleaning support
ifndef MAKECMDGOALS
TESTABLEGOALS:=$(.DEFAULT_GOAL)
else
TESTABLEGOALS:=$(MAKECMDGOALS)
endif

# The CLEANING variable can be used to determine whether the top-level
# goal is cleaning related.  In particular, it can be used to prevent
# including generated files when cleaning, which might otherwise
# trigger undesirable activity.
ifeq "$(strip $(patsubst clean%,,$(patsubst %clean,,$(TESTABLEGOALS))))" ""
CLEANING:=true
endif

# Include a generated makefile fragment
#
# Note that this includes using "-include", and thus make will proceed
# even if an error occurs while the fragment is being re-made (we
# don't use "include" becuase it will produce a superfluous error
# message when the fragment is re-made because it doesn't exist).
# Thus you should also list the fragment as a dependency of any rules
# that will refer to the contents of the fragment.
define safe_include
ifndef CLEANING
-include $(1)

# If we fail to make the fragment, make will just loop trying to
# create it.  So we have to explicitly catch that case.
$$(if $$(MAKE_RESTARTS),$$(if $$(wildcard $(1)),,$$(error Failed to produce $(1))))

endif
endef

# This is not the make default, but it is a good idea
.DELETE_ON_ERROR:

# Declarations for global targets
.PHONY: all-releasable copy-releasable copy-srcdist all-packages clean-all-packages
all-releasable::
copy-releasable::
copy-srcdist::
all-packages::
clean-all-packages::
check-xref-packages::
