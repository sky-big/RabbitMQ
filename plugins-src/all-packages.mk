UMBRELLA_BASE_DIR:=.

include common.mk

CHAIN_TESTS:=true

# Pull in all the packages
$(foreach PACKAGE_MK,$(wildcard */package.mk),$(eval $(call do_package,$(call canonical_path,$(patsubst %/,%,$(dir $(PACKAGE_MK)))))))

# ...and the non-integrated ones
$(foreach V,$(.VARIABLES),$(if $(filter NON_INTEGRATED_%,$(filter-out NON_INTEGRATED_DEPS_%,$V)),$(eval $(call do_package,$(subst NON_INTEGRATED_,,$V)))))

test-all-packages: $(CHAINED_TESTS)
