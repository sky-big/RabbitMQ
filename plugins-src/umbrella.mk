# The default goal
dist:

UMBRELLA_BASE_DIR:=..

include $(UMBRELLA_BASE_DIR)/common.mk

# We start at the initial package (i.e. the one in the current directory)
PACKAGE_DIR:=$(call canonical_path,.)

# Produce all of the releasable artifacts of this package
.PHONY: dist
dist: $(PACKAGE_DIR)+dist

# Produce a source tarball for this package
.PHONY: srcdist
srcdist: $(PACKAGE_DIR)+srcdist

# Clean the package and all its dependencies
.PHONY: clean
clean: $(PACKAGE_DIR)+clean-with-deps

# Clean just the initial package
.PHONY: clean-local
clean-local: $(PACKAGE_DIR)+clean

# Run erlang with the package, its tests, and all its dependencies
# available.
.PHONY: run
run: $(PACKAGE_DIR)+run

# Run the broker with the package, its tests, and all its dependencies
# available.
.PHONY: run-in-broker
run-in-broker: $(PACKAGE_DIR)+run-in-broker

# Runs the package's tests
.PHONY: test
test: $(PACKAGE_DIR)+test

# Test the package with code coverage recording on.  Note that
# coverage only covers the in-broker tests.
.PHONY: coverage
coverage: $(PACKAGE_DIR)+coverage

# Runs the package's tests
.PHONY: check-xref
check-xref: $(PACKAGE_DIR)+check-xref

# Do the initial package
include $(UMBRELLA_BASE_DIR)/do-package.mk

# We always need the coverage package to support the coverage goal
PACKAGE_DIR:=$(COVERAGE_PATH)
$(eval $(call do_package,$(COVERAGE_PATH)))
