.PHONY: default
default:
	@echo No default target && false

REPOS:= \
    rabbitmq-server \
    rabbitmq-codegen \
    rabbitmq-java-client \
    rabbitmq-dotnet-client \
    rabbitmq-test \
    cowboy-wrapper \
    eldap-wrapper \
    mochiweb-wrapper \
    rabbitmq-amqp1.0 \
    rabbitmq-auth-backend-ldap \
    rabbitmq-auth-mechanism-ssl \
    rabbitmq-consistent-hash-exchange \
    rabbitmq-erlang-client \
    rabbitmq-federation \
    rabbitmq-federation-management \
    rabbitmq-management \
    rabbitmq-management-agent \
    rabbitmq-management-visualiser \
    rabbitmq-metronome \
    rabbitmq-web-dispatch \
    rabbitmq-mqtt \
    rabbitmq-shovel \
    rabbitmq-shovel-management \
    rabbitmq-stomp \
    rabbitmq-toke \
    rabbitmq-tracing \
    rabbitmq-web-stomp \
    rabbitmq-web-stomp-examples \
    sockjs-erlang-wrapper \
    toke \
    webmachine-wrapper

BRANCH:=master

UMBRELLA_REPO_FETCH:=$(shell git remote -v 2>/dev/null | awk '/^origin\t.+ \(fetch\)$$/ { print $$2; }')
ifdef UMBRELLA_REPO_FETCH
GIT_CORE_REPOBASE_FETCH:=$(shell dirname $(UMBRELLA_REPO_FETCH))
GIT_CORE_SUFFIX_FETCH:=$(suffix $(UMBRELLA_REPO_FETCH))
else
GIT_CORE_REPOBASE_FETCH:=https://github.com/rabbitmq
GIT_CORE_SUFFIX_FETCH:=.git
endif

UMBRELLA_REPO_PUSH:=$(shell git remote -v 2>/dev/null | awk '/^origin\t.+ \(push\)$$/ { print $$2; }')
ifdef UMBRELLA_REPO_PUSH
GIT_CORE_REPOBASE_PUSH:=$(shell dirname $(UMBRELLA_REPO_PUSH))
GIT_CORE_SUFFIX_PUSH:=$(suffix $(UMBRELLA_REPO_PUSH))
else
GIT_CORE_REPOBASE_PUSH:=git@github.com:rabbitmq
GIT_CORE_SUFFIX_PUSH:=.git
endif

VERSION:=0.0.0

#----------------------------------

all:
	$(MAKE) -f all-packages.mk all-packages VERSION=$(VERSION)

test:
	$(MAKE) -f all-packages.mk test-all-packages VERSION=$(VERSION)

release:
	$(MAKE) -f all-packages.mk all-releasable VERSION=$(VERSION)

clean:
	$(MAKE) -f all-packages.mk clean-all-packages

check-xref:
	$(MAKE) -f all-packages.mk check-xref-packages

plugins-dist: release
	rm -rf $(PLUGINS_DIST_DIR)
	mkdir -p $(PLUGINS_DIST_DIR)
	$(MAKE) -f all-packages.mk copy-releasable VERSION=$(VERSION) PLUGINS_DIST_DIR=$(PLUGINS_DIST_DIR)

plugins-srcdist:
	rm -rf $(PLUGINS_SRC_DIST_DIR)
	mkdir -p $(PLUGINS_SRC_DIST_DIR)/licensing

	rsync -a --exclude '.git*' rabbitmq-erlang-client $(PLUGINS_SRC_DIST_DIR)/
	touch $(PLUGINS_SRC_DIST_DIR)/rabbitmq-erlang-client/.srcdist_done

	rsync -a --exclude '.git*' rabbitmq-server $(PLUGINS_SRC_DIST_DIR)/
	touch $(PLUGINS_SRC_DIST_DIR)/rabbitmq-server/.srcdist_done

	$(MAKE) -f all-packages.mk copy-srcdist VERSION=$(VERSION) PLUGINS_SRC_DIST_DIR=$(PLUGINS_SRC_DIST_DIR)
	cp Makefile *.mk generate* $(PLUGINS_SRC_DIST_DIR)/
	echo "This is the released version of rabbitmq-public-umbrella. \
You can clone the full version with: git clone https://github.com/rabbitmq/rabbitmq-public-umbrella.git" > $(PLUGINS_SRC_DIST_DIR)/README

	PRESERVE_CLONE_DIR=1 $(MAKE) -C $(PLUGINS_SRC_DIST_DIR) clean
	rm -rf $(PLUGINS_SRC_DIST_DIR)/rabbitmq-server

#----------------------------------
# Convenience aliases

.PHONY: co
co: checkout

.PHONY: ci
ci: checkin

.PHONY: up
up: update

.PHONY: st
st: status

.PHONY: up_c
up_c: named_update

#----------------------------------

$(REPOS):
	retries=5; \
	while ! git clone $(GIT_CORE_REPOBASE_FETCH)/$@$(GIT_CORE_SUFFIX_FETCH); do \
	  retries=$$((retries - 1)); \
	  if test "$$retries" = 0; then break; fi; \
	  sleep 1; \
	done
	test -d $@
	global_user_name="$$(git config --global user.name)"; \
	global_user_email="$$(git config --global user.email)"; \
	user_name="$$(git config user.name)"; \
	user_email="$$(git config user.email)"; \
	cd $@ && \
	git remote set-url --push origin $(GIT_CORE_REPOBASE_PUSH)/$@$(GIT_CORE_SUFFIX_PUSH) && \
	if test "$$global_user_name" != "$$user_name"; then git config user.name "$$user_name"; fi && \
	if test "$$global_user_email" != "$$user_email"; then git config user.email "$$user_email"; fi


.PHONY: checkout
checkout: $(REPOS)

.PHONY: list-repos
list-repos:
	@for repo in $(REPOS); do echo $$repo; done

.PHONY: sync-gituser
sync-gituser:
	@global_user_name="$$(git config --global user.name)"; \
	global_user_email="$$(git config --global user.email)"; \
	user_name="$$(git config user.name)"; \
	user_email="$$(git config user.email)"; \
	for repo in $(REPOS); do \
	cd $$repo && \
	git config --unset user.name && \
	git config --unset user.email && \
	if test "$$global_user_name" != "$$user_name"; then git config user.name "$$user_name"; fi && \
	if test "$$global_user_email" != "$$user_email"; then git config user.email "$$user_email"; fi && \
	cd ..; done

.PHONY: sync-gitremote
sync-gitremote:
	@for repo in $(REPOS); do \
	cd $$repo && \
	git remote set-url origin $(GIT_CORE_REPOBASE_FETCH)/$$repo$(GIT_CORE_SUFFIX_FETCH) && \
	git remote set-url --push origin $(GIT_CORE_REPOBASE_PUSH)/$$repo$(GIT_CORE_SUFFIX_PUSH) && \
	cd ..; done

#----------------------------------
# Subrepository management


# $(1) is the target
# $(2) is the target dependency. Can use % to get current REPO
# $(3) is the target body. Can use % to get current REPO
define repo_target

.PHONY: $(1)
$(1): $(2)
	$(3)

endef

# $(1) is the list of repos
# $(2) is the suffix
# $(3) is the target dependency. Can use % to get current REPO
# $(4) is the target body. Can use % to get current REPO
define repo_targets
$(foreach REPO,$(1),$(call repo_target,$(REPO)+$(2),\
	$(patsubst %,$(3),$(REPO)),$(patsubst %,$(4),$(REPO))))
endef

# Do not allow status to fork with -j otherwise output will be garbled
.PHONY: status
status: checkout
	@for repo in . $(REPOS); do \
		echo "$$repo:"; \
		cd "$$repo" && git status -s && cd - >/dev/null; \
	done

.PHONY: pull
pull: $(foreach DIR,. $(REPOS),$(DIR)+pull)

$(eval $(call repo_targets,. $(REPOS),pull,| %,\
	(cd % && git pull --ff-only)))

.PHONY: update
update: pull

.PHONY: named_update
named_update: $(foreach DIR,. $(REPOS),$(DIR)+named_update)

$(eval $(call repo_targets,. $(REPOS),named_update,| %,\
	(cd % && git fetch -p && git checkout $(BRANCH) && \
	 (test "$$$$(git branch | grep '^*')" = "* (detached from $(BRANCH))" || \
	 git pull --ff-only))))

.PHONY: tag
tag: $(foreach DIR,. $(REPOS),$(DIR)+tag)

$(eval $(call repo_targets,. $(REPOS),tag,| %,\
	(cd % && git tag $(TAG))))

.PHONY: push
push: $(foreach DIR,. $(REPOS),$(DIR)+push)

$(eval $(call repo_targets,. $(REPOS),push,| %,\
	(cd % && git push && git push --tags)))

.PHONY: checkin
checkin: $(foreach DIR,. $(REPOS),$(DIR)+checkin)

$(eval $(call repo_targets,. $(REPOS),checkin,| %,\
	(cd % && (test -z "$$$$(git status -s -uno)" || git commit -a))))
