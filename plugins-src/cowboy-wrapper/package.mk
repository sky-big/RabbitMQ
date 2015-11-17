APP_NAME:=cowboy

UPSTREAM_GIT:=https://github.com/rabbitmq/cowboy.git
UPSTREAM_REVISION:=4b93c2d19a10e5d9cee
RETAIN_ORIGINAL_VERSION:=true
WRAPPER_PATCHES:=\
	0001-R12-fake-iodata-type.patch \
	0002-R12-drop-all-references-to-boolean-type.patch \
	0003-R12-drop-all-references-to-reference-type.patch \
	0004-R12-drop-references-to-iodata-type.patch \
	0005-R12-drop-references-to-Default-any-type.patch \
	0006-Use-erlang-integer_to_list-and-lists-max-instead-of-.patch \
	0007-R12-type-definitions-must-be-ordered.patch \
	0008-sec-websocket-protocol.patch

# Path include/http.hrl is needed during compilation
INCLUDE_DIRS+=$(CLONE_DIR)

ORIGINAL_APP_FILE:=$(CLONE_DIR)/src/$(APP_NAME).app.src
DO_NOT_GENERATE_APP_FILE=true

define construct_app_commands
	cp $(CLONE_DIR)/LICENSE $(APP_DIR)/LICENSE-ISC-Cowboy
endef
