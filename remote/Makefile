# fls-wh — a block-device write head, built for a QNX aarch64 target.
#
# Built from:
#   - this source (fls-wh.c, start.S)
#   - standard C/POSIX headers (include/)
#   - a minimal JSON symbol manifest (stubs/*.min.json)  <- source of truth
#   - open tooling: aarch64-elf-gcc (GCC) + ld.lld (LLVM) + stubs/so_stub.py
#
# The stub .so files are build artifacts (gitignored). At runtime, the
# target's dynamic linker resolves the SONAME (libc.so.5) to the library
# present on the system.
#
# Container build: `make container-build` builds the Containerfile with
# podman and runs the whole build inside it (fedora + cross toolchain).
#
# `make help` prints this file's `##`-annotated targets, grouped by
# `##@` section headers.

CC      := aarch64-elf-gcc
LD      := ld.lld
CFLAGS  := -I include
LDFLAGS := -dynamic-linker /usr/lib/ldqnx-64.so.2
STUBDIR := stubs

CONTAINER ?= podman
IMAGE     ?= fls-wh-builder

# host arch/os for the native binary name (flswh-<arch>-<os>)
UNAME_M := $(shell uname -m)
UNAME_S := $(shell uname -s)
NATIVE_ARCH := $(if $(filter arm64,$(UNAME_M)),aarch64,$(UNAME_M))
NATIVE_OS   := $(if $(filter Darwin,$(UNAME_S)),darwin,$(if $(filter Linux,$(UNAME_S)),linux,$(UNAME_S)))
NATIVE_BIN  := flswh-$(NATIVE_ARCH)-$(NATIVE_OS)

.PHONY: all clean stubs native container container-build test help

##@ build
all: flswh-aarch64-qnx7 ## build fls-wh for QNX aarch64 (default)

flswh-aarch64-qnx7: fls-wh.o start.o stubs ## link the QNX aarch64 binary
	$(LD) -o $@ fls-wh.o start.o -L$(STUBDIR) -l:libc.min.so_stub.so $(LDFLAGS)

native: $(NATIVE_BIN) ## build fls-wh for the host (system compiler, no stubs)

$(NATIVE_BIN): fls-wh.c
	cc -O2 $< -o $@

# build the minimal stub .so from the minimal JSON (artifacts, gitignored)
stubs: ## build the stub .so files from the JSON manifests
	python3 stubs/so_stub.py $(STUBDIR)/libc.min.json -o $(STUBDIR)/libc.min.so_stub.so

fls-wh.o: fls-wh.c
	$(CC) $(CFLAGS) -c $< -o $@

start.o: start.S
	$(CC) -c $< -o $@

clean: ## remove all build artifacts
	rm -f flswh-* fls-wh.o start.o $(STUBDIR)/*.so_stub.so*

##@ test
test: ## native test: build with the system compiler, run, assert
	python3 test.py

##@ container (podman)
container: ## build the fedora container image
	$(CONTAINER) build -t $(IMAGE) .

container-build: container ## build fls-wh inside the container (GNU ld)
	$(CONTAINER) run --rm -v $(PWD):/src -w /src $(IMAGE) make \
	LD=aarch64-linux-gnu-ld STUB_LD=aarch64-linux-gnu-ld

help: ## show this help
	@awk 'BEGIN {FS = ":.*##"} \
		/^##@/ { printf "\n\033[1;33m%s\033[0m\n", substr($$0, 5) } \
		/^[a-zA-Z0-9_-]+:.*##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
