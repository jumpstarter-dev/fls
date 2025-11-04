# Makefile for fls - ARM64 static builds

.PHONY: all build clean test arm64-static help

# Default target
all: build

# Build for native target
build:
	cargo build --release

# Build for ARM64 static (musl) using cross
arm64-static:
	rustup target add aarch64-unknown-linux-musl
	cross build --release --target aarch64-unknown-linux-musl
	@if [ -f "target/aarch64-unknown-linux-musl/release/fls" ]; then \
		cp "target/aarch64-unknown-linux-musl/release/fls" "fls-arm64-static"; \
		echo "✓ ARM64 static binary saved as: fls-arm64-static"; \
		file fls-arm64-static; \
		ls -lh fls-arm64-static; \
	fi

# Build for all targets (native + ARM64 static)
all-targets:
	./build.sh

# Run tests
test:
	cargo test

# Clean build artifacts
clean:
	cargo clean
	rm -f fls-*

# Install dependencies for ARM64 cross-compilation
install-deps:
	@echo "Installing cross for cross-compilation..."
	cargo install cross
	@echo "Make sure Podman is installed and running:"
	@echo "  macOS: brew install podman"
	@echo "  Then run: podman machine init && podman machine start"

# Show help
help:
	@echo "Available targets:"
	@echo "  build        - Build for native target"
	@echo "  arm64-static - Build for ARM64 static (musl) - single binary, no dependencies"
	@echo "  all-targets  - Build for native + ARM64 static"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  install-deps - Install ARM64 musl cross-compilation dependencies (Ubuntu/Debian)"
	@echo "  help         - Show this help"
