# Makefile for fls - ARM64 static builds

.PHONY: all build clean test arm64-static lint fmt clippy check help compress-upx

# Default target
all: build

# Build for native target
build:
	cargo build --release

# Build and compress for native target
build-compressed: build
	@$(MAKE) compress-upx BINARY=target/release/fls OUTPUT=fls-native

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

# Build ARM64 static and compress with UPX
arm64-static-compressed: arm64-static
	@$(MAKE) compress-upx BINARY=fls-arm64-static OUTPUT=fls-arm64-static

# Build for all targets (native + ARM64 static)
all-targets:
	./build.sh

# Run tests
test:
	cargo test

# Run all lints (format check + clippy)
lint: fmt-check clippy

# Check code formatting
fmt-check:
	@echo "Checking code formatting..."
	cargo fmt -- --check

# Format code
fmt:
	@echo "Formatting code..."
	cargo fmt

# Run clippy linter
clippy:
	@echo "Running clippy..."
	cargo clippy --all-targets --all-features -- -D warnings

# Run cargo check (quick compile check)
check:
	@echo "Running cargo check..."
	cargo check --all-targets

# Compress a binary with UPX
# Usage: make compress-upx BINARY=path/to/binary OUTPUT=output-name
compress-upx:
	@if [ -z "$(BINARY)" ] || [ -z "$(OUTPUT)" ]; then \
		echo "Error: BINARY and OUTPUT must be specified"; \
		echo "Usage: make compress-upx BINARY=path/to/binary OUTPUT=output-name"; \
		exit 1; \
	fi
	@if ! command -v upx >/dev/null 2>&1; then \
		echo "Error: UPX not found. Install it first:"; \
		echo "  macOS: brew install upx"; \
		echo "  Linux: apt-get install upx-ucl or download from https://github.com/upx/upx/releases"; \
		exit 1; \
	fi
	@if [ -f "$(BINARY)" ]; then \
		if [ "$(BINARY)" != "$(OUTPUT)" ]; then \
			cp "$(BINARY)" "$(OUTPUT)"; \
		fi; \
		echo "📦 Original size:"; \
		ls -lh "$(OUTPUT)"; \
		echo "🗜️  Compressing with UPX --best --lzma..."; \
		if upx --best --lzma "$(OUTPUT)"; then \
			echo "✅ Compression successful!"; \
			ls -lh "$(OUTPUT)"; \
		else \
			echo "⚠️  UPX compression failed"; \
			exit 1; \
		fi; \
	else \
		echo "Error: Binary $(BINARY) not found"; \
		exit 1; \
	fi

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
	@echo ""
	@echo "Build targets:"
	@echo "  build                    - Build for native target"
	@echo "  build-compressed         - Build for native target and compress with UPX"
	@echo "  arm64-static             - Build for ARM64 static (musl)"
	@echo "  arm64-static-compressed  - Build for ARM64 static and compress with UPX"
	@echo "  all-targets              - Build for native + ARM64 static"
	@echo ""
	@echo "Compression:"
	@echo "  compress-upx             - Compress a binary with UPX (requires BINARY and OUTPUT args)"
	@echo ""
	@echo "Testing and Quality:"
	@echo "  test                     - Run tests"
	@echo "  lint                     - Run all lints (format check + clippy)"
	@echo "  fmt                      - Format code with rustfmt"
	@echo "  fmt-check                - Check code formatting without modifying files"
	@echo "  clippy                   - Run clippy linter"
	@echo "  check                    - Quick compile check"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean                    - Clean build artifacts"
	@echo "  install-deps             - Install cross-compilation dependencies"
	@echo "  help                     - Show this help"
