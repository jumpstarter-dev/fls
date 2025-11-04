#!/bin/bash

# Build script for fls - ARM64 static builds only
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building fls for ARM64 static target...${NC}"

# Build for native target first
echo -e "${YELLOW}Building for native target...${NC}"
cargo build --release
if [ -f "target/release/fls" ]; then
    cp "target/release/fls" "fls-native"
    echo -e "${GREEN}✓ Native binary saved as: fls-native${NC}"
fi

# Build for ARM64 static (musl)
echo -e "${YELLOW}Building for ARM64 static (musl) using cross...${NC}"

# Add the target if not already installed
rustup target add aarch64-unknown-linux-musl 2>/dev/null || true

# Build for the target using cross
if cross build --release --target aarch64-unknown-linux-musl; then
    echo -e "${GREEN}✓ Successfully built ARM64 static binary${NC}"
    
    # Copy binary to a target-specific name
    if [ -f "target/aarch64-unknown-linux-musl/release/fls" ]; then
        cp "target/aarch64-unknown-linux-musl/release/fls" "fls-arm64-static"
        echo -e "${GREEN}  ARM64 static binary saved as: fls-arm64-static${NC}"
        
        # Show binary info
        echo -e "${YELLOW}Binary information:${NC}"
        file fls-arm64-static
        ls -lh fls-arm64-static
    fi
else
    echo -e "${RED}✗ Failed to build ARM64 static binary${NC}"
    echo -e "${YELLOW}Make sure you have Podman installed and running:${NC}"
    echo -e "${YELLOW}  macOS: brew install podman${NC}"
    echo -e "${YELLOW}  Then run: podman machine init && podman machine start${NC}"
    echo -e "${YELLOW}  Or run: cross build --target aarch64-unknown-linux-musl --release${NC}"
    exit 1
fi

echo -e "${GREEN}Build complete! Available binaries:${NC}"
ls -la fls-* 2>/dev/null || echo "No binaries found"
