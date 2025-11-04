#!/bin/bash

# Manual Podman build for ARM64 static binary
set -e

echo "Building ARM64 static binary using Podman..."

# Build the Podman image
echo "Building Podman image..."
podman build -f Containerfile -t smallrs-builder .

# Extract the binary using base64 encoding (works better than volume mounting)
echo "Extracting ARM64 binary..."
podman run --rm smallrs-builder base64 target/aarch64-unknown-linux-gnu/release/smallrs | base64 -d > smallrs-arm64-static

# Make it executable
chmod +x smallrs-arm64-static

echo "✓ ARM64 static binary saved as: smallrs-arm64-static"
file smallrs-arm64-static
ls -lh smallrs-arm64-static
