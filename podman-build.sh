#!/bin/bash

# Manual Podman build for ARM64 static binary
set -e -x

echo "Building ARM64 static binary using Podman..."

# Build the Podman image
echo "Building Podman image..."
podman build -f Containerfile -t fls-builder .

# Extract the binary using base64 encoding (works better than volume mounting)
echo "Extracting ARM64 binary..."
podman run --rm fls-builder base64 target/aarch64-unknown-linux-gnu/release/fls | base64 -d > fls-arm64-static

# Make it executable
chmod +x fls-arm64-static

echo "✓ ARM64 static binary saved as: fls-arm64-static"
file fls-arm64-static
ls -lh fls-arm64-static
