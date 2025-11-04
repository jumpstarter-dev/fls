FROM rust:1.90-slim

# Install cross-compilation tools for ARM64 glibc
RUN apt-get update && apt-get install -y \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    libssl-dev \
    pkg-config \
    make \
    perl \
    file \
    upx-ucl \
    && rm -rf /var/lib/apt/lists/*

# Add ARM64 glibc target
RUN rustup target add aarch64-unknown-linux-gnu

# Set up cross-compilation environment for glibc static linking
ENV CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc
ENV CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++
ENV AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
# Force static linking with glibc
# Note: glibc static linking is more complex than musl, but OpenSSL works better with glibc
ENV RUSTFLAGS="-C linker=aarch64-linux-gnu-gcc -C link-arg=-static -C link-arg=-lpthread"
# Enable static linking for OpenSSL (vendored feature is set in Cargo.toml)
ENV OPENSSL_STATIC=1

WORKDIR /app

# Copy only dependency manifests first
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo

# Create a dummy source file to allow dependency compilation
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies only (this layer will be cached)
RUN cargo build --release --target aarch64-unknown-linux-gnu

# Remove the dummy source and build artifacts (keep dependencies cached)
RUN rm -rf src target/aarch64-unknown-linux-gnu/release/fls* \
    target/aarch64-unknown-linux-gnu/release/deps/fls*

# Now copy the real source code and Makefile
COPY Makefile .
COPY src src

# Build the actual project (dependencies are already cached)
RUN cargo build --release --target aarch64-unknown-linux-gnu

# Compress the binary with UPX for smaller size
# Using --best for maximum compression, --lzma for best algorithm
# UPX compresses in-place, so the original binary will be compressed
RUN cd target/aarch64-unknown-linux-gnu/release && \
    upx --best --lzma fls || \
    echo "UPX compression failed, using uncompressed binary"

# The compressed binary will be in target/aarch64-unknown-linux-gnu/release/fls

