# fls - The Fast Flash Tool

A high-performance command-line tool for flashing disk images to block devices. `fls` can download images from URLs, decompress them on-the-fly, and write directly to block devices with optimized buffering and progress reporting.

## Features

- **Stream-based flashing**: Download, decompress, and write in parallel
- **Multiple compression formats**: Supports `.xz`, `.gz`, `.bz2`, and more
- **Progress monitoring**: Real-time progress bars for download, decompression, and write operations
- **Automatic retries**: Built-in retry logic for network failures
- **Optimized I/O**: Configurable buffer sizes for optimal performance

## Installation

### From GitHub Releases

Download the latest release for your architecture:

```bash
# For ARM64/aarch64 systems
curl -L https://github.com/jumpstarter-dev/fls/releases/download/0.1.5/fls-aarch64-linux -o /usr/local/bin/fls
chmod +x /usr/local/bin/fls

# For x86_64 systems
curl -L https://github.com/jumpstarter-dev/fls/releases/download/0.1.5/fls-x86_64-linux -o /usr/local/bin/fls
chmod +x /usr/local/bin/fls
```

Replace `0.1.5` with the desired version from the [releases page](https://github.com/jumpstarter-dev/fls/releases).

### From Source

```bash
cargo build --release
sudo cp target/release/fls /usr/local/bin/
```

## Usage

### Basic Example

Flash a compressed image from a URL to a block device:

```bash
fls from-url \
  -k \
  -n \
  "https://example.com/path/to/image.raw.xz" \
  /dev/mmcblk1
```

**Flags used:**
- `-k` - Skip SSL certificate verification (useful for internal servers with self-signed certs)
- `-n` - Print progress on new lines (better for logging)

### Advanced Example

Flash with custom headers and progress interval:

```bash
fls from-url \
  --header "Authorization: Bearer token123" \
  --progress-interval 1.0 \
  --buffer-size 2048 \
  "https://cdn.example.com/rhivos-image.raw.xz" \
  /dev/nvme0n1
```

### Example Output

```
Block flash command:
  URL: https://example.com/path/to/image.raw.xz
  Device: /dev/mmcblk1
  Buffer size: 1024 MB
  Max retries: 10
  Retry delay: 2 seconds

Using decompressor: xzcat
Opening block device for writing: /dev/mmcblk1
Starting download from: https://example.com/path/to/image.raw.xz
Content length: 547832416 bytes

Download: 27.92 MB / 522.45 MB (5.3%) | 27.92 MB/s | Decompressed: [░░░░░░░░░░] 0.4% | Written: 63.00 MB | 62.99 MB/s
Download: 101.40 MB / 522.45 MB (19.4%) | 33.78 MB/s | Decompressed: [░░░░░░░░░░] 5.3% | Written: 63.00 MB | 20.99 MB/s
...
Download: 506.08 MB / 522.45 MB (96.9%) | 31.60 MB/s | Decompressed: [█░░░░░░░░░] 12.8% | Written: 319.00 MB | 19.92 MB/s
Download: Done | Decompressed: [██████████████████░░] 91.5% | Written: 1407.00 MB | 18.57 MB/s
...
Download: Done | Decompressed: Done | Written: Done

Download complete: 522.45 MB in 16.46s (31.75 MB/s)
Decompression complete: 5120.00 MB in 87.83s (58.29 MB/s)
Write complete: 5120.00 MB in 281.23s (18.21 MB/s)
Compression ratio: 9.80x
```

### OCI Images

`fls` can pull OCI images from registries and flash them either to a block device
or to fastboot partitions.

#### Flash an OCI image to a block device

Use `from-url` with an `oci://` prefix:

```bash
fls from-url \
  -u "$REGISTRY_USER" \
  -p "$REGISTRY_PASS" \
  "oci://quay.io/org/image:latest" \
  /dev/mmcblk1
```

#### Flash an OCI image via fastboot

`fls fastboot` pulls the OCI image, extracts partition images, and flashes them
using the system `fastboot` CLI:

```bash
fls fastboot quay.io/org/image:latest
```

Provide explicit partition mappings when the OCI image contains multiple files:

```bash
fls fastboot quay.io/org/image:latest \
  -t boot_a:boot_a.simg \
  -t system_a:system_a.simg
```

Registry credentials can be provided with `-u/--username` and `-p/--password`
(`FLS_REGISTRY_PASSWORD` env var is supported for the password).

## Command Options

### `fls from-url`

Flash an image from a URL to a block device.

```
fls from-url [OPTIONS] <URL> <DEVICE>
```

**Arguments:**
- `<URL>` - URL to download the image from
- `<DEVICE>` - Destination device path (e.g., `/dev/sdb`, `/dev/mmcblk1`)

**Options:**
- `-k, --insecure-tls` - Ignore SSL certificate verification
- `--cacert <CACERT>` - Path to CA certificate PEM file for TLS validation
- `--buffer-size <SIZE>` - Buffer size in MB for download buffering (default: 1024)
- `--max-retries <NUM>` - Maximum number of retry attempts (default: 10)
- `--retry-delay <SECONDS>` - Delay in seconds between retry attempts (default: 2)
- `--debug` - Enable debug output
- `--o-direct` - Enable O_DIRECT mode for direct I/O (bypasses OS cache)
- `-H, --header <HEADER>` - Custom HTTP headers (can be used multiple times, format: `Header: value`)
- `-i, --progress-interval <SECONDS>` - Progress update interval in seconds (default: 0.5, accepts float values)
- `-n, --newline-progress` - Print progress on new lines instead of overwriting

## Safety Notes

⚠️ **WARNING**: `fls` writes directly to block devices and will **overwrite all data** on the target device. Always double-check the device path before running.

- Requires root/sudo privileges to write to block devices
- Ensure the target device is not mounted
- Verify the device path to avoid data loss
- Use `lsblk` or `fdisk -l` to identify the correct device before flashing


