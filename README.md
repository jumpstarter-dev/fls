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

### Remote devices over SSH

Use `[user@]host:/absolute/device` as the destination for HTTP/HTTPS or OCI images:

```bash
fls from-url "https://example.com/image.img.xz" root@board:/dev/emmc0
fls from-url "oci://quay.io/org/image:latest" root@board:/dev/emmc0
```

`fls` uploads its embedded aarch64-QNX write head over SSH, then streams framed
commands. For another target architecture/OS, build the appropriate write head
and use `--wh-bin /path/to/flswh`. When building from source, run
`make remote-wh` before `cargo build` to embed the QNX binary; local-only builds
work without it. See [remote/README.md](remote/README.md) for build instructions.

SSH keys, agents, host keys, ports, and aliases use the system `ssh` and your
`~/.ssh/config`. `--ssh-port <port>` overrides the port for platform detection,
upload, and flashing; without it the SSH configuration is used. `-p` remains
the registry-password option. Password authentication uses `sshpass` with
`--ssh-password-file <path>` (`FLS_SSH_PASS_FILE`) or, if no file is specified,
the `SSHPASS` environment variable. `--ssh-compress` enables SSH compression;
`--wh-bin` can also be supplied through `FLS_WH_BIN`.

Before uploading, `fls` queries the remote OS, CPU architecture, and release
using `uname` (`-p` for the QNX processor, `-m` elsewhere). The embedded binary
is selected only for aarch64 QNX 7. Other platforms require `--wh-bin` with a
compatible write head; automatic Linux/macOS binary embedding can be added
when those artifacts are available.

Remote zero fills use ZERO rather than transferring their contents. Adjacent
DATA writes are coalesced into frames of up to 8 MiB, flushing before SEEK,
FILL, and final SYNC so sparse-image fragments do not each require an ACK.
DATA, ZERO, and SEEK frames are pipelined in order while a separate reader
consumes progress and ACKs. `--write-buffer-size` also limits unacknowledged
DATA bytes (default: 128 MiB); smaller windows reduce the maximum frame size.
Small-command metadata is bounded to 1024 outstanding requests. Closing drains
the ACKs before SYNC/QUIT, and remote errors stop the pipeline.
The writer checks write, fill, and seek ranges against remote device capacity, and waits
for successful SYNC and QUIT before reporting completion. `--o-direct` applies
to local devices; remote durability uses SYNC. One flash session per host is
supported because uploads share `/tmp/fls-wh`.
The reported Written count is the logical device position, including sparse
skips, matching local flashing; DONE still validates only actual written bytes.

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
fls fastboot oci://quay.io/org/image:latest
```

To avoid using `/tmp` for fastboot extraction, set `FLS_TMP_DIR` to a directory
on persistent storage (default is `/var/lib/fls`).

If the OCI manifest includes
`automotive.sdv.cloud.redhat.com/default-partitions` (comma-separated),
`fls fastboot` flashes only those partitions by default. Otherwise it flashes
all annotated partitions.

Provide explicit partition mappings when the OCI image contains multiple files:

```bash
fls fastboot oci://quay.io/org/image:latest \
  -t boot_a:boot_a.simg \
  -t system_a:system_a.simg
```

When `-t` is provided, `fls` applies those mappings on top of the OCI layer
annotations and includes those partitions in the flash set (e.g., add
`-t vbmeta_a:vbmeta_a.simg` to flash vbmeta). If the image lacks annotations,
it falls back to looking for the specified files directly in the image.

Registry credentials can be provided with `-u/--username` and `-p/--password`
(`FLS_REGISTRY_PASSWORD` env var is supported for the password). Both are
required for authenticated access.

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
- `--write-buffer-size <SIZE>` - Write queue and remote unacknowledged DATA limit (default: 128 MiB)
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
