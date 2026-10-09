# fls-wh — the remote write head

`fls-wh` is a small C program that runs on a remote system (QNX today;
extensible to others, e.g. a U-Boot command talking over the network) and
writes framed commands to a raw block device (e.g. `/dev/emmc0`). It is the
remote counterpart of the local writer in `src/fls/block_writer.rs` (which
uses `O_DIRECT` on Linux/macOS). QNX has no `O_DIRECT`, so durability comes
from an explicit `SYNC` (`fdatasync`; `fsync` on macOS, which has no
`fdatasync`).

## Usage

```
./flswh-<arch>-<os> <device>
```

Opens `<device>` (`O_RDWR`), reports its size (`READY`), then reads framed
commands from stdin and writes them to the device, emitting framed records to
stdout. Intended to be driven by a remote client (e.g. over SSH). QUIT or
EOF exits.
Device I/O errors are reported (`ERR`) and processing continues.

## Protocol

Frame (both directions): `[magic:4][opcode:1][payload_len:u32 LE][payload]`,
little-endian throughout. The magic (`FLSW`) is a sync header: the reader
scans for it to resync after any corruption.

| | opcode | | opcode |
|---|---|---|---|
| **Requests** | `DATA 0x01` `SKIP 0x02` `SEEK 0x03` `SYNC 0x04` `QUIT 0x05` `READ 0x06` `ZERO 0x09` `SIZE 0x0a` | **Responses** | `READY 0x81` `OK 0x82` `PROG 0x83` `ERR 0x84` `DONE 0x85` `READ_DATA 0x86` `SIZE 0x89` |
| **Reserved** | `AUTH 0x07` `HELLO 0x08` | | `AUTH_RESULT 0x87` `HELLO 0x88` |

The `READY` payload is `version:u8 + size:u64` (total device bytes, measured
at open); the protocol version is currently 1. `DATA` is
`size:u32 + content + crc:u32` (CRC32-IEEE);
`READ` is `len:u64` (reads from the current offset, advances it, capped at
1 MiB) and returns `READ_DATA` with the bytes. `ZERO` is `len:u64` — writes
that many zeros from the current offset and advances it, sending only the 8
length bytes over the wire (for filling regions without transferring bytes).

`SIZE` has no request payload and returns `SIZE 0x89` with `size:u64` (the
same total device bytes measured at open), without changing the cursor. If
the size could not be determined, it returns `ERR` with the original errno;
the startup `READY` uses zero for unknown size.
Before writing an image, the client can use `READY` or `SIZE` to check
`image_size <= device_size - write_offset` after checking that
`write_offset <= device_size`. For sparse images, compare the expanded size.

## Build

Binaries are named `flswh-<arch>-<os>` (matching the `fls` artifact scheme):

- **QNX aarch64** (cross): `make` (or `make -C remote` from the repo root) → `flswh-aarch64-qnx7`. Needs an aarch64 cross gcc toolchain (the Makefile's `aarch64-elf-gcc` + `ld.lld` names map to any gcc cross toolchain; `make container-build` uses Fedora's in a container).
- **Native** (host, incl. macOS): `make native` → `flswh-<arch>-<os>` (e.g. `flswh-aarch64-darwin`, `flswh-x86_64-linux`). Uses the system compiler and system headers, no stubs.

## Test

`python3 test.py` — builds the source with the system compiler, drives it with
a framed command script against a temp file, and asserts the records and bytes.
