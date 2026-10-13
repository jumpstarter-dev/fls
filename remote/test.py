#!/usr/bin/env python3
"""Native test for fls-wh: build with the system compiler, run it against a
temp file (standing in for a block device), feed a framed command script, and
assert the framed records + the file bytes.

Runs on Linux and macOS: `SYNC` uses `fdatasync` where available and falls
back to `fsync` on macOS (which has no `fdatasync`).
"""
import errno
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import zlib

MAGIC = b"FLSW"
OP_DATA, OP_SEEK, OP_SYNC, OP_QUIT, OP_READ, OP_ZERO = 1, 3, 4, 5, 6, 9
R_READY, R_ACK, R_PROG, R_ERR, R_DONE, R_READ_DATA = 0x81, 0x82, 0x83, 0x84, 0x85, 0x86
OP_SIZE, R_SIZE = 0x0A, 0x89


def frame(op, payload):
    return MAGIC + bytes([op]) + struct.pack("<I", len(payload)) + payload


def parse(out):
    """Parse framed records; resync on a bad magic (mirrors the C reader)."""
    recs = []
    i, n = 0, len(out)
    while i + 9 <= n:
        if out[i:i + 4] != MAGIC:
            j = out.find(MAGIC, i + 1)
            if j < 0:
                break
            i = j
            continue
        op = out[i + 4]
        (plen,) = struct.unpack_from("<I", out, i + 5)
        payload = out[i + 9:i + 9 + plen]
        recs.append((op, payload))
        i += 9 + plen
    return recs


def find_cc():
    # native compiler first (CI runner); fall back to the cross gcc, which is
    # native to the aarch64 container.
    for c in ("cc", "gcc", "aarch64-linux-gnu-gcc"):
        p = shutil.which(c)
        if p:
            return p
    sys.exit("no C compiler found (tried cc, gcc, aarch64-linux-gnu-gcc)")


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(here, "fls-wh.c")
    tmpdir = tempfile.mkdtemp(prefix="fls-wh-test-")
    binp = os.path.join(tmpdir, "fls-wh-native")
    blk = os.path.join(tmpdir, "blk")

    # 1. build with the system compiler (system headers, not include/)
    subprocess.run([find_cc(), "-O2", src, "-o", binp], check=True)

    # 2. create a 1 MiB "device"
    with open(blk, "wb") as f:
        f.truncate(1 << 20)

    # SIZE between SEEK and ZERO must leave the logical and device offsets intact.
    content = bytes(range(16))
    crc = zlib.crc32(content) & 0xFFFFFFFF
    script = b""
    script += frame(OP_SEEK, struct.pack("<Q", 0))
    script += frame(OP_DATA, struct.pack("<I", len(content)) + content
                   + struct.pack("<I", crc))
    script += frame(OP_SEEK, struct.pack("<Q", 0))
    script += frame(OP_READ, struct.pack("<Q", len(content)))
    script += frame(OP_SEEK, struct.pack("<Q", 24))
    script += frame(OP_SIZE, b"")
    script += frame(OP_ZERO, struct.pack("<Q", 256))
    script += frame(OP_SIZE, b"")
    script += frame(OP_SYNC, b"")
    script += frame(OP_QUIT, b"")

    # 4. run
    r = subprocess.run([binp, blk], input=script, capture_output=True)
    if r.returncode != 0:
        sys.exit(f"fls-wh exited {r.returncode}\n"
                 f"stdout={r.stdout!r}\nstderr={r.stderr!r}")

    # 5. parse + assert the records
    recs = parse(r.stdout)
    assert recs, "no records"
    op0, p0 = recs[0]
    assert op0 == R_READY, f"first record not READY: {op0:#x}"
    ver, size = struct.unpack_from("<BQ", p0)
    assert ver == 1, f"version {ver} != 1"
    assert size == 1 << 20, f"size {size} != {1 << 20}"

    ops = [op for op, _ in recs]
    assert R_ERR not in ops, f"ERR record present: {recs}"
    assert ops[-1] == R_DONE, f"last record not DONE: {ops[-1]:#x}"
    (total,) = struct.unpack_from("<Q", recs[-1][1])
    assert total == len(content) + 256, f"total {total} != {len(content) + 256}"

    ok_ops = [struct.unpack_from("<B", p)[0] for op, p in recs if op == R_ACK]
    for want in (OP_SEEK, OP_DATA, OP_SYNC, OP_ZERO):
        assert want in ok_ops, f"no OK for op {want:#x}: {ok_ops}"

    # 5b. the READ_DATA record carries exactly the bytes we wrote
    rd = [p for op, p in recs if op == R_READ_DATA]
    assert len(rd) == 1, f"expected one READ_DATA, got {len(rd)}"
    assert rd[0] == content, f"READ_DATA mismatch: {rd[0].hex()} != {content.hex()}"
    assert [p for op, p in recs if op == R_SIZE] == [struct.pack("<Q", size)] * 2, recs
    zero_acks = [struct.unpack("<BQQ", p) for op, p in recs if op == R_ACK and p[0] == OP_ZERO]
    assert zero_acks == [(OP_ZERO, 24, 256)], zero_acks

    # 6. the file bytes were written at offset 0, and ZERO zeroed 256 bytes
    #    at offset 24 (after SEEK 24)
    with open(blk, "rb") as f:
        head = f.read(16)
        f.seek(24)
        zeros = f.read(256)
    assert head == content, f"file bytes mismatch: {head.hex()} != {content.hex()}"
    assert zeros == b"\0" * 256, f"ZERO region not zeroed: {zeros[:16].hex()}..."

    # Inject short I/O followed by EIO, zero-progress writes, and SYNC failure.
    # Successful lseek deliberately changes errno to catch lost error codes.
    fault_src = os.path.join(tmpdir, "faults.c")
    fault_bin = os.path.join(tmpdir, "fls-wh-faults")
    with open(fault_src, "w") as f:
        f.write('''#include <unistd.h>
#include <errno.h>
static ssize_t fault_write(int fd, const void *buf, size_t n) {
    if (fd > 2) {
        if (n == 32) return 0;
        if (n == 8) { errno = EIO; return -1; }
        if (n == 16) n = 8;
    }
    return write(fd, buf, n);
}
static ssize_t fault_read(int fd, void *buf, size_t n) {
    if (fd > 2) {
        if (n == 8) { errno = EIO; return -1; }
        if (n == 16) n = 8;
    }
    return read(fd, buf, n);
}
static off_t fault_seek(int fd, off_t off, int whence) {
    if (whence == SEEK_END) { errno = EIO; return -1; }
    off_t result = lseek(fd, off, whence);
    if (result >= 0) errno = EINVAL;
    return result;
}
static int fault_sync(int fd) { (void)fd; errno = EIO; return -1; }
#define write fault_write
#define read fault_read
#define lseek fault_seek
#define fsync fault_sync
#define fdatasync fault_sync
#include "fls-wh.c"
''')
    subprocess.run([find_cc(), "-O2", "-I", here, fault_src, "-o", fault_bin], check=True)

    def data(content):
        return frame(OP_DATA, struct.pack("<I", len(content)) + content
                     + struct.pack("<I", zlib.crc32(content)))

    seek0 = frame(OP_SEEK, struct.pack("<Q", 0))

    # Every ERR is fatal: the write head exits right after reporting it.
    # Run each fault in a fresh process and check the exact record sequence.
    # (A partial write is not counted: total tracks acknowledged bytes only.)
    seek_ack = (R_ACK, struct.pack("<BQQ", OP_SEEK, 0, 0))

    def fault_run(script, err, total):
        r = subprocess.run([fault_bin, blk], input=script,
                           capture_output=True, timeout=10, check=True)
        assert parse(r.stdout) == [
            (R_READY, struct.pack("<BQ", 1, 0)),
            seek_ack,
            (R_ERR, struct.pack("<BBIQ", *err)),
            (R_DONE, struct.pack("<Q", total)),
        ], r.stdout

    # SIZE: the SEEK_END probe at open failed (EIO), so the query reports it.
    # (No SEEK in this script, so no ACK record.)
    r = subprocess.run([fault_bin, blk], input=frame(OP_SIZE, b""),
                       capture_output=True, timeout=10, check=True)
    assert parse(r.stdout) == [
        (R_READY, struct.pack("<BQ", 1, 0)),
        (R_ERR, struct.pack("<BBIQ", 0, OP_SIZE, errno.EIO, 0)),
        (R_DONE, struct.pack("<Q", 0)),
    ], r.stdout
    # DATA: 16 -> 8 partial write, then EIO on the retry.
    fault_run(seek0 + data(bytes(range(16))), (0, OP_DATA, errno.EIO, 0), 0)
    # DATA: a zero-progress write is a hard error.
    fault_run(seek0 + data(bytes(32)), (0, OP_DATA, errno.EIO, 0), 0)
    # READ: 16 -> 8 partial read, then EIO on the retry.
    fault_run(seek0 + frame(OP_READ, struct.pack("<Q", 16)),
              (0, OP_READ, errno.EIO, 0), 0)
    # ZERO: 16 -> 8 partial write, then EIO on the retry.
    fault_run(seek0 + frame(OP_ZERO, struct.pack("<Q", 16)),
              (0, OP_ZERO, errno.EIO, 0), 0)
    # SYNC: the sync always fails.
    fault_run(seek0 + frame(OP_SYNC, b""), (0, OP_SYNC, errno.EIO, 0), 0)

    # A malformed SIZE is fatal too: the write head exits and the second
    # SIZE in the stream is never processed.
    empty = os.path.join(tmpdir, "empty")
    open(empty, "wb").close()
    r = subprocess.run([binp, empty],
                       input=frame(OP_SIZE, b"x") + frame(OP_SIZE, b"") + frame(OP_QUIT, b""),
                       capture_output=True, timeout=10, check=True)
    assert parse(r.stdout) == [
        (R_READY, struct.pack("<BQ", 1, 0)),
        (R_ERR, struct.pack("<BBIQ", 2, OP_SIZE, 0, 0)),
        (R_DONE, struct.pack("<Q", 0)),
    ], r.stdout

    print("native test OK")


if __name__ == "__main__":
    main()
