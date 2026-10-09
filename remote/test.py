#!/usr/bin/env python3
"""Native test for fls-wh: build with the system compiler, run it against a
temp file (standing in for a block device), feed a framed command script, and
assert the framed records + the file bytes.

Runs on Linux and macOS: `SYNC` uses `fdatasync` where available and falls
back to `fsync` on macOS (which has no `fdatasync`).
"""
import os
import shutil
import struct
import subprocess
import sys
import zlib

MAGIC = b"FLSW"
OP_DATA, OP_SKIP, OP_SEEK, OP_SYNC, OP_QUIT, OP_READ = 1, 2, 3, 4, 5, 6
R_READY, R_ACK, R_PROG, R_ERR, R_DONE, R_READ_DATA = 0x81, 0x82, 0x83, 0x84, 0x85, 0x86


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
    binp = "/tmp/fls-wh-native"
    blk = "/tmp/fls-wh-test-blk"

    # 1. build with the system compiler (system headers, not include/)
    subprocess.run([find_cc(), "-O2", src, "-o", binp], check=True)

    # 2. create a 1 MiB "device"
    with open(blk, "wb") as f:
        f.truncate(1 << 20)

    # 3. build the command script: SEEK 0, DATA 16, SEEK 0, READ 16, SKIP 8,
    #    SYNC, QUIT
    content = bytes(range(16))
    crc = zlib.crc32(content) & 0xFFFFFFFF
    script = b""
    script += frame(OP_SEEK, struct.pack("<Q", 0))
    script += frame(OP_DATA, struct.pack("<I", len(content)) + content
                   + struct.pack("<I", crc))
    script += frame(OP_SEEK, struct.pack("<Q", 0))
    script += frame(OP_READ, struct.pack("<Q", len(content)))
    script += frame(OP_SKIP, struct.pack("<Q", 8))
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
    assert total == len(content), f"total {total} != {len(content)}"

    ok_ops = [struct.unpack_from("<B", p)[0] for op, p in recs if op == R_ACK]
    for want in (OP_SEEK, OP_DATA, OP_SKIP, OP_SYNC):
        assert want in ok_ops, f"no OK for op {want:#x}: {ok_ops}"

    # 5b. the READ_DATA record carries exactly the bytes we wrote
    rd = [p for op, p in recs if op == R_READ_DATA]
    assert len(rd) == 1, f"expected one READ_DATA, got {len(rd)}"
    assert rd[0] == content, f"READ_DATA mismatch: {rd[0].hex()} != {content.hex()}"

    # 6. the file bytes were written at offset 0
    with open(blk, "rb") as f:
        head = f.read(16)
    assert head == content, f"file bytes mismatch: {head.hex()} != {content.hex()}"

    print("native test OK")


if __name__ == "__main__":
    main()
