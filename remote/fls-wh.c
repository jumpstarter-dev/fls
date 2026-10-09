#include <sys/types.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

/* macOS has no fdatasync(); fsync() is the equivalent. */
#ifdef __APPLE__
#define sync_file(fd) fsync(fd)
#else
#define sync_file(fd) fdatasync(fd)
#endif

/* fls-wh: a block-device write head.
 *
 * Usage: ./fls-wh <device>
 *
 * Opens <device> (O_RDWR), reports its size, then reads framed commands from
 * fd 0 (stdin) and writes them to the device, emitting framed records to fd 1
 * (stdout). Intended to be driven over SSH by a remote client (the Rust `fls`
 * tool). The device is a raw block device (e.g. /dev/emmc0); there is no
 * O_DIRECT on QNX, so durability comes from an explicit SYNC (fdatasync;
 * fsync on macOS, which has no fdatasync).
 *
 * Frame (both directions): [magic:4][opcode:1][payload_len:u32 LE][payload]
 * Little-endian throughout. The magic is a sync header: the reader scans for
 * it to resync after any corruption.
 *
 * Requests:  DATA(0x01) SKIP(0x02) SEEK(0x03) SYNC(0x04) QUIT(0x05)
 *            READ(0x06)                       [implemented]
 *            AUTH(0x07) HELLO(0x08)          [reserved, spec only]
 * Responses: READY(0x81) OK(0x82) PROG(0x83) ERR(0x84) DONE(0x85)
 *            READ_DATA(0x86)                 [implemented]
 *            AUTH_RESULT(0x87) HELLO(0x88)   [reserved]
 *
 * DATA payload:  size:u32 + content[size] + crc:u32 (CRC32-IEEE of content)
 * SKIP payload:  len:u64
 * SEEK payload:  offset:u64
 * SYNC/QUIT:     (no payload)
 * READ payload:  len:u64  (read len bytes from the current offset, advance it;
 *                len must be <= 1 MiB)
 *
 * READY payload: version:u8 + size:u64
 * OK payload:    op:1 + offset:u64 + bytes:u64
 * PROG payload:  offset:u64 + bytes_done:u64   (every 1 MiB of a DATA)
 * ERR payload:   code:1 + op:1 + errno:u32 + offset:u64
 *                code: 0=device I/O error, 1=CRC mismatch, 2=not implemented
 * DONE payload:  total_bytes:u64
 * READ_DATA payload: content   (the bytes read; the frame's payload_len is the
 *                count, which may be < requested near end of device)
 *
 * Uses read()/write() on fds directly (portable; some libc variants do not
 * export stdin/stdout/stderr). No userspace buffering, so every record is
 * written to the kernel immediately (no flush needed over SSH).
 */

enum {
    OP_DATA = 0x01, OP_SKIP = 0x02, OP_SEEK = 0x03, OP_SYNC = 0x04,
    OP_QUIT = 0x05,
    OP_READ = 0x06, OP_AUTH = 0x07, OP_HELLO = 0x08,   /* reserved */
    R_READY = 0x81, R_ACK = 0x82, R_PROG = 0x83, R_ERR = 0x84, R_DONE = 0x85,
    R_READ_DATA = 0x86, R_AUTH_RESULT = 0x87, R_HELLO = 0x88  /* reserved */
};
enum { ERR_DEV = 0, ERR_CRC = 1, ERR_NI = 2 };  /* NI = not implemented */

static const unsigned char MAGIC[4] = { 0x46, 0x4c, 0x53, 0x57 }; /* "FLSW" */
static const unsigned char PROTO_VER = 1;
static const size_t PROG_STEP = 1u << 20;  /* emit PROG every 1 MiB */
#define MAX_READ (1u << 20)                /* cap a single READ at 1 MiB */
static unsigned char dbuf[MAX_READ];       /* device read buffer */

/* --- little-endian packing --- */
static unsigned int rd_u32(const unsigned char *p)
{ return p[0] | p[1] << 8 | p[2] << 16 | (unsigned)p[3] << 24; }
static unsigned long long rd_u64(const unsigned char *p)
{ return (unsigned long long)rd_u32(p) | ((unsigned long long)rd_u32(p + 4) << 32); }
static void wr_u32(unsigned char *p, unsigned int v)
{ p[0] = v; p[1] = v >> 8; p[2] = v >> 16; p[3] = v >> 24; }
static void wr_u64(unsigned char *p, unsigned long long v)
{ wr_u32(p, (unsigned int)v); wr_u32(p + 4, (unsigned int)(v >> 32)); }

/* --- CRC32 (IEEE), running state so it can be computed while streaming --- */
static unsigned int crc_tab[256];
static unsigned int crc_state;
static void crc_init(void)
{
    for (unsigned int i = 0; i < 256; i++) {
        unsigned int c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        crc_tab[i] = c;
    }
}
static void crc_reset(void) { crc_state = 0xFFFFFFFFu; }
static void crc_update(const unsigned char *b, size_t n)
{
    unsigned int c = crc_state;
    for (size_t i = 0; i < n; i++)
        c = crc_tab[(c ^ b[i]) & 0xFF] ^ (c >> 8);
    crc_state = c;
}
static unsigned int crc_final(void) { return crc_state ^ 0xFFFFFFFFu; }

/* --- framed output (stdout) --- */
static void emit(int op, const unsigned char *payload, size_t len)
{
    unsigned char hdr[9];
    hdr[0] = MAGIC[0]; hdr[1] = MAGIC[1];
    hdr[2] = MAGIC[2]; hdr[3] = MAGIC[3];
    hdr[4] = (unsigned char)op;
    wr_u32(hdr + 5, (unsigned int)len);
    (void)write(1, hdr, 9);
    if (len) (void)write(1, payload, len);
}
static void ok_rec(int op, unsigned long long off, unsigned long long bytes)
{
    unsigned char p[17];
    p[0] = (unsigned char)op; wr_u64(p + 1, off); wr_u64(p + 9, bytes);
    emit(R_ACK, p, sizeof p);
}
static void err_rec(int code, int op, unsigned int e, unsigned long long off)
{
    unsigned char p[14];
    p[0] = (unsigned char)code; p[1] = (unsigned char)op;
    wr_u32(p + 2, e); wr_u64(p + 6, off);
    emit(R_ERR, p, sizeof p);
}

/* --- buffered reader (stdin) --- */
static unsigned char rbuf[1 << 16];
static size_t rlen = 0, rpos = 0;
static int rfill(void)
{
    if (rpos < rlen) return 1;
    ssize_t n = read(0, rbuf, sizeof rbuf);
    if (n <= 0) { rlen = 0; rpos = 0; return 0; }
    rlen = (size_t)n; rpos = 0; return 1;
}
static int rnext(unsigned char *b)
{
    if (!rfill()) return 0;
    *b = rbuf[rpos++];
    return 1;
}
static int rreadn(unsigned char *dst, size_t n)
{
    while (n > 0) {
        if (rpos >= rlen && !rfill()) return 0;
        size_t avail = rlen - rpos;
        size_t take = (n < avail) ? n : avail;
        memcpy(dst, rbuf + rpos, take);
        dst += take; rpos += take; n -= take;
    }
    return 1;
}
/* scan for the 4-byte magic (resyncs after corruption); 0 on EOF */
static int read_magic(void)
{
    unsigned char w[3] = { 0, 0, 0 };
    for (;;) {
        unsigned char b;
        if (!rnext(&b)) return 0;
        if (w[0] == MAGIC[0] && w[1] == MAGIC[1] &&
            w[2] == MAGIC[2] && b == MAGIC[3])
            return 1;
        w[0] = w[1]; w[1] = w[2]; w[2] = b;
    }
}
/* skip n bytes from the stream (drop a malformed frame's remainder); 0 on EOF */
static int skip(unsigned long long n)
{
    unsigned char b[1 << 16];
    while (n > 0) {
        size_t take = (n > sizeof b) ? sizeof b : (size_t)n;
        if (!rreadn(b, take)) return 0;
        n -= take;
    }
    return 1;
}

/* write all, retrying on partial writes; -1 on error (errno set) */
static int write_all(int fd, const unsigned char *buf, size_t len)
{
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

/* read all, retrying on partial reads; -1 on error (errno set), else the bytes
 * read (which may be < len near end of device) */
static int read_all(int fd, unsigned char *buf, size_t len)
{
    size_t off = 0;
    while (off < len) {
        ssize_t n = read(fd, buf + off, len - off);
        if (n < 0) return -1;
        if (n == 0) return (int)off;  /* EOF: partial read */
        off += (size_t)n;
    }
    return (int)len;
}

int main(int argc, char **argv)
{
    if (argc < 2) { err_rec(ERR_DEV, 0, 22, 0); return 1; }  /* 22 = EINVAL */
    int fd = open(argv[1], O_RDWR);
    if (fd < 0) { err_rec(ERR_DEV, 0, (unsigned)errno, 0); return 1; }

    long long sz = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);  /* the size query left the offset at the end */
    unsigned char rp[9];
    rp[0] = PROTO_VER; wr_u64(rp + 1, (unsigned long long)(sz < 0 ? 0 : sz));
    emit(R_READY, rp, sizeof rp);

    unsigned long long cur = 0;  /* current logical write position */
    unsigned long long total = 0;
    crc_init();

    for (;;) {
        if (!read_magic()) break;  /* EOF */
        unsigned char op;
        if (!rnext(&op)) break;
        unsigned char pl[4];
        if (!rreadn(pl, 4)) break;
        unsigned long long plen = rd_u32(pl);

        if (op == OP_QUIT)
            break;

        else if (op == OP_DATA) {
            unsigned char szf[4];
            if (!rreadn(szf, 4)) break;
            unsigned int size = rd_u32(szf);
            if (plen != 8 + (unsigned long long)size) {  /* malformed frame */
                if (plen >= 4) skip(plen - 4);
                err_rec(ERR_NI, OP_DATA, 0, cur);
                continue;
            }
            unsigned long long start = cur;
            crc_reset();
            unsigned long long done = 0, next_prog = PROG_STEP;
            int status = 0;  /* 0=ok, 1=eof, 2=write error */
            unsigned char chunk[1 << 16];
            while (done < size) {
                size_t want = (size_t)(size - done);
                if (want > sizeof chunk) want = sizeof chunk;
                if (!rreadn(chunk, want)) { status = 1; break; }
                crc_update(chunk, want);
                if (write_all(fd, chunk, want) < 0) { status = 2; break; }
                done += want;
                while (done >= next_prog) {
                    unsigned char pp[16];
                    wr_u64(pp, start + done); wr_u64(pp + 8, done);
                    emit(R_PROG, pp, sizeof pp);
                    next_prog += PROG_STEP;
                }
            }
            cur = start + done;
            total += done;
            if (status == 1) { err_rec(ERR_DEV, OP_DATA, 0, start); break; }
            if (status == 2) { err_rec(ERR_DEV, OP_DATA, (unsigned)errno, start); }
            else if (done < size) { err_rec(ERR_DEV, OP_DATA, 0, start); break; }
            else {
                unsigned char crcf[4];
                if (!rreadn(crcf, 4)) { err_rec(ERR_DEV, OP_DATA, 0, start); break; }
                if (crc_final() != rd_u32(crcf))
                    err_rec(ERR_CRC, OP_DATA, 0, start);
                else
                    ok_rec(OP_DATA, start, done);
            }
        }

        else if (op == OP_SKIP) {
            if (plen != 8) { skip(plen); err_rec(ERR_NI, OP_SKIP, 0, cur); continue; }
            unsigned char lf[8];
            if (!rreadn(lf, 8)) break;
            unsigned long long len = rd_u64(lf);
            if (lseek(fd, cur + len, SEEK_SET) < 0)
                err_rec(ERR_DEV, OP_SKIP, (unsigned)errno, cur);
            else { ok_rec(OP_SKIP, cur, len); cur += len; }
        }

        else if (op == OP_SEEK) {
            if (plen != 8) { skip(plen); err_rec(ERR_NI, OP_SEEK, 0, cur); continue; }
            unsigned char of[8];
            if (!rreadn(of, 8)) break;
            unsigned long long off = rd_u64(of);
            if (lseek(fd, off, SEEK_SET) < 0)
                err_rec(ERR_DEV, OP_SEEK, (unsigned)errno, off);
            else { ok_rec(OP_SEEK, off, 0); cur = off; }
        }

        else if (op == OP_READ) {
            if (plen != 8) { skip(plen); err_rec(ERR_NI, OP_READ, 0, cur); continue; }
            unsigned char lf[8];
            if (!rreadn(lf, 8)) break;
            unsigned long long len = rd_u64(lf);
            if (len > MAX_READ) { err_rec(ERR_DEV, OP_READ, 22, cur); continue; }  /* 22 = EINVAL */
            int n = read_all(fd, dbuf, (size_t)len);
            if (n < 0) { err_rec(ERR_DEV, OP_READ, (unsigned)errno, cur); continue; }
            emit(R_READ_DATA, dbuf, (size_t)n);  /* the bytes read (may be empty) */
            cur += (unsigned long long)n;
        }

        else if (op == OP_SYNC) {
            if (plen != 0) { skip(plen); err_rec(ERR_NI, OP_SYNC, 0, cur); continue; }
            (void)sync_file(fd);
            ok_rec(OP_SYNC, cur, 0);
        }

        else {  /* reserved (AUTH/HELLO) or unknown opcode */
            skip(plen);
            err_rec(ERR_NI, op, 0, cur);
        }
    }

    unsigned char dp[8];
    wr_u64(dp, total);
    emit(R_DONE, dp, sizeof dp);
    return 0;
}
