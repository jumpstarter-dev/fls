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
 *            READ(0x06) ZERO(0x09)           [implemented]
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
    OP_READ = 0x06, OP_AUTH = 0x07, OP_HELLO = 0x08, OP_ZERO = 0x09,
    R_READY = 0x81, R_ACK = 0x82, R_PROG = 0x83, R_ERR = 0x84, R_DONE = 0x85,
    R_READ_DATA = 0x86, R_AUTH_RESULT = 0x87, R_HELLO = 0x88  /* reserved */
};
enum { ERR_DEV = 0, ERR_CRC = 1, ERR_NI = 2 };  /* NI = not implemented */

static const unsigned char MAGIC[4] = { 0x46, 0x4c, 0x53, 0x57 }; /* "FLSW" */
static const unsigned char PROTO_VER = 1;
#ifndef WRITE_CHUNK_SIZE
#define WRITE_CHUNK_SIZE (8u << 20)              /* 8 MiB disk write chunks (-DWRITE_CHUNK_SIZE=N to override) */
#endif
static const size_t PROG_STEP = WRITE_CHUNK_SIZE; /* emit PROG every chunk */
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

/* --- CRC32 (IEEE), running state so it can be computed while streaming.
 * Base table and per-byte update from gityf/crc (Wang Yaofu,
 * Apache License 2.0, https://github.com/gityf/crc), adapted to a
 * running state. */
static const unsigned int crc_tab[256] = {
    0x00000000L, 0x77073096L, 0xee0e612cL, 0x990951baL, 0x076dc419L,
    0x706af48fL, 0xe963a535L, 0x9e6495a3L, 0x0edb8832L, 0x79dcb8a4L,
    0xe0d5e91eL, 0x97d2d988L, 0x09b64c2bL, 0x7eb17cbdL, 0xe7b82d07L,
    0x90bf1d91L, 0x1db71064L, 0x6ab020f2L, 0xf3b97148L, 0x84be41deL,
    0x1adad47dL, 0x6ddde4ebL, 0xf4d4b551L, 0x83d385c7L, 0x136c9856L,
    0x646ba8c0L, 0xfd62f97aL, 0x8a65c9ecL, 0x14015c4fL, 0x63066cd9L,
    0xfa0f3d63L, 0x8d080df5L, 0x3b6e20c8L, 0x4c69105eL, 0xd56041e4L,
    0xa2677172L, 0x3c03e4d1L, 0x4b04d447L, 0xd20d85fdL, 0xa50ab56bL,
    0x35b5a8faL, 0x42b2986cL, 0xdbbbc9d6L, 0xacbcf940L, 0x32d86ce3L,
    0x45df5c75L, 0xdcd60dcfL, 0xabd13d59L, 0x26d930acL, 0x51de003aL,
    0xc8d75180L, 0xbfd06116L, 0x21b4f4b5L, 0x56b3c423L, 0xcfba9599L,
    0xb8bda50fL, 0x2802b89eL, 0x5f058808L, 0xc60cd9b2L, 0xb10be924L,
    0x2f6f7c87L, 0x58684c11L, 0xc1611dabL, 0xb6662d3dL, 0x76dc4190L,
    0x01db7106L, 0x98d220bcL, 0xefd5102aL, 0x71b18589L, 0x06b6b51fL,
    0x9fbfe4a5L, 0xe8b8d433L, 0x7807c9a2L, 0x0f00f934L, 0x9609a88eL,
    0xe10e9818L, 0x7f6a0dbbL, 0x086d3d2dL, 0x91646c97L, 0xe6635c01L,
    0x6b6b51f4L, 0x1c6c6162L, 0x856530d8L, 0xf262004eL, 0x6c0695edL,
    0x1b01a57bL, 0x8208f4c1L, 0xf50fc457L, 0x65b0d9c6L, 0x12b7e950L,
    0x8bbeb8eaL, 0xfcb9887cL, 0x62dd1ddfL, 0x15da2d49L, 0x8cd37cf3L,
    0xfbd44c65L, 0x4db26158L, 0x3ab551ceL, 0xa3bc0074L, 0xd4bb30e2L,
    0x4adfa541L, 0x3dd895d7L, 0xa4d1c46dL, 0xd3d6f4fbL, 0x4369e96aL,
    0x346ed9fcL, 0xad678846L, 0xda60b8d0L, 0x44042d73L, 0x33031de5L,
    0xaa0a4c5fL, 0xdd0d7cc9L, 0x5005713cL, 0x270241aaL, 0xbe0b1010L,
    0xc90c2086L, 0x5768b525L, 0x206f85b3L, 0xb966d409L, 0xce61e49fL,
    0x5edef90eL, 0x29d9c998L, 0xb0d09822L, 0xc7d7a8b4L, 0x59b33d17L,
    0x2eb40d81L, 0xb7bd5c3bL, 0xc0ba6cadL, 0xedb88320L, 0x9abfb3b6L,
    0x03b6e20cL, 0x74b1d29aL, 0xead54739L, 0x9dd277afL, 0x04db2615L,
    0x73dc1683L, 0xe3630b12L, 0x94643b84L, 0x0d6d6a3eL, 0x7a6a5aa8L,
    0xe40ecf0bL, 0x9309ff9dL, 0x0a00ae27L, 0x7d079eb1L, 0xf00f9344L,
    0x8708a3d2L, 0x1e01f268L, 0x6906c2feL, 0xf762575dL, 0x806567cbL,
    0x196c3671L, 0x6e6b06e7L, 0xfed41b76L, 0x89d32be0L, 0x10da7a5aL,
    0x67dd4accL, 0xf9b9df6fL, 0x8ebeeff9L, 0x17b7be43L, 0x60b08ed5L,
    0xd6d6a3e8L, 0xa1d1937eL, 0x38d8c2c4L, 0x4fdff252L, 0xd1bb67f1L,
    0xa6bc5767L, 0x3fb506ddL, 0x48b2364bL, 0xd80d2bdaL, 0xaf0a1b4cL,
    0x36034af6L, 0x41047a60L, 0xdf60efc3L, 0xa867df55L, 0x316e8eefL,
    0x4669be79L, 0xcb61b38cL, 0xbc66831aL, 0x256fd2a0L, 0x5268e236L,
    0xcc0c7795L, 0xbb0b4703L, 0x220216b9L, 0x5505262fL, 0xc5ba3bbeL,
    0xb2bd0b28L, 0x2bb45a92L, 0x5cb36a04L, 0xc2d7ffa7L, 0xb5d0cf31L,
    0x2cd99e8bL, 0x5bdeae1dL, 0x9b64c2b0L, 0xec63f226L, 0x756aa39cL,
    0x026d930aL, 0x9c0906a9L, 0xeb0e363fL, 0x72076785L, 0x05005713L,
    0x95bf4a82L, 0xe2b87a14L, 0x7bb12baeL, 0x0cb61b38L, 0x92d28e9bL,
    0xe5d5be0dL, 0x7cdcefb7L, 0x0bdbdf21L, 0x86d3d2d4L, 0xf1d4e242L,
    0x68ddb3f8L, 0x1fda836eL, 0x81be16cdL, 0xf6b9265bL, 0x6fb077e1L,
    0x18b74777L, 0x88085ae6L, 0xff0f6a70L, 0x66063bcaL, 0x11010b5cL,
    0x8f659effL, 0xf862ae69L, 0x616bffd3L, 0x166ccf45L, 0xa00ae278L,
    0xd70dd2eeL, 0x4e048354L, 0x3903b3c2L, 0xa7672661L, 0xd06016f7L,
    0x4969474dL, 0x3e6e77dbL, 0xaed16a4aL, 0xd9d65adcL, 0x40df0b66L,
    0x37d83bf0L, 0xa9bcae53L, 0xdebb9ec5L, 0x47b2cf7fL, 0x30b5ffe9L,
    0xbdbdf21cL, 0xcabac28aL, 0x53b39330L, 0x24b4a3a6L, 0xbad03605L,
    0xcdd70693L, 0x54de5729L, 0x23d967bfL, 0xb3667a2eL, 0xc4614ab8L,
    0x5d681b02L, 0x2a6f2b94L, 0xb40bbe37L, 0xc30c8ea1L, 0x5a05df1bL,
    0x2d02ef8dL
};
static unsigned int crc_state;
static void crc_init(void) { }  /* table is static */
static void crc_reset(void) { crc_state = 0xFFFFFFFFu; }
static void crc_update(const unsigned char *b, size_t n)
{
    unsigned int c = crc_state;
    for (size_t i = 0; i < n; i++)
        c = crc_tab[(c ^ b[i]) & 0xFF] ^ ((c >> 8) & 0x00FFFFFF);
    crc_state = c;
}
static unsigned int crc_final(void) { return crc_state ^ 0xFFFFFFFFu; }

/* write all, retrying on partial writes; -1 on error (errno set) */
static int write_all(int fd, const unsigned char *buf, size_t len)
{
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) return -1;
        if (n == 0) { errno = EIO; return -1; }
        off += (size_t)n;
    }
    return 0;
}

/* --- framed output (stdout) --- */
static void emit(int op, const unsigned char *payload, size_t len)
{
    unsigned char hdr[9];
    hdr[0] = MAGIC[0]; hdr[1] = MAGIC[1];
    hdr[2] = MAGIC[2]; hdr[3] = MAGIC[3];
    hdr[4] = (unsigned char)op;
    wr_u32(hdr + 5, (unsigned int)len);
    (void)write_all(1, hdr, 9);
    if (len) (void)write_all(1, payload, len);
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

/* read all, retrying on partial reads; -1 on error (errno set), else the bytes
 * read (which may be < len near end of device) */
static ssize_t read_all(int fd, unsigned char *buf, size_t len)
{
    size_t off = 0;
    while (off < len) {
        ssize_t n = read(fd, buf + off, len - off);
        if (n < 0) return -1;
        if (n == 0) return (ssize_t)off;  /* EOF: partial read */
        off += (size_t)n;
    }
    return (ssize_t)len;
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
            if (plen < 8) { skip(plen); err_rec(ERR_NI, OP_DATA, 0, cur); continue; }
            unsigned char szf[4];
            if (!rreadn(szf, 4)) break;
            unsigned int size = rd_u32(szf);
            if (plen != 8 + (unsigned long long)size) {  /* malformed frame */
                skip(plen - 4);
                err_rec(ERR_NI, OP_DATA, 0, cur);
                continue;
            }
            unsigned long long start = cur;
            crc_reset();
            unsigned long long done = 0, rd = 0, next_prog = PROG_STEP;  /* rd = bytes read from the stream */
            int status = 0;  /* 0=ok, 1=eof, 2=write error */
            static unsigned char chunk[WRITE_CHUNK_SIZE];
            while (done < size) {
                size_t want = (size_t)(size - done);
                if (want > sizeof chunk) want = sizeof chunk;
                if (!rreadn(chunk, want)) { status = 1; break; }
                rd += want;
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
            if (status == 2) {
                int e = errno;
                if (!skip((unsigned long long)(size - rd) + 4)) {  /* unread content + CRC; failed chunk already consumed */
                    err_rec(ERR_DEV, OP_DATA, (unsigned)e, start);
                    break;
                }
                if (lseek(fd, (off_t)cur, SEEK_SET) < 0) {
                    err_rec(ERR_DEV, OP_DATA, (unsigned)errno, cur);
                    break;
                }
                err_rec(ERR_DEV, OP_DATA, (unsigned)e, start);
            }
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

        else if (op == OP_ZERO) {
            if (plen != 8) { skip(plen); err_rec(ERR_NI, OP_ZERO, 0, cur); continue; }
            unsigned char lf[8];
            if (!rreadn(lf, 8)) break;
            unsigned long long len = rd_u64(lf);
            static unsigned char zbuf[WRITE_CHUNK_SIZE];  /* zero-initialized */
            unsigned long long start = cur, done = 0, next_prog = PROG_STEP;
            int status = 0;
            while (done < len) {
                size_t want = (size_t)(len - done);
                if (want > sizeof zbuf) want = sizeof zbuf;
                if (write_all(fd, zbuf, want) < 0) { status = errno; break; }
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
            if (status) {
                if (lseek(fd, (off_t)cur, SEEK_SET) < 0) {
                    err_rec(ERR_DEV, OP_ZERO, (unsigned)errno, cur);
                    break;
                }
                err_rec(ERR_DEV, OP_ZERO, (unsigned)status, start);  /* no drain: nothing left in the stream */
            }
            else ok_rec(OP_ZERO, start, done);
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
            ssize_t n = read_all(fd, dbuf, (size_t)len);
            if (n < 0) {
                int e = errno;
                if (lseek(fd, (off_t)cur, SEEK_SET) < 0) {
                    err_rec(ERR_DEV, OP_READ, (unsigned)errno, cur);
                    break;
                }
                err_rec(ERR_DEV, OP_READ, (unsigned)e, cur);
                continue;
            }
            emit(R_READ_DATA, dbuf, (size_t)n);  /* the bytes read (may be empty) */
            cur += (unsigned long long)n;
        }

        else if (op == OP_SYNC) {
            if (plen != 0) { skip(plen); err_rec(ERR_NI, OP_SYNC, 0, cur); continue; }
            if (sync_file(fd) < 0)
                err_rec(ERR_DEV, OP_SYNC, (unsigned)errno, cur);  /* durability not achieved; don't claim it */
            else
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
