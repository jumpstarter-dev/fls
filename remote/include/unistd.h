#ifndef _UNISTD_H
#define _UNISTD_H
#include <sys/types.h>

ssize_t read(int fd, void *buf, size_t count);
ssize_t write(int fd, const void *buf, size_t count);
off_t lseek(int fd, off_t offset, int whence);
int fdatasync(int fd);

#define SEEK_SET 0
#define SEEK_CUR 1
#define SEEK_END 2
#endif
