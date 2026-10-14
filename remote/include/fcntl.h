#ifndef _FCNTL_H
#define _FCNTL_H

int open(const char *path, int flags);

#define O_RDONLY 0
#define O_WRONLY 1
#define O_RDWR   2
#endif
