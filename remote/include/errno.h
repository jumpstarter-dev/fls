#ifndef _ERRNO_H
#define _ERRNO_H
/* QNX: errno is a per-thread location reached via __get_errno_ptr(). */
int *__get_errno_ptr(void);
#define errno (*__get_errno_ptr())
/* only the constants the code assigns */
#define EIO 5
#endif
