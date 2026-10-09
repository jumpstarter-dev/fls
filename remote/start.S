    .globl _start
    .type _start, @function
_start:
    /* initial stack: [argc, argv..., NULL, envp..., NULL] */
    ldr x0, [sp]          /* x0 = argc */
    add x1, sp, #8       /* x1 = argv = sp+8 */
    add x2, x1, x0, lsl #3  /* x2 = argv + argc*8 */
    add x2, x2, #8       /* x2 = envp (skip NULL terminator) */
    bl main              /* main(argc, argv, envp) */
    bl exit              /* exit(main's return) */
    .size _start, .-_start
