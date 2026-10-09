#!/usr/bin/env python3
"""
Build a minimal stub .so from a JSON symbol manifest.

Given a JSON file describing a library (its SONAME plus the symbol names
it exports), this tool assembles and links a standard, well-formed shared
library that exports exactly those names. The stub contains no real code
(each function is a `ret` no-op) and no real data (each variable is 8
zero bytes) just enough for a linker to resolve symbol names against
the library's SONAME.

JSON input format (version 1):
  {
    "version": 1,
    "soname": "libz.so.2",
    "needed": [],               # reserved, currently unused
    "funcs": ["inflate", ...],  # exported as STT_FUNC
    "vars":  ["errno", ...]     # exported as STT_OBJECT, 8 bytes each
  }

Usage:
  python3 stubs/so_stub.py <in.json> [-o out.so]
  e.g. python3 stubs/so_stub.py stubs/libz.min.json -o stubs/libz.min.so_stub.so
"""
import json
import os
import struct
import subprocess
import sys

# --- ELF constants (64-bit, little-endian) --------------------------------
PT_LOAD = 1       # program header: loadable segment (vaddr -> file offset)
PT_DYNAMIC = 2    # program header: dynamic section (DT_* entries)
DT_NULL = 0       # end of the dynamic section
DT_STRTAB = 5     # virtual address of the dynamic string table
DT_SONAME = 11    # string-table offset of the library's SONAME

# Offsets within a 64-bit ELF header
E_PHOFF = 0x20    # e_phoff: file offset of the program header table
E_PHNUM = 0x38    # e_phnum: number of program headers
E_PHENTSIZE = 0x36  # e_phentsize: size of one program header


class Elf:
    """Minimal ELF reader: find a segment by type, map vaddrs to file offsets."""

    def __init__(self, data):
        self.d = data
        self.e_phoff = struct.unpack_from('<Q', data, E_PHOFF)[0]
        self.e_phnum = struct.unpack_from('<H', data, E_PHNUM)[0]
        self.e_phentsize = struct.unpack_from('<H', data, E_PHENTSIZE)[0]

    def _segments(self):
        """Yield (p_type, header_offset) for each program header."""
        for i in range(self.e_phnum):
            ph = self.e_phoff + i * self.e_phentsize
            yield struct.unpack_from('<I', self.d, ph)[0], ph

    def find_segment(self, p_type):
        """File offset of the first program header of the given type."""
        for t, ph in self._segments():
            if t == p_type:
                return struct.unpack_from('<Q', self.d, ph + 8)[0]
        return None

    def vaddr_to_file(self, vaddr):
        """Map a virtual address to a file offset via the PT_LOAD segments."""
        for t, ph in self._segments():
            if t != PT_LOAD:
                continue
            p_off = struct.unpack_from('<Q', self.d, ph + 8)[0]
            p_vaddr = struct.unpack_from('<Q', self.d, ph + 0x10)[0]
            p_filesz = struct.unpack_from('<Q', self.d, ph + 0x20)[0]
            if p_vaddr <= vaddr < p_vaddr + p_filesz:
                return p_off + (vaddr - p_vaddr)
        return None


def build_stub(soname, funcs, variables, out):
    """Assemble + link a stub .so exporting `funcs` (STT_FUNC) and
    `variables` (STT_OBJECT, 8 bytes each) with `soname`."""
    # 1. Emit assembly: each function is a `ret` no-op, each variable is 8
    #    zero bytes. The linker only needs the names + types to be present.
    asm = out + '.s'
    with open(asm, 'w') as f:
        f.write('.text\n')
        for s in funcs:
            f.write(f'.globl {s}\n.type {s}, @function\n{s}:\n  ret\n')
        if variables:
            f.write('.data\n')
            for s in variables:
                f.write(f'.globl {s}\n.type {s}, @object\n.size {s}, 8\n{s}:\n  .quad 0\n')

    # 2. Assemble, then link into a shared library.
    obj = out + '.o'
    subprocess.run(['aarch64-elf-gcc', '-c', asm, '-o', obj], check=True)
    ld = os.environ.get('STUB_LD', 'ld.lld')
    subprocess.run([ld, '-shared', '-o', out, obj,
                    '--soname=' + soname], check=True)

    # 3. lld records --soname in DT_STRTPATH instead of DT_SONAME, so patch
    #    DT_SONAME by hand after linking (see patch_soname).
    patch_soname(out, soname)


def patch_soname(path, soname):
    """Point DT_SONAME at the soname string (workaround for an lld quirk)."""
    d = bytearray(open(path, 'rb').read())
    elf = Elf(bytes(d))

    dyn_off = elf.find_segment(PT_DYNAMIC)
    if dyn_off is None:
        return

    # Walk the dynamic section: 16-byte DT_* entries, DT_NULL-terminated.
    dyn = []
    for i in range(64):
        o = dyn_off + i * 16
        tag = struct.unpack_from('<q', d, o)[0]
        val = struct.unpack_from('<q', d, o + 8)[0]
        dyn.append((tag, val))
        if tag == DT_NULL:
            break

    strtab_vaddr = next((v for t, v in dyn if t == DT_STRTAB), None)
    if strtab_vaddr is None:
        return

    # lld already wrote the soname into the string table; locate it.
    idx = d.find(soname.encode())
    if idx < 0:
        return

    # DT_STRTAB is a vaddr; convert to a file offset to find the table.
    strtab_foff = elf.vaddr_to_file(strtab_vaddr)
    if strtab_foff is None:
        return

    # DT_SONAME wants an offset *within* the string table, not a vaddr.
    strtab_off = idx - strtab_foff
    for i in range(64):
        o = dyn_off + i * 16
        tag = struct.unpack_from('<q', d, o)[0]
        if tag == DT_NULL:
            break
        if tag == DT_SONAME:
            struct.pack_into('<Q', d, o + 8, strtab_off)
            open(path, 'wb').write(d)
            return


def compile_json(ir, out):
    """Build a stub .so from a JSON symbol manifest."""
    if ir.get('version', 1) != 1:
        raise SystemExit(f"unsupported json version {ir.get('version')}")
    build_stub(ir['soname'], ir.get('funcs', []), ir.get('vars', []), out)


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    path = sys.argv[1]
    out = None
    if '-o' in sys.argv[2:]:
        out = sys.argv[sys.argv.index('-o') + 1]
    ir = json.load(open(path))
    out = out or os.path.splitext(os.path.basename(path))[0] + '_stub.so'
    compile_json(ir, out)
    print(f"{path}: SONAME={ir['soname']}, {len(ir.get('funcs', []))} funcs + "
          f"{len(ir.get('vars', []))} vars -> {out}")
    print("link against it:  ld.lld -o prog start.o prog.o -L. -l:" +
          os.path.basename(out))


if __name__ == '__main__':
    main()
