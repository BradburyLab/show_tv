#!/usr/bin/env python
# coding: utf-8

from call_cmd import call_cmd
import o_p, os

if __name__ == '__main__':
    is_libav = False
    
    if is_libav:
        src_dir = "/home/ilya/opt/src/ffmpeg/git/libav"
        add_opts = ""
    else:
        src_dir = "/home/ilil/show_tv/ffmpeg"
        add_opts = "--disable-stripping"
    bld_dir = o_p.join(src_dir, "objs")

    if os.path.exists(bld_dir):
        import shutil
        shutil.rmtree(bld_dir)
        
    o_p.force_makedirs(bld_dir)
    inst_dir = o_p.join(bld_dir, "inst")
    call_cmd("../configure --prefix=%(inst_dir)s --disable-optimizations --extra-cflags='-O0 -g' --extra-ldflags='-Wl,-rpath=%(inst_dir)s/lib' \
%(add_opts)s --enable-shared --disable-static" % locals(), bld_dir)
    
    call_cmd("make -j 7 V=1 install", bld_dir)
