#!/usr/bin/env python
# coding: utf-8

from call_cmd import call_cmd
import o_p, os

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--is_libav',
        dest='is_libav', type=bool, default=False,
        help='to build libav instead of ffmpeg',
    )
    parser.add_argument(
        '--is_debug',
        type=bool, default=False,
    )
    
    parser.add_argument("src_path", help="where is ffmpeg/libav source directory")
    args = parser.parse_args()
    
    is_libav = args.is_libav
    src_dir  = args.src_path
    if is_libav:
        add_opts = ""
    else:
        if args.is_debug:
            add_opts = "--disable-stripping"
        else:
            add_opts = ""
    bld_dir = o_p.join(src_dir, "objs")

    if os.path.exists(bld_dir):
        import shutil
        shutil.rmtree(bld_dir)
        
    o_p.force_makedirs(bld_dir)
    inst_dir = o_p.join(bld_dir, "inst")
    debug_opts = "--disable-optimizations --extra-cflags='-O0 -g' " if args.is_debug else ""
    call_cmd("../configure --prefix=%(inst_dir)s %(debug_opts)s--extra-ldflags='-Wl,-rpath=%(inst_dir)s/lib' \
%(add_opts)s --enable-shared --disable-static" % locals(), bld_dir)
    
    call_cmd("make -j 7 V=1 install", bld_dir)
