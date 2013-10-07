# coding: utf-8

is_test = True
name = "development"

import os
prefix_dir = os.path.expanduser("~/opt/bl/f451")
out_dir = os.path.join(prefix_dir, 'tmp/out_dir')

ffmpeg_bin = os.path.expanduser("~/opt/src/ffmpeg/git/ffmpeg/objs/inst/bin/ffmpeg")

use_hds = True # False # 
verbose_tornado = is_test

