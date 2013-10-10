# coding: utf-8

# для тестирования мультикаста "на живую"
#cast_one_source = "udp://239.0.0.5:1234"

is_test = True
#name = "development"

import os
out_dir = os.path.join(os.path.expanduser("~/opt/bl/f451"), 'tmp/out_dir')
ffmpeg_bin = os.path.expanduser("~/opt/src/ffmpeg/git/ffmpeg/objs/inst/bin/ffmpeg")

use_hds = True # False # 
verbose_tornado = is_test

