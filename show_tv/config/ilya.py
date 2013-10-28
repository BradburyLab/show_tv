# coding: utf-8

# для тестирования мультикаста "на живую"
#cast_one_source = "udp://239.0.0.5:1234" # 239 - после транскодера
#cast_one_source = "udp://235.3.3.3:8888" # некий tsplay
#cast_one_source = "udp://236.1.1.1:1234" # cразу после "декриптора"

is_test = True
#emulate_live = False

import os
out_dir = os.path.join(os.path.expanduser("~/opt/bl/f451"), 'tmp/out_dir')
ffmpeg_bin = os.path.expanduser("~/opt/src/ffmpeg/git/ffmpeg/objs/inst/bin/ffmpeg")

use_hds = True # False # 
#real_hds_chunking = False
verbose_tornado = is_test

# по умолчанию DVR включен и хост равен "localhost"; None полностью
# отключает
dvr_host = None # "127.0.0.1" # 
# при тестировании: разные файлы для разных битрейтов, а не один
multibitrate_testing = False

# отладочное логирование
debug_logging = False
