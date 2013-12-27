#!/usr/bin/env python3
# coding: utf-8
#
# Copyright Bradbury Lab, 2013
# Авторы:
#  Илья Муравьев
#

__version__ = "0.3-dev"

import os
import o_p
import s_
# import list_bl_tv
import logging
import re
import datetime
# functools.partial
import functools

import tornado.process
import tornado.web
import tornado.ioloop

import configuration
from configuration import (
    make_struct,
    get_cfg_value,
    cfg,
    cast_one_source, is_test,
    use_sendfile,
    chunk_dur

)

import api
import mp_server

int_ceil = api.int_ceil

StreamType = api.StreamType

def enum_iterator(enum):
    return iter((k, v) for k, v in vars(enum).items() if k.upper() == k)

def enum_values(enum):
    return tuple(v for k, v in enum_iterator(enum))

from collections import namedtuple

# используем namedtuple в качестве ключей, так как
# они порождены от tuple => имеют адекватное упорядочивание

RTClass = namedtuple('RTClass', ['refname', 'typ'])
RtPClass = namedtuple('RtPClass', ['r_t', 'profile'])

def r_t_p_key(refname, typ, profile):
    return RtPClass(RTClass(refname, typ), profile)


# формат фрагментов
NUM_FORMAT_SIZE = 8
chunk_tmpl = "out%%0%sd.ts" % NUM_FORMAT_SIZE
def hls_chunk_name(i):
    """ Имя i-го фрагмента/чанка """
    return chunk_tmpl % i

db_path = configuration.db_path

def out_fpath(chunk_dir, *fname):
    """ Вернуть полный путь до файла в db-path """
    # return o_p.join(OUT_DIR, chunk_dir, *fname)
    return o_p.join(db_path, chunk_dir, *fname)

real_hds_chunking = get_cfg_value("real_hds_chunking", True)

def get_chunk_fpath(r_t_p, i):
    r_t = r_t_p.r_t
    typ = r_t.typ
    if typ == StreamType.HLS:
        fname = hls_chunk_name(i)
    elif typ == StreamType.HDS:
        fname = hds_chunk_name(i)
    return out_fpath(r_t.refname, r_t_p.profile, fname)

def remove_chunks(rng, cr):
    assert is_master_proc()
    
    r_t_p = cr.r_t_p
    try:
        for i in rng:
            os.unlink(get_chunk_fpath(r_t_p, i))
    except FileNotFoundError:
        # :KLUDGE: не знаю почему, но рисует только один кадр стека
        # (а раньше, в ~python2 было нормально) => нужно самому высчитывать
        # стек с помощью traceback.print_stack, см. Formatter.formatException
        stream_logger.error("remove_chunks", exc_info=True)
        
def ready_chk_end(chunk_range):
    """ Конец списка готовых, полностью записанных фрагментов """
    return chunk_range.end-1
def ready_chunks(chunk_range):
    """ Кол-во дописанных до конца фрагментов """
    return ready_chk_end(chunk_range) - chunk_range.beg
def written_chunks(chunk_range):
    """ Range готовых чанков """
    return range(chunk_range.beg, ready_chk_end(chunk_range))

def is_test_hds_ex(typ):
    return not real_hds_chunking and (typ == StreamType.HDS)

def is_test_hds(chunk_range):
    # :REFACTOR: chunk_range.r_t_p.typ
    return is_test_hds_ex(chunk_range.r_t_p.r_t.typ)

def may_serve_pl(chunk_range):
    """ Можно ли вещать канал = достаточно ли чанков для выдачи в плейлисте """
    if is_test_hds(chunk_range):
        this_chunk_dur = 3
    else:
        this_chunk_dur = chunk_dur

    # максимум столько секунд храним
    min_total = 12
    # :REFACTOR:
    min_cnt = int_ceil(float(min_total) / this_chunk_dur)

    return ready_chunks(chunk_range) >= min_cnt

def emulate_live():
    return is_test and get_cfg_value("emulate_live", True)

is_transcoder = get_cfg_value("transcoder-mode", False)

TPClass = namedtuple('TPClass', ['typ', 'profile'])

def make_chunk_options(t_p, chunk_dir, force_transcoding=False):
    out_dir = out_fpath(chunk_dir)
    o_p.force_makedirs(out_dir)

    typ, profile = t_p

    profiles = {
        "270p": "-s 480x270 -b:v 300k -level 2.1 -b:a 64k",
        "360p": "-s 640x360 -b:v 512k -level 3.0 -b:a 96k",
        "406p": "-s 720x406 -b:v 750k -level 3.1 -b:a 96k",
        "540p": "-s 960x540 -b:v 1400k -level 3.1 -b:a 128k",
        "720p": "-s 1280x720 -b:v 2500k -level 4.0 -b:a 128k",
        "1080p": "-s 1920x1080 -b:v 6000k -level 5.0 -b:a 192k",
        
        # телевизионные (4:3), как у TightVideo
        # уровни: http://en.wikipedia.org/wiki/H.264#Levels
        # :TODO: -b:a скопастен => надо правильный поставить
        "240tv": "-s 320x240 -b:v 450k -level 2.1 -b:a 64k",
        "480tv": "-s 640x480 -b:v 850k -level 3.1 -b:a 96k",
        "540tv": "-s 720x540 -b:v 1300k -level 3.1 -b:a 128k",
    }

    # 44kHz - для HDS
    aac_opts = "-strict experimental -c:a aac -ac 2 -ar 44100"

    def make_out_opts(template, copy_opts):
        if is_transcoder or force_transcoding:
            # :TRICKY: сигнал с головной станции содержит какой-то непонятный третий поток => избавляемся от него
            p_name = profile #[:-1] # без p
            out_opts = "-map 0:0 -map 0:1 -c:v libx264 -profile:v high %s -r 25 %s" % (aac_opts, profiles[p_name])
        else:
            out_opts = copy_opts
        return template % (out_opts, chunk_dur)
    
    is_hls = typ == StreamType.HLS
    if is_hls:
        # :KLUDGE: разобраться наконец, зачем нужен -map 0 для -f ssegment
        chunk_options = make_out_opts("%s -f ssegment -segment_time %s", "-map 0 -codec copy")
    else:
        # :TRICKY: ну никак сейчас без перекодирования
        if get_cfg_value("reencode_hds_sound_to_44kHz", True):
            chunk_options = "-vcodec copy %s" % aac_opts 
        else:
            # :TRICKY: ради нагрузочного тестирования отключаем перекодирование, пускай даже
            # звука и не будет
            chunk_options = "-codec copy -bsf:a aac_adtstoasc"
        chunk_options = make_out_opts("%s -f hds -hds_time %s", chunk_options)
        
        # ts в плейлистах и фрагментах должны совпадать
        # :REFACTOR: + :KLUDGE: либо там, либо тут, но не 2 раза
        now = datetime.datetime.utcnow()
        chunk_options += " -ss -%s" % api.calc_flv_sec(now)

    if get_cfg_value("dump-hls-playlist", False) and is_hls:
        chunk_options += " -segment_list %(out_dir)s/playlist.m3u8" % locals()

    o_fpath = out_fpath(chunk_dir, chunk_tmpl if is_hls else "manifest.f4m")
    chunk_options += " %s" % o_fpath
    
    return chunk_options

def run_chunker_ex(src_media_path, typ, co_lst, on_line, on_stop_chunking, is_batch):
    # ffmpeg_bin = environment.ffmpeg_bin
    ffmpeg_bin = os.path.expanduser(cfg['live']['ffmpeg-bin'])

    is_debug_out = typ == StreamType.HLS
    if is_debug_out:
        ffmpeg_bin += " -v debug"
    
    # :TRICKY: так отлавливаем сообщение от segment.c вида "starts with packet stream"
    in_opts = "-i " + src_media_path
    if emulate_live() and not is_batch:
        # эмулируем выдачу видео в реальном времени
        in_opts = "-re " + in_opts
        
    co_str = " ".join(co_lst)
    cmd = "%(ffmpeg_bin)s %(in_opts)s %(co_str)s" % locals()
    stream_logger.debug("Chunker start: %s", cmd)

    Subprocess = tornado.process.Subprocess
    
    #via_shell = True
    via_shell = False
    import shlex
    cmd = shlex.split(cmd)
    
    STREAM = Subprocess.STREAM
    ffmpeg_proc = Subprocess(cmd, stdout=STREAM, stderr=STREAM, shell=via_shell)

    line_sep = re.compile(br"(\n|\r\n?).", re.M)
    errdat = make_struct(txt=b'')
    
    max_line_size = 10
    err_lst = collections.deque() #[]
    def send_line(line):
        err_lst.append(line)
        overflow = len(err_lst) - max_line_size
        for i in range(overflow):
            err_lst.popleft()
        
        on_line(line)

    def process_lines(dat):
        errdat.txt += dat

        line_end = 0
        while True:
            m = line_sep.search(errdat.txt, line_end)
            if m:
                line_beg = line_end
                line_end = m.end(1)
                send_line(errdat.txt[line_beg:line_end])
            else:
                break

        if line_end:
            errdat.txt = errdat.txt[line_end:]

    def on_stderr(dat):
        #print("data:", dat)
        process_lines(dat)

    # фиксируем прекращение активности транскодера после
    # двух событий
    end_set = set([True, False])

    def set_stop(is_stderr):
        end_set.discard(is_stderr)
        # оба события прошли
        if not end_set:
            on_stop_chunking(make_struct(
                retcode = ffmpeg_proc.returncode, 
                err_lst = err_lst
            ))

    # все придет в on_stderr, сюда только - факт того, что файл
    # закрыли с той стороны (+ пустая строка)
    def on_stderr_end(dat):
        process_lines(dat)
        # последняя строка - может быть без eol
        if errdat.txt:
            send_line(errdat.txt)

        set_stop(True)
    # в stdout ffmpeg ничего не пишет
    #ffmpeg_proc.stdout.read_until_close(on_data, on_data)
    ffmpeg_proc.stderr.read_until_close(on_stderr_end, on_stderr)

    def on_proc_exit(_exit_code):
        #print("exit_code:", _exit_code)
        set_stop(False)
    ffmpeg_proc.set_exit_callback(on_proc_exit)

    return ffmpeg_proc.pid

def run_chunker(src_media_path, t_p, chunk_dir, on_new_chunk, on_stop_chunking, is_batch=False):
    """ Запустить ffmpeg для фрагментирования файла/исходника src_media_path для
        вещания канала chunk_dir;
        - on_new_chunk - что делать в момент начала создания нового фрагмента
        - on_stop_chunking - что делать, если ffmpeg закончил работу
        - is_batch - не эмулировать вещание, только для VOD-файлов """
        
    chunk_options = make_chunk_options(t_p, chunk_dir)

    segment_sign = api.segment_sign
    def on_line(line):
        m = segment_sign.search(line)
        if m:
            #print("new segment:", line)
            chunk_ts = float(m.group("pt"))
            on_new_chunk(chunk_ts)
    
    return run_chunker_ex(src_media_path, t_p.typ, [chunk_options], on_line, on_stop_chunking, is_batch)

def test_src_fpath(fname):
    return out_fpath(o_p.join('../test_src', fname))

def test_media_path(profile):
    # return list_bl_tv.make_path("pervyj.ts")+
    # return test_src_fpath("pervyj-720x406.ts")
    if is_transcoder:
        fname = "pervyj_in_min.ts"
    else:
        fname = "pervyj-{0}.ts".format(profile) if get_cfg_value('multibitrate_testing', True) else "pervyj-720x406.ts"
    return test_src_fpath(fname)

global_variables = make_struct(
    stop_streaming=False
)

a_global_vars = api.global_variables
def is_master_proc():
    return a_global_vars.master_slave_data[0]

def init_cr_start(chunk_range, is_ffmpeg_start):
    chunk_range.is_started = True
    chunk_range.beg = 0
    # номер следующего за пишущимся чанком
    chunk_range.end = 0
    
    if is_ffmpeg_start:
        chunk_range.start_times = []

import json
class WorkerCommand:
    START     = 0
    NEW_CHUNK = 1
    STOP      = 2

def send_worker_command(cmd, chunk_range, *args):
    if a_global_vars.run_workers and is_master_proc():
        r_t_p = chunk_range.r_t_p
        # сериализация r_t_p - вручную раскрываем
        rtp = r_t_p.r_t + (r_t_p.profile,) # r_t_p
        
        data = [cmd, rtp] + list(args)
        msg  = bytes(json.dumps(data), "ascii")
        
        for stream in a_global_vars.master_slave_data[1]:
            mp_server.send_message(stream, msg)

def start_chunking(chunk_range):
    """ Запустить процесс вещания канала chunk_range.r_t_p.refname c типом
        вещания chunk_range.r_t_p.typ (HLS или HDS) """
    assert is_master_proc()
    
    if global_variables.stop_streaming:
        return

    # инициализация
    init_cr_start(chunk_range, False)

    if is_test_hds(chunk_range):
        start_test_hds_chunking(chunk_range)
    else:
        # def start_ffmpeg_chunking(chunk_range):
        chunk_range.start_times = []
    
        def on_new_chunk(chunk_ts):
            add_new_chunk(chunk_range, chunk_ts)
    
        def on_stop_chunking(exit_data):
            do_stop_chunking(chunk_range, exit_data)
    
        (refname, typ), profile = chunk_range.r_t_p
        if cast_one_source:
            src_media_path = cast_one_source
        elif is_test:
            src_media_path = test_media_path(profile)
        else:
            src_media_path = refname2address_dictionary[refname]["res-src"][profile]
    
        chunk_dir = "{0}/{1}".format(refname, profile)
        chunk_range.pid = run_chunker(src_media_path, TPClass(typ, profile), chunk_dir, on_new_chunk, on_stop_chunking)
        
    send_worker_command(WorkerCommand.START, chunk_range)

stream_logger = api.stream_logger
log_status = stream_logger.info

def stop_chunk_range(chunk_range):
    chunk_range.is_started = False
    remove_chunks(range(chunk_range.beg, chunk_range.end), chunk_range)
    
    send_worker_command(WorkerCommand.STOP, chunk_range)

def stop_io_loop(stop_lst):
    if not stop_lst:
        global_variables.io_loop.stop()

def handle_stop_event(chunking_proc, key, exit_data):
    may_restart = not chunking_proc.stop_signal
    if chunking_proc.stop_signal:
        chunking_proc.stop_signal = False
    else:
        # обычно это означает, что ffmpeg не хочет работать сразу
        log_status("""Chunking has been stopped unexpectedly: %s
Return code: %s
Last lines:
'
%s
'
""", chunking_proc, exit_data.retcode, b"".join(exit_data.err_lst).decode("utf-8"))

    need_restart = False
    if global_variables.stop_streaming:
        stop_lst = global_variables.stop_lst
        stop_lst.discard(key)
        
        stop_io_loop(stop_lst)
    else:
        need_restart = may_restart
        
    return need_restart

def do_stop_chunking(chunk_range, exit_data):
    """ Функция, которую нужно выполнить по окончанию вещания (когда закончил
        работу chunker=ffmpeg """
    stop_chunk_range(chunk_range)

    if handle_stop_event(chunk_range, chunk_range.r_t_p, exit_data):
        start_chunking(chunk_range)

from tornado import gen
from app.models.dvr_writer import DVRWriter, write_to_dvr

dvr_host = cfg['live']['dvr-host']

dvr_writer = DVRWriter(
    cfg=cfg,
    host=dvr_host,
    port=cfg['storage']['write-port'],
    use_sendfile=use_sendfile,
)

# :TRICKY: наш DVR-сервер не справляется с 132 каналами * 3 битрейта,
# поэтому такое умолчание (1)
max_dvr_bitrates = get_cfg_value("max-dvr-bitrates", 1)

def filter_profiles(profile_keys):
    # :KLUDGE: ключи словаря не отсортированы, поэтому вручную
    all_profiles = list(profile_keys)
    all_profiles.sort()
    
    return all_profiles[-max_dvr_bitrates:]

def is_bitrate_allowed_to_write(r_t_p):
    res = True
    if max_dvr_bitrates:
        all_profiles = get_profiles(r_t_p.r_t, False).keys()
        res = r_t_p.profile in filter_profiles(all_profiles)
    return res

def add_new_chunk(chunk_range, chunk_ts):
    send_worker_command(WorkerCommand.NEW_CHUNK, chunk_range, chunk_ts)
    
    if chunk_range.end == 0:
        # GMT-время начала трансляции 
        # (чтобы время записи первого чанка было = datetime.datetime.utcnow())
        chunk_range.start = api.utc_dt2ts(datetime.datetime.utcnow()) - chunk_ts

    chunk_range.start_times.append(chunk_ts)
    chunk_range.end += 1

    if may_serve_pl(chunk_range):
        hdls = chunk_range.on_first_chunk_handlers
        chunk_range.on_first_chunk_handlers = []
        for hdl in hdls:
            hdl()

    max_total = get_cfg_value('max_total', 72) # максимум столько секунд храним
    max_cnt = int_ceil(float(max_total) / chunk_dur)
    diff = ready_chunks(chunk_range) - max_cnt

    r_t_p = chunk_range.r_t_p

    # <DVR writer> --------------------------------------------------------
    if (
        is_master_proc() and 
        bool(dvr_host) and
        chunk_range.end > 1 and
        is_bitrate_allowed_to_write(r_t_p)
    ):
        # индекс того чанка, который готов
        i = chunk_range.end-2
        start_time = chunk_range.start_times[i-chunk_range.beg]
        # путь до файла с готовым чанком
        path_payload = get_chunk_fpath(
            r_t_p,
            i
        )

        if r_t_p.r_t.typ == StreamType.HDS:
            # ffmpeg может выдавать большее значение, поэтому снова
            # считаем остаток
            flv_ts = api.calc_flv_rest(api.dur2millisec(start_time))
            utc_ts = api.utc_dt2ts(api.restore_utc_ts(flv_ts))
        else:
            utc_ts = chunk_range.start + start_time
        # длина чанка
        duration = chunk_duration(i, chunk_range)
        write_to_dvr(dvr_writer, path_payload, api.dur2millisec(utc_ts), duration, chunk_range)

        # :TRICKY: tornado умеет генерить ссылки только для простых случаев
        #print(global_variables.application.reverse_url("playlist_multibitrate", chunk_range.r_t_p.refname, 1, 1, "f4m"))
    # ------------------------------------------------------- </DVR writer>

    if diff > 0:
        old_beg = chunk_range.beg
        chunk_range.beg += diff
        del chunk_range.start_times[:diff]
        if is_master_proc():
            remove_chunks(range(old_beg, chunk_range.beg), chunk_range)

def chunk_duration(i, chunk_range):
    """ Длительность фрагмента в секундах, float """
    i -= chunk_range.beg
    st = chunk_range.start_times
    return st[i+1] - st[i]

#
# HLS
#

def serve_hls_pl(hdl, chunk_range):
    """ Выдача плейлиста HLS, .m3u8;
        type(hdl) == tornado.web.RequestHandler """
    # :TRICKY: по умолчанию tornado выставляет
    # "text/html; charset=UTF-8", и вроде как по
    # документации HLS, http://tools.ietf.org/html/draft-pantos-http-live-streaming-08,
    # такое возможно, если путь оканчивается на .m3u8 , но в реальности
    # Safari/IPad такое не принимает (да и Firefox/Linux тоже)
    hdl.set_header("Content-Type", "application/vnd.apple.mpegurl")
    profile = chunk_range.r_t_p.profile

    write = hdl.write
    # EXT-X-TARGETDURATION - должен быть, и это
    # должен быть максимум
    max_dur = 0
    chunk_lst = []
    for i in written_chunks(chunk_range):
        dur = chunk_duration(i, chunk_range)
        name = hls_chunk_name(i)

        max_dur = max(dur, max_dur)

        # используем %f (6 знаков по умолчанию) вместо %s, чтобы на
        # '%s' % 0.0000001 не получать '1e-07'
        chunk_lst.append("""#EXTINF:{dur:f},
{profile}/{name}
""".format(**locals()))

    # по спеке это должно быть целое число, иначе не работает (IPad)
    max_dur = int_ceil(max_dur)

    # EXT-X-MEDIA-SEQUENCE - номер первого сегмента,
    # нужен для указания клиенту на то, что список живой,
    # т.е. его элементы будут добавляться/исчезать по FIFO
    write("""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-ALLOW-CACHE:NO
#EXT-X-TARGETDURATION:%(max_dur)s
#EXT-X-MEDIA-SEQUENCE:%(chunk_range.beg)s
""" % s_.EvalFormat())

    for s in chunk_lst:
        write(s)

    # а вот это для live не надо
    #write("#EXT-X-ENDLIST")

    hdl.finish()

#
# HDS
#

import gen_hds

def hds_chunk_name(i):
    return "Seg1-Frag%s" % (i+1)

# копипаст shutil._ensure_directory()
def ensure_directory(path):
    """Ensure that the parent directory of `path` exists"""
    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

# :REFACTOR: куча всего одинакового с start_ffmpeg_chunking(),
# однако этот код все равно тестовый
def start_test_hds_chunking(chunk_range):
    # import abst
    import test_hds
    frg_base, frg_tbl = test_hds.get_frg_tbl()
    assert frg_base == 1
    chunk_range.frg_tbl = frg_tbl

    # не принимаем пустые таблицы
    assert frg_tbl
    first_chunk = frg_tbl[0]

    import timeit
    timer_func = timeit.default_timer
    streaming_start = gen_hds.get_frg_ts(first_chunk) - timer_func()

    def on_new_chunk():
        chunk_range.end += 1

        # копируем готовый фрагмент
        # (заранее, хоть он пока и "не готов")
        done_chunk_idx = chunk_range.end-1
        fname = hds_chunk_name(done_chunk_idx)
        src_fname = o_p.join(test_hds.get_frg_test_dir(), fname)
        dst_fname = get_chunk_fpath(chunk_range.r_t_p, done_chunk_idx)
        import shutil
        ensure_directory(dst_fname)
        shutil.copyfile(src_fname, dst_fname)

        # :REFACTOR: on_first_chunk_handlers + удаление старых
        if may_serve_pl(chunk_range):
            hdls = chunk_range.on_first_chunk_handlers
            chunk_range.on_first_chunk_handlers = []
            for hdl in hdls:
                hdl()

        # максимум столько секунд храним
        max_total = cfg['live']['max_total']
        #max_cnt = int_ceil(float(max_total) / chunk_dur)
        max_cnt = int_ceil(float(max_total) / 3)
        diff = ready_chunks(chunk_range) - max_cnt
        if diff > 0:
            old_beg = chunk_range.beg
            chunk_range.beg += diff
            #del chunk_range.start_times[:diff]
            remove_chunks(range(old_beg, chunk_range.beg), chunk_range)

        # эмуляция получения следующего фрагмента
        if chunk_range.stop_signal or (chunk_range.end >= len(frg_tbl)):
            do_stop_chunking(chunk_range)
        else:
            next_idx = chunk_range.end
            frg = frg_tbl[next_idx]

            if not is_test or emulate_live():
                timeout = gen_hds.get_frg_ts(frg) - streaming_start - timer_func()
                global_variables.io_loop.add_timeout(datetime.timedelta(seconds=timeout), on_new_chunk)
            else:
                global_variables.io_loop.add_callback(on_new_chunk)

    on_new_chunk()

def disable_caching(hdl):
    # судя по всему, для live нужно ставить эти заголовки, иначе плейер
    # решит, что список не изменяется (так делает 1tv (не всегда) и wowza)
    no_cache = [
        ('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0'),
        ("Pragma",        "no-cache"),
    ]
    for header in no_cache:
        hdl.set_header(*header)

    # :TRICKY: 1tv ставит еще Expires, Last-Modified, Vary, Accept-Ranges,
    # Age, но полагаю, что это к делу не относится (OSMFу все равно)

def serve_hds_abst(hdl, start_idx, frg_tbl, is_live):
    hdl.set_header("Content-Type", "binary/octet")
    disable_caching(hdl)

    frg_base = gen_hds.make_frg_base(start_idx)
    abst = gen_hds.gen_abst(frg_base, frg_tbl, is_live)

    hdl.write(abst)
    hdl.finish()

def serve_hds_pl(hdl, chunk_range):
    if real_hds_chunking:
        lst = gen_hds.make_frg_tbl(chunk_range.start_times)
    else:
        lst = chunk_range.frg_tbl[chunk_range.beg:ready_chk_end(chunk_range)]

    serve_hds_abst(hdl, chunk_range.beg, lst, True)

def profile2res(profile):
    res = cfg['res'].get(profile)
    if res is None:
        raise_error(404, "No such video profile")
    return res

def get_f4m(hdl, refname, is_live, url_prefix=None):
    """ url_prefix доп. url для плейлистов и фрагментов """
    # согласно FlashMediaManifestFileFormatSpecification.pdf
    hdl.set_header("Content-Type", "application/f4m+xml")
    # :TRICKY: не нужно вроде
    disable_caching(hdl)

    profiles = get_profiles_by_rt(refname, StreamType.HDS, is_live)
    medias = []
    for profile in profiles:
        abst_url = "%s.abst" % profile
        seg_url  = "%s/" % profile
        if url_prefix:
            def join(idx, url):
                return "{0}/{1}".format(url_prefix[idx], url)
                
            abst_url = join(0, abst_url)
            seg_url  = join(1, seg_url)
        medias.append(gen_hds.gen_f4m_media(refname, abst_url, profile2res(profile)["bitrate"], seg_url))
    f4m = gen_hds.gen_f4m(refname, is_live, "\n".join(medias))
    hdl.write(f4m)

# словарь "имя канала" => адрес входящий адрес вещания
refname2address_dictionary = cfg['udp-source']

# словарь (имя канала, тип вещания, разрешение) => chunk_range
chunk_range_dictionary = {}

def make_dictionary(**kwargs):
    return kwargs

stream_def_attrs = make_dictionary(
    is_started=False,
    stop_signal=False,
)

def make_c_r(c_r_key):
    # :TODO: создавать по требованию (ленивая инициализация)
    return make_struct(
        on_first_chunk_handlers=[],
        r_t_p=c_r_key,
        **stream_def_attrs
    )

def get_profiles(r_t, raise_404):
    channel = chunk_range_dictionary.get(r_t)
    if channel:
        profiles = channel.profiles
    else:
        if raise_404:
            raise_error(404)
        profiles = None

    return profiles

def get_c_r(r_t_p, raise_404=False):
    c_r = None
    
    profiles = get_profiles(r_t_p.r_t, raise_404)
    if profiles:
        c_r = profiles.get(r_t_p.profile)
        if not c_r and raise_404:
            raise_error(404)

    return c_r

def get_profiles_by_rt(refname, typ, is_live):
    profiles = get_profiles(RTClass(refname, typ), True).keys()
    if not is_live and max_dvr_bitrates:
        profiles = filter_profiles(profiles)
    return profiles

import collections
def init_crd():
    for refname, cfg in refname2address_dictionary.items():
        # пока только 2 варианта - вещание и транскодирование из одного
        # источника
        
        is_transcoding = False
        res_p = cfg.get('res')
        
        # :TODO: остальные варианты деления на чанки реализовать
        res_src_p = cfg.get('res-src')
        if not res_src_p:
            params = cfg.get('params')
            if params:
                is_transcoding = params.get("transcoding")

            if not (is_transcoding and res_p):
                continue
        
        for typ in enum_values(StreamType):
            r_t = RTClass(refname, typ)
            channel = chunk_range_dictionary[r_t] = make_struct(
                r_t = r_t,
                profiles = collections.OrderedDict(), # {}
                is_transcoding = is_transcoding,
                # :KLUDGE: для is_transcoding используется этот is_started,
                # иначе используется c_r.is_started (дублирование)
                **stream_def_attrs
            )
            
            profiles_src = res_p if is_transcoding else res_src_p
            profiles = channel.profiles
            for profile in profiles_src:
                profiles[profile] = make_c_r(RtPClass(r_t, profile))
                    
init_crd()

def raise_error(status, desc=None):
    raise tornado.web.HTTPError(status, reason=desc)

def make_get_handler(match_pattern, get_handler, is_async=True, name=None):
    """ Альтернатива созданию торнадовского обработчика: get_handler - обработка
        GET-запроса
        По умолчанию создается асинхронный обработчик, т.е. дополнительный
        декоратор @tornado.web.asynchronous не нужен"""
    class Handler(tornado.web.RequestHandler):
        pass

    Handler.get = tornado.web.asynchronous(get_handler) if is_async else get_handler
    return tornado.web.url(match_pattern, Handler, name=name)

def start_channel(channel):
    r_t = channel.r_t
    assert not is_test_hds_ex(r_t.typ)
    channel.is_started = True
    
    # :REFACTOR!!!:
    refname, typ = r_t
    if cast_one_source:
        src_media_path = cast_one_source
    elif is_test:
        src_media_path = test_src_fpath("pervyj_in_min.ts")
    else:
        src_media_path = refname2address_dictionary[refname]["src"]

    profiles = channel.profiles
    co_lst = []
    for p_name in profiles:
        # :REFACTOR!!!:
        chunk_dir = "{0}/{1}".format(refname, p_name)
        co_lst.append(make_chunk_options(TPClass(typ, p_name), chunk_dir, True))

    segment_sign = api.create_segment_re("(.+/(?P<profile>%s)/.+)" % "|".join(profiles))
    def on_line(line):
        m = segment_sign.search(line)
        if m:
            profile, chunk_ts = str(m.group("profile"), "ascii"), float(m.group("pt"))
            add_new_chunk(profiles[profile], chunk_ts)
            
    def on_stop_chunking(exit_data):
        for chunk_range in iterate_cr(channel):
            stop_chunk_range(chunk_range)
    
        if handle_stop_event(channel, r_t, exit_data):
            start_channel(channel)

    channel.pid = run_chunker_ex(src_media_path, typ, co_lst, on_line, on_stop_chunking, False)
    
    for chunk_range in iterate_cr(channel):
        # :REFACTOR!!!:
        # инициализация
        init_cr_start(chunk_range, False)
        
        # def start_ffmpeg_chunking(chunk_range):
        chunk_range.start_times = []
        
        send_worker_command(WorkerCommand.START, chunk_range)

def force_chunking(r_t):
    """ Начать вещание канала по требованию """

    # стартуем все разрешения - мультибитрейт же
    res = True
    
    channel = chunk_range_dictionary.get(r_t)
    if channel:
        is_master = is_master_proc()
        
        if channel.is_transcoding:
            if is_master and not(global_variables.stop_streaming or channel.is_started):
                start_channel(channel)
            res = channel.is_started
        else:
            for c_r in iterate_cr(channel):
                if is_master and not c_r.is_started:
                    start_chunking(c_r)
        
                if not c_r.is_started:
                    res = False
                    break
    else:
        res = False
        stream_logger.error("force_chunking: no such channel, %s", r_t, stack_info=True)

    return res

from app.models.dvr_reader import DVRReader, call_dvr_cmd
dvr_reader = DVRReader(
    cfg=cfg,
    host=cfg['live']['dvr-host'],
    port=cfg['storage']['read-port'],
)

import file_dvr

def make_rtb_db(r_t_p):
    return file_dvr.RTPDbClass(
        r_t_p = r_t_p,
        db_path = db_path
    )

@gen.engine
def serve_dvr_chunk(hdl, r_t_p, startstamp, callback=None):
    if configuration.local_dvr:
        payload = file_dvr.request_chunk(make_rtb_db(r_t_p), startstamp)
    else:
        payload = check_dvr_backend((yield gen.Task(
            call_dvr_cmd,
            dvr_reader,
            dvr_reader.load,
            r_t_p=r_t_p,
            startstamp=startstamp,
        )))
    hdl.finish(payload)

    if callback:
        callback(None)

def get_hls_dvr(hdl, asset, startstamp, duration, profile):
    r_t_p = r_t_p_key(asset, StreamType.HLS, profile)
    startstamp = int(startstamp)
    serve_dvr_chunk(hdl, r_t_p, startstamp)

from tornado import template

def check_dvr_pars(startstamp, duration):
    if (startstamp is None) != (duration is None):
        raise_error(400)

def get_mb_playlist(hdl, asset, extension, is_live, url_prefix):
    typ = Fmt2Typ[extension]
    if typ == StreamType.HLS:
        hdl.set_header("Content-Type", "application/vnd.apple.mpegurl")

        loader = template.Loader(os.path.join(
            configuration.cur_directory,
            'app',
            'templates',
        ))
        profile_bandwitdhs = [[profile, profile2res(profile)["bandwidth"]] for profile in get_profiles_by_rt(asset, StreamType.HLS, is_live)]
        playlist = loader.load('multibitrate/playlist.m3u8').generate(
            profile_bandwitdhs=profile_bandwitdhs,
        )
        hdl.write(playlist)
    elif typ == StreamType.HDS:
        get_f4m(hdl, asset, is_live, url_prefix)
    else:
        assert False

    hdl.finish()

def get_playlist_multibitrate(hdl, asset, extension, startstamp=None, duration=None):
    check_dvr_pars(startstamp, duration)
    get_mb_playlist(hdl, asset, extension, startstamp is None, None)

def ts2sec(ts):
    # хранилка держит timestamp'ы в миллисекундах
    return ts / 1000.0

def load_remote_dvr_pl(r_t_p, startstamp, duration, callback):
    call_dvr_cmd(
        dvr_reader, 
        dvr_reader.request_range,
        r_t_p=r_t_p,
        startstamp=startstamp,
        duration=duration,
        callback=callback
    )

def check_dvr_backend(res):
    is_ok, data = res
    if not is_ok:
        raise_error(502)
    return data

@gen.engine
def load_dvr_pl(r_t_p, startstamp, duration, callback):
    if configuration.local_dvr:
        playlist_data = file_dvr.request_range(make_rtb_db(r_t_p), startstamp, int(duration))
    else:
        playlist_data = check_dvr_backend((yield gen.Task(load_remote_dvr_pl, r_t_p, startstamp, duration)))
    
    callback(playlist_data)

@gen.engine
def get_playlist_dvr(hdl, r_t_p, startstamp, duration):
    playlist_data = yield gen.Task(load_dvr_pl, r_t_p, startstamp, duration)

    if playlist_data:
        r_t = r_t_p.r_t
        if r_t.typ == StreamType.HLS:
            hdl.set_header('Content-Type', 'application/vnd.apple.mpegurl')
            playlist = dvr_reader.generate_playlist(
                host=hdl.request.host,
                asset=r_t.refname,
                startstamps_durations=[
                    (r['startstamp'], ts2sec(r['duration']))
                    for r in playlist_data
                ],
                profile=r_t_p.profile,
            )
            hdl.finish(playlist)
        elif r_t.typ == StreamType.HDS:
            # :TRICKY: время в плейлистах и фрагментах должно совпадать!
            #first_ts = playlist_data[0]['startstamp']
            #frg_tbl = [[ts2sec(r['startstamp']-first_ts), ts2sec(r['duration'])] for r in playlist_data]
            #frg_tbl = [[ts2sec(r['startstamp']), ts2sec(r['duration'])] for r in playlist_data]
            
            frg_tbl = [[api.ts2flv(r['startstamp']), ts2sec(r['duration'])] for r in playlist_data]
            
            serve_hds_abst(hdl, 0, frg_tbl, False)
        else:
            assert False
    else:
        raise_error(404)

@gen.engine
def get_hds_dvr(hdl, r_t_p, startstamp, duration, frag_num):
    frag_num = int(frag_num)
    if frag_num < 1:
        raise_error(404)
    idx = frag_num - 1

    # :TRICKY: заново берем плейлист, потому что нужно отсчитать
    # фрагмент с индексом idx; решено, что в "продакшене" хранилка сумеет
    # поддержать вызов load c offset'ом и сама отдаст по HTTP, т.е. лишней
    # работы не будет

    playlist_data = yield gen.Task(load_dvr_pl, r_t_p, startstamp, duration)
    if idx > len(playlist_data):
        raise_error(404)

    startstamp = playlist_data[idx]['startstamp']
    serve_dvr_chunk(hdl, r_t_p, startstamp)

Fmt2Typ = {
    "m3u8": StreamType.HLS,
    "abst": StreamType.HDS,
    "f4m":  StreamType.HDS,
}

def get_playlist_singlebitrate(hdl, asset, profile, extension, startstamp=None, duration=None):
    """ Обработчик выдачи плейлистов playlist.m3u8 и manifest.f4m """

    check_dvr_pars(startstamp, duration)

    typ = Fmt2Typ[extension]
    r_t_p = r_t_p_key(asset, typ, profile)
    if startstamp is not None and duration is not None:
        get_playlist_dvr(hdl, r_t_p, startstamp, duration)
        return

    chunk_range = get_c_r(r_t_p, True)

    r_t = r_t_p.r_t
    if not force_chunking(r_t):
        # например, из-за сигнала остановить сервер
        raise_error(503)

    # :TODO: переделать через полиморфизм?
    serve_pl = {
        StreamType.HLS: serve_hls_pl,
        StreamType.HDS: serve_hds_pl,
    }[typ]
    if may_serve_pl(chunk_range):
        serve_pl(hdl, chunk_range)
    else:
        chunk_range.on_first_chunk_handlers.append(functools.partial(serve_pl, hdl, chunk_range))

    if stream_by_request:
        activity_set.add(r_t)

#
# остановка сервера
#
import signal

def try_kill_chunking_proc(c_p, is_real_proc=True):
    """ Послать сигнал chunker'у (ffmpeg) прекратить работу """
    is_started = c_p.is_started
    if is_started:
        c_p.stop_signal = True
    
        if is_real_proc:
            # :TRICKY: если нет мультикаста, то ffmpeg не завершается
            # на первый SIGTERM
            # :TODO: в идеале надо бы пускать пару (SIGTERM, SIGKILL)
            # через таймаут, но лень (только сообщение о новом чанке может не успеть
            # придти)
            os.kill(c_p.pid, signal.SIGKILL) # SIGTERM)
    return is_started

def iterate_cr(channel):
    return channel.profiles.values()

def stop_channel(channel, stop_lst):
    def stop_cp(cp, is_real_proc, key):
        if try_kill_chunking_proc(cp, is_real_proc):
            stop_lst.append(key)
    
    if channel.is_transcoding:
        stop_cp(channel, True, channel.r_t)
    else:
        for cr in iterate_cr(channel):
            is_real_proc = not is_test_hds(cr)
            stop_cp(cr, is_real_proc, cr.r_t_p)

def on_signal(_signum, _ignored_):
    """ Прекратить работу сервера show_tv по Ctrl+C """
    
    if is_master_proc():
        log_status("Request to stop ...")
        # :TRICKY: вариант с ожиданием завершения оставшихся работ
        # есть на http://tornadogists.org/4643396/ , нам пока не нужен
        global_variables.stop_streaming = True
    
        stop_lst = []
        for channel in chunk_range_dictionary.values():
            stop_channel(channel, stop_lst)
    
        # :TRICKY: даже если stop_lst пуст, все равно заводим атрибут - 
        # может сработать ожидающий handle_stop_event()
        global_variables.stop_lst = set(stop_lst)

        stop_io_loop(global_variables.stop_lst)
    else:
        # рабочие процессы завершаются по закрытию мастера
        pass

#
# вещание по запросу
#

stream_by_request = get_cfg_value("stream_by_request", False)

def calc_sal():
    def get_channel_lst():
        return sorted(refname2address_dictionary)
    
    if not stream_by_request and get_cfg_value("stream_all_channels", False):
        stream_always_lst = get_channel_lst()
    else:
        stream_range = get_cfg_value("stream-range", None)
        if stream_range:
            full_lst = get_channel_lst()
            stream_always_lst = api.calc_from_stream_range(full_lst, stream_range)
        else:
            stream_always_lst = get_cfg_value("stream-always-lst", ['pervyj'])
    return stream_always_lst

stream_always_lst = calc_sal()

if stream_by_request:
    # список вещаемых каналов типа RTClass = (refname, typ) прямо сейчас 
    activity_set = set()
    
    def stop_inactives():
        """ Прекратить вещание каналов, которые никто не смотрит в течении STOP_PERIOD=10 минут """
        
        for r_t, channel in chunk_range_dictionary.items():

            # :TRICKY: пока старт c_r будет происходить всегда, даже в случае 1-M-процесса,
            # это будет работать
            is_started = False
            for c_r in iterate_cr(channel):
                if c_r.is_started:
                    is_started = True
                    break
            
            if is_started and r_t not in activity_set and r_t.refname not in stream_always_lst:
                log_status("Stopping inactive: %s", r_t)
                stop_channel(channel, [])
                
        activity_set.clear()
        set_stop_timer()
    
    STOP_PERIOD = 600 # 10 минут
    def set_stop_timer():
        period = datetime.timedelta(seconds=STOP_PERIOD)
        global_variables.io_loop.add_timeout(period, stop_inactives)

def activate_web(sockets):
    if use_sendfile:
        from static_handler import StaticFileHandler
        static_cls_handler = StaticFileHandler
    else:
        static_cls_handler = tornado.web.StaticFileHandler
        
    def make_static_handler(pattern, root_fdir, handler_cls):
        return pattern, handler_cls, {"path": root_fdir}
    
    wowza_links = get_cfg_value("wowza-links", True)

    handlers = []
    def extend_hdls(*hdls):
        handlers.extend(hdls)

    def append_hdl(hdl):
        handlers.append(hdl)
    
    def append_sync_hdl(match_pattern, get_handler):
        append_hdl(make_get_handler(match_pattern, get_handler, False))
        
    def force_exception(hdl):
        1 / 0
    append_sync_hdl(r"/force_exception", force_exception)
    
    # всеразрешающий crossdomain.xml для HDS
    def get_cd_xml(hdl):
        hdl.write(gen_hds.least_restrictive_cd_xml())
    append_sync_hdl(r"/crossdomain.xml", get_cd_xml)

    def append_static_handler(pattern, handler_cls):
        append_hdl(make_static_handler(pattern, db_path, handler_cls))

    # :TODO: все ссылки обрабатывать рекурсивным развертыванием %()s или
    # чем-то подобным
    def make_link_pattern(fmt, **kwargs):
        kwargs.update(
            # nauka2.0
            asset = r"(?P<asset>[-\w\.]+)",
            profile = r"(?P<profile>\w+)",
        )
        
        return fmt % kwargs

    if wowza_links:
        def make_wwz_pattern(pattern):
            return r"^/live/(?:_definst_/)?" + pattern
        
        def wwz_mb_playlist(hdl, asset, is_live, url_prefix):
            get_mb_playlist(hdl, asset, "f4m", is_live, url_prefix)
        
        # live 
        def make_wwz_live_pattern(pattern):
            return make_wwz_pattern(make_link_pattern(r"smil:%(asset)s_sd/") + pattern)
        
        def make_wwz_live_handler(pattern, get_handler):
            return make_get_handler(make_wwz_live_pattern(pattern), get_handler)
        
        def wwz_mb_live_playlist(hdl, asset, url_prefix):
            wwz_mb_playlist(hdl, asset, True, url_prefix)

        def wwz_sb_live_playlist(hdl, asset, profile):
            get_playlist_singlebitrate(hdl, asset, profile, "abst")
            
        # DVR 
        def parse_wwz_ts(month, day, startstamp):
            dt_class = datetime.datetime
            
            # вычисление startstamp
            td = datetime.timedelta(milliseconds=int(startstamp))
            def make_ts(year):
                dt = dt_class(year=year, month=int(month), day=int(day)) + td
                return dt
            
            # :TRICKY: с портала время приходит в локальном времени =>
            # упоротость разработчиков конечно зашкаливает
            is_local_time = True
            now = dt_class.now() if is_local_time else dt_class.utcnow()
            today = now.date()
            
            ts1, ts2 = make_ts(today.year), make_ts(today.year-1)
            # выбираем дату, что ближе к сейчас
            res_ts = ts1 if abs(now - ts1) < abs(now - ts2) else ts2

            if is_local_time:
                res_ts = dt_class.utcfromtimestamp(res_ts.timestamp())
            return res_ts

        wwz_simplified_links = get_cfg_value("wowza-simplified-links", True)
        
        www_dvr_link    = get_cfg_value("www-dvr-server",    "")
        www_stream_link = get_cfg_value("www-stream-server", "")
        if not www_stream_link:
            www_stream_link = www_dvr_link
        
        if not wwz_simplified_links:
            assert not www_dvr_link
            assert not www_stream_link

        def wwz_mb_dvr_playlist(hdl, month, day, asset):
            start, duration = hdl.get_argument("start", None), hdl.get_argument("duration", None)
            if not(start and duration):
                # :TODO: сделать "live dvr" последнего часа 
                #start = 0
                #duration = 86400*1000
                #raise_error(400) # Bad Request
                
                # редирект на live
                # :TRICKY: 3XX не работает, так как OSMF запоминает оригинальный url манифеста,
                # и считает url-ы листов и фрагментов от него (тупица)
                #hdl.redirect("../smil:%s_sd/manifest.f4m" % asset)
                # :REFACTOR: расчет ссылок "smil:%s_sd" дважды
                url = "../smil:%s_sd" % asset
                wwz_mb_live_playlist(hdl, asset, [url, url])
                return
                
            if configuration.local_dvr:
                # :TRICKY: потому что только для тестов
                r_t_p = (asset, api.StreamType.HDS), "270p"
                ts, duration = file_dvr.test_dvr_range(make_rtb_db(r_t_p))
                ts = api.bl_int_ts2bl_str(ts)
            else:
                ts = api.ts2bl_str(parse_wwz_ts(month, day, start))
            
            url_prefix = "{0}/{1}".format(ts, duration)
            if wwz_simplified_links:
                url_prefix = "/{0}/{1}".format(asset, url_prefix)
                url_prefix = [url_prefix, url_prefix if configuration.local_dvr else "/data{0}".format(url_prefix)]
                # :KLUDGE: клиент не умеет ходить по корневым относительным
                # ссылкам, поэтому везде приходится писать абсолютные ссылки
                def transform(idx, prefix):
                    if not prefix:
                        host_port = hdl.request.headers["Host"]
                        # :KLUDGE: схему тоже нужно от proxy получать
                        # :TODO: по стандарту Host должен содержать оригинальный порт,
                        # а не содержит (nginx не передает либо клиент/wget?/браузер в Host не проставляет)
                        if host_port:
                            prefix = "http://%s" % host_port
                    
                    if prefix:
                        url_prefix[idx] = "{0}{1}".format(prefix, url_prefix[idx])
                        
                transform(0, www_stream_link)
                transform(1, www_dvr_link)
            else:
                url_prefix = [url_prefix, url_prefix]
            wwz_mb_playlist(hdl, asset, False, url_prefix)
            
        def make_wwz_dvr_handler(pattern, get_handler):
            pattern = make_wwz_pattern(make_link_pattern(r"(?P<month>\d\d)_(?P<day>\d\d)_%(asset)s_(?:\d+)p/") + pattern)
            return make_get_handler(pattern, get_handler)

        extend_hdls(
            # /live/ [ _definst_/ ] smil:discoverychannel_sd/manifest.f4m
            make_wwz_live_handler(r"manifest.f4m",           functools.partial(wwz_mb_live_playlist, url_prefix=None)), #on_wwz_mb_live_playlist),
            make_wwz_live_handler(r"(?P<profile>\w+)\.abst", wwz_sb_live_playlist),
            
            # DVR
            # live/ [ _definst_/ ] 11_07_discoverychannel_576p/manifest.f4m?DVR&start=123456789000&duration=60000
            make_wwz_dvr_handler(r"manifest.f4m", wwz_mb_dvr_playlist),
        )
        
        def run_dvr_handler(get_handler, hdl, asset, full_ts, duration, profile, **kwargs):
            # :REFACTOR:
            typ = Fmt2Typ["abst"]
            r_t_p = r_t_p_key(asset, typ, profile)
            
            #res_ts = parse_wwz_ts(month, day, startstamp)
            #ts = int(res_ts.timestamp()*1000)
            ts = api.parse_bl_ts(full_ts)
            return get_handler(hdl, r_t_p, ts, duration, **kwargs)
        
        if wwz_simplified_links:
            # /discoverychannel/131113131113.000/60000/360.abst
            # /discoverychannel/131113131113.000/60000/360/Seg1-FragN
            def make_dvr_handler(is_pl, get_handler, add_keys):
                def handler(hdl, asset, full_ts, duration, profile, **kwargs):
                    kwargs = {k:kwargs[k] for k in add_keys if k in kwargs}
                    return run_dvr_handler(get_handler, hdl, asset, full_ts, duration, profile, **kwargs)
                content_prefix = r"(?:data/)?" # ""
                m_pat = make_link_pattern(r"^/%(c_p)s%(asset)s/%(ts)s/(?P<duration>\d+)/%(profile)s", 
                                          c_p=content_prefix, ts=api.timestamp_pattern)
                pattern = r"\.abst" if is_pl else r"/Seg1-Frag(?P<frag_num>\d+)"
                return make_get_handler(m_pat + pattern, handler)
            
            def append_dvr_handler(is_pl, get_handler, add_keys=[]):
                append_hdl(make_dvr_handler(is_pl, get_handler, add_keys))
            
            append_dvr_handler(True, get_playlist_dvr)
            append_dvr_handler(False, get_hds_dvr, ["frag_num"])
        else:
            # :TEMP: удалить, как только станет ясно что wwz_simplified_links работает
            def make_wwz_dvr_proxy_handler(pattern, get_handler):
                def handler(hdl, month, day, asset, full_ts, duration, profile, **kwargs):
                    return run_dvr_handler(get_handler, hdl, asset, full_ts, duration, profile, **kwargs)
                return make_wwz_dvr_handler(api.timestamp_pattern + r"/(?P<duration>\d+)/" + pattern, handler)
             
            handlers.extend([
                # live/ [ _definst_/ ] 11_07_discoverychannel_576p/123456789000/60000/360.abst
                make_wwz_dvr_proxy_handler(r"(?P<profile>\w+)\.abst", get_playlist_dvr),
                # live/ [ _definst_/ ] 11_07_discoverychannel_576p/123456789000/60000/360/Seg1-FragN
                make_wwz_dvr_proxy_handler(r"(?P<profile>\w+)/Seg1-Frag(?P<frag_num>\d+)", get_hds_dvr),
            ])
        
        class WowzaStaticHandler(static_cls_handler):
            def get(self, asset, path, include_body=True):
                path = "{0}/{1}".format(asset, path)
                super().get(path, include_body=include_body)
                
        # /live/_definst_/smil:discoverychannel_sd/360/Seg1-Frag22
        append_static_handler(make_wwz_live_pattern(r"(?P<path>.*)"), WowzaStaticHandler)
    else:
        def make_stream_handler(match_pattern, get_handler):
            return make_get_handler(make_link_pattern("^/%(asset)s/") + match_pattern, get_handler)
        
        def on_get_hds_dvr(hdl, asset, startstamp, duration, profile, frag_num):
            r_t_p = r_t_p_key(asset, StreamType.HDS, profile)
            get_hds_dvr(hdl, r_t_p, startstamp, duration, frag_num)
        
        extend_hdls(
            # :REFACTOR: куча копипасты
            make_stream_handler(r"(?:(?P<startstamp>\d+)/(?P<duration>\d+)/)?smil.(?P<extension>m3u8|f4m)", get_playlist_multibitrate),
            make_stream_handler(r"(?:(?P<startstamp>\d+)/(?P<duration>\d+)/)?(?P<profile>\w+)\.(?P<extension>m3u8|abst)", get_playlist_singlebitrate),
            make_stream_handler(r"(?P<startstamp>\d+)/(?P<duration>\d+)/(?P<profile>\w+)\.ts", get_hls_dvr),
            make_stream_handler(r"(?P<startstamp>\d+)/(?P<duration>\d+)/(?P<profile>\w+)/Seg1-Frag(?P<frag_num>\d+)", on_get_hds_dvr),
        )
    
        append_static_handler(r"/(.*)", static_cls_handler)

    application = tornado.web.Application(handlers)
    #application.listen(port)
    
    from tornado.httpserver import HTTPServer
    server = HTTPServer(application)
    #sockets = tornado.netutil.bind_sockets(port)
    server.add_sockets(sockets)
     
    # не нужно
    #global_variables.application = application

import sentry

def main():
    logo = """Fahrenheit 451 mediaserver. Frontend OTT server.
Copyright Bradbury Lab, 2013
"""
    
    if cfg["do_show_version"]:
        print(logo + "Version %s" % __version__)
        import sys
        sys.exit(0)
    
    port = get_cfg_value("port", 9451)
    log_status(
        '\n'
        '{logo}'
        'Listens at 0.0.0.0:{port}\n'
        .format(**locals())
    )
    
    # для мультипроцессинга: биндим порт сразу, чтобы потом работник(и)/основной
    # процесс могли подключаться к нему TCPServer.add_sockets() для ожидания соединений 
    from tornado.netutil import bind_sockets
    sockets = bind_sockets(port)

    # увеличиваем максим. возможное кол-во открываемых файлов до возможного
    # максимума (под Ubuntu это значение по умолчанию есть `ulimit -n` = 4096)
    # :TRICKY: если 4096 не хватает, т.е. вылезает "Too many open files", и это не
    # утечка незакрытых файлов, то поправить /etc/security/limits.conf и перезайти: 
    # * hard nofile 100500
    import resource
    hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))

    run_workers = get_cfg_value("run-web-workers", True)
    if run_workers:
        MAX_W_CNT = -1
        workers_count = get_cfg_value("web-workers-count", MAX_W_CNT)
        if workers_count == MAX_W_CNT:
            workers_count = max(tornado.process.cpu_count() - 1, 1) 
        
        is_child, data = mp_server.fork_slaves(workers_count)
        
        is_master = not is_child
        m_s_data  = is_master, data
        
        from lib.log import update_fmt_prefix, FMT_PREFIX
        
        def update_fp(name):
            update_fmt_prefix("{0} [{1}]".format(FMT_PREFIX, name))
            
        if is_master:
            update_fp("M")
        else:
            idx, stream = data
            update_fp(str(idx))
            
            def on_message(tpl, data):
                stream_logger.debug("New message: %s, %s", tpl, data)
                data = json.loads(str(data, "ascii"))
                
                cmd, r_t_p, *args = data
                r_t_p = r_t_p_key(*r_t_p)
                chunk_range = get_c_r(r_t_p)
                
                if cmd == WorkerCommand.START:
                    # :TRICKY: режим run_workers совместим только 
                    # с реальным фрагментированием
                    assert real_hds_chunking
                    init_cr_start(chunk_range, True)
                elif cmd == WorkerCommand.NEW_CHUNK:
                    chunk_ts = args[0]
                    add_new_chunk(chunk_range, chunk_ts)
                elif cmd == WorkerCommand.STOP:
                    chunk_range.is_started = False
                else:
                    assert False
                
            def on_stop():
                log_status("Request to exit from master")
                
            mp_server.setup_msg_handler(stream, on_message, on_stop)
            log_status("Worker %s is started", idx)
    else:
        is_master = True
        m_s_data  = is_master, []

    a_global_vars.run_workers       = run_workers
    a_global_vars.master_slave_data = m_s_data
        
    global_variables.io_loop = tornado.ioloop.IOLoop.instance()

    if is_master:
        # включенные по умолчанию форматы вещания
        stream_fmt_defaults = {
            "hls": True,
            "hds": True,
        }
        stream_fmt_set = set()
        for key, def_val in stream_fmt_defaults.items():
            is_on = get_cfg_value("stream_{0}".format(key), def_val)
            if is_on:
                stream_fmt_set.add(vars(StreamType)[key.upper()])
        
        for i, refname in enumerate(stream_always_lst):
            # эмуляция большого запуска для разработчика - не грузим сильно машину
            # :TRICKY: вещание всех каналов (132) в 6 битрейтах (3*HLS + 3*HDS) 
            # у меня (Муравьев) уже не влезает по памяти (8Gb),
            # поэтому только первые 80 (а с 4Gb - только 30 без тормозов)
            if is_test and i >= 30:
                break
            
            for typ in stream_fmt_set:
                force_chunking(RTClass(refname, typ))
    
        if stream_by_request:
            set_stop_timer()
    
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, on_signal)

    if not(is_master and run_workers):
        activate_web(sockets)

    log_status("Starting IOLoop...")
    
    io_loop = global_variables.io_loop
    def start_io_loop():
        sentry.update_to_async_client()
        io_loop.start()
    
    if get_cfg_value("do_profiling", False):
        #from profile import Profile
        from cProfile import Profile
        prof = Profile()

        orig_impl = io_loop._impl
        orig_poll = orig_impl.poll
        
        def poll_wrapper(*args, **kw):
            prof.disable()
            try:
                return orig_poll(*args, **kw)
            finally:
                prof.enable()
        
        class Proxy:
            def __init__(self, obj):
                self.obj = obj
                
            def __getattr__(self, attr_name):
                attr = poll_wrapper if attr_name == "poll" else getattr(self.obj, attr_name)
                return attr
        
        io_loop._impl = Proxy(orig_impl)
        
        # по аналогии с runcall()
        prof.enable()
        
        start_io_loop()
        
        prof.disable()
        if a_global_vars.run_workers:
            suffix = "M" if is_master else str(a_global_vars.master_slave_data[1][0])
        else:
            suffix = ""
            
        if suffix:
            suffix = "[%s]" % suffix
        fstats = o_p.join(cfg['path_log'], "stream-{0}{1}.{2}".format(api.utcnow_str(), suffix, "py_stats"))
        prof.dump_stats(fstats)        
    else:
        start_io_loop()
    
if __name__ == "__main__":
    with sentry.catched_exceptions():
        main()
