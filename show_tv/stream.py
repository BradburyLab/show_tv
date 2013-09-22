#!/usr/bin/env python3
# coding: utf-8

import os
import o_p, s_
import math
import list_bl_tv

import getpass
IsTest = getpass.getuser() in ["muravyev", "ilya"]

PORT = 8910

def int_ceil(float_):
    return int(math.ceil(float_))

class StreamType:
    HLS = 0
    HDS = 1

def enum_values(enum):
    return tuple(v for k, v in vars(StreamType).items() if k.upper() == k)
    
# объект самого класса object минималистичен, поэтому не содержит
# __dict__ (который и дает функционал атрибутов); а вот наследники
# получают __dict__ по умолчанию, если только в их описании нет __slots__ - 
# явного списка атрибутов, которые должен иметь класс
class Struct(object):
    pass

def make_struct(**kwargs):
    """ Сделать объект с атрибутами """
    # вообще, для спец. случаев, требующих оптимизации по памяти, можно
    # установить __slots__ равным kwargs.keys()
    stct = Struct()
    stct.__dict__.update(kwargs)
    return stct

def main():
    rn_dct = get_channels()[0]
    # в Bradbury обычно 3 секунду GOP, а фрагмент:
    # - HLS: 9 секунд
    # - HDS: 6 секунд
    std_chunk_dur = 6
    # формат фрагментов
    num_sz = 8
    chunk_tmpl = "out%%0%sd.ts" % num_sz
    def hls_chunk_name(i):
        return chunk_tmpl % i

    # используем namedtuple в качестве ключей, так как 
    # они порождены от tuple => имеют адекватное упорядочивание
    import collections
    RTClass = collections.namedtuple('RTClass', ['refname', 'typ'])
    def r_t_key(refname, typ):
        return RTClass(refname, typ)

    def r_t_iter(iteratable):
        for refname in iteratable:
            for typ in enum_values(StreamType):
                yield r_t_key(refname, typ)

    cr_dct = {}
    # :TODO: создавать по требованию (ленивая инициализация)
    for r_t in r_t_iter(rn_dct):
        cr_dct[r_t] = make_struct(
            is_started = False,
            on_first_chunk_handlers = [],
            r_t = r_t,
            
            stop_signal = False
        )

    if IsTest:
        prefix_dir = os.path.expanduser("~/opt/bl/f451")
        out_dir = o_p.join(prefix_dir, 'tmp/out_dir')
    else:
        out_dir = "/home/ilil/show_tv/out_dir"

    def out_fpath(chunk_dir, *fname):
        return o_p.join(out_dir, chunk_dir, *fname)

    def remove_chunks(rng, cr):
        r_t = cr.r_t
        typ = r_t.typ
        for i in rng:
            if typ == StreamType.HLS:
                fname = hls_chunk_name(i)
            elif typ == StreamType.HDS:
                fname = hds_chunk_name(cr.frg_tbl, i)
                 
            fname = out_fpath(r_t.refname, fname)
            os.unlink(fname)
            
    def ready_chk_end(chunk_range):
        return chunk_range.end-1
    def ready_chunks(chunk_range):
        """ Кол-во дописанных до конца фрагментов """
        return ready_chk_end(chunk_range) - chunk_range.beg
    def written_chunks(chunk_range):
        return range(chunk_range.beg, ready_chk_end(chunk_range))
    
    def may_serve_pl(cnt):
        return cnt >= 2

    def channel_dir(chunk_dir):
        return out_fpath(chunk_dir)
    
    def run_chunker(src_media_path, chunk_dir, on_new_chunk, on_stop_chunking, is_batch=False):
        o_p.force_makedirs(channel_dir(chunk_dir))
        
        if IsTest:
            ffmpeg_bin = os.path.expanduser("~/opt/src/ffmpeg/git/ffmpeg/objs/inst/bin/ffmpeg")
        else:
            ffmpeg_bin = "/home/ilil/show_tv/ffmpeg/objs/inst/bin/ffmpeg"
            
        # :TRICKY: так отлавливаем сообщение от segment.c вида "starts with packet stream"
        log_type = "debug"
        in_opts = "-i " + src_media_path
        emulate_live  = True # False # 
        if IsTest and emulate_live and not is_batch:
            # эмулируем выдачу видео в реальном времени
            in_opts = "-re " + in_opts
        bl_options = "-segment_time %s" % std_chunk_dur
        cmd = "%(ffmpeg_bin)s -v %(log_type)s %(in_opts)s -map 0 -codec copy -f ssegment %(bl_options)s" % locals()
        if IsTest:
            #cmd += " -segment_list %(out_dir)s/playlist.m3u8" % locals()
            pass

        cmd += " %s" % out_fpath(chunk_dir, chunk_tmpl)
        #print(cmd)
        
        import tornado.process
        Subprocess = tornado.process.Subprocess
    
        STREAM = Subprocess.STREAM
        ffmpeg_proc = Subprocess(cmd, stdout=STREAM, stderr=STREAM, shell=True)
     
        import re
        segment_sign = re.compile(b"segment:'(.+)' starts with packet stream:.+pts_time:(?P<pt>[\d,\.]+)")
        def on_line(line):
            m = segment_sign.search(line)
            if m:
                #print("new segment:", line)
                chunk_ts = float(m.group("pt"))
                on_new_chunk(chunk_ts)
    
        line_sep = re.compile(br"(\n|\r\n?).", re.M)
        class errdat:
            txt = b''
        def process_lines(dat):
            errdat.txt += dat
            
            line_end = 0
            while True:
                m = line_sep.search(errdat.txt, line_end)
                if m:
                    line_beg = line_end
                    line_end = m.end(1)
                    on_line(errdat.txt[line_beg:line_end])
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
                on_stop_chunking()
        
        # все придет в on_stderr, сюда только - факт того, что файл
        # закрыли с той стороны (+ пустая строка)
        def on_stderr_end(dat):
            process_lines(dat)
            # последняя строка - может быть без eol
            if errdat.txt:
                on_line(errdat.txt)

            set_stop(True)
        # в stdout ffmpeg ничего не пишет
        #ffmpeg_proc.stdout.read_until_close(on_data, on_data)
        ffmpeg_proc.stderr.read_until_close(on_stderr_end, on_stderr)
    
        def on_proc_exit(exit_code):
            #print("exit_code:", exit_code)
            set_stop(False)
        ffmpeg_proc.set_exit_callback(on_proc_exit)
        
        return ffmpeg_proc.pid

    def test_src_fpath(fname):
        return out_fpath(o_p.join('../test_src', fname))
    
    def test_media_path():
        #return list_bl_tv.make_path("pervyj.ts")
        return test_src_fpath("pervyj-720x406.ts")

    main.stop_streaming = False
    def start_chunking(chunk_range):
        if main.stop_streaming:
            return
        
        # инициализация
        chunk_range.is_started = True
        
        chunk_range.beg = 0
        chunk_range.end = 0
        
        # :TODO: переделать через полиморфизм
        typ = chunk_range.r_t.typ
        if typ == StreamType.HLS:
            start_hls_chunking(chunk_range)
        elif typ == StreamType.HDS:
            start_hds_chunking(chunk_range)
            
    def do_stop_chunking(chunk_range):
        chunk_range.is_started = False
        remove_chunks(range(chunk_range.beg, chunk_range.end), chunk_range)

        may_restart = not chunk_range.stop_signal
        if chunk_range.stop_signal:
            chunk_range.stop_signal = False

        if main.stop_streaming:
            pids = main.stop_pids
            pids.discard(chunk_range.pid)
            if not pids:
                ioloop.stop()
        else:
            if may_restart:
                start_chunking(chunk_range)
            
    #
    # HLS
    #
        
    def start_hls_chunking(chunk_range):
        chunk_range.start_times = []

        def on_new_chunk(chunk_ts):
            chunk_range.start_times.append(chunk_ts)
            chunk_range.end += 1
    
            cnt = ready_chunks(chunk_range)
            if may_serve_pl(cnt):
                hdls = chunk_range.on_first_chunk_handlers
                chunk_range.on_first_chunk_handlers = []
                for hdl in hdls:
                    hdl()
            
            max_total = 72 # максимум столько секунд храним
            max_cnt = int_ceil(float(max_total) / std_chunk_dur)
            diff = cnt - max_cnt
            if diff > 0:
                old_beg = chunk_range.beg
                chunk_range.beg += diff
                del chunk_range.start_times[:diff]
                remove_chunks(range(old_beg, chunk_range.beg), chunk_range)
     
        def on_stop_chunking():
            do_stop_chunking(chunk_range)

        refname = chunk_range.r_t.refname
        if IsTest:
            src_media_path = test_media_path()
        else:
            src_media_path = rn_dct[refname]
        
        chunk_range.pid = run_chunker(src_media_path, refname, on_new_chunk, on_stop_chunking)
    
    def chunk_duration(i, chunk_range):
        i -= chunk_range.beg
        st = chunk_range.start_times
        return st[i+1] - st[i]

    def serve_pl(hdl, chunk_range):
        # :TRICKY: по умолчанию tornado выставляет
        # "text/html; charset=UTF-8", и вроде как по 
        # документации HLS, http://tools.ietf.org/html/draft-pantos-http-live-streaming-08 ,
        # такое возможно, если путь оканчивается на .m3u8 , но в реальности
        # Safari/IPad такое не принимает (да и Firefox/Linux тоже)
        hdl.set_header("Content-Type", "application/vnd.apple.mpegurl")
        
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
            chunk_lst.append("""#EXTINF:%(dur)f,
%(name)s
""" % locals())

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
    def hds_ts(chunk):
        return chunk[1]
    def hds_duration(chunk):
        return chunk[2]

    def hds_chunk_name(frg_tbl, i):
        return "Seg1-Frag%s" % frg_tbl[i][0]

    def start_hds_chunking(chunk_range):
        chunk_range.pid = -1 # эмуляция сущeствования процесса
        import abst
        chunk_range.frg_tbl = frg_tbl = abst.parse_frg_tbl(abst.parse_bi_from_test_f4m())

        # не принимаем пустые таблицы
        assert frg_tbl
        first_chunk = frg_tbl[0]
        
        import timeit
        timer_func = timeit.default_timer
        streaming_start = hds_ts(first_chunk) - timer_func()
        
        def on_new_chunk():
            chunk_range.end += 1
            done_chunk_idx = chunk_range.end-2
            if done_chunk_idx >= 0:
                # копируем готовый фрагмент
                fname = "Seg1-Frag%s" % hds_chunk_name(frg_tbl, i)
                src_fname = o_p.join(abst.get_frg_test_dir(), fname)
                dst_fname = o_p.join(out_fpath(chunk_range.r_t.refname), fname)
                import shutil
                shutil.copyfile(src_fname, dst_fname)
                
            # :REFACTOR: on_first_chunk_handlers + удаление старых
            cnt = ready_chunks(chunk_range)
            if may_serve_pl(cnt):
                hdls = chunk_range.on_first_chunk_handlers
                chunk_range.on_first_chunk_handlers = []
                for hdl in hdls:
                    hdl()
            
            max_total = 72 # максимум столько секунд храним
            #max_cnt = int_ceil(float(max_total) / std_chunk_dur)
            max_cnt = int_ceil(float(max_total) / 3)
            diff = cnt - max_cnt
            if diff > 0:
                old_beg = chunk_range.beg
                chunk_range.beg += diff
                #del chunk_range.start_times[:diff]
                remove_chunks(range(old_beg, chunk_range.beg), chunk_range)

            # эмуляция получения следующего фрагмента
            if chunk_range.end + 1 < len(frg_tbl):
                next_idx = chunk_range.end
                frg = frg_tbl[next_idx]
                
                timeout = hds_ts(frg) - streaming_start - timer_func()
                import datetime
                ioloop.add_timeout(datetime.timedelta(seconds=timeout), on_new_chunk)
            else:
                do_stop_chunking(chunk_range)
            
        on_new_chunk()
            
    #
    # выдача
    #
    
    activity_set = set()
    
    import tornado.web
    import functools
    
    def raise_error(status):
        raise tornado.web.HTTPError(status)
    def make_get_handler(match_pattern, get_handler):
        class Handler(tornado.web.RequestHandler):
            pass
        
        Handler.get = tornado.web.asynchronous(get_handler)
        return match_pattern, Handler

    def get_cr(r_t):
        chunk_range = cr_dct.get(r_t)
        if not chunk_range:
            raise_error(404)
        return chunk_range
    
    def force_chunking(chunk_range):
        if not chunk_range.is_started:
            start_chunking(chunk_range)
            
        return chunk_range.is_started

    # :TODO: поменять playlist.f4m на manifest.f4m
    fmt2typ = {
        "m3u8": StreamType.HLS,
        "f4m":  StreamType.HDS,
    }
    
    def get_playlist(hdl, refname, fmt):
        typ = fmt2typ[fmt]
        
        # :TEMP!!!:
        if typ == StreamType.HDS:
            import abst
            frg_tbl = abst.parse_frg_tbl(abst.parse_bi_from_test_f4m())
            import gen_hds
            is_live = False
            f4m = gen_hds.gen_f4m(refname, gen_hds.gen_abst(frg_tbl, is_live), is_live)
            
            hdl.write(f4m)
            hdl.finish()
            return
        
        r_t = r_t_key(refname, typ)
        chunk_range = get_cr(r_t)
        if not force_chunking(chunk_range):
            # например, из-за сигнала остановить сервер
            raise_error(503)
            
        if may_serve_pl(ready_chunks(chunk_range)):
            serve_pl(hdl, chunk_range)
        else:
            chunk_range.on_first_chunk_handlers.append(functools.partial(serve_pl, hdl, chunk_range))
            
        activity_set.add(r_t)

    handlers = [
        make_get_handler(r"/([-\w]+)/playlist.(m3u8|f4m)", get_playlist),
    ]
    def make_static_handler(chunk_dir):
        return r"/%s/(.*)" % chunk_dir, tornado.web.StaticFileHandler, {"path": channel_dir(chunk_dir)}
        
    for refname in rn_dct:
        handlers.append(
            make_static_handler(refname),
        )
    
    application = tornado.web.Application(handlers)
    application.listen(PORT)

    import tornado.ioloop
    ioloop = tornado.ioloop.IOLoop.instance()
    import signal

    def kill_cr(cr):
        os.kill(cr.pid, signal.SIGTERM)
    
    def on_signal(signum, _ignored_):
        print("Request to stop ...")
        # :TRICKY: вариант с ожиданием завершения оставшихся работ
        # есть на http://tornadogists.org/4643396/ , нам пока не нужен

        main.stop_streaming = True
        
        stop_lst = []
        for cr in cr_dct.values():
            if cr.is_started:
                kill_cr(cr)
                stop_lst.append(cr.pid)
        
        if stop_lst:
            main.stop_pids = set(stop_lst)
        else:
            ioloop.stop()
        
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, on_signal)

    #
    # вещание по запросу
    #
    if IsTest:
        stream_always_lst = ['pervyj']
    else:
        #stream_always_lst = ['pervyj', 'rossia1', 'ntv', 'rossia24', 'peterburg5', 'rbktv']
        stream_always_lst = ['pervyj']
    for r_t in r_t_iter(stream_always_lst):
        # :TEMP!!!:
        if r_t.typ == StreamType.HDS:
            continue
        
        start_chunking(cr_dct[r_t])
        
    def stop_inactives():
        for r_t, cr in cr_dct.items():
            if cr.is_started and r_t not in activity_set and r_t.refname not in stream_always_lst:
                print("Stopping inactive:", r_t)
                cr.stop_signal = True
                kill_cr(cr)
                
        activity_set.clear()
        set_stop_timer()

    stop_period = 600 # 10 минут
    import datetime
    period = datetime.timedelta(seconds=stop_period)
    def set_stop_timer():
        ioloop.add_timeout(period, stop_inactives)
    set_stop_timer()

    ioloop.start()

def get_channels():
    # 1 - наилучшее качество, 3 - наихудшее
    num = 1 # 2
    def mc_out(suffix):
        return "mc_%s_out_%s" % (num, suffix)
    req_clns = ["refname", mc_out("address"), mc_out("port")]

    rn_dct = {}
    names_dct = {}
    
    # :REFACTOR: all_channels()
    with list_bl_tv.make_tbl_clns(req_clns) as (tbl, clns):
        for row in tbl:
            def get_val(name):
                return row[clns[name]]
            def mc_out_val(name):
                return get_val(mc_out(name))
            
            refname = get_val("refname")
            if refname and list_bl_tv.is_streaming(row, clns):
                addr = "udp://%s:%s" % (mc_out_val("address"), mc_out_val("port"))
                rn_dct[refname] = addr
                
                name = list_bl_tv.channel_name(row, clns)
                names_dct[name] = refname
                
    return rn_dct, names_dct

def get_channel_addr(req_channel):
    rn_dct, names_dct = get_channels()
    
    res = names_dct.get(req_channel)
    if res:
        res = rn_dct[res]
    return res
    
if __name__ == "__main__":
    if True:
        main()
        
    if False:
        print(get_channel_addr("Первый канал"))
