#!/usr/bin/env python3
# coding: utf-8

import argparse
import struct
import os
import math

def int_ceil(float_):
    """ Округлить float в больщую сторону """
    return int(math.ceil(float_))

# в модуле argparse уже есть "rock solid"-реализация
# структуры, поэтому используем ее
USE_NAMESPACE = True
if USE_NAMESPACE:
    def make_struct(**kwargs):
        return argparse.Namespace(**kwargs)
else:
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


segment_sign_tmpl = r"(segment|hds):'%s' starts with packet stream:.+pts_time:(?P<pt>[\d,\.]+)"

import re
def create_segment_re(path):
    return re.compile(bytes(segment_sign_tmpl % path, "ascii"))

#segment_sign = re.compile(br"(segment|hds):'(.+)' starts with packet stream:.+pts_time:(?P<pt>[\d,\.]+)")
segment_sign = create_segment_re("(.+)")

class StreamType:
    HLS = 0
    HDS = 1

# :KLUDGE: пока DVR-хранилка не знает о типе вещания => храним в asset'е
DVR_SUFFEXES = {
    StreamType.HLS: "hls",
    StreamType.HDS: "hds",
}

def asset_name_rt(refname, typ):
    return "{0}_{1}".format(refname, DVR_SUFFEXES[typ])

def asset_name(r_t_p):
    r_t = r_t_p.r_t
    return asset_name_rt(r_t.refname, r_t.typ)

DVR_MAGIC_NUMBER = 0x0000f451

# (1) (32s) Имя ассета
# (2) (6s) Профиль
# (3) (Q) Время начала чанка
# (4) (L) Длительность чанка в мс (int),
# (5) (B) Это PVR?
DVR_PREFIX_FMT = "32s6sQLB"

def make_prefix_format(insert_dvr_magic_number=True, mid_format=''):
    # (0) (L) DVR_MAGIC_NUMBER
    c = "L" if insert_dvr_magic_number else ""
        
    # little-endian правильней, чем "=" = native, ящетаю
    # (-1) (L) Длина payload
    return "<{0}{1}L".format(c, mid_format)

def make_dvr_prefix_format(insert_dvr_magic_number):
    return make_prefix_format(insert_dvr_magic_number, DVR_PREFIX_FMT)

class FormatType:
    META = 0
    HLS  = 1
    HDS  = 2
    HLS_ENCRYPTED = 3

def pack_cmd(fmt, cmd, *args):
    return struct.pack("<B" + fmt, cmd, *args)

def encode_strings(*args):
    return tuple(s.encode() for s in args)

def pack_rtp_cmd(cmd, r_t_p, fmt_tail, *tail_args):
    (refname, typ), profile = r_t_p
    
    return pack_cmd(
        "B32s6s" + fmt_tail,
        cmd,
        FormatType.HLS if typ == StreamType.HLS else FormatType.HDS,
        *(encode_strings(refname, profile) + tail_args)
    )

from lib.log import Formatter
import logging

stream_logger = logging.getLogger('stream')

def setup_handler(handler, level, logger, is_colored):
    handler.setLevel(level)
    
    formatter = Formatter(color=is_colored)
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)

def setup_file_handler(logger, fpath, level, is_append=False):
    f = logging.FileHandler(
        fpath,
        mode= "a" if is_append else "w"
    )
    
    setup_handler(f, level, logger, False)
    
def setup_console_logger(logger, logging_level):
    ch = logging.StreamHandler()

    setup_handler(ch, logging_level, logger, True)
    return ch
    
def setup_logger(logger, fpath, logging_level):
    logger.setLevel(logging_level)
    setup_console_logger(logger, logging_level)
    setup_file_handler(logger, fpath, logging_level)


import datetime
import calendar
import s_

def utcnow():
    return datetime.datetime.utcnow()

def ts2str(ts):
    """Формат передачи timestamp - строка в ISO 8601 (в UTC)"""
    res = ""
    if ts:
        # если нет tzinfo, то Py ничего не добавляет, а по стандарту надо
        suffix = "" if ts.tzinfo else "Z"
        res = ts.isoformat() + suffix
    return res

def utcnow_str():
    return ts2str(utcnow())

def utc_dt2ts(dt):
    # :TRICKY: .timestamp() вызывает mktime() => изменяет на величину
    # часового пояса, черт
    #utc_tm = res_ts.timestamp()
    # :TRICKY: разницы между utctimetuple() и timetuple() нет, если
    # в dt не установлен tzinfo
    return calendar.timegm(dt.utctimetuple()) + dt.microsecond / 1000000.

def dur2millisec(duration):
    return int(duration*1000)

# принятый в Bradbury стандарт записи дата-времени
timestamp_pattern = r"(?P<full_ts>(?P<year_base>\d{2})?(?P<startstamp>\d{12})\.(?P<milliseconds>\d{3}))"
def parse_bl_ts(full_ts):
    m = re.match(timestamp_pattern, full_ts)
    assert m

    startstamp, milliseconds = m.group("startstamp"), m.group("milliseconds")
    year_base = m.group("year_base")
    year_base = (20 if year_base is None else int(year_base))*100
    
    def rng2int(idx, ln=2):
        return int(startstamp[idx:idx+2])

    res_ts = datetime.datetime(year_base + rng2int(0), rng2int(2), rng2int(4), 
                               rng2int(6),        rng2int(8), rng2int(10))
    utc_tm = utc_dt2ts(res_ts)
    return dur2millisec(utc_tm) + int(milliseconds) # в миллисекундах

def ts2bl_str(ts):
    year = ts.year
    
    yy = year % 100
    if (year - yy) - 2000:
        yy = year
    
    # :TRICKY: новое форматирование имеет сущ. ограничения на имен идентификаторов, поэтому
    # для сложных вычислений непригодно, :(
    #ts_str = "{yy:02d}{ts.month:02d}{ts.day:02d}{ts.hour:02d}{ts.minute:02d}{ts.second:02d}.{int(ts.microsecond/1000):03d}".format_map(s_.EvalFormat())
    # :TRICKY: int_ceil() вместо int(), чтобы избежать погрешностей в 1 миллисекунду, см. request_names(rtp_db, startstamp, 1)
    ts_str = "%(yy)02d%(ts.month)02d%(ts.day)02d%(ts.hour)02d%(ts.minute)02d%(ts.second)02d.%(int_ceil(ts.microsecond/1000))03d" % s_.EvalFormat()
    return ts_str

def bl_int_ts2py_ts(ts):
    return datetime.datetime.utcfromtimestamp(ts / 1000.)

def bl_int_ts2bl_str(ts):
    dt = bl_int_ts2py_ts(ts)
    return ts2bl_str(dt)

global_variables = make_struct(
    run_workers = False,
)

import tornado.iostream
import socket

def connect(host, port, callback):
    stream = tornado.iostream.IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM))

    def on_connection():
        assert not stream._connecting
        # успех => не нужен
        stream.set_close_callback(None)
        
        callback(stream)
    stream.connect((host, port), on_connection)
    assert stream._connecting
    
    def on_close():
        callback(None)
    stream.set_close_callback(on_close)

class StreamState:
    """ tornado.iostream.IOStream дает информацию
        о соединении в плохом виде, поэтому сами держим состояние """
    CLOSED     = 0
    CONNECTING = 1
    OPENED     = 2

dvr_wlogger = logging.getLogger("DVRWriter")

def connect_to_dvr(obj, addr, write_func):
    dvr_ctx = getattr(obj, "dvr_ctx", None)
    if not dvr_ctx:
        obj.dvr_ctx = dvr_ctx = make_struct(state=StreamState.CLOSED)
        
    def start_connection():
        dvr_ctx.state = StreamState.CONNECTING
        
        host, port = addr
        def on_connection(stream):
            if stream:
                dvr_ctx.state = StreamState.OPENED
                dvr_ctx.stream = stream
                dvr_ctx.is_first = True
            else:
                dvr_ctx.state = StreamState.CLOSED
                dvr_wlogger.error("Can't connect to DVR server %s:%s", host, port)
                
        connect(host, port, on_connection)
        
    if dvr_ctx.state == StreamState.CLOSED:
        start_connection()
    elif dvr_ctx.state == StreamState.OPENED:
        def check_stream():
            is_closed = dvr_ctx.stream.closed()
            if is_closed:
                dvr_ctx.stream = None
                dvr_ctx.state = StreamState.CLOSED
            return not is_closed
            
        if check_stream():
            dvr_wlogger.debug('[DVRWriter] write start >>>>>>>>>>>>>>>')
            
            try:
                write_func(dvr_ctx.stream, dvr_ctx.is_first)
            except:
                dvr_wlogger.error("DVR Writer Exception", exc_info=1)
                
            dvr_wlogger.debug('[DVRWriter] write finish <<<<<<<<<<<<<<\n')
            
            dvr_ctx.is_first = False
            check_stream()
        else:
            start_connection()

def calc_from_stream_range(full_lst, stream_range):
    stream_lst = []
    def append_range(beg, end):
        stream_lst.extend(full_lst[beg:end])
        
    def append_ch_range(p1, p2):
        assert (p1 >= 1) and (p2 >= p1) and (p2 <= q), "for example, 2/2 means second part of two parts"
        ln = len(full_lst)
        beg, end = (ln*(p1-1))//q, (ln*p2)//q
        
        append_range(beg, end)
    
    if 'names' in stream_range:
        s_lst = stream_range["names"].split(",")
        for s in s_lst:
            def get_index(s):
                return full_lst.index(s.strip())
            if "-" in s:
                beg, end = [get_index(s) for s in s.split("-")]
            else:
                beg = end = get_index(s)
                
            append_range(beg, end+1)
    elif 'part' in stream_range:
        p_lst, q = stream_range["part"].split("/")
        q = int(q)
        
        for p in p_lst.split(","):
            p_r = p.split("-")
            ln  = len(p_r)
            assert ln <= 2
            
            p1 = p_r[0]
            p2 = p1
            if ln > 1:
                p2 = p_r[1]
            append_ch_range(int(p1), int(p2))
    elif 'python' in stream_range:
        p_lst, q = stream_range["python"]
        stream_range = []
        for p in p_lst:
            if type(p) == list:
                p1, p2 = p
            else:
                p1, p2 = p, p
            append_ch_range(p1, p2)
    elif 'size' in stream_range:
        stream_lst = full_lst[:stream_range["size"]]
    else:
        assert False, "stream-range accepts part or size attributes"
    return stream_lst

def rtp2local_dvr(r_t_p, db_path):
    (name, typ), profile = r_t_p
    return os.path.join(db_path, "local_dvr", "%s=%s=%s" % (name, DVR_SUFFEXES[typ], profile))

# константы, не менять при работающем DVR
# :TRICKY: ровно такое число значений может принимать поле ts в FLV-контейнере
# (чуть меньше 25 дней)
FLV_PERIOD = 2**31
first_date = datetime.datetime(2013, 12, 1)

def calc_flv_rest(delta):
    return delta % FLV_PERIOD

def calc_flv_delta(py_ts):
    return dur2millisec((py_ts - first_date).total_seconds())

def calc_flv_ts(py_ts):
    return calc_flv_rest(calc_flv_delta(py_ts))

def calc_flv_sec(py_ts):
    return calc_flv_ts(py_ts) / 1000.

def restore_utc_ts(flv_ts):
    now = datetime.datetime.utcnow()
    now_delta = calc_flv_delta(now)
    flv_now   = calc_flv_rest(now_delta)

    diff1 = flv_now - flv_ts
    if diff1 > 0:
        diff2 = diff1 - FLV_PERIOD
    else:
        diff2 = diff1 + FLV_PERIOD
        
    nearest_diff = diff1 if abs(diff1) < abs(diff2) else diff2
    delta = now_delta - nearest_diff
    return first_date + datetime.timedelta(milliseconds=delta)
        
def ts2flv(ts):
    return calc_flv_sec(bl_int_ts2py_ts(ts))
