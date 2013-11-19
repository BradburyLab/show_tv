#!/usr/bin/env python3
# coding: utf-8

import argparse

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

import re
segment_sign = re.compile(br"(segment|hds):'(.+)' starts with packet stream:.+pts_time:(?P<pt>[\d,\.]+)")

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

def asset_name(r_t_b):
    return asset_name_rt(r_t_b.refname, r_t_b.typ)

DVR_MAGIC_NUMBER = 0x0000f451

# (1) (32s) Имя ассета
# (2) (L) Битрейт
# (3) (Q) Время начала чанка
# (4) (L) Длительность чанка в мс (int),
# (5) (B) Это PVR?
DVR_PREFIX_FMT = "32sLQLB"

def make_prefix_format(insert_dvr_magic_number=True, mid_format=''):
    # (0) (L) DVR_MAGIC_NUMBER
    c = "L" if insert_dvr_magic_number else ""
        
    # little-endian правильней, чем "=" = native, ящетаю
    # (-1) (L) Длина payload
    return "<{0}{1}L".format(c, mid_format)

def make_dvr_prefix_format(insert_dvr_magic_number):
    return make_prefix_format(insert_dvr_magic_number, DVR_PREFIX_FMT)

from lib.log import Formatter
import logging

stream_logger = logging.getLogger('stream')

def setup_file_handler(logger, fpath, level):
    formatter = Formatter(color=False)
    f = logging.FileHandler(
        fpath,
        mode='w'
    )
    f.setLevel(level)
    f.setFormatter(formatter)
    logger.addHandler(f)
    
def create_console_handler():
    formatter = Formatter(color=True)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    return ch

def setup_console_logger(logger, logging_level):
    ch = create_console_handler()
    ch.setLevel(logging_level)
    logger.addHandler(ch)
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
    return calendar.timegm(dt.utctimetuple())

# принятый в Bradbury стандарт записи дата-времени
timestamp_pattern = r"(?P<startstamp>\d{12})\.(?P<milliseconds>\d{3})"
def parse_bl_ts(startstamp, milliseconds):
    def rng2int(idx, ln=2):
        return int(startstamp[idx:idx+2])
    res_ts = datetime.datetime(2000 + rng2int(0), rng2int(2), rng2int(4), 
                               rng2int(6),        rng2int(8), rng2int(10))
    utc_tm = utc_dt2ts(res_ts)
    return int(utc_tm*1000) + int(milliseconds) # в миллисекундах

def ts2bl_str(ts):
    yy = ts.year % 100
    
    # :TRICKY: новое форматирование имеет сущ. ограничения на имен идентификаторов, поэтому
    # для сложных вычислений непригодно, :(
    #ts_str = "{yy:02d}{ts.month:02d}{ts.day:02d}{ts.hour:02d}{ts.minute:02d}{ts.second:02d}.{int(ts.microsecond/1000):03d}".format_map(s_.EvalFormat())
    ts_str = "%(yy)02d%(ts.month)02d%(ts.day)02d%(ts.hour)02d%(ts.minute)02d%(ts.second)02d.%(int(ts.microsecond/1000))03d" % s_.EvalFormat()
    return ts_str

def bl_int_ts2bl_str(ts):
    dt = datetime.datetime.utcfromtimestamp(ts / 1000.)
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
