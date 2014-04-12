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
# (6) (L) Длина payload
DVR_PREFIX_FMT = "32sLQLBL"

def make_dvr_prefix_format(insert_dvr_magic_number):
    # (0) (L) DVR_MAGIC_NUMBER
    c = "L" if insert_dvr_magic_number else ""
        
    # little-endian правильней, чем "=" = native, ящетаю
    return "<{0}{1}".format(c, DVR_PREFIX_FMT)

from lib.log import Formatter
import logging

def setup_file_logger(fpath, level, logger):
    formatter = Formatter(color=False)
    f = logging.FileHandler(
        fpath,
        mode='w'
    )
    f.setLevel(level)
    f.setFormatter(formatter)
    logger.addHandler(f)

def setup_console_logger(logger, logging_level):
    formatter = Formatter(color=True)
    ch = logging.StreamHandler()
    ch.setLevel(logging_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
def setup_logger(logger, fpath, logging_level):
    logger.setLevel(logging_level)
    setup_console_logger(logger, logging_level)
    setup_file_logger(fpath, logging_level, logger)


import datetime

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

