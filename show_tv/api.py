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

