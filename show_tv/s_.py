#!/usr/bin/env python
# coding: utf-8

import sys

#
# Код работы со строками
# Название s_ выбрано исходя их краткости и того, что оно
# непохоже на имя переменной (по крайней мере для Python;
# стереотип, что в C++ так обозначают приватные имена атрибутов
# классов, очень мешал, но все равно)
#
# И да, понимаю, что это неформатное имя для публичного кода на Python'е,
# но тут я к этому и не стремлюсь
#

# :TODO: разобраться и оптимизировать (должно же быть быстрее) по такому сценарию:
# import codecs
# encoder_func = codecs.lookup("utf_8").encode
# ...
#     return encoder_func(s)

def to_utf8(s):
    """ unicode => str """
    return s.encode("utf_8")

def to_uni(s):
    """ str => unicode """
    return s.decode("utf_8")

class EvalFormat:
    """ locals() не подходит для глобальных переменных и вызова функций """
    def init_context(self):
        fo = sys._getframe(2)
        self.globs = fo.f_globals
        self.locs  = fo.f_locals

    def eval_key(self, key):
        return eval(key, self.globs, self.locs)
    
    def __init__(self):
        self.init_context()

    def __getitem__(self, key):
        return self.eval_key(key)

def make_stream():
    import StringIO
    return StringIO.StringIO()

def timedelta2str(tdelta):
    days = tdelta.days
    res = ""
    if days:
        if days in [1, -1]:
            res = "%s day " % days
        else:
            res = "% days " % days
            
    d = {}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return res + "%(hours)02d:%(minutes)02d:%(seconds)02d" % d

