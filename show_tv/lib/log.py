# -*- coding: utf-8 -*-
#!Python33
import time

from tornado.log import LogFormatter
from tornado.escape import _unicode
from tornado.util import basestring_type

FMT_PREFIX = '[%(asctime)s] [%(levelname)s]'

fmt_preffix = FMT_PREFIX

def update_fmt_prefix(new_prefix):
    global fmt_preffix
    fmt_preffix = new_prefix

class Formatter(LogFormatter):
    def __init__(
        self,
        fmt_date='%d/%b/%Y %H:%M:%S',
        *a, **k
    ):
        self.fmt_date = fmt_date
        super().__init__(*a, **k)

    def format(self, record):
        try:
            record.message = record.getMessage()
        except Exception as e:
            record.message = "Bad message (%r): %r" % (e, record.__dict__)
        assert isinstance(record.message, basestring_type)  # guaranteed by logging

        record.asctime = time.strftime(
            self.fmt_date, self.converter(record.created))
        prefix = fmt_preffix % record.__dict__

        if self._color:
            prefix = (self._colors.get(record.levelno, self._normal) +
                      prefix + self._normal)

        def safe_unicode(s):
            try:
                return _unicode(s)
            except UnicodeDecodeError:
                return repr(s)

        formatted = prefix + " " + safe_unicode(record.message)
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            lines = [formatted.rstrip()]
            lines.extend(safe_unicode(ln) for ln in record.exc_text.split('\n'))
            formatted = '\n'.join(lines)
        return formatted.replace("\n", "\n    ")
