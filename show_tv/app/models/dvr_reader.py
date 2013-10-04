# coding: utf-8
import struct

from tornado import gen

from .dvr_base import DVRBase


class DVRReader(DVRBase):
    commands = {
        'load': 0x02,
        'range': 0x04,
    }

    def __init__(self, host='127.0.0.1', port=7451):
        super().__init__(host, port)

    @gen.engine
    def load(self, name, bitrate, start, callback):
        '''
        '''
        if not hasattr(self, 'c'):
            yield gen.Task(self.reconnect)

        if self.c.closed():
            yield gen.Task(self.reconnect)

        if self.c.closed():
            print('[DVRReader] failed to connect')
            return

        print('[DVRReader] load start >>>>>>>>>>>>>>>')

        if isinstance(name, str):
            name = name.encode()
        if isinstance(start, str):
            start = int(start)

        print('[DVRReader] => name = {0}'.format(name))
        print('[DVRReader] => bitrate = {0}'.format(bitrate))
        print('[DVRReader] => start = {0}'.format(start))

        pack = struct.pack(
            "=B32sLQ",
            # (1) (B) Команда
            self.commands['load'],
            # (2) (32s) Имя ассета
            name,
            # (3) (L) Битрейт
            bitrate,
            # (4) (Q) Время начала
            start,
        )

        yield gen.Task(self.c.write, pack)
        data = yield gen.Task(self.c.read_bytes, 8, streaming_callback=None)
        length = struct.unpack('=Q', data)[0]
        print('[DVRReader] <= length = {0}'.format(length))

        print('[DVRReader] load finish <<<<<<<<<<<<<<<\n')

        callback(None)

    @gen.engine
    def range(self, start, stop, callback):
        '''
        '''
        pass
