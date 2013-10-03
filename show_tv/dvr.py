# coding: utf-8
import tornado.iostream
import socket
import struct

from tornado import gen


class DVR(object):
    def __init__(self, host='127.0.0.1', port=6451):
        self.host = host
        self.port = port
        print('[DVR] initialized on {0}:{1}'.format(host, port))

    @gen.engine
    def reconnect(self, callback):
        print('[DVR] reconnect start')
        self.c = tornado.iostream.IOStream(
            socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
                0,
            )
        )
        yield gen.Task(
            self.c.connect,
            (
                self.host,
                self.port,
            ),
        )
        print('[DVR] reconnect finish\n')
        callback(None)

    @gen.engine
    def write(
        self,
        name, bitrate,
        start_utc, start_seconds,
        duration, is_pvr,
        path_payload, metadata
    ):
        '''
        '''
        if not hasattr(self, 'c'):
            yield gen.Task(self.reconnect)

        if self.c.closed():
            yield gen.Task(self.reconnect)

        if self.c.closed():
            print('[DVR] failed to connect')
            return

        print('[DVR] write start >>>>>>>>>>>>>>>')

        if (
            isinstance(name, str) or
            isinstance(name, unicode)
        ):
            name = name.encode()

        with open(path_payload, 'rb') as f:
            payload = f.read()
        start = int((start_utc.timestamp() + start_seconds))
        duration = int(duration*1000)
        len_metadata = len(metadata)
        len_payload = len(payload)

        print('[DVR] => name = {0}'.format(name))
        print('[DVR] => bitrate = {0}'.format(bitrate))
        print('[DVR] => start = {0}'.format(start))
        print('[DVR] => duration = {0}'.format(duration))
        print('[DVR] => is_pvr = {0}'.format(is_pvr))
        print('[DVR] => len_metadata = {0}'.format(len_metadata))
        print('[DVR] => len_payload = {0}'.format(len_payload))
        print('[DVR] => metadata = {0}'.format(metadata))
        print('[DVR] => path_payload = {0}'.format(path_payload))

        pack = struct.pack(
            # "=32sLLLBHL",
            "=32sLLBHL",
            # (1) (32s) Имя ассета
            name,
            # (2) (L) Битрейт
            bitrate,
            # (3) (L) Время начала чанка
            start,
            # (4) (L) Длительность чанка в мс (int),
            # duration,
            # (5) (B) Это PVR?
            is_pvr,
            # (6) (H) Длина метаданных
            len_metadata,
            # (7) (q) Длина payload
            len_payload,
        )

        yield gen.Task(
            self.c.write,
            b''.join([
                pack,
                metadata,
                payload,
            ])
        )
        print('[DVR] write finish <<<<<<<<<<<<<<<\n')
