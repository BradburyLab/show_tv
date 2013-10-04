# coding: utf-8
import tornado.iostream
import socket
import struct

from tornado import gen

from .dvr_base import DVRBase


class DVRWriter(DVRBase):
    def __init__(self, host='127.0.0.1', port=6451):
        super().__init__(host, port)

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
            print('[DVRWriter] failed to connect')
            return

        print('[DVRWriter] write start >>>>>>>>>>>>>>>')

        if isinstance(name, str):
            name = name.encode()
        with open(path_payload, 'rb') as f:
            payload = f.read()
        start = int((start_utc.timestamp() + start_seconds)*1000)
        duration = int(duration*1000)
        len_metadata = len(metadata)
        len_payload = len(payload)

        print('[DVRWriter] => name = {0}'.format(name))
        print('[DVRWriter] => bitrate = {0}'.format(bitrate))
        print('[DVRWriter] => start = {0}'.format(start))
        print('[DVRWriter] => duration = {0}'.format(duration))
        print('[DVRWriter] => is_pvr = {0}'.format(is_pvr))
        print('[DVRWriter] => len_metadata = {0}'.format(len_metadata))
        print('[DVRWriter] => len_payload = {0}'.format(len_payload))
        print('[DVRWriter] => metadata = {0}'.format(metadata))
        print('[DVRWriter] => path_payload = {0}'.format(path_payload))

        pack = struct.pack(
            "=32sLQLBHL",
            # (1) (32s) Имя ассета
            name,
            # (2) (L) Битрейт
            bitrate,
            # (3) (Q) Время начала чанка
            start,
            # (4) (L) Длительность чанка в мс (int),
            duration,
            # (5) (B) Это PVR?
            is_pvr,
            # (6) (H) Длина метаданных
            len_metadata,
            # (7) (L) Длина payload
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
        print('[DVRWriter] write finish <<<<<<<<<<<<<<<\n')
