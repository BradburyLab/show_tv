# coding: utf-8
import os
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
            self.l.debug('[DVRWriter] failed to connect')
            return

        self.l.debug('[DVRWriter] write start >>>>>>>>>>>>>>>')

        if isinstance(name, str):
            name = name.encode()
        with open(path_payload, 'rb') as f:
            payload = f.read()
        start = int((start_utc.timestamp() + start_seconds)*1000)
        duration = int(duration*1000)
        metalen = len(metadata)
        payloadlen = os.stat(path_payload).st_size

        self.l.debug('[DVRWriter] => name = {0}'.format(name))
        self.l.debug('[DVRWriter] => bitrate = {0}'.format(bitrate))
        self.l.debug('[DVRWriter] => start = {0}'.format(start))
        self.l.debug('[DVRWriter] => duration = {0}'.format(duration))
        self.l.debug('[DVRWriter] => is_pvr = {0}'.format(is_pvr))
        self.l.debug('[DVRWriter] => metalen = {0}'.format(metalen))
        self.l.debug('[DVRWriter] => payloadlen = {0}'.format(payloadlen))
        self.l.debug('[DVRWriter] => metadata = {0}'.format(metadata))
        self.l.debug('[DVRWriter] => path_payload = {0}'.format(path_payload))

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
            metalen,
            # (7) (L) Длина payload
            payloadlen,
        )

        yield gen.Task(
            self.c.write,
            b''.join([
                pack,
                metadata,
                payload,
            ])
        )
        self.l.debug('[DVRWriter] write finish <<<<<<<<<<<<<<<\n')
