# coding: utf-8
import struct

from io import BytesIO

from tornado import gen

from .dvr_base import DVRBase


class DVRReader(DVRBase):
    commands = {
        'load': 0x02,
        'range': 0x04,
    }

    def __init__(self, cfg, host='127.0.0.1', port=7451):
        super().__init__(cfg, host, port)

    @gen.engine
    def range(self, asset, bitrate, startstamp, duration, callback):
        '''
        '''
        yield gen.Task(self.reconnect)
        # if not hasattr(self, 'c'):
        #     yield gen.Task(self.reconnect)

        # if self.c.closed():
        #     yield gen.Task(self.reconnect)

        if self.c.closed():
            self.l.debug('[DVRReader] failed to connect')
            return

        self.l.debug('[DVRReader] range start >>>>>>>>>>>>>>>')

        if isinstance(asset, str):
            asset = asset.encode()
        if isinstance(startstamp, str):
            startstamp = int(startstamp)
        if isinstance(duration, str):
            duration = int(duration)
        endstamp = startstamp + duration

        self.l.debug('[DVRReader] => asset = {0}'.format(asset))
        self.l.debug('[DVRReader] => bitrate = {0}'.format(bitrate))
        self.l.debug('[DVRReader] => start = {0}'.format(startstamp))
        self.l.debug('[DVRReader] => end = {0}'.format(endstamp))

        pack = struct.pack(
            "=B32sLQQ",
            # (1) (B) Команда
            self.commands['range'],
            # (2) (32s) Имя ассета
            asset,
            # (3) (L) Битрейт
            bitrate,
            # (4) (Q) Время начала
            startstamp,
            # (5) (Q) Время окончания
            endstamp,
        )

        yield gen.Task(self.c.write, pack)
        data = yield gen.Task(self.c.read_bytes, 8, streaming_callback=None)
        length = struct.unpack('=Q', data)[0]
        self.l.debug('[DVRReader]')
        self.l.debug('[DVRReader] <= length = {0}'.format(length))

        chunks_data = yield gen.Task(self.c.read_bytes, length, streaming_callback=None)
        self.l.debug('[DVRReader] <= chunks_data_len = {0}'.format(len(chunks_data)))

        io = BytesIO(chunks_data)
        playlist = []
        while True:
            chunk_data = io.read(16)
            if len(chunk_data) != 16:
                break

            self.l.debug('[DVRReader]')

            (
                startstamp,
                duration,
            ) = struct.unpack('=QQ', chunk_data)
            self.l.debug('[DVRReader] <= startstamp = {0}'.format(startstamp))
            self.l.debug('[DVRReader] <= duration = {0}'.format(duration))

            playlist.append({
                'startstamp': startstamp,
                'duration': duration,
            })

        self.l.debug('[DVRReader] range finish <<<<<<<<<<<<<<<\n')

        callback(playlist)

    @gen.engine
    def load(self, asset, bitrate, startstamp, callback):
        '''
        '''
        yield gen.Task(self.reconnect)
        # if not hasattr(self, 'c'):
        #     yield gen.Task(self.reconnect)

        # if self.c.closed():
        #     yield gen.Task(self.reconnect)

        if self.c.closed():
            self.l.debug('[DVRReader] failed to connect')
            return

        self.l.debug('[DVRReader] load start >>>>>>>>>>>>>>>')

        if isinstance(asset, str):
            asset = asset.encode()
        if isinstance(startstamp, str):
            startstamp = int(startstamp)

        self.l.debug('[DVRReader] => asset = {0}'.format(asset))
        self.l.debug('[DVRReader] => bitrate = {0}'.format(bitrate))
        self.l.debug('[DVRReader] => startstamp = {0}'.format(startstamp))

        pack = struct.pack(
            "=B32sLQ",
            # (1) (B) Команда
            self.commands['load'],
            # (2) (32s) Имя ассета
            asset,
            # (3) (L) Битрейт
            bitrate,
            # (4) (Q) Время начала
            startstamp,
        )

        yield gen.Task(self.c.write, pack)
        data = yield gen.Task(self.c.read_bytes, 8, streaming_callback=None)
        length = struct.unpack('=Q', data)[0]
        self.l.debug('[DVRReader]')
        self.l.debug('[DVRReader] <= length = {0}'.format(length))

        payload = yield gen.Task(self.c.read_bytes, length, streaming_callback=None)
        self.l.debug('[DVRReader] <= payloadlen = {0}'.format(len(payload)))

        self.l.debug('[DVRReader] load finish <<<<<<<<<<<<<<<\n')

        callback(payload)
