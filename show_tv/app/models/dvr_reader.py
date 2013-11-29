# coding: utf-8

from .dvr_base import DVRBase
import api

import struct
from io import BytesIO
from tornado import gen

@gen.engine
def call_dvr_cmd(dvr_reader, func, *args, callback, **kwargs):
    stream = yield gen.Task(api.connect, dvr_reader.host, dvr_reader.port)
    if stream:
        def on_result(data):
            callback((True, data))
            stream.close()
        func(*args, stream=stream, callback=on_result, **kwargs)
    else:
        dvr_reader.l.debug('[DVRReader] failed to connect')
        callback((False, None))

def pack_read_cmd(cmd, r_t_p, startstamp, tail_fmt, *tail_args):
    return api.pack_rtp_cmd(cmd, r_t_p, "Q" + tail_fmt,
        # (4) (Q) Время начала
        startstamp,
        *tail_args
    )    

class DVRReader(DVRBase):
    commands = {
        'load': 0x02,
        'range': 0x04,
    }

    def __init__(self, cfg, host='127.0.0.1', port=7451):
        super().__init__(cfg, host, port)

    @gen.engine
    def request_range(self, r_t_p, startstamp, duration, stream, callback):
        '''
        '''
        self.l.debug('[DVRReader] range start >>>>>>>>>>>>>>>')

        if isinstance(startstamp, str):
            startstamp = int(startstamp)
        if isinstance(duration, str):
            duration = int(duration)
        endstamp = startstamp + duration

        self.l.debug('[DVRReader] => asset = {0}'.format(r_t_p))
        self.l.debug('[DVRReader] => start = {0}'.format(startstamp))
        self.l.debug('[DVRReader] => end = {0}'.format(endstamp))

        pack = pack_read_cmd(self.commands['range'], r_t_p, startstamp, "Q",
            # (5) (Q) Время окончания
            endstamp,
        )

        yield gen.Task(stream.write, pack)
        data = yield gen.Task(stream.read_bytes, 8, streaming_callback=None)
        length = struct.unpack('=Q', data)[0]
        self.l.debug('[DVRReader]')
        self.l.debug('[DVRReader] <= length = {0}'.format(length))

        chunks_data = yield gen.Task(stream.read_bytes, length, streaming_callback=None)
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
    def load(self, r_t_p, startstamp, stream, callback):
        '''
        '''
        self.l.debug('[DVRReader] load start >>>>>>>>>>>>>>>')

        if isinstance(startstamp, str):
            startstamp = int(startstamp)

        self.l.debug('[DVRReader] => asset = {0}'.format(r_t_p))
        self.l.debug('[DVRReader] => startstamp = {0}'.format(startstamp))

        pack = pack_read_cmd(self.commands['load'], r_t_p, startstamp, '')

        yield gen.Task(stream.write, pack)
        data = yield gen.Task(stream.read_bytes, 8, streaming_callback=None)
        length = struct.unpack('=Q', data)[0]
        self.l.debug('[DVRReader]')
        self.l.debug('[DVRReader] <= length = {0}'.format(length))

        payload = yield gen.Task(stream.read_bytes, length, streaming_callback=None)
        self.l.debug('[DVRReader] <= payloadlen = {0}'.format(len(payload)))

        self.l.debug('[DVRReader] load finish <<<<<<<<<<<<<<<\n')

        callback(payload)
