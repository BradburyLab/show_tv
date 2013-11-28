# coding: utf-8
import os
# import pyaio
import struct
import logging

from tornado import gen

from .dvr_base import DVRBase

import configuration
import api
from sendfile import sendfile

insert_dvr_magic_number = configuration.get_cfg_value("insert_dvr_magic_number", False)

def pack_prefix(*args):
    if insert_dvr_magic_number:
        args = (api.DVR_MAGIC_NUMBER,) + args
    return struct.pack(api.make_dvr_prefix_format(insert_dvr_magic_number), *args)

logger = logging.getLogger("DVRWriter")

def make_QLBQ(path_payload, start_offset, duration, start_utc, is_pvr):
    start = int((api.utc_dt2ts(start_utc) + start_offset)*1000)
    duration = int(duration*1000)
    payloadlen = os.stat(path_payload).st_size
    
    logger.debug('[DVRWriter] => start = {0}'.format(api.bl_int_ts2bl_str(start)))
    logger.debug('[DVRWriter] => duration = {0}'.format(duration))
    logger.debug('[DVRWriter] => is_pvr = {0}'.format(is_pvr))
    logger.debug('[DVRWriter] => payloadlen = {0}'.format(payloadlen))
    logger.debug('[DVRWriter] => path_payload = {0}'.format(path_payload))
    
    return start, duration, is_pvr, payloadlen

def write_chunk(stream, path_payload, payloadlen, prefix):
    stream.write(prefix)
    
    if configuration.use_sendfile:
        sendfile(stream, path_payload, payloadlen)
    else:
        with open(path_payload, 'rb') as f:
            stream.write(f.read())
        
    if stream.closed():
        logger.error("Write to DVR failed")
    else:
        queue = stream.ws_buffer if self.use_sendfile else stream._write_buffer
        q_len = len(queue)
        if q_len > 200:
            logger.info("Write queue is too big, %s", q_len)

class DVRWriter(DVRBase):
    def __init__(self, cfg, host='127.0.0.1', port=6451, use_sendfile=False):
        super().__init__(cfg, host, port, use_sendfile)

    @gen.engine
    def write(
        self,
        r_t_p,
        start_utc, start_seconds,
        duration, is_pvr,
        path_payload,
    ):
        '''
        '''
        name = api.asset_name(r_t_p)
        profile = r_t_p.profile

        if not hasattr(self, 'c'):
            yield gen.Task(self.reconnect)

        if self.c.closed():
            yield gen.Task(self.reconnect)

        if self.c.closed():
            logger.debug('[DVRWriter] failed to connect')
            return

        logger.debug('[DVRWriter] write start >>>>>>>>>>>>>>>')

        if isinstance(name, str):
            name = name.encode()

        logger.debug('[DVRWriter] => name = {0}'.format(name))
        logger.debug('[DVRWriter] => profile = {0}'.format(profile))
        start, duration, is_pvr, payloadlen = make_QLBQ(path_payload, start_seconds, duration, start_utc, is_pvr)

        pack = pack_prefix(
            # (1) (32s) Имя ассета
            name,
            # (2) (L) Битрейт
            profile,
            # (3) (Q) Время начала чанка
            start,
            # (4) (L) Длительность чанка в мс (int),
            duration,
            # (5) (B) Это PVR?
            is_pvr,
            # (6) (L) Длина payload
            payloadlen,
        )

        #yield [
            #gen.Task(self.c.write, pack),
            #gen.Task(self.c.write, metadata),
        #]
        write_chunk(self.c, path_payload, payloadlen, pack)

        logger.debug('[DVRWriter] write finish <<<<<<<<<<<<<<<\n')

        # fd = os.open(path_payload, os.O_RDONLY)

        # @gen.engine
        # def on_read(buf, rcode, errno):
        #     os.close(fd)
        #     if rcode > 0:
        #         yield gen.Task(
        #             stream.write,
        #             b''.join([
        #                 pack,
        #                 metadata,
        #                 buf,
        #             ])
        #         )
        #     elif rcode == 0:
        #         print("EOF")
        #     else:
        #         print("Error: %d" % errno)
        #     logger.debug('[DVRWriter] write finish <<<<<<<<<<<<<<<\n')

        # pyaio.aio_read(fd, 0, payloadlen, on_read)

write_dvr_per_profile = configuration.get_cfg_value('write_dvr_per_profile', True)

class WriteCmd:
    USE  = 1
    DATA = 2

class WriteType:
    META = 0
    HLS  = 1
    HDS  = 2
    HLS_ENCRYPTED = 3

def pack_cmd(fmt, cmd, *args):
    return struct.pack("<B" + fmt, cmd, *args)

def write_to_dvr(dvr_writer, path_payload, start_offset, duration, chunk_range):
    start_utc = chunk_range.start
    
    if write_dvr_per_profile:
        def write_func(stream, is_first):
            (refname, typ), profile = chunk_range.r_t_p
            
            if is_first:
                use_cmd = pack_cmd(
                    "B32s6s",
                    WriteCmd.USE,
                    WriteType.HLS if typ == api.StreamType.HLS else WriteType.HDS,
                    refname.encode(),
                    profile.encode()
                )
                stream.write(use_cmd)
                
            qlbq = make_QLBQ(path_payload, start_offset, duration, start_utc, True)
            pack = pack_cmd(
                "QLBQ",
                WriteCmd.DATA,
                *qlbq
            )
            
            write_chunk(stream, path_payload, qlbq[-1], pack)
            
        api.connect_to_dvr(chunk_range, (dvr_writer.host, dvr_writer.port), write_func)
    else:
        dvr_writer.write(
            r_t_p=chunk_range.r_t_p,
            start_utc=chunk_range.start,
            start_seconds=start_offset,
            duration=duration,
            is_pvr=True,
            path_payload=chunk_fpath,
        )
