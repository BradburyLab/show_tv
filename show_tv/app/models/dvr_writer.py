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

def dur2millisec(duration):
    return int(duration*1000)

def make_QLBQ(path_payload, start_offset, duration, start_utc, is_pvr):
    start = int((api.utc_dt2ts(start_utc) + start_offset)*1000)
    duration = dur2millisec(duration)
    payloadlen = os.stat(path_payload).st_size
    
    logger.debug('[DVRWriter] => start = {0}'.format(api.bl_int_ts2bl_str(start)))
    logger.debug('[DVRWriter] => duration = {0}'.format(duration))
    logger.debug('[DVRWriter] => is_pvr = {0}'.format(is_pvr))
    logger.debug('[DVRWriter] => payloadlen = {0}'.format(payloadlen))
    logger.debug('[DVRWriter] => path_payload = {0}'.format(path_payload))
    
    return start, duration, is_pvr, payloadlen

def write_chunk(stream, chunk_fpath, payloadlen, prefix):
    stream.write(prefix)
    
    use_sendfile = configuration.use_sendfile
    if use_sendfile:
        sendfile(stream, chunk_fpath, payloadlen)
    else:
        with open(chunk_fpath, 'rb') as f:
            stream.write(f.read())
    
    if stream.closed():
        logger.error("Write to DVR failed")

def log_queue(q_len):
    if q_len > 200:
        logger.info("Write queue is too big, %s", q_len)

class DVRWriter(DVRBase):
    def __init__(self, cfg, host='127.0.0.1', port=6451, use_sendfile=False):
        super().__init__(cfg, host, port, use_sendfile)

def write_full_chunk(
    stream,
    chunk_range,
    start_seconds,
    duration, is_pvr,
    chunk_fpath,
):
    '''
    '''
    r_t_p = chunk_range.r_t_p
    start_utc = chunk_range.start
    
    name = api.asset_name(r_t_p)
    profile = r_t_p.profile

    #if not hasattr(self, 'c'):
        #yield gen.Task(self.reconnect)

    #if stream.closed():
        #yield gen.Task(self.reconnect)

    #if stream.closed():
        #logger.debug('[DVRWriter] failed to connect')
        #return

    name, profile = api.encode_strings(name, profile)

    logger.debug('[DVRWriter] => name = {0}'.format(name))
    logger.debug('[DVRWriter] => profile = {0}'.format(profile))
    start, duration, is_pvr, payloadlen = make_QLBQ(chunk_fpath, start_seconds, duration, start_utc, is_pvr)

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
        #gen.Task(stream.write, pack),
        #gen.Task(stream.write, metadata),
    #]
    write_chunk(stream, chunk_fpath, payloadlen, pack)

    queue = stream.ws_buffer if use_sendfile else stream._write_buffer
    log_queue(len(queue))

    # fd = os.open(chunk_fpath, os.O_RDONLY)

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

def write_to_dvr(dvr_writer, chunk_fpath, start_offset, duration, chunk_range):
    start_utc = chunk_range.start

    if configuration.get_cfg_value("local_dvr", False):
        (name, typ), profile = chunk_range.r_t_p
        dvr_dir = os.path.join(configuration.db_path, "local_dvr", "%s=%s=%s" % (name, api.DVR_SUFFEXES[typ], profile))
        import o_p
        o_p.force_makedirs(dvr_dir)
        
        import datetime
        off = dur2millisec(start_offset)
        fname = "%s=%s=%s.dvr" % (api.ts2bl_str(start_utc + datetime.timedelta(milliseconds=off)), off, dur2millisec(duration))
        import shutil
        shutil.copyfile(chunk_fpath, os.path.join(dvr_dir, fname))
    else:
        if write_dvr_per_profile:
            # расчет суммы размеров очередей по всем сокетам
            if not "queue_size" in dir(dvr_writer):
                dvr_writer.queue_size = 0
            
            obj = chunk_range
            def write_func(stream, is_first):
                if is_first:
                    use_cmd = api.pack_rtp_cmd(WriteCmd.USE, chunk_range.r_t_p, '')
                    stream.write(use_cmd)
                    
                    def on_queue_change(change):
                        dvr_writer.queue_size += change
                    stream.on_queue_change = on_queue_change
                    
                qlbq = make_QLBQ(chunk_fpath, start_offset, duration, start_utc, True)
                pack = api.pack_cmd(
                    "QLBQ",
                    WriteCmd.DATA,
                    *qlbq
                )
                
                write_chunk(stream, chunk_fpath, qlbq[-1], pack)
                log_queue(dvr_writer.queue_size)
        else:
            obj = dvr_writer
            def write_func(stream, is_first):
                write_full_chunk(
                    stream,
                    chunk_range,
                    start_seconds=start_offset,
                    duration=duration,
                    is_pvr=True,
                    chunk_fpath=chunk_fpath,
                )
                    
        api.connect_to_dvr(obj, (dvr_writer.host, dvr_writer.port), write_func)
