# coding: utf-8
# import argparse
import socket
import os
import errno

import logging
gen_log = logging.getLogger("tornado.general")

import tornado.iostream

def writing(self):
    return bool(self.ws_buffer)

_merge_prefix = tornado.iostream._merge_prefix

def handle_write(self):
    while self.ws_buffer:
        is_sendfile, dat = self.ws_buffer[0]
        try:
            # :TODO: учитывать нулевое копирование байт, см. IOStream._write_buffer_frozen
            if is_sendfile:
                while dat.sz > 0:
                    num_bytes = os.sendfile(self.fileno(), dat.f.fileno(), dat.off, dat.sz)
                    dat.off += num_bytes
                    dat.sz  -= num_bytes
                    
                dat.f.close()
            else:
                while dat:
                    _merge_prefix(dat, 128 * 1024)
                    num_bytes = self.write_to_fd(dat[0])
                    _merge_prefix(dat, num_bytes)
                    dat.popleft()

            self.ws_buffer.popleft()
            self.on_queue_change(-1)
            
        except socket.error as e:
            # :COPY_N_PASTE:
            if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                # if isinstance(dat, argparse.Namespace):
                #     dat.f.close()
                # self.ws_buffer.popleft()
                break
            else:
                if e.args[0] not in (errno.EPIPE, errno.ECONNRESET):
                    # Broken pipe errors are usually caused by connection
                    # reset, and its better to not log EPIPE errors to
                    # minimize log spam
                    gen_log.warning("Write error on %d: %s",
                                    self.fileno(), e)
                self.close(exc_info=True)
                return
    if not self.ws_buffer and self._write_callback:
        # :COPY_N_PASTE:
        callback = self._write_callback
        self._write_callback = None
        self._run_callback(callback)

from tornado.util import bytes_type
from tornado import stack_context
from collections import deque, namedtuple

def try_write(self, callback):
    self._write_callback = stack_context.wrap(callback)
    if not self._connecting:
        self._handle_write()
        if self.ws_buffer:
            self._add_io_state(self.io_loop.WRITE)
        self._maybe_add_error_listener()

def append_to_q(self, is_sendfile, item):
    self.ws_buffer.append((is_sendfile, item))
    self.on_queue_change(1)

def append_wbuf(self, wbuf):
    append_to_q(self, False, wbuf)
    
def append_new_wbuf(self):
    wbuf = deque()
    append_wbuf(self, wbuf)
    return wbuf

def write_to_stream(self, data, callback=None):
    # :COPY_N_PASTE:
    assert isinstance(data, bytes_type)
    self._check_closed()
    
    # We use bool(_write_buffer) as a proxy for write_buffer_size>0,
    # so never put empty strings in the buffer.
    if data:
        if self.ws_buffer:
            is_sendfile, last_buf = self.ws_buffer[-1]
            if is_sendfile:
                wbuf = append_new_wbuf(self)
            else:
                wbuf = last_buf
        else:
            wbuf = append_new_wbuf(self)
            
        # Break up large contiguous strings before inserting them in the
        # write buffer, so we don't have to recopy the entire thing
        # as we slice off pieces to send to the socket.
        WRITE_BUFFER_CHUNK_SIZE = 128 * 1024
        if len(data) > WRITE_BUFFER_CHUNK_SIZE:
            for i in range(0, len(data), WRITE_BUFFER_CHUNK_SIZE):
                wbuf.append(data[i:i + WRITE_BUFFER_CHUNK_SIZE])
        else:
            wbuf.append(data)
            
    try_write(self, callback)

def handle_close_fd(self):
    ws_buffer = self.ws_buffer
    
    for is_sendfile, dat in ws_buffer:
        if is_sendfile:
            dat.f.close()
            
    ln = len(ws_buffer)
    ws_buffer.clear()
    self.on_queue_change(-ln)

import api

def sendfile(self, fpath, size, callback=None):
    self._check_closed()
    
    if not "ws_buffer" in dir(self):
        self.ws_buffer = deque()

        if not "on_queue_change" in dir(self):
            def on_queue_change(change):
                pass
            self.on_queue_change = on_queue_change
        
        if self._write_buffer:
            append_wbuf(self, self._write_buffer)
            self._write_buffer = None # не используем
        
        def replace_meth(name, sf_meth):
            #old = getattr(self, name)
            def wrapper(*args, **kw):
                #if self.is_sendfile_on:
                    #res = sf_meth(self, *args, **kw)
                #else:
                    #res = old(*args, **kw)
                #return res
                return sf_meth(self, *args, **kw)
            setattr(self, name, wrapper)
            
        
        replace_meth("_handle_write", handle_write)
        replace_meth("writing", writing)
        replace_meth("write", write_to_stream)
        
        orig_close_fd = self.close_fd
        def close_fd(self):
            handle_close_fd(self)
            orig_close_fd()
        replace_meth("close_fd", close_fd)

    # :TRICKY: сразу открываем файл, потому что так проще

    # :TRICKY: жестокая реальность говорит нам, что файлы
    # могут не открываться, "Too many open files" => тогда
    # ничего не остается, кроме как закрыть поток, иначе будем
    # писать ерунду
    is_ok = True
    try:
        f = open(fpath, "rb")
    except Exception:
        is_ok = False
        
    if is_ok:
        # :TRICKY: нельзя менять значения tuple'а
        #SFClass = collections.namedtuple('SFClass', ['f', 'off', 'sz'])
        #sf = SFClass(open(fpath, "rb"), 0, size)
        sf = api.make_struct(
            f   = f, 
            off = 0, 
            sz  = size,
        )
        append_to_q(self, True, sf)
    
        try_write(self, callback)
    else:
        self.close()
        if callback:
            callback()
        
    # возвращать всегда нужно None, ведь ответ получают через callback

