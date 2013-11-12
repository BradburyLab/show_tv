#!/usr/bin/env python3
# coding: utf-8

# не нужен, такие дела
#import multiprocessing

from dumb_tcp_server import read_messages
import api

import os
from tornado.ioloop import IOLoop
import struct

def fork_slaves(slave_cnt):
    from tornado.process import _pipe_cloexec, PipeIOStream
    
    is_child, lst = False, []
    for i in range(slave_cnt):
        #r, w = os.pipe()
        r, w = _pipe_cloexec()
        
        # fork можно делать только до создания ioloop'а
        # (вообще говоря любого), см. tornado.process.fork_processes()
        assert not IOLoop.initialized()
        
        pid = os.fork()
        is_child = pid == 0
        
        fd       = r if is_child else w
        to_close = w if is_child else r
        os.close(to_close)
    
        if is_child:
            res = True, (i, PipeIOStream(fd))
            # :KLUDGE: а без лишних движений как?
            for w_fd in lst:
                os.close(w_fd)
            break
        else:
            lst.append(fd)
        
    if not is_child:
        res = False, [PipeIOStream(fd) for fd in lst]
    return res

mp_format = api.make_prefix_format()

def setup_msg_handler(stream, on_message, callback):
    stop = IOLoop.instance().stop
    def on_stop():
        callback()
        stop()
    read_messages(stream, mp_format, on_message, on_stop)

def send_message(stream, data):
    msg = struct.pack(mp_format, api.DVR_MAGIC_NUMBER, len(data)) + data
    stream.write(msg)

if __name__ == '__main__':
    
    is_child, data = fork_slaves(20)
    
    io_loop = IOLoop.instance()

    if is_child:
        assert len(data) == 2
        idx, stream = data
        
        def child_print(*args):
            print(idx, ":", *args)
            
        def on_message(tpl, data):
            child_print(tpl, data)
            
        def on_stop():
            child_print("Exit")
            
        setup_msg_handler(stream, on_message, on_stop)
        
        io_loop.start()
    else:
        from test_sendfile import one_second_pause

        def do_on_write_completed(stream, callback):
            stream.write(b'', callback)
            
        #assert len(data) == 1
        print("Master:", len(data))
        for stream in data:
            for i in range(3):
                send_message(stream, b'ggg')
                
            #do_on_write_completed(stream, stream.close)
            stream.close()
        one_second_pause()

        io_loop.start()
