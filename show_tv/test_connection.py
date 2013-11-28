#!/usr/bin/env python3
# coding: utf-8

import logging
import api

class StreamState:
    """ tornado.iostream.IOStream дает информацию
        о соединении в плохом виде, поэтому сами держим состояние """
    CLOSED     = 0
    CONNECTING = 1
    OPENED     = 2

logger = logging.getLogger()

def write_to_dvr(obj, addr, write_func):
    dvr_writer = getattr(obj, "dvr_writer", None)
    if not dvr_writer:
        obj.dvr_writer = dvr_writer = api.make_struct(state=StreamState.CLOSED)
        
    def start_connection():
        dvr_writer.state = StreamState.CONNECTING
        
        host, port = addr
        def on_connection(stream):
            if stream:
                dvr_writer.state = StreamState.OPENED
                dvr_writer.stream = stream
            else:
                dvr_writer.state = StreamState.CLOSED
                logging.error("Can't connect to DVR server %s:%s", host, port)
                
        connect(host, port, on_connection)
        
    if dvr_writer.state == StreamState.CLOSED:
        start_connection()
    elif dvr_writer.state == StreamState.OPENED:
        def check_stream():
            is_closed = dvr_writer.stream.closed()
            if is_closed:
                dvr_writer.stream = None
                dvr_writer.state = StreamState.CLOSED
            return not is_closed
            
        if check_stream():
            write_func(dvr_writer.stream)
            check_stream()
        else:
            start_connection()

# :REFACTOR:
import datetime
def set_timer(seconds, func):
    period = datetime.timedelta(seconds=seconds)
    
    def on_timeout():
        func()
        io_loop.add_timeout(period, on_timeout)
    io_loop.add_timeout(period, on_timeout)

if __name__ == "__main__":
    from api import connect
    
    from tornado import gen
    from tornado.ioloop import IOLoop
    io_loop = IOLoop.instance()
        
    host, port = "localhost", 8000
    if False:
        
        @gen.engine
        def process(callback=None):
            stream = yield gen.Task(connect, host, port)
            
            if stream:
                stream.close()
            
            if callback:
                callback(None)
            io_loop.stop()
    
        process()    
        io_loop.start()

    if True:
        test_timer = False
        if test_timer:
            def on_test_timer():
                print("!")
            set_timer(1, on_test_timer)
        else:
            def write_func(stream):
                stream.write(b"Hello\n")
                print("written!")
                
            obj = api.make_struct()
            def on_timeout():
                write_to_dvr(obj, (host, port), write_func)
            
            set_timer(1, on_timeout)
        
        io_loop.start()
        