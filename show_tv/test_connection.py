#!/usr/bin/env python3
# coding: utf-8

import logging
import api

# :REFACTOR:
import datetime
def set_timer(seconds, func):
    period = datetime.timedelta(seconds=seconds)
    
    def on_timeout():
        func()
        io_loop.add_timeout(period, on_timeout)
    io_loop.add_timeout(period, on_timeout)

if __name__ == "__main__":
    from api import connect_to_dvr, StreamState
    
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
            def write_func(stream, is_first):
                stream.write(b"Hello\n")
                print("written!")
                
            obj = api.make_struct()
            def on_timeout():
                connect_to_dvr(obj, (host, port), write_func)
            
            set_timer(1, on_timeout)
        
        io_loop.start()
        