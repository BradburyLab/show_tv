#!/usr/bin/env python3
# coding: utf-8

if __name__ == "__main__":
    from api import connect
        
    from tornado import gen
    from tornado.ioloop import IOLoop
    io_loop = IOLoop.instance()
    
    @gen.engine
    def process(callback=None):
        host, port = "localhost", 8000
        stream = yield gen.Task(connect, host, port)
        
        if stream:
            stream.close()
        
        if callback:
            callback(None)
        io_loop.stop()

    process()    
    io_loop.start()
    