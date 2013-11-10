#!/usr/bin/env python3
# coding: utf-8

if __name__ == '__main__':
    # не нужен, такие дела
    #import multiprocessing

    import os
    from tornado.ioloop import IOLoop
    
    # fork можно делать только до создания ioloop'а
    # (вообще говоря любого), см. tornado.process.fork_processes()
    assert not IOLoop.initialized()

    #r, w = os.pipe()
    from tornado.process import _pipe_cloexec, PipeIOStream
    r, w = _pipe_cloexec()
    
    pid = os.fork()
    is_child = pid == 0
    
    fd       = r if is_child else w
    to_close = w if is_child else r
    os.close(to_close)

    io_loop = IOLoop.instance()
    stream  = PipeIOStream(fd)

    from dumb_tcp_server import try_read_bytes

    if is_child:
        @gen.engine
        def handle_message():
            while True:
                # :REFACTOR:
                is_ok, data = yield gen.Task(try_read_bytes, stream, PREFIX_SZ)
                if not is_ok:
                    # :TRICKY: закрытый сокет без данных - ок
                    if data:
                        write_error("not full prefix: %s" % data)
                break
            
            io_loop.stop()
        handle_message()
        io_loop.start()
    else:
        pass

    #class MainHandler(tornado.web.RequestHandler):
        #def get(self):
            #self.write("Greetings from the instance %s!" % tornado.process.task_id())

    #app = tornado.web.Application([
        #(r"/", MainHandler),
    #])

    #server = tornado.httpserver.HTTPServer(app)
    #server.bind(8888)
    #server.start(0)  # autodetect number of cores and fork a process for each
    #tornado.ioloop.IOLoop.instance().start()
