#!/usr/bin/env python3
# coding: utf-8

def make_client(dsn, is_async):
    if is_async:
        from raven.contrib.tornado import AsyncSentryClient
        client = AsyncSentryClient(dsn)
    else:
        from raven import Client
        client = Client(dsn)
    return client

handler = None

def setup(dsn, level, propagate_sentry_errors=False):
    client = make_client(dsn, False)
    
    from raven.handlers.logging import SentryHandler
    global handler
    handler = SentryHandler(client)
    handler.setLevel(level)
    handler.dsn = dsn
    
    from raven.conf import setup_logging
    kwargs = {}
    if propagate_sentry_errors:
        kwargs["exclude"] = []
    setup_logging(handler, **kwargs)
    
def update_to_async_client():
    """ Поменять синхронного клиента на асинхронного - вызывать,
        когда подошли к вызову tornado.ioloop.IOLoop.instance().start() """
    global handler
    if handler:
        handler.client = make_client(handler.dsn, True)

import contextlib

@contextlib.contextmanager
def catched_exceptions():
    """ Правильней вызвать этот функционал сразу после
        setup(dsn) """
    try:
        yield
    except Exception as exc:
        import logging
        logging.error(exc, exc_info=1)
        raise

if __name__ == '__main__':
    import argparse
    # :REFACTOR:
    parser = argparse.ArgumentParser()
    def add_arg(name, help):
        parser.add_argument(name, help=help)
        
    add_arg("dsn", "Sentry DSN")
    dsn = parser.parse_args().dsn
    #print(dsn)

    import logging
    setup(dsn, logging.WARNING)

    with catched_exceptions():
        import logging
        def func():
            #logging.error("Error")
            1/0
        func()
    
        import tornado.ioloop
        import tornado.web
        
        class MainHandler(tornado.web.RequestHandler):
            def get(self):
                self.write("Hello, world")
                1/0
        
        application = tornado.web.Application([
            (r"/", MainHandler),
        ])
        
        application.listen(8888)
        
        update_to_async_client()
        tornado.ioloop.IOLoop.instance().start()
    