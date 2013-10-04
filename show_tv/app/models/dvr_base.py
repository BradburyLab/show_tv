# coding: utf-8
import tornado.iostream
import socket

from tornado import gen


class DVRBase(object):
    def __init__(self, host='127.0.0.1', port=6451):
        self.host = host
        self.port = port
        self.n = self.__class__.__name__
        print('[{n}] initialized on {host}:{port}'.format(**self.__dict__))

    @gen.engine
    def reconnect(self, callback):
        print('[{n}] reconnect start'.format(**self.__dict__))
        self.c = tornado.iostream.IOStream(
            socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
                0,
            )
        )
        yield gen.Task(
            self.c.connect,
            (
                self.host,
                self.port,
            ),
        )
        print('[{n}] reconnect finish\n'.format(**self.__dict__))
        callback(None)
