# coding: utf-8
import os
import math
import socket
import logging
import tornado.iostream

from tornado import gen
from tornado import template


class DVRBase(object):
    def __init__(self, host='127.0.0.1', port=6451, use_sendfile=False):
        self.host = host
        self.port = port
        self.use_sendfile = use_sendfile
        self.n = self.__class__.__name__
        self.l = logging.getLogger(self.n)
        self.l.info('[{n}] initialized on {host}:{port}'.format(**self.__dict__))

    @gen.engine
    def reconnect(self, callback):
        self.l.debug('[{n}] reconnect start'.format(**self.__dict__))
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
        self.l.debug('[{n}] reconnect finish\n'.format(**self.__dict__))
        callback(None)

    def generate_playlist(self, host, asset, startstamps_durations, bitrate):
        loader = template.Loader(os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            '..',
            'templates',
        ))
        return loader.load('dvr/playlist.m3u8').generate(
            host=host,
            port=8910,
            asset=asset,
            targetduration=math.ceil(max([r[1] for r in startstamps_durations])),
            startstamps_durations=startstamps_durations,
            bitrate=bitrate,
        )
