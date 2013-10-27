#!/usr/bin/env python3
# coding: utf-8

import os, o_p
import argparse
import logging
import api

make_struct = api.make_struct

cur_directory = os.path.dirname(__file__)
log_directory = os.path.join(cur_directory, '../log')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-e', '--environment',
        dest='environment', type=str, default='development',
        help='app environment',
    )
    
    env_name = parser.parse_args().environment
    #print(env_name)
    fpath = o_p.join(cur_directory, "config", env_name + ".py")
    with open(fpath) as f:
        txt = f.read()
    res = {"env_name": env_name}
    exec(txt, {}, res)
    return make_struct(**res)

from lib.log import Formatter

def setup_file_logger(logger_name, level, logger):
    formatter = Formatter(color=False)
    f = logging.FileHandler(
        os.path.join(
            log_directory, '{0}.{1}.log'.format(get_env_value("name", environment.env_name), logger_name)
        ),
        mode='w'
    )
    f.setLevel(level)
    f.setFormatter(formatter)
    logger.addHandler(f)

def setup_logging():
    # <logging.tornado> -----
    for stream in (
        'tornado.access',
        'tornado.application',
        # 'tornado.general',
    ):
        logger = logging.getLogger(stream)
        tornado_lvl = logging.INFO if get_env_value("verbose_tornado", False) else logging.WARNING
        logger.setLevel(tornado_lvl)

        setup_file_logger(stream, tornado_lvl, logger)
    # ----- </logging.tornado>
    # <logging.application> -----
    level = logging.DEBUG if get_env_value("debug_logging", is_test) else logging.INFO
    for logger_name in (
        'stream',
        'DVRReader',
        'DVRWriter',
    ):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.propagate = False
        logger.handlers = []

        formatter = Formatter(color=True)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        setup_file_logger(logger_name, level, logger)
    # ----- </logging.application>

# :TRICKY: окружение нужно в самом начале, поэтому -
environment = parse_args()

def get_env_value(key, def_value=None):
    return getattr(environment, key, def_value)

cast_one_source = get_env_value("cast_one_source", None)
is_test = not cast_one_source and environment.is_test

# Устанавливаем логи
setup_logging()


