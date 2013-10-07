#!/usr/bin/env python3
# coding: utf-8

import os, o_p
import argparse
import logging

# в модуле argparse уже есть "rock solid"-реализация
# структуры, поэтому используем ее
USE_NAMESPACE = True
if USE_NAMESPACE:
    def make_struct(**kwargs):
        return argparse.Namespace(**kwargs)
else:
    # объект самого класса object минималистичен, поэтому не содержит
    # __dict__ (который и дает функционал атрибутов); а вот наследники
    # получают __dict__ по умолчанию, если только в их описании нет __slots__ - 
    # явного списка атрибутов, которые должен иметь класс
    class Struct(object):
        pass

    def make_struct(**kwargs):
        """ Сделать объект с атрибутами """
        # вообще, для спец. случаев, требующих оптимизации по памяти, можно
        # установить __slots__ равным kwargs.keys()
        stct = Struct()
        stct.__dict__.update(kwargs)
        return stct

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
    for logger_name, level in (
        ('stream', logging.DEBUG),
        ('DVRReader', logging.DEBUG),
        ('DVRWriter', logging.DEBUG),
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

# Устанавливаем логи
setup_logging()
