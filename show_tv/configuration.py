#!/usr/bin/env python3
# coding: utf-8
# Import Python libs
import os
import argparse
import logging

# Import vendor libs
import yaml

# Import f451 libs
# import o_p
import api


# в Bradbury обычно 3 секунду GOP, а фрагмент:
# - HLS: 6 секунд (раньше было 9)
# - HDS: 6 секунд
DEFAULT_CHUNK_DURATION = 6

make_struct = api.make_struct

cur_directory = os.path.dirname(__file__)
log_directory = os.path.join(cur_directory, '../log')

def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '-e', '--environment',
    #     dest='environment', type=str, default='development',
    #     help='app environment',
    # )
    parser.add_argument(
        '-c', '--config',
        dest='config', type=str, default='/etc/tara/451',
        help='path to configuration files',
    )
    parser.add_argument(
        '-l', '--log',
        dest='log', type=str, default=None,
        help='path to log folder',
    )
    parser.add_argument(
        '-v', '--version',
        action="store_true",
        help='show version number',
    )
    parser.add_argument(
        '-d', '--duration',
        dest='chunk_duration',
        type=int,
        default=None,
        help="chunk duration"
    )

    # env_name = parser.parse_args().environment
    # print(env_name)
    # fpath = o_p.join(cur_directory, "config", env_name + ".py")
    # with open(fpath) as f:
    #     txt = f.read()
    # res = {"env_name": env_name}
    # exec(txt, {}, res)
    # return make_struct(**res)
    return parser.parse_args()

def log_name2path(logger_name):
    return os.path.join(
        cfg['path_log'],
        '{0}.{1}.log'.format(
            cfg['live']['environment'],
            logger_name,
        )
    )

def setup_logger(logger, fname, logging_level):
    api.setup_logger(logger, log_name2path(fname), logging_level)

# уровень логирования в errors и Sentry
root_level = logging.WARNING

def setup_custom_logger(name, logging_level, propagate, is_append=False):
    logger = logging.getLogger(name)
    logger.propagate = propagate

    #setup_logger(logger, name, logging_level)
    logger.setLevel(logging_level)
    # :TRICKY: с помощью logging невозможно настроить 1 StreamHandler так,
    # чтобы для некоторых логгеров задействовались свои уровни, а в общем случае
    # срабатывали >= WARNING (и без использования propagate = False) => 
    # поэтому, чтобы не было дублирования в stderr ставим фильтр на каждый
    # custom-StreamHandler
    # :KLUGDE: однако это все равно данный хак не спасет от иерархических
    # логгеров ("stream" и "stream.web", например)
    ch = api.setup_console_logger(logger, logging_level)
    if propagate:
        def on_record(record):
            return record.levelno < root_level
        ch.addFilter(on_record)
    
    api.setup_file_handler(logger, log_name2path(name), logging_level, is_append)

def setup_logging():
    path_log = args.log
    if path_log is None:
        path_log = get_cfg_value("log-path", '/var/log/451')
    cfg['path_log'] = os.path.expanduser(path_log)
    
    # логи ошибок и предупреждений
    root_logger = logging.getLogger()
    # вначале - в файл
    setup_logger(root_logger, "errors", root_level)
    
    # затем тоже самое - в Sentry
    orig_dsn = "http://6d156edf539242cf994b5bf2af126fae:f48856d38be44797839d7353bb4dbc34@vladimirsky-sentry.bradburylab.tv/2"
    dsn = get_cfg_value("sentry-dsn", None if is_test else orig_dsn)
    if dsn:
        import sentry
        # propagate_sentry_errors=False => хотим видеть ошибки sentry в логе "errors"
        sentry.setup(dsn, root_level, propagate_sentry_errors=False)

    # <logging.application> -----
    ll_def_dct = {
        "tornado.access": "WARNING", 
        "stream":    "INFO",
        "DVRReader": "INFO",
        "DVRWriter": "INFO",
    }    
    
    # tornado.access - это не ошибки, которые надо чинить, поэтому
    # propagate=False
    do_not_propagate = set(["tornado.access"])
    
    ll_dct = ll_def_dct.copy()
    ll_dct2 = cfg['live']['logging_level']
    if ll_dct2:
        ll_dct.update(ll_dct2)
    
    # журналы постоянного хранения
    ll_append_set = set([
        "stream",
    ])
    
    for name, level in ll_dct.items():
        setup_custom_logger(name, getattr(logging, level), not(name in do_not_propagate), name in ll_append_set)
    # ----- </logging.application>

# :TRICKY: окружение нужно в самом начале, поэтому -
# environment = parse_args()
args = parse_args()

cfg = {
    'path_config': args.config,
    'do_show_version': args.version,
}
for cfg_file_name in (
    'live',
    'storage',
    'res',
    'udp-source',
):
    with open(
        os.path.join(
            cfg['path_config'],
            '{0}.yaml'.format(cfg_file_name)
        ),
        'r',
        encoding='utf-8',
    ) as cfg_file:
        cfg[cfg_file_name] = yaml.load(cfg_file)

# вычисление environment по имени директории конфигов
if not "environment" in cfg['live']:
    cfg['live']["environment"] = os.path.basename(args.config)

def get_cfg_value(key, def_value=None):
    #return getattr(environment, key, def_value)
    return cfg['live'].get(key, def_value)

cast_one_source = get_cfg_value("cast_one_source", None)
is_test = not cast_one_source and cfg['live']['is_test']
use_sendfile = get_cfg_value('use_sendfile', True)
local_dvr    = get_cfg_value("local_dvr", False)

db_path = os.path.expanduser(cfg['live']['db-path'])

# Устанавливаем логи
setup_logging()


chunk_dur = args.chunk_duration if args.chunk_duration \
                else get_cfg_value("chunk-duration",
                                   DEFAULT_CHUNK_DURATION)
