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
        dest='log', type=str, default='/var/log/451',
        help='path to log folder',
    )
    parser.add_argument(
        '-v', '--version',
        dest='version', type=bool, default=False,
        help='show verion number',
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

def setup_logging():
    # <logging.tornado> -----
    for name in (
        'tornado.access',
        'tornado.application',
        # 'tornado.general',
    ):
        logger = logging.getLogger(name)
        # tornado_lvl = logging.INFO if get_env_value("verbose_tornado", False) else logging.WARNING
        logging_level = getattr(logging, cfg['live']['logging_level'][name])
        logger.setLevel(logging_level)

        api.setup_file_logger(log_name2path(name), logging_level, logger)
    # ----- </logging.tornado>
    # <logging.application> -----
    for name in (
        'stream',
        'DVRReader',
        'DVRWriter',
    ):
        logging_level = getattr(logging, cfg['live']['logging_level'][name])

        logger = logging.getLogger(name)
        logger.propagate = False
        logger.handlers = []

        api.setup_logger(logger, log_name2path(name), logging_level)
    # ----- </logging.application>

# :TRICKY: окружение нужно в самом начале, поэтому -
# environment = parse_args()
args = parse_args()

cfg = {
    'path_config': args.config,
    'path_log': args.log,
    'do_show_version': args.version,
}
for cfg_file_name in (
    'hds',
    'hls',
    'live',
    'storage',
    'udp-source',
    'wv-source',
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

# Устанавливаем логи
setup_logging()
