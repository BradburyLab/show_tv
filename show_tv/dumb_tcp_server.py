# coding: utf-8

from tornado import gen

def try_read_bytes(stream, num_bytes, callback, streaming_callback=None):
    """ Прочитать num_bytes либо меньше, если поток закроется раньше """
    def finish(is_ok, data):
        callback((is_ok, data))

    def read_from_buffer(num_bytes):    
        rest_cnt = stream._read_buffer_size
        
        cnt = min(rest_cnt, num_bytes)
        is_ok = rest_cnt >= num_bytes
        
        def handle_read(data):
            finish(is_ok, data)
        stream.read_bytes(cnt, handle_read, streaming_callback=streaming_callback)
    
    # :TRICKY: hack-вариант - одновременно read_until_close() и 
    # read_bytes() вызвать нельзя из-за повторной установки _read_callback =>
    # эмулируем read_bytes() установкой атрибута _read_bytes
    # Побочный эффект: если сработает _read_bytes, то останется висеть флаг
    # _read_until_close, и наоборот
    #stream._read_bytes = num_bytes
    #stream.read_until_close(handle_read_end, streaming_callback=streaming_callback)
    
    if stream.closed():
        # :TRICKY: случай, когда канал закрыли, и буфера недостаточно -
        # read_bytes() вызовет исключение StreamClosedError("Stream is closed"), поэтому
        # смотрим только на буфер
        read_from_buffer(num_bytes)
    else:
        def handle_read_end(data):
            # убираем старый, иначе он после может сработать и вызвать "assert cnt is not None"
            stream.set_close_callback(None)
            finish(True, data)
        stream.read_bytes(num_bytes, handle_read_end, streaming_callback=streaming_callback)
        
        def handle_close():
            # cnt - остаток от num_bytes, что требовалось прочитать 
            cnt = stream._read_bytes
            # если сработал close(), значит read_bytes() не успел отработать =>
            assert cnt is not None
            
            # :TRICKY: считаю, что это больший хак, чем получение доступа к _read_buffer_size
            # и _read_bytes
            #finish(False, b''.join(stream._read_buffer))
            read_from_buffer(cnt)
        stream.set_close_callback(handle_close)

#
#
#

import api
import struct

import logging
logger = api.stream_logger # logging.getLogger()

def write_error(txt):
    logger.error(txt)

def print_log(arg):
    #if is_verbose:
        #print(arg)
    logger.debug(arg)

def print_stream_event(is_open, address):
    txt = "Stream is opened" if is_open else "Stream is closed"
    logger.info("%s: %s", txt, address)

@gen.engine
def read_messages(stream, dvr_prefix_format, on_message, callback=None, streaming_callback=None):
    prefix_sz = struct.calcsize(dvr_prefix_format)
    while True:
        is_ok, data = yield gen.Task(try_read_bytes, stream, prefix_sz)
        if not is_ok:
            # :TRICKY: закрытый сокет без данных - ок
            if data:
                write_error("not full prefix: %s" % data)
            break
        
        tpl = struct.unpack(dvr_prefix_format, data)
        mn  = tpl[0]
        if mn != api.DVR_MAGIC_NUMBER:
            write_error("DVR_MAGIC_NUMBER is wrong: 0x%x" % mn)
        print_log(tpl)
        
        payload_len = tpl[-1]
        is_ok, data = yield gen.Task(try_read_bytes, stream, payload_len, streaming_callback=streaming_callback)
        if not is_ok:
            write_error("not full payload")
            break
        on_message(tpl, data)
     
    if callback:   
        callback()

@gen.engine
def handle_dvr_stream(self, stream, address):
    print_stream_event(True, address)
    
    def on_message(tpl, data):
        pass
    yield gen.Task(read_messages, stream, api.make_dvr_prefix_format(True), on_message, streaming_callback=on_read)
    
    print_stream_event(False, address)

def on_read(data):
    print_log(len(data))

if __name__ == '__main__':
    # :REFACTOR:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--verbose',
        dest='is_verbose', action="store_true",
    )
    # :TODO: разобраться, как можно попроще записывать
    # булевские аргументы по умолчанию=True
    parser.add_argument(
        '--is_not_dvr_read',
        dest='is_not_dvr_read', action="store_true",
    )
    
    args = parser.parse_args()
    
    is_dvr_read = not args.is_not_dvr_read

    # логирование
    import os
    log_fpath = os.path.join(os.path.dirname(__file__), '../log/d_t_s.log')
    logging_level = logging.DEBUG if args.is_verbose else logging.INFO
    api.setup_logger(logger, log_fpath, logging_level)
    
    if is_dvr_read:
        handle_stream = handle_dvr_stream
    else:
        def handle_stream(self, stream, address):
            stream.read_until_close(on_read, streaming_callback=on_read)
            
    from test_sendfile import start_tcp_server, run_loop
    
    port = 6451
    start_tcp_server(handle_stream, port)
    run_loop()
