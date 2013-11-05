# coding: utf-8

from tornado import gen

def try_read_bytes(stream, num_bytes, callback, streaming_callback=None):
    """ Прочитать num_bytes либо меньше, если поток закроется раньше """
    def handle_read(data):
        callback((True, data))
    
    # :TRICKY: hack-вариант - одновременно read_until_close() и 
    # read_bytes() вызвать нельзя из-за повторной установки _read_callback =>
    # эмулируем read_bytes() установкой атрибута _read_bytes
    # Побочный эффект: если сработает _read_bytes, то останется висеть флаг
    # _read_until_close, и наоборот
    #stream._read_bytes = num_bytes
    #stream.read_until_close(handle_read, streaming_callback=handle_read)
    
    stream.read_bytes(num_bytes, handle_read, streaming_callback=streaming_callback)
    def handle_close():
        callback((False, b''.join(stream._read_buffer)))
    stream.set_close_callback(handle_close)

#
#
#

is_verbose = False

import api
import struct

PREFIX_SZ = struct.calcsize(api.DVR_PREFIX_FMT)

def write_error(txt):
    print("ERROR:", txt)

def print_log(*args):
    if is_verbose:
        print(*args)

@gen.engine
def handle_dvr_stream(self, stream, address):
    print("Stream is opened:", address)
    while True:
        is_ok, data = yield gen.Task(try_read_bytes, stream, PREFIX_SZ)
        if not is_ok:
            # :TRICKY: закрытый сокет без данных - ок
            if data:
                write_error("not full prefix: %s" % data)
            break
        
        tpl = struct.unpack(api.DVR_PREFIX_FMT, data)
        mn  = tpl[0]
        if mn != api.DVR_MAGIC_NUMBER:
            write_error("DVR_MAGIC_NUMBER is wrong: 0x%x" % mn)
        print_log(tpl)
        
        payload_len = tpl[-1]
        is_ok, data = yield gen.Task(try_read_bytes, stream, payload_len, streaming_callback=on_read)
        if not is_ok:
            write_error("not full payload")
            break
    print("Stream is closed:", address)

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
    
    is_verbose  = args.is_verbose
    is_dvr_read = not args.is_not_dvr_read
    
    if is_dvr_read:
        handle_stream = handle_dvr_stream
    else:
        def handle_stream(self, stream, address):
            stream.read_until_close(on_read, streaming_callback=on_read)
            
    from test_sendfile import start_tcp_server, run_loop
    
    port = 6451
    start_tcp_server(handle_stream, port)
    run_loop()
