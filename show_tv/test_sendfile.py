# coding: utf-8

from sendfile import sendfile

import sys, os
import struct
import socket

from tornado.ioloop import IOLoop
from tornado import gen
import tornado.iostream

PORT = 8889
HOST = 'localhost'

io_loop = IOLoop.instance()

def run_loop():
    io_loop.start()
    # не нужно, если только один IOLoop для всего процесса
    #io_loop.close()

is_checked_io = True

length_format = ">Q"

def make_hash_obj():
    import hashlib
    return hashlib.md5()

@gen.engine
def write_number(strm, n, callback):
    l_dat = struct.pack(length_format, n)
    yield gen.Task(strm.write, l_dat)
    callback()

@gen.engine
def finish_packet(strm, m, callback):
    yield gen.Task(strm.write, m.digest())
    print("Data has been sent")
    callback()

@gen.engine
def write_string(strm, s, callback):
    if is_checked_io:
        yield gen.Task(write_number, strm, len(s))
        yield gen.Task(strm.write, s)

        m = make_hash_obj()
        m.update(s)
        yield gen.Task(finish_packet, strm, m)
    else:
        yield gen.Task(strm.write, s)
    callback()

def do_client():
    
    class lcls:
        strm = None
    
    @gen.engine
    def reconnect(callback):
        strm = tornado.iostream.IOStream(
            socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
                0,
            )
        )
        lcls.strm = strm
        yield gen.Task(strm.connect, (HOST, PORT))
        
        callback(None)
    
    @gen.engine
    def write():
        if not lcls.strm:
            yield gen.Task(reconnect)

        if lcls.strm.closed():
            yield gen.Task(reconnect)

        strm = lcls.strm
        if strm.closed():
            print('failed to connect')
            return

        yield gen.Task(write_string, strm, b"Hello, world")

        #fname = '/home/muravyev/opt/bl/f451/git/show_tv/test-data/dump1.txt'
        fname = os.path.expanduser("~/opt/bl/f451/tmp/test_src/pervyj-720x406.ts")
        sz = os.stat(fname).st_size
        a_str = b"abcbx"
        cnt = 3
        yield gen.Task(write_number, strm, cnt*(sz + len(a_str)))
        for i in range(cnt):
            sendfile(strm, fname, sz)
            strm.write(a_str)
        
        with open(fname, "rb") as f:
            m = make_hash_obj()
            txt = f.read()
            for i in range(cnt):
                m.update(txt)
                m.update(a_str)
            yield gen.Task(finish_packet, strm, m)

        # запись подряд 2х строк
        s1 = b"|".join([b"aaaa" for i in range(1000000)])
        s11 = s1 + s1
        m = make_hash_obj()
        m.update(s11)
        yield gen.Task(write_number, strm, len(s11))
        strm.write(s1)
        strm.write(s1)
        yield gen.Task(finish_packet, strm, m)

        yield gen.Task(write_string, strm, b"Bye, world")
        
        # конец передачи данных
        yield gen.Task(write_number, strm, 0)
        
        # :TRICKY: если не закрывать соединение, то 
        # данные к клиенту не придут: flush()'а нет, а другой вариант - 
        # пробовать TCP_NODELAY, http://stackoverflow.com/questions/855544/is-there-a-way-to-flush-a-posix-socket
        strm.close()
        
        # сразу не выходим, потому что тогда серверу не дадим отработать
        #io_loop.stop()
        import datetime
        def on_timeout():
            io_loop.stop()
        io_loop.add_timeout(datetime.timedelta(seconds=1), on_timeout)
        
    write()

def make_tcp_server(handle_stream):
    from tornado.tcpserver import TCPServer
    
    class Server(TCPServer):
        pass
    Server.handle_stream = handle_stream
        
    # max_buffer_size - иначе ошибка "Reached maximum read buffer size" на
    # больших чтениях
    return Server(max_buffer_size=404857600)

def start_tcp_server(handle_stream, port):
    server = make_tcp_server(handle_stream)
    server.listen(port)
        
    def handle_signal(sig, frame):
        io_loop.add_callback(io_loop.stop)
    
    import signal
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, handle_signal)

def do_server():
    if is_checked_io:
        @gen.engine
        def handle_stream(self, stream, address):
            while True:
                l_sz = struct.calcsize(length_format)
                assert l_sz == 8
                data = yield gen.Task(stream.read_bytes, l_sz)
                length = struct.unpack(length_format, data)[0]
                if not length:
                    stream.close()
                    break
                
                # :TODO: надо последовательно читать
                data = yield gen.Task(stream.read_bytes, length)
                m = make_hash_obj()
                m.update(data)
                
                client_hash = yield gen.Task(stream.read_bytes, m.digest_size)
                assert m.digest() == client_hash
                print("Data has been received")
    else:
        def handle_stream(self, stream, address):
            def handle_read(data):
                #print(data.decode('utf-8'))
                print("###############")
                #if not stream.closed():
                    #setup_reading(stream)
                
            stream.read_until_close(handle_read, handle_read)
    
    start_tcp_server(handle_stream, PORT)
        
if __name__ == '__main__':
    is_client = len(sys.argv) > 1
    if is_client:
        async_write = True # False # 
        if async_write:
            do_client()
            run_loop()
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((HOST, PORT))
            s.send(b'Hello, world')
    
            fname = '/home/muravyev/opt/bl/f451/git/show_tv/test-data/dump1.txt'
            #fname = '/home/muravyev/opt/bl/f451/tmp/test_src/pervyj-720x406.ts'
            ln = os.stat(fname).st_size
            
            f = open(fname, "rb")
            use_sf = True
            if use_sf:
                s_fd = s.fileno()
                
                def get_blocking():
                    import fcntl
                    flags = fcntl.fcntl(s_fd, fcntl.F_GETFL)
                    print("flags:", flags)
                    return flags
    
                get_blocking()
                
                s.setblocking(0)
                
                flags = get_blocking()
                assert flags & os.O_NONBLOCK
                
                off = 0
                sz  = ln
                while True:
                    if sz:
                        try:
                            res = os.sendfile(s_fd, f.fileno(), off, sz)
                        except socket.error as es:
                            print(es)
                            raise
                        except Exception as e:
                            print(e)
                            raise
                        print(res)
                        assert res > 0, res
                        off += res
                        sz -= res
                    else:
                        break
                    
                print("end")
            else:
                txt = f.read(ln)
                s.send(txt)
            f.close()
            
            #data = s.recv(1024)
            s.close()
    else:
        do_server()
        do_client()
        run_loop()
