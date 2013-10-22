# coding: utf-8

import socket
import os
import errno

import logging
gen_log = logging.getLogger("tornado.general")

from tornado.ioloop import IOLoop
from tornado import gen

def is_pending_sendfile(strm):
    return strm.sf_sz > 0

def handle_sendfile(self):
    while is_pending_sendfile(self):
        try:
            num_bytes = os.sendfile(self.fileno(), self.sf_f.fileno(), self.sf_off, self.sf_sz)
            self.sf_off += num_bytes
            self.sf_sz  -= num_bytes
        except socket.error as e:
            # :COPY_N_PASTE:
            if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                break
            else:
                if e.args[0] not in (errno.EPIPE, errno.ECONNRESET):
                    # Broken pipe errors are usually caused by connection
                    # reset, and its better to not log EPIPE errors to
                    # minimize log spam
                    gen_log.warning("Write error on %d: %s",
                                    self.fileno(), e)
                self.close(exc_info=True)
                return
    if not is_pending_sendfile(self) and self._write_callback:
        self.sf_f.close()
        self.is_sendfile_on = False
        
        # :COPY_N_PASTE:
        callback = self._write_callback
        self._write_callback = None
        self._run_callback(callback)

def sendfile(strm, fpath, size, callback=None):
    self = strm
    if not "is_sendfile_on" in dir(self):
        self.is_sendfile_on = False
        
        def replace_meth(name, sf_meth):
            old = getattr(self, name) # self.__dict__[name]
            def wrapper(*args, **kw):
                if self.is_sendfile_on:
                    res = sf_meth(self, *args, **kw)
                else:
                    res = old(*args, **kw)
                return res
            setattr(self, name, wrapper) #self.__dict__[name] = wrapper
            
        
        replace_meth("_handle_write", handle_sendfile)
        replace_meth("writing", is_pending_sendfile)
        
        def write_fallback(data, callback=None):
            # нельзя писать, когда есть незаконченный sendfile
            # :TODO: избавиться от этого ограничения (хотя и не сильно нужно)
            assert False
        replace_meth("write", write_fallback)
        
    # не умеем сразу несколько sendfile()'ов сразу, пока
    assert not self.is_sendfile_on
    # пред. буфер должен быть дописан до конца
    assert not self.writing()

    self.is_sendfile_on = True
    self.sf_f = open(fpath, "rb")

    self.sf_off = 0
    self.sf_sz  = size

    from tornado import stack_context
    # :COPY_N_PASTE: аналог IOStream
    self._check_closed()
    self._write_callback = stack_context.wrap(callback)
    if not self._connecting:
        self._handle_write()
        if is_pending_sendfile(self):
            self._add_io_state(self.io_loop.WRITE)
        self._maybe_add_error_listener()

#
# Тестовый код
#

import sys
import struct

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
    import tornado.iostream
    
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
        fname = '/home/muravyev/opt/bl/f451/tmp/test_src/pervyj-720x406.ts'
        sz = os.stat(fname).st_size
        yield gen.Task(write_number, strm, sz)
        yield gen.Task(sendfile, strm, fname, sz)
        
        with open(fname, "rb") as f:
            m = make_hash_obj()
            m.update(f.read())
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
        
    return Server()

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
    else:
        def handle_stream(self, stream, address):
            def handle_read(data):
                #print(data.decode('utf-8'))
                print("###############")
                #if not stream.closed():
                    #setup_reading(stream)
                
            stream.read_until_close(handle_read, handle_read)
    
    server = make_tcp_server(handle_stream)
    server.listen(PORT)
        
    def handle_signal(sig, frame):
        io_loop.add_callback(io_loop.stop)
    
    import signal
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, handle_signal)
        
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
