# coding: utf-8

if __name__ == '__main__':
    PORT = 8889
    
    import sys
    is_client = len(sys.argv) > 1
    
    if is_client:
        import socket
        import os

        HOST = 'localhost'    # The remote host
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        s.send(b'Hello, world')

        #fname = '/home/muravyev/opt/bl/f451/git/show_tv/test-data/dump1.txt'
        fname = '/home/muravyev/opt/bl/f451/tmp/test_src/pervyj-720x406.ts'
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
        import signal
        
        from tornado.ioloop import IOLoop
        from tornado.tcpserver import TCPServer
        
        #from tornado_pyuv import UVLoop
        #IOLoop.configure(UVLoop)
        
        class EchoServer(TCPServer):
        
            def handle_stream(self, stream, address):
                def handle_read(data):
                    #print(data.decode('utf-8'))
                    print("###############")
                    #if not stream.closed():
                        #setup_reading(stream)
                    
                stream.read_until_close(handle_read, None)
    
        def handle_signal(sig, frame):
            IOLoop.instance().add_callback(IOLoop.instance().stop)
        
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, handle_signal)
            
        server = EchoServer()
        server.listen(PORT)
        IOLoop.instance().start()
        # не нужно, если только один IOLoop для всего процесса
        #IOLoop.instance().close()

