# coding: utf-8

if __name__ == '__main__':
    from tornado import gen
    from test_sendfile import start_tcp_server, run_loop

    def on_read(data):
        print(len(data))
    
    @gen.engine
    def handle_stream(self, stream, address):
        stream.read_until_close(on_read, streaming_callback=on_read)
            
    port = 6451

    start_tcp_server(handle_stream, port)
    run_loop()
