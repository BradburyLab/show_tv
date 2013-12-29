# coding: utf-8

import tornado.web
from sendfile import sendfile

def is_head(request):
    return request.method == "HEAD"

#
# Замечание: при очень больших нагрузках (15Gbit/s) уже вылезают
# такие ошибки:
# - validate_absolute_path() прошел, но последующий get_content_size()
#   вывалился, потому что файл был удален
# - stream.write(b'', connection._on_write_complete) вызвал исключение,
#   потому что в этот момент сокет уже оказался закрыт
# Итого: на адекватных нагрузках такое не происходит => не лечим
#

class StaticFileHandler(tornado.web.StaticFileHandler):
    
    def initialize(self, **kwargs):
        self.write_mode = True
        super().initialize(**kwargs)
    
    def head(self, path):
        self.get(path, include_body=False)

    def get(self, path, include_body=True):
        self.path = self.parse_url_path(path)
        del path  # make sure we don't refer to path instead of self.path again
        absolute_path = self.get_absolute_path(self.root, self.path)
        self.absolute_path = self.validate_absolute_path(
            self.root, absolute_path)
        if self.absolute_path is None:
            return

        # :TRICKY: заголовки оригинального StaticFileHandler трудно вычислять
        #self.modified = self.get_modified_time()
        #self.set_headers()
        self.set_header("Content-Type", "binary/octet")
        
        size = self.get_content_size()
        # :TODO: поддержка Range, как в оригинальном StaticFileHandler
        beg, end = 0, size
        self.send_range = beg, end
        
        if not include_body:
            assert is_head(request)
            
        # явно размер выставляем
        self.set_header("Content-Length", end-beg)
        
        self.write_mode = False
            
    def finish(self, chunk=None):
        """Finishes this response, ending the HTTP request."""
        
        # :TRICKY: другое поведение здесь - не пользуемся RequestHandler.write();
        # однако в случае исключений (404) write_error() использует write(), поэтому
        # включаем прошлое поведение
        if chunk is not None:
            self.write(chunk)
        if self._write_buffer:
            assert self.write_mode
            super().finish()
            return
        
        if self._finished:
            raise RuntimeError("finish() called twice.  May be caused "
                               "by using async operations without the "
                               "@asynchronous decorator.")

        # :TRICKY: другое поведение здесь - не пользуемся RequestHandler.write()
        if chunk is not None:
            #self.write(chunk)
            assert None
        assert not self._write_buffer

        if hasattr(self.request, "connection"):
            # Now that the request is finished, clear the callback we
            # set on the HTTPConnection (which would otherwise prevent the
            # garbage collection of the RequestHandler when there
            # are keepalive connections)
            self.request.connection.set_close_callback(None)

        # :TRICKY: актуально ли?
        assert not self.application._wsgi

        #
        # RequestHandler.flush()
        #
        #self.flush(include_footers=True)
        headers = self._generate_headers()
        
        #
        # HTTPRequest.write() = HTTPConnection.write()
        # 
        connection = self.request.connection
        stream = connection.stream
        if not stream.closed():
            stream.write(headers)
            if not is_head(self.request):
                beg, end = self.send_range 
                assert beg == 0, "TODO: implement offset in sendfile.sendfile()"
                sendfile(stream, self.absolute_path, end-beg)
           
            stream.write(b'', connection._on_write_complete)
        
        self.request.finish()
        self._log()
        
        self._finished = True
        self.on_finish()
        # Break up a reference cycle between this handler and the
        # _ui_module closures to allow for faster GC on CPython.
        self.ui = None

    def compute_etag(self):
        # запрещаем все манипуляции с содержимым контента
        # (собственно cls.get_content() не запретить, потому что это
        # метод класса)
        assert self.write_mode
        return None
    
def main():
    out_fpath = '/home/muravyev/opt/bl/f451/tmp/test_src'
    
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    def make_static_handler():
        return r"/(.*)", StaticFileHandler, {"path": out_fpath}
        
    handlers = [
        make_static_handler(),
    ]
    
    application = tornado.web.Application(handlers)
    application.listen(8000)

    import tornado.ioloop
    io_loop = tornado.ioloop.IOLoop.instance()
    io_loop.start()
    

if __name__ == '__main__':
    main()
