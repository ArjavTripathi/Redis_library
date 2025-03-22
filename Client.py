from ProtocolHandler import Protocol
from gevent import socket
from server import CommandError, Error, logger, Disconnect
from SocketPool import SocketPool




class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        logger.info('Client: Called initially')
        self._protocol = Protocol()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._host = host
        self._port = port
        self._max_pool = 60
        self._socket_pool = SocketPool(host, port, 60)


        try:
            self._socket.connect((host, port))
            logger.info('Client: Connected to host: ' + host + ' on port: ' + str(port))
        except ConnectionError:
            logger.debug('Client: error connecting to host: ' + host + ' on port: ' + str(port))

        self._fh = self._socket.makefile('rwb')
    
    def execute(self, *args):
        conn = self._socket_pool.checkout()
        close_conn = args[0] in (b'QUIT', b'SHUTDOWN')
        self._protocol.write_response(conn, args)
        try:
            resp = self._protocol.handle_request(conn)
        except EOFError:
            self._socket_pool.close()
            raise Disconnect('server went away')
        except Exception:
            self._socket_pool.close()
            raise Disconnect('internal server error')
        else:
            if close_conn:
                self._socket_pool.close()
            else:
                self._socket_pool.checkin()
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp


    def get(self, key):
        return self.execute('GET', key)
    def set(self, key):
        return self.execute('SET', key)
    def delete(self, key):
        return self.execute('DELETE', key)
    def flush(self, key):
        return self.execute('FLUSH')
    def mget(self, *keys):
        return self.execute('MGET', *keys)
    def mset(self, *items):
        return self.execute('MSET', *items)
