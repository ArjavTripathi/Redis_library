from ProtocolHandler import Protocol
from gevent import socket
from server import CommandError, Error, logger





class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        logger.info('Client: Called initially')
        self._protocol = Protocol()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._socket.connect((host, port))
            logger.info('Client: Connected to host: ' + host + ' on port: ' + str(port))
        except ConnectionError:
            logger.debug('Client: error connecting to host: ' + host + ' on port: ' + str(port))

     
        try:
            self._fh = self._socket.makefile('rwb')
            logger.log('Client: Socket file made')
        except Exception:
            logger.debug('Client: Socket file could not be made. Exception:')
            
    
    def execute(self, *args):
        self._protocol.write_response(self._fh, args)
        response = self._protocol.handle_request(self._fh)
        if isinstance(response, Error):
            raise CommandError(response.message)
        return response
    


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
