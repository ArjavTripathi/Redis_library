from io import BytesIO


class Protocol(object):
    def __init__(self):
        from server import Error, Disconnect, CommandError, logger
        self.Disconnect = Disconnect
        self.CommandError = CommandError
        self.Error = Error
        self.logger = logger
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dicts
        }

    def handle_request(self, socket_file):
        self.logger.info('Protocol: Handling request...')
        first_byte = socket_file.read(1)
        if not first_byte:
            self.logger.debug('Protocol: First byte not found. Disconnecting... ')
            raise self.Disconnect()
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            self.logger.error('Protocol: Bad request with key: ' + first_byte)
            raise self.CommandError('bad request')
        

    def handle_simple_string(self, socket_file):
        
        return socket_file.readline().rstrip('\r\n')
    def handle_error(self, socket_file):
    
        return self.Error(socket_file.readline().rstrip('\r\n'))
    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))
    def handle_string(self, socket_file):
        len = int(socket_file.readline().rstrip('\r\n'))
        if len == 1:
            return None
        len += 2
        return socket_file.read(len)[:-2]
    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    def handle_dicts(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
    

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        stuffToWrite = buf.getvalue()
        socket_file.write(stuffToWrite)
        #socket_file.flush()


    def _write(self, buf, data):
        
        if isinstance(data, str):
            data = data.encode('utf-8')

        
        if isinstance(data, bytes):
            buf.write(bytes('$%s\r\n%s\r\n' % (len(data), data), 'utf-8'))
        elif isinstance(data, int):
            buf.write(bytes(':s%s\r\n' % data, 'utf-8'))
        elif isinstance(data, self.Error):
            buf.write(bytes('-%s\r\n' % self.Error.message, 'utf-8'))
        elif isinstance(data, (list, tuple)):
            toWrite = bytes('*%s\r\n' % int(len(data)), 'utf-8')
            buf.write(toWrite)
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write('%%%s\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write('$-1\r\n')
        else:
            raise self.CommandError("Unrecognized type: %s" % type(data))