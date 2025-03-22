from io import BytesIO
import time
import sys
import datetime
from collections import deque
import json



if sys.version_info[0] == 3:
    unicode = str
    basestring = (bytes, str)

def encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    elif isinstance(s, bytes):
        return s
    else:
        return str(s).encode('utf-8')


def decode(s):
    if isinstance(s, unicode):
        return s
    elif isinstance(s, bytes):
        return s.decode('utf-8')
    else:
        return str(s)


class Protocol(object):
    def __init__(self):
        from server import Error, Disconnect, CommandError, logger
        self.Disconnect = Disconnect
        self.CommandError = CommandError
        self.Error = Error
        self.logger = logger
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dicts,
            b'^': self.handle_unicode,
            b'@': self.handle_json,
            b'&': self.handle_set
        }

    def handle_request(self, socket_file):
        try:
            first_byte = socket_file.read(1)
            self.logger.info('Protocol: Read successfully. First byte has value: ' + str(first_byte))
        except Exception as e:
            self.logger.info('Protocol: Issue with reading socket file. Exception: ' + str(e))
            raise self.Disconnect()

        # if not first_byte:
        #     self.logger.debug('Protocol: First byte not proper. Disconnecting... ')
        #     raise self.Disconnect()
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError as e:
            self.logger.error('Protocol: Bad request with key: ' + str(first_byte) + ' with exception: KeyError')
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
    
    def handle_unicode(self, socket_file):
        return self.handle_string(socket_file).decode('utf-8')

    def handle_json(self, socket_file):
        return json.loads(self.handle_string(socket_file))

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def handle_set(self, socket_file):
        return set(self.handle_array(socket_file))

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise EOFError()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            rest = socket_file.readline().rstrip(b'\r\n')
            return first_byte + rest
    

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        stuffToWrite = buf.getvalue()
        try:
            socket_file.write(stuffToWrite)
            time.sleep(1)
            socket_file.flush()
            self.logger.info('Protocol: Socket file has been written to with value: ' + str(stuffToWrite))
        except Exception as e:
            self.logger.info('Protocol: Socket file could not be written to. Exception: ' + str(e))
            raise self.Disconnect()

    def _write(self, buf, data):
        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, unicode):
            bdata = data.encode('utf-8')
            buf.write(b'^%d\r\n%s\r\n' % (len(bdata), bdata))
        elif data is True or data is False:
            buf.write(b':%d\r\n' % (1 if data else 0))
        elif isinstance(data, (int, float)):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, self.Error):
            buf.write(b'-%s\r\n' % encode(data.message))
        elif isinstance(data, (list, tuple, deque)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif isinstance(data, set):
            buf.write(b'&%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif data is None:
            buf.write(b'$-1\r\n')
        elif isinstance(data, datetime.datetime):
            self._write(buf, str(data))