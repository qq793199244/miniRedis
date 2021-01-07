from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from collections import namedtuple
from io import BytesIO
from socket import error as socket_error
import sys
import pickle
import os
import time
import logging

now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
path = './saved_files/'
if not os.path.exists(path):
    os.mkdir(path)

FILE_NAME = path + now + "_save_to_disk.pkl"


# 用异常通知连接处理中的问题
class CommandError(Exception):
    def __init__(self, message):
        self.message = message
        super(CommandError, self).__init__()


class Disconnect(Exception):
    pass


Error = namedtuple('Error', ('message',))

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


class ProtocolHandler(object):
    def __init__(self):
        # 网络中以字节传播，前面写b
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict
        }

    def handle_request(self, socket_file):
        # 将来自客户端的请求解析为它的组件部分。
        first_byte = socket_file.read(1)
        if not first_byte:
            raise EOFError()

        try:
            # 根据第一个字节委托给适当的处理程序。
            return self.handlers[first_byte](socket_file)
        except KeyError:
            rest = socket_file.readline().rstrip(b'\r\n')
            return first_byte + rest

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))

    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip(b'\r\n'))

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    # 对于协议的序列化方面，执行与上述相反的操作：将Python对象转换为其序列化的对象！
    def write_response(self, socket_file, data):
        # 序列化响应数据，并将它发送给客户端
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, (int, float)):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, Error):
            buf.write(b'-%s\r\n' % encode(data.message))
        elif isinstance(data, (list, tuple)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b'$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s' % type(data))


class ClientQuit(Exception):
    pass


class Shutdown(Exception):
    pass


class Server(object):
    def __init__(self, host='127.0.0.1', port=33333, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn=self._pool)

        self._protocol = ProtocolHandler()
        self._kv = {}

        self._commands = self.get_commands()
        self._schedule = []

    def get_commands(self):
        return {
            b'GET': self.get,
            b'SET': self.set,
            b'DELETE': self.delete,
            b'FLUSH': self.flush,
            b'MGET': self.mget,
            b'MSET': self.mset,
            b'SAVE': self.save_to_disk,
            b'RESTORE': self.restore_from_disk,
            b'MERGE': self.merge_from_disk,
            b'QUIT': self.client_quit,
            b'SHUTDOWN': self.shutdown,
        }

    def get_response(self, data):
        # 解压客户端发送的数据，执行它们指定的命令，并传回返回值
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        return self._commands[command](*data[1:])

    def _get_state(self):
        return {'kv': self._kv, 'schedule': self._schedule}

    def _set_state(self, state, merge=False):
        if not merge:
            self._kv = state['kv']
            self._schedule = state['schedule']
        else:
            def merge(orig, updates):
                orig.update(updates)
                return orig

            self._kv = merge(state['kv'], self._kv)
            self._schedule = state['schedule']

    # 持久化，保存到磁盘
    def save_to_disk(self):
        filename = FILE_NAME
        with open(filename, 'wb') as fh:
            pickle.dump(self._get_state(), fh, pickle.HIGHEST_PROTOCOL)
        print('已保存到磁盘。')
        return True

    # 从磁盘恢复
    def restore_from_disk(self, filename, merge=False):
        if not os.path.exists(filename):
            return False
        with open(filename, 'rb') as fh:
            state = pickle.load(fh)
        self._set_state(state, merge=merge)
        return True

    # 从磁盘合并
    def merge_from_disk(self, filename):
        return self.restore_from_disk(filename, merge=True)

    def client_quit(self):
        raise ClientQuit('客户端关闭连接。')

    def shutdown(self):
        raise Shutdown('Shutting down')

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = list(zip(items[::2], items[1::2]))
        for key, value in data:
            self._kv[key] = value
        return len(data)

    def quit(self):
        pass

    def connection_handler(self, conn, address):
        # 将conn（套接字对象）转换为类文件的对象
        socket_file = conn.makefile('rwb')

        # 处理客户端请求，直到客户端断开连接
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break
            except EOFError:
                socket_file.close()
                print('客户端关闭连接。')
                break
            except ClientQuit:
                print('客户端关闭连接。')
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

    def run(self):
        self._server.serve_forever()


class Client(object):
    def __init__(self, host='127.0.0.1', port=33333):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp

    def command(cmd):
        def method(self, *args):
            return self.execute(cmd.encode('utf-8'), *args)

        return method

    # def close(self):
    #     self.execute(b'QUIT')

    # 操作命令
    get = command('GET')
    set = command('SET')
    delete = command('DELETE')
    flush = command('FLUSH')
    mget = command('MGET')
    mset = command('MSET')

    # 控制命令
    save = command('SAVE')
    restore = command('RESTORE')
    merge = command('MERGE')
    quit = command('QUIT')
    shutdown = command('SHUTDOWN')

    '''def get(self, key):
        return self.execute(b'GET', key)

    def set(self, key, value):
        return self.execute(b'SET', key, value)

    def delete(self, key):
        return self.execute(b'DELETE', key)

    def flush(self):
        return self.execute(b'FLUSH')

    def mget(self, *keys):
        return self.execute(b'MGET', *keys)

    def mset(self, *items):
        return self.execute(b'MSET', *items)'''


if __name__ == '__main__':
    from gevent import monkey

    monkey.patch_all()
    print('Server运行中...')
    Server().run()
