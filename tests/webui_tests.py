#!/usr/bin/env python3

import io
import socket
import unittest

import webui.http_handler as http_handler
import cli.task_webui as task_webui

class FakeArgs(object): pass

class FakeUIState(object):
    def make_http_handler(self):
        return http_handler.HttpHandler(self)

class FakeLogger(object):
    def log_info(self, what, which, comment=''):
        pass
    def log_error(self, what, which, comment=''):
        pass
    def print(self, msg):
        pass

def connect_server(port):
    sock = socket.socket()
    sock.settimeout(0.1)
    sock.connect(('127.0.0.1', port))
    return sock

class ResponseIncomplete(Exception):
    def __init__(self, msg, headers=None, body=None):
        Exception.__init__(self, msg)
        self.headers = headers
        self.body = body

def parse_response(data):
    header_end = data.find(b'\r\n\r\n')
    if header_end < 0:
        raise ResponseIncomplete('Headers incomplete')
    headers = {}
    for line in data[:header_end].split(b'\r\n'):
        if not headers:
            headers[None] = line
        else:
            name, value = line.split(b':', 1)
            assert value.startswith(b' ')
            value = value[1:]
            assert name not in headers
            headers[name] = value
    body = data[header_end+4:]
    rest = b''
    if headers.get(b'Transfer-Encoding') == b'chunked':
        assert not rest
        body, rest = parse_chunked_encoding(body)
        return headers, body, rest
    raise ResponseIncomplete(
        'End of body not found', headers=headers, body=body)

def parse_chunked_encoding(data):
    body = []
    done = 0
    while True:
        end = data.find(b'\r\n', done)
        if end < 0:
            raise ResponseIncomplete('Chunk length incomplete')
        chunklen = int(data[done:end], 16)
        done = end+2
        if chunklen == 0:
            if data[done:done+2] != b'\r\n':
                raise ResponseIncomplete(
                    'Final CRLF of chunked encoding missing')
            return b''.join(body), data[done+2:]
        body.append(data[done:done+chunklen])
        done += chunklen
        if len(data) < done:
            raise ResponseIncomplete('Chunk incomplete')
        assert data[done:done+2] == b'\r\n'
        done += 2

def receive_one_response(sock, data):
    while True:
        headers = {}
        try:
            return parse_response(data)
        except ResponseIncomplete as e:
            headers = e.headers
            body = e.body
        more = sock.recv(4096)
        if more == b'':
            if headers.get(b'Connection') == b'close':
                return headers, body, b''
            raise ResponseIncomplete('Connection closed', headers, body)
        data += more

class TestWebUI(unittest.TestCase):
    def setUp(self):
        self.ui = FakeUIState()
        self.services = {
            'logger': FakeLogger(),
            'uistate': self.ui,
            }
        self.args = FakeArgs()
        self.args.services = self.services
        task = task_webui.WebUITask(None, self.args)
        task.execute()

    def test_basic_404(self):
        sock = connect_server(self.ui.http_server.port)
        sock.send(b'GET /nosuchurl HTTP/1.1\r\nHost: localhost\r\n\r\n')
        headers, body, rest = receive_one_response(sock, b'')
        self.assertEqual(b'', rest)
        self.assertEqual(b'HTTP/1.1 404 Not found', headers[None])
