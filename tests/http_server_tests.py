#!/usr/bin/env python3

import socket
import threading
import time
import unittest

import http_server

class Handler(object):
    def __init__(self):
        self.raw_data = b''
        self.method = None
        self.resource = None
        self.headers = {}
        self.body = b''
        self.headers_complete = False
        self.request_complete = False
        self.response = None

    def handle_raw_data_received(self, data):
        self.raw_data += data

    def handle_request_received(self, method, resource):
        assert self.method is None
        self.method = method
        self.resource = resource
        assert self.method is not None

    def handle_header_received(self, header, value):
        assert self.method is not None
        assert not self.headers_complete
        if header not in self.headers:
            self.headers[header] = value
            return
        oldvalue = self.headers[header]
        if isinstance(oldvalue, list):
            oldvalue.append(value)
            return
        self.headers[header] = [oldvalue, value]

    def handle_headers_complete(self):
        assert self.method is not None
        assert not self.headers_complete
        self.headers_complete = True

    def handle_body_data(self, data):
        assert self.headers_complete
        assert not self.request_complete
        self.body += data

    def handle_request_complete(self):
        assert self.headers_complete
        assert not self.request_complete
        self.request_complete = True

    def handle_response_ready(self, response):
        self.response = response

    def handle_waiting_for_response(self):
        assert self.response
        assert self.request_complete
        if self.resource == b'/simple':
            self._send_simple_resource()
            return
        if self.resource.startswith(b'/echo/'):
            self._send_echo_resource()
            return
        if self.resource == b'/async':
            self._send_async_resource()
            return
        self._send_404()

    def _send_simple_resource(self):
        self.response.send_response(b'200 OK', b'text/plain')
        self.response.send_headers_done()
        self.response.send_body_data(b'simple resource')
        self.response.send_response_done()

    def _send_echo_resource(self):
        self.response.send_response(b'200 OK', b'text/plain')
        self.response.send_headers_done()
        self.response.send_body_data(b'echo: ')
        self.response.send_body_data(self.resource[6:])
        self.response.send_response_done()

    def _send_async_resource(self):
        self.async_resource_body = None
        self.response.send_response(b'200 OK', b'text/plain')
        self.response.send_headers_done()

    def set_async_body(self, body):
        # Called on non-http-server thread!
        # Something must call the server's wakeup() method.
        self.async_resource_body = body

    def _send_404(self):
        self.response.send_response(b'404 Not found', b'text/plain')
        self.response.send_headers_done()
        self.response.send_body_data(b'Failed to find ' + self.resource)
        self.response.send_response_done()

    def handle_wakeup(self, what):
        if self.async_resource_body:
            self.response.send_body_data(self.async_resource_body)
            self.async_resource_body = None
            self.response.send_response_done()

    def handle_request_aborted(self):
        self.request_aborted = True
        raise NotImplementedError()

    def handle_data_sent(self, pending_size):
        raise NotTestedError()


def connect_server(port):
    sock = socket.socket()
    sock.settimeout(0.1)
    sock.connect(('127.0.0.1', port))
    return sock

def parse_response(data):
    header_end = data.find(b'\r\n\r\n')
    assert header_end > 0
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

def parse_chunked_encoding(data):
    body = []
    done = 0
    while True:
        end = data.find(b'\r\n', done)
        chunklen = int(data[done:end], 16)
        done = end+2
        if chunklen == 0:
            assert data[done:done+2] == b'\r\n'
            return b''.join(body), data[done+2:]
        body.append(data[done:done+chunklen])
        done += chunklen
        assert data[done:done+2] == b'\r\n'
        done += 2

class TestBasics(unittest.TestCase):
    def make_handler(self):
        handler = Handler()
        self.handlers.append(handler)
        return handler

    def setUp(self):
        self.handlers = []
        self.server = http_server.HttpServer(
            handler=self.make_handler, port_low=20000, port_high=21000)
        self.server_thread = threading.Thread(
            target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        while self.server.port is None:
            if not self.server_thread.is_alive():
                self.fail('Server failed to start')
            time.sleep(0.0001)

    def test_simple_request(self):
        sock = connect_server(self.server.port)
        sock.send(b'GET /simple HTTP/1.1\r\nHost: localhost\r\n\r\n')
        data = b''
        while not data.endswith(b'\r\n0\r\n\r\n'):
            data += sock.recv(4096)
        self.server.stop()
        sock.close()
        self.assertEqual(b'HTTP/1.1 200 OK\r\n', data[:17])
        self.assertEqual(1, len(self.handlers))
        handler = self.handlers[0]
        self.assertEqual({b'Host':b'localhost'}, handler.headers)
        self.assertEqual(
            b'\r\n\r\nf\r\nsimple resource\r\n0\r\n\r\n', data[-29:])
        headers, body, rest = parse_response(data)
        self.assertEqual(b'', rest)
        self.assertEqual(b'simple resource', body)

    def test_pipelined_requests(self):
        sock = connect_server(self.server.port)
        sock.send(
            b'GET /simple HTTP/1.1\r\nHost: localhost\r\n\r\n'
            b'GET /echo/second_request HTTP/1.1\r\nHost: localhost\r\n\r\n')
        data = b''
        while not b'second_request' in data:
            data += sock.recv(4096)
        sock.send(
            b'GET /echo/third_request HTTP/1.1\r\nHost: localhost\r\n\r\n')
        while not b'third_request' in data:
            data += sock.recv(4096)
        while not data.endswith(b'\r\n0\r\n\r\n'):
            data += sock.recv(4096)
        self.server.stop()
        sock.close()
        headers, body, rest = parse_response(data)
        self.assertEqual(b'simple resource', body)
        headers, body, rest = parse_response(rest)
        self.assertEqual(b'echo: second_request', body)
        headers, body, rest = parse_response(rest)
        self.assertEqual(b'', rest)
        self.assertEqual(b'echo: third_request', body)

    def test_parallel_pipelined_requests(self):
        sock = connect_server(self.server.port)
        sock2 = connect_server(self.server.port)
        sock.send(
            b'GET /simple HTTP/1.1\r\nHost: localhost\r\n\r\n'
            b'GET /echo/second_request HTTP/1.1\r\nHost: localhost\r\n\r\n')
        sock2.send(
            b'GET /echo/parallel_one HTTP/1.1\r\nHost: localhost\r\n\r\n')
        data = b''
        data2 = b''
        while not b'parallel_one' in data2:
            data2 += sock2.recv(4096)
        while not b'second_request' in data:
            data += sock.recv(4096)
        sock.send(
            b'GET /echo/third_request HTTP/1.1\r\nHost: localhost\r\n\r\n')
        sock2.send(
            b'GET /echo/parallel_two HTTP/1.1\r\nHost: localhost\r\n\r\n')
        while not b'third_request' in data:
            data += sock.recv(4096)
        while not b'parallel_two' in data2:
            data2 += sock2.recv(4096)
        while not data.endswith(b'\r\n0\r\n\r\n'):
            data += sock.recv(4096)
        while not data2.endswith(b'\r\n0\r\n\r\n'):
            data2 += sock.recv(4096)
        self.server.stop()
        sock.close()
        sock2.close()
        headers, body, rest = parse_response(data)
        self.assertEqual(b'simple resource', body)
        headers, body, rest = parse_response(rest)
        self.assertEqual(b'echo: second_request', body)
        headers, body, rest = parse_response(rest)
        self.assertEqual(b'', rest)
        self.assertEqual(b'echo: third_request', body)
        headers, body, rest = parse_response(data2)
        self.assertEqual(b'echo: parallel_one', body)
        headers, body, rest = parse_response(rest)
        self.assertEqual(b'', rest)
        self.assertEqual(b'echo: parallel_two', body)

    def test_wakeup(self):
        sock = connect_server(self.server.port)
        sock.send(b'GET /async HTTP/1.1\r\nHost: localhost\r\n\r\n')
        data = b''
        while not b'\r\n\r\n' in data:
            data += sock.recv(4096)
        self.assertRaises(socket.timeout, sock.recv, 4096)
        self.assertNotIn(b'async-data', data)
        self.assertEqual(1, len(self.handlers))
        handler = self.handlers[0]
        handler.set_async_body(b'async request: async-data')
        self.server.wakeup(b'async')
        while not data.endswith(b'\r\n0\r\n\r\n'):
            data += sock.recv(4096)
        self.server.stop()
        sock.close()
        self.assertEqual(b'HTTP/1.1 200 OK\r\n', data[:17])
        self.assertIn(b'async-data', data)
        self.assertEqual(1, len(self.handlers))
        self.assertEqual({b'Host':b'localhost'}, handler.headers)
        headers, body, rest = parse_response(data)
        self.assertEqual(b'', rest)
        self.assertEqual(b'async request: async-data', body)
