#!/usr/bin/env python3

import errno
import os
import re
import select
import socket

class ParseFailedError(Exception): pass

class NullHandler(object):
    '''Implements the request handler interface with empty methods.

    A single handler instance will receive exactly one call to each of
    these methods, in order:
    - handle_request_received()
    - handle_headers_complete()
    - handle_request_complete()
    - handle_waiting_for_response()

    In addition, handle_response_ready() will be called exactly once,
    at any time before handle_waiting_for_response().

    There is one exception: handle_request_aborted() may be called at
    any time. If that happens, no further methods will be called on
    that object.
    '''
    def handle_raw_data_received(self, data):
        '''Provides the raw data of the request.

        This method is called after the methods providing the parsed
        data, such as handle_request_received() and
        handle_header_received().

        But it is called after methods marking state changes, such as
        handle_headers_complete() and handle_request_complete().
        '''

    def handle_request_received(self, method, resource):
        '''Called once at the start of a request.

        This method is called when the request line (the first line of
        the full request) is parsed. 'method' is the method of the
        request (e.g. b'GET') and 'request' is the resource requested
        (e.g. b'/').
        '''

    def handle_header_received(self, header, value):
        '''Called whenever a new header is received.

        This will only be called after handle_request_received() and
        before handle_headers_complete().
        '''

    def handle_headers_complete(self):
        '''Called once when all the request's headers have been received.
        '''

    def handle_body_data(self, data):
        '''Called whenever more data is received for the body of the request.

        This will only be called after handle_headers_complete() and
        before handle_request_complete().
        '''

    def handle_request_complete(self):
        '''Called once when the whole request has been received.
        '''

    def handle_response_ready(self, response):
        '''Called once when the connection is ready to send the response.

        This may happen much later than handle_request_complete() if
        the connection is pipelining requests and it takes a long time
        to send an earlier response. Conversely it may happen long
        before handle_request_complete(), e.g. if this is the first
        request on a connection and it takes a long time to receive
        the complete request.

        The 'response' object provides the methods to construct the
        response. It remains valid to use until the response is
        completed or handle_request_aborted() is called.

        The 'response' object provides at least:
        - send_response(response, content_type)
        - send_header(header, value)
        - send_body_size(size)
        - send_headers_done()
        - send_body_data(data)
        - send_response_done()

        send_response(), send_headers_done() and send_response_done()
        MUST be called once each, in the given order. send_header()
        and send_body_size() can only be called between
        send_response() and send_headers_done(). send_body_data() can
        only be called between send_headers_done() and
        send_response_done().

        Each of the send_* methods sends some data to the client, but
        the data may be buffered rather than sent immediately. It is
        guaranteed that send_headers_done() will cause all the headers
        to be sent and send_response_done() will cause the whole
        response to be sent.

        The 'response' object provides some methods to manage the
        actual sending:
        - send_buffered_data()

        send_buffered_data() makes sure that all the data that has
        been buffered so far will be sent to the client. This is
        essentially the same thing that send_headers_done() and
        send_response_done() does to ensure that the headers and
        response is sent.
        '''

    def handle_waiting_for_response(self):
        '''Called once when the whole request has been received and the
        connection is ready to send the response.

        This method will be called immediately after
        handle_request_complete() or handle_response_ready(),
        whichever is called last.
        '''

    def handle_wakeup(self, what):
        '''Called once for each handler that has an unfinished response
        whenever the server's wakeup() method is called.

        'what' is the value passed to wakeup().
        '''

    def handle_request_aborted(self):
        '''Called if the request was aborted.

        Once this is called, no more methods will be called on this
        object. And there is no need to do anything more about the
        response.
        '''

    def handle_data_sent(self, pending_size):
        '''Called whenever some of the response data has been successfully
        sent to the network.

        'pending_size' is the number of octets of response data still
        waiting to be sent.
        '''

class HttpServer(object):
    def __init__(self, handler, port=None, port_low=None, port_high=None):
        if port is not None:
            assert port_low is None
            assert port_high is None
            self._port_low = port
            self._port_high = port
        else:
            assert port_low is not None
            assert port_high is not None
            self._port_low = port_low
            self._port_high = port_high
        self._handler = handler
        self.port = None

    def stop(self):
        self.running = False

    def serve_forever(self):
        self.running = True
        self._sock_listen = socket.socket()
        self._sock_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bound = False
        port = self._port_low
        while not bound:
            try:
                self._sock_listen.bind(('127.0.0.1', port))
                bound = True
            except OSError as e:
                if e.errno != errno.EADDRINUSE:
                    raise
                port += 1
                if port > self._port_high:
                    raise
        self._sock_listen.settimeout(0)
        self._sock_listen.listen(10)
        self.port = port
        self._sockets = {}
        self._poller = select.poll()
        self._poller.register(self._sock_listen)
        self._pipe_wakeup = os.pipe()
        self._poller.register(self._pipe_wakeup[0])
        while self.running:
            for pollfd in self._poller.poll():
                if pollfd[0] == self._sock_listen.fileno():
                    self._handle_connect()
                elif pollfd[0] == self._pipe_wakeup[0]:
                    self._handle_wakeup()
                else:
                    self._handle_communicate(self._sockets[pollfd[0]])

    def _handle_connect(self):
        try:
            sock, remote_addr = self._sock_listen.accept()
        except BlockingIOError:
            return
        sock.settimeout(0)
        self._poller.register(sock)
        self._sockets[sock.fileno()] = SocketData(
            socket=sock, server=self, handler=self._handler)

    def _handle_communicate(self, socket):
        socket.write_pending_data()
        socket.read_data()

    def _handle_wakeup(self):
        what = b''
        while True:
            what += os.read(self._pipe_wakeup[0], 4096)
            events = what.split(b'\n')
            for event in events[1:]:
                for sock in self._sockets.values():
                    sock.handle_wakeup(event)
            if events[-1] == b'':
                return
            what = events[-1]

    def wakeup(self, what):
        assert b'\n' not in what
        os.write(self._pipe_wakeup[1], what + b'\n')

    def register_for_write(self, socket):
        self._poller.modify(socket.fileno, select.POLLIN | select.POLLOUT)

    def unregister_for_write(self, socket):
        self._poller.modify(socket.fileno, select.POLLIN)

    def handle_socket_closed(self, socket):
        self._poller.unregister(socket.fileno)
        del self._sockets[socket.fileno]


class SocketData(object):
    def __init__(self, socket, server, handler):
        self.socket = socket
        self.server = server
        self.fileno = socket.fileno()
        self.pending_data = b''
        self.handler = handler
        self.requests = []

    def __str__(self):
        return '<SocketData ' + str(self.socket) + '>'

    def write_data(self, data):
        #print('write', self.socket, data)
        had_pending_data = self.has_pending_data
        self.pending_data += data
        self.write_pending_data()
        if not had_pending_data and self.has_pending_data:
            self.server.register_for_write(self)

    @property
    def has_pending_data(self):
        return bool(self.pending_data)

    def write_pending_data(self):
        if self.pending_data:
            try:
                wrote = self.socket.send(self.pending_data)
            except BlockingIOError:
                return
            except BrokenPipeError:
                self._handle_socket_closed()
                return
            self.pending_data = self.pending_data[wrote:]
            if not self.pending_data:
                self.server.unregister_for_write(self)

    def read_data(self):
        try:
            data = self.socket.recv(4096 * 64)
        except BlockingIOError:
            return False
        if data == b'':
            self._handle_socket_closed()
            self.socket.close()
            self.server.handle_socket_closed(self)
        else:
            self._handle_new_data(data)
        return True

    def _handle_socket_closed(self):
        requests = self.requests
        self.requests = None
        for req in requests:
            req.handle_socket_closed()

    def _handle_new_data(self, data):
        while data:
            if not self.requests:
                req = HttpHandler(self, self.handler())
                req.handle_response_ready()
                self.requests.append(req)
            req = self.requests[-1]
            data = req.handle_new_data(data)
            if data and self.requests:
                self.requests.append(HttpHandler(self, self.handler()))

    def handle_response_complete(self, handler):
        assert handler == self.requests[0]
        self.requests = self.requests[1:]
        if self.requests:
            self.requests[0].handle_response_ready()

    def handle_wakeup(self, what):
        if not self.requests:
            return
        self.requests[0].handle_wakeup(what)

class HttpHandler(object):
    def __init__(self, socket, handler):
        self.socket = socket
        self.handler = handler
        self.received_data = b''
        self.state = 0 # 0:Start, 1:Headers, 2:Body, 3:End
        self.response = None

    def write_data(self, data):
        self.socket.write_data(data)

    def handle_response_complete(self, response):
        assert response == self.response
        self.socket.handle_response_complete(self)

    def handle_socket_closed(self):
        self.handler.handle_request_aborted()

    def handle_new_data(self, data):
        data = self.received_data + data
        self.received_data = b''
        start = 0
        done = start
        if self.state == 0:
            done = self._parse_request_line(data, done)
        prev_state = self.state
        prev = done - 1
        while self.state == 1 and prev != done:
            prev = done
            done = self._parse_header_line(data, done)
        if self.state == 2 and start < done:
            self.handler.handle_raw_data_received(data[start:done])
            start = done
        if prev_state < 2 and self.state >= 2:
            self.handler.handle_headers_complete()
        prev_state = self.state
        if self.state == 2:
            done = self._parse_body_data(data, done)
        if done > start:
            self.handler.handle_raw_data_received(data[start:done])
        if prev_state < 3 and self.state >= 3:
            self.handler.handle_request_complete()
            if self.response:
                self.handler.handle_waiting_for_response()
        if self.state >= 3:
            return data[done:]
        self.received_data = data[done:]
        return None

    def handle_response_ready(self):
        if self.response:
            return
        self.response = HttpResponse(self)
        self.handler.handle_response_ready(self.response)
        if self.state >= 3:
            self.handler.handle_waiting_for_response()

    re_request_line = re.compile(
        br'([a-zA-Z0-9]+) (.*) HTTP/([0-9]+)\.([0-9]+)\r?\n')
    def _parse_request_line(self, data, done):
        while data.startswith(b'\r\n', done):
            done += 2
        while data.startswith(b'\n', done):
            done += 1
        end = data.find(b'\n', done)
        if end < 0:
            return done
        match = self.re_request_line.match(data, done, end+1)
        if not match:
            raise ParseFailedError(
                'Failed to parse request line: ' + repr(data[done:end+1]))
        method = match.group(1)
        resource = match.group(2)
        http_maj = match.group(3)
        http_min = match.group(4)
        if http_maj != b'1' or http_min not in (b'0', b'1'):
            raise ParseFailedError(
                'Unknown HTTP version: ' + str(http_maj) + '.' + str(http_min))
        self.state = 1
        self.handler.handle_request_received(method, resource)
        return match.end()

    re_header_line = re.compile(br'([-a-zA-Z0-9]+): *([^\r\n]*)\r?\n')
    def _parse_header_line(self, data, done):
        if data.startswith(b'\r\n', done):
            self.state = 2
            return done + 2
        elif data.startswith(b'\n', done):
            self.state = 2
            return done + 1
        match = self.re_header_line.match(data, done)
        if not match:
            if len(data) < done + 2:
                return done
            line_end = data.find(b'\n', done)
            if line_end < 0:
                return done
            raise ParseFailedError(
                'Failed to parse header line: ' + repr(data[done:line_end+1]))
        header = match.group(1)
        value = match.group(2)
        self._handle_header(header, value)
        self.handler.handle_header_received(header, value)
        return match.end()

    def _handle_header(self, header, value):
        pass

    def _parse_body_data(self, data, done):
        self.state = 3
        return done

    def handle_wakeup(self, what):
        self.handler.handle_wakeup(what)


class HttpResponse(object):
    def __init__(self, http):
        self._http = http
        self._responsedata = []
        self._state = 0 # 0: start, 1: headers, 2: body, 3: done
        self._close_connection = False
        self._response_body_size = None
        self._response_body_sent = 0
        self._te_chunked = False

    def send_response(self, response, contenttype=None):
        assert self._state == 0
        assert not self._responsedata
        self._responsedata.append(b'HTTP/1.1 ' + response + b'\r\n')
        self._state = 1
        if contenttype is not None:
            self.send_header(b'Content-Type', contenttype)

    def send_header(self, header, value):
        assert self._state == 1
        self._responsedata.append(header + b': ' + value + b'\r\n')

    def set_response_body_size(self, size):
        assert self._state == 1
        self._response_body_size = size

    def send_headers_done(self):
        assert self._state == 1
        if self._response_body_size is not None:
            self.send_header(
                b'Content-Size', str(self._response_body_size).encode('utf-8'))
        else:
            self._te_chunked = True
            self.send_header(b'Transfer-Encoding', b'chunked')
        if self._close_connection:
            self.send_header(b'Connection', b'close')
        self._responsedata.append(b'\r\n')
        self.send_buffered_data()
        self._state = 2

    def send_buffered_data(self):
        if not self._responsedata:
            return
        chunked = self._state >= 2 and self._te_chunked
        if chunked:
            self._responsedata.append(b'\r\n')
        data = b''.join(self._responsedata)
        self._responsedata = []
        if chunked:
            self._http.write_data(
                hex(len(data)-2)[2:].encode('utf-8') +
                b'\r\n')
        self._http.write_data(data)

    def send_body_data(self, data):
        assert self._state == 2
        self._response_body_sent += len(data)
        if self._response_body_size is not None:
            assert self._response_body_sent <= self._response_body_size
        self._responsedata.append(data)

    def send_response_done(self):
        assert self._state == 2
        self._state = 3
        self.send_buffered_data()
        if self._te_chunked:
            self._http.write_data(b'0\r\n\r\n')
        else:
            assert self._response_body_sent == self._response_body_size
        if self._close_connection:
            self._http.close_connection()
        self._http.handle_response_complete(self)
