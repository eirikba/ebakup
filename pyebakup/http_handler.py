#!/usr/bin/env python3

import http_server

class HttpHandler(http_server.NullHandler):
    def __init__(self, state):
        self._state = state

    def handle_request_received(self, method, resource):
        self.method = method
        self.resource = resource

    def handle_response_ready(self, response):
        self.response = response

    def handle_waiting_for_response(self):
        self._send_404()

    def _send_404(self):
        self.response.send_response(b'404 Not found', b'text/plain')
        self.response.send_headers_done()
        self.response.send_body_data(b'Unknown resource: ' + self.resource)
        self.response.send_response_done()
