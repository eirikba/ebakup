#!/usr/bin/env python3

try:
    import mako.template
except ImportError:
    mako = None
import os.path
import re

import http_server

class TemplateError(Exception): pass

class HttpHandler(http_server.NullHandler):
    def __init__(self, state):
        self._state = state
        maindir = os.path.dirname(os.path.dirname(__file__))
        self._datadir = os.path.join(maindir, 'datafiles').encode('utf-8')
        self._handlers = {
            b'/css/main.css': lambda: self._send_static_file(b'text/css'),
            b'/': lambda: self._send_template_file(
                b'text/html', filepath=b'frontpage.html'),
            }

    def handle_request_received(self, method, resource):
        self.method = method
        self.resource = resource

    def handle_response_ready(self, response):
        self.response = response

    def handle_waiting_for_response(self):
        handler = self._handlers.get(self.resource)
        if handler:
            return handler()
        self._send_404()

    def _send_404(self):
        self.response.send_response(b'404 Not found', b'text/plain')
        self.response.send_headers_done()
        self.response.send_body_data(b'Unknown resource: ' + self.resource)
        self.response.send_response_done()

    def _send_static_file(self, contenttype):
        body  = self._get_file_content()
        self._send_static_data(contenttype, body)

    def _get_file_content(self, filepath=None):
        if filepath is None:
            filepath = self.resource
        filepath = [x for x in filepath.split(b'/') if x]
        path = os.path.join(self._datadir, *filepath)
        with open(path, 'rb') as f:
            body = f.read()
        return body

    def _send_static_data(self, contenttype, data):
        self.response.send_response(b'200 OK', contenttype)
        self.response.send_headers_done()
        self.response.send_body_data(data)
        self.response.send_response_done()

    def _send_template_file(self, contenttype, filepath=None):
        if mako is None:
            if contenttype.startswith(b'text/'):
                self._send_static_data(
                    b'text/plain',
                    b'Mako is required for generating this data, '
                    b'and it seems it is not available.\n')
                return
            self._send_404()
            return
        body = self._get_file_content(filepath)
        body = self._expand_template(body)
        self._send_static_data(contenttype, body)

    def _expand_template(self, body):
        template = mako.template.Template(body)
        return template.render(
            bk=TemplateVars(self._state)).encode('utf-8')


class TemplateVars(object):
    def __init__(self, state):
        self._state = state

    @property
    def start_time(self):
        return self._state.start_time

    @property
    def args_command(self):
        return self._state.args.command
