#!/usr/bin/env python3

import os.path
import re

import http_server


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
        filepath = [x for x in self.resource.split(b'/') if x]
        path = os.path.join(self._datadir, *filepath)
        with open(path, 'rb') as f:
            body = f.read()
        self._send_static_data(contenttype, body)

    def _send_static_data(self, contenttype, data):
        self.response.send_response(b'200 OK', contenttype)
        self.response.send_headers_done()
        self.response.send_body_data(data)
        self.response.send_response_done()

    def _send_template_file(self, contenttype, filepath=None):
        if filepath is None:
            filepath = self.resource
        filepath = [x for x in filepath.split(b'/') if x]
        path = os.path.join(self._datadir, *filepath)
        with open(path, 'rb') as f:
            body = f.read()
        body = self._expand_template(body)
        self._send_static_data(contenttype, body)

    def _expand_template(self, body):
        parts = re.split(br'\${([a-z_]*)(:[^}]*)?}', body)
        new = []
        done = 0
        while done + 2 < len(parts):
            new.append(parts[done])
            name = parts[done+1]
            args = parts[done+2]
            if args:
                args = args[1:]
            done += 3
            new.append(self._get_template_value(name, args))
        while done < len(parts):
            new.append(parts[done])
            done += 1
        return b''.join(new)

    def _get_template_value(self, name, args):
        try:
            handler = getattr(self, 'tmpl_' + name.decode('utf-8'))
        except AttributeError:
            handler = None
        if handler:
            return handler(args)
        if name == b'' and args is None:
            return b'${'
        if args is not None:
            argstr = b':' + args
        else:
            argstr = b''
        return b'{UNKNOWN COMMAND:' + name + argstr + b'}'

    def tmpl_start_time(self, args):
        return str(self._state.start_time).encode('utf-8')

    def tmpl_args_command(self, args):
        return self._state.args.command.encode('utf-8')
