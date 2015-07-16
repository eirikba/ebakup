#!/usr/bin/env python3

class UIState(object):
    def __init__(self, args):
        self.args = args
        self._handlers = []
        self._http_handler = None
        self.start_time = None

    def set_http_handler(self, http_handler):
        self._http_handler = http_handler

    def make_http_handler(self):
        h = self._http_handler(self)
        self._handlers.append(h)
        return h

    def set_args(self, args):
        self.args = args

    def set_start_time(self, when):
        self.start_time = when
