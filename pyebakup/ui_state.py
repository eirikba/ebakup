#!/usr/bin/env python3

class UIState(object):
    def __init__(self):
        self._handlers = []
        self._http_handler = None

    def set_http_handler(self, http_handler):
        self._http_handler = http_handler

    def make_http_handler(self):
        h = self._http_handler(self)
        self._handlers.append(h)
        return h
