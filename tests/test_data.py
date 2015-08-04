#!/usr/bin/env python3

import datetime

_files = None
def files():
    global _files
    if not _files:
        _files = [
            FileData(
                ('path', 'to', 'file'),
                b'Simple file content',
                datetime.datetime(2014, 1, 25, 3, 16, 53), 947291049),
            FileData(
                ('empty',),
                b'',
                datetime.datetime(2014, 12, 1, 8, 10, 16), 883117062),
            ]
    return _files

class FileData(object):
    def __init__(self, path, content, mtime, mtime_ns):
        self.path = path
        self.content = content
        self.mtime = mtime
        self.mtime_ns = mtime_ns
