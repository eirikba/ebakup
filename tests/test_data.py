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

def dbfiledata(which):
    if which == 'main-1':
        return (
            b'ebakup database v1\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'checksum:sha256\n' +
            b'\x00' * 3990 +
            b'\xfbT\x16=\xf4\xe9j\x9fG\xdf\xbb!\xe0\xc9\xe9\xaa\xe3/'
            b'\xe9\x8e\xd5\xf5\xe4\xdc\xb1C\xbf\xd6\x03\xf2\xf0\xce')


class FileData(object):
    def __init__(self, path, content, mtime, mtime_ns):
        self.path = path
        self.content = content
        self.mtime = mtime
        self.mtime_ns = mtime_ns
