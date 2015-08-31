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
    elif which == 'content-1':
        return (
            b'ebakup content data\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n' +
            b'\x00' * 4005 +
            b'`{\xafg\x156E\x99*\x05|\x14\xf6fg\xd3\xc4\xde\x80'
            b'\xa5g\xf1\xa0\xf8\xc28\xe4J9\xd5\xa2-'
            b'\xdd\x20\x20'
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^'
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\x09\x69\x21\x55' # 2015-04-05 16:55:37
            b'\xdd\x20\x20'
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde'
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\xa1'
            b'k\x8c\xba\x8b\x17\x8b\rL\x13\xde\xc9$<\x90\x04\xeb\xc3'
            b'\x03\xcbJ\xaf\xe93\x0c\x8d\x12^.\x94yS\xae'
            b'\x45\x30\x18\x55' # 2015-03-29 17:03:01
            b'\x4b\xea\x1b\x55' # 2015-04-01 12:53:31
            b'\xa0'
            b'\x3b\xeb\x1b\x55' # 2015-04-01 12:57:31
            b'\x09\x69\x21\x55' # 2015-04-05 16:55:37
            b'\xdd\x22\x20'
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73"
            b'\xd1\xd6\x13\x55' # 2015-03-26 09:52:17
            b'\xac\x8f\x16\x55' # 2015-03-28 11:25:32
            b'\xa1'
            b'\x01\xfa\x04^\x9c\x11\xd5\x8d\xfe\x19]}\xd1((\x0c'
            b'\x00h\xad0\x13\xa3(\xb5\xe8\xb3\xac\xa3\x9e_\xfbb'
            b'\x91\xb1\x17\x55' # 2015-03-29 08:02:25
            b'\x00\x12\x1d\x55' # 2015-04-02 09:55:12
            + b'\x00' * 3842 +
            b'\x909\xee+%\x92;A\xa3\xed\xb1\xd6\x98\x84\xfdB7\x93,'
            b'\x16\xeb7 \xfb\xc1\x00\x02\xfe\xa2\xf1\x1a\xea')
    elif which == 'backup-1':
        return (
            b'ebakup backup data\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'start:2015-04-03T10:46:06\n'
            b'end:2015-04-03T10:47:59\n' +
            b'\x00' * 3956 +
            b'\xf5\xf8\xa5\xcf\x94\xd7\x97\xd6j\xab8\xf1\xc8-&\xd50'
            b':\x9f\x8c4H\xf2\xf4\x1d\x04\xab\x8b]:\xe9\xfe'
            b'\x90\x08\x00\x04path'
            b'\x90\x09\x08\x02to'
            b'\x91\x09\x04file\x20'
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^'
            b'\xaa\x3d\xdf\x07\x42\xa0\x42\x30\x23\x7e\xb6'
            # ^ size: 7850, mtime: 2015-02-20 12:53:22.76543
            b'\x91\x00\x04file\x20'
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde'
            b'\x17\xdd\x07\xa0\xdb\x0a\x80\x00\x00\x00' +
            # ^ size: 23, mtime: 2013-07-22 10:00:00.0
            b'\x00' * 3949 +
            b'H\x15XVH\x9aJ\x019\x0e\xe8\x93%\xa7\xa4A\xaf*'
            b'\xdb\\oqU\x8eGHmxv\xc9\xdb\x15')


class FileData(object):
    def __init__(self, path, content, mtime, mtime_ns):
        self.path = path
        self.content = content
        self.mtime = mtime
        self.mtime_ns = mtime_ns
