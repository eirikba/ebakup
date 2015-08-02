#!/usr/bin/env python3

import datetime
import unittest

import streamingdatafile

class FakeFileSystem(object):
    def __init__(self):
        self._files = {}

    def get_item_at_path(self, path):
        return FakeFile(self._files[path])

class FakeFile(object):
    def __init__(self, fd):
        self._fd = fd

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        pass

    def lock_for_reading(self):
        pass

    def get_size(self):
        return len(self._fd)

    def get_data_slice(self, start, end):
        return self._fd[start:end]


files = {
    'dbmain':
        b'ebakup database v1\n'
        b'edb-blocksize:4096\n'
        b'edb-blocksum:sha256\n'
        b'checksum:sha256\n' +
        b'\x00' * 3990 +
        b'\xfbT\x16=\xf4\xe9j\x9fG\xdf\xbb!\xe0\xc9\xe9\xaa\xe3/'
        b'\xe9\x8e\xd5\xf5\xe4\xdc\xb1C\xbf\xd6\x03\xf2\xf0\xce',

    'dbbk':
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
        b'\xdb\\oqU\x8eGHmxv\xc9\xdb\x15',

    'dbcontent':
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
        b'\x16\xeb7 \xfb\xc1\x00\x02\xfe\xa2\xf1\x1a\xea',
    }

class TestReadSimpleDatabase(unittest.TestCase):
    def setUp(self):
        tree = FakeFileSystem()
        self.tree = tree
        tree._files[('path', 'to', 'db', 'main')] = files['dbmain']
        tree._files[('path' ,'to', 'db', '2015', '04-03T10:46')] = files['dbbk']
        tree._files[('path', 'to', 'db', 'content')] = files['dbcontent']

    def test_read_main(self):
        reader = streamingdatafile.StreamingReader(
            self.tree, ('path', 'to', 'db', 'main'))
        expected_list = (
            { 'kind':'magic', 'value':b'ebakup database v1'},
            { 'kind':'setting', 'key':b'edb-blocksize', 'value':b'4096'},
            { 'kind':'setting', 'key':b'edb-blocksum', 'value':b'sha256'},
            { 'kind':'setting', 'key':b'checksum', 'value':b'sha256'},
            { 'kind':'sentinel'}
            )
        expected_iter = iter(expected_list)
        for item, expected in zip(reader, expected_iter):
            self.assertNotEqual(expected['kind'], 'sentinel')
            for key, value in expected.items():
                self.assertEqual(value, getattr(item, key), msg='key=' + key)
        self.assertEqual('sentinel', next(expected_iter)['kind'])

    def test_read_backup(self):
        reader = streamingdatafile.StreamingReader(
            self.tree, ('path', 'to', 'db', '2015', '04-03T10:46'))
        expected_list = (
            { 'kind':'magic', 'value':b'ebakup backup data' },
            { 'kind':'setting', 'key':b'edb-blocksize', 'value':b'4096' },
            { 'kind':'setting', 'key':b'edb-blocksum', 'value':b'sha256' },
            { 'kind':'setting',
              'key':b'start', 'value':b'2015-04-03T10:46:06' },
            { 'kind':'setting', 'key':b'end', 'value':b'2015-04-03T10:47:59' },
            { 'kind':'directory', 'dirid':8, 'parent':0, 'name':b'path' },
            { 'kind':'directory', 'dirid':9, 'parent':8, 'name':b'to' },
            { 'kind':'file', 'parent':9, 'name':b'file',
              'cid':b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
                  b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
              'size':7850,
              'mtime_year':2015, 'mtime_second':4366402, 'mtime_ns':765430000 },
            { 'kind':'file', 'parent':0, 'name':b'file',
              'cid':b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                  b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
              'size': 23,
              'mtime_year':2013, 'mtime_second':17488800, 'mtime_ns':0 },
            { 'kind':'sentinel'}
            )
        expected_iter = iter(expected_list)
        for item, expected in zip(reader, expected_iter):
            self.assertNotEqual(expected['kind'], 'sentinel')
            for key, value in expected.items():
                self.assertEqual(value, getattr(item, key), msg='key=' + key)
        self.assertEqual('sentinel', next(expected_iter)['kind'])

    def test_read_content(self):
        reader = streamingdatafile.StreamingReader(
            self.tree, ('path', 'to', 'db', 'content'))
        expected_list = (
            { 'kind':'magic', 'value':b'ebakup content data' },
            { 'kind':'setting', 'key':b'edb-blocksize', 'value':b'4096' },
            { 'kind':'setting', 'key':b'edb-blocksum', 'value':b'sha256' },
            { 'kind':'content',
              'cid':b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y'
                  b'\x00\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
              'checksum':b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y'
                  b'\x00\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
              'first':0x55154078, 'last':0x55216909,
              'updates':() },
            { 'kind':'content',
              'cid':b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                  b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
              'checksum':b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                  b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
              'first':0x55154078, 'last':0x55154078,
              'updates': (
                  { 'kind':'changed',
                    'checksum':b'k\x8c\xba\x8b\x17\x8b\rL\x13\xde\xc9$<\x90\x04'
                        b'\xeb\xc3\x03\xcbJ\xaf\xe93\x0c\x8d\x12^.\x94yS\xae',
                    'first':0x55183045, 'last':0x551bea4b },
                  { 'kind':'restored',
                    'checksum':None,
                    'first':0x551beb3b, 'last':0x55216909 } ) },
            { 'kind':'content',
              'cid':b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
                  b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73",
              'checksum':b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
                  b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98",
              'first':0x5513d6d1, 'last':0x55168fac,
              'updates': (
                  { 'kind':'changed',
                    'checksum':b'\x01\xfa\x04^\x9c\x11\xd5\x8d\xfe\x19]}\xd1(('
                       b'\x0c\x00h\xad0\x13\xa3(\xb5\xe8\xb3\xac\xa3\x9e_\xfbb',
                    'first':0x5517b191, 'last':0x551d1200 }, ) },
            { 'kind':'sentinel'}
            )
        expected_iter = iter(expected_list)
        for item, expected in zip(reader, expected_iter):
            self.assertNotEqual(expected['kind'], 'sentinel')
            for key, value in expected.items():
                if key == 'updates':
                    for itemupd, expectupd in zip(item.updates, value):
                        for upkey, upvalue in expectupd.items():
                            self.assertEqual(
                                upvalue, getattr(itemupd, upkey),
                                msg='key:' + upkey)
                else:
                    self.assertEqual(
                        value, getattr(item, key), msg='key:' + key)
        self.assertEqual('sentinel', next(expected_iter)['kind'])

    def test_read_content_settings_checksum_wrong(self):
        self.tree._files[('path', 'to', 'db', 'content')] = (
            files['dbcontent'][:4070] + b'aa' + files['dbcontent'][4072:])
        reader = streamingdatafile.StreamingReader(
            self.tree, ('path', 'to', 'db', 'content'))
        self.assertRaisesRegex(
            streamingdatafile.InvalidDataError, 'Block checksum failed at 0',
            next, reader)

    def test_read_content_first_data_checksum_wrong(self):
        self.tree._files[('path', 'to', 'db', 'content')] = (
            files['dbcontent'][:8170] + b'aa' + files['dbcontent'][8172:])
        reader = streamingdatafile.StreamingReader(
            self.tree, ('path', 'to', 'db', 'content'))
        for i in range(3):
            item = next(reader)
            self.assertIn(item.kind, ('magic', 'setting'))
        self.assertRaisesRegex(
            streamingdatafile.InvalidDataError, 'Block checksum failed at 4096',
            next, reader)
