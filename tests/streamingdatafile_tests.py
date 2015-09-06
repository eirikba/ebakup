#!/usr/bin/env python3

import datetime
import unittest

import testdata
from streamingdatafile import (
    Item, StreamingReader, StreamingWriter, InvalidDataError)

class FakeFileSystem(object):
    def __init__(self):
        self._files = {}
        self._modified = set()

    def get_item_at_path(self, path):
        return FakeFile(self, path)

    def create_regular_file(self, path):
        if path in self._files:
            raise FileExistsError("File exists: '/" + '/'.join(path) + "'")
        self._modified.add(path)
        return FakeFileWriter(self, path)

    def rename_and_overwrite(self, source, target):
        self._modified.add(source)
        self._modified.add(target)
        self._files[target] = self._files[source]
        del self._files[source]

class FakeFile(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        pass

    def lock_for_reading(self):
        pass

    def get_size(self):
        return len(self._tree._files[self._path])

    def get_data_slice(self, start, end):
        return self._tree._files[self._path][start:end]

class FakeFileWriter(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        self._write_locked = False
        self._tree._files[self._path] = b''

    def lock_for_writing(self):
        assert not self._write_locked
        self._write_locked = True

    def drop_all_cached_data(self):
        pass

    def get_size(self):
        return len(self._tree._files[self._path])

    def write_data_slice(self, start, data):
        # This is for a STREAMING writer, remember?
        assert start == self.get_size()
        # And we really should have locked the file
        assert self._write_locked is True
        self._tree._modified.add(self._path)
        self._tree._files[self._path] += data
        return len(self._tree._files[self._path])

    def close(self):
        assert self._path is not None
        self._path = None
        self._write_locked = False


class TestReadSimpleDatabase(unittest.TestCase):
    def setUp(self):
        tree = FakeFileSystem()
        self.tree = tree
        tree._files[('path', 'to', 'db', 'main')] = (
            testdata.dbfiledata('main-1'))
        tree._files[('path', 'to', 'db', '2015', '04-03T10:46')] = (
            testdata.dbfiledata('backup-1'))
        tree._files[('path', 'to', 'db', 'content')] = (
            testdata.dbfiledata('content-1'))

    def test_read_main(self):
        reader = StreamingReader(self.tree, ('path', 'to', 'db', 'main'))
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
        reader = StreamingReader(
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
        reader = StreamingReader(self.tree, ('path', 'to', 'db', 'content'))
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
                    self.assertEqual(len(item.updates), len(value))
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
        dbcontent = testdata.dbfiledata('content-1')
        self.tree._files[('path', 'to', 'db', 'content')] = (
            dbcontent[:4070] + b'aa' + dbcontent[4072:])
        reader = StreamingReader(self.tree, ('path', 'to', 'db', 'content'))
        self.assertRaisesRegex(
            InvalidDataError, 'Block checksum failed at 0',
            next, reader)

    def test_read_content_first_data_checksum_wrong(self):
        dbcontent = testdata.dbfiledata('content-1')
        self.tree._files[('path', 'to', 'db', 'content')] = (
            dbcontent[:8170] + b'aa' + dbcontent[8172:])
        reader = StreamingReader(self.tree, ('path', 'to', 'db', 'content'))
        for i in range(3):
            item = next(reader)
            self.assertIn(item.kind, ('magic', 'setting'))
        self.assertRaisesRegex(
            InvalidDataError, 'Block checksum failed at 4096',
            next, reader)

class TestWriteFiles(unittest.TestCase):

    def assertReaderItemsEqual(self, item_data, reader):
        count = 0
        for expected in item_data:
            item = next(reader)
            count += 1
            for key, value in expected.items():
                self.assertEqual(value, getattr(item, key), msg='key=' + key)
        self.assertEqual(len(item_data), count)
        self.assertRaises(StopIteration, next, reader)

    def test_create_empty_main_file(self):
        item_data = [
            { 'kind': 'magic', 'value':b'ebakup database v1' },
            { 'kind': 'setting', 'key':b'edb-blocksize', 'value':b'4096' },
            { 'kind': 'setting', 'key':b'edb-blocksum', 'value':b'sha256' },
        ]
        tree = FakeFileSystem()
        writer = StreamingWriter.create(tree, ('path', 'to', 'new', 'dbfile'))
        for data in item_data:
            item = Item(data['kind'])
            for key, value in data.items():
                if key != 'kind':
                    setattr(item, key, value)
            writer.write(item)
        writer.close()
        self.assertCountEqual(
            set((
                ('path', 'to', 'new', 'dbfile'),
                ('path', 'to', 'new', 'dbfile.new'))),
            tree._modified)
        reader = StreamingReader(tree, ('path', 'to', 'new', 'dbfile'))
        count = 0
        for item, expected in zip(reader, item_data):
            count += 1
            for key, value in expected.items():
                self.assertEqual(value, getattr(item, key), msg='key=' + key)
        self.assertEqual(len(item_data), count)
        self.assertEqual(
            4096, len(tree._files[('path', 'to', 'new', 'dbfile')]))
        self.assertNotIn(('path', 'to', 'new', 'dbfile.new'), tree._files)

    def test_simple_content_file(self):
        item_data = (
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
            )
        tree = FakeFileSystem()
        writer = StreamingWriter.create(tree, ('path', 'to', 'new', 'content'))
        for data in item_data:
            item = Item(data['kind'])
            for key, value in data.items():
                if key == 'kind':
                    pass
                elif key == 'updates':
                    updates = []
                    for upd in value:
                        upditem = Item(upd['kind'])
                        upditem.checksum = upd['checksum']
                        upditem.first = upd['first']
                        upditem.last = upd['last']
                        updates.append(upditem)
                    item.updates = updates
                else:
                    setattr(item, key, value)
            writer.write(item)
        writer.close()
        self.assertCountEqual(
            set((
                ('path', 'to', 'new', 'content'),
                ('path', 'to', 'new', 'content.new'))),
            tree._modified)
        reader = StreamingReader(tree, ('path', 'to', 'new', 'content'))
        count = 0
        for item, expected in zip(reader, item_data):
            count += 1
            for key, value in expected.items():
                if key == 'updates':
                    self.assertEqual(len(item.updates), len(value))
                    for itemupd, expectupd in zip(item.updates, value):
                        for upkey, upvalue in expectupd.items():
                            self.assertEqual(
                                upvalue, getattr(itemupd, upkey),
                                msg='key:' + upkey)
                else:
                    self.assertEqual(
                        value, getattr(item, key), msg='key:' + key)
        self.assertEqual(len(item_data), count)
        self.assertEqual(
            8192, len(tree._files[('path', 'to', 'new', 'content')]))
        self.assertNotIn(('path', 'to', 'new', 'content.new'), tree._files)

    def test_simple_backup_file(self):
        item_data = (
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
            )
        tree = FakeFileSystem()
        writer = StreamingWriter.create(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:46'))
        for data in item_data:
            item = Item(data['kind'])
            for key, value in data.items():
                if key == 'kind':
                    pass
                else:
                    setattr(item, key, value)
            writer.write(item)
        writer.close()
        self.assertCountEqual(
            set((
                ('path', 'to', 'bk', '2015', '04-03T10:46'),
                ('path', 'to', 'bk', '2015', '04-03T10:46.new'))),
            tree._modified)
        reader = StreamingReader(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:46'))
        count = 0
        for item, expected in zip(reader, item_data):
            count += 1
            for key, value in expected.items():
                self.assertEqual(value, getattr(item, key), msg='key=' + key)
        self.assertEqual(len(item_data), count)
        self.assertEqual(
            8192, len(tree._files[('path', 'to', 'bk', '2015', '04-03T10:46')]))
        self.assertNotIn(
            ('path', 'to', 'bk', '2015', '04-03T10:46.new'), tree._files)

    def test_write_multi_block_file(self):
        item_data = [
            { 'kind':'magic', 'value':b'ebakup backup data' },
            { 'kind':'setting', 'key':b'edb-blocksize', 'value':b'4096' },
            { 'kind':'setting', 'key':b'edb-blocksum', 'value':b'sha256' },
            { 'kind':'setting',
              'key':b'start', 'value':b'2015-04-03T10:46:06' },
            { 'kind':'setting', 'key':b'end', 'value':b'2015-04-03T10:47:59' },
            { 'kind':'directory', 'dirid':8, 'parent':0, 'name':b'pat' },
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
            ]
        for i in range(161):
            # Each of these is 50 bytes. There are two directories,
            # adding up to 11 bytes, one file at 52 bytes and one at
            # 50 bytes already. So 79 of these items should exactly
            # fill up the first block. Then 81 more items will leave
            # 14 octets available in the next block.
            item_data.append({ 'kind':'file', 'parent':0, 'name':b'file',
              'cid':b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                  b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
              'size': 23,
              'mtime_year':2013, 'mtime_second':17488800,
              'mtime_ns':(0x42 << 22) })  # To get the last octet set to 0x42
        tree = FakeFileSystem()
        writer = StreamingWriter.create(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:46'))
        for data in item_data:
            item = Item(data['kind'])
            for key, value in data.items():
                if key == 'kind':
                    pass
                else:
                    setattr(item, key, value)
            writer.write(item)
        writer.close()
        self.assertCountEqual(
            set((
                ('path', 'to', 'bk', '2015', '04-03T10:46'),
                ('path', 'to', 'bk', '2015', '04-03T10:46.new'))),
            tree._modified)
        reader = StreamingReader(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:46'))
        count = 0
        for item, expected in zip(reader, item_data):
            count += 1
            for key, value in expected.items():
                self.assertEqual(value, getattr(item, key), msg='key=' + key)
        self.assertEqual(len(item_data), count)
        self.assertNotIn(
            ('path', 'to', 'bk', '2015', '04-03T10:46.new'), tree._files)
        data = tree._files[('path', 'to', 'bk', '2015', '04-03T10:46')]
        # Check that the file has the correct number of blocks. This
        # tests that the code successfully creates new blocks (both
        # when an item exactly fills a block and when there is extra
        # space left over).
        self.assertEqual(16384, len(data))
        # Check that there is only padding after the first 50 bytes of
        # the fourth block. This tests that there is space for no more
        # than a single item in the last block, which indicates that
        # the writing code correctly packs the blocks full.
        self.assertEqual(b'', data[4096*3+50:4096*3+4064].strip(b'\x00'))
        # Check that the second block is exactly full. This tests that
        # both writing and reading succeeds at the block boundary.
        self.assertEqual(b'\x80\x00\x00\x42', data[4096+4064-4:4096+4064])

    def test_create_file_over_existing_file_fails(self):
        tree = FakeFileSystem()
        tree._files[('path', 'to', 'new', 'dbfile')] = b'hello'
        self.assertRaisesRegex(
            FileExistsError, 'File.*exists:.*path.*to.*new.*dbfile',
            StreamingWriter.create, tree, ('path', 'to', 'new', 'dbfile'))
        self.assertEqual(set(), tree._modified)

    def test_write_backup_with_path_not_matching_start_time_fails(self):
        item_data = (
            { 'kind':'magic', 'value':b'ebakup backup data' },
            { 'kind':'setting', 'key':b'edb-blocksize', 'value':b'4096' },
            { 'kind':'setting', 'key':b'edb-blocksum', 'value':b'sha256' },
            )
        tree = FakeFileSystem()
        writer = StreamingWriter.create(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:44'))
        for data in item_data:
            item = Item(data['kind'])
            for key, value in data.items():
                if key == 'kind':
                    pass
                else:
                    setattr(item, key, value)
            writer.write(item)
        startitem = Item('setting')
        startitem.key = b'start'
        startitem.value = b'2015-04-03T10:46:06'
        self.assertRaisesRegex(
            InvalidDataError, 'name and start time do not match.*10:44',
            writer.write, startitem)

    def test_write_header(self):
        tree = FakeFileSystem()
        writer = StreamingWriter.create(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:44'))
        writer.write_header(b'ebakup backup data', 4096, b'sha256')
        writer.close()
        item_data = (
            { 'kind':'magic', 'value':b'ebakup backup data' },
            { 'kind':'setting', 'key':b'edb-blocksize', 'value':b'4096' },
            { 'kind':'setting', 'key':b'edb-blocksum', 'value':b'sha256' },
            )
        reader = StreamingReader(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:44'))
        self.assertReaderItemsEqual(item_data, reader)

    def test_write_magic_and_settings(self):
        tree = FakeFileSystem()
        writer = StreamingWriter.create(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:44'))
        writer.write_magic(b'ebakup backup data')
        writer.write_setting(b'edb-blocksize', b'4096')
        writer.write_setting(b'edb-blocksum', b'sha256')
        writer.write_setting(b'extrasetting', b'extra value')
        writer.close()
        item_data = (
            { 'kind':'magic', 'value':b'ebakup backup data' },
            { 'kind':'setting', 'key':b'edb-blocksize', 'value':b'4096' },
            { 'kind':'setting', 'key':b'edb-blocksum', 'value':b'sha256' },
            { 'kind':'setting', 'key':b'extrasetting', 'value':b'extra value' },
            )
        reader = StreamingReader(
            tree, ('path', 'to', 'bk', '2015', '04-03T10:44'))
        self.assertReaderItemsEqual(item_data, reader)
