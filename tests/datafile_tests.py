#!/usr/bin/env python3

import datetime
import hashlib
import unittest

import datafile
import testdata

class FakeTree(object):
    def __init__(self):
        self._files_modified = []
        self._files = {}
        self._dirs = set()
        self._dirs.add(())

    def _add_directory(self, path, byuser=False):
        if path in self._files:
            if byuser:
                raise FileNotFoundError('File exists: /' + '/'.join(path))
            raise AssertionError('Already exists as file: ' + str(path))
        if path in self._dirs:
            return
        if not path:
            raise AssertionError('Not possible!')
        self._add_directory(path[:-1])
        self._dirs.add(path)
        if byuser:
            self._files_modified.append(path)

    def _add_file(self, path, content, byuser=False):
        self._add_directory(path[:-1], byuser=byuser)
        if path in self._dirs:
            raise AssertionError('Already exists as dir: ' + str(path))
        if path in self._files:
            raise AssertionError('Already exists: ' + str(path))
        self._files[path] = FakeFileData(content)
        if byuser:
            self._files_modified.append(path)

    def is_same_file_system_as(self, other):
        return True

    def create_regular_file(self, path):
        self._add_file(path, b'', byuser=True)
        return FakeFile(self, path)

    def does_path_exist(self, path):
        if path in self._files:
            return True
        if path in self._dirs:
            return True
        return False

    def get_item_at_path(self, path):
        return FakeFile(self, path)

    def rename_and_overwrite(self, sourcepath, targetpath):
        self._files_modified.append(targetpath)
        self._files[targetpath] = self._files[sourcepath]
        del self._files[sourcepath]

    def delete_file_at_path(self, path):
        if path in self._dirs:
            raise AssertionError('Not a file')
        self._files_modified.append(path)
        del self._files[path]

class FakeFileData(object):
    def __init__(self, content):
        self.content = content
        self.locked = False

class FakeFile(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        self._data = self._tree._files.get(self._path)
        if self._data is None:
            raise FileNotFoundError('File not found: ' + str(path))
        self._locked = False

    def lock_for_writing(self):
        assert self._locked is False
        assert self._data.locked is False
        self._locked = True
        self._data.locked = True

    def lock_for_reading(self):
        assert self._locked is False
        assert self._data.locked is False
        self._locked = 1
        self._data.locked = 1

    def close(self):
        if self._locked is not False:
            if self._locked is True:
                assert self._data.locked is True
                self._data.locked = False
                self._locked = False
            else:
                assert self._locked == 1
                assert self._data.locked > 0
                self._data.locked -= 1
                self._locked = False
                if self._data.locked == 0:
                    self._data.locked = False
        self._data = None

    def drop_all_cached_data(self):
        pass

    def get_size(self):
        return len(self._data.content)

    def get_data_slice(self, start, end):
        assert start >= 0
        assert end >= start
        assert self._locked is not False
        return self._data.content[start:end]

    def write_data_slice(self, start, data):
        assert start >= 0
        assert self._locked is True
        # Actually, writing beyond the end is allowed, but it
        # shouldn't happen here, I think.
        assert start <= len(self._data.content)
        datalen = len(data)
        self._data.content = (
            self._data.content[:start] +
            data +
            self._data.content[start + datalen:])
        return start + datalen

class TestDataFile(unittest.TestCase):
    def test_create_typical_main(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to'))
        main = datafile.create_main_in_replacement_mode(
            tree, ('path', 'to', 'db'))
        main.append_item(datafile.ItemSetting(b'checksum', b'sha256'))
        self.assertCountEqual(
            (('path', 'to', 'db', 'main.new'), ('path', 'to', 'db')),
            tree._files_modified)
        main.commit_and_close()
        self.assertCountEqual(
            (('path', 'to', 'db'),
             ('path', 'to', 'db', 'main'),
             ('path', 'to', 'db', 'main.new')),
            tree._files_modified)
        self.assertNotIn(('path', 'to', 'db', 'main.new'), tree._files)
        self.assertEqual(
            testdata.dbfiledata('main-1'),
            tree._files[('path', 'to', 'db', 'main')].content)

    def test_read_typical_main(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            testdata.dbfiledata('main-1'))
        main = datafile.open_main(tree, ('path', 'to', 'db'))
        expect = (
            {'kind': 'magic', 'value': b'ebakup database v1'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting', 'key': b'checksum', 'value': b'sha256'} )
        for x in expect:
            item = next(main)
            for key, value in x.items():
                self.assertEqual(value, getattr(item, key), msg=key)
        self.assertRaises(StopIteration, next, main)
        main.close()
        self.assertCountEqual((), tree._files_modified)

    def test_create_main_directory_already_exists(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        self.assertRaisesRegex(
            FileExistsError, 'exists.*path.*to.*db',
            datafile.create_main_in_replacement_mode,
            tree, ('path', 'to', 'db'))
        self.assertEqual([], tree._files_modified)

    def test_open_main_does_not_exist(self):
        tree = FakeTree()
        self.assertRaisesRegex(
            FileNotFoundError, 'path.*to.*db.*main',
            datafile.open_main, tree, ('path', 'to', 'db'))
        self.assertEqual([], tree._files_modified)

    def test_main_with_non_matching_checksum(self):
        tree = FakeTree()
        dbdata = testdata.dbfiledata('main-1')
        self.assertEqual(4096, len(dbdata))
        dbdata = dbdata[:-3] + b'xxx'
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            dbdata)
        self.assertRaisesRegex(
            datafile.InvalidDataError, 'hecksum mismatch',
            datafile.open_main, tree, ('path', 'to', 'db'))
        self.assertCountEqual((), tree._files_modified)

    def test_raw_create_main_with_non_default_block_size(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to'))
        main = datafile.DataFile(tree, ('path', 'to', 'db', 'main'))
        main.create_and_lock()
        main.append_item(datafile.ItemMagic(b'ebakup database v1'))
        main.append_item(datafile.ItemSetting(b'edb-blocksize', b'1387'))
        main.append_item(datafile.ItemSetting(b'edb-blocksum', b'sha256'))
        main.append_item(datafile.ItemSetting(b'checksum', b'sha256'))
        main.close()
        self.assertCountEqual(
            (('path', 'to', 'db'), ('path', 'to', 'db', 'main')),
            tree._files_modified)
        data = testdata.dbfiledata('main-1')[:1355].replace(
            b'blocksize:4096', b'blocksize:1387')
        self.assertEqual(
            data + hashlib.sha256(data).digest(),
            tree._files[('path', 'to', 'db', 'main')].content)

    def test_raw_create_main_with_non_default_block_sum(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to'))
        main = datafile.DataFile(tree, ('path', 'to', 'db', 'main'))
        main.create_and_lock()
        main.append_item(datafile.ItemMagic(b'ebakup database v1'))
        main.append_item(datafile.ItemSetting(b'edb-blocksize', b'4096'))
        main.append_item(datafile.ItemSetting(b'edb-blocksum', b'md5'))
        main.append_item(datafile.ItemSetting(b'checksum', b'sha256'))
        main.close()
        self.assertCountEqual(
            (('path', 'to', 'db'), ('path', 'to', 'db', 'main')),
            tree._files_modified)
        data = testdata.dbfiledata('main-1')[:4064].replace(
            b'blocksum:sha256', b'blocksum:md5') + b'\x00' * 19
        self.assertEqual(
            data + hashlib.md5(data).digest(),
            tree._files[('path', 'to', 'db', 'main')].content)

    def test_read_main_with_non_default_block_size(self):
        data = testdata.dbfiledata('main-1')[:1355].replace(
            b'blocksize:4096', b'blocksize:1387')
        data += hashlib.sha256(data).digest()
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            data)
        main = datafile.open_main(tree, ('path', 'to', 'db'))
        expect = (
            {'kind': 'magic', 'value': b'ebakup database v1'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'1387'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting', 'key': b'checksum', 'value': b'sha256'} )
        for x in expect:
            item = next(main)
            for key, value in x.items():
                self.assertEqual(value, getattr(item, key), msg=key)
        self.assertRaises(StopIteration, next, main)
        main.close()
        self.assertCountEqual((), tree._files_modified)

    def test_read_main_with_non_default_block_sum(self):
        tree = FakeTree()
        data = testdata.dbfiledata('main-1')[:4064].replace(
            b'blocksum:sha256', b'blocksum:md5') + b'\x00' * 19
        data += hashlib.md5(data).digest()
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            data)
        main = datafile.open_main(tree, ('path', 'to', 'db'))
        expect = (
            {'kind': 'magic', 'value': b'ebakup database v1'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'md5'},
            {'kind': 'setting', 'key': b'checksum', 'value': b'sha256'} )
        for x in expect:
            item = next(main)
            for key, value in x.items():
                self.assertEqual(value, getattr(item, key), msg=key)
        self.assertRaises(StopIteration, next, main)
        main.close()
        self.assertCountEqual((), tree._files_modified)

    def test_read_typical_content_db(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'content'),
            testdata.dbfiledata('content-1'))
        content = datafile.open_content(tree, ('path', 'to', 'db'))
        expect = (
            {'kind': 'magic', 'value': b'ebakup content data'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
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
        for x in expect:
            item = next(content)
            for key, value in x.items():
                if key == 'updates':
                    self.assertEqual(len(item.updates), len(value))
                    for itemupd, expectupd in zip(item.updates, value):
                        for upkey, upvalue in expectupd.items():
                            self.assertEqual(
                                upvalue, getattr(itemupd, upkey),
                                msg='key:' + upkey)
                else:
                    self.assertEqual(value, getattr(item, key), msg=key)
        self.assertRaises(StopIteration, next, content)
        content.close()
        self.assertCountEqual((), tree._files_modified)

    def test_create_content_db(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        content = datafile.create_content_in_replacement_mode(
            tree, ('path', 'to', 'db'))
        cid1 = b'010----hhhh'
        content.append_item(
            datafile.ItemContent(cid1, cid1, 1417658340, 1417658340))
        cid2 = b'0200000000000000000000a'
        cksum2 = b'0200000000000000000000'
        item = datafile.ItemContent(cid2, cksum2, 1405569942, 1410763215)
        cksum3 = b'030abcdefghijklmnopqrs'
        item.content_changed(1411788080, 1415631138, cksum3)
        item.content_restored(1419507674, 1419507674)
        content.append_item(item)
        cid3 = b'040xxxxxx'
        item = datafile.ItemContent(cid3, cid3, 1402958556, 1427582355)
        content.append_item(item)
        self.assertCountEqual(
            (('path', 'to', 'db', 'content.new'),),
            tree._files_modified)
        content.commit_and_close()
        self.assertCountEqual(
            (('path', 'to', 'db', 'content.new'),
             ('path', 'to', 'db', 'content')),
            tree._files_modified)
        tree._files_modified = []
        self.assertEqual(
            8192,
            len(tree._files[('path', 'to', 'db', 'content')].content))
        content = datafile.open_content(tree, ('path', 'to', 'db'))
        self.assertEqual('magic', next(content).kind)
        item = next(content)
        while item.kind == 'setting':
            item = next(content)
        self.assertEqual('content', item.kind)
        self.assertEqual(cid1, item.cid)
        self.assertEqual(cid1, item.checksum)
        self.assertEqual(1417658340, item.first)
        self.assertEqual(1417658340, item.last)
        self.assertEqual([], item.updates)
        item = next(content)
        self.assertEqual('content', item.kind)
        self.assertEqual(cid2, item.cid)
        self.assertEqual(cksum2, item.checksum)
        self.assertEqual(1405569942, item.first)
        self.assertEqual(1410763215, item.last)
        self.assertEqual(2, len(item.updates))
        update = item.updates[0]
        self.assertEqual('changed', update.kind)
        self.assertEqual(cksum3, update.checksum)
        self.assertEqual(1411788080, update.first)
        self.assertEqual(1415631138, update.last)
        update = item.updates[1]
        self.assertEqual('restored', update.kind)
        self.assertFalse(hasattr(update, 'checksum'))
        self.assertEqual(1419507674, update.first)
        self.assertEqual(1419507674, update.last)
        item = next(content)
        self.assertEqual('content', item.kind)
        self.assertEqual(cid3, item.cid)
        self.assertEqual(cid3, item.checksum)
        self.assertEqual(1402958556, item.first)
        self.assertEqual(1427582355, item.last)
        self.assertEqual([], item.updates)
        self.assertRaises(StopIteration, next, content)
        content.close()
        self.assertCountEqual((), tree._files_modified)

    def test_create_multi_block_content_db(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        content = datafile.create_content_in_replacement_mode(
            tree, ('path', 'to', 'db'))
        # This item is sized so that the first data block is exactly filled.
        content.append_item(
            datafile.ItemContent(b'000000', b'000000', 1403044159, 1412770688))
        cid1 = b'010----x'
        for i in range(500):
            item = datafile.ItemContent(cid1, cid1, 1417658340, 1417658340)
            content.append_item(item)
        self.assertCountEqual(
            (('path', 'to', 'db', 'content.new'),),
            tree._files_modified)
        content.commit_and_close()
        self.assertCountEqual(
            (('path', 'to', 'db', 'content.new'),
             ('path', 'to', 'db', 'content')),
            tree._files_modified)
        tree._files_modified = []
        self.assertEqual(
            4 * 4096,
            len(tree._files[('path', 'to', 'db', 'content')].content))
        content = datafile.open_content(tree, ('path', 'to', 'db'))
        self.assertEqual('magic', next(content).kind)
        item = next(content)
        while item.kind == 'setting':
            item = next(content)
        self.assertEqual('content', item.kind)
        self.assertEqual(b'000000', item.cid)
        self.assertEqual(b'000000', item.checksum)
        self.assertEqual(1403044159, item.first)
        self.assertEqual(1412770688, item.last)
        self.assertEqual([], item.updates)
        for i in range(500):
            item = next(content)
            self.assertEqual('content', item.kind)
            self.assertEqual(cid1, item.cid)
            self.assertEqual(cid1, item.checksum)
            self.assertEqual(1417658340, item.first)
            self.assertEqual(1417658340, item.last)
            self.assertEqual([], item.updates)
        self.assertRaises(StopIteration, next, content)
        content.close()
        self.assertCountEqual((), tree._files_modified)
        data = tree._files[('path', 'to', 'db', 'content')].content
        self.assertEqual(
            b'ebakup content data\nedb-blocksize:4096\n', data[:39])
        # Check that the first data block starts with the first item
        # (which is different from the others so it is identifiable).
        self.assertEqual(
            b'\xdd\x06\x06000000\x3f\xc1\xa0\x53\x80\x2b\x35\x54',
            data[4096:4113])
        # Check that the first data block is exactly filled.
        self.assertEqual(
            b'\xdd\x08\x08' + cid1 + b'\xe4\xbf\x7f\x54\xe4\xbf\x7f\x54',
            data[4096+4045:4096+4064])
        # Check that the second data block has its last item in the
        # expected place...
        self.assertEqual(
            b'\xdd\x08\x08' + cid1 + b'\xe4\xbf\x7f\x54\xe4\xbf\x7f\x54',
            data[8192+4028:8192+4047])
        # ... followed by correct padding
        self.assertEqual(b'\x00' * 17, data[8192+4047:8192+4064])
        # Check that the final item is in the expected place...
        self.assertEqual(
            b'\xdd\x08\x08' + cid1 + b'\xe4\xbf\x7f\x54\xe4\xbf\x7f\x54',
            data[12288 + 73 * 19 : 12288 + 74 * 19])
        # ... followed by correct padding
        self.assertEqual(b'\x00' * 2658, data[12288 + 74 * 19 : 12288 + 4064])

    def test_read_simple_backup(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', '2015', '04-03T10:46'),
            testdata.dbfiledata('backup-1'))
        backup = datafile.open_backup(
            tree, ('path', 'to', 'db'), datetime.datetime(2015, 4, 3, 10, 46))
        expect = (
            {'kind': 'magic', 'value': b'ebakup backup data'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting',
             'key': b'start', 'value': b'2015-04-03T10:46:06'},
            {'kind': 'setting',
             'key': b'end', 'value': b'2015-04-03T10:47:59'},
            {'kind': 'directory', 'dirid': 8, 'parent': 0, 'name': b'path' },
            {'kind': 'directory', 'dirid': 9, 'parent': 8, 'name': b'to' },
            {'kind': 'file', 'parent': 9, 'name': b'file',
             'cid': b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
                    b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
             'size': 7850, 'mtime_year': 2015, 'mtime_second': 0x42a042,
             'mtime_ns': 765430000 },
            {'kind': 'file', 'parent': 0, 'name': b'file',
             'cid': b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                    b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
             'size': 23, 'mtime_year': 2013, 'mtime_second': 0x10adba0,
             'mtime_ns': 0 },
            )
        for x in expect:
            item = next(backup)
            for key, value in x.items():
                self.assertEqual(value, getattr(item, key), msg=key)
        self.assertRaises(StopIteration, next, backup)
        backup.close()
        self.assertCountEqual((), tree._files_modified)

    def test_open_backup_with_wrong_name(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', '2015', '04-03T10:45'),
            testdata.dbfiledata('backup-1'))
        self.assertRaisesRegex(
            datafile.InvalidDataError, 'non-matching start time.*10:46.*10:45',
            datafile.open_backup,
            tree, ('path', 'to', 'db'), datetime.datetime(2015, 4, 3, 10, 45))

    def test_create_simple_backup(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        starttime = datetime.datetime(2015, 9, 5, 21, 22, 42)
        backup = datafile.create_backup_in_replacement_mode(
            tree, ('path', 'to', 'db'), starttime)
        items = (
            {'kind': 'magic', 'value': b'ebakup backup data'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting',
             'key': b'start', 'value': b'2015-09-05T21:22:42'},
            {'kind': 'setting',
             'key': b'end', 'value': b'2015-09-05T21:24:06'},
            {'kind': 'directory', 'dirid': 8, 'parent': 0, 'name': b'path' },
            {'kind': 'directory', 'dirid': 9, 'parent': 8, 'name': b'to' },
            {'kind': 'file', 'parent': 9, 'name': b'file',
             'cid': b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
                    b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
             'size': 7850, 'mtime_year': 2015, 'mtime_second': 0x42a042,
             'mtime_ns': 765430000 },
            {'kind': 'file', 'parent': 0, 'name': b'file',
             'cid': b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                    b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
             'size': 23, 'mtime_year': 2013, 'mtime_second': 0x10adba0,
             'mtime_ns': 0 },
            )
        for item in items:
            if item['kind'] in ('magic', 'setting'):
                continue
            dataitem = datafile.Item(item['kind'])
            for name, value in item.items():
                setattr(dataitem, name, value)
            backup.append_item(dataitem)
        backup.insert_item(
            0, -1, datafile.ItemSetting(b'end', b'2015-09-05T21:24:06'))
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '09-05T21:22'), tree._files)
        self.assertEqual(
            True,
            tree._files[('path', 'to', 'db', '2015', '09-05T21:22.new')].locked)
        backup.commit_and_close()
        self.assertCountEqual(
            (('path', 'to', 'db', '2015'),
             ('path', 'to', 'db', '2015', '09-05T21:22.new'),
             ('path', 'to', 'db', '2015', '09-05T21:22')),
            set(tree._files_modified))
        tree._files_modified = []
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '09-05T21:22.new'), tree._files)
        backup = datafile.open_backup(tree, ('path', 'to', 'db'), starttime)
        for x in items:
            item = next(backup)
            for key, value in x.items():
                self.assertEqual(value, getattr(item, key), msg=key)
        self.assertRaises(StopIteration, next, backup)
        backup.close()
        self.assertCountEqual((), tree._files_modified)

    def test_create_simple_backup_without_commit_will_abort(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        starttime = datetime.datetime(2015, 9, 5, 21, 22, 42)
        backup = datafile.create_backup_in_replacement_mode(
            tree, ('path', 'to', 'db'), starttime)
        items = (
            {'kind': 'magic', 'value': b'ebakup backup data'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting',
             'key': b'start', 'value': b'2015-09-05T21:22:42'},
            {'kind': 'setting',
             'key': b'end', 'value': b'2015-09-05T21:24:06'},
            {'kind': 'directory', 'dirid': 8, 'parent': 0, 'name': b'path' },
            {'kind': 'directory', 'dirid': 9, 'parent': 8, 'name': b'to' },
            {'kind': 'file', 'parent': 9, 'name': b'file',
             'cid': b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
                    b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
             'size': 7850, 'mtime_year': 2015, 'mtime_second': 0x42a042,
             'mtime_ns': 765430000 },
            {'kind': 'file', 'parent': 0, 'name': b'file',
             'cid': b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                    b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
             'size': 23, 'mtime_year': 2013, 'mtime_second': 0x10adba0,
             'mtime_ns': 0 },
            )
        for item in items:
            if item['kind'] in ('magic', 'setting'):
                continue
            dataitem = datafile.Item(item['kind'])
            for name, value in item.items():
                setattr(dataitem, name, value)
            backup.append_item(dataitem)
        backup.insert_item(
            0, -1, datafile.ItemSetting(b'end', b'2015-09-05T21:24:06'))
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '09-05T21:22'), tree._files)
        self.assertEqual(
            True,
            tree._files[('path', 'to', 'db', '2015', '09-05T21:22.new')].locked)
        backup.close()
        self.assertCountEqual(
            (('path', 'to', 'db', '2015'),
             ('path', 'to', 'db', '2015', '09-05T21:22.new')),
            set(tree._files_modified))
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '09-05T21:22.new'), tree._files)
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '09-05T21:22'), tree._files)
