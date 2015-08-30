#!/usr/bin/env python3

import hashlib
import unittest

import datafile
import test_data

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
        main = datafile.create_main(tree, ('path', 'to', 'db'))
        main.append_item(datafile.ItemSetting(b'checksum', b'sha256'))
        main.close()
        self.assertCountEqual(
            (('path', 'to', 'db'), ('path', 'to', 'db', 'main')),
            tree._files_modified)
        self.assertEqual(
            test_data.dbfiledata('main-1'),
            tree._files[('path', 'to', 'db', 'main')].content)

    def test_read_typical_main(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            test_data.dbfiledata('main-1'))
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
            datafile.create_main, tree, ('path', 'to', 'db'))
        self.assertEqual([], tree._files_modified)

    def test_open_main_does_not_exist(self):
        tree = FakeTree()
        self.assertRaisesRegex(
            FileNotFoundError, 'path.*to.*db.*main',
            datafile.open_main, tree, ('path', 'to', 'db'))
        self.assertEqual([], tree._files_modified)

    def test_main_with_non_matching_checksum(self):
        tree = FakeTree()
        dbdata = test_data.dbfiledata('main-1')
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
        data = test_data.dbfiledata('main-1')[:1355].replace(
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
        data = test_data.dbfiledata('main-1')[:4064].replace(
            b'blocksum:sha256', b'blocksum:md5') + b'\x00' * 19
        self.assertEqual(
            data + hashlib.md5(data).digest(),
            tree._files[('path', 'to', 'db', 'main')].content)

    def test_read_main_with_non_default_block_size(self):
        data = test_data.dbfiledata('main-1')[:1355].replace(
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
        data = test_data.dbfiledata('main-1')[:4064].replace(
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
