#!/usr/bin/env python3

import hashlib
import io
import unittest

import dbfile

class FileData(object):
    def __init__(self, tree, content):
        self.tree = tree
        self.content = content
        self.locked = 0 # True: write locked, number: count of read locks

class FakeDirectory(object):
    def __init__(self):
        self._files = {}
        self._allowed_access = {}
        self._lock_proxies = {}

    def _add_file(self, path, content):
        assert path
        assert path not in self._files
        parent = path[:-1]
        while parent:
            assert parent not in self._files
            parent = parent[:-1]
        self._files[path] = FileData(self, content)

    def _is_path_directory(self, path):
        pathlen = len(path)
        for k in self._files:
            if len(k) > pathlen and k[:pathlen] == path:
                return True
        return False

    def _is_write_allowed(self, path):
        access = self._allowed_access.get(path)
        if not access:
            return False
        return 'write' in access

    def _is_write_to_file_object_allowed(self, fobj):
        current = self._files.get(fobj._path)
        if current is not fobj._data:
            return False
        return self._is_write_allowed(fobj._path)

    def _is_create_file_allowed(self, path):
        access = self._allowed_access.get(path)
        if not access:
            return False
        return 'create' in access

    def _is_rename_file_allowed(self, source_path):
        access = self._allowed_access.get(source_path)
        if not access:
            return False
        return 'rename' in access

    def _is_overwrite_file_allowed(self, path):
        access = self._allowed_access.get(path)
        if not access:
            return False
        return 'overwrite' in access

    def _allow_create_regular_file(self, path):
        self._add_allowed_access(path, 'create')

    def _add_allowed_access(self, path, what):
        access = self._allowed_access.get(path, set())
        if not access:
            self._allowed_access[path] = access
        access.add(what)

    def _disallow_create_regular_file(self, path):
        self._remove_allowed_access(path, 'create')

    def _remove_allowed_access(self, path, what):
        access = self._allowed_access.get(path)
        if access is None:
            return
        access.remove(what)
        if not access:
            del self._allowed_access[path]

    def _allow_modification(self, path):
        self._add_allowed_access(path, 'write')

    def _disallow_modification(self, path):
        self._remove_allowed_access(path, 'write')

    def _allow_delete_file(self, path):
        self._add_allowed_access(path, 'delete')

    def _disallow_delete_file(self, path):
        self._remove_allowed_access(path, 'delete')

    def _allow_overwrite_file(self, path):
        self._add_allowed_access(path, 'overwrite')

    def _disallow_overwrite_file(self, path):
        self._remove_allowed_access(path, 'overwrite')

    def _allow_rename_file(self, path):
        self._add_allowed_access(path, 'rename')

    def _disallow_rename_file(self, path):
        self._remove_allowed_access(path, 'rename')

    def _set_lock_proxy(self, path, lockpath):
        self._lock_proxies[path] = lockpath

    def _get_lock_proxy(self, path):
        proxypath = self._lock_proxies.get(path)
        if proxypath is None:
            return None
        return self._files.get(proxypath, True)

    def is_same_file_system_as(self, other):
        return True

    def does_path_exist(self, path):
        plen = len(path)
        for k in self._files:
            if k[:plen] == path:
                return True
        return False

    def get_item_at_path(self, path):
        data = self._files.get(path)
        if data:
            return FakeFile(path, data)
        for k in self._files:
            if k[:len(path)] == path:
                raise AssertionError('directories not supported yet')
        raise FileNotFoundError('No such file: ' + repr(path))

    def get_modifiable_item_at_path(self, path):
        f = self.get_item_at_path(path)
        f._writable = True
        return f

    def get_directory_listing(self, path=()):
        if path in self._files:
            raise NotADirectoryError('Not a directory')
        dirs = set()
        files = []
        pathlen = len(path)
        for cand in self._files:
            if cand[:pathlen] != path:
                continue
            name = cand[pathlen]
            if len(cand) > pathlen + 1:
                dirs.add(name)
            else:
                files.append(name)
        return tuple(dirs), tuple(files)


    def create_regular_file(self, path):
        assert isinstance(path, tuple)
        if not self._is_create_file_allowed(path):
            raise AssertionError(
                'Unexpected creation of regular file: ' + str(path))
        if path in self._files:
            raise FileExistsError('Path already exists: ' + str(path))
        for k in self._files:
            if k == path[:len(k)]:
                raise NotAdirectoryError('Not a directory: ' + str(path))
            if k[:len(path)] == path:
                raise IsADirectoryError('Is a directory: ' + str(path))
        fd = FileData(self, b'')
        self._files[path] = fd
        f = FakeFile(path, fd)
        f._writable = True
        return f

    def rename_and_overwrite(self, sourcepath, targetpath):
        if not self._is_overwrite_file_allowed(targetpath):
            raise AssertionError(
                'Unexpected rename, overwrite of ' + repr(targetpath))
        if not self._is_rename_file_allowed(sourcepath):
            raise AssertionError(
                'Unexpected rename of ' + repr(sourcepath))
        if self._is_path_directory(targetpath):
            raise IsADirectoryError('Is a directory: ' + str(targetpath))
        data = self._files[sourcepath]
        del self._files[sourcepath]
        self._files[targetpath] = data


class FakeFile(object):
    def __init__(self, path, data):
        self._path = path
        self._data = data
        self._locked = 0 # 0: unlocked, 1: read locked, True: write locked
        self._writable = False

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def drop_all_cached_data(self):
        pass

    def lock_for_reading(self):
        if self._locked == 1:
            raise AssertionError('Multiple read locks')
        if self._data.locked is True:
            raise AssertionError('Deadlock!')
        self._locked = 1
        self._data.locked += 1

    def lock_for_writing(self):
        if not self._writable:
            raise io.UnsupportedOperation('write lock')
        if self._locked != 0:
            raise AssertionError('Deadlock!')
        if self._data.locked != 0:
            raise AssertionError('Deadlock!')
        self._locked = True
        self._data.locked = True

    def get_size(self):
        return len(self._data.content)

    def get_data_slice(self, start, end):
        lockproxy = self._data.tree._get_lock_proxy(self._path)
        if lockproxy is None and self._locked == 0:
            raise AssertionError('Read from unlocked file')
        if lockproxy is True:
            raise AssertionError('Read from unlocked file (no proxy)')
        if lockproxy is not None and lockproxy.locked == 0:
            raise AssertionError('Read from unlocked file (proxy lock)')
        return self._data.content[start:end]

    def write_data_slice(self, start, data):
        if not self._writable:
            raise io.UnsupportedOperation('write')
        lockproxy = self._data.tree._get_lock_proxy(self._path)
        if lockproxy is None and  self._locked is not True:
            raise AssertionError('Write to unlocked file')
        if lockproxy is True:
            raise AssertionError('Write to unlocked file (no proxy)')
        if lockproxy is not None and lockproxy.locked is not True:
            raise AssertionError('Write to unlocked file (proxy lock)')
        if not self._data.tree._is_write_to_file_object_allowed(self):
            raise AssertionError('Unexpected write to ' + str(self._path))
        old = self._data.content
        self._data.content = old[:start] + data + old[start+len(data):]
        return start + len(data)

    def close(self):
        if self._locked == 1:
            assert self._data.locked > 0
            self._data.locked -= 1
            self._locked = 0
        assert self._locked == 0

class TestReadSimpleDBFile(unittest.TestCase):
    def setUp(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3963 +
            b'I\xe2\xf50Wx|\xb9\x07\x99\xe9\xa74\xd1\xa8'
            b'\xc5\x10\x99\xddb\xbc\x12\x99\x15yc\x02z\xb2z\xd9\xcb'
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.open_for_reading()

    def tearDown(self):
        self.dbfile.close_and_unlock()

    def test_magic(self):
        dbf = self.dbfile
        self.assertEqual(b'dbfile magic', dbf.get_magic())

    def test_get_block_data_size(self):
        self.assertEqual(4064, self.dbfile.get_block_data_size())

    def test_get_block_count(self):
        self.assertEqual(3, self.dbfile.get_block_count())

    def test_setting_key_list(self):
        dbf = self.dbfile
        self.assertCountEqual((b'key', b'a setting'), dbf.get_setting_keys())

    def test_single_value_setting_bytes(self):
        dbf = self.dbfile
        self.assertEqual(b' its value', dbf.get_single_setting(b'a setting'))

    def test_single_value_setting_string(self):
        dbf = self.dbfile
        self.assertEqual(' its value', dbf.get_single_setting('a setting'))

    def test_get_single_value_from_multi_value_setting_bytes(self):
        dbf = self.dbfile
        self.assertRaisesRegex(
            ValueError, 'multiple values',
            dbf.get_single_setting, b'key')

    def test_get_single_value_from_multi_value_setting_with_default_bytes(self):
        dbf = self.dbfile
        self.assertRaisesRegex(
            ValueError, 'multiple values',
            dbf.get_single_setting, b'key', b'defaultvalue')

    def test_get_single_value_from_multi_value_setting_string(self):
        dbf = self.dbfile
        self.assertRaisesRegex(
            ValueError, 'multiple values',
            dbf.get_single_setting, 'key')

    def test_get_single_value_from_multi_value_setting_with_default_string(
            self):
        dbf = self.dbfile
        self.assertRaisesRegex(
            ValueError, 'multiple values',
            dbf.get_single_setting, 'key', 'defaultvalue')

    def test_get_single_value_from_nonexisting_setting_bytes(self):
        dbf = self.dbfile
        self.assertRaisesRegex(
            KeyError, 'not found.*nosuchkey',
            dbf.get_single_setting, b'nosuchkey')

    def test_get_single_value_from_nonexisting_setting_string(self):
        dbf = self.dbfile
        self.assertRaisesRegex(
            KeyError, 'not found.*nosuchkey',
            dbf.get_single_setting, 'nosuchkey')

    def test_get_single_value_setting_with_default_bytes(self):
        dbf = self.dbfile
        self.assertEqual(
            b' its value',
            dbf.get_single_setting(b'a setting', b'defaultvalue'))
        self.assertEqual(
            b'defaultvalue',
            dbf.get_single_setting(b'nosuchkey', b'defaultvalue'))

    def test_get_single_value_setting_with_default_string(self):
        dbf = self.dbfile
        self.assertEqual(
            ' its value',
            dbf.get_single_setting('a setting', 'defaultvalue'))
        self.assertEqual(
            'defaultvalue',
            dbf.get_single_setting('nosuchkey', 'defaultvalue'))

    def test_multi_value_setting_bytes(self):
        dbf = self.dbfile
        self.assertEqual(
            (b'value', b'another value'), dbf.get_multi_setting(b'key'))

    def test_multi_value_setting_string(self):
        dbf = self.dbfile
        self.assertEqual(
            ('value', 'another value'), dbf.get_multi_setting('key'))

    def test_get_multi_value_from_single_value_setting_bytes(self):
        dbf = self.dbfile
        self.assertEqual((b' its value',), dbf.get_multi_setting(b'a setting'))

    def test_get_multi_value_from_single_value_setting_string(self):
        dbf = self.dbfile
        self.assertEqual((' its value',), dbf.get_multi_setting('a setting'))

    def test_get_multi_value_from_nonexisting_setting_bytes(self):
        dbf = self.dbfile
        self.assertEqual((), dbf.get_multi_setting(b'nosuchkey'))

    def test_get_multi_value_from_nonexisting_setting_string(self):
        dbf = self.dbfile
        self.assertEqual((), dbf.get_multi_setting('nosuchkey'))

    def test_get_multi_value_setting_with_default_bytes(self):
        dbf = self.dbfile
        self.assertEqual(
            (b'value', b'another value'),
            dbf.get_multi_setting(b'key'), b'defaultvalue')
        self.assertEqual(
            b'defaultvalue',
            dbf.get_multi_setting(b'nosuchkey', b'defaultvalue'))

    def test_get_multi_value_setting_with_default_string(self):
        dbf = self.dbfile
        self.assertEqual(
            ('value', 'another value'),
            dbf.get_multi_setting('key'), 'defaultvalue')
        self.assertEqual(
            'defaultvalue',
            dbf.get_multi_setting('nosuchkey', 'defaultvalue'))

    def test_read_block_1(self):
        self.assertEqual(
            b'second block\n' + b'\x00' * 4051, self.dbfile.get_block(1))

    def test_read_last_block(self):
        self.assertEqual(
            b'last block\n' + b'\x00' * 4053, self.dbfile.get_block(2))

    def test_read_first_block_beyond_end(self):
        self.assertEqual(None, self.dbfile.get_block(3))

    def test_read_second_block_beyond_end(self):
        self.assertEqual(None, self.dbfile.get_block(4))

class TestInspectUnopenedSimpleDBFile(unittest.TestCase):
    def setUp(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3963 +
            b'I\xe2\xf50Wx|\xb9\x07\x99\xe9\xa74\xd1\xa8'
            b'\xc5\x10\x99\xddb\xbc\x12\x99\x15yc\x02z\xb2z\xd9\xcb'
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))

    def test_get_block_data_size(self):
        self.assertRaisesRegex(
            dbfile.DBFileUsageError, 'must be called on an open',
            self.dbfile.get_block_data_size,)

    def test_get_block_count(self):
        self.assertRaisesRegex(
            dbfile.DBFileUsageError, 'must be called on an open',
            self.dbfile.get_block_count,)

class TestBrokenFiles(unittest.TestCase):
    def test_non_matching_checksum_of_settings_block(self):
        tree = FakeDirectory()
        tree._add_file(
            ('dbfile',),
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:valux\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3963 +
            b'I\xe2\xf50Wx|\xb9\x07\x99\xe9\xa74\xd1\xa8'
            b'\xc5\x10\x99\xddb\xbc\x12\x99\x15yc\x02z\xb2z\xd9\xcb'
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        dbf = dbfile.DBFile(tree, ('dbfile',))
        self.assertRaisesRegex(
            dbfile.DataCorruptError, 'checksum of block 0 did not match',
            dbf.open_for_reading)

    def test_non_matching_checksum_of_data_block(self):
        tree = FakeDirectory()
        tree._add_file(
            ('dbfile',),
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3963 +
            b'I\xe2\xf50Wx|\xb9\x07\x99\xe9\xa74\xd1\xa8'
            b'\xc5\x10\x99\xddb\xbc\x12\x99\x15yc\x02z\xb2z\xd9\xcb'
            b'second block?\n'
            + b'\x00' * 4050 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        dbf = dbfile.DBFile(tree, ('dbfile',))
        dbf.open_for_reading()
        self.assertEqual(b'last block\n' + b'\x00' * 4053, dbf.get_block(2))
        self.assertRaisesRegex(
            dbfile.DataCorruptError, 'checksum of block 1 did not match',
            dbf.get_block, 1)

class TestReadBlockConfigurationFromSettings(unittest.TestCase):

    def test_other_block_size(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'edb-blocksize:2777\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 2644 +
            b'\xbb\xcdL\x8d\x085N4\x16\x8a)\xf3\xba\xfd\xf9'
            b'\x03X\xea\xa6\xc7\x00\xf4pZ\xa7\xe5$!\xabf\xe4\xeb'
            b'second block\n'
            + b'\x00' * 2732 +
            b"\x9cY\xe7\xc8\xb2\x11\xcdn\xa5\xdd\xf9\xc2\x89j\xa9\xfc"
            b"\xfb\xbe'\xa4\x18\xbd\x06.\xfd\x88\xf2\xc8\xe4\xe2\xa3\xbb"
            b'last block\n'
            + b'\x00' * 2734 +
            b"\xfeF*}\t\x1c4\x1ek[\xce8\xec'\xc5W"
            b"\xec\x95$\xaf\xf1_4U\x88\x92\xdc\xdd\x07X\xea\x96")
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.open_for_reading()
        self.assertEqual(
            b'second block\n' + b'\x00' * 2732, self.dbfile.get_block(1))
        self.assertEqual(2777-32, self.dbfile.get_block_data_size())
        self.dbfile.close_and_unlock()

    def test_other_block_checksum_algorithm(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:md5\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3982 +
            b'\x10\x96sd\xe0\xc8r,\xc4\xe6n\xaeT\x84p\x8a'
            b'second block\n'
            + b'\x00' * 4067 +
            b'v8Gi&e\xe4\x0f\xbai\x0f\xe4\xaf\xe6\x0b\xb0'
            b'last block\n'
            + b'\x00' * 4069 +
            b'\x8dCSA9\xeb@V\x93b~\x89z\xa0\xf1>')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.open_for_reading()
        self.assertEqual(
            b'second block\n' + b'\x00' * 4067, self.dbfile.get_block(1))
        self.dbfile.close_and_unlock()

class TestModifySimpleDBFile(unittest.TestCase):
    def setUp(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3963 +
            b'I\xe2\xf50Wx|\xb9\x07\x99\xe9\xa74\xd1\xa8'
            b'\xc5\x10\x99\xddb\xbc\x12\x99\x15yc\x02z\xb2z\xd9\xcb'
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))

    def test_add_block(self):
        filedata = self.tree._files[('path', 'to', 'file')]
        oldcontent = filedata.content[:]
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_block(3, b'added block')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                oldcontent + b'added block' + b'\x00' * 4053 +
                b'\xd7E\x1fRG\x96\x00\xbd(mi\xd7\x87\xf1\x86'
                b'\xce2\xd4\xcfXr\xb5\\\x14P\x1b\xe3<\xb5;^\xfc',
                filedata.content)
        with self.dbfile.open_for_reading():
            self.assertEqual(4, self.dbfile.get_block_count())

    def test_change_block(self):
        filedata = self.tree._files[('path', 'to', 'file')]
        oldcontent = filedata.content[:]
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_block(1, b'added block')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                oldcontent[:4096] + b'added block' + b'\x00' * 4053 +
                b'\xd7E\x1fRG\x96\x00\xbd(mi\xd7\x87\xf1\x86'
                b'\xce2\xd4\xcfXr\xb5\\\x14P\x1b\xe3<\xb5;^\xfc' +
                oldcontent[8192:],
                filedata.content)
        with self.dbfile.open_for_reading():
            self.assertEqual(3, self.dbfile.get_block_count())

    def test_change_first_block_fails(self):
        with self.dbfile.open_for_in_place_modification():
            self.assertRaisesRegex(
                dbfile.DBFileUsageError,
                'overwrite blocks before the first data block',
                self.dbfile.set_block, 0, b'added block')

    def test_add_too_big_block_fails(self):
        with self.dbfile.open_for_in_place_modification():
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'data too big',
                self.dbfile.set_block, 3, b'a' * 4065)

    def test_add_block_beyond_first_unused_block_fails(self):
        filedata = self.tree._files[('path', 'to', 'file')]
        with self.dbfile.open_for_in_place_modification():
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'skip empty space',
                self.dbfile.set_block, 4, b'added block')

    def test_change_block_on_non_writable_file_fails(self):
        with self.dbfile.open_for_reading():
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'not open for writing',
                self.dbfile.set_block, 1, b'added block')

    def test_change_block_on_non_open_file_fails(self):
        self.assertRaisesRegex(
            dbfile.DBFileUsageError, 'not open for writing',
            self.dbfile.set_block, 1, b'added block')

    def test_add_new_setting_string(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting('new key', 'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_add_new_setting_string_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting('new key', 'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(b'dbfile magic', self.dbfile.get_magic())
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_add_new_setting_bytes(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting(b'new key', b'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_add_new_setting_bytes_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting(b'new key', b'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_replace_old_setting_string(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting('a setting', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('a setting'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'a setting'))

    def test_replace_old_setting_string_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting('a setting', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('a setting'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'a setting'))

    def test_replace_old_setting_bytes(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting(b'a setting', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('a setting'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'a setting'))

    def test_replace_old_setting_bytes_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting(b'a setting', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('a setting'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'a setting'))

    def test_replace_old_multi_setting_string(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting('key', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('key'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'key'))

    def test_replace_old_multi_setting_string_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting('key', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('key'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'key'))

    def test_replace_old_multi_setting_bytes(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting(b'key', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('key'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'key'))

    def test_replace_old_multi_setting_bytes_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.set_setting(b'key', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                'changed', self.dbfile.get_single_setting('key'))
            self.assertEqual(
                b'changed', self.dbfile.get_single_setting(b'key'))

    def test_set_setting_with_disallowed_characters_fails(self):
        with self.dbfile.open_for_in_place_modification():
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'keys can not contain.*:.*ke:y',
                self.dbfile.set_setting, 'ke:y', 'value')
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'keys can not contain newline',
                self.dbfile.set_setting, 'ke\ny', 'value')
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'values can not contain newline',
                self.dbfile.set_setting, 'key', 'val\nue')

    def test_append_new_setting_string(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting('new key', 'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_append_new_setting_string_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting('new key', 'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_append_new_setting_bytes(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting(b'new key', b'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_append_new_setting_bytes_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting(b'new key', b'new value')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                'new value', self.dbfile.get_single_setting('new key'))
            self.assertEqual(
                b'new value', self.dbfile.get_single_setting(b'new key'))

    def test_append_old_setting_string(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting('a setting', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                (' its value', 'changed'),
                self.dbfile.get_multi_setting('a setting'))
            self.assertEqual(
                (b' its value', b'changed'),
                self.dbfile.get_multi_setting(b'a setting'))

    def test_append_old_setting_string_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting('a setting', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                (' its value', 'changed'),
                self.dbfile.get_multi_setting('a setting'))
            self.assertEqual(
                (b' its value', b'changed'),
                self.dbfile.get_multi_setting(b'a setting'))

    def test_append_old_setting_bytes(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting(b'a setting', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                (' its value', 'changed'),
                self.dbfile.get_multi_setting('a setting'))
            self.assertEqual(
                (b' its value', b'changed'),
                self.dbfile.get_multi_setting(b'a setting'))

    def test_append_old_setting_bytes_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting(b'a setting', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                (' its value', 'changed'),
                self.dbfile.get_multi_setting('a setting'))
            self.assertEqual(
                (b' its value', b'changed'),
                self.dbfile.get_multi_setting(b'a setting'))

    def test_append_old_multi_setting_string(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting('key', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                ('value', 'another value', 'changed'),
                self.dbfile.get_multi_setting('key'))
            self.assertEqual(
                (b'value', b'another value', b'changed'),
                self.dbfile.get_multi_setting(b'key'))

    def test_append_old_multi_setting_string_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting('key', 'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                ('value', 'another value', 'changed'),
                self.dbfile.get_multi_setting('key'))
            self.assertEqual(
                (b'value', b'another value', b'changed'),
                self.dbfile.get_multi_setting(b'key'))

    def test_append_old_multi_setting_bytes(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting(b'key', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
            self.assertEqual(
                ('value', 'another value', 'changed'),
                self.dbfile.get_multi_setting('key'))
            self.assertEqual(
                (b'value', b'another value', b'changed'),
                self.dbfile.get_multi_setting(b'key'))

    def test_append_old_multi_setting_bytes_durable(self):
        with self.dbfile.open_for_in_place_modification():
            self.tree._allow_modification(('path', 'to', 'file'))
            self.dbfile.append_setting(b'key', b'changed')
            self.tree._disallow_modification(('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                ('value', 'another value', 'changed'),
                self.dbfile.get_multi_setting('key'))
            self.assertEqual(
                (b'value', b'another value', b'changed'),
                self.dbfile.get_multi_setting(b'key'))

    def test_append_setting_with_disallowed_characters_fails(self):
        with self.dbfile.open_for_in_place_modification():
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'keys can not contain.*:.*ke:y',
                self.dbfile.append_setting, 'ke:y', 'value')
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'keys can not contain newline',
                self.dbfile.append_setting, 'ke\ny', 'value')
            self.assertRaisesRegex(
                dbfile.DBFileUsageError, 'values can not contain newline',
                self.dbfile.append_setting, 'key', 'val\nue')

    def test_rewrite_file_write_nothing(self):
        old_content = self.tree._files[('path', 'to', 'file')].content
        self.tree._set_lock_proxy(
            ('path', 'to', 'file.new'), ('path', 'to', 'file'))
        self.tree._allow_create_regular_file(('path', 'to', 'file.new'))
        self.tree._allow_modification(('path', 'to', 'file.new'))
        with self.dbfile.open_for_full_rewrite():
            self.tree._disallow_create_regular_file(('path', 'to', 'file.new'))
            self.tree._allow_rename_file(('path', 'to', 'file.new'))
            self.tree._allow_overwrite_file(('path', 'to', 'file'))
            self.dbfile.commit()
            self.tree._disallow_rename_file(('path', 'to', 'file.new'))
            self.tree._disallow_overwrite_file(('path', 'to', 'file'))
        self.assertEqual(
            old_content[:4096],
            self.tree._files[('path', 'to', 'file')].content)

    def test_rewrite_file_add_setting(self):
        old_content = self.tree._files[('path', 'to', 'file')].content
        self.tree._set_lock_proxy(
            ('path', 'to', 'file.new'), ('path', 'to', 'file'))
        self.tree._allow_create_regular_file(('path', 'to', 'file.new'))
        self.tree._allow_modification(('path', 'to', 'file.new'))
        with self.dbfile.open_for_full_rewrite():
            self.tree._disallow_create_regular_file(('path', 'to', 'file.new'))
            self.dbfile.set_setting('new setting', 'good value')
            self.dbfile.set_block(1, self.dbfile.get_block(1))
            self.dbfile.set_block(2, self.dbfile.get_block(2))
            self.tree._disallow_modification(('path', 'to', 'file.new'))
            self.tree._allow_rename_file(('path', 'to', 'file.new'))
            self.tree._allow_overwrite_file(('path', 'to', 'file'))
            self.dbfile.commit()
            self.tree._disallow_rename_file(('path', 'to', 'file.new'))
            self.tree._disallow_overwrite_file(('path', 'to', 'file'))
        self.assertEqual(
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            b'new setting:good value\n'
            + b'\x00' * 3940 +
            b'\xc4\x8cb;,\x03\x19\xbf\x93\x9dH\xff\x90f\xfa\xb4,f'
            b'I\x0f\xad\xfe\xec\xb8>Y\xba\xf1\x025\x88\x91' +
            old_content[4096:],
            self.tree._files[('path', 'to', 'file')].content)

    def test_rewrite_file_set_setting(self):
        old_content = self.tree._files[('path', 'to', 'file')].content
        self.tree._set_lock_proxy(
            ('path', 'to', 'file.new'), ('path', 'to', 'file'))
        self.tree._allow_create_regular_file(('path', 'to', 'file.new'))
        self.tree._allow_modification(('path', 'to', 'file.new'))
        with self.dbfile.open_for_full_rewrite():
            self.tree._disallow_create_regular_file(('path', 'to', 'file.new'))
            self.dbfile.set_setting('new setting', 'good value')
            self.tree._disallow_modification(('path', 'to', 'file.new'))
            self.tree._allow_rename_file(('path', 'to', 'file.new'))
            self.tree._allow_overwrite_file(('path', 'to', 'file'))
            self.dbfile.commit()
            self.tree._disallow_rename_file(('path', 'to', 'file.new'))
            self.tree._disallow_overwrite_file(('path', 'to', 'file'))
        self.assertEqual(
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            b'new setting:good value\n'
            + b'\x00' * 3940 +
            b'\xc4\x8cb;,\x03\x19\xbf\x93\x9dH\xff\x90f\xfa\xb4,f'
            b'I\x0f\xad\xfe\xec\xb8>Y\xba\xf1\x025\x88\x91',
            self.tree._files[('path', 'to', 'file')].content)

    def test_rewrite_file_swap_blocks(self):
        old_content = self.tree._files[('path', 'to', 'file')].content
        self.tree._set_lock_proxy(
            ('path', 'to', 'file.new'), ('path', 'to', 'file'))
        self.tree._allow_create_regular_file(('path', 'to', 'file.new'))
        self.tree._allow_modification(('path', 'to', 'file.new'))
        with self.dbfile.open_for_full_rewrite():
            self.tree._disallow_create_regular_file(('path', 'to', 'file.new'))
            self.dbfile.set_block(1, self.dbfile.get_block(2))
            self.dbfile.set_block(2, self.dbfile.get_block(1))
            self.tree._disallow_modification(('path', 'to', 'file.new'))
            self.tree._allow_rename_file(('path', 'to', 'file.new'))
            self.tree._allow_overwrite_file(('path', 'to', 'file'))
            self.dbfile.commit()
            self.tree._disallow_rename_file(('path', 'to', 'file.new'))
            self.tree._disallow_overwrite_file(('path', 'to', 'file'))
        self.assertEqual(
            old_content[:4096] + old_content[8192:] + old_content[4096:8192],
            self.tree._files[('path', 'to', 'file')].content)

    def test_rewrite_file_without_explicit_commit_will_abort(self):
        old_content = self.tree._files[('path', 'to', 'file')].content
        self.tree._set_lock_proxy(
            ('path', 'to', 'file.new'), ('path', 'to', 'file'))
        self.tree._allow_create_regular_file(('path', 'to', 'file.new'))
        self.tree._allow_modification(('path', 'to', 'file.new'))
        with self.dbfile.open_for_full_rewrite():
            self.tree._disallow_create_regular_file(('path', 'to', 'file.new'))
            self.dbfile.set_setting('new setting', 'good value')
            self.tree._allow_delete_file(('path', 'to', 'file.new'))
            self.tree._allow_delete_file(('path', 'to', 'file'))
        self.tree._disallow_delete_file(('path', 'to', 'file.new'))
        self.tree._disallow_delete_file(('path', 'to', 'file'))
        self.assertEqual(
            old_content, self.tree._files[('path', 'to', 'file')].content)

class TestOtherOperations(unittest.TestCase):
    def test_open_for_reading_context(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3963 +
            b'I\xe2\xf50Wx|\xb9\x07\x99\xe9\xa74\xd1\xa8'
            b'\xc5\x10\x99\xddb\xbc\x12\x99\x15yc\x02z\xb2z\xd9\xcb'
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        with self.dbfile.open_for_reading():
            self.assertEqual(
                b'second block\n' + b'\x00' * 4051, self.dbfile.get_block(1))
            self.assertNotEqual(None, self.dbfile._read_file)
        self.assertEqual(None, self.dbfile._read_file)

    def test_create_empty_file(self):
        tree = FakeDirectory()
        dbf = dbfile.DBFile(tree, ('new', 'db'))
        tree._allow_create_regular_file(('new', 'db'))
        tree._allow_create_regular_file(('new', 'db.new'))
        tree._allow_modification(('new', 'db.new'))
        with dbf.create(b'new db magic', 4096, hashlib.sha256):
            tree._disallow_create_regular_file(('new', 'db'))
            tree._disallow_create_regular_file(('new', 'db.new'))
            tree._disallow_modification(('new', 'db.new'))
            self.assertEqual(b'', tree._files[('new', 'db')].content)
            tree._allow_overwrite_file(('new', 'db'))
            tree._allow_rename_file(('new', 'db.new'))
            dbf.commit()
            tree._disallow_overwrite_file(('new', 'db'))
            tree._disallow_rename_file(('new', 'db.new'))
        self.assertEqual(
            b'new db magic\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n' +
            b'\x00' * 4012 +
            b"\xc9\xe7\x1b\x96\xe2\xf8\x9c\xf8(\xbe#\xfb"
            b"\xec:\x9d'\x8c\xa4\xbb\xb6\x05@\xd7r>\xbes\x88\x91\xb7YL",
            tree._files[('new', 'db')].content)

    def test_create_fails_if_file_exists(self):
        tree = FakeDirectory()
        tree._add_file(
            ('file', 'already', 'exists'),
            b'Hello world')
        dbf = dbfile.DBFile(tree, ('file', 'already', 'exists'))
        tree._allow_create_regular_file(('file', 'already', 'exists'))
        self.assertRaises(FileExistsError, dbf.create, b'db magic')

    def test_create_file_with_data(self):
        tree = FakeDirectory()
        dbf = dbfile.DBFile(tree, ('path', 'to', 'db'))
        tree._allow_create_regular_file(('path', 'to', 'db'))
        tree._allow_create_regular_file(('path', 'to', 'db.new'))
        tree._allow_modification(('path', 'to', 'db.new'))
        with dbf.create(b'ebadb file', 4096, hashlib.sha256):
            tree._disallow_create_regular_file(('path', 'to', 'db'))
            tree._disallow_create_regular_file(('path', 'to', 'db.new'))
            dbf.set_setting('first setting', 'yes')
            dbf.set_setting('another setting', 'no')
            dbf.set_block(1, b'And some data')
            tree._disallow_modification(('path', 'to', 'db.new'))
            tree._allow_overwrite_file(('path', 'to', 'db'))
            tree._allow_rename_file(('path', 'to', 'db.new'))
            dbf.commit()
            tree._disallow_overwrite_file(('path', 'to', 'db'))
            tree._disallow_rename_file(('path', 'to', 'db.new'))
        self.assertEqual(
            b'ebadb file\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'first setting:yes\nanother setting:no\n' +
            b'\x00' * 3977 +
            b"\x17\xd4W@\xf9\x83\xac\x17#n\xe886}G\xc1"
            b"\xa5S\x84n\xceP\xe4\xd4H\x030'\xff\xe4\xd7M"
            b'And some data' + b'\x00' * 4051 +
            b'\xc4\xd5d\xe7ZV\x18\x1bw\x0cC\xce9\xaa\xdd'
            b'\xa9_\x7f\xddb\xc6\xaa(\xe8J\xb9\x90\xe5\x02\x82z\xdd',
            tree._files[('path', 'to', 'db')].content)

    def test_create_without_explicit_commit_will_abort(self):
        tree = FakeDirectory()
        dbf = dbfile.DBFile(tree, ('path', 'to', 'db'))
        tree._allow_create_regular_file(('path', 'to', 'db'))
        tree._allow_create_regular_file(('path', 'to', 'db.new'))
        tree._allow_modification(('path', 'to', 'db.new'))
        with dbf.create(b'ebadb file', 4096, hashlib.sha256):
            tree._disallow_create_regular_file(('path', 'to', 'db'))
            tree._disallow_create_regular_file(('path', 'to', 'db.new'))
            dbf.set_setting('first setting', 'yes')
            dbf.set_setting('another setting', 'no')
            dbf.set_block(1, b'And some data')
            tree._disallow_modification(('path', 'to', 'db.new'))
            tree._allow_delete_file(('path', 'to', 'db.new'))
            tree._allow_delete_file(('path', 'to', 'db'))
        tree._disallow_delete_file(('path', 'to', 'db'))
        tree._disallow_delete_file(('path', 'to', 'db'))
