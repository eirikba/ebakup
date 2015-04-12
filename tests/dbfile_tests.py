#!/usr/bin/env python3

import dbfile

import hashlib
import unittest

class FileData(object):
    def __init__(self, tree, content):
        self.tree = tree
        self.content = content
        self.locked = 0 # True: write locked, number: count of read locks

class FakeDirectory(object):
    def __init__(self):
        self._files = {}
        self._allowed_access = {}

    def _add_file(self, path, content):
        assert path
        assert path not in self._files
        parent = path[:-1]
        while parent:
            assert parent not in self._files
            parent = parent[:-1]
        self._files[path] = FileData(self, content)

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

    def _allow_modification(self, path):
        access = self._allowed_access.get(path, set())
        if not access:
            self._allowed_access[path] = access
        if 'write' not in access:
            access.add('write')

    def _disallow_modification(self, path):
        access = self._allowed_access.get(path)
        if access is None:
            return
        access.remove('write')
        if not access:
            del self._allowed_access[path]

    def get_item_at_path(self, path):
        data = self._files.get(path)
        if data:
            return FakeFile(self, path, data)
        for k in self._files:
            if k[:len(path)] == path:
                raise AssertionError('directories not supported yet')
        raise FileNotFoundError('No such file: ' + repr(path))


class FakeFile(object):
    def __init__(self, tree, path, data):
        self._tree = tree
        self._path = path
        self._data = data
        self._locked = 0 # 0: unlocked, 1: read locked, True: write locked

    def lock_for_reading(self):
        if self._locked == 1:
            raise AssertionError('Multiple read locks')
        if self._data.locked is True:
            raise AssertionError('Deadlock!')
        self._locked = 1
        self._data.locked += 1

    def lock_for_writing(self):
        if self._locked != 0:
            raise AssertionError('Deadlock!')
        if self._data.locked != 0:
            raise AssertionError('Deadlock!')
        self._locked = True
        self._data.locked = True

    def get_size(self):
        return len(self._data.content)

    def get_data_slice(self, start, end):
        if self._locked == 0:
            raise AssertionError('Read from unlocked file')
        return self._data.content[start:end]

    def write_data_slice(self, start, data):
        if self._locked is not True:
            raise AssertionError('Write to unlocked file')
        if not self._tree._is_write_to_file_object_allowed(self):
            raise AssertionError('Unexpected write to ' + str(self._path))
        old = self._data.content
        self._data.content = old[:start] + data + old[start+len(data):]

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
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 4002 +
            b"\x91\xce@;X5\xd1\xa9\xd9c\x8f\xf3\xfar\xf2\xc1\xdb"
            b"\x1a'\xb7\xabv\xa3d\xa7H\x8b\x96\xa7\x9fm\xfa"
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.set_block_size(4096)
        self.dbfile.set_block_checksum_algorithm(hashlib.sha256)
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
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 4002 +
            b"\x91\xce@;X5\xd1\xa9\xd9c\x8f\xf3\xfar\xf2\xc1\xdb"
            b"\x1a'\xb7\xabv\xa3d\xa7H\x8b\x96\xa7\x9fm\xfa"
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.set_block_size(4096)
        self.dbfile.set_block_checksum_algorithm(hashlib.sha256)

    def test_get_block_data_size(self):
        self.assertEqual(4064, self.dbfile.get_block_data_size())

    def test_get_block_count(self):
        self.assertEqual(3, self.dbfile.get_block_count())

class TestBrokenFiles(unittest.TestCase):
    def test_non_matching_checksum_of_settings_block(self):
        tree = FakeDirectory()
        tree._add_file(
            ('dbfile',),
            b'dbfile magic\n'
            b'key:valux\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 4002 +
            b"\x91\xce@;X5\xd1\xa9\xd9c\x8f\xf3\xfar\xf2\xc1\xdb"
            b"\x1a'\xb7\xabv\xa3d\xa7H\x8b\x96\xa7\x9fm\xfa"
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        dbf = dbfile.DBFile(tree, ('dbfile',))
        dbf.set_block_size(4096)
        dbf.set_block_checksum_algorithm(hashlib.sha256)
        self.assertRaisesRegex(
            dbfile.DataCorruptError, 'checksum of block 0 did not match',
            dbf.open_for_reading)

    def test_non_matching_checksum_of_data_block(self):
        tree = FakeDirectory()
        tree._add_file(
            ('dbfile',),
            b'dbfile magic\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 4002 +
            b"\x91\xce@;X5\xd1\xa9\xd9c\x8f\xf3\xfar\xf2\xc1\xdb"
            b"\x1a'\xb7\xabv\xa3d\xa7H\x8b\x96\xa7\x9fm\xfa"
            b'second block?\n'
            + b'\x00' * 4050 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        dbf = dbfile.DBFile(tree, ('dbfile',))
        dbf.set_block_size(4096)
        dbf.set_block_checksum_algorithm(hashlib.sha256)
        dbf.open_for_reading()
        self.assertEqual(b'last block\n' + b'\x00' * 4053, dbf.get_block(2))
        self.assertRaisesRegex(
            dbfile.DataCorruptError, 'checksum of block 1 did not match',
            dbf.get_block, 1)

class TestReadBlockConfigurationFromSettings(unittest.TestCase):

    def test_read_block_size_from_settings(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'blocksize:4096\n'
            b'key:another value\n'
            + b'\x00' * 3987 +
            b'\x18\xeaJ\xb3h.\x7f\xc30F`rE\x17\x16\x99\xae1'
            b'8\xb0\x88\x87\x01\xbdi*P\xbf0G\x00B'
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.take_block_size_from_setting('blocksize')
        self.dbfile.set_block_checksum_algorithm(hashlib.sha256)
        self.dbfile.open_for_reading()
        self.assertEqual(
            b'second block\n' + b'\x00' * 4051, self.dbfile.get_block(1))
        self.dbfile.close_and_unlock()

    def test_read_other_block_size_from_settings(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'blocksize:2777\n'
            b'key:another value\n'
            + b'\x00' * 2668 +
            b'o\x9a\xcbBa\xd6G\x93\xef\x92\xdcR\x9eC*\xe2+'
            b'\xd6\nU,\xea\x04^\x1eu\x1aM\x97\xdb\xe6('
            b'second block\n'
            + b'\x00' * 2732 +
            b"\x9cY\xe7\xc8\xb2\x11\xcdn\xa5\xdd\xf9\xc2\x89j\xa9\xfc"
            b"\xfb\xbe'\xa4\x18\xbd\x06.\xfd\x88\xf2\xc8\xe4\xe2\xa3\xbb"
            b'last block\n'
            + b'\x00' * 2734 +
            b"\xfeF*}\t\x1c4\x1ek[\xce8\xec'\xc5W"
            b"\xec\x95$\xaf\xf1_4U\x88\x92\xdc\xdd\x07X\xea\x96")
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.take_block_size_from_setting('blocksize')
        self.dbfile.set_block_checksum_algorithm(hashlib.sha256)
        self.dbfile.open_for_reading()
        self.assertEqual(
            b'second block\n' + b'\x00' * 2732, self.dbfile.get_block(1))
        self.assertEqual(2777-32, self.dbfile.get_block_data_size())
        self.dbfile.close_and_unlock()

    def test_read_block_checksum_algorithm_from_settings(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'key:value\n'
            b'blockchecksum:sha256\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 3981 +
            b'\x9a\xeb\x82\x08\xcf)\x0c\xa3D)Nq\xe3\xa9\n\x05'
            b'\x02\xf0\x06\xd0\xed\x02\xf3\x842Y\xc7\xb3\x9c\xc3?\xbd'
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.set_block_size(4096)
        self.dbfile.take_block_checksum_algorithm_from_setting('blockchecksum')
        self.dbfile.open_for_reading()
        self.assertEqual(
            b'second block\n' + b'\x00' * 4051, self.dbfile.get_block(1))
        self.dbfile.close_and_unlock()

    def test_read_other_block_checksum_algorithm_from_settings(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'key:value\n'
            b'blockchecksum:md5\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 4000 +
            b'\x89"d\x97\xa7\xccC$w\x81\xc2\x82-\xc75\x04'
            b'second block\n'
            + b'\x00' * 4067 +
            b'v8Gi&e\xe4\x0f\xbai\x0f\xe4\xaf\xe6\x0b\xb0'
            b'last block\n'
            + b'\x00' * 4069 +
            b'\x8dCSA9\xeb@V\x93b~\x89z\xa0\xf1>')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.set_block_size(4096)
        self.dbfile.take_block_checksum_algorithm_from_setting('blockchecksum')
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
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 4002 +
            b"\x91\xce@;X5\xd1\xa9\xd9c\x8f\xf3\xfar\xf2\xc1\xdb"
            b"\x1a'\xb7\xabv\xa3d\xa7H\x8b\x96\xa7\x9fm\xfa"
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.set_block_size(4096)
        self.dbfile.set_block_checksum_algorithm(hashlib.sha256)

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

class TestOtherOperations(unittest.TestCase):
    def test_open_for_reading_context(self):
        self.tree = FakeDirectory()
        self.tree._add_file(
            ('path', 'to', 'file'),
            b'dbfile magic\n'
            b'key:value\n'
            b'a setting: its value\n'
            b'key:another value\n'
            + b'\x00' * 4002 +
            b"\x91\xce@;X5\xd1\xa9\xd9c\x8f\xf3\xfar\xf2\xc1\xdb"
            b"\x1a'\xb7\xabv\xa3d\xa7H\x8b\x96\xa7\x9fm\xfa"
            b'second block\n'
            + b'\x00' * 4051 +
            b'\x14\xffcF\xf7?\xb2\xc0\xd5`\x15\xf8\xf9\\ZN\x14s'
            b'{\x06d\xed\x97\xd7\x82\xa2h\xa4\x96k\xc2\xa8'
            b'last block\n'
            + b'\x00' * 4053 +
            b'\xbd\xe6G4\xf5$&\xda\xaa5\xf3\x96N\x08'
            b'x\xf3\x82\x9aG"\x89\x11\x8f\x1f\xa0\x0fw\xc2$wk\xbd')
        self.dbfile = dbfile.DBFile(self.tree, ('path', 'to', 'file'))
        self.dbfile.set_block_size(4096)
        self.dbfile.set_block_checksum_algorithm(hashlib.sha256)
        with self.dbfile.open_for_reading():
            self.assertEqual(
                b'second block\n' + b'\x00' * 4051, self.dbfile.get_block(1))
            self.assertNotEqual(None, self.dbfile._read_file)
        self.assertEqual(None, self.dbfile._read_file)
