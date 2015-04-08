#!/usr/bin/env python3

import dbfile

import hashlib
import unittest

class FileData(object):
    def __init__(self, content):
        self.content = content
        self.locked = 0 # True: write locked, number: count of read locks

class FakeDirectory(object):
    def __init__(self):
        self._files = {}

    def _add_file(self, path, content):
        assert path
        assert path not in self._files
        parent = path[:-1]
        while parent:
            assert parent not in self._files
            parent = parent[:-1]
        self._files[path] = FileData(content)

    def get_item_at_path(self, path):
        data = self._files.get(path)
        if data:
            return FakeFile(data)
        for k in self._files:
            if k[:len(path)] == path:
                raise AssertionError('directories not supported yet')
        raise FileNotFoundError('No such file: ' + repr(path))

class FakeFile(object):
    def __init__(self, data):
        self._data = data
        self._locked = 0

    def lock_for_reading(self):
        if self._locked == 1:
            return
        if self._data.locked is True:
            raise AssertionError('Deadlock!')
        self._locked = 1
        self._data.locked += 1

    def get_data_slice(self, start, end):
        if self._locked == 0:
            raise AssertionError('Read from unlocked file')
        return self._data.content[start:end]

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
