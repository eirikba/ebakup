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
        return FakeFile(self, path, modifiable=True)

    def does_path_exist(self, path):
        if path in self._files:
            return True
        if path in self._dirs:
            return True
        return False

    def is_open_file_same_as_path(self, f, path):
        return self._files[path] == f._data

    def get_item_at_path(self, path):
        return FakeFile(self, path)

    def get_modifiable_item_at_path(self, path):
        return FakeFile(self, path, modifiable=True)

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
    def __init__(self, tree, path, modifiable=False):
        self._tree = tree
        self._path = path
        self._modifiable = modifiable
        self._data = self._tree._files.get(self._path)
        if self._data is None:
            raise FileNotFoundError('File not found: ' + str(path))
        self._locked = False

    def lock_for_writing(self):
        assert self._modifiable
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
        assert self._modifiable
        assert start >= 0
        assert self._locked is True
        modded = self._tree._files_modified
        if not modded or modded[-1] != self._path:
            modded.append(self._path)
        # Actually, writing beyond the end is allowed, but it
        # shouldn't happen here, I think.
        assert start <= len(self._data.content)
        datalen = len(data)
        self._data.content = (
            self._data.content[:start] +
            data +
            self._data.content[start + datalen:])
        return start + datalen

class StandardItemData(object):
    def load_content_1(self):
        self.items = [
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
            ]

    def load_backup_1(self):
        self.items = [
            {'kind': 'magic', 'value': b'ebakup backup data'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting',
             'key': b'start', 'value': b'2015-04-03T10:46:06'},
            {'kind': 'setting',
             'key': b'end', 'value': b'2015-04-03T10:47:59'},
            {'kind': 'directory', 'dirid': 8, 'parent': 0, 'name': b'path',
             'extra_data': 0 },
            {'kind': 'directory', 'dirid': 9, 'parent': 8, 'name': b'to',
             'extra_data': 0 },
            {'kind': 'file', 'parent': 9, 'name': b'file',
             'cid': b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
                    b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
             'size': 7850, 'mtime_year': 2015, 'mtime_second': 0x42a042,
             'mtime_ns': 765430000, 'extra_data': 0 },
            {'kind': 'file', 'parent': 0, 'name': b'file',
             'cid': b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                    b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
             'size': 23, 'mtime_year': 2013, 'mtime_second': 0x10adba0,
             'mtime_ns': 0, 'extra_data': 0 },
            ]

    def append_item(self, item):
        self.items.append(item)

    def change_setting(self, key, value):
        found = 0
        for item in self.items:
            if item['kind'] == 'setting' and item['key'] == key:
                found += 1
                item['value'] = value
        if found != 1:
            raise AssertionError(
                'Expected 1 setting with key "' + str(key) +
                '", but found ' + str(found))

    def change_extra_data_for_dirid(self, dirid, extra):
        found = 0
        for item in self.items:
            if item['kind'] == 'directory' and item['dirid'] == dirid:
                found += 1
                item['extra_data'] = extra
        if found != 1:
            raise AssertionError(
                'Expected 1 directory with id ' + str(dirid) +
                ', but found ' + str(found))

    def change_extra_data_for_file(self, parent, name, extra):
        found = 0
        for item in self.items:
            if (item['kind'] == 'file' and item['parent'] == parent and
                    item['name'] == name):
                found += 1
                item['extra_data'] = extra
        if found != 1:
            raise AssertionError(
                'Expected 1 file in directory ' + str(parent) +
                ' with name "' + str(name) +
                '", but found ' + str(found))

class TestDataFile(unittest.TestCase):
    def assertItemSequence(self, expect, actual):
        for x in expect:
            item = next(actual)
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

    def assertItemSequenceWithExtras(self, expect, actual, kvids, xids):
        has_seen_data_item = False
        for x in expect:
            item = next(actual)
            while item.kind in ('key-value', 'extradef'):
                self.assertFalse(has_seen_data_item)
                if item.kind == 'key-value':
                    self.assertNotIn(item.kvid, kvids)
                    kvids[item.kvid] = (item.key, item.value)
                else:
                    self.assertNotIn(item.xid, xids)
                    xids[item.xid] = item.kvids
                item = next(actual)
            if item.kind not in ('magic', 'setting'):
                has_seen_data_item = True
            for key, value in x.items():
                if key == 'extra_data':
                    extra = {}
                    for kvid in xids[item.extra_data]:
                        k, v = kvids[kvid]
                        extra[k] = v
                    self.assertExtraDataEqual(value, extra)
                else:
                    self.assertEqual(value, getattr(item, key), msg=key)

    def assertExtraDataEqual(self, expected, actual):
        x = {}
        for k,v in expected.items():
            k,v = self._encode_key_value(k, v)
            x[k] = v
        self.assertEqual(x, actual)

    def assertKeyValueDictsEqual(self, expected, actual):
        x = {}
        for k,v in expected.items():
            self.assertEqual(2, len(v))
            key, value = self._encode_key_value(v[0], v[1])
            x[k] = (key, value)
        self.assertEqual(x, actual)

    def append_item_sequence(self, items, output):
        self.append_item_sequence_with_extras(items, output, None, None)

    def append_item_sequence_with_extras(self, items, output, kvids, xids):
        xidblock = None
        for item in items:
            if item['kind'] == 'directory':
                dataitem = datafile.ItemDirectory(
                    item['dirid'], item['parent'], item['name'])
            elif item['kind'] == 'file':
                dataitem = datafile.ItemFile(
                    item['parent'], item['name'], item['cid'],
                    item['size'],
                    (item['mtime_year'], item['mtime_second'],
                     item['mtime_ns']))
            elif item['kind'].startswith('file-'):
                kind = item['kind'][5:]
                dataitem = datafile.ItemSpecialFile(
                    kind, item['parent'], item['name'], item['cid'],
                    item['size'],
                    (item['mtime_year'], item['mtime_second'],
                     item['mtime_ns']))
            else:
                raise AssertionError('Unexpected kind: ' + item['kind'])
            if 'extra_data' in item:
                extra_data = item['extra_data']
                if extra_data == 0:
                    extra_data = {}
                itemkvids = []
                for key, value in extra_data.items():
                    kvid = kvids.get(key, value)
                    if kvid is None:
                        kvid = kvids.add(key,value)
                        if xidblock is None:
                            xidblock = 1
                            if output.does_block_exist(1):
                                output.move_block(1, -1)
                            else:
                                output.create_block()
                                # And create the first data block, too
                                # (so we don't put data in the
                                # definition block).
                                output.create_block()
                        key, value = self._encode_key_value(key, value)
                        output.insert_item(
                            xidblock, -1, datafile.ItemKeyValue(
                                kvid, key, value))
                    itemkvids.append(kvid)
                if not itemkvids:
                    xid = 0
                else:
                    xid = xids.get(itemkvids)
                if xid is None:
                    xid = xids.add(itemkvids)
                    output.insert_item(
                        xidblock, -1, datafile.ItemExtraDef(xid, itemkvids))
                dataitem.set_extra_data(xid)
            output.append_item(dataitem)

    def _encode_key_value(self, key, value):
        if key in ('owner', 'group'):
            value = value.encode('utf-8')
        elif key == 'unix-access':
            if value == 0o644:
                value = b'0644'
            elif value == 0o755:
                value = b'0755'
            elif value == 0o640:
                value = b'0640'
            else:
                raise NotImplementedError(
                    'Unhandled access value: ' + str(oct(value)))
        else:
            raise NotImplementedError('Unhandled key: ' + key)
        key = key.encode('utf-8')
        return key, value

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
        expect = (
            {'kind': 'magic', 'value': b'ebakup database v1'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting', 'key': b'checksum', 'value': b'sha256'} )
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            testdata.dbfiledata('main-1'))

        main = datafile.open_main(tree, ('path', 'to', 'db'))
        self.assertItemSequence(expect, main)
        self.assertRaises(StopIteration, next, main)
        self.assertRaises(StopIteration, next, main)
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
        dbdata = testdata.dbfiledata('main-1')
        self.assertEqual(4096, len(dbdata))
        dbdata = dbdata[:-3] + b'xxx'
        tree = FakeTree()
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
        expect = (
            {'kind': 'magic', 'value': b'ebakup database v1'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'1387'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting', 'key': b'checksum', 'value': b'sha256'} )
        data = testdata.dbfiledata('main-1')[:1355].replace(
            b'blocksize:4096', b'blocksize:1387')
        data += hashlib.sha256(data).digest()
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            data)

        main = datafile.open_main(tree, ('path', 'to', 'db'))
        self.assertItemSequence(expect, main)
        self.assertRaises(StopIteration, next, main)
        main.close()
        self.assertCountEqual((), tree._files_modified)

    def test_read_main_with_non_default_block_sum(self):
        expect = (
            {'kind': 'magic', 'value': b'ebakup database v1'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'md5'},
            {'kind': 'setting', 'key': b'checksum', 'value': b'sha256'} )
        tree = FakeTree()
        data = testdata.dbfiledata('main-1')[:4064].replace(
            b'blocksum:sha256', b'blocksum:md5') + b'\x00' * 19
        data += hashlib.md5(data).digest()
        tree._add_file(
            ('path', 'to', 'db', 'main'),
            data)

        main = datafile.open_main(tree, ('path', 'to', 'db'))
        self.assertItemSequence(expect, main)
        self.assertRaises(StopIteration, next, main)
        main.close()
        self.assertCountEqual((), tree._files_modified)

    def test_read_typical_content_db(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'content'),
            testdata.dbfiledata('content-1'))
        expect = StandardItemData()
        expect.load_content_1()

        content = datafile.open_content(tree, ('path', 'to', 'db'))
        self.assertItemSequence(expect.items, content)
        self.assertRaises(StopIteration, next, content)
        self.assertRaises(StopIteration, next, content)
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

    def test_create_content_db_then_open_and_write_to_it(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))

        content = datafile.create_content_in_replacement_mode(
            tree, ('path', 'to', 'db'))
        self.assertCountEqual(
            (('path', 'to', 'db', 'content.new'),),
            tree._files_modified)
        content.commit_and_close()
        self.assertCountEqual(
            (('path', 'to', 'db', 'content.new'),
             ('path', 'to', 'db', 'content')),
            tree._files_modified)
        tree._files_modified = []
        content = datafile.open_content(
            tree, ('path', 'to', 'db'), writable=True)
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
        content.close()
        self.assertCountEqual(
            (('path', 'to', 'db', 'content'),),
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

    def test_read_and_write_content_db(self):
        expect = StandardItemData()
        expect.load_content_1()
        expect.append_item(
            { 'kind':'content',
              'cid':b'this is a new file',
              'checksum':b'this is a new file',
              'first':1409428462, 'last':1409428462,
              'updates': () })
        expect.append_item(
            { 'kind':'content',
              'cid':b'this is another one',
              'checksum':b'this is another one',
              'first':1402611839, 'last':1402611839,
              'updates': () } )
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'content'),
            testdata.dbfiledata('content-1'))

        content = datafile.open_content(
            tree, ('path', 'to', 'db'), writable=True)
        content.append_item(
            datafile.ItemContent(
                b'this is a new file', b'this is a new file',
                1409428462, 1409428462))
        content.append_item(
            datafile.ItemContent(
                b'this is another one', b'this is another one',
                1402611839, 1402611839))
        self.assertItemSequence(expect.items, content)
        self.assertRaises(StopIteration, next, content)
        content.close()
        self.assertCountEqual(
            (('path', 'to', 'db', 'content'),), tree._files_modified)
        tree._files_modified = []
        content = datafile.open_content(tree, ('path', 'to', 'db'))
        self.assertItemSequence(expect.items, content)
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

    def test_get_unopened_content(self):
        expect = StandardItemData()
        expect.load_content_1()
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'content'),
            testdata.dbfiledata('content-1'))

        content = datafile.get_unopened_content(tree, ('path', 'to', 'db'))
        content.open_and_lock_readonly()
        self.assertEqual(6, len(expect.items))
        # Don't read to the end before closing, to test that
        # re-opening the file really starts at the beginning.
        self.assertItemSequence(expect.items[:4], content)
        content.close()
        content.open_and_lock_readonly()
        self.assertItemSequence(expect.items, content)
        self.assertRaises(StopIteration, next, content)
        self.assertRaises(StopIteration, next, content)
        self.assertRaises(StopIteration, next, content)
        content.close()
        self.assertCountEqual((), tree._files_modified)

    def test_access_content_without_opening_it(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'content'),
            testdata.dbfiledata('content-1'))

        content = datafile.get_unopened_content(tree, ('path', 'to', 'db'))
        self.assertRaisesRegex(AssertionError, 'is not open', next, content)
        self.assertRaisesRegex(
            AssertionError, 'is not open',
            content.append_item, datafile.ItemSetting(b'key', b'value'))

    def test_access_content_after_closing_it(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'content'),
            testdata.dbfiledata('content-1'))

        content = datafile.get_unopened_content(tree, ('path', 'to', 'db'))
        content.open_and_lock_readonly()
        item = next(content)
        self.assertEqual('magic', item.kind)
        self.assertEqual(b'ebakup content data', item.value)
        content.close()
        self.assertRaisesRegex(AssertionError, 'is not open', next, content)
        self.assertRaisesRegex(
            AssertionError, 'is not open',
            content.append_item, datafile.ItemSetting(b'key', b'value'))

    def test_open_content_when_already_opened(self):
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', 'content'),
            testdata.dbfiledata('content-1'))

        content = datafile.get_unopened_content(tree, ('path', 'to', 'db'))
        content.open_and_lock_readonly()
        self.assertRaisesRegex(
            AssertionError, 'already open', content.open_and_lock_readonly)

    def test_get_and_open_content_when_it_does_not_exist(self):
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))

        content = datafile.get_unopened_content(tree, ('path', 'to', 'db'))
        self.assertRaises(FileNotFoundError, content.open_and_lock_readonly)

    def test_read_simple_backup(self):
        expect = StandardItemData()
        expect.load_backup_1()
        tree = FakeTree()
        tree._add_file(
            ('path', 'to', 'db', '2015', '04-03T10:46'),
            testdata.dbfiledata('backup-1'))

        backup = datafile.open_backup(
            tree, ('path', 'to', 'db'), datetime.datetime(2015, 4, 3, 10, 46))
        self.assertItemSequence(expect.items, backup)
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
        # This one is not using StandardItemData in order to preserve
        # the particular choices of data and their comments.
        items = (
            {'kind': 'magic', 'value': b'ebakup backup data'},
            {'kind': 'setting', 'key': b'edb-blocksize', 'value': b'4096'},
            {'kind': 'setting', 'key': b'edb-blocksum', 'value': b'sha256'},
            {'kind': 'setting',
             'key': b'start', 'value': b'2015-09-05T21:22:42'},
            {'kind': 'setting',
             'key': b'end', 'value': b'2015-09-05T21:24:06'},
            {'kind': 'directory', 'dirid': 8, 'parent': 0, 'name': b'path',
             'extra_data': 0 },
            {'kind': 'directory', 'dirid': 9, 'parent': 8, 'name': b'to',
             'extra_data': 0 },
            # size here is set small to check that I don't
            # accidentally overwrite the "size" of the data during
            # parsing with the "size" of this file.
            {'kind': 'file', 'parent': 9, 'name': b'file',
             'cid': b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
                    b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
             'size': 23, 'mtime_year': 2015, 'mtime_second': 0x42a042,
             'mtime_ns': 765430000, 'extra_data': 0 },
            # size here is set large enough to require multi-byte encoding
            {'kind': 'file', 'parent': 0, 'name': b'file',
             'cid': b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                    b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
             'size': 7850, 'mtime_year': 2013, 'mtime_second': 0x10adba0,
             'mtime_ns': 0, 'extra_data': 0 },
            )
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        starttime = datetime.datetime(2015, 9, 5, 21, 22, 42)

        backup = datafile.create_backup_in_replacement_mode(
            tree, ('path', 'to', 'db'), starttime)
        self.append_item_sequence(items[5:], backup)
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
        self.assertItemSequence(items, backup)
        self.assertRaises(StopIteration, next, backup)
        backup.close()
        self.assertCountEqual((), tree._files_modified)

    def test_create_simple_backup_without_commit_will_abort(self):
        items = StandardItemData()
        items.load_backup_1()
        items.change_setting(b'start', b'2015-09-05T21:22:42')
        items.change_setting(b'end', b'2015-09-05T21:24:06')
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        starttime = datetime.datetime(2015, 9, 5, 21, 22, 42)

        backup = datafile.create_backup_in_replacement_mode(
            tree, ('path', 'to', 'db'), starttime)
        self.assertEqual('setting', items.items[4]['kind'])
        self.assertEqual('directory', items.items[5]['kind'])
        self.append_item_sequence(items.items[5:], backup)
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

    def test_create_simple_backup_with_special_files(self):
        items = StandardItemData()
        items.load_backup_1()
        items.append_item(
            {'kind': 'file-symlink', 'parent': 0, 'name': b'symbolic_link',
             'cid': b':&h)\x02-\xaf`\x92\xde\x11\xbb\xd7\xaaK4\xb7\xa0E\xa1\x8d'
                    b'\xb8#(\x02"\xc2s\x01\xd6\x03\xd1',
             'size': 27, 'mtime_year': 2014, 'mtime_second': 29899012,
             'mtime_ns': 259388602, 'extra_data': 0 })
        items.append_item(
            {'kind': 'file-socket', 'parent': 0, 'name': b'fs_socket',
             'cid': b'', 'size': 0, 'mtime_year': 2014,
             'mtime_second': 24395803, 'mtime_ns': 946662039, 'extra_data': 0})
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        starttime = datetime.datetime(2015, 4, 3, 10, 46, 6)

        backup = datafile.create_backup_in_replacement_mode(
            tree, ('path', 'to', 'db'), starttime)
        self.assertEqual('setting', items.items[4]['kind'])
        self.assertEqual('directory', items.items[5]['kind'])
        self.append_item_sequence(items.items[5:], backup)
        backup.insert_item(
            0, -1, datafile.ItemSetting(b'end', b'2015-04-03T10:47:59'))
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '04-03T10:46'), tree._files)
        self.assertEqual(
            True,
            tree._files[('path', 'to', 'db', '2015', '04-03T10:46.new')].locked)
        backup.commit_and_close()
        self.assertCountEqual(
            (('path', 'to', 'db', '2015'),
             ('path', 'to', 'db', '2015', '04-03T10:46.new'),
             ('path', 'to', 'db', '2015', '04-03T10:46')),
            set(tree._files_modified))
        tree._files_modified = []
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '04-03T10:46.new'), tree._files)
        backup = datafile.open_backup(tree, ('path', 'to', 'db'), starttime)
        self.assertItemSequence(items.items, backup)
        self.assertRaises(StopIteration, next, backup)
        backup.close()
        self.assertCountEqual((), tree._files_modified)

    def test_create_simple_backup_with_extra_file_data(self):
        items = StandardItemData()
        items.load_backup_1()
        items.change_extra_data_for_dirid(
            8, { 'owner': 'me', 'group': 'me', 'unix-access': 0o755 })
        items.change_extra_data_for_dirid(
            9, { 'owner': 'me', 'group': 'me', 'unix-access': 0o755 })
        items.change_extra_data_for_file(
            9, b'file', { 'owner': 'me', 'group': 'me', 'unix-access': 0o644 })
        items.change_extra_data_for_file(
            0, b'file', { 'owner': 'me', 'group': 'me', 'unix-access': 0o755 })
        items.append_item(
            {'kind': 'file-symlink', 'parent': 0, 'name': b'symbolic_link',
             'cid': b':&h)\x02-\xaf`\x92\xde\x11\xbb\xd7\xaaK4\xb7\xa0E\xa1\x8d'
                    b'\xb8#(\x02"\xc2s\x01\xd6\x03\xd1',
             'size': 27, 'mtime_year': 2014, 'mtime_second': 29899012,
             'mtime_ns': 259388602,
             'extra_data': {
                 'owner': 'other', 'group': 'other', 'unix-access': 0o644 } })
        items.append_item(
            {'kind': 'file-socket', 'parent': 0, 'name': b'fs_socket',
             'cid': b'', 'size': 0, 'mtime_year': 2014,
             'mtime_second': 24395803, 'mtime_ns': 946662039,
             'extra_data': {
                 'owner': 'root', 'group': 'staff', 'unix-access': 0o640 } })
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        starttime = datetime.datetime(2015, 4, 3, 10, 46, 6)

        backup = datafile.create_backup_in_replacement_mode(
            tree, ('path', 'to', 'db'), starttime)
        kvs = KeyValueDict()
        extradefs = ExtraDataDict()
        self.assertEqual('setting', items.items[4]['kind'])
        self.assertEqual('directory', items.items[5]['kind'])
        self.append_item_sequence_with_extras(
            items.items[5:], backup, kvs, extradefs)
        backup.insert_item(
            0, -1, datafile.ItemSetting(b'end', b'2015-04-03T10:47:59'))
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '04-03T10:46'), tree._files)
        self.assertEqual(
            True,
            tree._files[('path', 'to', 'db', '2015', '04-03T10:46.new')].locked)
        backup.commit_and_close()
        self.assertCountEqual(
            (('path', 'to', 'db', '2015'),
             ('path', 'to', 'db', '2015', '04-03T10:46.new'),
             ('path', 'to', 'db', '2015', '04-03T10:46')),
            set(tree._files_modified))
        tree._files_modified = []
        self.assertNotIn(
            ('path', 'to', 'db', '2015', '04-03T10:46.new'), tree._files)
        backup = datafile.open_backup(tree, ('path', 'to', 'db'), starttime)
        kvids = {}
        xids = { 0: tuple() }
        self.assertItemSequenceWithExtras(items.items, backup, kvids, xids)
        self.assertRaises(StopIteration, next, backup)
        self.assertKeyValueDictsEqual(kvs.kvids, kvids)
        self.assertEqual(extradefs.xids, xids)
        backup.close()
        self.assertCountEqual((), tree._files_modified)

    def test_move_block_to_end(self):
        items = StandardItemData()
        items.load_backup_1()
        tree = FakeTree()
        tree._add_directory(('path', 'to', 'db'))
        starttime = datetime.datetime(2015, 4, 3, 10, 46, 6)

        backup = datafile.create_backup_in_replacement_mode(
            tree, ('path', 'to', 'db'), starttime)
        self.assertEqual('setting', items.items[4]['kind'])
        self.assertEqual('directory', items.items[5]['kind'])
        self.append_item_sequence(items.items[5:], backup)
        self.assertEqual(1, backup.get_last_block_index())
        backup.move_block(1, -1)
        self.assertEqual(2, backup.get_last_block_index())
        backup.insert_item(
            1, -1, datafile.ItemKeyValue(0, b'extra key', b'extra value'))
        backup.insert_item(
            0, -1, datafile.ItemSetting(b'end', b'2015-04-03T10:47:59'))
        backup.commit_and_close()
        backup = datafile.open_backup(tree, ('path', 'to', 'db'), starttime)
        self.assertEqual(2, backup.get_last_block_index())
        self.assertItemSequence(items.items[:5], backup)
        item = next(backup)
        self.assertEqual('key-value', item.kind)
        self.assertEqual(b'extra key', item.key)
        self.assertEqual(b'extra value', item.value)
        self.assertItemSequence(items.items[5:], backup)
        backup.close()

class KeyValueDict(object):
    def __init__(self):
        self.next_kvid = 0
        self.kvids = {}

    def get(self, key, value):
        for kvid, (k,v) in self.kvids.items():
            if k == key and v == value:
                return kvid
        return None

    def add(self, key, value):
        kvid = self.next_kvid
        self.next_kvid += 1
        self.kvids[kvid] = (key, value)
        return kvid

    def get_or_add(self, key, value):
        kvid = self.get(key, value)
        if kvid is not None:
            return kvid
        return self.add(key,value)

class ExtraDataDict(object):
    def __init__(self):
        self.next_xid = 8
        self.xids = { 0: tuple() }

    def get(self, kvids):
        kvidset = set(kvids)
        for xid, kvs in self.xids.items():
            if set(kvs) == kvidset:
                return xid
        return None

    def add(self, kvids):
        xid = self.next_xid
        self.next_xid += 1
        self.xids[xid] = tuple(kvids)
        return xid

    def get_or_add(self, kvids):
        xid = self.get(kvids)
        if xid is not None:
            return xid
        return self.add(kvids)
