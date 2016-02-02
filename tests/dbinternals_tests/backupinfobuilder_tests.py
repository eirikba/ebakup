#!/usr/bin/env python3

import datetime
import unittest

import datafile

import dbinternals.backupinfobuilder as backupbuilder


class FakeDatabase(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path


class FakeTree(object):
    def __init__(self):
        self._files = {}

    def create_regular_file(self, path):
        assert path[0] == 'db'
        f = FakeFile()
        self._files[path] = f
        return f

    def get_item_at_path(self, path):
        return self._files[path]

    def is_open_file_same_as_path(self, f, path):
        return f == self._files[path]

    def is_same_file_system_as(self, other):
        return self == other

    def rename_and_overwrite(self, source, target):
        self._files[target] = self._files[source]
        del self._files[source]


class FakeFile(object):
    def __init__(self):
        self._content = b''
        self._locked = None

    def drop_all_cached_data(self):
        pass

    def lock_for_writing(self):
        assert self._locked is None
        self._locked = True

    def lock_for_reading(self):
        assert self._locked is None
        self._locked = 1

    def close(self):
        self._locked = None

    def get_size(self):
        return len(self._content)

    def get_data_slice(self, start, end):
        assert self._locked == 1
        return self._content[start:end]

    def write_data_slice(self, start, data):
        assert self._locked is True
        assert 0 <= start <= len(self._content)
        old  = self._content
        self._content = old[:start] + data + old[start + len(data):]


class DecodedBackup(object):
    def __init__(self, tree, dbpath, name):
        self._items = []
        self._dirs = { 0: () }
        df = datafile.open_backup_by_name(tree, dbpath, name)
        for item in df:
            if item.kind == 'directory':
                assert item.dirid not in self._dirs
                self._dirs[item.dirid] = self._full_path(item)
            self._items.append(item)

    def _full_path(self, item):
        return self._dirs[item.parent] + (item.name,)

    def get_start_time(self):
        for item in self._items:
            if item.kind == 'setting' and item.key == b'start':
                return item.value

    def get_end_time(self):
        for item in self._items:
            if item.kind == 'setting' and item.key == b'end':
                return item.value

    def list_files(self):
        files = []
        for item in self._items:
            if item.kind == 'file':
                files.append(self._full_path(item))
        return files

    def get_file(self, path):
        for item in self._items:
            if item.kind == 'file' and self._full_path(item) == path:
                return item

    def get_dir(self, path):
        for item in self._items:
            if item.kind == 'directory' and self._full_path(item) == path:
                return item

    def get_extra(self, xid):
        kv = {}
        xitem = None
        for item in self._items:
            if item.kind == 'key-value':
                assert item.kvid not in kv
                kv[item.kvid] = (item.key, item.value)
            if item.kind == 'extradef' and item.xid == xid:
                assert xitem is None
                xitem = item
        return [kv[x] for x in xitem.kvids]



class TestBackupInfoBuilder(unittest.TestCase):
    def setUp(self):
        self.tree = FakeTree()
        self.db = FakeDatabase(self.tree, ('db',))
        self.start = datetime.datetime(2014, 12, 29, 14, 19, 43)
        self.builder = backupbuilder.BackupInfoBuilder(self.db, self.start)

    def test_build_a_simple_backup(self):
        self.builder.add_file(
            ('a file',), b'first cid', 20043,
            datetime.datetime(2014, 6, 27, 11, 7, 1), 907388851)
        self.builder.add_directory(('path',))
        self.builder.add_directory(('path', 'to'))
        self.builder.add_file(
            ('path', 'to', 'file'), b'other cid', 11307,
            datetime.datetime(2014, 7, 28, 18, 46, 11), 433570807)
        endtime = datetime.datetime(2014, 12, 29, 14, 51, 33)
        self.builder.commit(endtime)

        bk = DecodedBackup(self.tree, ('db',), '2014-12-29T14:19')
        self.assertEqual(b'2014-12-29T14:19:43', bk.get_start_time())
        self.assertEqual(b'2014-12-29T14:51:33', bk.get_end_time())
        self.assertCountEqual(
            ((b'a file',), (b'path', b'to', b'file')), bk.list_files())
        f = bk.get_file((b'a file',))
        self.assertEqual(b'first cid', f.cid)
        self.assertEqual(20043, f.size)
        self.assertEqual(2014, f.mtime_year)
        sec = ((31 * 3 + 28 + 30 + 26) * 24 * 3600) + 11 * 3600 + 7 * 60 + 1
        self.assertEqual(sec, f.mtime_second)
        self.assertEqual(907388851, f.mtime_ns)

    def test_build_a_simple_backup_with_extra_data(self):
        self.builder.add_file(
            ('a file',), b'first cid', 20043,
            datetime.datetime(2014, 6, 27, 11, 7, 1), 907388851,
            extra={'owner':'me', 'group':'us'})
        self.builder.add_directory(('path',), extra_data={'owner': 'someone'})
        self.builder.add_directory(('path', 'to'), extra_data={'group': 'yes'})
        self.builder.add_file(
            ('path', 'to', 'file'), b'other cid', 11307,
            datetime.datetime(2014, 7, 28, 18, 46, 11), 433570807,
            extra={'owner':'him', 'group':'they'})
        endtime = datetime.datetime(2014, 12, 29, 14, 51, 33)
        self.builder.commit(endtime)

        bk = DecodedBackup(self.tree, ('db',), '2014-12-29T14:19')
        self.assertCountEqual(
            ((b'a file',), (b'path', b'to', b'file')), bk.list_files())
        f = bk.get_file((b'path', b'to', b'file'))
        self.assertCountEqual(
            ((b'owner', b'him'), (b'group', b'they')),
            bk.get_extra(f.extra_data))
        d = bk.get_dir((b'path', b'to'))
        self.assertCountEqual(
            ((b'group', b'yes'),), bk.get_extra(d.extra_data))
