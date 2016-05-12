#!/usr/bin/env python3

import datetime
import unittest

import database.datafile as datafile

import database.dbinternals.backupinfobuilder as backupbuilder
# test_various_timestamps_for_mtime uses backupinfo to test round-trip
# conversion.
import database.dbinternals.backupinfo as backupinfo


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
        self.bkname = '2014-12-29T14:19'

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

        bk = DecodedBackup(self.tree, ('db',), self.bkname)
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

        bk = DecodedBackup(self.tree, ('db',), self.bkname)
        self.assertCountEqual(
            ((b'a file',), (b'path', b'to', b'file')), bk.list_files())
        f = bk.get_file((b'path', b'to', b'file'))
        self.assertCountEqual(
            ((b'owner', b'him'), (b'group', b'they')),
            bk.get_extra(f.extra_data))
        d = bk.get_dir((b'path', b'to'))
        self.assertCountEqual(
            ((b'group', b'yes'),), bk.get_extra(d.extra_data))

    def test_build_small_backup_should_create_small_file(self):
        dirs = (('path',), ('path', 'to'), ('subdir',))
        for d in dirs:
            self.builder.add_directory(d)
        size = 17
        mtime = datetime.datetime(2014, 4, 25, 21, 42, 15)
        mtime_inc = datetime.timedelta(seconds=22)
        nsecs = 134407806
        fnames = ('file', 'other file', 'stuff')
        for d in dirs:
            for name in fnames:
                self.builder.add_file(
                    d + (name,),
                    (name + str(mtime)).encode('utf-8'), # cid
                    size, mtime, nsecs)
                size += 13
                mtime += mtime_inc
                nsecs += 142476838
                if nsecs > 999999999:
                    nsecs -= 999999999
        endtime = datetime.datetime(2014, 12, 29, 14, 51, 33)
        self.builder.commit(endtime)
        bkfile = self.tree._files[('db', '2014', self.bkname[5:])]
        self.assertEqual(4096 * 2, len(bkfile._content))

    def test_invalid_utf8_in_file_names(self):
        cid = b'a cid'
        size = 17
        mtime = datetime.datetime(2014, 4, 25, 21, 42, 15)
        nsecs = 134407806
        # This is how python decodes file names to strings
        test_filename = b'INVUTF8:ab\xddcd'
        test_filename_str = test_filename.decode(
            'utf-8', errors='surrogateescape')
        test_dirname = b'INVUTF8:vx\xeeyz'
        test_dirname_str = test_dirname.decode(
            'utf-8', errors='surrogateescape')
        self.builder.add_directory((test_dirname_str,))
        self.builder.add_file((test_filename_str,), cid, size, mtime, nsecs)
        endtime = datetime.datetime(2014, 12, 29, 14, 51, 33)
        self.builder.commit(endtime)
        bkfile = self.tree._files[('db', '2014', self.bkname[5:])]
        self.assertIn(b'\x0d' + test_filename + b'\x05a cid', bkfile._content)
        self.assertIn(b'\x0d' + test_dirname, bkfile._content)

    def test_multioctet_utf8_characters_in_file_names(self):
        cid = b'a cid'
        size = 17
        mtime = datetime.datetime(2014, 4, 25, 21, 42, 15)
        nsecs = 134407806
        test_filename = b'Seigmen-Dr\xc3\xa5ben.txt'
        test_dirname = b'MULTI\xe5\x83\xa1UTF8'
        self.builder.add_directory((test_dirname.decode('utf-8'),))
        self.builder.add_file(
            (test_filename.decode('utf-8'),), cid, size, mtime, nsecs)
        endtime = datetime.datetime(2014, 12, 29, 14, 51, 33)
        self.builder.commit(endtime)
        bkfile = self.tree._files[('db', '2014', self.bkname[5:])]
        self.assertIn(b'\x13' + test_filename + b'\x05a cid', bkfile._content)
        self.assertIn(b'\x0c' + test_dirname, bkfile._content)

    def test_various_timestamps_for_mtime(self):
        tests = (
            (('file1',), datetime.datetime(2014, 9, 12, 11, 9, 15), 0),
            (('file2',),
             datetime.datetime(2014, 1, 12, 11, 9, 15, 682246), 682246552),
            (('file3',), datetime.datetime(2014, 2, 28, 11, 9, 15), 0),
            (('file4',), datetime.datetime(2014, 3, 1, 11, 9, 15), 0),
            (('file5',), datetime.datetime(2012, 2, 28, 11, 9, 15), 0),
            (('file6',), datetime.datetime(2012, 2, 29, 11, 9, 15), 0),
            (('file7',), datetime.datetime(2012, 3, 1, 11, 9, 15), 0),
        )
        size = 17
        for test in tests:
            mtime = test[1].replace(microsecond=0)
            self.builder.add_file(
                test[0], test[0][0].encode('utf-8'), size, mtime, test[2])
        endtime = datetime.datetime(2014, 12, 29, 14, 51, 33)
        self.builder.commit(endtime)
        bk = backupinfo.BackupInfo(self.db, self.bkname)
        for test in tests:
            info = bk.get_file_info(test[0])
            self.assertEqual(test[1], info.mtime)
            self.assertEqual(test[2], info.mtime_nsec)
