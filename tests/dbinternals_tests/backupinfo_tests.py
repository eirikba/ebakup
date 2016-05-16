#!/usr/bin/env python3

import datetime
import unittest

import testdata

import pyebakup.database.backupinfo as backupinfo


class FakeDatabase(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path


class FakeTree(object):
    def __init__(self):
        self._files = {}

    def _add_file(self, path, fileobj):
        self._files[path] = fileobj

    def get_item_at_path(self, path):
        return self._files[path]

    def is_open_file_same_as_path(self, fileobj, path):
        return fileobj == self._files[path]


class FakeFile(object):
    def __init__(self, content):
        self._content = content
        self._locked_for_reading = False

    def lock_for_reading(self):
        assert not self._locked_for_reading
        self._locked_for_reading = True

    def close(self):
        self._locked_for_reading = False

    def get_size(self):
        return len(self._content)

    def get_data_slice(self, start, end):
        assert self._locked_for_reading
        return self._content[start:end]


class TestBackupInfo(unittest.TestCase):
    def setUp(self):
        self.tree = FakeTree()
        self.tree._add_file(
            ('dbpath', '2015', '04-03T10:46'),
            FakeFile(testdata.dbfiledata('backup-1')))
        self.db = FakeDatabase(self.tree, ('dbpath',))
        self.bk = backupinfo.BackupInfo(self.db, '2015-04-03T10:46')

    def test_get_start_time(self):
        self.assertEqual(
            datetime.datetime(2015, 4, 3, 10, 46, 6), self.bk.get_start_time())

    def test_get_end_time(self):
        self.assertEqual(
            datetime.datetime(2015, 4, 3, 10, 47, 59), self.bk.get_end_time())

    def test_directory_listing_of_root_directory(self):
        self.assertEqual(
            (['path'],['file']),
            self.bk.get_directory_listing(()))

    def test_directory_listing_of_directory_path(self):
        self.assertEqual(
            (['to'],[]),
            self.bk.get_directory_listing(('path',)))

    def test_directory_listing_of_directory_path_to(self):
        self.assertEqual(
            ([],['file']),
            self.bk.get_directory_listing(('path', 'to')))

    def test_is_directory_for_directory_should_be_true(self):
        self.assertTrue(self.bk.is_directory(('path', 'to')))

    def test_is_directory_for_file_should_be_false(self):
        self.assertFalse(self.bk.is_directory(('file',)))

    def test_is_file_for_directory_should_be_false(self):
        self.assertFalse(self.bk.is_file(('path', 'to')))

    def test_is_file_for_file_should_be_true(self):
        self.assertTrue(self.bk.is_file(('file',)))

    def test_get_file_info_for_directory_should_be_none(self):
        self.assertEqual(None, self.bk.get_file_info(('path', 'to')))

    def test_get_file_info_for_file(self):
        info = self.bk.get_file_info(('path', 'to', 'file',))
        self.assertEqual(
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
            info.contentid)
        self.assertEqual(7850, info.size)
        self.assertEqual(
            datetime.datetime(2015, 2, 20, 12, 53, 22, 765430), info.mtime)
        self.assertEqual(765430000, info.mtime_nsec)
        self.assertEqual('file', info.filetype)
        self.assertEqual({}, info.extra_data)

    def test_get_dir_info_for_directory(self):
        info = self.bk.get_dir_info(('path', 'to'))
        self.assertEqual({}, info.extra_data)
