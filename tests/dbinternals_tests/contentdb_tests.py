#!/usr/bin/env python3

import datetime
import unittest

import testdata

import dbinternals.contentdb as contentdb


class FakeDatabase(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path


class FakeTree(object):
    def __init__(self):
        self._files = {}

    def _add_file(self, path, content):
        self._files[path] = FakeFileData(content)

    def get_item_at_path(self, path):
        return FakeFile(self._files[path])

    def get_modifiable_item_at_path(self, path):
        f = FakeFile(self._files[path])
        f._modifiable = True
        return f

    def is_open_file_same_as_path(self, f, path):
        return f._is_same_as(self._files[path])


class FakeFileData(object):
    def __init__(self, content):
        self._content = content
        self._locked_for_reading = False
        self._locked_for_writing = False


class FakeFile(object):
    def __init__(self, fd):
        self._fd = fd
        self._locked_for_reading = False
        self._locked_for_writing = False
        self._modifiable = False

    def _assertLockedForReading(self):
        assert self._locked_for_reading
        assert self._fd._locked_for_reading

    def _assertLockedForWriting(self):
        self._assertLockedForReading()
        assert self._locked_for_writing
        assert self._fd._locked_for_writing

    def lock_for_reading(self):
        assert not self._locked_for_reading
        assert not self._locked_for_writing
        assert not self._fd._locked_for_reading
        assert not self._fd._locked_for_writing
        self._locked_for_reading = True
        self._fd._locked_for_reading = True

    def lock_for_writing(self):
        self.lock_for_reading()
        self._locked_for_writing = True
        self._fd._locked_for_writing = True

    def get_data_slice(self, start, end):
        self._assertLockedForReading()
        return self._fd._content[start:end]

    def write_data_slice(self, start, data):
        self._assertLockedForWriting()
        assert 0 <= start <= len(self._fd._content)
        old = self._fd._content
        self._fd._content = old[:start] + data + old[start + len(data):]

    def get_size(self):
        return len(self._fd._content)

    def close(self):
        if self._locked_for_writing:
            assert self._fd._locked_for_writing
            self._fd._locked_for_writing = False
        if self._locked_for_reading:
            assert self._fd._locked_for_reading
            self._fd._locked_for_reading = False

    def _is_same_as(self, other):
        return self._fd == other


class TestContentDB(unittest.TestCase):
    def setUp(self):
        self.tree = FakeTree()
        self.tree._add_file(
            ('db', 'content'), testdata.dbfiledata('content-1'))
        self.db = FakeDatabase(self.tree, ('db',))
        self.contentfile = contentdb.ContentInfoFile(self.db)

    def test_iterate_contentids(self):
        cids = (
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73")
        self.assertCountEqual(
            cids, [x for x in self.contentfile.iterate_contentids()])

    def test_info_for_cid(self):
        cid = (b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
               b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde')
        info = self.contentfile.get_info_for_cid(cid)
        self.assertEqual(cid, info.get_contentid())
        self.assertEqual(cid, info.get_good_checksum())
        self.assertEqual(
            datetime.datetime(2015, 3, 27, 11, 35, 20),
            info.get_first_seen_time())

    def test_get_infos_for_checksum(self):
        cid = (b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
               b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^')
        infos = self.contentfile.get_all_content_infos_with_checksum(cid)
        self.assertCountEqual((cid,), [x.get_contentid() for x in infos])

    def test_add_item(self):
        firstseen = datetime.datetime(2015, 5, 12, 6, 22, 57)
        checksum = b'new content checksum'
        cid = self.contentfile.add_content_item(firstseen, checksum)
        self.assertIn(
            b'\x14' + checksum,
            self.tree._files[('db', 'content')]._content)
        cf2 = contentdb.ContentInfoFile(self.db)
        info = cf2.get_info_for_cid(cid)
        self.assertEqual(checksum, info.get_good_checksum())
        self.assertEqual(firstseen, info.get_first_seen_time())
        self.assertEqual(cid, info.get_contentid())
