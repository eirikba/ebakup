#!/usr/bin/env python3

import database

from dbfile_tests import FileData, FakeDirectory, FakeFile

import datetime
import unittest

class TestSimpleDatabase(unittest.TestCase):
    def test_read_simple_database(self):
        d = FakeDirectory()
        d._add_file(
            ('db', 'main'),
            b'ebakup database v1\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'checksum:sha256\n' +
            b'\x00' * 3990 +
            b'\xfbT\x16=\xf4\xe9j\x9fG\xdf\xbb!\xe0\xc9\xe9\xaa\xe3/'
            b'\xe9\x8e\xd5\xf5\xe4\xdc\xb1C\xbf\xd6\x03\xf2\xf0\xce')
        d._add_file(
            ('db', '2015', '04-03T10:46'),
            b'ebakup backup data\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'start:2015-04-03T10:46:06\n'
            b'end:2015-04-03T10:47:59\n' +
            b'\x00' * 3956 +
            b'\xf5\xf8\xa5\xcf\x94\xd7\x97\xd6j\xab8\xf1\xc8-&\xd50'
            b':\x9f\x8c4H\xf2\xf4\x1d\x04\xab\x8b]:\xe9\xfe'
            b'\x90\x08\x00\x04path'
            b'\x90\x09\x08\x02to'
            b'\x91\x09\x04file\x20'
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^'
            b'\xaa\x3d\xdf\x07\x42\xa0\x42\x30\x23\x7e\xb6'
            # ^ size: 7850, mtime: 2015-02-20 12:53:22.76543
            b'\x91\x00\x04file\x20'
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde'
            b'\x17\xdd\x07\xa0\xdb\x0a\x80\x00\x00\x00' +
            # ^ size: 23, mtime: 2013-07-22 10:00:00.0
            b'\x00' * 3949 +
            b'H\x15XVH\x9aJ\x019\x0e\xe8\x93%\xa7\xa4A\xaf*'
            b'\xdb\\oqU\x8eGHmxv\xc9\xdb\x15')
        d._add_file(
            ('db', 'content'),
            b'ebakup content data\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n' +
            b'\x00' * 4005 +
            b'`{\xafg\x156E\x99*\x05|\x14\xf6fg\xd3\xc4\xde\x80'
            b'\xa5g\xf1\xa0\xf8\xc28\xe4J9\xd5\xa2-'
            b'\xdd\x20\x20'
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^'
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\x09\x69\x21\x55' # 2015-04-05 16:55:37
            b'\xdd\x20\x20'
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde'
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\xa1'
            b'k\x8c\xba\x8b\x17\x8b\rL\x13\xde\xc9$<\x90\x04\xeb\xc3'
            b'\x03\xcbJ\xaf\xe93\x0c\x8d\x12^.\x94yS\xae'
            b'\x45\x30\x18\x55' # 2015-03-29 17:03:01
            b'\x4b\xea\x1b\x55' # 2015-04-01 12:53:31
            b'\xa0'
            b'\x3b\xeb\x1b\x55' # 2015-04-01 12:57:31
            b'\x09\x69\x21\x55' # 2015-04-05 16:55:37
            b'\xdd\x22\x20'
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73"
            b'\xd1\xd6\x13\x55' # 2015-03-26 09:52:17
            b'\xac\x8f\x16\x55' # 2015-03-28 11:25:32
            b'\xa1'
            b'\x01\xfa\x04^\x9c\x11\xd5\x8d\xfe\x19]}\xd1((\x0c'
            b'\x00h\xad0\x13\xa3(\xb5\xe8\xb3\xac\xa3\x9e_\xfbb'
            b'\x91\xb1\x17\x55' # 2015-03-29 08:02:25
            b'\x00\x12\x1d\x55' # 2015-04-02 09:55:12
            + b'\x00' * 3842 +
            b'\x909\xee+%\x92;A\xa3\xed\xb1\xd6\x98\x84\xfdB7\x93,'
            b'\x16\xeb7 \xfb\xc1\x00\x02\xfe\xa2\xf1\x1a\xea'
            )
        db = database.Database(d, ('db',))
        self.assertEqual('sha256', db.get_checksum_algorithm_name())
        backup = db.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(2015, 4, 3, 10, 46, 6),
            backup.get_start_time())
        self.assertEqual(
            datetime.datetime(2015, 4, 3, 10, 47, 59),
            backup.get_end_time())
        dirlist = backup.get_directory_listing(())
        self.assertCountEqual(('path',), dirlist[0])
        self.assertCountEqual(('file',), dirlist[1])
        self.assertTrue(backup.is_directory(('path',)))
        self.assertFalse(backup.is_file(('path',)))
        self.assertFalse(backup.is_directory(('file',)))
        self.assertTrue(backup.is_file(('file',)))
        dirlist = backup.get_directory_listing(('path',))
        self.assertCountEqual(('to',), dirlist[0])
        self.assertCountEqual((), dirlist[1])
        self.assertTrue(backup.is_directory(('path','to')))
        self.assertFalse(backup.is_file(('path', 'to')))
        dirlist = backup.get_directory_listing(('path','to'))
        self.assertCountEqual((), dirlist[0])
        self.assertCountEqual(('file',), dirlist[1])
        self.assertFalse(backup.is_directory(('path','to','file')))
        self.assertTrue(backup.is_file(('path', 'to','file')))
        info = backup.get_file_info(('file',))
        self.assertEqual(23, info.size)
        self.assertEqual(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
            info.contentid)
        self.assertEqual(
            datetime.datetime(2013, 7, 22, 10, 0, 0),
            info.mtime)
        self.assertEqual(0, info.mtime_nsec)
        info = backup.get_file_info(('path', 'to', 'file'))
        self.assertEqual(7850, info.size)
        self.assertEqual(
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
            info.contentid)
        self.assertEqual(
            datetime.datetime(2015, 2, 20, 12, 53, 22),
            info.mtime)
        self.assertEqual(765430000, info.mtime_nsec)
        backup = db.get_oldest_backup()
        self.assertEqual(
            datetime.datetime(2015, 4, 3, 10, 46, 6),
            backup.get_start_time())
        self.assertEqual(
            datetime.datetime(2015, 4, 3, 10, 47, 59),
            backup.get_end_time())
        info = db.get_content_info(
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^')
        self.assertNotEqual(None, info)
        self.assertEqual(
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
            info.get_good_checksum())
        self.assertEqual(
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
            info.get_last_known_checksum())
        checks = info.get_checksum_timeline()
        self.assertEqual(1, len(checks))
        self.assertEqual(
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^',
            checks[0].checksum)
        self.assertEqual(
            datetime.datetime(2015, 3, 27, 11, 35, 20), checks[0].first)
        self.assertEqual(
            datetime.datetime(2015, 4, 5, 16, 55, 37), checks[0].last)
        self.assertTrue(checks[0].restored)
        info = db.get_content_info(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde')
        self.assertEqual(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
            info.get_good_checksum())
        self.assertEqual(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
            info.get_last_known_checksum())
        self.assertEqual(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
            info.get_contentid())
        checks = info.get_checksum_timeline()
        self.assertEqual(3, len(checks))
        self.assertEqual(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
            checks[0].checksum)
        self.assertEqual(
            datetime.datetime(2015, 3, 27, 11, 35, 20), checks[0].first)
        self.assertEqual(
            datetime.datetime(2015, 3, 27, 11, 35, 20), checks[0].last)
        self.assertTrue(checks[0].restored)
        self.assertEqual(
            b'k\x8c\xba\x8b\x17\x8b\rL\x13\xde\xc9$<\x90\x04\xeb\xc3'
            b'\x03\xcbJ\xaf\xe93\x0c\x8d\x12^.\x94yS\xae',
            checks[1].checksum)
        self.assertEqual(
            datetime.datetime(2015, 3, 29, 17, 3, 1), checks[1].first)
        self.assertEqual(
            datetime.datetime(2015, 4, 1, 12, 53, 31), checks[1].last)
        self.assertFalse(checks[1].restored)
        self.assertEqual(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde',
            checks[2].checksum)
        self.assertEqual(
            datetime.datetime(2015, 4, 1, 12, 57, 31), checks[2].first)
        self.assertEqual(
            datetime.datetime(2015, 4, 5, 16, 55, 37), checks[2].last)
        self.assertTrue(checks[2].restored)
        info = db.get_content_info(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73")
        self.assertEqual(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73",
            info.get_contentid())
        self.assertEqual(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98",
            info.get_good_checksum())
        self.assertEqual(
            b'\x01\xfa\x04^\x9c\x11\xd5\x8d\xfe\x19]}\xd1((\x0c'
            b'\x00h\xad0\x13\xa3(\xb5\xe8\xb3\xac\xa3\x9e_\xfbb',
            info.get_last_known_checksum())
        checks = info.get_checksum_timeline()
        self.assertEqual(2, len(checks))
        self.assertEqual(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98",
            checks[0].checksum)
        self.assertEqual(
            datetime.datetime(2015, 3, 26, 9, 52, 17), checks[0].first)
        self.assertEqual(
            datetime.datetime(2015, 3, 28, 11, 25, 32), checks[0].last)
        self.assertTrue(checks[0].restored)
        self.assertEqual(
            b'\x01\xfa\x04^\x9c\x11\xd5\x8d\xfe\x19]}\xd1((\x0c'
            b'\x00h\xad0\x13\xa3(\xb5\xe8\xb3\xac\xa3\x9e_\xfbb',
            checks[1].checksum)
        self.assertEqual(
            datetime.datetime(2015, 3, 29, 8, 2, 25), checks[1].first)
        self.assertEqual(
            datetime.datetime(2015, 4, 2, 9, 55, 12), checks[1].last)
        self.assertFalse(checks[1].restored)
        self.assertEqual(None, db.get_content_info(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98"))
        infos = db.get_all_content_infos_with_checksum(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde')
        self.assertEqual(1, len(infos))
        infos = db.get_all_content_infos_with_checksum(
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xdd')
        self.assertEqual([], infos)
        infos = db.get_all_content_infos_with_checksum(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98")
        self.assertEqual(1, len(infos))
        self.assertEqual(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73",
            infos[0].get_contentid())
        infos = db.get_all_content_infos_with_checksum(
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73")
        self.assertEqual([], infos)

    def test_get_out_of_range_backups(self):
        d = FakeDirectory()
        d._add_file(('db', 'main'),
                    b'ebakup database v1\n'
                    b'edb-blocksize:4096\n'
                    b'edb-blocksum:sha256\n'
                    b'checksum:sha256\n'
                    b'blocksize:4096\n'
                    + b'\x00' * 3975 +
                    b'M\x99\xa2a!\xb7\xb0,\xd1\xcc^\xc4\xfb\xb4|bV\x9b'
                    b'm\x915\xc9\x02\x16\xfa\xb6\xf9\xbb\x87\x12\xad"')
        d._add_file(('db', '2005', '04-03T10:46'),
                    b'ebakup backup data\n'
                    b'edb-blocksize:4096\n'
                    b'edb-blocksum:sha256\n'
                    b'start:2005-04-03T10:46:06\n'
                    b'end:2005-04-03T10:47:59\n'
                    + b'\x00' * 3956 +
                    b'\xc9*\xe3N\xf8i\xbf\xdb\xf1C\xf4)\x81\x10\x14\xab`'
                    b'\x08\x95\xe6\x8f\xb8;\xb8\xefb\x84\n\xab\x04\x8f$'
                    b'\x90\x08\x00\x04path'
                    b'\x90\x09\x08\x02to'
                    b'\x91\x09\x04file\x20'
                    b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
                    b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^'
                    b'\xaa\x3d\xdf\x07\x42\xa0\x42\x30\x23\x7e\xb6' # size: 7850, mtime: 2015-02-20 12:53:22.76543
                    b'\x91\x00\x04file\x20'
                    b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
                    b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde'
                    b'\x17\xdd\x07\xa0\xdb\x0a\x80\x00\x00\x00' # size: 23, mtime: 2013-07-22 10:00:00.0
                    + b'\x00' * 3949 +
                    b'H\x15XVH\x9aJ\x019\x0e\xe8\x93%\xa7\xa4A\xaf*'
                    b'\xdb\\oqU\x8eGHmxv\xc9\xdb\x15'
                    )
        d._add_file(('db', 'content'),
            b'ebakup content data\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n' +
            b'\x00' * 4005 +
            b'`{\xafg\x156E\x99*\x05|\x14\xf6fg\xd3\xc4\xde'
            b'\x80\xa5g\xf1\xa0\xf8\xc28\xe4J9\xd5\xa2-')
        db = database.Database(d, ('db',))
        self.assertEqual(
            None,
            db.get_oldest_backup_after(
                datetime.datetime(2015, 4, 4, 9, 40, 0)))
        self.assertEqual(
            None,
            db.get_oldest_backup_after(
                datetime.datetime(2018, 1, 2, 3, 4, 5)))
        self.assertEqual(
            None,
            db.get_oldest_backup_after(
                datetime.datetime(2015, 4, 3, 10, 46, 6)))
        self.assertEqual(
            None,
            db.get_oldest_backup_after(
                datetime.datetime(2005, 4, 3, 10, 46, 6)))
        self.assertEqual(
            None,
            db.get_most_recent_backup_before(
                datetime.datetime(2005, 4, 2, 12, 49, 9)))
        self.assertEqual(
            None,
            db.get_most_recent_backup_before(
                datetime.datetime(2001, 8, 7, 18, 50, 24)))
        self.assertEqual(
            None,
            db.get_most_recent_backup_before(
                datetime.datetime(1881, 1, 1, 1, 1, 1)))
        self.assertEqual(
            None,
            db.get_most_recent_backup_before(
                datetime.datetime(2005, 4, 3, 10, 46, 6)))

class TestWriteDatabase(unittest.TestCase):

    def patch_one(self, name, double, create=False):
        patcher = patch(name, double, create=create)
        patcher.start()
        self.addCleanup(patcher.stop)

    def create_empty_database(self, tree, path):
        tree._allow_create_regular_file(path + ('main',))
        tree._allow_create_regular_file(path + ('main.new',))
        tree._allow_modification(path + ('main.new',))
        tree._allow_overwrite_file(path + ('main',))
        tree._allow_rename_file(path + ('main.new',))
        tree._allow_create_regular_file(path + ('content',))
        tree._allow_create_regular_file(path + ('content.new',))
        tree._allow_modification(path + ('content.new',))
        tree._allow_overwrite_file(path + ('content',))
        tree._allow_rename_file(path + ('content.new',))
        db = database.create_database(tree, ('path', 'to', 'db'))
        tree._disallow_overwrite_file(path + ('content',))
        tree._disallow_rename_file(path + ('content.new',))
        tree._disallow_modification(path + ('content.new',))
        tree._disallow_create_regular_file(path + ('content',))
        tree._disallow_create_regular_file(path + ('content.new',))
        tree._disallow_overwrite_file(path + ('main',))
        tree._disallow_rename_file(path + ('main.new',))
        tree._allow_modification(path + ('main.new',))
        tree._disallow_create_regular_file(path + ('main',))
        tree._disallow_create_regular_file(path + ('main.new',))
        return db

    def allow_create_dbfile(self, tree, path):
        newpath = path[:-1] + (path[-1] + '.new',)
        tree._allow_create_regular_file(path)
        tree._allow_create_regular_file(newpath)
        tree._allow_modification(newpath)
        tree._allow_overwrite_file(path)
        tree._allow_rename_file(newpath)

    def disallow_create_dbfile(self, tree, path):
        newpath = path[:-1] + (path[-1] + '.new',)
        tree._disallow_create_regular_file(path)
        tree._disallow_create_regular_file(newpath)
        tree._disallow_modification(newpath)
        tree._disallow_overwrite_file(path)
        tree._disallow_rename_file(newpath)

    def test_create_empty_database(self):
        tree = FakeDirectory()
        self.create_empty_database(tree, ('path', 'to', 'db'))
        self.assertCountEqual(
            (('path', 'to', 'db', 'main'),('path', 'to', 'db', 'content')),
            tree._files.keys())
        db = database.Database(tree, ('path', 'to', 'db'))
        self.assertEqual(None, db.get_most_recent_backup())
        self.assertEqual(None, db.get_oldest_backup())
        self.assertEqual('sha256', db.get_checksum_algorithm_name())
        checksum_algo = db.get_checksum_algorithm()
        self.assertNotEqual(None, checksum_algo)
        checksummer = checksum_algo()
        checksummer.update(b'Some text')
        self.assertEqual(
            b'L.\x9em\xa3\x1ad\xc7\x06#a\x9cD\x9a\x04\th\xcd'
            b'\xbe\xa8YE\xbf8O\xa3\x0e\xd2\xd5\xd2O\xa3',
            checksummer.digest())

    def test_create_database_in_existing_directory_fails(self):
        tree = FakeDirectory()
        tree._add_file(('path', 'to', 'db', 'ignore_me'), b'hi')
        self.assertRaisesRegex(
            FileExistsError, 'already exists:.*path.*to.*db',
            database.create_database, tree, ('path', 'to', 'db'))

    def test_create_database_over_existing_file_fails(self):
        tree = FakeDirectory()
        tree._add_file(('path', 'to', 'db'), b'hi')
        self.assertRaisesRegex(
            FileExistsError, 'already exists:.*path.*to.*db',
            database.create_database, tree, ('path', 'to', 'db'))

    def test_create_database_with_single_backup(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'02' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 39), b'03' + b'0' * 30)
            backup.add_file(
                ('toplevel',),
                cid, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))

        db = database.Database(tree, ('path', 'to', 'db'))
        backup = db.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 12), backup.get_start_time())
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 41), backup.get_end_time())
        dirlist = backup.get_directory_listing(())
        self.assertCountEqual(('home',), dirlist[0])
        self.assertCountEqual(('toplevel',), dirlist[1])
        self.assertTrue(backup.is_directory(('home',)))
        self.assertFalse(backup.is_file(('home',)))
        self.assertTrue(backup.is_file(('toplevel',)))
        self.assertFalse(backup.is_directory(('toplevel',)))
        dirlist = backup.get_directory_listing(('home',))
        self.assertCountEqual(('me',), dirlist[0])
        self.assertCountEqual((), dirlist[1])
        self.assertTrue(backup.is_directory(('home', 'me')))
        dirlist = backup.get_directory_listing(('home', 'me'))
        self.assertCountEqual(('important',), dirlist[0])
        self.assertCountEqual((), dirlist[1])
        self.assertTrue(backup.is_directory(('home', 'me', 'important')))
        dirlist = backup.get_directory_listing(('home', 'me', 'important'))
        self.assertCountEqual((), dirlist[0])
        self.assertCountEqual(('stuff.txt', 'other.txt'), dirlist[1])
        self.assertFalse(backup.is_directory(
            ('home', 'me', 'important', 'stuff.txt')))
        self.assertFalse(backup.is_directory(
            ('home', 'me', 'important', 'other.txt')))
        self.assertTrue(backup.is_file(
            ('home', 'me', 'important', 'stuff.txt')))
        self.assertTrue(backup.is_file(
            ('home', 'me', 'important', 'other.txt')))
        bkd = tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content
        first = bkd.find(b'\x09important')
        self.assertGreater(first, 4000)
        second = bkd.find(b'\x09important', first+1)
        self.assertEqual(-1, second)
        filedata = backup.get_file_info(('toplevel',))
        self.assertNotEqual(None, filedata)
        self.assertEqual(2323, filedata.size)
        self.assertEqual(
            datetime.datetime(2015, 4, 13, 13, 0, 0), filedata.mtime)
        self.assertEqual(397261917, filedata.mtime_nsec)
        contentinfo = db.get_content_info(filedata.contentid)
        self.assertNotEqual(None, contentinfo)
        self.assertEqual(b'03' + b'0' * 30, contentinfo.get_good_checksum())
        self.assertEqual(
            b'03' + b'0' * 30, contentinfo.get_last_known_checksum())
        timeline = contentinfo.get_checksum_timeline()
        self.assertEqual(1, len(timeline))
        self.assertEqual(True, timeline[0].restored)
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 39), timeline[0].first)
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 39), timeline[0].last)
        filedata = backup.get_file_info(
            ('home', 'me', 'important', 'stuff.txt'))
        self.assertNotEqual(None, filedata)
        self.assertEqual(111, filedata.size)
        self.assertEqual(
            datetime.datetime(2014, 9, 12, 11, 9, 15), filedata.mtime)
        self.assertEqual(0, filedata.mtime_nsec)
        contentinfo = db.get_content_info(filedata.contentid)
        self.assertNotEqual(None, contentinfo)
        self.assertEqual(b'01' + b'0' * 30, contentinfo.get_good_checksum())
        self.assertEqual(
            b'01' + b'0' * 30, contentinfo.get_last_known_checksum())
        timeline = contentinfo.get_checksum_timeline()
        self.assertEqual(1, len(timeline))
        self.assertEqual(True, timeline[0].restored)
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 36), timeline[0].first)
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 36), timeline[0].last)
        filedata = backup.get_file_info(
            ('home', 'me', 'important', 'other.txt'))
        self.assertNotEqual(None, filedata)
        self.assertEqual(2323, filedata.size)
        self.assertEqual(
            datetime.datetime(2014, 5, 5, 19, 23, 2), filedata.mtime)
        self.assertEqual(0, filedata.mtime_nsec)
        contentinfo = db.get_content_info(filedata.contentid)
        self.assertNotEqual(None, contentinfo)
        self.assertEqual(b'02' + b'0' * 30, contentinfo.get_good_checksum())
        self.assertEqual(
            b'02' + b'0' * 30, contentinfo.get_last_known_checksum())
        timeline = contentinfo.get_checksum_timeline()
        self.assertEqual(1, len(timeline))
        self.assertEqual(True, timeline[0].restored)
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 38), timeline[0].first)
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 38), timeline[0].last)
        self.assertEqual(None, backup.get_file_info(('home', 'me')))

    def test_new_database_with_a_few_files_has_small_data_files(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'02' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 39), b'03' + b'0' * 30)
            backup.add_file(
                ('toplevel',),
                cid, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        # Three entries in the content database should have plenty of
        # space in a single block. Add the initial block and there
        # should be 2 blocks of 4096 bytes in this file:
        self.assertEqual(
            8192, len(tree._files[('path', 'to', 'db', 'content')].content))

    def test_read_data_from_database_being_created(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'02' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            contentinfos = db.get_all_content_infos_with_checksum(
                b'02' + b'0' * 30)
            self.assertNotEqual(None, contentinfos)
            self.assertEqual(1, len(contentinfos))
            self.assertEqual(cid, contentinfos[0].get_contentid())
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))

    def test_add_data_with_same_checksum(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        cids = []
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid1 = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            cids.append(cid1)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid1, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid2 = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'01' + b'0' * 30)
            cids.append(cid2)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid2, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            self.assertNotEqual(cid1, cid2)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        self.assertCountEqual(cids, [x for x in db.iterate_contentids()])

    def test_database_with_multiple_backups(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))

        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2011', '08-30T04:30'))
        backup = db.start_backup(datetime.datetime(2011, 8, 30, 4, 30, 0))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid5 = db.add_content_item(
                datetime.datetime(2009, 4, 14, 21, 36, 36), b'05' + b'0' * 30)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.add_file(
                ('store', 'big'),
                cid5, 2291407333111,
                datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            backup.commit(datetime.datetime(2011, 8, 30, 5, 2, 11))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2011', '08-30T04:30'))

        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid1 = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid1, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid2 = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'02' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid2, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            cid3 = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 39), b'03' + b'0' * 30)
            backup.add_file(
                ('toplevel',),
                cid3, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))

        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-16T19:50'))
        backup = db.start_backup(datetime.datetime(2015, 4, 16, 19, 50, 36))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid4 = db.add_content_item(
                datetime.datetime(2015, 4, 16, 19, 50, 42), b'04' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid4, 5111,
                datetime.datetime(2015, 4, 16, 12, 22, 5), 121198088)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid2, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            backup.add_file(
                ('toplevel',),
                cid3, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 16, 19, 50, 55))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-16T19:50'))

        db = database.Database(tree, ('path', 'to', 'db'))

        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-16T21:02'))
        backup = db.start_backup(datetime.datetime(2015, 4, 16, 21, 2, 6))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid6 = db.add_content_item(
                datetime.datetime(2015, 4, 16, 21, 2, 11), b'06' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid6, 128,
                datetime.datetime(2015, 4, 16, 19, 58, 47), 650620639)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid2, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            backup.add_file(
                ('toplevel',),
                cid3, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 16, 19, 50, 55))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-16T19:50'))

        db = database.Database(tree, ('path', 'to', 'db'))

        backup = db.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(2015, 4, 16, 21, 2, 6), backup.get_start_time())

        backup = db.get_most_recent_backup_before(
            datetime.datetime(2015, 4, 16, 21, 2, 7))
        self.assertEqual(
            datetime.datetime(2015, 4, 16, 21, 2, 6), backup.get_start_time())

        backup = db.get_most_recent_backup_before(
            datetime.datetime(2015, 4, 16, 21, 2, 6))
        self.assertEqual(
            datetime.datetime(2015, 4, 16, 19, 50, 36), backup.get_start_time())

        backup = db.get_most_recent_backup_before(
            datetime.datetime(2015, 4, 16, 19, 50, 36))
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 12), backup.get_start_time())

        backup = db.get_most_recent_backup_before(
            datetime.datetime(2015, 4, 14, 21, 36, 12))
        self.assertEqual(
            datetime.datetime(2011, 8, 30, 4, 30, 0), backup.get_start_time())

        backup = db.get_most_recent_backup_before(
            datetime.datetime(2011, 8, 30, 4, 30, 0))
        self.assertEqual(None, backup)

        backup = db.get_most_recent_backup_before(
            datetime.datetime(1257, 8, 30, 4, 30, 0))
        self.assertEqual(None, backup)

        backup = db.get_oldest_backup()
        self.assertEqual(
            datetime.datetime(2011, 8, 30, 4, 30, 0), backup.get_start_time())

        backup = db.get_oldest_backup_after(
            datetime.datetime(2011, 8, 30, 4, 29, 59))
        self.assertEqual(
            datetime.datetime(2011, 8, 30, 4, 30, 0), backup.get_start_time())

        backup = db.get_oldest_backup_after(
            datetime.datetime(2011, 8, 30, 4, 30, 0))
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 12), backup.get_start_time())

        backup = db.get_oldest_backup_after(
            datetime.datetime(2015, 4, 14, 21, 36, 11))
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 12), backup.get_start_time())

        backup = db.get_oldest_backup_after(
            datetime.datetime(2015, 4, 14, 21, 36, 12))
        self.assertEqual(
            datetime.datetime(2015, 4, 16, 19, 50, 36), backup.get_start_time())

        backup = db.get_oldest_backup_after(
            datetime.datetime(2015, 4, 16, 19, 50, 36))
        self.assertEqual(
            datetime.datetime(2015, 4, 16, 21, 2, 6), backup.get_start_time())

        backup = db.get_oldest_backup_after(
            datetime.datetime(2015, 4, 16, 21, 2, 6))
        self.assertEqual(None, backup)

        backup = db.get_oldest_backup_after(
            datetime.datetime(2066, 8, 30, 4, 30, 0))
        self.assertEqual(None, backup)

    def test_invalid_utf8_in_file_names(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        # This is how python decodes file names to strings
        test_filename = b'INVUTF8:ab\xddcd'.decode(
            'utf-8', errors='surrogateescape')
        test_dirname = b'INVUTF8:vx\xeeyz'.decode(
            'utf-8', errors='surrogateescape')
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', test_filename),
                cid, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'02' + b'0' * 30)
            backup.add_file(
                ('home', 'me', test_dirname, 'other.txt'),
                cid, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 39), b'03' + b'0' * 30)
            backup.add_file(
                ('toplevel',),
                cid, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        self.assertIn(
            b'\x0dINVUTF8:ab\xddcd\x20',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x0dINVUTF8:vx\xeeyz',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        bk = db.get_most_recent_backup()
        self.assertCountEqual(
            (test_filename,),
            bk.get_directory_listing(('home', 'me', 'important'))[1])
        self.assertCountEqual(
            ('important', test_dirname),
            bk.get_directory_listing(('home', 'me'))[0])
        self.assertCountEqual(
            ('other.txt',),
            bk.get_directory_listing(('home', 'me', test_dirname))[1])
        fileinfo = bk.get_file_info(('home', 'me', 'important', test_filename))
        self.assertEqual(b'01' + b'0' * 30, fileinfo.contentid)
        fileinfo = bk.get_file_info(('home', 'me', test_dirname, 'other.txt'))
        self.assertEqual(b'02' + b'0' * 30, fileinfo.contentid)

    def test_multioctet_utf8_characters_in_file_names(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        test_filename = b'Seigmen-Dr\xc3\xa5ben.txt'.decode('utf-8')
        test_dirname = b'MULTI\xe5\x83\xa1UTF8'.decode('utf-8')
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            backup.add_file(
                ('home', 'me', 'important', test_filename),
                cid, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'02' + b'0' * 30)
            backup.add_file(
                ('home', 'me', test_dirname, 'other.txt'),
                cid, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 39), b'03' + b'0' * 30)
            backup.add_file(
                ('toplevel',),
                cid, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 48))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        self.assertIn(
            b'\x13Seigmen-Dr\xc3\xa5ben.txt\x2001000000000',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x0cMULTI\xe5\x83\xa1UTF8',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        bk = db.get_most_recent_backup()
        self.assertCountEqual(
            (test_filename,),
            bk.get_directory_listing(('home', 'me', 'important'))[1])
        self.assertCountEqual(
            ('important', test_dirname),
            bk.get_directory_listing(('home', 'me'))[0])
        self.assertCountEqual(
            ('other.txt',),
            bk.get_directory_listing(('home', 'me', test_dirname))[1])
        fileinfo = bk.get_file_info(('home', 'me', 'important', test_filename))
        self.assertEqual(b'01' + b'0' * 30, fileinfo.contentid)
        fileinfo = bk.get_file_info(('home', 'me', test_dirname, 'other.txt'))
        self.assertEqual(b'02' + b'0' * 30, fileinfo.contentid)

    def test_various_timestamps_for_mtime(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.add_file(
                ('file1',),
                cid, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            backup.add_file(
                ('file2',),
                cid, 111, datetime.datetime(2014, 1, 12, 11, 9, 15), 682246552)
            backup.add_file(
                ('file3',),
                cid, 111, datetime.datetime(2014, 2, 28, 11, 9, 15), 0)
            backup.add_file(
                ('file4',),
                cid, 111, datetime.datetime(2014, 3, 1, 11, 9, 15), 0)
            backup.add_file(
                ('file5',),
                cid, 111, datetime.datetime(2012, 2, 28, 11, 9, 15), 0)
            backup.add_file(
                ('file6',),
                cid, 111, datetime.datetime(2012, 2, 29, 11, 9, 15), 0)
            backup.add_file(
                ('file7',),
                cid, 111, datetime.datetime(2012, 3, 1, 11, 9, 15), 0)
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        self.assertIn(
            b'\x05file1\x20' + cid + b'\x6f'
            b'\xde\x07\xdb\x79\x4f\x80\x00\x00\x00',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x05file2\x20' + cid + b'\x6f'
            b'\xde\x07\x5b\x1d\x0f\x18\x06\xa9\xa2',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x05file3\x20' + cid + b'\x6f'
            b'\xde\x07\xdb\x13\x4d\x00\x00\x00\x00',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x05file4\x20' + cid + b'\x6f'
            b'\xde\x07\x5b\x65\x4e\x00\x00\x00\x00',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x05file5\x20' + cid + b'\x6f'
            b'\xdc\x07\xdb\x13\x4d\x00\x00\x00\x00',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x05file6\x20' + cid + b'\x6f'
            b'\xdc\x07\x5b\x65\x4e\x00\x00\x00\x00',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        self.assertIn(
            b'\x05file7\x20' + cid + b'\x6f'
            b'\xdc\x07\xdb\xb6\x4f\x00\x00\x00\x00',
            tree._files[('path', 'to', 'db', '2015', '04-14T21:36')].content)
        bk = db.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(2015, 4, 14, 21, 36, 12), bk.get_start_time())
        tests = (
            (('file1',), datetime.datetime(2014, 9, 12, 11, 9, 15), 0),
            (('file2',), datetime.datetime(2014, 1, 12, 11, 9, 15), 682246552),
            (('file3',), datetime.datetime(2014, 2, 28, 11, 9, 15), 0),
            (('file4',), datetime.datetime(2014, 3, 1, 11, 9, 15), 0),
            (('file5',), datetime.datetime(2012, 2, 28, 11, 9, 15), 0),
            (('file6',), datetime.datetime(2012, 2, 29, 11, 9, 15), 0),
            (('file7',), datetime.datetime(2012, 3, 1, 11, 9, 15), 0),
        )
        for test in tests:
            f = bk.get_file_info(test[0])
            self.assertEqual(test[1], f.mtime)
            self.assertEqual(test[2], f.mtime_nsec)

    def test_iterate_contentids(self):
        tree = FakeDirectory()
        db = self.create_empty_database(tree, ('path', 'to', 'db'))
        self.allow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))
        backup = db.start_backup(datetime.datetime(2015, 4, 14, 21, 36, 12))
        cids = []
        with backup:
            tree._allow_modification(('path', 'to', 'db', 'content'))
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 36), b'01' + b'0' * 30)
            cids.append(cid)
            backup.add_file(
                ('home', 'me', 'important', 'stuff.txt'),
                cid, 111, datetime.datetime(2014, 9, 12, 11, 9, 15), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 38), b'02' + b'0' * 30)
            cids.append(cid)
            backup.add_file(
                ('home', 'me', 'important', 'other.txt'),
                cid, 2323, datetime.datetime(2014, 5, 5, 19, 23, 2), 0)
            cid = db.add_content_item(
                datetime.datetime(2015, 4, 14, 21, 36, 39), b'03' + b'0' * 30)
            cids.append(cid)
            backup.add_file(
                ('toplevel',),
                cid, 2323, datetime.datetime(2015, 4, 13, 13, 0, 0), 397261917)
            tree._disallow_modification(('path', 'to', 'db', 'content'))
            backup.commit(datetime.datetime(2015, 4, 14, 21, 36, 41))
        self.disallow_create_dbfile(
            tree, ('path', 'to', 'db', '2015', '04-14T21:36'))

        db = database.Database(tree, ('path', 'to', 'db'))
        self.assertCountEqual(cids, [x for x in db.iterate_contentids()])

class TestDBUtils(unittest.TestCase):
    def test_mtime_to_db_codec(self):
        encode = database._make_mtime_with_nsec
        decode = lambda x: database._parse_mtime(x, 0)
        pairs = (
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 0),
            (b'\x01\x00\x01\x00\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 1), 0),
            (b'\x01\x00\x3c\x00\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 1, 0), 0),
            (b'\x01\x00\x10\x0e\x00\x00\x00\x00\x00',
             datetime.datetime(1, 1, 1, 1, 0, 0), 0),
            (b'\x01\x00\x80\x51\x01\x00\x00\x00\x00',
             datetime.datetime(1, 1, 2, 0, 0, 0), 0),
            (b'\x01\x00\x00\x8d\x27\x00\x00\x00\x00',
             datetime.datetime(1, 1, 31, 0, 0, 0), 0),
            (b'\x01\x00\x80\xde\x28\x00\x00\x00\x00',
             datetime.datetime(1, 2, 1, 0, 0, 0), 0),
            (b'\x01\x00\x80\x25\x4b\x00\x00\x00\x00',
             datetime.datetime(1, 2, 27, 0, 0, 0), 0),
            (b'\x01\x00\x00\x77\x4c\x00\x00\x00\x00',
             datetime.datetime(1, 2, 28, 0, 0, 0), 0),
            (b'\x01\x00\x80\xc8\x4d\x00\x00\x00\x00',
             datetime.datetime(1, 3, 1, 0, 0, 0), 0),
            (b'\x01\x00\x00\x00\x00\x01\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1),
            (b'\x01\x00\x00\x00\x00\x3f\x00\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 63),
            (b'\x01\x00\x00\x00\x00\x00\x01\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 64),
            (b'\x01\x00\x00\x00\x00\x00\x20\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1<<11),
            (b'\x01\x00\x00\x00\x00\x00\x80\x00\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1<<13),
            (b'\x01\x00\x00\x00\x00\x00\x00\x01\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1<<14),
            (b'\x01\x00\x00\x00\x00\x00\x00\x80\x00',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1<<21),
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x01',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1<<22),
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x40',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1<<28),
            (b'\x01\x00\x00\x00\x00\x00\x00\x00\x80',
             datetime.datetime(1, 1, 1, 0, 0, 0), 1<<29),
        )
        for pair in pairs:
            self.assertEqual(pair[0], encode(pair[1], pair[2]))
            self.assertEqual((pair[1], pair[2]), decode(pair[0]))
        mtime = datetime.datetime(2015, 1, 1, 12, 42, 18)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime, nsec), decode(encoded))
        mtime = datetime.datetime(2012, 1, 1, 12, 42, 18)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime, nsec), decode(encoded))
        mtime = datetime.datetime(2015, 6, 6, 12, 42, 18)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime, nsec), decode(encoded))
        mtime = datetime.datetime(2012, 6, 6, 12, 42, 18)
        nsec = 249778391
        encoded = encode(mtime, nsec)
        self.assertEqual((mtime, nsec), decode(encoded))
