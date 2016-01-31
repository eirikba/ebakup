#!/usr/bin/env python3

import datetime
import unittest

import database
import datafile


class FakeTree(object):
    def __init__(self):
        self._dir_listings = {}

    def _set_dir_listing(self, dirname, dirs, files):
        self._dir_listings[dirname] = (dirs, files)

    def _set_dir_not_exists(self, dirname):
        self._dir_listings[dirname] = None

    def get_directory_listing(self, path):
        listing = self._dir_listings[path]
        if listing is None:
            raise FileNotFoundError('No such file or directory: ' + str(path))
        return listing


class FakeDbOpener(object):
    def __init__(self):
        self._backups = {}
        self._added_backups = []

    def _add_backup(self, name):
        bk = FakeBackup()
        self._backups[name] = bk
        return bk

    def _make_content_file(self):
        self._content_file = FakeContentFile()
        return self._content_file

    def create_backup_in_replacement_mode(self, tree, path, start):
        bk = FakeBackup()
        self._added_backups.append(bk)
        return bk

    def create_backup(self, db, when):
        bk = FakeBackupCreator(db, when)
        self._added_backups.append(bk)
        return bk

    def open_backup(self, db, name):
        assert not self._added_backups
        return self._backups[name]

    def open_raw_backup(self, tree, path, name):
        assert not self._added_backups
        bk = self._backups.get(name)
        if bk is None:
            return None
        return FakeRawBackup(bk)

    def open_main(self, tree, path):
        return FakeMain()

    def open_content_file(self, db):
        return self._content_file


class FakeMain(object):
    def __init__(self):
        self._iter = iter(self._start_iter())

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        return

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iter)

    def _start_iter(self):
        yield datafile.ItemMagic(b'ebakup database v1')
        yield datafile.ItemSetting(b'checksum', b'sha256')


class FakeContentInfo(object):
    def __init__(self, cid=None, checksum=None, firstseen=None):
        if cid is not None:
            self._cid = cid
        if checksum is not None:
            self._checksum = checksum
        if firstseen is not None:
            self._firstseen = firstseen

    def get_content_id(self):
        return self._cid

    def get_good_checksum(self):
        return self._checksum

    def get_first_seen(self):
        return self._firstseen


class FakeContentFile(object):
    def __init__(self):
        self._cids = []
        self._cid_infos = {}
        self._added_cids = []

    def _add_cid(self, cid):
        self._cids.append(cid)

    def _set_cid_info(self, cid, checksum, firstseen):
        self._cid_infos[cid] = FakeContentInfo(cid, checksum, firstseen)

    def iterate_contentids(self):
        assert not self._added_cids
        for cid in self._cids:
            yield cid

    def get_info_for_cid(self, cid):
        assert not self._added_cids
        return self._cid_infos.get(cid)

    def get_all_content_infos_with_checksum(self, cksum):
        assert not self._added_cids
        infos = []
        for info in self._cid_infos.values():
            if info._checksum == cksum:
                infos.append(info)
        return infos

    def add_content_item(self, when, checksum):
        cid = b'cid for ' + checksum
        self._added_cids.append(FakeContentInfo(cid, checksum, when))
        return cid


class FakeBackup(object):
    def __init__(self):
        self._start_time = None

    def get_start_time(self):
        return self._start_time


class FakeRawBackup(object):
    def __init__(self, bk):
        self._bk = bk


class FakeBackupCreator(object):
    def __init__(self, db, start):
        self._db = db
        self._start = start


class TestDatabaseWithOneBackup(unittest.TestCase):
    def setUp(self):
        self.tree = FakeTree()
        self.dbpath = ('dbpath',)
        self.dbopener = FakeDbOpener()
        self._make_database()
        self.db = database.Database(self.tree, self.dbpath)
        self.db._set_dbfileopener(self.dbopener)

    def _make_database(self):
        self.tree._set_dir_listing(
            self.dbpath, ('2015',), ('content', 'main'))
        self.tree._set_dir_listing(
            self.dbpath + ('2015',), (), ('06-07T09:19',))

        self.bk1 = self.dbopener._add_backup('2015-06-07T09:19')
        self.bk1._start_time = datetime.datetime(2015, 6, 7, 9, 19, 23)

        self.all_cids = (b'cid1', b'cid2', b'other cid', b'last cid')
        self.contents1 = self.dbopener._make_content_file()
        for cid in self.all_cids:
            self.contents1._add_cid(cid)
        self.contents1._set_cid_info(
            b'other cid',
            checksum=b'other checksum',
            firstseen=datetime.datetime(2015, 6, 7, 9, 19, 26))

    def test_backup_names_is_the_single_backup(self):
        self.assertCountEqual(
            ('2015-06-07T09:19',), self.db.get_all_backup_names())

    def test_oldest_backup_is_the_backup(self):
        self.assertEqual(self.db.get_oldest_backup(), self.bk1)

    def test_most_recent_backup_is_the_backup(self):
        self.assertEqual(self.db.get_most_recent_backup(), self.bk1)

    def test_most_recent_backup_before_the_backup_is_none(self):
        self.assertEqual(
            self.db.get_most_recent_backup_before(
                datetime.datetime(2015, 6, 7, 9, 19, 23)),
            None)
        self.assertEqual(
            self.db.get_most_recent_backup_before(
                datetime.datetime(2015, 6, 7, 9, 19, 22)),
            None)
        self.assertEqual(
            self.db.get_most_recent_backup_before(
                datetime.datetime(2015, 6, 7, 9, 10, 23)),
            None)
        self.tree._set_dir_not_exists(self.dbpath + ('1999',))
        self.assertEqual(
            self.db.get_most_recent_backup_before(
                datetime.datetime(1999, 8, 12, 15, 32, 44)),
            None)

    def test_most_recent_backup_before_after_the_backup_is_the_backup(self):
        self.assertEqual(
            self.db.get_most_recent_backup_before(
                datetime.datetime(2015, 6, 7, 9, 19, 24)),
            self.bk1)
        self.assertEqual(
            self.db.get_most_recent_backup_before(
                datetime.datetime(2015, 6, 7, 9, 29, 23)),
            self.bk1)
        self.tree._set_dir_not_exists(self.dbpath + ('2135',))
        self.assertEqual(
            self.db.get_most_recent_backup_before(
                datetime.datetime(2135, 4, 5, 2, 12, 18)),
            self.bk1)

    def test_oldest_backup_after_the_backup_is_none(self):
        self.assertEqual(
            self.db.get_oldest_backup_after(
                datetime.datetime(2015, 6, 7, 9, 19, 23)),
            None)
        self.assertEqual(
            self.db.get_oldest_backup_after(
                datetime.datetime(2015, 6, 7, 9, 19, 24)),
            None)
        self.assertEqual(
            self.db.get_oldest_backup_after(
                datetime.datetime(2015, 6, 7, 9, 29, 23)),
            None)
        self.tree._set_dir_not_exists(self.dbpath + ('2135',))
        self.assertEqual(
            self.db.get_oldest_backup_after(
                datetime.datetime(2135, 4, 5, 2, 12, 18)),
            None)

    def test_oldest_backup_after_before_the_backup_is_the_backup(self):
        self.assertEqual(
            self.db.get_oldest_backup_after(
                datetime.datetime(2015, 6, 7, 9, 19, 22)),
            self.bk1)
        self.assertEqual(
            self.db.get_oldest_backup_after(
                datetime.datetime(2015, 6, 7, 9, 10, 23)),
            self.bk1)
        self.tree._set_dir_not_exists(self.dbpath + ('1999',))
        self.assertEqual(
            self.db.get_oldest_backup_after(
                datetime.datetime(1999, 8, 12, 15, 32, 44)),
            self.bk1)

    def test_get_checksum_algorithm_name_is_sha256(self):
        self.assertEqual(self.db.get_checksum_algorithm_name(), 'sha256')

    def test_get_checksum_algorithm_is_sha256(self):
        algo = self.db.get_checksum_algorithm()
        self.assertEqual(algo().name, 'sha256')
        h = algo()
        h.update(b'hello')
        self.assertEqual(
            h.hexdigest(),
            '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824')

    def test_iterate_contentids_provides_the_correct_cids(self):
        self.assertCountEqual(
            tuple(x for x in self.db.iterate_contentids()),
            self.all_cids)

    def test_get_contentinfo_for_missing_cid_returns_none(self):
        self.assertEqual(None, self.db.get_content_info(b'nocid'))

    def test_get_contentinfo_provides_the_correct_data(self):
        info = self.db.get_content_info(b'other cid')
        self.assertEqual(b'other cid', info.get_content_id())
        self.assertEqual(b'other checksum', info.get_good_checksum())
        self.assertEqual(
            datetime.datetime(2015, 6, 7, 9, 19, 26), info.get_first_seen())

    def test_get_all_contentinfos_with_missing_checksum_returns_none(self):
        self.assertCountEqual(
            (), self.db.get_all_content_infos_with_checksum(b'nock'))

    def test_get_all_contentinfos_with_checksum_returns_the_correct_info(self):
        infos = self.db.get_all_content_infos_with_checksum(b'other checksum')
        self.assertEqual(1, len(infos))
        self.assertEqual(b'other cid', infos[0].get_content_id())

    def test_add_content_item_adds_a_content_item(self):
        checktime = datetime.datetime(2015, 6, 7, 9, 19, 37)
        checksum = b'new checksum'
        self.assertEqual(0, len(self.contents1._added_cids))

        cid = self.db.add_content_item(checktime, checksum)

        self.assertEqual(1, len(self.contents1._added_cids))
        info = self.contents1._added_cids[0]
        self.assertEqual(cid, info.get_content_id())
        self.assertEqual(checktime, info.get_first_seen())
        self.assertEqual(checksum, info.get_good_checksum())

    def test_get_backup_file_reader_for_name_returns_proper_file_reader(self):
        reader = self.db.get_backup_file_reader_for_name('2015-06-07T09:19')
        self.assertEqual(self.bk1, reader._bk)
        self.assertTrue(isinstance(reader, FakeRawBackup))

    def test_create_backup_file_returns_proper_object(self):
        start = datetime.datetime(2015, 6, 23, 7, 11, 50)
        self.assertEqual(0, len(self.dbopener._added_backups))

        bk = self.db.create_backup_file_in_replacement_mode(start)

        self.assertEqual(1, len(self.dbopener._added_backups))
        added = self.dbopener._added_backups[0]
        self.assertEqual(added, bk)
        self.assertTrue(isinstance(bk, FakeBackup))

    def test_start_backup_returns_proper_object(self):
        start = datetime.datetime(2015, 6, 23, 7, 11, 50)
        self.assertEqual(0, len(self.dbopener._added_backups))

        bk = self.db.start_backup(start)

        self.assertEqual(1, len(self.dbopener._added_backups))
        added = self.dbopener._added_backups[0]
        self.assertEqual(added, bk)
        self.assertTrue(isinstance(bk, FakeBackupCreator))


class TestDatabaseWithManyBackups(unittest.TestCase):
    def setUp(self):
        self.tree = FakeTree()
        self.dbpath = ('dbpath',)
        self.dbopener = FakeDbOpener()
        self._make_database()
        self.db = database.Database(self.tree, self.dbpath)
        self.db._set_dbfileopener(self.dbopener)

    def _make_database(self):
        self.tree._set_dir_listing(
            self.dbpath, ('2011', '2014', '2015'), ('content', 'main'))
        self.tree._set_dir_listing(
            self.dbpath + ('2011',), (), ('11-01T18:19',))
        self.tree._set_dir_listing(
            self.dbpath + ('2014',), (),
            ('05-09T14:39', '10-24T22:18', '12-21T23:57'))
        self.tree._set_dir_listing(
            self.dbpath + ('2015',), (),
            ('03-23T22:30', '05-21T03:35', '06-07T09:19', '06-15T00:21'))

        self.bk1 = self.dbopener._add_backup('2011-11-01T18:19')
        self.bk1._start_time = datetime.datetime(2011, 11, 1, 18, 19, 3)
        self.bk2 = self.dbopener._add_backup('2014-05-09T14:39')
        self.bk2._start_time = datetime.datetime(2014, 5, 9, 14, 39, 1)
        self.bk3 = self.dbopener._add_backup('2014-10-24T22:18')
        self.bk3._start_time = datetime.datetime(2014, 10, 24, 22, 18, 48)
        self.bk4 = self.dbopener._add_backup('2014-12-21T23:57')
        self.bk4._start_time = datetime.datetime(2014, 12, 21, 23, 57, 57)
        self.bk5 = self.dbopener._add_backup('2015-03-23T22:30')
        self.bk5._start_time = datetime.datetime(2015, 3, 23, 22, 30, 50)
        self.bk6 = self.dbopener._add_backup('2015-05-21T03:35')
        self.bk6._start_time = datetime.datetime(2015, 5, 21, 3, 35, 32)
        self.bk7 = self.dbopener._add_backup('2015-06-07T09:19')
        self.bk7._start_time = datetime.datetime(2015, 6, 7, 9, 19, 23)
        self.bk8 = self.dbopener._add_backup('2015-06-15T00:21')
        self.bk8._start_time = datetime.datetime(2015, 6, 15, 0, 21, 2)
        self.bks = (self.bk1, self.bk2, self.bk3, self.bk4, self.bk5,
            self.bk6, self.bk7, self.bk8)

        self.all_cids = (b'cid1', b'cid2', b'other cid', b'last cid')
        self.contents1 = self.dbopener._make_content_file()
        for cid in self.all_cids:
            self.contents1._add_cid(cid)
        self.contents1._set_cid_info(
            b'other cid',
            checksum=b'other checksum',
            firstseen=datetime.datetime(2015, 6, 7, 9, 19, 26))

    def test_backup_names_lists_all_backups(self):
        self.assertCountEqual(
            ('2011-11-01T18:19',
             '2014-05-09T14:39', '2014-10-24T22:18', '2014-12-21T23:57',
             '2015-03-23T22:30', '2015-05-21T03:35',
             '2015-06-07T09:19', '2015-06-15T00:21'),
            self.db.get_all_backup_names())

    def test_get_most_recent_backup_is_correct(self):
        self.assertEqual(self.bk8, self.db.get_most_recent_backup())

    def test_get_oldest_backup_is_correct(self):
        self.assertEqual(self.bk1, self.db.get_oldest_backup())

    def test_get_most_recent_backup_before_scans_through_all_backups(self):
        self.tree._set_dir_not_exists(self.dbpath + ('2112',))
        self.assertEqual(
            self.bk8,
            self.db.get_most_recent_backup_before(
                datetime.datetime(2112, 1, 1)))
        for i in range(len(self.bks) - 1):
            self.assertEqual(
                self.bks[i],
                self.db.get_most_recent_backup_before(
                    self.bks[i+1].get_start_time()), msg=i)
        self.assertEqual(
            None,
            self.db.get_most_recent_backup_before(
                self.bks[0].get_start_time()))

    def test_get_oldest_backup_after_scans_through_all_backups(self):
        self.tree._set_dir_not_exists(self.dbpath + ('1030',))
        self.assertEqual(
            self.bk1,
            self.db.get_oldest_backup_after(
                datetime.datetime(1030, 1, 1)))
        for i in range(len(self.bks) - 1):
            self.assertEqual(
                self.bks[i+1],
                self.db.get_oldest_backup_after(
                    self.bks[i].get_start_time()), msg=i)
        self.assertEqual(
            None,
            self.db.get_oldest_backup_after(
                self.bks[-1].get_start_time()))
