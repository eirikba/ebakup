#!/usr/bin/env python3

import datetime
import hashlib
import io
import unittest

import backupstorage.backupcollection as backupcollection


def raiseUnexpectedCallError():
    raise UnexpectedCallError()


class Empty(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class EmptyDirectoryStub(object):
    def does_path_exist(self, path):
        return False


class SimpleFileStub(object):
    def __exit__(self, a, b, c):
        pass

    def __enter__(self):
        return self

    def write_data_slice(self, pos, data):
        return len(data)

    def rename_without_overwrite_on_close(self, target_tree, target_path):
        pass


class SimpleDirectoryStub(object):
    def create_directory(self, path):
        pass

    def does_path_exist(self, path):
        return True

    def create_temporary_file(self, path):
        return SimpleFileStub()

    def is_same_file_system_as(self, tree):
        return True


class SimpleChecksummer(object):
    def __init__(self):
        self._state = b'cksum:'

    def update(self, data):
        if len(self._state) >= 20:
            return
        self._state += data[:20 - len(self._state)]

    def digest(self):
        return self._state[:20]


class SimpleFileItemStub(object):
    def __init__(self, content):
        self._content = content

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        pass

    def get_size(self):
        return len(self._content)

    def get_data_slice(self, start, end):
        return self._content[start:end]


class EmptyDatabaseStub(object):
    def get_most_recent_backup(self):
        return None


class FileExistsDatabaseStub(object):
    def start_backup(self, starttime):
        return FileExistsBackupStub()


class TwoBackupsDatabaseStub(object):
    def __init__(self):
        bk1 = SimpleBackupStub()
        bk1._start_time = datetime.datetime(2015, 2, 14, 19, 55, 32, 328629)
        bk2 = SimpleBackupStub()
        bk2._start_time = datetime.datetime(2015, 4, 20, 17, 0, 22, 737955)
        self._bks = [bk1, bk2]

    def get_most_recent_backup(self):
        return self._bks[-1]

    def get_oldest_backup(self):
        return self._bks[0]

    def get_most_recent_backup_before(self, when):
        prev = None
        for bk in self._bks:
            if bk._start_time >= when:
                return prev
            prev = bk
        return prev

    def get_oldest_backup_after(self, when):
        for bk in self._bks:
            if bk._start_time > when:
                return bk
        return None

    def get_backup_by_name(self, name):
        if name == '2015-02-14T19:55':
            return self._bks[0]
        if name == '2015-04-20T17:00':
            return self._bks[1]
        return None


class SimpleDatabaseSpy(object):
    def __init__(self, testcase):
        self._tc = testcase
        self._backups_created = []
        self._content_added = []

    def start_backup(self, starttime):
        bk = SimpleBackupSpy(starttime)
        self._backups_created.append(bk)
        return bk

    def get_checksum_algorithm(self):
        return lambda : SimpleChecksummer()

    def get_all_content_infos_with_checksum(self, ck):
        return []

    def add_content_item(self, now, checksum):
        self._tc.assertTrue(checksum.startswith(b'cksum:'))
        cid = b'cid:' + checksum[6:]
        self._content_added.append(cid)
        return cid


class SimpleBackupStub(object):
    def get_start_time(self):
        return self._start_time


class SimpleBackupSpy(object):
    def __init__(self, starttime):
        self._starttime = starttime
        self._files_added = []

    def add_file(self, path, cid, size, mtime, mtime_nsec, filetype, extra):
        f = Empty()
        f.path = path
        f.cid = cid
        f.size = size
        f.mtime = mtime
        f.mtime_nsec = mtime_nsec
        f.filetype = filetype
        f.extra = extra
        self._files_added.append(f)

    def commit(self, endtime):
        self._endtime = endtime

    def abort(self):
        pass


class BackupDummy(object):
    def abort(self):
        pass


class FileExistsBackupStub(BackupDummy):
    def add_file(self, path, cid, size, mtime, mtime_nsec, filetype, extra):
        raise FileExistsError('File already exists: ' + str(path))


class SimpleDirectorySpy(object):
    def __init__(self):
        self._commands = []

    def make_cheap_copy(self, source, target):
        self._commands.append(('cheap copy', source, target))


class BasicBackupDirectoryStub(SimpleDirectoryStub):
    def get_item_at_path(self, path):
        if path == ('backup', 'content', '63', '69',
                '643a' + '313237' * 4 + '3132'):
            return SimpleFileItemStub(b'127' * 42 + b'1')
        return None


class BasicBackupStub(object):
    def __init__(self):
        self._file_infos = {
            ('homedir', 'file.txt'): Empty(
                _content=b'127' * 42 + b'1',
                mtime=datetime.datetime(2014, 9, 11, 9, 3, 54),
                mtime_nsec=759831036,
                filetype='file',
                extra_data={}),
            ('homedir', 'other.txt'): Empty(
                _content=b'7029' * 1757 + b'7',
                mtime=datetime.datetime(2015, 2, 1, 22, 43, 34),
                mtime_nsec=51746409,
                filetype='file',
                extra_data={}),
            ('outside', 'store', 'deep', 'data'): Empty(
                _content=b'5028' * 1257,
                mtime=datetime.datetime(2014, 4, 21, 8, 29, 46),
                mtime_nsec=826447,
                filetype='file',
                extra_data={}),
            ('toplevel',): Empty(
                _content=b'21516' * 4303 + b'2',
                mtime=datetime.datetime(2014, 10, 17, 15, 33, 2),
                mtime_nsec=781606397,
                filetype='file',
                extra_data={}),
            ('homedir', 'copy'): Empty(
                _content=b'127' * 42 + b'1',
                mtime=datetime.datetime(2014, 9, 22, 2, 11, 1),
                mtime_nsec=797641421,
                filetype='file',
                extra_data={}),
            }
        for fi in self._file_infos.values():
            fi.contentid = b'cid:' + fi._content[:14]
            fi.size = len(fi._content)

    def get_file_info(self, path):
        return self._file_infos.get(path)

    def get_directory_listing(self, path):
        if path == ():
            return ('homedir', 'outside'), ('toplevel',)
        if path == ('homedir',):
            return (), ('file.txt', 'other.txt', 'copy')
        if path == ('outside',):
            return ('store',), ()
        if path == ('outside', 'store'):
            return ('deep',), ()
        if path == ('outside', 'store', 'deep'):
            return (), ('data',)
        return (), ()

    def get_start_time(self):
        return datetime.datetime(2015, 2, 14, 19, 55, 32, 328629)

    def get_end_time(self):
        return datetime.datetime(2015, 2, 14, 19, 55, 54, 954321)


class BasicBackupDatabaseStub(object):
    def get_checksum_algorithm(self):
        return lambda : SimpleChecksummer()

    def get_backup_by_name(self, name):
        if name == '2015-02-14T19:55':
            return BasicBackupStub()
        return None

    def get_most_recent_backup(self):
        return BasicBackupStub()

    def get_oldest_backup(self):
        return BasicBackupStub()

    def get_most_recent_backup_before(self, when):
        bk = BasicBackupStub()
        if when > bk.get_start_time():
            return bk
        return None

    def get_oldest_backup_after(self, when):
        bk = BasicBackupStub()
        if when < bk.get_start_time():
            return bk
        return None

    def get_all_content_infos_with_checksum(self, cksum):
        if cksum == b'cksum:' + b'127' * 4 + b'12':
            ci = Empty()
            ci.get_contentid = lambda: b'cid:' + b'127' * 4 + b'12'
            return (ci,)
        raise NotImplementedError('unexpected checksum')

    def add_content_item(self, when, cksum):
        assert cksum == b'cksum:' + b'127' * 4 + b'12'
        return b'cid:' + b'127' * 4 + b'123'

    def get_content_info(self, cid):
        if cid == b'cid:' + b'127' * 4 + b'12':
            ci = Empty()
            ci.get_good_checksum = lambda: b'cksum:' + b'127' * 4 + b'12'
            ci.get_first_seen_time = lambda: datetime.datetime(
                2015, 2, 14, 19, 56, 7)
            return ci
        if cid == b'cid:' + b'7029' * 3 + b'70':
            ci = Empty()
            ci.get_good_checksum =lambda: b'cksum:' + b'7029' * 3 + b'70'
            return ci
        return None


class TestUtilities(unittest.TestCase):
    def test_make_path_from_contentid_in_new_collection(self):
        # _make_path_from_contentid() is currently slightly broken in
        # that it doesn't check the existing splitting choices. Nor
        # does it make any attempt at optimized number of splits.
        # However, for a clean, new collection, I think the strategy
        # in this test makes sense anyway.
        services = {
            'database.open': lambda tree, path: BackupDummy(),
            'database.create': raiseUnexpectedCallError
            }
        bc = backupcollection.open_collection(
            SimpleDirectoryStub(), ('path', 'to', 'store'), services=services)
        mkpath = bc._make_path_from_contentid

        self.assertEqual(
            ('path', 'to', 'store', 'content', '00', '01', '0203'),
            mkpath(b'\x00\x01\x02\x03'))
        self.assertEqual(
            ('path', 'to', 'store', 'content', '61', '62', '63646566676869'),
            mkpath(b'abcdefghi'))
        self.assertEqual(
            ('path', 'to', 'store', 'content', '6c', 'a4',
             '98884051015ba8bba86e70ffea620166e65ef7c86d4e9400dfdb340a7364d2'),
            mkpath(b'l\xa4\x98\x88@Q\x01[\xa8\xbb\xa8np\xff\xeab\x01f'
                   b'\xe6^\xf7\xc8mN\x94\x00\xdf\xdb4\nsd\xd2'))


class TestCreateBasicBackup(unittest.TestCase):
    def test_create_backup(self):
        db = SimpleDatabaseSpy(self)
        services = {
            'database.open': lambda tree, path: db,
            'database.create': raiseUnexpectedCallError
            }
        bc = backupcollection.open_collection(
            SimpleDirectoryStub(), ('backup',), services=services)
        backup = bc.start_backup(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629))
        with backup:
            content = [
                b'127' * 42 + b'1',
                b'7029' * 1757 + b'7',
                b'5028' * 1257,
                b'21516' * 4303 + b'2',
                b'127' * 42 + b'1' ]
            cids = []
            cids.append(bc.add_content(
                SimpleFileItemStub(content[0]),
                now=datetime.datetime(2015, 2, 14, 19, 56, 7)))
            backup.add_file(
                ('homedir', 'file.txt'), cids[-1], 127,
                datetime.datetime(2014, 9, 11, 9, 3, 54), 759831036)
            cids.append(bc.add_content(SimpleFileItemStub(content[1])))
            backup.add_file(
                ('homedir', 'other.txt'), cids[-1], 7029,
                datetime.datetime(2015, 2, 1, 22, 43, 34), 51746409)
            cids.append(bc.add_content(SimpleFileItemStub(content[2])))
            backup.add_file(
                ('outside', 'store', 'deep', 'data'), cids[-1], 5028,
                datetime.datetime(2014, 4, 21, 8, 29, 46), 826447)
            cids.append(bc.add_content(SimpleFileItemStub(content[3])))
            backup.add_file(
                ('toplevel',), cids[-1], 21516,
                datetime.datetime(2014, 10, 17, 15, 33, 2), 781606397)
            cids.append(bc.add_content(SimpleFileItemStub(content[4])))
            backup.add_file(
                ('homedir', 'copy'), cids[-1], 127,
                datetime.datetime(2014, 9, 22, 2, 11, 1), 797641421)
            backup.commit(datetime.datetime(2015, 2, 14, 19, 55, 54, 954321))
        self.assertEqual(1, len(db._backups_created))
        self.assertEqual(5, len(db._content_added))
        expectedcids = [ b'cid:' + x[:14] for x in content ]
        for expected, cid in zip(expectedcids, cids):
            self.assertEqual(expected, cid)
        for expected, added in zip(expectedcids, db._content_added):
            self.assertEqual(expected, added)
        bk = db._backups_created[0]
        self.assertEqual(5, len(bk._files_added))
        for expected, added in zip(expectedcids, bk._files_added):
            self.assertEqual(expected, added.cid)


class TestBasicBackup(unittest.TestCase):
    def setUp(self):
        self.storetree = SimpleDirectorySpy()
        services = {
            'database.open': lambda tree, path: BasicBackupDatabaseStub(),
            'database.create': raiseUnexpectedCallError,
            }
        self.backupcollection = backupcollection.open_collection(
            BasicBackupDirectoryStub(), ('backup',), services=services)

    def test_backup_sequence(self):
        backup = self.backupcollection.get_most_recent_backup()
        self.assertNotEqual(None, backup)
        oldest = self.backupcollection.get_oldest_backup()
        self.assertEqual(backup.get_start_time(), oldest.get_start_time())
        self.assertEqual(
            None,
            self.backupcollection.get_most_recent_backup_before(
                backup.get_start_time()))
        self.assertEqual(
            None,
            self.backupcollection.get_oldest_backup_after(
                backup.get_start_time()))

    def test_backup_start_time(self):
        backup = self.backupcollection.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629),
            backup.get_start_time())

    def test_backup_end_time(self):
        backup = self.backupcollection.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(2015, 2, 14, 19, 55, 54, 954321),
            backup.get_end_time())

    def test_list_directory(self):
        backup = self.backupcollection.get_most_recent_backup()
        dirs, files = backup.list_directory(())
        self.assertCountEqual(('homedir', 'outside'), dirs)
        self.assertCountEqual(('toplevel',), files)
        dirs, files = backup.list_directory(('homedir',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('file.txt', 'other.txt', 'copy'), files)
        dirs, files = backup.list_directory(('outside',))
        self.assertCountEqual(('store',), dirs)
        self.assertCountEqual((), files)
        dirs, files = backup.list_directory(('outside','store'))
        self.assertCountEqual(('deep',), dirs)
        self.assertCountEqual((), files)
        dirs, files = backup.list_directory(('outside','store','deep'))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('data',), files)
        dirs, files = backup.list_directory(('not-on-toplevel',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual((), files)
        dirs, files = backup.list_directory(('toplevel',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual((), files)

    def test_get_file_info(self):
        backup = self.backupcollection.get_most_recent_backup()
        info = backup.get_file_info(('homedir', 'other.txt'))
        bk = BasicBackupStub()
        self.assertEqual(
            b'cid:' + b'7029' * 3 + b'70',
            info.contentid)
        self.assertEqual(7029, info.size)
        self.assertEqual(datetime.datetime(2015, 2, 1, 22, 43, 34), info.mtime)
        self.assertEqual(51746409, info.mtime_nsec)
        self.assertEqual(b'cksum:' + b'7029' * 3 + b'70', info.good_checksum)
        info = backup.get_file_info(('homedir', 'first.txt'))
        self.assertEqual(None, info)
        info = backup.get_file_info(('homedir', 'file.txt'))
        self.assertEqual(b'cid:' + b'127' * 4 + b'12', info.contentid)
        self.assertEqual(127, info.size)
        self.assertEqual(datetime.datetime(2014, 9, 11, 9, 3, 54), info.mtime)
        self.assertEqual(759831036, info.mtime_nsec)
        self.assertEqual(b'cksum:' + b'127' * 4 + b'12', info.good_checksum)

    def test_get_content_info(self):
        bc = self.backupcollection
        info = bc.get_content_info(b'cid:' + b'127' * 4 + b'12')
        self.assertEqual(b'cksum:' + b'127' * 4 + b'12', info.goodsum)
        self.assertEqual(
            datetime.datetime(2015, 2, 14, 19, 56, 7), info.first_seen)

    def test_get_content_reader(self):
        bc = self.backupcollection
        reader = bc.get_content_reader(b'cid:' + b'127' * 4 + b'12')
        self.assertNotEqual(None, reader)
        self.assertEqual(127, reader.get_size())
        data = reader.get_data_slice(0, 1024)
        self.assertEqual(data, b'127' * 42 + b'1')

    def test_add_duplicate_content(self):
        bk = BasicBackupStub()
        contentid = self.backupcollection.add_content(
            SimpleFileItemStub(
                bk._file_infos[('homedir', 'file.txt')]._content),
            now=datetime.datetime(2015, 2, 18, 5, 27, 43))
        self.assertEqual(b'cid:' + b'127' * 4 + b'12', contentid)

    def test_add_content_with_same_checksum(self):
        bk = BasicBackupStub()
        contentid = self.backupcollection.add_content(
            SimpleFileItemStub(
                bk._file_infos[('homedir', 'file.txt')]._content + b'--'),
            now=datetime.datetime(2015, 2, 18, 5, 27, 43))
        self.assertEqual(b'cid:' + b'127' * 4 + b'123', contentid)

    def test_make_shadow_copy(self):
        bk = self.backupcollection.get_backup_by_name('2015-02-14T19:55')
        info = bk.get_file_info(('homedir', 'other.txt'))
        self.backupcollection.make_shadow_copy(
            info, self.storetree, ('path', 'to', 'shadow', 'other.txt'))
        contentpath = self.backupcollection._make_path_from_contentid(
            info.contentid)
        self.assertEqual(('backup', 'content'), contentpath[:2])
        self.assertEqual(
            [('cheap copy',
              ('backup', 'content', '63', '69',
               '643a' + '37303239' * 3 + '3730'),
              ('path', 'to', 'shadow', 'other.txt'))],
            self.storetree._commands)

    @unittest.skip('look into reviving when verification data is in place')
    def test_checksum_timeline(self):
        bc = backupcollection.open_collection(
            self.storetree, ('path', 'to', 'store'), services=self.services)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 2, 15, 8, 4, 32), self.checksum1)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 2, 15, 12, 9, 2), self.checksum1)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 2, 17, 9, 9, 11), self.checksum1)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 2, 22, 12, 18, 41), b'1' * 32)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 2, 27, 23, 0, 5), b'1' * 32)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 3, 4, 18, 35, 12), b'1' * 32)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 3, 7, 10, 23, 55), b'2' * 32)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 3, 9, 4, 13, 18), b'3' * 32)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 3, 11, 12, 18, 3),
            self.checksum1, restored=True)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 3, 12, 7, 22, 7), self.checksum1)
        bc.update_content_checksum(
            self.cid1, datetime.datetime(2015, 3, 16, 9, 52, 14), b'4' * 32)

        info = self.backupcollection.get_content_info(self.cid1)
        self.assertEqual(self.checksum1, info.goodsum)
        self.assertEqual(b'4' * 32, info.lastsum)

        self.assertEqual(6, len(info.timeline))
        cs = info.timeline[0]
        self.assertEqual(self.checksum1, cs.checksum)
        self.assertTrue(cs.restored)
        self.assertEqual(datetime.datetime(2015, 2, 14, 19, 56, 7), cs.first)
        self.assertEqual(datetime.datetime(2015, 2, 17, 9, 9, 11), cs.last)
        cs = info.timeline[1]
        self.assertEqual(b'1' * 32, cs.checksum)
        self.assertFalse(cs.restored)
        self.assertEqual(datetime.datetime(2015, 2, 22, 12, 18, 41), cs.first)
        self.assertEqual(datetime.datetime(2015, 3, 4, 18, 35, 12), cs.last)
        cs = info.timeline[2]
        self.assertEqual(b'2' * 32, cs.checksum)
        self.assertFalse(cs.restored)
        self.assertEqual(datetime.datetime(2015, 3, 7, 10, 23, 55), cs.first)
        self.assertEqual(datetime.datetime(2015, 3, 7, 10, 23, 55), cs.last)
        cs = info.timeline[3]
        self.assertEqual(b'3' * 32, cs.checksum)
        self.assertFalse(cs.restored)
        self.assertEqual(datetime.datetime(2015, 3, 9, 4, 13, 18), cs.first)
        self.assertEqual(datetime.datetime(2015, 3, 9, 4, 13, 18), cs.last)
        cs = info.timeline[4]
        self.assertEqual(self.checksum1, cs.checksum)
        self.assertTrue(cs.restored)
        self.assertEqual(datetime.datetime(2015, 3, 11, 12, 18, 3), cs.first)
        self.assertEqual(datetime.datetime(2015, 3, 12, 7, 22, 7), cs.last)
        cs = info.timeline[5]
        self.assertEqual(b'4' * 32, cs.checksum)
        self.assertFalse(cs.restored)
        self.assertEqual(datetime.datetime(2015, 3, 16, 9, 52, 14), cs.first)
        self.assertEqual(datetime.datetime(2015, 3, 16, 9, 52, 14), cs.last)


class TestTwoBackups(unittest.TestCase):
    def setUp(self):
        services = {
            'database.open': lambda tree, path: TwoBackupsDatabaseStub(),
            'database.create': raiseUnexpectedCallError,
            }
        self.backupcollection = backupcollection.open_collection(
            SimpleDirectoryStub(), ('path', 'to', 'store'), services=services)

    def test_backup_sequence(self):
        backup = self.backupcollection.get_most_recent_backup()
        self.assertNotEqual(None, backup)
        self.assertEqual(
            datetime.datetime(2015, 4, 20, 17, 0, 22, 737955),
            backup.get_start_time())
        oldest = self.backupcollection.get_oldest_backup()
        self.assertEqual(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629),
            oldest.get_start_time())
        recent2 = self.backupcollection.get_most_recent_backup_before(
            backup.get_start_time())
        old2 = self.backupcollection.get_oldest_backup_after(
            oldest.get_start_time())
        self.assertEqual(backup.get_start_time(), old2.get_start_time())
        self.assertEqual(oldest.get_start_time(), recent2.get_start_time())
        self.assertEqual(
            None,
            self.backupcollection.get_most_recent_backup_before(
                recent2.get_start_time()))
        self.assertEqual(
            None,
            self.backupcollection.get_oldest_backup_after(
                old2.get_start_time()))

    def test_get_backup_by_name(self):
        backup = self.backupcollection.get_backup_by_name('2015-04-20T17:00')
        self.assertEqual(
            datetime.datetime(2015, 4, 20, 17, 0, 22, 737955),
            backup.get_start_time())
        backup = self.backupcollection.get_backup_by_name('2015-06-18T12:33')
        self.assertEqual(None, backup)
        backup = self.backupcollection.get_backup_by_name('2015-02-14T19:55')
        self.assertEqual(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629),
            backup.get_start_time())
        backup = self.backupcollection.get_backup_by_name('2012-06-18T12:33')
        self.assertEqual(None, backup)


class TestSingleStuff(unittest.TestCase):
    def test_default_start_and_end_time(self):
        db = SimpleDatabaseSpy(self)
        services = {
            'database.open': lambda tree, path: db,
            'database.create': raiseUnexpectedCallError,
            }
        bc = backupcollection.open_collection(
            SimpleDirectoryStub(), ('backup',), services=services)

        before_backup = datetime.datetime.utcnow()
        backup = bc.start_backup()
        after_backup_started = datetime.datetime.utcnow()
        with backup:
            cid = bc.add_content_data(
                b'some data', now=datetime.datetime.utcnow())
            backup.add_file(
                ('homedir', 'file.txt'), cid, 127,
                datetime.datetime(2014, 9, 11, 9, 3, 54), 759831036)
            before_backup_committed = datetime.datetime.utcnow()
            backup.commit()
        after_backup_committed = datetime.datetime.utcnow()

        self.assertEqual(1, len(db._backups_created))
        bk = db._backups_created[0]
        self.assertLessEqual(before_backup, bk._starttime)
        self.assertLessEqual(bk._starttime, after_backup_started)
        self.assertLessEqual(before_backup_committed, bk._endtime)
        self.assertLessEqual(bk._endtime, after_backup_committed)

    def test_get_most_recent_backup_when_no_backup_available(self):
        services = {
            'database.open': lambda tree, path: EmptyDatabaseStub(),
            'database.create': raiseUnexpectedCallError,
            }
        bc = backupcollection.open_collection(
            SimpleDirectoryStub(), ('backup',), services=services)
        self.assertEqual(None, bc.get_most_recent_backup())

    def test_add_content_data(self):
        db = SimpleDatabaseSpy(self)
        services = {
            'database.open': lambda tree, path: db,
            'database.create': raiseUnexpectedCallError,
            }
        bc = backupcollection.open_collection(
            SimpleDirectoryStub(), ('backup',), services=services)
        backup = bc.start_backup(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629))
        with backup:
            cid1 = bc.add_content_data(
                b'This is some content!',
                now=datetime.datetime(2015, 2, 14, 19, 56, 7))
            mtime1 = datetime.datetime(2014, 9, 11, 9, 3, 54)
            mtime1_nsec = 759831036
            backup.add_file(
                ('homedir', 'file.txt'), cid1, 21, mtime1, mtime1_nsec)
            backup.commit()
        self.assertEqual(1, len(db._backups_created))
        bk = db._backups_created[0]
        self.assertEqual(1, len(db._content_added))
        self.assertEqual(b'cid:This is some c', db._content_added[0])
        self.assertEqual(1, len(bk._files_added))
        f = bk._files_added[0]
        self.assertEqual(('homedir', 'file.txt'), f.path)
        self.assertEqual(cid1, f.cid)
        self.assertEqual(21, f.size)
        self.assertEqual(mtime1, f.mtime)
        self.assertEqual(mtime1_nsec, f.mtime_nsec)
        self.assertEqual('file', f.filetype)
        self.assertEqual({}, f.extra)


class TestBrokenUsage(unittest.TestCase):

    def test_add_two_files_with_same_path(self):
        services = {
            'database.open': lambda tree, path: FileExistsDatabaseStub(),
            'database.create': raiseUnexpectedCallError,
            }
        bc = backupcollection.open_collection(
            SimpleDirectoryStub(), ('backup',), services=services)
        backup = bc.start_backup(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629))
        with backup:
            cid = b'one cid'
            self.assertRaisesRegex(
                FileExistsError, 'already exists.*file\\.txt',
                backup.add_file,
                ('homedir', 'file.txt'), cid, 127,
                datetime.datetime(2014, 9, 11, 9, 3, 54), 759831036)

    def test_open_collection_that_does_not_exist(self):
        self.assertRaisesRegex(
            FileNotFoundError,
            'Backup collection does not exist.*path.*to.*store',
            backupcollection.open_collection,
            EmptyDirectoryStub(), ('path', 'to', 'store'), services={})
