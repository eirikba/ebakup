#!/usr/bin/env python3

import datetime
import hashlib
import io
import unittest

import backupcollection

class FileData(object):
    def __init__(self, **kwargs):
        self.content_type = None
        for k,v in kwargs.items():
            if k not in (
                    'content', 'content_generator',
                    'mtime', 'mtime_ns', 'checksum'):
                raise AssertionError('Unknown arg for FileData: ' + k)
            if k == 'content_generator' or k == 'content':
                self.content_type = k
                assert not hasattr(self, 'content_generator')
                assert not hasattr(self, 'content')
            setattr(self, k, v)
            self.generate_missing_data()

    def generate_missing_data(self):
        if self.content_type == 'content':
            self.checksum = hashlib.sha256(self.content).digest()

class FakeDirectory(object):

    def __init__(self):
        self._files = {}
        self._directories = set()
        self._tempfilecounter = 0

    def _set_file(self, path, **kwargs):
        for i in range(1, len(path)-1):
            self._directories.add(path[:i])
        self._files[path] = FileData(**kwargs)

    def _get_checksum(self, path):
        return self._files[path].checksum

    def does_path_exist(self, path):
        if path in self._files:
            return True
        if path in self._directories:
            return True
        return False

    def create_directory(self, path):
        self._verify_path_does_not_exist(path)
        self._ensure_parent_directory(path)
        self._directories.add(path)

    def _verify_path_does_not_exist(self, path):
        if path in self._files:
            raise FileExistsError(
                'Path already exists and is a file: ' + str(path))
        if path in self._directories:
            raise FileExistsError(
                'Path already exists and is a directory: ' + str(path))

    def _ensure_parent_directory(self, path):
        for i in range(1, len(path)-2):
            if path[:i] in self._files:
                raise NotADirectoryError(
                    'Path is not a directory: ' + str(path[:i]))
        for i in range(1, len(path)-2):
            self._directories.add(path[:i])

    def create_temporary_file(self, path):
        self._verify_path_does_not_exist(path)
        self._ensure_parent_directory(path)
        tmpname = 'tmp' + str(self._tempfilecounter)
        while path + (tmpname,) in self._files:
            self._tempfilecounter += 1
            tmpname = 'tmp' + str(self._tempfilecounter)
        tmppath = path + (tmpname,)
        tempfile = FakeTempFile(self, tmppath)
        self._files[tmppath] = FileData(content=b'')
        return tempfile

    def make_cheap_copy(self, sourcepath, targetpath):
        if sourcepath not in self._files:
            raise FileNotFoundError('Source does not exist: ' + str(sourcepath))
        self._verify_path_does_not_exist(targetpath)
        self._ensure_parent_directory(targetpath)
        self._files[targetpath] = self._files[sourcepath]

    def get_item_at_path(self, path):
        if path in self._files:
            return FakeFile(self, path)
        if path in self._directories:
            raise NotImplementedError('Fake directories not implemented')
        return None

    def get_modifiable_item_at_path(self, path):
        f = self.get_item_at_path(path)
        f.writable = True
        return f

    def rename_without_overwrite(self, sourcepath, targetpath):
        if sourcepath not in self._files:
            raise FileNotFoundError('Source does not exist: ' + str(sourcepath))
        self._verify_path_does_not_exist(targetpath)
        self._ensure_parent_directory(targetpath)
        self._files[targetpath] = self._files[sourcepath]
        del self._files[sourcepath]


class FakeFile(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        self._writable = False

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def get_size(self):
        return len(self._tree._files[self._path].content)

    def close(self):
        pass

    def get_data_slice(self, start, end):
        assert start >= 0
        assert end >= 0
        return self._tree._files[self._path].content[start:end]

    def write_data_slice(self, start, data):
        if not self._writable:
            raise io.UnsupportedOperation('write')
        fd = self._tree._files[self._path]
        assert start >= 0
        assert start <= len(fd.content)
        fd.content = fd.content[:start] + data + fd.content[start+len(data):]
        return start + len(data)


class FakeTempFile(FakeFile):
    def __init__(self, tree, path):
        FakeFile.__init__(self, tree, path)
        self._writable = True
        self._rename_path = None

    def rename_without_overwrite_on_close(self, tree, path):
        if tree != self._tree:
            raise AssertionError('Requested rename to other tree')
        self._rename_path = path

    def close(self):
        if self._rename_path is not None:
            self._tree.rename_without_overwrite(self._path, self._rename_path)
        FakeFile.close(self)


class FakeDatabases(object):
    def __init__(self):
        self.databases = []

    def open(self, tree, path):
        for cand in self.databases:
            if cand._tree == tree and cand._path == path:
                return cand
        raise AssertionError('No database at ' + str(tree) + ', ' + str(path))

    def create(self, tree, path):
        assert path not in tree._files
        assert path not in tree._directories
        tree.create_directory(path)
        db = FakeDatabase(tree, path)
        self.databases.append(db)
        return db


class FakeDatabase(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        assert path in tree._directories
        self._backups = []
        self._content = {}

    def start_backup(self, start_time):
        return FakeBackupBuilder(self, start_time)

    def _add_backup(self, bkup):
        self._backups.append(bkup)
        self._backups.sort(key=lambda x: x._start_time)

    def get_most_recent_backup(self):
        if not self._backups:
            return None
        return self._backups[-1]

    def add_content_item(self, when, checksum):
        content_id_base = checksum[:4] + b'ci' + checksum[4:]
        content_id = content_id_base
        altcount = 0
        while content_id in self._content:
            content_id = content_id_base + str(altcount).encode('utf8')
        self._content[content_id] = FakeContentInfo(content_id, when, checksum)
        return content_id

    def get_content_info(self, content_id):
        return self._content.get(content_id)

    def get_all_content_infos_with_checksum(self, checksum):
        return [x for x in self._content.values()
                if x.get_good_checksum() == checksum ]

    def get_checksum_algorithm(self):
        return hashlib.sha256

class FakeBackupBuilder(object):

    def __init__(self, db, start_time):
        self._db = db
        self._backup = FakeBackup(start_time)

    def add_file(self, path, content_id, size, mtime, mtime_nsec):
        if path in self._backup._files:
            raise FileExistsError('File already exists: ' + str(path))
        for i in range(1, len(path)):
            if path[:i] in self._backup._files:
                raise NotADirectoryError(
                    'Path is not a directory: ' + str(path[:i]))
            self._backup._directories.add(path[:i])
        self._backup._files[path] = FakeFileData(
            content_id, size, mtime, mtime_nsec)

    def commit(self, end_time):
        backup = self._backup
        self._backup = None
        assert backup
        backup._end_time = end_time
        self._db._add_backup(backup)

    def abort(self):
        self._done = True

class FakeBackup(object):
    def __init__(self, start_time):
        self._start_time = start_time
        self._end_time = None
        self._files = {}
        self._directories = set()

    def get_start_time(self):
        return self._start_time

    def get_end_time(self):
        return self._end_time

    def get_directory_listing(self, path):
        dirs = [ x[-1] for x in self._directories if x[:-1] == path ]
        files = [ x[-1] for x in self._files if x[:-1] == path ]
        return dirs, files

    def get_file_info(self, path):
        return self._files.get(path)

class FakeContentInfo(object):
    def __init__(self, content_id, when, checksum):
        self._content_id = content_id
        self._timeline = [FakeContentTimelineItem(checksum, when, when, True)]

    def get_content_id(self):
        return self._content_id

    def get_good_checksum(self):
        return self._timeline[0].checksum

    def get_last_known_checksum(self):
        return self._timeline[-1].checksum

    def get_checksum_timeline(self):
        return self._timeline

    def register_checksum(self, when, checksum):
        if when < self._timeline[-1].last:
            raise AssertionError(
                'Did not expect checksums registered non-chronologically')
        if checksum == self._timeline[-1].checksum:
            self._timeline[-1].last = when
        else:
            self._timeline.append(
                FakeContentTimelineItem(checksum, when, when, False))

    def register_content_recovered(self, when, checksum):
        if when < self._timeline[-1].last:
            raise AssertionError(
                'Did not expect checksums registered non-chronologically')
        if checksum != self.get_good_checksum():
            raise AssertionError(
                'Recovered content must have checksum matching original')
        if (checksum == self._timeline[-1].checksum and
                self._timeline[-1].restored):
            self._timeline[-1].last = when
        else:
            self._timeline.append(
                FakeContentTimelineItem(checksum, when, when, True))


class FakeContentTimelineItem(object):
    def __init__(self, checksum, first, last, restored):
        self.checksum = checksum
        self.first = first
        self.last = last
        self.restored = restored

class FakeFileData(object):
    def __init__(self, content_id, size, mtime, mtime_nsec):
        self.contentid = content_id
        self.size = size
        self.mtime = mtime
        self.mtime_nsec = mtime_nsec

class TestUtilities(unittest.TestCase):
    def test_make_path_from_content_id_in_new_collection(self):
        # _make_path_from_content_id() is currently slightly broken in
        # that it doesn't check the existing splitting choices. Nor
        # does it make any attempt at optimized number of splits.
        # However, for a clean, new collection, I think the strategy
        # in this test makes sense anyway.
        storetree = FakeDirectory()
        sourcetree = FakeDirectory()
        db = FakeDatabases()
        bcfactory = backupcollection.BackupCollectionFactory(
            storetree, ('path', 'to', 'store'))
        bcfactory.set_database_opener(db.open)
        bcfactory.set_database_creator(db.create)
        bc = bcfactory.create_collection()
        mkpath = bc._make_path_from_content_id

        self.assertEqual(('00', '01', '0203'), mkpath(b'\x00\x01\x02\x03'))
        self.assertEqual(('61', '62', '63646566676869'), mkpath(b'abcdefghi'))
        self.assertEqual(
            ('6c', 'a4',
             '98884051015ba8bba86e70ffea620166e65ef7c86d4e94dfdb340a7364d2'),
            mkpath(b'l\xa4\x98\x88@Q\x01[\xa8\xbb\xa8np\xff\xeab\x01f'
                   b'\xe6^\xf7\xc8mN\x94\xdf\xdb4\nsd\xd2'))



class TestBasicBackup(unittest.TestCase):
    def setUp(self):
        storetree = FakeDirectory()
        self.storetree = storetree
        sourcetree = FakeDirectory()
        self.sourcetree = sourcetree
        db = FakeDatabases()
        self.db = db
        bcfactory = backupcollection.BackupCollectionFactory(
            storetree, ('path', 'to', 'store'))
        bcfactory.set_database_opener(db.open)
        bcfactory.set_database_creator(db.create)
        bc = bcfactory.create_collection()

        sourcetree._set_file(
            ('home', 'me', 'file.txt'), content=b'127' * 42 + b'1')
        sourcetree._set_file(
            ('otherfile',), content=b'7029' * 1757 + b'7')
        sourcetree._set_file(
            ('home','me','deep','data'), content=b'5028' * 1257)
        sourcetree._set_file(
            ('4',), content=b'21516' * 4303 + b'2')

        backup = bc.start_backup(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629))
        with backup:
            cid = bc.add_content(
                sourcetree, ('home', 'me', 'file.txt'),
                now=datetime.datetime(2015, 2, 14, 19, 56, 7))
            self.cid1 = cid
            self.checksum1 = sourcetree._get_checksum(
                ('home', 'me', 'file.txt'))
            backup.add_file(
                ('homedir', 'file.txt'), cid, 127,
                datetime.datetime(2014, 9, 11, 9, 3, 54), 759831036)
            cid = bc.add_content(sourcetree, ('otherfile',))
            self.cid2 = cid
            self.checksum2 = sourcetree._get_checksum(('otherfile',))
            backup.add_file(
                ('homedir', 'other.txt'), cid, 7029,
                datetime.datetime(2015, 2, 1, 22, 43, 34), 51746409)
            cid = bc.add_content(sourcetree, ('home', 'me', 'deep', 'data'))
            self.cid3 = cid
            backup.add_file(
                ('outside', 'store', 'deep', 'data'), cid, 5028,
                datetime.datetime(2014, 4, 21, 8, 29, 46), 826447)
            cid = bc.add_content(sourcetree, ('4',))
            self.cid4 = cid
            backup.add_file(
                ('toplevel',), cid, 21516,
                datetime.datetime(2014, 10, 17, 15, 33, 2), 781606397)
            backup.commit(datetime.datetime(2015, 2, 14, 19, 55, 54, 954321))

        bcfactory = backupcollection.BackupCollectionFactory(
            storetree, ('path', 'to', 'store'))
        bcfactory.set_database_opener(db.open)
        bcfactory.set_database_creator(db.create)
        self.backupcollection = bcfactory.open_collection()

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
        self.assertCountEqual(('file.txt', 'other.txt'), files)
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
        self.assertEqual(self.cid2, info.contentid)
        self.assertEqual(7029, info.size)
        self.assertEqual(datetime.datetime(2015, 2, 1, 22, 43, 34), info.mtime)
        self.assertEqual(51746409, info.mtime_nsec)
        self.assertEqual(self.checksum2, info.good_checksum)
        info = backup.get_file_info(('homedir', 'first.txt'))
        self.assertEqual(None, info)
        info = backup.get_file_info(('homedir', 'file.txt'))
        self.assertEqual(self.cid1, info.contentid)
        self.assertEqual(127, info.size)
        self.assertEqual(datetime.datetime(2014, 9, 11, 9, 3, 54), info.mtime)
        self.assertEqual(759831036, info.mtime_nsec)
        self.assertEqual(self.checksum1, info.good_checksum)

    def test_get_content_info(self):
        bc = self.backupcollection
        info = bc.get_content_info(self.cid1)
        self.assertEqual(self.checksum1, info.goodsum)
        self.assertEqual(self.checksum1, info.lastsum)
        timeline = info.timeline
        self.assertEqual(1, len(timeline))
        cs = timeline[0]
        self.assertEqual(self.checksum1, cs.checksum)
        self.assertTrue(cs.restored)
        self.assertEqual(datetime.datetime(2015, 2, 14, 19, 56, 7), cs.first)
        self.assertEqual(datetime.datetime(2015, 2, 14, 19, 56, 7), cs.last)

    def test_get_content_reader(self):
        bc = self.backupcollection
        reader = bc.get_content_reader(self.cid1)
        self.assertNotEqual(None, reader)
        self.assertEqual(127, reader.get_size())
        data = reader.get_data_slice(0, 1024)
        self.assertEqual(data, b'127' * 42 + b'1')

    def test_add_duplicate_content(self):
        content_id = self.backupcollection.add_content(
            self.sourcetree, ('home', 'me', 'file.txt'),
            now=datetime.datetime(2015, 2, 18, 5, 27, 43))
        self.assertEqual(self.cid1, content_id)

    def test_checksum_timeline(self):
        bcfactory = backupcollection.BackupCollectionFactory(
            self.storetree, ('path', 'to', 'store'))
        bcfactory.set_database_opener(self.db.open)
        bcfactory.set_database_creator(self.db.create)
        bc = bcfactory.open_collection()
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

    def test_shadow_tree_created(self):
        store = self.storetree
        contentroot = ('path', 'to', 'store', 'content')
        shadowroot = ('path', 'to', 'store', '2015', '02-14T19:55')
        self.assertEqual(
            store._files[contentroot + ('ba', '0f', 'e9fe63690ce68315439e5a089'
                '86170b39937594c75fc4bbc32039db5ff0713be')],
            store._files[shadowroot + ('homedir', 'file.txt')])
        self.assertEqual(
            store._files[contentroot + ('d1', '87', 'bda8636907240be66b50338fa'
                'f862a4c3df3d7c4d30cef45a03f2ae5aa91365e')],
            store._files[shadowroot + ('outside', 'store', 'deep', 'data')])

class TestSingleStuff(unittest.TestCase):
    def test_default_start_and_end_time(self):
        storetree = FakeDirectory()
        sourcetree = FakeDirectory()
        db = FakeDatabases()
        bcfactory = backupcollection.BackupCollectionFactory(
            storetree, ('path', 'to', 'store'))
        bcfactory.set_database_opener(db.open)
        bcfactory.set_database_creator(db.create)
        bc = bcfactory.create_collection()

        sourcetree._set_file(
            ('home', 'me', 'file.txt'), content=b'127' * 42 + b'1')

        before_backup = datetime.datetime.utcnow()
        backup = bc.start_backup()
        after_backup_started = datetime.datetime.utcnow()
        with backup:
            cid = bc.add_content(
                sourcetree, ('home', 'me', 'file.txt'),
                now=datetime.datetime(2015, 2, 14, 19, 56, 7))
            backup.add_file(
                ('homedir', 'file.txt'), cid, 127,
                datetime.datetime(2014, 9, 11, 9, 3, 54), 759831036)
            backup.commit()

        bcfactory = backupcollection.BackupCollectionFactory(
            storetree, ('path', 'to', 'store'))
        bcfactory.set_database_opener(db.open)
        bcfactory.set_database_creator(db.create)
        bc2 = bcfactory.open_collection()
        backup2 = bc2.get_most_recent_backup()
        self.assertLessEqual(before_backup, backup2.get_start_time())
        self.assertLessEqual(backup2.get_start_time(), after_backup_started)
        self.assertLessEqual(after_backup_started, backup2.get_end_time())
        self.assertLessEqual(backup2.get_end_time(), datetime.datetime.utcnow())

    def test_get_most_recent_backup_when_no_backup_available(self):
        storetree = FakeDirectory()
        sourcetree = FakeDirectory()
        db = FakeDatabases()
        bcfactory = backupcollection.BackupCollectionFactory(
            storetree, ('path', 'to', 'store'))
        bcfactory.set_database_opener(db.open)
        bcfactory.set_database_creator(db.create)
        bc = bcfactory.create_collection()
        bc2 = bcfactory.open_collection()
        self.assertEqual(None, bc2.get_most_recent_backup())

class TestBrokenUsage(unittest.TestCase):

    def test_add_two_files_with_same_path(self):
        storetree = FakeDirectory()
        sourcetree = FakeDirectory()
        bcfactory = backupcollection.BackupCollectionFactory(
            storetree, ('path', 'to', 'store'))
        db = FakeDatabases()
        bcfactory.set_database_opener(db.open)
        bcfactory.set_database_creator(db.create)
        bc = bcfactory.create_collection()

        sourcetree._set_file(
            ('home', 'me', 'file.txt'), content=b'127' * 42 + b'1')
        sourcetree._set_file(
            ('otherfile',), content=b'7029' * 1757 + b'7')

        backup = bc.start_backup(
            datetime.datetime(2015, 2, 14, 19, 55, 32, 328629))
        with backup:
            cid = bc.add_content(sourcetree, ('home', 'me', 'file.txt'))
            backup.add_file(
                ('homedir', 'file.txt'), cid, 127,
                datetime.datetime(2014, 9, 11, 9, 3, 54), 759831036)
            self.assertRaisesRegex(
                FileExistsError, 'already exists.*file\\.txt',
                backup.add_file,
                ('homedir', 'file.txt'), cid, 127,
                datetime.datetime(2014, 9, 11, 9, 3, 54), 759831036)
            cid = bc.add_content(sourcetree, ('otherfile',))
            self.assertRaisesRegex(
                FileExistsError, 'already exists.*file\\.txt',
                backup.add_file,
                ('homedir', 'file.txt'), cid, 3412,
                datetime.datetime(2014, 9, 11, 9, 5, 22), 989894082)
