#!/usr/bin/env python3

import collections
import datetime
import io
import unittest

import backupoperation
import logger

from config_subtree import CfgSubtree


def add_backup_handlers(tree, ignore=None, dynamic=None, static=None):
    root = CfgSubtree(None, None)
    for paths, handler in (
            (ignore, 'ignore'), (dynamic, 'dynamic'), (static, 'static') ):
        if paths is None:
            continue
        for path in paths:
            root._add_child_path('plain', path, handler=handler)
    tree.set_backup_handlers(root)


class Empty(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class File(object):
    def __init__(self, fdata, path):
        assert fdata[0] in ('directory', 'file', 'socket', 'symlink')
        self._ftype = fdata[0]
        self._realfid = fdata[1]
        self._fid = fdata[1]
        if self._fid == b'1~c':
            # Everything should be the same as b'1', except the content.
            # But currently, nothing ever reads the content of a File.
            # So this will only change the cid
            self._fid = b'1'
        self._path = path
        self._seed = None
        if self._fid is not None:
            self._seed = sum(self._fid)

    def get_filetype(self):
        return self._ftype

    def get_size(self):
        assert self._ftype == 'file'
        return self._get_size()

    def _get_size(self):
        if self._ftype == 'socket':
            return 0
        if self._ftype == 'symlink':
            return len(self.readsymlink())
        assert self._ftype == 'file'
        return self._seed * 3 + 7

    def get_mtime(self):
        assert self._ftype != 'symlink'
        return self._get_mtime()

    def get_link_mtime(self):
        assert self._ftype == 'symlink'
        return self._get_mtime()

    def _get_mtime(self):
        mtime_ns = (999999960 + self._seed * 7) % 1000000000
        mtime = (datetime.datetime(2015, 2, 14) +
            datetime.timedelta(
                seconds=self._seed, milliseconds=mtime_ns//1000))
        return mtime, mtime_ns

    def readsymlink(self):
        assert self._ftype == 'symlink'
        assert self._path == ('home', 'me', 'myfiles', 'sl')
        return b'/home/missing'

    def get_backup_extra_data(self):
        return {}

    def _get_cid(self):
        if self._realfid == b's':
            return b'cid:/home/missing'
        if self._realfid == b'':
            return b''
        return b'cid:' + self._realfid

class BasicTree(object):
    _basic_tree_data = {
        ('home', 'me', 'myfiles', 'file.txt'): ('file', b'1'),
        ('home', 'me', 'myfiles', 'goodstuff'): ('file', b'2'),
        ('home', 'me', 'myfiles', 'more data'): ('file', b'3'),
        ('home', 'me', 'myfiles', 'static', 'one'): ('file', b'4'),
        ('home', 'me', 'myfiles', 'static', 'two'): ('file', b'5'),
        ('home', 'me', 'myfiles', 'static', 'more', 'three'): ('file', b'6'),
        ('home', 'me', 'myfiles', 'static', 'more', 'four'): ('file', b'7'),
        ('home', 'other', 'more'): ('file', b'8'),
        ('home', 'other', 'notmine'): ('file', b'9'),
        ('home', 'me', 'tmp', 'boring'): ('file', b'a'),
        ('home', 'me', 'tmp', 'forgetme'): ('file', b'b'),
        ('home', 'me', 'tmp', 'stuff'): ('file', b'c'),
        ('home', 'me', 'tmp', 'subdir', 'neither'): ('file', b'd'),
        ('home', 'me', 'tmp', 'subdir', 'nor'): ('file', b'e'),
        ('home', 'me', 'toplevel'): ('file', b'f'),
        ('home', 'outside'): ('file', b'g'),
        ('home', 'and more'): ('file', b'h'),
        ('home', 'me', 'myfiles', 'sl'): ('symlink', b's'),
        ('home', 'me', 'myfiles', 'sock'): ('socket', b''),
    }

    @classmethod
    def get_file_data(cls, path, overrides=None):
        if overrides is None:
            overrides = {}
        if path in overrides:
            return overrides[path]
        pathlen = len(path)
        for p in overrides:
            if (len(p) > pathlen and
                    p[:pathlen] == path and
                    overrides[p] is not None):
                return ('directory', None)
        if path in cls._basic_tree_data:
            return cls._basic_tree_data[path]
        for p in cls._basic_tree_data:
            if (len(p) > pathlen and
                    p[:pathlen] == path and
                    p not in overrides):
                return ('directory', None)
        return None

    @classmethod
    def get_directory_listing(cls, path, overrides=None):
        if overrides is None:
            overrides = {}
        dirs = set()
        files = set()
        pathlen = len(path)
        for p in overrides:
            if len(p) > pathlen and p[:pathlen] == path:
                if overrides[p] is None:
                    pass
                elif len(p) == pathlen + 1:
                    files.add(p[-1])
                else:
                    dirs.add(p[pathlen])
        for p in cls._basic_tree_data:
            if len(p) > pathlen and p[:pathlen] == path:
                if p in overrides:
                    pass
                elif len(p) == pathlen + 1:
                    files.add(p[-1])
                else:
                    dirs.add(p[pathlen])
        return tuple(dirs), tuple(files)


class BasicTreeSpy(object):
    def __init__(self):
        self._overrides = {}
        self._directories_listed = []

    def _get_file_data(self, path):
        return BasicTree.get_file_data(path, self._overrides)

    def path_to_full_string(self, path):
        return 'basictree:' + '/'.join(path)

    def get_directory_listing(self, path):
        self._directories_listed.append(path)
        return BasicTree.get_directory_listing(path, self._overrides)

    def get_item_at_path(self, path):
        fdata = self._get_file_data(path)
        if fdata is None:
            return None
        return File(fdata, path)


class NewBackupSpy(object):
    def __init__(self):
        self._committed = False
        self._added_files = []
        self._added_directories = []

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        pass

    def add_directory(self, path, extra=None):
        assert not self._committed
        self._added_directories.append((path, extra))

    def add_file(self, path, cid, size, mtime, mtime_ns, filetype, extra):
        assert not self._committed
        self._added_files.append(
            (path, cid, size, mtime, mtime_ns, filetype, extra))

    def commit(self):
        assert not self._committed
        self._committed = True

    def _get_data_for_added_file(self, path):
        return [x for x in self._added_files if x[0] == path]

    def get_file_info(self, path):
        ds = self._get_data_for_added_file(path)
        assert len(ds) < 2
        if len(ds) < 1:
            return None
        d = ds[0]
        return Empty()


class BackupFileInfo(object):
    def __init__(self, fdata, path):
        self._file = File(fdata, path)

    @property
    def filetype(self):
        return self._file.get_filetype()

    @property
    def size(self):
        return self._file.get_size()

    @property
    def mtime(self):
        return self._file.get_mtime()[0]

    @property
    def mtime_nsec(self):
        return self._file.get_mtime()[1]

    @property
    def contentid(self):
        return self._file._get_cid()


class SingleBackupStub(object):
    def get_file_info(self, path):
        assert path[0] == 'main'
        fdata = BasicTree.get_file_data(('home', 'me') + path[1:])
        if fdata is not None:
            return BackupFileInfo(fdata, path)
        return None

    def list_directory(self, path):
        if path == ():
            return ('main',), ()
        assert path[0] == 'main'
        return BasicTree.get_directory_listing(('home', 'me') + path[1:])


class BasicCollectionSpy(object):
    def __init__(self):
        self._added_backup = None
        self._added_cids = []

    def get_most_recent_backup(self):
        return self._added_backup

    def start_backup(self):
        assert self._added_backup is None
        self._added_backup = NewBackupSpy()
        return self._added_backup

    def add_content(self, f):
        cid = f._get_cid()
        self._added_cids.append(cid)
        return cid

    def add_content_data(self, d):
        cid = b'cid:' + d[:16]
        self._added_cids.append(cid)
        return cid


class SingleBackupCollectionSpy(BasicCollectionSpy):
    def __init__(self):
        BasicCollectionSpy.__init__(self)
        self._old_backup = SingleBackupStub()

    def get_most_recent_backup(self):
        if self._added_backup is not None:
            return self._added_backup
        return self._old_backup


class TestBasicBackup(unittest.TestCase):
    def setUp(self):
        self.sourcetree = BasicTreeSpy()
        self.collection = BasicCollectionSpy()
        self.logger = logger.Logger()
        services = {
            'logger': self.logger,
            }
        bo = backupoperation.BackupOperation(self.collection, services=services)
        tree = bo.add_tree_to_backup(
            self.sourcetree, ('home', 'me'), ('main',))
        add_backup_handlers(
            tree,
            ignore=(('tmp',),),
            dynamic=(('tmp', 'stuff'),),
            static=(('myfiles', 'static'),))
        bo.execute_backup()

        self.expected_backed_up_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'more data'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'myfiles', 'sl'),
            ('main', 'myfiles', 'sock'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]
        self.expected_backed_up_dirs = [
            ('main',),
            ('main', 'myfiles'),
            ('main', 'myfiles', 'static'),
            ('main', 'myfiles', 'static', 'more'),
            ('main', 'tmp')]

    def assertNoLoggedProblems(self):
        for event in self.logger.raw_log:
            self.assertLessThan(
                event.severity, logger.Logger.LOG_WARNING, msg=str(event))

    def test_correct_files_are_backed_up(self):
        backup = self.collection._added_backup
        self.assertCountEqual(
            self.expected_backed_up_files,
            [x[0] for x in backup._added_files])
        self.assertCountEqual(
            self.expected_backed_up_dirs,
            [x[0] for x in backup._added_directories])
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_content(self):
        backup = self.collection._added_backup
        expectedcids = []
        for totest in self.expected_backed_up_files:
            sourcepath = ('home', 'me') + totest[1:]
            sourcefile = self.sourcetree.get_item_at_path(sourcepath)
            expectedcid = sourcefile._get_cid()
            if expectedcid != b'':
                expectedcids.append(expectedcid)
            bkfiles = backup._get_data_for_added_file(totest)
            self.assertEqual(1, len(bkfiles))
            self.assertEqual(expectedcid, bkfiles[0][1])
        self.assertCountEqual(expectedcids, self.collection._added_cids)
        self.assertCountEqual(
            expectedcids + [b''], [x[1] for x in backup._added_files])
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_metadata(self):
        backup = self.collection._added_backup
        for totest in self.expected_backed_up_files:
            sourcepath = ('home', 'me') + totest[1:]
            sourcefile = self.sourcetree.get_item_at_path(sourcepath)
            bkfiles = backup._get_data_for_added_file(totest)
            self.assertEqual(1, len(bkfiles))
            bkfile = bkfiles[0]
            self.assertEqual(sourcefile._get_size(), bkfile[2],
                msg='Size mismatch for ' + str(totest))
            mtime, mtime_ns = sourcefile._get_mtime()
            self.assertEqual(mtime, bkfile[3],
                msg='mtime mismatch for ' + str(totest))
            self.assertEqual(mtime_ns, bkfile[4],
                msg='mtime_ns mismatch for ' + str(totest))
        self.assertNoLoggedProblems()

    def test_ignored_subtrees_are_not_traversed(self):
        sourcetree = self.sourcetree
        self.assertIn(('home', 'me', 'tmp'), sourcetree._directories_listed)
        self.assertNotIn(
            ('home', 'me', 'tmp', 'subdir'), sourcetree._directories_listed)


class TestSecondBackup(unittest.TestCase):
    def setUp(self):
        self.logger = logger.Logger()
        self.stdout = io.StringIO()
        self.logger.set_outfile(self.stdout)
        services = {
            'logger': self.logger
            }
        self.collection = SingleBackupCollectionSpy()
        self.sourcetree = BasicTreeSpy()
        self.bo = backupoperation.BackupOperation(
            self.collection, services=services)
        tree = self.bo.add_tree_to_backup(
            self.sourcetree, ('home', 'me'), ('main',))
        add_backup_handlers(
            tree,
            ignore=(('tmp',),),
            dynamic=(('tmp', 'stuff'),),
            static=(('myfiles', 'static'),))

    def assertNoLoggedProblems(self):
        for event in self.logger.raw_log:
            self.assertLessThan(
                event.severity, logger.Logger.LOG_WARNING, msg=str(event))

    def assertLoggedError(self, item, what):
        for event in self.logger.raw_log:
            if (event.severity >= logger.Logger.LOG_ERROR and
                    event.what == what and
                    event.which == item):
                return
        self.fail('Log has no error type "' + what + '" for ' + str(item))

    def test_changed_static_data_causes_error_to_be_reported(self):
        self.assertNoLoggedProblems()
        # Change one static file
        relpath = ('myfiles', 'static', 'more', 'three')
        self.sourcetree._overrides[('home', 'me') + relpath] = ('file', b'n')
        self.bo.execute_backup()

        self.assertRegex(
            self.stdout.getvalue(), 'static file changed.*more.*three')
        self.assertLoggedError(('main',) + relpath, 'static file changed')

    def test_changed_static_data_is_backed_up(self):
        # Change one static file
        relpath = ('myfiles', 'static', 'more', 'three')
        self.sourcetree._overrides[('home', 'me') + relpath] = ('file', b'n')
        self.bo.execute_backup()

        backup = self.collection._added_backup
        bkfiles = backup._get_data_for_added_file(('main',) + relpath)
        expectedcid = File(('file', b'n'), None)._get_cid()
        self.assertEqual(1, len(bkfiles))
        self.assertEqual(expectedcid, bkfiles[0][1])

    def test_removed_static_data_causes_error_to_be_reported(self):
        # Change one static file
        relpath = ('myfiles', 'static', 'more', 'three')
        self.sourcetree._overrides[('home', 'me') + relpath] = None
        self.assertNoLoggedProblems()
        self.bo.execute_backup()

        self.assertRegex(
            self.stdout.getvalue(), 'static file removed.*more.*three')
        self.assertLoggedError(
            ('main', 'myfiles', 'static', 'more', 'three'),
            'static file removed')

    def test_removed_static_data_is_removed_in_new_backup(self):
        # Change one static file
        relpath = ('myfiles', 'static', 'more', 'three')
        self.sourcetree._overrides[('home', 'me') + relpath] = None
        self.assertNoLoggedProblems()
        self.bo.execute_backup()

        backup = self.collection._added_backup
        bkfiles = backup._get_data_for_added_file(('main',) + relpath)
        self.assertEqual([], bkfiles)

    def test_moved_static_data_causes_no_error_to_be_reported(self):
        # Move one static file
        relpath1 = ('myfiles', 'static', 'more', 'three')
        relpath2 = ('myfiles', 'static', 'new')
        self.sourcetree._overrides[('home', 'me') + relpath1] = None
        self.sourcetree._overrides[('home', 'me') + relpath2] = ('file', b'6')
        self.assertNoLoggedProblems()
        self.bo.execute_backup()

        self.assertEqual('', self.stdout.getvalue())
        self.assertNoLoggedProblems()

    def test_moved_static_data_is_backed_up(self):
        # Move one static file
        relpath1 = ('myfiles', 'static', 'more', 'three')
        relpath2 = ('myfiles', 'static', 'new')
        self.sourcetree._overrides[('home', 'me') + relpath1] = None
        self.sourcetree._overrides[('home', 'me') + relpath2] = ('file', b'6')
        self.assertNoLoggedProblems()
        self.bo.execute_backup()

        backup = self.collection._added_backup
        bkfiles = backup._get_data_for_added_file(('main',) + relpath1)
        self.assertEqual([], bkfiles)
        bkfiles = backup._get_data_for_added_file(('main',) + relpath2)
        self.assertEqual(1, len(bkfiles))
        bkfile = bkfiles[0]
        self.assertEqual(bkfile[1], File(('file', b'6'), None)._get_cid())

    def test_move_static_data_to_nonstatic_causes_error_to_be_reported(self):
        # Move one static file to non-static area
        relpath1 = ('myfiles', 'static', 'more', 'three')
        relpath2 = ('myfiles', 'new')
        self.sourcetree._overrides[('home', 'me') + relpath1] = None
        self.sourcetree._overrides[('home', 'me') + relpath2] = ('file', b'6')
        self.assertNoLoggedProblems()
        self.bo.execute_backup()

        self.assertRegex(
            self.stdout.getvalue(), 'static file removed.*more.*three')
        self.assertLoggedError(
            ('main', 'myfiles', 'static', 'more', 'three'),
            'static file removed')

    def test_changed_file_is_updated(self):
        # Change one file
        relpath = ('myfiles', 'more data')
        self.sourcetree._overrides[('home', 'me') + relpath] = ('file', b'n')
        self.bo.execute_backup()

        self.assertNoLoggedProblems()
        self.assertEqual('', self.stdout.getvalue())
        self.assertNoLoggedProblems()
        backup = self.collection._added_backup
        bkfiles = backup._get_data_for_added_file(('main',) + relpath)
        self.assertEqual(1, len(bkfiles))
        bkfile = bkfiles[0]
        new_cid = File(('file', b'n'), None)._get_cid()
        self.assertEqual(bkfile[1], new_cid)
        self.assertIn(new_cid, self.collection._added_cids)

    def test_files_with_unchanged_mtime_and_size_are_assumed_same(self):
        # Intentionally break the assumption that unchanged mtime and
        # size implies unchanged content.
        relpath = ('myfiles', 'file.txt')
        self.sourcetree._overrides[('home', 'me') + relpath] = ('file', b'1~c')
        self.assertNoLoggedProblems()
        self.bo.execute_backup()

        self.assertNoLoggedProblems()
        # symlinks are currently never "assumed unchanged", so it gets
        # added each time. They should typically be small and
        # relatively few, so it doesn't matter much.
        self.assertEqual([b'cid:/home/missing'], self.collection._added_cids)
        backup = self.collection._added_backup
        bkfiles = backup._get_data_for_added_file(('main',) + relpath)
        self.assertEqual(1, len(bkfiles))
        bkfile = bkfiles[0]
        # File not updated, even though content has changed (Yes,
        # that's NOT good. But it should only happen if both the size
        # and the mtime is unchanged for the same file. So I think it
        # is a small price to pay for a huge performance advantage.)
        old_cid = File(('file', b'1'), None)._get_cid()
        self.assertEqual(bkfile[1], old_cid)
        self.assertNotEqual(old_cid, File(('file', b'1~c'), None)._get_cid())

    def test_second_backup_includes_the_correct_files_content_and_metadata(self):
        # Remove one file, add one and change one
        relpath1 = ('myfiles', 'more data')
        relpath2 = ('myfiles', 'newdir', 'goodstuff')
        relpath3 = ('tmp', 'stuff')
        srcbase = ('home', 'me')
        bkbase = ('main',)
        self.sourcetree._overrides[srcbase + relpath1] = None
        self.sourcetree._overrides[srcbase + relpath2] = ('file', b'r')
        self.sourcetree._overrides[srcbase + relpath3] = ('file', b't')
        self.bo.execute_backup()

        self.assertNoLoggedProblems()
        expected_files = {
            ('main', 'myfiles', 'file.txt'): b'1',
            ('main', 'myfiles', 'goodstuff'): b'2',
            ('main', 'myfiles', 'newdir' , 'goodstuff'): b'r',
            ('main', 'myfiles', 'static', 'one'): b'4',
            ('main', 'myfiles', 'static', 'two'): b'5',
            ('main', 'myfiles', 'static', 'more', 'three'): b'6',
            ('main', 'myfiles', 'static', 'more', 'four'): b'7',
            ('main', 'myfiles', 'sl'): b's',
            ('main', 'myfiles', 'sock'): b'',
            ('main', 'tmp', 'stuff'): b't',
            ('main', 'toplevel'): b'f'}
        expected_dirs = (
            ('main',),
            ('main', 'myfiles'),
            ('main', 'myfiles', 'newdir'),
            ('main', 'myfiles', 'static'),
            ('main', 'myfiles', 'static', 'more'),
            ('main', 'tmp'))
        backup = self.collection._added_backup
        self.assertCountEqual(
            expected_files.keys(), [x[0] for x in backup._added_files])
        self.assertCountEqual(
            expected_dirs, [x[0] for x in backup._added_directories])
        for p in expected_files:
            bkfiles = backup._get_data_for_added_file(p)
            self.assertEqual(1, len(bkfiles))
            fid = expected_files[p]
            bkf = bkfiles[0]
            fdata = File(('file', fid), None)
            self.assertEqual(bkf[1], fdata._get_cid())
            srcsize = fdata.get_size()
            if fid == b's':
                srcsize = 13
            elif fid == b'':
                srcsize = 0
            self.assertEqual(bkf[2], srcsize)
            self.assertEqual(bkf[3], fdata._get_mtime()[0])
            self.assertEqual(bkf[4], fdata._get_mtime()[1])
        added_cids = self.collection._added_cids
        for fid in (b'r', b't'):
            self.assertIn(File(('file', fid), None)._get_cid(), added_cids)
        for fid in (b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'f'):
            self.assertNotIn(File(('file', fid), None)._get_cid(), added_cids)
