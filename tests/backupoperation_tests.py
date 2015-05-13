#!/usr/bin/env python3

import collections
import datetime
import logger
import unittest

import backupoperation

class FakeBackupCollection(object):
    def __init__(self):
        self._backups = []
        self._content = {} # { content: content_id }
        self._content_add_count = {} # { content: count }

    def start_backup(self):
        return FakeBackupBuilder(self)

    def get_most_recent_backup(self):
        if not self._backups:
            return None
        return self._backups[-1]

    def add_content(self, tree, path):
        data = tree._files[path]._get_content()
        self._content_add_count[data] = self._content_add_count.get(data, 0) + 1
        if data not in self._content:
            self._content[data] = b'content' + data
        return self._content[data]

class FakeBackupBuilder(object):
    def __init__(self, collection):
        self._collection = collection
        self._done = False
        self._backup = FakeBackup()

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self._done = True

    def commit(self):
        assert self._done is False
        self._done = True
        self._collection._backups.append(self._backup)

    def add_file(self, path, content_id, size, mtime, mtime_ns):
        assert path not in self._backup._files
        self._backup._files[path] = FakeBackupFileInfo(
            content_id, size, mtime, mtime_ns)

FakeBackupFileInfo = collections.namedtuple(
    'FakeBackupFileInfo', ('contentid', 'size', 'mtime', 'mtime_ns'))
class FakeBackup(object):
    def __init__(self):
        self._files = {} # { path: FakeBackupFileInfo }

    def get_file_info(self, path):
        return self._files.get(path)

    def list_directory(self, path):
        dirs = set()
        files = set()
        pathlen = len(path)
        for cand in self._files:
            if cand[:-1] == path:
                files.add(cand[-1])
            elif cand[:pathlen] == path:
                dirs.add(cand[pathlen])
        return tuple(dirs), tuple(files)

filenum = 0
class FakeTree(object):
    def __init__(self):
        self._files = {}

    def _copy_tree(self, other):
        self._files.update(other._files)

    def _add_files(self, folder, files):
        global filenum
        for name in files:
            self._files[folder + (name,)] = FakeFile(filenum)
            filenum += 1

    def get_directory_listing(self, path):
        pathlen = len(path)
        dirs = set()
        files = set()
        for cand in self._files:
            if len(cand) > pathlen and cand[:pathlen] == path:
                if len(cand) == pathlen + 1:
                    files.add(cand[-1])
                else:
                    dirs.add(cand[pathlen])
        return tuple(dirs), tuple(files)

    def get_item_at_path(self, path):
        if path not in self._files:
            raise AssertionError('Directories not supported')
        return self._files[path]

class FakeFile(object):
    def __init__(self, fileid):
        self._fileid = fileid
        self._overrides = {}

    def _get_content(self):
        '''Not really the actual content. Rather, a short bytes object that
        uniquely identifies the actual content of this file.
        '''
        override = self._overrides.get('content')
        if override is not None:
            return override
        return str(self._fileid).encode('utf-8')

    def _override(self, content=None):
        if content is not None:
            self._overrides['content'] = content

    def _change(self):
        global filenum
        self._override_content = None
        self._fileid = filenum
        filenum += 1

    def get_size(self):
        return self._fileid * 3 + 7

    def get_mtime(self):
        mtime = (datetime.datetime(2015, 2, 14) +
                 datetime.timedelta(seconds=self._fileid))
        nanosecond = (999999960 + self._fileid * 7) % 1000000000
        mtime = mtime.replace(microsecond=nanosecond//1000)
        return mtime, nanosecond

class TestBasicBackup(unittest.TestCase):
    def setUp(self):
        bc = FakeBackupCollection()
        self.backupcollection = bc
        self.logger = logger.Logger()
        bo = backupoperation.BackupOperation(bc)
        bo.set_logger(self.logger)
        self.backupoperation = bo
        sourcetree = FakeTree()
        self.sourcetree = sourcetree
        sourcetree._add_files(
            ('home', 'me', 'myfiles'), ('file.txt', 'goodstuff', 'more data'))
        sourcetree._add_files(
            ('home', 'me', 'myfiles', 'static'), ('one', 'two'))
        sourcetree._add_files(
            ('home', 'me', 'myfiles', 'static', 'more'), ('three', 'four'))
        sourcetree._add_files(('home', 'other'), ('more', 'notmine'))
        sourcetree._add_files(
            ('home', 'me', 'tmp'), ('boring', 'forgetme', 'stuff'))
        sourcetree._add_files(('home', 'me'), ('toplevel',))
        sourcetree._add_files(('home',), ('outside', 'and more'))
        tree = bo.add_tree_to_backup(sourcetree, ('home', 'me'), ('main',))
        tree.ignore_subtree(('tmp',))
        tree.back_up_subtree(('tmp', 'stuff'))
        tree.back_up_static_subtree(('myfiles', 'static'))
        self.backuptree = tree
        bo.execute_backup()

        self.expected_backed_up_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'more data'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]

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

    def test_correct_files_are_backed_up(self):
        self.assertEqual(1, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[-1]
        self.assertCountEqual(self.expected_backed_up_files, backup._files)
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_content(self):
        sourcetree = self.sourcetree
        self.assertEqual(1, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[-1]
        for totest in self.expected_backed_up_files:
            fcid = backup._files[totest][0]
            sourcename = ('home', 'me') + totest[1:]
            content = sourcetree._files[sourcename]._get_content()
            ccid = self.backupcollection._content[content]
            self.assertEqual(
                fcid, ccid,
                msg='Content mismatch for ' + str(totest))
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_metadata(self):
        sourcetree = self.sourcetree
        self.assertEqual(1, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[-1]
        for totest in self.expected_backed_up_files:
            bkfile = backup._files[totest]
            sourcename = ('home', 'me') + totest[1:]
            srcfile = sourcetree._files[sourcename]
            self.assertEqual(
                srcfile.get_size(), bkfile[1],
                msg='Size mismatch for ' + str(totest))
            mtime, mtime_ns = srcfile.get_mtime()
            self.assertEqual(
                mtime, bkfile[2],
                msg='mtime mismatch for ' + str(totest))
            self.assertEqual(
                mtime_ns, bkfile[3],
                msg='mtime_ns mismatch for ' + str(totest))
        self.assertNoLoggedProblems()

    def test_changed_static_data_causes_error_to_be_reported(self):
        bo = backupoperation.BackupOperation(self.backupcollection)
        bo.set_logger(self.logger)
        sourcetree2 = FakeTree()
        sourcetree2._copy_tree(self.sourcetree)
        # Change one static file
        changed = sourcetree2._files[
            ('home', 'me', 'myfiles', 'static', 'more', 'three')]
        oldcontent = changed._get_content()
        changed._change()
        self.assertNotEqual(oldcontent, changed._get_content())
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        self.assertNoLoggedProblems()
        bo.execute_backup()
        self.assertLoggedError(
            ('main', 'myfiles', 'static', 'more', 'three'),
            'static file changed')
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        self.assertEqual(
            self.backupcollection._content[oldcontent],
            backup._files[
                ('main', 'myfiles', 'static', 'more', 'three')].contentid)
        backup2 = self.backupcollection._backups[1]
        self.assertEqual(
            self.backupcollection._content[changed._get_content()],
            backup2._files[
                ('main', 'myfiles', 'static', 'more', 'three')].contentid)

    def test_removed_static_data_causes_error_to_be_reported(self):
        bo = backupoperation.BackupOperation(self.backupcollection)
        bo.set_logger(self.logger)
        sourcetree2 = FakeTree()
        sourcetree2._copy_tree(self.sourcetree)
        # Change one static file
        oldcontent = sourcetree2._files[
            ('home', 'me', 'myfiles', 'static', 'more', 'three')]._get_content()
        del sourcetree2._files[
            ('home', 'me', 'myfiles', 'static', 'more', 'three')]
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        self.assertNoLoggedProblems()
        bo.execute_backup()
        self.assertLoggedError(
            ('main', 'myfiles', 'static', 'more', 'three'),
            'static file removed')
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        self.assertEqual(
            self.backupcollection._content[oldcontent],
            backup._files[
                ('main', 'myfiles', 'static', 'more', 'three')].contentid)
        backup2 = self.backupcollection._backups[1]
        self.assertNotIn(
            ('main', 'myfiles', 'static', 'more', 'three'), backup2._files)

    def test_moved_static_data_causes_no_error_to_be_reported(self):
        bo = backupoperation.BackupOperation(self.backupcollection)
        bo.set_logger(self.logger)
        sourcetree2 = FakeTree()
        sourcetree2._copy_tree(self.sourcetree)
        # Change one static file
        oldcontent = sourcetree2._files[
            ('home', 'me', 'myfiles', 'static', 'more', 'three')]._get_content()
        sourcetree2._files[('home', 'me', 'myfiles', 'static', 'new')] = (
            sourcetree2._files[
                ('home', 'me', 'myfiles', 'static', 'more', 'three')])
        del sourcetree2._files[
            ('home', 'me', 'myfiles', 'static', 'more', 'three')]
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        self.assertNoLoggedProblems()
        bo.execute_backup()
        self.assertNoLoggedProblems()
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        self.assertEqual(
            self.backupcollection._content[oldcontent],
            backup._files[
                ('main', 'myfiles', 'static', 'more', 'three')].contentid)
        backup2 = self.backupcollection._backups[1]
        self.assertNotIn(
            ('main', 'myfiles', 'static', 'more', 'three'), backup2._files)
        self.assertEqual(
            self.backupcollection._content[oldcontent],
            backup2._files[('main', 'myfiles', 'static', 'new')].contentid)
        self.assertNoLoggedProblems()

    def test_move_static_data_to_nonstatic_causes_error_to_be_reported(self):
        bo = backupoperation.BackupOperation(self.backupcollection)
        bo.set_logger(self.logger)
        sourcetree2 = FakeTree()
        sourcetree2._copy_tree(self.sourcetree)
        # Change one static file
        oldcontent = sourcetree2._files[
            ('home', 'me', 'myfiles', 'static', 'more', 'three')]._get_content()
        sourcetree2._files[('home', 'me', 'myfiles', 'new')] = (
            sourcetree2._files[
                ('home', 'me', 'myfiles', 'static', 'more', 'three')])
        del sourcetree2._files[
            ('home', 'me', 'myfiles', 'static', 'more', 'three')]
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        self.assertNoLoggedProblems()
        bo.execute_backup()
        self.assertLoggedError(
            ('main', 'myfiles', 'static', 'more', 'three'),
            'static file removed')
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        self.assertEqual(
            self.backupcollection._content[oldcontent],
            backup._files[
                ('main', 'myfiles', 'static', 'more', 'three')].contentid)
        backup2 = self.backupcollection._backups[1]
        self.assertNotIn(
            ('main', 'myfiles', 'static', 'more', 'three'), backup2._files)
        self.assertEqual(
            self.backupcollection._content[oldcontent],
            backup2._files[('main', 'myfiles', 'new')].contentid)

    def test_changed_file_is_updated(self):
        # First, verify that each content has only been added once
        # during the first back-up operation.
        self.assertEqual(1, len(self.backupcollection._backups))
        old_contents = []
        for content, count in self.backupcollection._content_add_count.items():
            old_contents.append(content)
            self.assertEqual(1, count)
        bo = backupoperation.BackupOperation(self.backupcollection)
        bo.set_logger(self.logger)
        sourcetree2 = FakeTree()
        sourcetree2._copy_tree(self.sourcetree)
        # Change one file
        changed = sourcetree2._files[('home', 'me', 'myfiles', 'more data')]
        oldcontent = changed._get_content()
        changed._change()
        self.assertNotEqual(oldcontent, changed._get_content())
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        self.assertNoLoggedProblems()
        bo.execute_backup()
        self.assertNoLoggedProblems()
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        self.assertEqual(
            self.backupcollection._content[oldcontent],
            backup._files[('main', 'myfiles', 'more data')].contentid)
        backup2 = self.backupcollection._backups[1]
        self.assertEqual(
            self.backupcollection._content[changed._get_content()],
            backup2._files[('main', 'myfiles', 'more data')].contentid)
        self.assertNotIn(changed._get_content(), old_contents)
        self.assertIn(changed._get_content(), self.backupcollection._content)
        self.assertIn(
            changed._get_content(), self.backupcollection._content_add_count)
        for content, count in self.backupcollection._content_add_count.items():
            self.assertEqual(1, count)
            if content != changed._get_content():
                self.assertIn(content, old_contents)
        self.assertNoLoggedProblems()

    def test_files_with_unchanged_mtime_and_size_are_assumed_same(self):
        # First, verify that each content has only been added once
        # during the first back-up operation.
        self.assertEqual(1, len(self.backupcollection._backups))
        old_contents = []
        for content, count in self.backupcollection._content_add_count.items():
            old_contents.append(content)
            self.assertEqual(1, count)
        bo = backupoperation.BackupOperation(self.backupcollection)
        bo.set_logger(self.logger)
        sourcetree2 = FakeTree()
        sourcetree2._copy_tree(self.sourcetree)
        # Intentionally break the assumption that unchanged mtime and
        # size implies unchanged content.
        changed = sourcetree2._files[('home', 'me', 'myfiles', 'file.txt')]
        oldcontent = changed._get_content()
        changed._override(content=b'changed')
        self.assertNotEqual(oldcontent, changed._get_content())
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        self.assertNoLoggedProblems()
        bo.execute_backup()
        self.assertNoLoggedProblems()
        self.assertEqual(2, len(self.backupcollection._backups))
        # And now, check that no contents have been added at all
        # during the second back-up operation.
        for content, count in self.backupcollection._content_add_count.items():
            self.assertEqual(1, count)
            self.assertIn(content, old_contents)
        self.assertNoLoggedProblems()

    def test_file_with_identical_content_gets_same_content_id(self):
        # First, verify that each content has only been added once
        # during the first back-up operation.
        self.assertEqual(1, len(self.backupcollection._backups))
        old_contents = []
        for content, count in self.backupcollection._content_add_count.items():
            old_contents.append(content)
            self.assertEqual(1, count)
        bo = backupoperation.BackupOperation(self.backupcollection)
        bo.set_logger(self.logger)
        sourcetree2 = FakeTree()
        sourcetree2._copy_tree(self.sourcetree)
        # And change one file, and let its content be the same as another
        original = sourcetree2._files[('home', 'me', 'myfiles', 'file.txt')]
        changed = sourcetree2._files[('home', 'me', 'myfiles', 'more data')]
        oldcontent = changed._get_content()
        changed._change()
        changed._override(content=original._get_content())
        self.assertNotEqual(oldcontent, changed._get_content())
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        self.assertNoLoggedProblems()
        bo.execute_backup()
        self.assertNoLoggedProblems()
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        backup2 = self.backupcollection._backups[1]
        self.assertEqual(
            self.backupcollection._content[changed._get_content()],
            backup2._files[('main', 'myfiles', 'more data')].contentid)
        self.assertEqual(
            backup2._files[('main', 'myfiles', 'file.txt')].contentid,
            backup2._files[('main', 'myfiles', 'more data')].contentid)
        self.assertEqual(
            backup._files[('main', 'myfiles', 'file.txt')].contentid,
            backup2._files[('main', 'myfiles', 'more data')].contentid)
        for content, count in self.backupcollection._content_add_count.items():
            if content == changed._get_content():
                self.assertEqual(2, count)
            else:
                self.assertEqual(1, count)
            self.assertIn(content, old_contents)
        self.assertNoLoggedProblems()

class TestTwoBackups(unittest.TestCase):
    def setUp(self):
        bc = FakeBackupCollection()
        self.backupcollection = bc
        self.logger = logger.Logger()
        bo = backupoperation.BackupOperation(bc)
        bo.set_logger(self.logger)
        self.backupoperation = bo
        sourcetree = FakeTree()
        self.sourcetree = sourcetree
        sourcetree._add_files(
            ('home', 'me', 'myfiles'), ('file.txt', 'goodstuff', 'more data'))
        sourcetree._add_files(
            ('home', 'me', 'myfiles', 'static'), ('one', 'two'))
        sourcetree._add_files(
            ('home', 'me', 'myfiles', 'static', 'more'), ('three', 'four'))
        sourcetree._add_files(('home', 'other'), ('more', 'notmine'))
        sourcetree._add_files(
            ('home', 'me', 'tmp'), ('boring', 'forgetme', 'stuff'))
        sourcetree._add_files(('home', 'me'), ('toplevel',))
        sourcetree._add_files(('home',), ('outside', 'and more'))
        tree = bo.add_tree_to_backup(sourcetree, ('home', 'me'), ('main',))
        tree.ignore_subtree(('tmp',))
        tree.back_up_subtree(('tmp', 'stuff'))
        tree.back_up_static_subtree(('myfiles', 'static'))
        self.backuptree = tree
        bo.execute_backup()

        bo = backupoperation.BackupOperation(bc)
        bo.set_logger(self.logger)
        self.backupoperation2 = bo
        sourcetree2 = FakeTree()
        self.sourcetree2 = sourcetree2
        sourcetree2._copy_tree(sourcetree)
        del sourcetree2._files[('home', 'me', 'myfiles', 'more data')]
        sourcetree2._add_files(('home', 'me', 'myfiles'), ('new file',))
        sourcetree2._add_files(('home', 'me', 'new dir'), ('new file',))
        tree2 = bo.add_tree_to_backup(sourcetree2, ('home', 'me'), ('main',))
        self.backuptree2 = tree2
        tree2.ignore_subtree(('tmp',))
        tree2.back_up_subtree(('tmp', 'stuff'))
        tree2.back_up_static_subtree(('myfiles', 'static'))
        bo.execute_backup()

    def assertNoLoggedProblems(self):
        for event in self.logger.raw_log:
            self.assertLessThan(
                event.severity, logger.Logger.LOG_WARNING, msg=str(event))

    def test_correct_files_are_backed_up_1(self):
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        expected_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'more data'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]
        self.assertCountEqual(expected_files, backup._files)
        self.assertNoLoggedProblems()

    def test_correct_files_are_backed_up_2(self):
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[1]
        expected_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'new file'),
            ('main', 'new dir', 'new file'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]
        self.assertCountEqual(expected_files, backup._files)
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_content_1(self):
        expected_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'more data'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]
        sourcetree = self.sourcetree
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        for totest in expected_files:
            fcid = backup._files[totest][0]
            sourcename = ('home', 'me') + totest[1:]
            content = sourcetree._files[sourcename]._get_content()
            ccid = self.backupcollection._content[content]
            self.assertEqual(
                fcid, ccid,
                msg='Content mismatch for ' + str(totest))
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_content_2(self):
        expected_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'new file'),
            ('main', 'new dir', 'new file'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]
        sourcetree = self.sourcetree2
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[1]
        for totest in expected_files:
            fcid = backup._files[totest][0]
            sourcename = ('home', 'me') + totest[1:]
            content = sourcetree._files[sourcename]._get_content()
            ccid = self.backupcollection._content[content]
            self.assertEqual(
                fcid, ccid,
                msg='Content mismatch for ' + str(totest))
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_metadata_1(self):
        expected_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'more data'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]
        sourcetree = self.sourcetree
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[0]
        for totest in expected_files:
            bkfile = backup._files[totest]
            sourcename = ('home', 'me') + totest[1:]
            srcfile = sourcetree._files[sourcename]
            self.assertEqual(
                srcfile.get_size(), bkfile[1],
                msg='Size mismatch for ' + str(totest))
            mtime, mtime_ns = srcfile.get_mtime()
            self.assertEqual(
                mtime, bkfile[2],
                msg='mtime mismatch for ' + str(totest))
            self.assertEqual(
                mtime_ns, bkfile[3],
                msg='mtime_ns mismatch for ' + str(totest))
        self.assertNoLoggedProblems()

    def test_files_are_backed_up_with_correct_metadata_2(self):
        expected_files = [
            ('main', 'myfiles', 'file.txt'),
            ('main', 'myfiles', 'goodstuff'),
            ('main', 'myfiles', 'new file'),
            ('main', 'new dir', 'new file'),
            ('main', 'myfiles', 'static', 'one'),
            ('main', 'myfiles', 'static', 'two'),
            ('main', 'myfiles', 'static', 'more', 'three'),
            ('main', 'myfiles', 'static', 'more', 'four'),
            ('main', 'tmp', 'stuff'),
            ('main', 'toplevel')]
        sourcetree = self.sourcetree2
        self.assertEqual(2, len(self.backupcollection._backups))
        backup = self.backupcollection._backups[1]
        for totest in expected_files:
            bkfile = backup._files[totest]
            sourcename = ('home', 'me') + totest[1:]
            srcfile = sourcetree._files[sourcename]
            self.assertEqual(
                srcfile.get_size(), bkfile[1],
                msg='Size mismatch for ' + str(totest))
            mtime, mtime_ns = srcfile.get_mtime()
            self.assertEqual(
                mtime, bkfile[2],
                msg='mtime mismatch for ' + str(totest))
            self.assertEqual(
                mtime_ns, bkfile[3],
                msg='mtime_ns mismatch for ' + str(totest))
        self.assertNoLoggedProblems()
