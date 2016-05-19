#!/usr/bin/env python3

import errno
import unittest

import pyebakup.cli.task_makeshadowtree as task_makeshadowtree


class FakeArgs(object):
    def __init__(self):
        self._logger = FakeLogger()
        self.services = { 'logger': self._logger }


class FakeLogger(object):
    def __init__(self):
        self._log = []

    def log_error(self, what, which, msg=None):
        self._log.append(('ERROR', what, which, msg))


class FakeConfig(object):
    def __init__(self):
        self._backups = [ FakeBackupConf() ]

    def _makestorage(self, tree, path):
        bc = FakeStorageConf(tree, path)
        self._backups[0].storages.append(bc)

    def get_all_backup_names(self):
        return ['main']

    def get_backup_by_name(self, name):
        assert name == 'main'
        return self._backups[0]


class FakeTree(object):
    def __init__(self):
        self._cwd = ('the', 'cwd')
        self._path_exists = {}
        self._actions = []

    def path_from_string(self, stringpath):
        if stringpath == 'shadow':
            return self._cwd + ('shadow',)

    def create_directory(self, path, exist_ok=False):
        if self._path_exists[path]:
            if exist_ok:
                return
            raise OSError(errno.EEXIST, 'File exists: ' + str(path))
        self._path_exists[path] = True
        self._actions.append(('mkdir', path))


class FakeBackupConf(object):
    def __init__(self):
        self.storages = []


class FakeStorageConf(object):
    def __init__(self, tree, path):
        self.filesystem = tree
        self.path = path


class FakeStorage(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        self._backups = {}
        self._shadow_copies = []

    def get_backup_by_name(self, name):
        return self._backups.get(name)

    def _make_backup(self, name):
        bk = FakeBackup()
        self._backups[name] = bk
        return bk

    def make_shadow_copy(self, info, tree, path):
        assert tree == self._tree
        self._shadow_copies.append((path, info))


class FakeBackup(object):
    def __init__(self):
        self._root = FakeDirInfo()

    def _add_file(self, path, filetype='file'):
        d = self._create_dir(path[:-1])
        f = FakeFileInfo(path, filetype)
        d._add_file(path[-1], f)
        return f

    def _create_dir(self, path):
        d = self._root
        for comp in path:
            assert comp not in d._children_files
            if comp not in d._children_dirs:
                d._children_dirs[comp] = FakeDirInfo()
            d = d._children_dirs[comp]
        return d

    def list_directory(self, path):
        d = self._get_dir(path)
        return [x for x in d._children_dirs], [x for x in d._children_files]

    def get_file_info(self, path):
        d = self._get_parent_dir(path)
        return d._children_files[path[-1]]

    def _get_dir(self, path):
        d = self._root
        for comp in path:
            d = d._children_dirs[comp]
        return d

    def _get_parent_dir(self, path):
        return self._get_dir(path[:-1])


class FakeDirInfo(object):
    def __init__(self):
        self._children_dirs = {}
        self._children_files = {}

    def _add_file(self, name, f):
        assert name not in self._children_dirs
        assert name not in self._children_files
        self._children_files[name] = f


class FakeFileInfo(object):
    def __init__(self, path, filetype):
        self.filetype = filetype
        if filetype in ('file', 'symlink'):
            self.contentid = path
        else:
            self.contentid = b''


class TestMakeShadowTree(unittest.TestCase):

    def get_filesystem(self, which):
        assert which == 'local'
        return self.tree

    def open_backupstorage(self, tree, path):
        assert tree == self.tree
        for coll in self._storages:
            if tree == coll._tree and path == coll._path:
                return coll

    def _make_storage(self, tree, path):
        bc = FakeStorage(tree, path)
        self._storages.append(bc)
        return bc

    def setUp(self):
        self._setUp_set_common_vars()
        self._setUp_make_basic_objects()
        self._setUp_make_storage_and_backup()
        self._setUp_set_up_file_system()
        self._setUp_set_up_args()

    def _setUp_set_common_vars(self):
        self.maxDiff = None
        self.shadowroot = ('the', 'cwd', 'shadow')
        self._storages = []

    def _setUp_make_basic_objects(self):
        self.tree = FakeTree()
        self.config = FakeConfig()
        self.args = FakeArgs()

    def _setUp_make_storage_and_backup(self):
        self.config._makestorage(self.tree, ('backup1',))
        self.bc = self._make_storage(self.tree, ('backup1',))
        self.bk = self.bc._make_backup('2014-08-05T05:07')

    def _setUp_set_up_file_system(self):
        self.tree._path_exists[('the', 'cwd', 'shadow')] = False

    def _setUp_set_up_args(self):
        args = self.args
        args.services['filesystem'] = self.get_filesystem
        args.services['backupstorage.open'] = self.open_backupstorage
        args.target = 'shadow'
        args.snapshotname = '2014-08-05T05:07'

    def assertDirsCreated(self, dirs):
        self.assertCountEqual(
            [('mkdir', x) for x in dirs],
             self.tree._actions)

    def assertShadowCopiesMade(self, items):
        self.assertCountEqual(items, self.bc._shadow_copies)

    def assertLogEmpty(self):
        self.assertEqual([], self.args._logger._log)

    def assertLogIs(self, items):
        self.assertEqual(items, self.args._logger._log)

    def run_makeshadowtree(self):
        task = task_makeshadowtree.MakeShadowTreeTask(self.config, self.args)
        task.execute()

    def assertRunMakeShadowTreeRaisesRegex(self, exc, re):
        task = task_makeshadowtree.MakeShadowTreeTask(self.config, self.args)
        self.assertRaisesRegex(exc, re, task.execute)

    def test_make_shadow_tree_for_empty_snapshot(self):
        self.run_makeshadowtree()
        self.assertDirsCreated((self.shadowroot,))
        self.assertShadowCopiesMade(())
        self.assertLogEmpty()

    def test_make_shadow_tree_at_existing_path(self):
        self.tree._path_exists[self.shadowroot] = True
        self.assertRunMakeShadowTreeRaisesRegex(OSError, 'File exists.*shadow')
        self.assertDirsCreated(())
        self.assertShadowCopiesMade(())

    def test_make_shadow_tree_from_missing_snapshot(self):
        name = '2014-08-03T11:11'
        self.args.snapshotname = name
        self.run_makeshadowtree()
        self.assertDirsCreated(())
        self.assertShadowCopiesMade(())
        self.assertLogIs([('ERROR', 'snapshot missing', name, None)])

    def test_make_shadow_tree_for_small_snapshot(self):
        files = self._make_small_snapshot(self.bk)
        self.run_makeshadowtree()
        self.assertDirsCreated(
            [self.shadowroot + x for x in ((), ('path',), ('path', 'to'))])
        self.assertShadowCopiesMade(
            [(self.shadowroot + x[0], x[1]) for x in files])
        self.assertLogEmpty()

    def test_make_shadow_tree_with_special_file(self):
        files = self._make_snapshot_with_special_files(self.bk)
        self.run_makeshadowtree()
        self.assertDirsCreated(
            [self.shadowroot + x for x in ((), ('path',), ('path', 'to'))])
        self.assertShadowCopiesMade(
            [(self.shadowroot + x[0], x[1]) for x in files
             if x[1].filetype == 'symlink'])
        self.assertLogEmpty()

    def _make_small_snapshot(self, bk):
        for d in (('path',), ('path', 'to')):
            self.tree._path_exists[self.shadowroot + d] = False
        names = (
            ('path', 'to', 'file'),
            ('toplevel',),
            ('path', 'to', 'other file'))
        return [(x, self.bk._add_file(x)) for x in names]

    def _make_snapshot_with_special_files(self, bk):
        for d in (('path',), ('path', 'to')):
            self.tree._path_exists[self.shadowroot + d] = False
        names = (
            ('socket', ('path', 'to', 'file')),
            ('symlink', ('toplevel',)),
            ('device', ('path', 'to', 'other file')))
        return [(x[1], self.bk._add_file(x[1], filetype=x[0]))
                for x in names]


# test: target directory does not support hard links from the storage
# test: target directory only supports hard links from second storage
