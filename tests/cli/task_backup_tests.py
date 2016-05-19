#!/usr/bin/env python3

import unittest

import pyebakup.cli.task_backup as task_backup

class FakeConfig(object):
    def __init__(self):
        self._backups = {}

    def get_backup_by_name(self, name):
        return self._backups.get(name)

class FakeBackupConfig(object):
    def __init__(self, name):
        self._name = name
        self.storages = []
        self.sources = []

    def _add_simple_storage(self):
        coll = FakeStorageData(FakeTree('local'), ('data', 'backup'))
        self.storages.append(coll)
        source = FakeBackupSource(FakeTree('local'), ('home', 'me'), ())
        source.subtree_handlers = [
            (('tmp',), 'ignore'),
            (('photos','mine'), 'static'),
            (('photos','mine','edited'), 'dynamic'),
            ]
        self.sources.append(source)

class FakeBackupSource(object):
    def __init__(self, filesystem, path, targetpath):
        self.filesystem = filesystem
        self.path = path
        self.targetpath = targetpath
        self.subtree_handlers = None

class FakeTree(object):
    def __init__(self, name):
        self._fsname = name

    def is_accessible(self):
        return True

    def path_to_full_string(self, path):
        return 'faketree:' + str(path)

class FakeStorageData(object):
    def __init__(self, filesystem, path):
        self.filesystem = filesystem
        self.path = path

def open_storage(tree, path, services=None):
    return FakeStorage(services.get('database.open'), tree, path)

def create_storage(tree, path, services=None):
    raise NotImplementedError()

class FakeStorage(object):
    def __init__(self, action, tree, path):
        self._open_action = action
        self._tree = tree
        self._path = path

class FakeBackupOperation(object):
    def __init__(self, storage, services=None):
        self._storage = storage
        self._trees = []
        self._backup_done = False
        self._services = services

    def add_tree_to_backup(self, tree, sourcepath, targetpath):
        assert not self._backup_done
        tree = FakeBackupTree(tree, sourcepath, targetpath)
        self._trees.append(tree)
        return tree

    def execute_backup(self):
        assert not self._backup_done
        self._backup_done = True

class FakeHandlerBuilder(object):
    def __init__(self, bktree, path):
        self.bktree = bktree
        self.path = path

    def make_subtree(self, component):
        return FakeHandlerBuilder(self.bktree, self.path + (component,))

    def set_ignored(self):
        self.bktree._handlers.append((self.path, 'ignore'))

    def set_backed_up(self):
        self.bktree._handlers.append((self.path, 'dynamic'))

    def set_backed_up_static(self):
        self.bktree._handlers.append((self.path, 'static'))

class FakeBackupTree(object):
    def __init__(self, tree, sourcepath, targetpath):
        self._tree = tree
        self._sourcepath = sourcepath
        self._targetpath = targetpath
        self._handlers = None

    def set_backup_handlers(self, handlers):
        self._handlers = handlers

class FakeUIState(object):
    def set_status(self, key, value):
        pass

class FakeArgs(object):
    def __init__(self, config):
        self._config = config
        self.services = {
            'backupoperation': self.create_operation,
            'backupstorage.open': open_storage,
            'backupstorage.create': create_storage,
            'database.create': 'argdbcreate',
            'database.open': 'argdbopen',
            'logger': 'arglogger',
            'uistate': FakeUIState(),
            'utcnow': 'argutcnow',
            }
        self.create = False
        self.backups = []
        self._operations = []

    def create_operation(self, storage, services=None):
        op = FakeBackupOperation(storage, services)
        self._operations.append(op)
        return op

    def _add_backup_config(self, name):
        cfg = FakeBackupConfig(name)
        self._config._backups[name] = cfg
        self.backups.append(name)
        return cfg

class TestSimpleBackup(unittest.TestCase):
    def setUp(self):
        config = FakeConfig()
        args = FakeArgs(config)
        self.args = args
        bkupcfg = args._add_backup_config('backup1')
        storage = bkupcfg._add_simple_storage()
        bkup = task_backup.BackupTask(config, args)
        bkup.execute()

    def test_backup_performed(self):
        self.assertEqual(1, len(self.args._operations))
        operation = self.args._operations[0]
        self.assertTrue(operation._backup_done)

    def test_backup_performed_to_existing_storage(self):
        operation = self.args._operations[0]
        storage = operation._storage
        self.assertEqual('argdbopen', storage._open_action)

    def test_backup_to_storage_with_correct_path(self):
        operation = self.args._operations[0]
        storage = operation._storage
        self.assertEqual('local', storage._tree._fsname)
        self.assertEqual(('data', 'backup'), storage._path)

    def test_backup_from_and_to_correct_path(self):
        operation = self.args._operations[0]
        self.assertEqual(1, len(operation._trees))
        tree = operation._trees[0]
        self.assertEqual('local', tree._tree._fsname)
        self.assertEqual(('home', 'me'), tree._sourcepath)
        self.assertEqual((), tree._targetpath)

    def test_backup_handlers(self):
        operation = self.args._operations[0]
        tree = operation._trees[0]
        self.assertCountEqual(
            ((('tmp',),'ignore'),
             (('photos','mine'), 'static'),
             (('photos','mine', 'edited'), 'dynamic')),
            tree._handlers)
