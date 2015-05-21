#!/usr/bin/env python3

import unittest

import task_backup

class FakeConfig(object):
    def __init__(self):
        self._backups = {}

    def get_backup_by_name(self, name):
        return self._backups.get(name)

class FakeBackupConfig(object):
    def __init__(self, name):
        self._name = name
        self.collections = []
        self.sources = []

    def _add_simple_collection(self):
        coll = FakeCollectionData('local', ('data', 'backup'))
        self.collections.append(coll)
        source = FakeBackupSource('local', ('home', 'me'), ())
        source._handlers = [
            (('tmp',), 'ignore'),
            (('photos','mine'), 'static'),
            (('photos','mine','edited'), 'dynamic'),
            ]
        self.sources.append(source)

class FakeBackupSource(object):
    def __init__(self, access, path, targetpath):
        self.accessor = access
        self.path = path
        self.targetpath = targetpath
        self._handlers = []

    def iterate_path_handlers(self):
        for handler in self._handlers:
            yield handler

class FakeTree(object):
    def __init__(self, accessor):
        self._accessor = accessor

    def is_accessible(self):
        return True

class FakeCollectionData(object):
    def __init__(self, access, path):
        self.accessor = access
        self.path = path

class FakeCollectionFactory(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path

    def set_database_creator(self, creator):
        self._creator = creator

    def set_database_opener(self, opener):
        self._opener = opener

    def open_collection(self):
        return FakeCollection(self._opener, self._tree, self._path)

class FakeCollection(object):
    def __init__(self, action, tree, path):
        self._open_action = action
        self._tree = tree
        self._path = path

    def set_utcnow(self, utcnow):
        self._utcnow = utcnow

    def set_logger(self, logger):
        self._logger = logger

class FakeBackupOperation(object):
    def __init__(self, collection):
        self._collection = collection
        self._trees = []
        self._backup_done = False

    def set_logger(self, logger):
        assert not self._backup_done
        self._logger = logger

    def add_tree_to_backup(self, tree, sourcepath, targetpath):
        assert not self._backup_done
        tree = FakeBackupTree(tree, sourcepath, targetpath)
        self._trees.append(tree)
        return tree

    def execute_backup(self):
        assert not self._backup_done
        self._backup_done = True

class FakeBackupTree(object):
    def __init__(self, tree, sourcepath, targetpath):
        self._tree = tree
        self._sourcepath = sourcepath
        self._targetpath = targetpath
        self._handlers = []

    def ignore_subtree(self, path):
        self._handlers.append((path, 'ignore'))

    def back_up_static_subtree(self, path):
        self._handlers.append((path, 'static'))

    def back_up_subtree(self, path):
        self._handlers.append((path, 'dynamic'))

class FakeArgs(object):
    def __init__(self, config):
        self._config = config
        self.logger = 'arglogger'
        self.factories = {
            'backupoperation': self.create_operation,
            'backupcollection': FakeCollectionFactory,
            'filesystem': FakeTree,
            'database.create': 'argdbcreate',
            'database.open': 'argdbopen',
            'utcnow': 'argutcnow',
            }
        self.create = False
        self.backups = []
        self._operations = []

    def create_operation(self, collection):
        op = FakeBackupOperation(collection)
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
        collection = bkupcfg._add_simple_collection()
        bkup = task_backup.BackupTask(config, args)
        bkup.execute()

    def test_backup_performed(self):
        self.assertEqual(1, len(self.args._operations))
        operation = self.args._operations[0]
        self.assertTrue(operation._backup_done)

    def test_backup_performed_to_existing_collection(self):
        operation = self.args._operations[0]
        collection = operation._collection
        self.assertEqual('argdbopen', collection._open_action)

    def test_backup_to_collection_with_correct_path(self):
        operation = self.args._operations[0]
        collection = operation._collection
        self.assertEqual('local', collection._tree._accessor)
        self.assertEqual(('data', 'backup'), collection._path)

    def test_backup_from_and_to_correct_path(self):
        operation = self.args._operations[0]
        self.assertEqual(1, len(operation._trees))
        tree = operation._trees[0]
        self.assertEqual('local', tree._tree._accessor)
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
