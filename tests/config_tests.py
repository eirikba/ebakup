#!/usr/bin/env python3

import textwrap
import unittest

import config


class FakeTree(object):
    def __init__(self):
        self._paths = {}

    def set_file(self, path, content=None):
        fd = FakeFileData()
        if content:
            fd.content = content
        self._paths[path] = fd

    def get_item(self, path):
        return FakeFile(self, path)

class FakeFileData(object): pass

class FakeFile(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        pass

    def get_data_slice(self, start, end):
        assert start >= 0
        assert end >= 0
        return self._tree._paths[self._path].content[start:end]

    def get_size(self):
        return len(self._tree._paths[self._path].content)

class TestSimpleConfig(unittest.TestCase):

    def setUp(self):
        conf = config.Config()
        self.config = conf
        tree = FakeTree()
        self.tree = tree
        tree.set_file(
            ('path', 'to', 'config'),
            content=textwrap.dedent('''\
                backup home
                   collection local:/backup/mine
                   source local:/home/me
                       targetpath home
                       path tmp
                           ignore
                           path Q.pdf
                               static
                       path Pictures
                           static
                ''').encode('utf-8'))
        conf.read_file(tree, ('path', 'to', 'config'))

    def test_backup_home(self):
        backup = self.config.get_backup_by_name('home')
        self.assertNotEqual(None, backup)
        self.assertEqual('home', backup.name)

    def test_backup_home_collection(self):
        backup = self.config.get_backup_by_name('home')
        self.assertNotEqual(None, backup)
        self.assertEqual(1, len(backup.collections))
        collection = backup.collections[0]
        self.assertEqual('local', collection.accessor)
        self.assertEqual(('backup', 'mine'), collection.path)

    def test_backup_home_source(self):
        backup = self.config.get_backup_by_name('home')
        self.assertNotEqual(None, backup)
        self.assertEqual(1, len(backup.sources))
        source = backup.sources[0]
        self.assertEqual('local', source.accessor)
        self.assertEqual(('home', 'me'), source.path)
        self.assertEqual(('home',), source.targetpath)

    def test_backup_home_source_item_handlers(self):
        backup = self.config.get_backup_by_name('home')
        source = backup.sources[0]
        self.assertEqual('dynamic', source.tree.get_handler_for_path(()))
        self.assertEqual(
            'dynamic', source.tree.get_handler_for_path(('random', 'path')))
        self.assertEqual('ignore', source.tree.get_handler_for_path(('tmp',)))
        self.assertEqual(
            'static', source.tree.get_handler_for_path(('tmp', 'Q.pdf')))
        self.assertEqual(
            'ignore', source.tree.get_handler_for_path(('tmp', 'R.pdf')))
        self.assertEqual(
            'static',
            source.tree.get_handler_for_path(('tmp', 'Q.pdf', 'other')))
        self.assertEqual(
            'static',
            source.tree.get_handler_for_path(
                ('Pictures', 'funny', 'strange man.jpg')))
        self.assertEqual(
            'static', source.tree.get_handler_for_path(('Pictures', 'mine')))
        self.assertEqual(
            'static', source.tree.get_handler_for_path(('Pictures',)))
        self.assertEqual(
            'dynamic', source.tree.get_handler_for_path(('Picture',)))
        self.assertEqual(
            'dynamic', source.tree.get_handler_for_path(('Picturess',)))

    def test_backup_home_source_subtree_handlers(self):
        backup = self.config.get_backup_by_name('home')
        source = backup.sources[0]
        self.assertFalse(source.tree.is_whole_subtree_ignored(()))
        self.assertTrue(source.tree.does_subtree_contain_static_items(()))
        self.assertFalse(source.tree.is_whole_subtree_ignored(('tmp',)))
        self.assertTrue(source.tree.does_subtree_contain_static_items(('tmp',)))
        self.assertTrue(source.tree.is_whole_subtree_ignored(('tmp', 'other')))
        self.assertFalse(
            source.tree.does_subtree_contain_static_items(('tmp', 'other')))
        self.assertFalse(
            source.tree.is_whole_subtree_ignored(('Pictures', 'mine')))
        self.assertTrue(
            source.tree.does_subtree_contain_static_items(('Pictures', 'mine')))
