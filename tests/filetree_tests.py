#!/usr/bin/env python3

# Unlike most other tests, some of these tests will touch the actual
# file system.

import os
import shutil
import unittest

import tests.settings

from ebakup_live_helpers.common import root_path, makepath
from ebakup_live_helpers.filetree import FileTree


class TestFileTree(unittest.TestCase):
    def setUp(self):
        os.makedirs(root_path)

    def tearDown(self):
        shutil.rmtree(root_path)

    def assertFileContentEqual(self, content, path):
        with open(path, 'rb') as f:
            actual = f.read()
        self.assertEqual(content, actual)

    def test_empty_tree_has_no_files(self):
        tree = FileTree()
        self.assertCountEqual((), [x for x in tree.iterate_files()])

    def test_tree_with_3_added_files_has_those_files(self):
        tree = self._get_tree_with_3_files()
        self.assertCountEqual(
            ('a file', 'path/to/something.txt', 'path/here'),
            [x for x in tree.iterate_files()])

    def test_tree_with_3_added_files_has_correct_file_content(self):
        tree = self._get_tree_with_3_files()
        self.assertEqual(b'nothing', tree.get_file_content('a file'))
        self.assertEqual(b'', tree.get_file_content('path/to/something.txt'))
        self.assertEqual(b'empty', tree.get_file_content('path/here'))

    def test_files_in_dropped_subtree_are_gone(self):
        tree = self._get_tree_with_3_files()
        tree.drop_subtree('path/to')
        self.assertCountEqual(
            ('a file', 'path/here'), [x for x in tree.iterate_files()])

    def test_dropped_files_are_gone(self):
        tree = self._get_tree_with_3_files()
        tree.drop_file('path/here')
        self.assertCountEqual(
            ('a file', 'path/to/something.txt'),
            [x for x in tree.iterate_files()])

    def test_copied_tree_has_the_same_files(self):
        tree = self._get_tree_with_3_files()
        tree2 = FileTree()
        tree2.add_files_from_tree(tree)
        self.assertCountEqual(
            ('a file', 'path/to/something.txt', 'path/here'),
            [x for x in tree2.iterate_files()])

    def test_copied_tree_has_the_same_content(self):
        tree = self._get_tree_with_3_files()
        tree2 = FileTree()
        tree2.add_files_from_tree(tree)
        self.assertEqual(b'nothing', tree.get_file_content('a file'))
        self.assertEqual(b'', tree.get_file_content('path/to/something.txt'))
        self.assertEqual(b'empty', tree.get_file_content('path/here'))

    def test_cloned_tree_has_the_same_files(self):
        tree = self._get_tree_with_3_files()
        tree2 = tree.clone()
        self.assertCountEqual(
            ('a file', 'path/to/something.txt', 'path/here'),
            [x for x in tree2.iterate_files()])

    def test_cloned_tree_has_the_same_content(self):
        tree = self._get_tree_with_3_files()
        tree2 = tree.clone()
        self.assertEqual(b'nothing', tree.get_file_content('a file'))
        self.assertEqual(b'', tree.get_file_content('path/to/something.txt'))
        self.assertEqual(b'empty', tree.get_file_content('path/here'))

    def test_clone_tree_with_ignored_subpath(self):
        tree = self._get_tree_with_3_files()
        tree2 = tree.clone(ignore_subtree='path')
        self.assertCountEqual(
            ('a file',),
            [x for x in tree2.iterate_files()])

    def test_change_file_changes_file_content(self):
        tree = self._get_tree_with_3_files()
        tree.change_file('a file', content=b'changed')
        self.assertEqual(b'changed', tree.get_file_content('a file'))
        self.assertEqual(b'', tree.get_file_content('path/to/something.txt'))
        self.assertEqual(b'empty', tree.get_file_content('path/here'))

    def test_file_content_found_also_by_name_as_bytes(self):
        tree = self._get_tree_with_3_files()
        self.assertEqual(b'nothing', tree.get_file_content(b'a file'))

    def test_broken_utf8_file_name_string(self):
        tree = FileTree()
        name = b'This\xa0is broken'
        strname = name.decode('utf-8', errors='surrogateescape')
        tree.add_file(strname, b'This is fine')
        self.assertEqual(b'This is fine', tree.get_file_content(strname))
        self.assertEqual(b'This is fine', tree.get_file_content(name))

    def test_broken_utf8_file_name_bytes(self):
        tree = FileTree()
        name = b'This\xa0is broken'
        strname = name.decode('utf-8', errors='surrogateescape')
        tree.add_file(name, b'This is fine')
        self.assertEqual(b'This is fine', tree.get_file_content(strname))
        self.assertEqual(b'This is fine', tree.get_file_content(name))

    @unittest.skipUnless(
        tests.settings.run_live_tests, 'Live tests are disabled')
    def test_write_to_disk(self):
        tree = self._get_tree_with_3_files()
        tree.write_to_disk(makepath('tree'))
        self.assertCountEqual(('a file', 'path'), os.listdir(makepath('tree')))
        self.assertCountEqual(
            ('to', 'here'), os.listdir(makepath('tree', 'path')))
        self.assertCountEqual(
            ('something.txt',), os.listdir(makepath('tree', 'path', 'to')))
        self.assertFileContentEqual(b'nothing', makepath('tree', 'a file'))
        self.assertFileContentEqual(
            b'', makepath('tree', 'path', 'to', 'something.txt'))
        self.assertFileContentEqual(
            b'empty', makepath('tree', 'path', 'here'))

    @unittest.skipUnless(
        tests.settings.run_live_tests, 'Live tests are disabled')
    def test_tree_loaded_from_disk_has_expected_files(self):
        self._make_3_files_on_disk(makepath('tree'))
        tree = FileTree()
        tree.load_from_path(makepath('tree'))
        self.assertCountEqual(
            ('other file', 'some/file/in/subdir.txt', 'some/other file'),
            [x for x in tree.iterate_files()])
        self.assertEqual(b'content', tree.get_file_content('other file'))
        self.assertEqual(
            b'subcontent', tree.get_file_content('some/file/in/subdir.txt'))
        self.assertEqual(b'empty', tree.get_file_content('some/other file'))

    def _get_tree_with_3_files(self):
        tree = FileTree()
        self._add_3_files(tree)
        return tree

    def _add_3_files(self, tree):
        tree.add_file('a file', content=b'nothing')
        tree.add_file('path/to/something.txt', content=b'')
        tree.add_file('path/here', content=b'empty')

    def _make_3_files_on_disk(self, path):
        self.assertIn('DELETEME_testebakup', path)
        for subpath, content in (
                ('other file', b'content'),
                ('some/file/in/subdir.txt', b'subcontent'),
                ('some/other file', b'empty')):
            fullpath = os.path.join(path, subpath)
            subdir = os.path.dirname(fullpath)
            os.makedirs(subdir, exist_ok=True)
            with open(fullpath, 'wb') as f:
                f.write(content)
