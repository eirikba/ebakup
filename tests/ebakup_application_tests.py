#!/usr/bin/env python3

# Unlike most other tests, some of these tests will touch the actual
# file system.

import datetime
import hashlib
import os
import shutil
import unittest

import tests.settings

from ebakup_live_helpers.backupreader import BackupReader
from ebakup_live_helpers.common import root_path, makepath
from ebakup_live_helpers.contentreader import ContentReader
from ebakup_live_helpers.ebakupinvocation import EbakupInvocation
from ebakup_live_helpers.filetree import FileTree

class TestEbakupInvocation(unittest.TestCase):
    '''Tests the most trivial invocations of ebakup, and so doubles as
    tests for the EbakupInvocation helper class.
    '''
    def test_run_ebakup_without_args_exits_with_failure(self):
        runner = EbakupInvocation()
        runner.set_testcase(self)
        runner.allowDefaultConfig()
        runner.run()
        runner.assertFailed()

    def test_run_ebakup_without_args_shows_usage_message(self):
        runner = EbakupInvocation()
        runner.set_testcase(self)
        runner.allowDefaultConfig()
        runner.run()
        runner.assertOutputMatchesRegex(b'^usage: ebakup')

    def test_run_ebakup_with_help_shows_help_message(self):
        runner = EbakupInvocation('--help')
        runner.set_testcase(self)
        runner.allowDefaultConfig()
        runner.run()
        runner.assertSuccess()
        runner.assertOutputMatchesRegex(b'^usage: ebakup')
        runner.assertOutputMatchesRegex(b'\n  -h, --help +show this help')


class TestFileTree(unittest.TestCase):
    def setUp(self):
        os.makedirs(root_path)

    def tearDown(self):
        shutil.rmtree(root_path)

    def test_empty_tree_has_no_files(self):
        tree = FileTree()
        self.assertCountEqual((), [x for x in tree.iterate_files()])

    def test_tree_with_3_added_files_has_those_files(self):
        tree = FileTree()
        self._add_3_files(tree)
        self.assertCountEqual(
            ('a file', 'path/to/something.txt', 'path/here'),
            [x for x in tree.iterate_files()])

    def test_tree_with_3_added_files_has_correct_file_content(self):
        tree = FileTree()
        self._add_3_files(tree)
        self.assertEqual(b'nothing', tree.get_file_content('a file'))
        self.assertEqual(b'', tree.get_file_content('path/to/something.txt'))
        self.assertEqual(b'empty', tree.get_file_content('path/here'))

    def test_files_in_dropped_subtree_are_gone(self):
        tree = FileTree()
        self._add_3_files(tree)
        tree.drop_subtree('path/to')
        self.assertCountEqual(
            ('a file', 'path/here'), [x for x in tree.iterate_files()])

    def test_file_content_found_also_by_name_as_bytes(self):
        tree = FileTree()
        self._add_3_files(tree)
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
    def test_tree_loaded_from_disk_has_expected_files(self):
        self._make_3_files_on_disk(makepath('tree'))
        tree = FileTree()
        tree.load_from_path(makepath('tree'))
        self.assertCountEqual(
            ('other file', 'some/file/in/subdir.txt', 'some/other file'),
            [x for x in tree.iterate_files()])

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


class BackupChecker(object):
    def __init__(self, testcase, dbpath, bkname):
        self.tc = testcase
        self.dbpath = dbpath
        self.bk = BackupReader(dbpath, bkname)
        self.content = ContentReader(dbpath)

    def set_reference_tree(self, tree):
        self.tree = tree

    def assertSameFilesPresent(self):
        treepaths = set(x for x in self.tree.iterate_files())
        bkpaths = set(x.decode('utf-8', errors='surrogateescape')
            for x in self.bk.iterate_files())
        self.tc.assertEqual(treepaths, bkpaths)

    def assertFilesHaveCorrectContentAndChecksums(self):
        for path in self.bk.iterate_files():
            self.assertFileHaveCorrectContent(path)
            self.assertFileHaveCorrectChecksum(path)

    def assertFileHaveCorrectContent(self, path):
        data = self.tree.get_file_content(path)
        bkdata = self._get_bk_data_for_path(path)
        self.tc.assertEqual(data, bkdata)

    def _get_bk_data_for_path(self, path):
        finfo = self.bk.get_file_info(path)
        cpath = makepath('backup', self.content.get_path(finfo.cid))
        with open(cpath, 'rb') as f:
            data = f.read()
        return data

    def assertFileHaveCorrectChecksum(self, path):
        data = self.tree.get_file_content(path)
        checksum = hashlib.sha256(data).digest()
        bkchecksum = self._get_bk_checksum_for_path(path)
        self.tc.assertEqual(checksum, bkchecksum)

    def _get_bk_checksum_for_path(self, path):
        finfo = self.bk.get_file_info(path)
        cinfo = self.content.get_content_info(finfo.cid)
        return cinfo.checksum


@unittest.skipUnless(tests.settings.run_live_tests, 'Live tests are disabled')
class TestEbakupLive(unittest.TestCase):

    def setUp(self):
        os.makedirs(root_path)

    def tearDown(self):
        if True:
            shutil.rmtree(root_path)

    def test_initial_backup(self):
        self._make_initial_config()
        self._make_initial_source()
        self.fake_start_time = '2014-03-29T20:20:40.369238'
        result = self._run_ebakup('backup', '--create', 'main')
        result.assertSuccess()
        result.assertOutputEmpty()
        self.assertInitialBackupIsCorrect()

    def _make_initial_config(self):
        self._write_file('config', _data.config_1)

    def _make_initial_source(self):
        for path, content in _data.source_1_files:
            self._write_file(os.path.join('source', path), content)

    def assertInitialBackupIsCorrect(self):
        checker = BackupChecker(self, makepath('backup'), '2014-03-29T20:20')
        tree = self._make_tree_from_path(makepath('source'), ignore_path='tmp')
        checker.set_reference_tree(tree)
        checker.assertSameFilesPresent()
        checker.assertFilesHaveCorrectContentAndChecksums()

    def _make_tree_from_path(self, path, ignore_path=None):
        tree = FileTree()
        tree.load_from_path(path)
        if ignore_path is not None:
            tree.drop_subtree(ignore_path)
        return tree

    def _write_file(self, innerpath, content):
        if isinstance(content, str):
            content = content.encode('utf-8')
        path = makepath(innerpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'xb') as f:
            wrote = f.write(content)
            self.assertEqual(len(content), wrote)

    def _run_ebakup(self, *args):
        runner = EbakupInvocation(
            '--config', makepath('config'),
            '--fake-start-time', self.fake_start_time,
            *args)
        runner.set_testcase(self)
        runner.run()
        return runner


class _data(object):
    @classmethod
    def make_data(cls, size):
        return cls._get_random_bytes(size)

    @classmethod
    def set_seed(cls, seed):
        cls._entropy = b''
        cls._seed = seed

    @classmethod
    def _get_random_bytes(cls, count):
        value = b''
        while len(cls._entropy) <= count:
            value += cls._entropy
            count -= len(cls._entropy)
            cls._entropy = b''
            cls._generate_entropy()
        value += cls._entropy[:count]
        cls._entropy = cls._entropy[count:]
        return value

    @classmethod
    def _generate_entropy(cls):
        cls._entropy += hashlib.md5(cls._seed).digest()


_data.config_1 = (
    'backup main\n'
    '  collection local:' + root_path + '/backup\n'
    '  source local:' + root_path + '/source\n'
    '    paths tmp transient\n'
    '      ignore\n'
    '    path photos\n'
    '      static' )

_data.set_seed(b'initial source files')
_data.source_1_files = (
    ('photos/DSC_2473.JPG', _data.make_data(size=102856)),
    ('photos/DSC_2474.JPG', _data.make_data(size=95172)),
    ('photos/DSC_2475.JPG', _data.make_data(size=26669)),
    ('tmp/notes.txt', _data.make_data(size=96962)),
    ('tmp/stuff.dat', _data.make_data(size=147674)),
    ('music/Seigmen-Slaver_av_solen_I-2-Dr\u00e5ben.mp3',
     _data.make_data(size=93158)),
    ('music/Garnet_crow-The_twilight_valley-07-\u5411\u65e5\u8475\u306e\u8272',
     _data.make_data(size=59709)),
    (b'other/broken\xa4utf8'.decode('utf-8', errors='surrogateescape'),
     _data.make_data(size=37)),
    ('other/plain', _data.make_data(size=809)))