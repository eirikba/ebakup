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
        self._prepare_initial_backup()
        result = self._run_ebakup('backup', '--create', 'main')
        result.assertSuccess()
        result.assertOutputEmpty()
        self.assertInitialBackupIsCorrect()

    def test_second_backup(self):
        self._prepare_second_backup()
        result = self._run_ebakup('backup', 'main')
        result.assertSuccess()
        result.assertOutputEmpty()
        self.assertSecondBackupIsCorrect()

    def _prepare_initial_backup(self):
        self._make_initial_config()
        self._make_initial_source()
        self.fake_start_time = '2014-03-29T20:20:40.369238'

    def _prepare_second_backup(self):
        self._prepare_initial_backup()
        self._run_ebakup('backup', '--create', 'main')
        self._make_source_for_second_backup()
        self.fake_start_time = '2014-04-17T01:01:43.623171'

    def _make_initial_config(self):
        self._write_file('config', _data.config_1)

    def _make_initial_source(self):
        if os.path.exists(makepath('source')):
            shutil.rmtree(makepath('source'))
        _data.source_tree_1.write_to_disk(makepath('source'))

    def _make_source_for_second_backup(self):
        if os.path.exists(makepath('source')):
            shutil.rmtree(makepath('source'))
        _data.source_tree_2.write_to_disk(makepath('source'))

    def assertInitialBackupIsCorrect(self):
        checker = BackupChecker(self, makepath('backup'), '2014-03-29T20:20')
        tree = _data.source_tree_1.clone(ignore_subtree='tmp')
        checker.set_reference_tree(tree)
        checker.assertSameFilesPresent()
        checker.assertFilesHaveCorrectContentAndChecksums()

    def assertSecondBackupIsCorrect(self):
        checker = BackupChecker(self, makepath('backup'), '2014-04-17T01:01')
        tree = _data.source_tree_2.clone(ignore_subtree='tmp')
        checker.set_reference_tree(tree)
        checker.assertSameFilesPresent()
        checker.assertFilesHaveCorrectContentAndChecksums()

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
_data.source_tree_1 = FileTree()
for path, content in (
    ('photos/DSC_2473.JPG', _data.make_data(size=102856)),
    ('photos/DSC_2474.JPG', _data.make_data(size=95172)),
    ('photos/DSC_2475.JPG', _data.make_data(size=26669)),
    ('tmp/notes.txt', _data.make_data(size=96962)),
    ('tmp/stuff.dat', _data.make_data(size=147674)),
    ('music/Seigmen-Slaver_av_solen_I-2-Dr\u00e5ben.mp3',
     _data.make_data(size=93158)),
    ('music/Garnet_crow-The_twilight_valley-'
     '07-\u5411\u65e5\u8475\u306e\u8272.ogg',
     _data.make_data(size=59709)),
    (b'other/broken\xa4utf8'.decode('utf-8', errors='surrogateescape'),
     _data.make_data(size=37)),
    ('other/plain', _data.make_data(size=809))):
    _data.source_tree_1.add_file(path, content=content)

_data.set_seed(b'second set of source files')
_data.source_tree_2 = FileTree()
_data.source_tree_2.add_files_from_tree(_data.source_tree_1)
_data.source_tree_2.drop_file('tmp/notes.txt')
_data.source_tree_2.add_file('tmp/more notes.txt', _data.make_data(size=2157))
_data.source_tree_2.change_file(
    'other/plain', content=_data.make_data(size=809))
_data.source_tree_2.add_file('new dir/new file', _data.make_data(size=6618))
