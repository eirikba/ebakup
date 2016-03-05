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


time_1 = '2014-03-29T20:20:40.369238'
time_2 = '2014-04-17T01:01:43.623171'
time_3 = '2014-04-30T08:24:40.640211'


@unittest.skipUnless(tests.settings.run_live_tests, 'Live tests are disabled')
class TestEbakupLive(unittest.TestCase):

    class SharedContext(object):
        def __init__(self):
            if not tests.settings.run_live_tests:
                return
            tree = self._create_tree_after_second_backup()
            self.tree_after_second_backup = tree

        def _create_tree_after_second_backup(self):
            testcase = TestEbakupLive()
            os.makedirs(root_path)
            testcase._setup_second_backup_completed()
            tree = FileTree()
            tree.load_from_path(makepath(''))
            shutil.rmtree(root_path)
            return tree

    shared_context = None

    def setUp(self):
        if self.shared_context is None:
            TestEbakupLive.shared_context = TestEbakupLive.SharedContext()
        os.makedirs(root_path)

    def tearDown(self):
        if True:
            shutil.rmtree(root_path)

    def test_making_first_backup(self):
        self._given_basic_config()
        self._given_source_tree(_data.source_tree_1)
        self._given_current_time_is(time_1)
        result = self._run_ebakup('backup', '--create', 'main')
        result.assertSuccessAndNoOutput()
        self.assertBackupMatchesTree(time_1, _data.backup_tree_1)

    def test_making_second_backup(self):
        self._given_first_backup_completed()
        self._given_source_tree(_data.source_tree_2)
        self._given_current_time_is(time_2)
        result = self._run_ebakup('backup', 'main')
        result.assertSuccessAndNoOutput()
        self.assertBackupMatchesTree(time_2, _data.backup_tree_2)

    def test_cached_state_after_second_backup(self):
        self._given_second_backup_completed()
        self._given_current_time_is(time_3)
        self.assertBackupMatchesTree(time_2, _data.backup_tree_2)

    def test_shadowcopy_matches_tree(self):
        self._given_second_backup_completed()
        self._given_current_time_is(time_3)
        result = self._run_ebakup(
            'shadowcopy',
            '--target', makepath('shadowtest'),
            '2014-04-17T01:01')
        result.assertSuccessAndNoOutput()
        self.assertDirMatchesTree(makepath('shadowtest'), _data.backup_tree_2)
        self.assertFileIsHardLinkToContent(
            makepath('shadowtest', 'other/plain'))

    def assertBackupMatchesTree(self, bkname, tree, dbpath=None):
        if dbpath is None:
            dbpath = makepath('backup')
        checker = BackupChecker(self, dbpath, bkname[:16])
        checker.set_reference_tree(tree)
        checker.assertSameFilesPresent()
        checker.assertFilesHaveCorrectContentAndChecksums()

    def assertFileIsHardLinkToContent(self, path):
        data = self._get_file_content_for_full_path(path)
        content_path = self._get_content_path_for_data(data)
        self.assertTrue(
            os.path.samefile(path, makepath('backup', content_path)))

    def assertDirMatchesTree(self, path, tree):
        for name in tree.iterate_files():
            fdata = self._get_file_content(os.path.join(path, name))
            self.assertEqual(tree.get_file_content(name), fdata)
        for base, dirs, files in os.walk(path):
            for name in files:
                fpath = os.path.relpath(os.path.join(base, name), path)
                self.assertTrue(tree.has_file(fpath), msg=fpath)

    def _given_basic_config(self):
        self._write_file('config', _data.config_1)

    def _given_source_tree(self, tree):
        if os.path.exists(makepath('source')):
            shutil.rmtree(makepath('source'))
        tree.write_to_disk(makepath('source'))

    def _given_current_time_is(self, time):
        self.fake_start_time = time

    def _given_first_backup_completed(self):
        self._given_basic_config()
        self._given_source_tree(_data.source_tree_1)
        self._given_current_time_is(time_1)
        self._run_ebakup('backup', '--create', 'main')

    def _setup_second_backup_completed(self):
        self._given_first_backup_completed()
        self._given_source_tree(_data.source_tree_2)
        self._given_current_time_is(time_2)
        self._run_ebakup('backup', 'main')

    def _given_second_backup_completed(self):
        self.shared_context.tree_after_second_backup.write_to_disk(
            makepath(''))

    def _run_ebakup(self, *args):
        runner = EbakupInvocation(
            '--config', makepath('config'),
            '--fake-start-time', self.fake_start_time,
            *args)
        runner.set_testcase(self)
        runner.run()
        return runner

    def _get_content_path_for_data(self, data):
        digest = hashlib.sha256(data).digest()
        return ContentReader.get_path(digest)

    def _write_file(self, innerpath, content):
        if isinstance(content, str):
            content = content.encode('utf-8')
        path = makepath(innerpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'xb') as f:
            wrote = f.write(content)
            self.assertEqual(len(content), wrote)

    def _get_file_content_for_full_path(self, path):
        with open(path, 'rb') as f:
            return f.read()

    def _get_file_content(self, innerpath):
            return self._get_file_content_for_full_path(makepath(innerpath))


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


    @classmethod
    def _create_data(cls):
        cls.config_1 = (
            'backup main\n'
            '  collection local:' + root_path + '/backup\n'
            '  source local:' + root_path + '/source\n'
            '    paths tmp transient\n'
            '      ignore\n'
            '    path photos\n'
            '      static' )

        cls.set_seed(b'initial source files')
        cls.source_tree_1 = FileTree()
        for path, content in (
                ('photos/DSC_2473.JPG', cls.make_data(size=102856)),
                ('photos/DSC_2474.JPG', cls.make_data(size=95172)),
                ('photos/DSC_2475.JPG', cls.make_data(size=26669)),
                ('tmp/notes.txt', cls.make_data(size=96962)),
                ('tmp/stuff.dat', cls.make_data(size=147674)),
                ('music/Seigmen-Slaver_av_solen_I-2-Dr\u00e5ben.mp3',
                 cls.make_data(size=93158)),
                ('music/Garnet_crow-The_twilight_valley-'
                 '07-\u5411\u65e5\u8475\u306e\u8272.ogg',
                 cls.make_data(size=59709)),
                (b'other/broken\xa4utf8'.decode(
                    'utf-8', errors='surrogateescape'),
                 cls.make_data(size=37)),
                ('other/plain', cls.make_data(size=809))):
            cls.source_tree_1.add_file(path, content=content)

        cls.backup_tree_1 = cls.source_tree_1.clone(ignore_subtree='tmp')

        cls.set_seed(b'second set of source files')
        cls.source_tree_2 = FileTree()
        cls.source_tree_2.add_files_from_tree(cls.source_tree_1)
        cls.source_tree_2.drop_file('tmp/notes.txt')
        cls.source_tree_2.add_file(
            'tmp/more notes.txt', cls.make_data(size=2157))
        cls.source_tree_2.change_file(
            'other/plain', content=cls.make_data(size=809))
        cls.source_tree_2.add_file('new dir/new file', cls.make_data(size=6618))

        cls.backup_tree_2 = cls.source_tree_2.clone(ignore_subtree='tmp')


_data._create_data()
