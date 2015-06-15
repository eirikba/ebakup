#!/usr/bin/env python3

# Unlike all other tests, these tests will touch the actual file
# system.

import datetime
import hashlib
import io
import os
import re
import shutil
import textwrap
import unittest

import cli
import filesys
import tests.settings

root_path = os.path.abspath(os.path.join(os.getcwd(), 'DELETEME_testebakup'))

class TestBackup(unittest.TestCase):
    def setUp(self):
        self._utcnow_dt = datetime.datetime(2014, 12, 1)
        os.makedirs(root_path)

    def tearDown(self):
        shutil.rmtree(root_path)

    def advance_utcnow(self, seconds=0):
        self._utcnow_dt += datetime.timedelta(seconds=seconds)

    def utcnow(self):
        return self._utcnow_dt

    @unittest.skipUnless(
        tests.settings.run_live_tests,
        'Live tests are disabled')
    def test_all(self):
        self._make_source_tree()
        self._make_config()
        services = { '*': None, 'utcnow': self.utcnow }
        self.services = services
        self.advance_utcnow(seconds=123)
        cli.main(
            ('--config', os.path.join(root_path, 'ebakup.config'),
             'backup', '--create', 'home'),
            services=services)
        self.advance_utcnow(seconds=160)
        self._check_first_backup_on_disk()
        self.advance_utcnow(seconds=400)
        self._check_first_backup_info()
        self.advance_utcnow(seconds=4000)

    def _make_source_tree(self):
        sourcedir = os.path.join(root_path, 'sources')
        self._make_file(
            os.path.join(sourcedir, 'toplevel'),
            content=b'This is a file')
        self._make_file(
            os.path.join(sourcedir, 'subdir', 'data'),
            content=b'More info')
        self._make_file(
            os.path.join(sourcedir, 'Pictures', 'funny.jpg'),
            content=b'This is a funny image')
        self._make_file(
            os.path.join(sourcedir, 'subdir', 'copy'),
            content=b'This is a file')
        self._make_file(
            os.path.join(sourcedir, 'tmp', 'boring.txt'),
            content=b'Ignore this file')

    def _make_file(self, path, content):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'xb') as f:
            f.write(content)

    def _make_config(self):
        self._make_file(
            os.path.join(root_path, 'ebakup.config'),
            content=textwrap.dedent('''\
            backup home
              collection local:''' + root_path + '''/backup
              source local:''' + root_path + '''/sources
                targetpath home
                path Pictures
                  static
                path tmp
                  ignore
            ''').encode('utf-8'))

    def _check_first_backup_on_disk(self):
        contentpath = os.path.join(root_path, 'backup', 'content')
        self.assertCountEqual(('45', '7d', '01'), os.listdir(contentpath))
        self.assertCountEqual(
            ('35',), os.listdir(os.path.join(contentpath, '45')))
        self.assertCountEqual(
            ('d4',), os.listdir(os.path.join(contentpath, '7d')))
        self.assertCountEqual(
            ('bd',), os.listdir(os.path.join(contentpath, '01')))
        self.assertCountEqual(
            ('6929829dc9fded17e755db91b93c25a4ed3fb9d60d92d4bd1e935a0ecc75',),
            os.listdir(os.path.join(contentpath, '45', '35')))
        self.assertCountEqual(
            ('d97d9aae1c07487d7c41af6a25781305ce9266c56ecd952b7c52172cd506',),
            os.listdir(os.path.join(contentpath, '7d', 'd4')))
        self.assertCountEqual(
            ('3d259f55c04ecd816b1e7efbd84f56d3e1e47a69154978b1d48e573af958',),
            os.listdir(os.path.join(contentpath, '01', 'bd')))
        shadowpath = os.path.join(root_path, 'backup', '2014', '12-01T00:02')
        checksum = hashlib.sha256(b'This is a file').hexdigest()
        path = os.path.join(
            contentpath, checksum[:2], checksum[2:4], checksum[4:])
        with open(path, 'rb') as f:
            data = f.read()
        self.assertEqual(b'This is a file', data)
        self.assertTrue(
            os.path.samefile(
                path, os.path.join(shadowpath, 'home', 'toplevel')))
        self.assertTrue(
            os.path.samefile(
                path, os.path.join(shadowpath, 'home', 'subdir', 'copy')))
        self.assertFalse(
            os.path.exists(os.path.join(shadowpath, 'home', 'tmp')))

    def _check_first_backup_info(self):
        out = io.StringIO()
        cli.main(
            ('--config', os.path.join(root_path, 'ebakup.config'),
             'info'),
            services=self.services, stdoutfile=out)
        info = out.getvalue()
        self.assertNotIn('local:/path/to/testbakup/', info)
        match = re.search('local:/[^\n]*/DELETEME_testebakup/', info)
        basepath = match.group(0)
        info = info.replace(basepath, 'local:/path/to/testbakup/')
        self.assertEqual(textwrap.dedent('''\
            Backup definitions:
              backup home
                collection local:/path/to/testbakup/backup
                  Least recently verified: 2014-12-01 00:02:03
                  Total number of content files: 3
                source local:/path/to/testbakup/sources
            '''),
        info)

@unittest.skipUnless(
    tests.settings.run_live_tests,
    'Live tests are disabled')
class TestLocalFileSys(unittest.TestCase):
    def setUp(self):
        os.makedirs(root_path)

    def tearDown(self):
        shutil.rmtree(root_path)

    def test_get_existing_item(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        with open(os.path.join(root_path, 'exist'), 'xb') as f:
            f.write(b'Yay!\n')
        item = fs.get_item_at_path(root + ('exist',))
        self.assertEqual(b'Yay!\n', item.get_data_slice(0, 100))

    def test_get_non_existing_item(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        self.assertRaisesRegex(
            FileNotFoundError,
            'file or directory.*DELETEME_testebakup.*noexist',
            fs.get_item_at_path, root + ('noexist',))

    def test_get_item_that_is_a_directory(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        os.mkdir(os.path.join(root_path, 'subdir'))
        self.assertRaisesRegex(
            IsADirectoryError,
            'is a directory.*DELETEME_testebakup.*subdir',
            fs.get_item_at_path, root + ('subdir',))
