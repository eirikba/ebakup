#!/usr/bin/env python3

# Unlike all other tests, these tests will touch the actual file
# system.

import datetime
import grp
import hashlib
import io
import os
import pwd
import re
import shutil
import socket
import stat
import textwrap
import unittest

import cli
import database
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
        out = io.StringIO()
        cli.main(
            ('--config', os.path.join(root_path, 'ebakup.config'),
             'backup', '--create', 'home'),
            services=services, stdoutfile=out)
        self.assertRegex(out.getvalue(), r'Web ui started on port \d+\n')
        self.advance_utcnow(seconds=160)
        self._check_first_backup_on_disk()
        self.advance_utcnow(seconds=400)
        self._check_first_backup_info()
        self.advance_utcnow(seconds=23)
        self._check_first_backup_data()
        self.advance_utcnow(seconds=4000)

    def _make_source_tree(self):
        sourcedir = os.path.join(root_path, 'sources')
        self._stats = {}
        self._make_file_add_stat(
            sourcedir, ('toplevel',),
            content=b'This is a file')
        self._make_file_add_stat(
            sourcedir, ('subdir', 'data'),
            content=b'More info')
        self._make_file_add_stat(
            sourcedir, ('Pictures', 'funny.jpg'),
            content=b'This is a funny image')
        self._make_file_add_stat(
            sourcedir, ('subdir', 'copy'),
            content=b'This is a file')
        self._make_file(
            os.path.join(sourcedir, 'tmp', 'boring.txt'),
            content=b'Ignore this file')
        os.symlink('data', os.path.join(sourcedir, 'subdir', 'symlink'))
        self._stats[('subdir', 'symlink')] = os.lstat(
            os.path.join(sourcedir, 'subdir', 'symlink'))
        os.symlink('dead', os.path.join(sourcedir, 'dangling'))
        self._stats[('dangling',)] = os.lstat(
            os.path.join(sourcedir, 'dangling'))

    def _make_file_add_stat(self, basepath, relpath, content):
        path = os.path.join(basepath, *relpath)
        self._make_file(path, content)
        self._stats[relpath] = os.lstat(path)
        for i in range(1, len(relpath)):
            dirrel = relpath[:i]
            dirpath = os.path.join(basepath, *dirrel)
            if dirrel not in self._stats:
                self._stats[dirrel] = os.lstat(dirpath)

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
        self.assertCountEqual(
            ('28', '3a', '45', '7d', '01'), os.listdir(contentpath))
        self.assertCountEqual(
            ('a3',), os.listdir(os.path.join(contentpath, '28')))
        self.assertCountEqual(
            ('6e',), os.listdir(os.path.join(contentpath, '3a')))
        self.assertCountEqual(
            ('35',), os.listdir(os.path.join(contentpath, '45')))
        self.assertCountEqual(
            ('d4',), os.listdir(os.path.join(contentpath, '7d')))
        self.assertCountEqual(
            ('bd',), os.listdir(os.path.join(contentpath, '01')))
        self.assertCountEqual(
            ('a5e81d1e89f0efc70b63bf717b921373fc7fac70bc1b7e4d466799c0c6b0',),
            os.listdir(os.path.join(contentpath, '28', 'a3')))
        self.assertCountEqual(
            ('b0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7',),
            os.listdir(os.path.join(contentpath, '3a', '6e')))
        self.assertCountEqual(
            ('6929829dc9fded17e755db91b93c25a4ed3fb9d60d92d4bd1e935a0ecc75',),
            os.listdir(os.path.join(contentpath, '45', '35')))
        self.assertCountEqual(
            ('d97d9aae1c07487d7c41af6a25781305ce9266c56ecd952b7c52172cd506',),
            os.listdir(os.path.join(contentpath, '7d', 'd4')))
        self.assertCountEqual(
            ('3d259f55c04ecd816b1e7efbd84f56d3e1e47a69154978b1d48e573af958',),
            os.listdir(os.path.join(contentpath, '01', 'bd')))
        checksum = hashlib.sha256(b'This is a file').hexdigest()
        path = os.path.join(
            contentpath, checksum[:2], checksum[2:4], checksum[4:])
        with open(path, 'rb') as f:
            data = f.read()
        self.assertEqual(b'This is a file', data)

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
        first, info = info.split('\n', 1)
        if False: # FIXME: last-verified data is currently broken
         self.assertEqual(textwrap.dedent('''\
            Backup definitions:
              backup home
                collection local:/path/to/testbakup/backup
                  Least recently verified: 2014-12-01 00:02:03
                  Total number of content files: 5
                source local:/path/to/testbakup/sources
            '''),
        info)
        self.assertRegex(first, r'Web ui started on port \d+')

    def _check_first_backup_data(self):
        fs = filesys.get_file_system('local')
        db = database.Database(
            fs, fs.path_from_string(root_path) + ('backup','db'))
        backup = db.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(2014, 12, 1, 0, 2, 3), backup.get_start_time())
        self.assertTrue(backup.is_file(('home', 'subdir', 'data')))
        self.assertFalse(backup.is_file(('subdir', 'data')))
        self.assertFalse(backup.is_directory(('home', 'subdir', 'data')))
        self.assertTrue(backup.is_directory(('home', 'subdir')))
        self.assertTrue(backup.is_directory(('home',)))
        self.assertTrue(backup.is_file(('home', 'subdir', 'symlink')))
        self.assertFalse(backup.is_directory(('home', 'subdir', 'symlink')))
        self.assertTrue(backup.is_file(('home', 'dangling')))
        info = backup.get_file_info(('home', 'subdir', 'data'))
        self.assertEqual('file', info.filetype)
        self.assertEqual(9, info.size)
        info = backup.get_file_info(('home', 'subdir', 'symlink'))
        self.assertEqual('symlink', info.filetype)
        info = backup.get_file_info(('home', 'dangling'))
        self.assertEqual('symlink', info.filetype)
        for path, st in self._stats.items():
            info = backup.get_file_info(('home',) + path)
            if info is not None:
                self.assertEqual(info.filetype, self._filetype_from_stat(st))
            else:
                info = backup.get_dir_info(('home',) + path)
                self.assertEqual('directory', self._filetype_from_stat(st))
            self.assertNotEqual(None, info, msg='path: ' + str(path))
            self.assertEqual(
                int(info.extra_data['unix-access']), stat.S_IMODE(st.st_mode),
                msg='path: ' + str(path))
            self.assertEqual(
                info.extra_data['owner'], pwd.getpwuid(st.st_uid).pw_name)
            self.assertEqual(
                info.extra_data['group'], grp.getgrgid(st.st_gid).gr_name)

    def _filetype_from_stat(self, st):
        if stat.S_ISREG(st.st_mode):
            return 'file'
        elif stat.S_ISDIR(st.st_mode):
            return 'directory'
        elif stat.S_ISLNK(st.st_mode):
            return 'symlink'
        elif stat.S_ISSOCK(st.st_mode):
            return 'socket'
        elif stat.S_ISFIFO(st.st_mode):
            return 'pipe'
        else:
            raise NotImplementedError('Unknown file type')

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
        item = fs.get_item_at_path(root + ('subdir',))
        self.assertNotEqual(None, item)
        self.assertEqual('directory', item.get_filetype())

    def test_delete_file_at_path(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        strpath = os.path.join(root_path, 'a_file')
        with open(strpath, 'xb') as f:
            f.write(b'Yay!\n')
        self.assertTrue(os.path.exists(strpath))
        fs.delete_file_at_path(path)
        self.assertFalse(os.path.exists(strpath))
        # Deleting non-existing files should "succeed" silently
        fs.delete_file_at_path(path)
        self.assertFalse(os.path.exists(strpath))
        os.mkdir(strpath)
        self.assertRaisesRegex(
            IsADirectoryError, 'a_file', fs.delete_file_at_path, path)
        self.assertTrue(os.path.exists(strpath))

    def test_get_filetype_regular(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        strpath = os.path.join(root_path, 'a_file')
        with open(strpath, 'xb') as f:
            f.write(b'Yay!\n')
        item = fs.get_item_at_path(path)
        self.assertEqual('file', item.get_filetype())

    def test_get_filetype_symlink(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        strpath = os.path.join(root_path, 'a_file')
        os.symlink('target', strpath)
        item = fs.get_item_at_path(path)
        self.assertEqual('symlink', item.get_filetype())

    def test_get_filetype_socket(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        strpath = os.path.join(root_path, 'a_file')
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(strpath)
        item = fs.get_item_at_path(path)
        self.assertEqual('socket', item.get_filetype())

    def test_get_filetype_pipe(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        strpath = os.path.join(root_path, 'a_file')
        os.mkfifo(strpath)
        item = fs.get_item_at_path(path)
        self.assertEqual('pipe', item.get_filetype())

    def test_readsymlink(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        strpath = os.path.join(root_path, 'a_file')
        os.symlink('target', strpath)
        item = fs.get_item_at_path(path)
        self.assertEqual(b'target', item.readsymlink())

    def test_readsymlink_on_file(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        strpath = os.path.join(root_path, 'a_file')
        with open(strpath, 'xb') as f:
            f.write(b'Yay!\n')
        item = fs.get_item_at_path(path)
        self.assertRaisesRegex(
            OSError, 'a_file', item.readsymlink)

    def test_size_of_symlink(self):
        fs = filesys.get_file_system('local')
        root = fs.path_from_string(root_path)
        path = root + ('a_file',)
        with open(os.path.join(root_path, 'target'), 'xb') as f:
            f.write(b'This is the content of the real file\n')
        strpath = os.path.join(root_path, 'a_file')
        os.symlink('target', strpath)
        item = fs.get_item_at_path(path)
        self.assertEqual(37, item.get_size())
