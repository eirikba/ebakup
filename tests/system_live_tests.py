#!/usr/bin/env python3

# Unlike most other tests, these tests will touch the actual file
# system.

import os
import shutil
import socket
import unittest

import filesys
import tests.settings

root_path = os.path.abspath(os.path.join(os.getcwd(), 'DELETEME_testebakup'))


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
