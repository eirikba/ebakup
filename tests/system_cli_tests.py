#!/usr/bin/env python3

import datetime
import io
import hashlib
import re
import textwrap
import unittest

import cli

import fake_filesys

class TestFullSequenceOfOperations(unittest.TestCase):

    def get_local_filesys(self, kind=None):
        if kind is not None and kind != 'local':
            raise NotImplementedError('Only local filesystem is supported')
        return self.local_filesys

    def utcnow(self):
        return self._utcnow

    def advance_utcnow(self, days=0, hours=0, seconds=0, microseconds=0):
        self._utcnow += datetime.timedelta(
            days=days, hours=hours, seconds=seconds, microseconds=microseconds)

    def test_everything(self):
        self.stdout = io.StringIO()
        self.local_filesys = fake_filesys.FakeFileSystem()
        self._utcnow = datetime.datetime(2014, 8, 1, 12, 30, 21, 429865)
        self.services = {
            'filesystem': self.get_local_filesys,
            'backupoperation': None,
            'backupcollection.create': None,
            'backupcollection.open': None,
            'database.create': None,
            'database.open': None,
            'logger': None,
            'uistate': None,
            'utcnow': self.utcnow,
            }
        self.advance_utcnow(seconds=1)
        self.make_first_backup()
        self.advance_utcnow(seconds=1)
        self.check_first_backup()
        self.advance_utcnow(seconds=1)
        self.sync_backups()
        self.check_synced_backups()

    def make_first_backup(self):
        self.build_initial_filesystem()
        self.advance_utcnow(seconds=1)
        self.local_filesys._allow_reading_subtree(('home', 'me'))
        self.local_filesys._allow_reading_path(
            ('etc', 'xdg', 'ebakup', 'config'))
        self.local_filesys._allow_full_access_to_subtree(('backup', 'mine'))
        self.local_filesys._allow_reading_subtree(
            ('home', 'me', '.config', 'ebakup'))
        self.local_filesys._allow_full_access_to_subtree(
            ('home', 'me', '.local', 'share', 'ebakup'))
        self.local_filesys._allow_full_access_to_subtree(
            ('home', 'me', '.cache', 'ebakup'))
        self.local_filesys._add_file(
            ('home', 'me', '.config', 'ebakup', 'config'),
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
                ''').encode('utf-8'),
            mtime=datetime.datetime(2014, 7, 14, 15, 11, 9, 912451),
            mtime_ns = 912451337,
            owner='the owner',
            group='the owning group',
            access=0o644)
        self.advance_utcnow(seconds=1)
        self.time_backup1_start = self._utcnow
        cli.cli_main(
            ('backup', '--create', 'home'),
            stdoutfile=self.stdout, services=self.services)
        self.advance_utcnow(seconds=1)
        self.assertRegex(
            self.stdout.getvalue(), r'Web ui started on port \d+\n')

    def build_initial_filesystem(self):
        basepath = ('home', 'me')
        self.local_filesys._make_files(
            basepath, ('.emacs', 'notes.txt'), fileid_first=0)
        self.local_filesys._make_files(
            basepath + ('.cache', 'chromium', 'Default', 'Cache'),
            ('index', 'data_0', 'data_1', 'f_000001', 'f_000002', 'f_000003'),
            fileid_first=10)
        self.local_filesys._make_files(
            basepath + ('Documents',), ('Letter.odt', 'Photo.jpg'),
            fileid_first=20)
        self.local_filesys._make_files(
            basepath + ('tmp',), ('scratchpad.txt', 'funny.jpg', 'Q.pdf'),
            fileid_first=30)
        self.local_filesys._make_files(
            basepath + ('Pictures', 'Christmas'),
            ('DSC_1886.JPG', 'DSC_1887.JPG', 'DSC_1888.JPG', 'DSC_1889.JPG'),
            fileid_first=40)

    def check_first_backup(self):
        self.check_backed_file(('.emacs',), self.time_backup1_start)
        self.check_backed_file(
            ('Documents','Letter.odt'), self.time_backup1_start)
        self.check_backed_file(
            ('Pictures','Christmas', 'DSC_1887.JPG'), self.time_backup1_start)

    def check_backed_file(self, path, backuptime):
        origpath = ('home', 'me') + path
        hexsha = hashlib.sha256(
            self.local_filesys._paths[origpath].data).hexdigest()
        contentpath = (
            'backup', 'mine', 'content', hexsha[:2], hexsha[2:4], hexsha[4:])
        self.assertNotEqual(
            self.local_filesys._paths[origpath],
            self.local_filesys._paths[contentpath])
        self.assertEqual(
            self.local_filesys._paths[origpath].data,
            self.local_filesys._paths[contentpath].data)

    def sync_backups(self):
        self.local_filesys._add_file(
            ('home', 'me', '.config', 'ebakup', 'config'),
            content=textwrap.dedent('''\
                backup home
                   collection local:/backup/mine
                   collection local:/backup/second
                   source local:/home/me
                       targetpath home
                       path tmp
                           ignore
                           path Q.pdf
                               static
                       path Pictures
                           static
                ''').encode('utf-8'),
            mtime=datetime.datetime(2014, 8, 1, 12, 30, 24, 157031),
            mtime_ns = 157031603,
            update=True)
        self.advance_utcnow(seconds=1)
        oldstdout = self.stdout.getvalue()
        # backup/second does not exist, so it should be created and
        # everything from backup/mine copied there. Thus, backup/mine
        # should not be modified.
        self.local_filesys._clear_all_access_rules()
        self.local_filesys._allow_reading_subtree(
            ('home', 'me', '.config', 'ebakup'))
        self.local_filesys._allow_reading_subtree(('etc', 'xdg', 'ebakup'))
        self.local_filesys._allow_full_access_to_subtree(('backup', 'second'))
        self.local_filesys._allow_reading_subtree(('backup', 'mine'))
        cli.cli_main(
            ('sync', '--create'),
            stdoutfile=self.stdout, services=self.services)
        self.advance_utcnow(seconds=1)
        self.assertTrue(self.stdout.getvalue().startswith(oldstdout))
        stdout = self.stdout.getvalue()[len(oldstdout):]
        match = re.match('^Web ui started on port \d+\n', stdout)
        if match:
            self.assertEqual(0, match.start())
            stdout = stdout[match.end():]
        self.assertEqual('', stdout)

    def check_synced_backups(self):
        self.assertCountEqual(
            ('db', 'content', 'tmp'),
            self._get_items_in_path(('backup', 'mine')))
        self.assertCountEqual(
            ('db', 'content', 'tmp'),
            self._get_items_in_path(('backup', 'second')))
        count = self._check_trees_are_equal(
            ('backup', 'mine', 'db', '2014'),
            ('backup', 'second', 'db', '2014'))
        self.assertEqual(2, count)
        count = self._check_trees_are_equal(
            ('backup', 'mine', 'content'),
            ('backup', 'second', 'content'))
        self.assertEqual(49, count) # I haven't checked this number

    def _get_items_in_path(self, basepath):
        items = []
        basepathlen = len(basepath)
        for path in self.local_filesys._paths:
            if path[:basepathlen] == basepath and len(path) == basepathlen + 1:
                items.append(path[-1])
        return items

    def _check_trees_are_equal(self, base1, base2):
        count = 0
        base1len = len(base1)
        base2len = len(base2)
        tree = self.local_filesys
        for path in tree._paths:
            if path[:base1len] == base1:
                count += 1
                item1 = tree._paths[path]
                path2 = base2 + path[base1len:]
                item2 = tree._paths.get(path2)
                self.assertNotEqual(
                    None, item2, msg='Missing file: ' + str(path2))
                self.assertEqual(
                    item1.is_directory, item2.is_directory,
                    msg='Different type: ' + str(path))
                if not item1.is_directory:
                    self.assertEqual(
                        item1.data, item2.data,
                        msg='Different data: ' + str(path))
            if path[:base2len] == base2:
                self.assertNotEqual(
                    None,
                    tree._paths.get(
                        base1 + path[base2len:]),
                    msg='Extra file: ' + str(path))
        return count
