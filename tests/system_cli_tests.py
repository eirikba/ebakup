#!/usr/bin/env python3

import datetime
import io
import hashlib
import textwrap
import unittest

import cli

import fake_filesys

class TestFullSequenceOfOperations(unittest.TestCase):

    def get_local_filesys(self, kind=None):
        if kind is not None and kind != 'local':
            raise NotImplementedError('Only local accessors supported')
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
        self.factories = {
            'filesystem': self.get_local_filesys,
            'backupoperation': None,
            'backupcollection': None,
            'database.create': None,
            'database.open': None,
            'utcnow': self.utcnow,
            }
        self.advance_utcnow(seconds=1)
        self.make_first_backup()
        self.advance_utcnow(seconds=1)
        self.check_first_backup()
        self.advance_utcnow(seconds=1)

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
            mtime_ns = 912451337)
        self.advance_utcnow(seconds=1)
        self.time_backup1_start = self._utcnow
        cli.main(
            ('backup', '--create', 'home'),
            stdoutfile=self.stdout, factories=self.factories)
        self.advance_utcnow(seconds=1)
        self.assertEqual('', self.stdout.getvalue())

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
        self.assertIn(
            self.shadowroot(self.time_backup1_start) +
            ('home', 'Documents', 'Photo.jpg'),
            self.local_filesys._paths)
        self.assertNotIn(
            self.shadowroot(self.time_backup1_start) +
            ('home', 'tmp', 'scratchpad.txt'),
            self.local_filesys._paths)

    def shadowroot(self, backuptime):
        return (
            'backup', 'mine',
            str(backuptime.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                backuptime.month, backuptime.day,
                backuptime.hour, backuptime.minute))

    def check_backed_file(self, path, backuptime):
        shadowpath = self.shadowroot(backuptime) + ('home',) + path
        origpath = ('home', 'me') + path
        self.assertNotEqual(
            self.local_filesys._paths[origpath],
            self.local_filesys._paths[shadowpath])
        self.assertEqual(
            self.local_filesys._paths[origpath].data,
            self.local_filesys._paths[shadowpath].data)
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
        self.assertEqual(
            self.local_filesys._paths[shadowpath],
            self.local_filesys._paths[contentpath])
