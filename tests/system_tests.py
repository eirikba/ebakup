#!/usr/bin/env python3

# These tests are testing the whole stack, except for the actual
# interactions with the system. Most importantly, that means the file
# system and utcnow are replaced by test doubles.

import hashlib

import datetime
import io
import textwrap
import unittest

import backupcollection
import cli
import fake_filesys

class TestFullSequence(unittest.TestCase):

    def setUp(self):
        self._utcnow_dt = datetime.datetime(1995, 1, 1)

    def advance_utcnow(self, days=0, seconds=0):
        self._utcnow_dt += datetime.timedelta(days=days, seconds=seconds)

    def utcnow(self):
        return self._utcnow_dt

    def get_filesys(self, which):
        if which == 'local':
            return self.fs
        raise NotImplementedError('Unknown file system: ' + str(which))

    def test_all(self):
        self.fs = fake_filesys.FakeFileSystem()
        self.factories = {
            'utcnow': self.utcnow,
            'filesystem': self.get_filesys,
            '*': None,
            }
        fs = self.fs
        fs._set_utcnow(self.utcnow)
        self.make_initial_source_tree(fs)
        fs._allow_full_access_to_subtree(('backups', 'home'))
        fs._allow_reading_subtree(('home', 'me'))
        fs._disallow_reading_subtree(('home', 'me', 'tmp'))
        fs._disallow_reading_subtree(('home', 'me', '.cache'))
        fs._disallow_reading_subtree(('home', 'me', 'tmp-data.txt'))
        fs._disallow_reading_subtree(('home', 'me', 'mount-fileserver'))
        fs._allow_reading_path(('etc', 'xdg', 'ebakup', 'config'))
        fs._add_file(
            ('home', 'me', '.config', 'ebakup', 'config'),
            content=self.get_initial_config_file_content(),
            mtime=datetime.datetime(1994, 12, 31, 23, 58))
        self._check_info_before_first_backup()
        self.assertRaisesRegex(
            fake_filesys.ForbiddenActionError,
            "No listdir access.*'me', 'tmp'\)",
            self.fs.get_directory_listing, ('home', 'me', 'tmp'))
        self.assertRaisesRegex(
            fake_filesys.ForbiddenActionError,
            "No listdir access.*'squiggle'\)",
            self.fs.get_directory_listing, ('home', 'me', '.cache', 'squiggle'))
        self.assertRaisesRegex(
            fake_filesys.ForbiddenActionError,
            "No stat access.*'tmp-data.txt'\)",
            self.fs.get_item_at_path, ('home', 'me', 'tmp-data.txt'))
        self.assertRaisesRegex(
            fake_filesys.ForbiddenActionError,
            "No stat access.*'nosuchfile'\)",
            self.fs.get_item_at_path, ('home', 'me', 'tmp', 'nosuchfile'))
        out = io.StringIO()
        self.advance_utcnow(seconds=20)
        cli.main(
            ('backup', '--create', 'home'),
            factories=self.factories,
            stdoutfile=out)
        self.advance_utcnow(seconds=20)
        self.assertEqual('', out.getvalue())
        self._check_result_of_initial_backup()
        self._check_info_after_initial_backup()

    def make_initial_source_tree(self, fs):
        fs._make_files(
            ('home', 'me', 'tmp'), ('t.txt', 'info', 'experiment.py'),
            filetype='noinfo')
        fs._make_files(('home', 'me'), ('tmp-data.txt',), filetype='noinfo')
        fs._make_files(
            ('home', 'me', '.cache', 'squiqqle'),
            ('aaaaaaaa', 'aaaaaaab', 'aaaaaaac', 'aaaaaaad'),
            filetype='noinfo')
        fs._make_files(
            ('home', 'me', 'mount-fileserver'),
            ('movie.mpg', 'movie2.avi'),
            filetype='noinfo')
        fs._add_file(
            ('home', 'me', 'notes.txt'),
            content=b'Some quick notes\n',
            mtime=datetime.datetime(1994, 11, 28, 16, 48, 56),
            mtime_ns=394323854)
        fs._add_file(
            ('home', 'me', 'My Pictures', 'DSC_1886.JPG'),
            content=b'A photo!',
            mtime=datetime.datetime(1994, 1, 17, 0, 12, 0),
            mtime_ns=748391204)
        fs._add_file(
            ('home', 'me', 'My Pictures', 'DSC_1903.JPG'),
            content=b'A different photo',
            mtime=datetime.datetime(1994, 4, 5, 2, 36, 23),
            mtime_ns=34763519)

    def get_initial_config_file_content(self):
        return textwrap.dedent('''\
            backup home
                collection local:/backups/home
                source local:/home/me
                   paths .cache .thumbnails tmp
                       ignore
                   path My Pictures
                       static
                   path-globs tmp-* mount-*
                       ignore
            ''').encode('utf-8')

    def _check_info_before_first_backup(self):
        out = io.StringIO()
        cli.main(('info',), factories=self.factories, stdoutfile=out)
        self.assertEqual(
            'Backup definitions:\n'
            '  backup home\n'
            '    collection local:/backups/home\n'
            '    source local:/home/me\n',
            out.getvalue())

    def _check_result_of_initial_backup(self):
        self._check_filesys_after_initial_backup()
        self._check_db_after_initial_backup()

    def _check_filesys_after_initial_backup(self):
        fs = self.fs
        self.assertIn(('backups', 'home', 'db'), fs._paths)
        self.assertIn(('backups', 'home', 'db', '1995'), fs._paths)
        self.assertIn(
            ('backups', 'home', 'db', '1995', '01-01T00:00'), fs._paths)
        self.assertFalse(
            fs._paths[('backups', 'home', 'db', '1995', '01-01T00:00')]
            .is_directory)
        self.assertIn(('backups', 'home', '1995'), fs._paths)
        self.assertIn(('backups', 'home', '1995', '01-01T00:00'), fs._paths)
        self.assertTrue(
            fs._paths[('backups', 'home', '1995', '01-01T00:00')].is_directory)
        self.assertIn(
            ('backups', 'home', '1995', '01-01T00:00', 'My Pictures',
             'DSC_1903.JPG'),
            fs._paths)
        self.assertNotIn(
            ('backups', 'home', '1995', '01-01T00:00', 'tmp-data.txt'),
            fs._paths)
        self.assertNotIn(
            ('backups', 'home', '1995', '01-01T00:00', 'tmp', 'info'),
            fs._paths)
        self.assertEqual(
            fs._paths[('home', 'me', 'notes.txt')].data,
            fs._paths[('backups', 'home', '1995', '01-01T00:00', 'notes.txt')]
            .data)
        self.assertNotEqual(
            fs._paths[('home', 'me', 'notes.txt')],
            fs._paths[('backups', 'home', '1995', '01-01T00:00', 'notes.txt')])
        self.assertEqual(
            fs._paths[('backups', 'home', '1995', '01-01T00:00', 'notes.txt')],
            fs._paths[
                ('backups', 'home', 'content',
                 '3f', '32', '379fe4108e99279890faf4'
                 '4a836c6ad836ad331ea276d0a4b7858437091a')])
        contentfiles = tuple(
            x[3:] for x in fs._paths if x[:3] == ('backups', 'home', 'content'))
        expected = set()
        expected.add(())
        for cid in (
            '3f32379fe4108e99279890faf44a836c6ad836ad331ea276d0a4b7858437091a',
            '9a56e724abdbeafb3c206603f085d887323695e4cbfabedbb25597d5d43012e0',
            '384d1cd1ecf7cbd6dfbd82894af0922bc113589d14d06c465eb145922ae00dd7',
            '9261f035a0e43e30550a73086daef1fb0bed7f6f7ee5f01b8c4862ae91c0de95',
            ):
            expected.add((cid[:2],))
            expected.add((cid[:2],cid[2:4]))
            expected.add((cid[:2],cid[2:4],cid[4:]))
        self.maxDiff = None
        self.assertCountEqual(expected, contentfiles)

    def _check_db_after_initial_backup(self):
        coll = (
            backupcollection
            .BackupCollectionFactory(self.fs, ('backups', 'home'))
            .open_collection())
        bkup = coll.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(1995, 1, 1, 0, 0, 20), bkup.get_start_time())
        self.assertEqual(
            datetime.datetime(1995, 1, 1, 0, 0, 20), bkup.get_end_time())
        self.assertEqual(
            None,
            coll.get_most_recent_backup_before(
                datetime.datetime(1995, 1, 1, 0, 0, 20)))
        self.assertEqual(
            bkup.get_start_time(), coll.get_oldest_backup().get_start_time())
        self.assertEqual(
            None,
            coll.get_oldest_backup_after(
                datetime.datetime(1995, 1, 1, 0, 0, 20)))
        dirs, files = bkup.list_directory(())
        self.assertCountEqual(('.config', 'My Pictures'), dirs)
        self.assertCountEqual(('notes.txt',), files)
        dirs, files = bkup.list_directory(('My Pictures',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('DSC_1886.JPG', 'DSC_1903.JPG'), files)

    def _check_info_after_initial_backup(self):
        out = io.StringIO()
        cli.main(('info',), factories=self.factories, stdoutfile=out)
        self.assertEqual(
            'Backup definitions:\n  backup home\n'
            '    collection local:/backups/home\n    source local:/home/me\n',
            out.getvalue())
