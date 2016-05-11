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
import datafile
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
        self.services = {
            'utcnow': self.utcnow,
            'filesystem': self.get_filesys,
            '*': None,
            }
        fs = self.fs
        fs._set_utcnow(self.utcnow)
        self.make_initial_source_tree(fs)
        fs._allow_full_access_to_subtree(('backups', 'home'))
        fs._allow_full_access_to_subtree(('backups', 'second'))
        fs._allow_reading_subtree(('home', 'me'))
        fs._disallow_reading_subtree(('home', 'me', 'tmp'))
        fs._disallow_reading_subtree(('home', 'me', '.cache'))
        fs._disallow_reading_subtree(('home', 'me', 'tmp-data.txt'))
        fs._disallow_reading_subtree(('home', 'me', 'mount-fileserver'))
        fs._allow_reading_path(('etc', 'xdg', 'ebakup', 'config'))
        fs._add_file(
            ('home', 'me', '.config', 'ebakup', 'config'),
            content=self.get_initial_config_file_content(),
            mtime=datetime.datetime(1994, 12, 31, 23, 58),
            owner='fileowner', group='fileownergroup', access=0o644)
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
        cli.cli_main(
            ('backup', '--create', 'home'),
            services=self.services,
            stdoutfile=out)
        self.advance_utcnow(seconds=20)
        self.assertRegex(out.getvalue(), r'^Web ui started on port \d+$')
        self._check_result_of_initial_backup()
        self._check_info_after_initial_backup()
        self.advance_utcnow(days=4, seconds=2000)
        self._update_sources_before_second_backup()
        self.advance_utcnow(seconds=600)
        cli.cli_main(('backup', 'home'), services=self.services, stdoutfile=out)
        self.advance_utcnow(seconds=80)
        self._check_result_of_second_backup(stdout=out.getvalue())
        self.advance_utcnow(seconds=32)
        self.assertRaisesRegex(
            FileNotFoundError, 'Backup collection does not exist.*second',
            cli.cli_main, ('sync',), services=self.services, stdoutfile=out)
        self.advance_utcnow(seconds=17)
        out = io.StringIO()
        cli.cli_main(
            ('sync', '--create'), services=self.services, stdoutfile=out)
        self._check_result_of_sync(stdout=out.getvalue())
        self.advance_utcnow(seconds=27)
        out = io.StringIO()
        cli.cli_main(
            ('sync', '--create'), services=self.services, stdoutfile=out)
        # This should not have caused any changes, so just do the same check
        self._check_result_of_sync(stdout=out.getvalue())
        out = io.StringIO()
        fs._allow_full_access_to_subtree(('backup', 'shadow'))
        cli.cli_main(
            ('shadowcopy', '--target', '/backup/shadow', '1995-01-01T00:00'),
            services=self.services, stdoutfile=out)
        fs._drop_all_access_to_subtree(('backup', 'shadow'))
        self.assertRegex(out.getvalue(), r'^Web ui started on port \d+$')

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
            mtime_ns=394323854,
            owner='me',
            group='me',
            access=0o644)
        fs._add_directory(
            ('home', 'me', 'My Pictures'),
            owner='me',
            group='me',
            access=0o755)
        fs._add_file(
            ('home', 'me', 'My Pictures', 'DSC_1886.JPG'),
            content=b'A photo!',
            mtime=datetime.datetime(1994, 1, 17, 0, 12, 0),
            mtime_ns=748391204,
            owner='me',
            group='me',
            access=0o644)
        fs._add_file(
            ('home', 'me', 'My Pictures', 'DSC_1903.JPG'),
            content=b'A different photo',
            mtime=datetime.datetime(1994, 4, 5, 2, 36, 23),
            mtime_ns=34763519,
            owner='me',
            group='me',
            access=0o644)
        fs._add_directory(
            ('home', 'me', 'system'),
            owner='root',
            group='root',
            access=0o755)
        fs._add_file(
            ('home', 'me', 'system', 'notes.txt'),
            content=b'Some notes by root',
            mtime=datetime.datetime(1994, 6, 2, 21, 36, 54),
            mtime_ns=419844710,
            owner='root',
            group='root',
            access=0o644)
        fs._add_file(
            ('home', 'me', 'socket'),
            filetype='socket',
            mtime=datetime.datetime(1994, 2, 20, 3, 59, 16),
            mtime_ns=60446634,
            owner='me',
            group='me',
            access=0o644)
        fs._add_file(
            ('home', 'me', 'runme'),
            content=b'An executable!',
            mtime=datetime.datetime(1994, 5, 29, 9, 28, 58),
            mtime_ns=751700129,
            owner='me',
            group='me',
            access=0o755)
        fs._add_symlink(
            ('home', 'me', 'symlink'),
            target=b'system/notes.txt',
            mtime=datetime.datetime(1994, 6, 15, 14, 2, 47),
            mtime_ns=145225216,
            owner='me',
            group='me',
            access=0o644)


    def get_initial_config_file_content(self):
        return textwrap.dedent('''\
            backup home
                collection local:/backups/home
                collection local:/backups/second
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
        cli.cli_main(('info',), services=self.services, stdoutfile=out)
        outstr = out.getvalue()
        if outstr.startswith('Web ui started'):
            firstline, outstr = outstr.split('\n', 1)
        self.assertEqual(
            'Backup definitions:\n'
            '  backup home\n'
            '    collection local:/backups/home\n'
            '      (Does not exist)\n'
            '    collection local:/backups/second\n'
            '      (Does not exist)\n'
            '    source local:/home/me\n',
            outstr)
        self.assertRegex(firstline, r'^Web ui started on port \d+$')

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
        contentfiles = tuple(
            x[3:] for x in fs._paths if x[:3] == ('backups', 'home', 'content'))
        expected = set()
        expected.add(())
        for cid in (
            '3f32379fe4108e99279890faf44a836c6ad836ad331ea276d0a4b7858437091a',
            '9a56e724abdbeafb3c206603f085d887323695e4cbfabedbb25597d5d43012e0',
            '384d1cd1ecf7cbd6dfbd82894af0922bc113589d14d06c465eb145922ae00dd7',
            '5e16a40318f071df23a3d2fb600f7943764bca4896020ba9a54a00c10f49e99e',
            'f4cefbc7f79d42224e638b1f1c9f26ec5e463d4258941ffb0159c2a97b846dc8',
            '49a95841301c58087a0b7b31bbed2d4bee95dda74a58a513457b7cd262f8759a',
            '9266be2ad9656a674b89ee113851aa31773a8c5414e6329fb9e605ea3d9415ac',
            ):
            expected.add((cid[:2],))
            expected.add((cid[:2],cid[2:4]))
            expected.add((cid[:2],cid[2:4],cid[4:]))
        self.maxDiff = None
        self.assertCountEqual(expected, contentfiles)

    def _check_db_after_initial_backup(self):
        coll = backupcollection.open_collection(
            self.fs, ('backups', 'home'), services=self.services)
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
        self.assertCountEqual(('.config', 'My Pictures', 'system'), dirs)
        self.assertCountEqual(
            ('notes.txt', 'socket', 'runme', 'symlink'), files)
        dirs, files = bkup.list_directory(('My Pictures',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('DSC_1886.JPG', 'DSC_1903.JPG'), files)
        dirs, files = bkup.list_directory(('system',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('notes.txt',), files)
        info = bkup.get_dir_info(('My Pictures',))
        self.assertEqual(
            {'owner': 'me', 'group': 'me', 'unix-access': 0o755},
            info.extra_data)
        info = bkup.get_dir_info(('system',))
        self.assertEqual(
            {'owner': 'root', 'group': 'root', 'unix-access': 0o755},
            info.extra_data)
        info = bkup.get_file_info(('notes.txt',))
        self.assertEqual(
            datetime.datetime(1994, 11, 28, 16, 48, 56, 394323), info.mtime)
        self.assertEqual(394323854, info.mtime_nsec)
        self.assertEqual(17, info.size)
        self.assertEqual(
            b"?27\x9f\xe4\x10\x8e\x99'\x98\x90\xfa"
            b"\xf4J\x83lj\xd86\xad3\x1e\xa2v\xd0\xa4\xb7\x85\x847\t\x1a",
            info.good_checksum)
        self.assertEqual('file', info.filetype)
        self.assertEqual(
            {'owner': 'me', 'group': 'me', 'unix-access': 0o644},
            info.extra_data)
        info = bkup.get_file_info(('My Pictures', 'DSC_1903.JPG'))
        self.assertEqual(
            datetime.datetime(1994, 4, 5, 2, 36, 23, 34763), info.mtime)
        self.assertEqual(34763519, info.mtime_nsec)
        self.assertEqual(17, info.size)
        self.assertEqual(
            b'8M\x1c\xd1\xec\xf7\xcb\xd6\xdf\xbd\x82\x89'
            b'J\xf0\x92+\xc1\x13X\x9d\x14\xd0lF^\xb1E\x92*\xe0\r\xd7',
            info.good_checksum)
        self.assertEqual('file', info.filetype)
        self.assertEqual(
            {'owner': 'me', 'group': 'me', 'unix-access': 0o644},
            info.extra_data)
        cid = info.contentid
        reader = coll.get_content_reader(cid)
        self.assertEqual(17, reader.get_size())
        self.assertEqual(b'A different photo', reader.get_data_slice(0,100))
        self.assertEqual(b'A different photo', reader.get_data_slice(0,17))
        self.assertEqual(b'A different pho', reader.get_data_slice(0,15))
        self.assertEqual(b'feren', reader.get_data_slice(5,10))
        contentinfo = coll.get_content_info(cid)
        self.assertEqual(
            b'8M\x1c\xd1\xec\xf7\xcb\xd6\xdf\xbd\x82\x89'
            b'J\xf0\x92+\xc1\x13X\x9d\x14\xd0lF^\xb1E\x92*\xe0\r\xd7',
            contentinfo.goodsum)
        info = bkup.get_file_info(('system', 'notes.txt'))
        self.assertEqual(
            datetime.datetime(1994, 6, 2, 21, 36, 54, 419844), info.mtime)
        self.assertEqual(419844710, info.mtime_nsec)
        self.assertEqual(18, info.size)
        self.assertEqual(
            b'\xf4\xce\xfb\xc7\xf7\x9dB"Nc\x8b\x1f\x1c\x9f&'
            b'\xec^F=BX\x94\x1f\xfb\x01Y\xc2\xa9{\x84m\xc8',
            info.good_checksum)
        self.assertEqual('file', info.filetype)
        self.assertEqual(
            {'owner': 'root', 'group': 'root', 'unix-access': 0o644},
            info.extra_data)
        info = bkup.get_file_info(('socket',))
        self.assertEqual(
            datetime.datetime(1994, 2, 20, 3, 59, 16, 60446), info.mtime)
        self.assertEqual(60446634, info.mtime_nsec)
        self.assertEqual(0, info.size)
        self.assertEqual(b'', info.good_checksum)
        self.assertEqual('socket', info.filetype)
        self.assertEqual(
            {'owner': 'me', 'group': 'me', 'unix-access': 0o644},
            info.extra_data)
        info = bkup.get_file_info(('runme',))
        self.assertEqual(
            datetime.datetime(1994, 5, 29, 9, 28, 58, 751700), info.mtime)
        self.assertEqual(751700129, info.mtime_nsec)
        self.assertEqual(14, info.size)
        self.assertEqual(
            b'I\xa9XA0\x1cX\x08z\x0b{1\xbb\xed-K\xee\x95\xdd\xa7J'
            b'X\xa5\x13E{|\xd2b\xf8u\x9a',
            info.good_checksum)
        self.assertEqual('file', info.filetype)
        self.assertEqual(
            {'owner': 'me', 'group': 'me', 'unix-access': 0o755},
            info.extra_data)
        info = bkup.get_file_info(('symlink',))
        self.assertEqual(
            datetime.datetime(1994, 6, 15, 14, 2, 47, 145225), info.mtime)
        self.assertEqual(145225216, info.mtime_nsec)
        self.assertEqual(16, info.size)
        self.assertEqual('symlink', info.filetype)
        self.assertEqual(
            {'owner': 'me', 'group': 'me', 'unix-access': 0o644},
            info.extra_data)

    def _check_info_after_initial_backup(self):
        out = io.StringIO()
        cli.cli_main(('info',), services=self.services, stdoutfile=out)
        outstr = out.getvalue()
        if outstr.startswith('Web ui started'):
            firstline, outstr = outstr.split('\n', 1)
        if False: # FIXME: last-verified data is currently broken
         self.assertEqual(
            'Backup definitions:\n  backup home\n'
            '    collection local:/backups/home\n'
            '      Least recently verified: 1995-01-01 00:00:20\n'
            '      Total number of content files: 7\n'
            '    collection local:/backups/second\n'
            '      (Does not exist)\n'
            '    source local:/home/me\n',
            outstr)
        self.assertRegex(firstline, r'^Web ui started on port \d+$')

    def _update_sources_before_second_backup(self):
        fs = self.fs
        # Invalid utf-8 in file name
        self.invalid_unicode = b'invalid utf-8 \xbd <--'.decode(
            'utf-8', errors='surrogateescape')
        fs._add_file(
            ('home', 'me', 'other', self.invalid_unicode),
            content=b'File with invalid utf-8 in its file name',
            mtime=datetime.datetime(1995, 1, 2, 7, 48, 24),
            mtime_ns=769151409,
            owner='other',
            group='me',
            access=0o640)
        fs._add_file(
            ('home', 'me', 'notes.txt'),
            content=b'Some quick notes\nWith some extra stuff added\n',
            mtime=datetime.datetime(1995, 1, 3, 11, 2, 28),
            mtime_ns=149212583,
            owner='me',
            group='me',
            access=0o644,
            update=True)
        fs._add_file(
            ('home', 'me', 'rootnotes.txt'),
            content=b'Notes made by root\n',
            mtime=datetime.datetime(1995, 1, 2, 18, 22, 40),
            mtime_ns=628691057,
            owner='root',
            group='root',
            access=0o644,
            perms='')
        fs._add_directory(
            ('home', 'me', 'rootdata'),
            perms='')
        fs._add_file(
            ('home', 'me', 'unstable'),
            content=b'initial content',
            mtime=datetime.datetime(1995, 1, 4, 21, 49, 29),
            mtime_ns=743283546,
            owner='me',
            group='me',
            access=0o644)

    def _check_result_of_second_backup(self, stdout):
        self._check_output_of_second_backup(stdout)
        self._check_db_after_second_backup()

    def _check_output_of_second_backup(self, stdout):
        output = stdout
        first, second, rest = output.split('\n', 2)
        self.assertRegex(first, r'^Web ui started on port \d+$')
        self.assertRegex(second, r'^Web ui started on port \d+$')
        self.assertEqual(
            '1995-01-05 00:44:00 ERROR: Permission denied to source file '
            "(('home', 'me', 'rootnotes.txt'))\n"
            '1995-01-05 00:44:00 ERROR: File not backed up '
            "(('home', 'me', 'rootnotes.txt'))\n"
            '1995-01-05 00:44:00 ERROR: Failed to descend source directory '
            "(('home', 'me', 'rootdata'))" ' - No "r" permission for '
            "('home', 'me', 'rootdata')\n",
            rest)

    def _check_db_after_second_backup(self):
        coll = backupcollection.open_collection(self.fs, ('backups', 'home'))
        bkup = coll.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(1995, 1, 5, 0, 44, 0), bkup.get_start_time())
        self.assertEqual(
            datetime.datetime(1995, 1, 5, 0, 44, 0), bkup.get_end_time())
        info = bkup.get_file_info(('other', self.invalid_unicode))
        self.assertEqual(
            datetime.datetime(1995, 1, 2, 7, 48, 24, 769151), info.mtime)
        self.assertEqual(40, info.size)
        self.assertEqual(
            b'\x92\xc7\xa7|(v2T\x9dmN9 (\x93\xe5\xe7\x7f\x823'
            b'Er\xc9:M\xcfZR\xa1b\x08\xf6',
            info.good_checksum)
        self.assertEqual('file', info.filetype)
        self.assertEqual(
            {'owner': 'other', 'group': 'me', 'unix-access': 0o640},
            info.extra_data)

    def _check_result_of_sync(self, stdout):
        self.assertRegex(stdout, r'^Web ui started on port \d{4}$')
        count = 0
        for path in self.fs._paths:
            if len(path) < 3:
                continue
            if (path[:2] == ('backups', 'home') and
                    path[2] not in ('1995', 'tmp')):
                if path == ('backups', 'home', 'db', 'content'):
                    continue
                count += 1
                item1 = self.fs._paths[path]
                item2 = self.fs._paths[('backups', 'second') + path[2:]]
                self.assertEqual(item1.is_directory, item2.is_directory)
                if not item1.is_directory:
                    self.assertEqual(item1.data, item2.data, path[2:])
        self.assertEqual(35, count)
        cids1 = []
        with datafile.open_content(self.fs, ('backups', 'home', 'db')) as cf:
            for item in cf:
                if item.kind == 'content':
                    cids1.append(item.cid)
        cids2 = []
        with datafile.open_content(self.fs, ('backups', 'second', 'db')) as cf:
            for item in cf:
                if item.kind == 'content':
                    self.assertEqual(789266769, item.first)
                    cids2.append(item.cid)
        self.assertCountEqual(cids1, cids2)
