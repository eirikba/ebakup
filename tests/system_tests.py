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
            services=self.services,
            stdoutfile=out)
        self.advance_utcnow(seconds=20)
        self.assertRegex(out.getvalue(), r'Web ui started on port \d+\n')
        self._check_result_of_initial_backup()
        self._check_info_after_initial_backup()
        self.advance_utcnow(days=4, seconds=2000)
        self._update_sources_before_second_backup()
        self.advance_utcnow(seconds=600)
        cli.main(('backup', 'home'), services=self.services, stdoutfile=out)
        self.advance_utcnow(seconds=80)
        self._check_result_of_second_backup(stdout=out.getvalue())
        self.advance_utcnow(seconds=32)
        self.assertRaisesRegex(
            FileNotFoundError, 'Backup collection does not exist.*second',
            cli.main, ('sync',), services=self.services, stdoutfile=out)
        self.advance_utcnow(seconds=17)
        out = io.StringIO()
        cli.main(('sync', '--create'), services=self.services, stdoutfile=out)
        self._check_result_of_sync(stdout=out.getvalue())
        self.advance_utcnow(seconds=27)
        out = io.StringIO()
        cli.main(('sync', '--create'), services=self.services, stdoutfile=out)
        # This should not have caused any changes, so just do the same check
        self._check_result_of_sync(stdout=out.getvalue())

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
        cli.main(('info',), services=self.services, stdoutfile=out)
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
        self.assertRegex(firstline, r'Web ui started on port \d+')

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
            '5e16a40318f071df23a3d2fb600f7943764bca4896020ba9a54a00c10f49e99e',
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
        self.assertCountEqual(('.config', 'My Pictures'), dirs)
        self.assertCountEqual(('notes.txt',), files)
        dirs, files = bkup.list_directory(('My Pictures',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('DSC_1886.JPG', 'DSC_1903.JPG'), files)
        info = bkup.get_file_info(('notes.txt',))
        self.assertEqual(
            datetime.datetime(1994, 11, 28, 16, 48, 56, 394323), info.mtime)
        self.assertEqual(394323854, info.mtime_nsec)
        self.assertEqual(17, info.size)
        self.assertEqual(
            b"?27\x9f\xe4\x10\x8e\x99'\x98\x90\xfa"
            b"\xf4J\x83lj\xd86\xad3\x1e\xa2v\xd0\xa4\xb7\x85\x847\t\x1a",
            info.good_checksum)
        info = bkup.get_file_info(('My Pictures', 'DSC_1903.JPG'))
        self.assertEqual(
            datetime.datetime(1994, 4, 5, 2, 36, 23, 34763), info.mtime)
        self.assertEqual(34763519, info.mtime_nsec)
        self.assertEqual(17, info.size)
        self.assertEqual(
            b'8M\x1c\xd1\xec\xf7\xcb\xd6\xdf\xbd\x82\x89'
            b'J\xf0\x92+\xc1\x13X\x9d\x14\xd0lF^\xb1E\x92*\xe0\r\xd7',
            info.good_checksum)
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
        self.assertEqual(
            b'8M\x1c\xd1\xec\xf7\xcb\xd6\xdf\xbd\x82\x89'
            b'J\xf0\x92+\xc1\x13X\x9d\x14\xd0lF^\xb1E\x92*\xe0\r\xd7',
            contentinfo.lastsum)
        timeline = contentinfo.timeline
        self.assertEqual(1, len(timeline))
        self.assertEqual(True, timeline[0].restored)
        self.assertEqual(
            datetime.datetime(1995, 1, 1, 0, 0, 20), timeline[0].first)
        self.assertEqual(
            datetime.datetime(1995, 1, 1, 0, 0, 20), timeline[0].last)

    def _check_info_after_initial_backup(self):
        out = io.StringIO()
        cli.main(('info',), services=self.services, stdoutfile=out)
        outstr = out.getvalue()
        if outstr.startswith('Web ui started'):
            firstline, outstr = outstr.split('\n', 1)
        self.assertEqual(
            'Backup definitions:\n  backup home\n'
            '    collection local:/backups/home\n'
            '      Least recently verified: 1995-01-01 00:00:20\n'
            '      Total number of content files: 4\n'
            '    collection local:/backups/second\n'
            '      (Does not exist)\n'
            '    source local:/home/me\n',
            outstr)
        self.assertRegex(firstline, r'Web ui started on port \d+')

    def _update_sources_before_second_backup(self):
        fs = self.fs
        # Invalid utf-8 in file name
        invalid_unicode = b'invalid utf-8 \xbd <--'.decode(
            'utf-8', errors='surrogateescape')
        fs._add_file(
            ('home', 'me', 'other', invalid_unicode),
            content=b'File with invalid utf-8 in its file name',
            mtime=datetime.datetime(1995, 1, 2, 7, 48, 24),
            mtime_ns=769151409)
        fs._add_file(
            ('home', 'me', 'notes.txt'),
            content=b'Some quick notes\nWith some extra stuff added\n',
            mtime=datetime.datetime(1995, 1, 3, 11, 2, 28),
            mtime_ns=149212583,
            update=True)
        fs._add_file(
            ('home', 'me', 'rootnotes.txt'),
            content=b'Notes made by root\n',
            mtime=datetime.datetime(1995, 1, 2, 18, 22, 40),
            mtime_ns=628691057,
            perms='')
        fs._add_directory(
            ('home', 'me', 'rootdata'),
            perms='')
        fs._add_file(
            ('home', 'me', 'unstable'),
            content=b'initial content',
            mtime=datetime.datetime(1995, 1, 4, 21, 49, 29),
            mtime_ns=743283546)

    def _check_result_of_second_backup(self, stdout):
        output = stdout
        first, second, rest = output.split('\n', 2)
        self.assertRegex(first, r'Web ui started on port \d+')
        self.assertRegex(second, r'Web ui started on port \d+')
        self.assertEqual(
            '1995-01-05 00:44:00 ERROR: Permission denied to source file '
            "(('home', 'me', 'rootnotes.txt'))\n"
            '1995-01-05 00:44:00 ERROR: File not backed up '
            "(('home', 'me', 'rootnotes.txt'))\n"
            '1995-01-05 00:44:00 ERROR: Failed to descend source directory '
            "(('home', 'me', 'rootdata'))" ' - No "r" permission for '
            "('home', 'me', 'rootdata')\n",
            rest)
        coll = backupcollection.open_collection(self.fs, ('backups', 'home'))
        bkup = coll.get_most_recent_backup()
        self.assertEqual(
            datetime.datetime(1995, 1, 5, 0, 44, 0), bkup.get_start_time())
        self.assertEqual(
            datetime.datetime(1995, 1, 5, 0, 44, 0), bkup.get_end_time())

    def _check_result_of_sync(self, stdout):
        self.assertRegex(stdout, r'Web ui started on port \d{4}\n')
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
        self.assertEqual(27, count)
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
                    self.assertEqual(789266769, item.last)
                    cids2.append(item.cid)
        self.assertCountEqual(cids1, cids2)
