#!/usr/bin/env python3

# Compares two backups from the same backup database. Prints the
# differences to standard output:
# '- <file>' : <file> exists in the first backup but not the second.
# '+ <file>' : <file> exists in the second backup but not the first.
# '/- <dir>' : <dir> (a directory) exists is the first backup but not
#     the second. The contents of the directory is ignored.
# '/+ <dir>' : <dir> (a directory) exists is the second backup but not
#     the first. The contents of the directory is ignored.
# '! <file> (<changes>)' : <file> exists in both backups, but has
#     differences. The original code checked lasts-modified time, size
#     and content id.

import os
import sys

sys.path.append(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pyebakup'))

import argparse
import datetime
import re

import database
import filesys

class CommandLineError(Exception): pass

def main():
    args = parse_arguments()
    tree = filesys.get_file_system('local')
    db = database.Database(tree, tree.path_from_string(args.database))
    comparer = BackupComparer(db, args.backup[0], args.backup[1])
    comparer.compare()

def parse_arguments():
    ap = argparse.ArgumentParser(
        description='Compare two backups from a single database')
    ap.add_argument('database', help='Path to the database directory')
    ap.add_argument(
        'backup', nargs=2,
        help='Backup timestamp (e.g. 2015-06-07T09:19). If there is no '
        'backup with that exact timestamp, the latest backup before the '
        'timestamp is used instead.')
    return ap.parse_args()

class BackupComparer(object):
    def __init__(self, db, bk1, bk2):
        self.db = db
        self.bk1name = bk1
        self.bk2name = bk2
        self._open_backups()

    def _open_backups(self):
        self.bk1 = self._open_one_backup(self.bk1name)
        self.bk2 = self._open_one_backup(self.bk2name)

    re_bkname = re.compile(r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$')
    def _open_one_backup(self, name):
        match = self.re_bkname.match(name)
        if not match:
            raise CommandLineError('Failed to parse backup name: ' + name)
        when = datetime.datetime(
            int(match.group(1)), int(match.group(2)), int(match.group(3)),
            int(match.group(4)), int(match.group(5)) + 1)
        bk = self.db.get_most_recent_backup_before(when)
        print('Using backup: ' + str(bk.get_start_time()))
        return bk

    def compare(self, path=()):
        dirs1, files1 = self.bk1.get_directory_listing(path)
        dirs2, files2 = self.bk2.get_directory_listing(path)
        for f in files1:
            fpath = path + (f,)
            if f not in files2:
                self._report_file_gone(fpath)
            else:
                info1 = self.bk1.get_file_info(fpath)
                info2 = self.bk2.get_file_info(fpath)
                diff = []
                if (info1.mtime != info2.mtime or
                        info1.mtime_nsec != info2.mtime_nsec):
                    assert info1.mtime.microsecond == info1.mtime_nsec // 1000
                    assert info2.mtime.microsecond == info2.mtime_nsec // 1000
                    diff.append((
                        'mtime',
                        '{}.{:09}'.format(
                            info1.mtime.replace(microsecond=0),
                            info1.mtime_nsec),
                        '{}.{:09}'.format(
                            info2.mtime.replace(microsecond=0),
                            info2.mtime_nsec)))
                if info1.size != info2.size:
                    diff.append(('size', str(info1.size), str(info2.size)))
                if info1.contentid != info2.contentid:
                    diff.append(
                        ('cid', str(info1.contentid), str(info2.contentid)))
                if diff:
                    self._report_file_changed(fpath, diff)
        for f in files2:
            fpath = path + (f,)
            if f not in files1:
                self._report_file_new(fpath)
        common_dirs = []
        for d in dirs1:
            dpath = path + (d,)
            if d not in dirs2:
                self._report_dir_gone(dpath)
            else:
                common_dirs.append(dpath)
        for d in dirs2:
            dpath = path + (d,)
            if d not in dirs1:
                self._report_dir_new(dpath)
        del dirs1
        del files1
        del dirs2
        del files2
        for dpath in common_dirs:
            self.compare(dpath)

    def _report_file_gone(self, fpath):
        print('-', fpath)

    def _report_file_new(self, fpath):
        print('+', fpath)

    def _report_file_changed(self, fpath, changes):
        print('!', fpath, changes)

    def _report_dir_gone(self, dpath):
        print('/-', dpath)

    def _report_dir_new(self, dpath):
        print('/+', dpath)

if __name__ == '__main__':
    main()
