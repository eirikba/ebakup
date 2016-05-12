#!/usr/bin/env python3

import database.datafile as datafile

from .backupinfo import BackupInfo
from .backupinfobuilder import BackupInfoBuilder
from .contentdb import ContentInfoFile

class DBFileOpener(object):

    @staticmethod
    def open_main(tree, path):
        return datafile.open_main(tree, path)

    @staticmethod
    def create_backup(db, when):
        return BackupInfoBuilder(db, when)

    @staticmethod
    def open_content_file(db):
        return ContentInfoFile(db)

    @staticmethod
    def create_backup_in_replacement_mode(tree, path, start):
        return datafile.create_backup_in_replacement_mode(
            tree, path, start)

    @staticmethod
    def open_raw_backup(tree, path, name):
        return datafile.open_backup_by_name(tree, path, name)

    @staticmethod
    def open_backup(db, name):
        return BackupInfo(db, name)
