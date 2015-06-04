#!/usr/bin/env python3

import filesys

class InfoTask(object):

    def __init__(self, config, args):
        self._config = config
        self._args = args
        self._logger = self._args.logger

    def execute(self):
        backup_names = self._config.get_all_backup_names()
        self._print('Backup definitions:')
        if not backup_names:
            self._print('  No backups defined')
        for name in backup_names:
            self._print_backup_info(name)

    def _print_backup_info(self, name):
        bkconfig = self._config.get_backup_by_name(name)
        self._print('  backup ' + name)
        for collection in bkconfig.collections:
            tree = filesys.get_file_system(collection.accessor)
            self._print(
                '    collection ' + tree.path_to_full_string(collection.path))
        for source in bkconfig.sources:
            tree = filesys.get_file_system(source.accessor)
            self._print('    source ' + tree.path_to_full_string(source.path))

    def _print(self, msg):
        self._logger.print(msg)
