#!/usr/bin/env python3

import filesys

class InfoTask(object):

    def __init__(self, config, args):
        self._config = config
        self._args = args
        self._factories = args.factories
        self._getfilesys = self._factories.get(
            'filesystem', filesys.get_file_system)
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
            self._print_collection_info('    ', collection)
        for source in bkconfig.sources:
            tree = self._getfilesys(source.accessor)
            self._print('    source ' + tree.path_to_full_string(source.path))

    def _print_collection_info(self, prefix, collcfg):
            tree = self._getfilesys(collcfg.accessor)
            self._print(
                prefix + 'collection ' + tree.path_to_full_string(collcfg.path))
            collfact = self._factories['backupcollection'](
                tree, collcfg.path, factories=self._factories)
            try:
                coll = collfact.open_collection()
            except FileNotFoundError:
                self._print(prefix + '  (Does not exist)')
                coll = None
            if coll:
                colldata = CollectionSummarizer(coll)
                self._print(
                    prefix + '  Least recently verified: ' +
                    str(colldata.least_recently_verified_timestamp))

    def _print(self, msg):
        self._logger.print(msg)

class CollectionSummarizer(object):
    def __init__(self, collection):
        self.collection = collection
        self._summarize()

    def _summarize(self):
        lrv = None
        lrv_time = None
        for cid in self.collection.iterate_content_ids():
            info = self.collection.get_content_info(cid)
            if lrv_time is None or info.timeline[-1].last < lrv_time:
                lrv_time = info.timeline[-1].last
                lrv = cid
        self.least_recently_verified_timestamp = lrv_time
