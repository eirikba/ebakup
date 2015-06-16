#!/usr/bin/env python3

import datetime

class InfoTask(object):

    def __init__(self, config, args):
        self._config = config
        self._args = args
        self._services = args.services
        self._logger = self._services['logger']

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
            tree = source.filesystem
            self._print('    source ' + tree.path_to_full_string(source.path))

    def _print_collection_info(self, prefix, collcfg):
            tree = collcfg.filesystem
            self._print(
                prefix + 'collection ' + tree.path_to_full_string(collcfg.path))
            opener = self._services['backupcollection.open']
            try:
                coll = opener(tree, collcfg.path, services=self._services)
            except FileNotFoundError:
                self._print(prefix + '  (Does not exist)')
                coll = None
            if coll:
                colldata = CollectionSummarizer(self._args, coll)
                self._print(
                    prefix + '  Least recently verified: ' +
                    str(colldata.least_recently_verified_timestamp))
                self._print(
                    prefix + '  Total number of content files: ' +
                    str(colldata.total_number_of_cids))
                if colldata.verified_in_the_future > 0:
                    self._print(
                        prefix + '  Verified in the future: ' +
                        str(colldata.verified_in_the_future) + ' files')
                for t in colldata.time_verify_stats:
                    if t[2] > 0:
                        self._print(
                            prefix + '  Not verified for ' + t[0] + ': ' +
                            str(t[2]) + ' files')

    def _print(self, msg):
        self._logger.print(msg)

class CollectionSummarizer(object):
    def __init__(self, args, collection):
        self.args = args
        self.collection = collection
        self._summarize()

    def _summarize(self):
        lrv = None
        lrv_time = None
        utcnow = self.args.services.get('utcnow')
        if utcnow:
            now = utcnow()
        else:
            now = datetime.datetime.utcnow()
        one_week_ago = now - datetime.timedelta(days=7)
        if now.month > 1:
            one_month_ago = now.replace(month=now.month-1)
        else:
            one_month_ago = now.replace(year=now.year-1, month=12)
        if now.month > 3:
            three_months_ago = now.replace(month=now.month-3)
        else:
            three_months_ago = now.replace(year=now.year-1, month=9+now.month)
        one_year_ago = now.replace(year=now.year-1)
        times = [
            ['one year', one_year_ago, 0],
            ['three months', three_months_ago, 0],
            ['one month', one_month_ago, 0],
            ['one week', one_week_ago, 0],
        ]
        verified_in_the_future = 0
        total_number_of_cids = 0
        for cid in self.collection.iterate_contentids():
            total_number_of_cids += 1
            info = self.collection.get_content_info(cid)
            if lrv_time is None or info.timeline[-1].last < lrv_time:
                lrv_time = info.timeline[-1].last
                lrv = cid
            for t in times:
                if t[1] >= info.timeline[-1].last:
                    t[2] += 1
            if info.timeline[-1].last > now:
                verified_in_the_future += 1
        self.least_recently_verified_timestamp = lrv_time
        self.time_verify_stats = times
        self.verified_in_the_future = verified_in_the_future
        self.total_number_of_cids = total_number_of_cids
