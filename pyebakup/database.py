#!/usr/bin/env python3

import datetime
import hashlib
import re

import datafile

from dbinternals.dbfileopener import DBFileOpener


class DataCorruptError(Exception):
    pass


def create_database(tree, path):
    '''Create a new, empty database at 'path' in 'tree'.

    A Database object for the new database is returned.
    '''
    if tree.does_path_exist(path):
        raise FileExistsError('Path already exists: ' + str(path))
    main = datafile.create_main_in_replacement_mode(tree, path)
    main.append_item(datafile.ItemSetting(b'checksum', b'sha256'))
    main.commit_and_close()
    datafile.create_content_in_replacement_mode(tree, path).commit_and_close()
    return Database(tree, path)


class Database(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        self._content = None
        self._fileopener = None
        self._read_main(tree, path)

    def _read_main(self, tree, path):
        with self._dbfileopener.open_main(tree, path) as main:
            item = next(main)
            if item.kind != 'magic':
                raise AssertionError('First item in main is not magic')
            if item.value != b'ebakup database v1':
                raise AssertionError('Wrong file type in database')
            for item in main:
                if item.kind != 'setting':
                    raise AssertionError('Non-setting in database main file')
                if item.key == b'checksum':
                    self._content_checksum_name = item.value.decode('utf-8')

    _re_backup_file = re.compile(r'^\d\d-\d\dT\d\d:\d\d$')

    def get_all_backup_names(self, order_by=None):
        '''Obtain a list of the names of all backups.

        Every backup has a name. This method returns a sequence
        containing the name of every backup.

        The name of a backup is a string representation of the time
        when the backup was started. You should not rely on the exact
        format of the string, but you can rely on the strings only
        depending on the starting time of the backup, and that the
        strings will sort chronologically.

        The list is ordered according to 'order_by':
          None - The list is unordered.
          'starttime' - The list is sorted on the time the backup
              started, oldest first.

        '''
        if order_by not in (None, 'starttime'):
            raise AssertionError('Unexpected order_by: ' + str(order_by))
        years = self._get_backup_year_list()
        names = []
        for year in years:
            names += self._get_backup_names_for_year(year)
        if order_by == 'starttime':
            names.sort()
        return names

    _re_backup_name = re.compile(r'^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d)')

    def get_backup_file_reader_for_name(self, name):
        '''Obtain a DataFile opened read-only for the backup named 'name'.

        WARNING: This method may change behaviour or go away in the
        (near) future. I'm still considering whether this is a good
        idea.

        See DataFile.open_backup() for details on the returned object.
        '''
        match = self._re_backup_name.match(name)
        start = datetime.datetime(
            int(match.group(1)), int(match.group(2)), int(match.group(3)),
            int(match.group(4)), int(match.group(5)))
        return self._dbfileopener.open_raw_backup(
            self._tree, self._path, start)

    def create_backup_file_in_replacement_mode(self, starttime):
        '''Create a backup file for a backup starting at 'starttime'.

        This will create a new backup according to whatever data it is
        fed. If there already exists a conflicting backup file, this
        method will fail.

        See DataFile.create_backup_in_replacement_mode() for details
        on the returned object.
        '''
        return self._dbfileopener.create_backup_in_replacement_mode(
            self._tree, self._path, starttime)

    def get_most_recent_backup(self):
        '''Obtain the data for the most recent backup according to the
        starting time.
        '''
        years = self._get_backup_year_list()
        if not years:
            return None
        backup_names = self._get_backup_names_for_year(years[-1])
        backup_name = backup_names[-1]
        return self._dbfileopener.open_backup(self, backup_name)

    def _get_backup_year_list(self):
        years = []
        dirs, files = self._tree.get_directory_listing(self._path)
        for name in dirs:
            try:
                name_as_num = int(name)
                years.append(name_as_num)
            except ValueError:
                pass
        years.sort()
        return years

    def _get_backup_names_for_year(self, year):
        year_name = str(year)
        dirs, files = self._tree.get_directory_listing(
            self._path + (year_name,))
        assert not dirs
        names = [year_name + '-' + x for x in files]
        names.sort()
        return names

    def get_oldest_backup(self):
        '''Obtain the data for the oldest backup according to the starting
        time.
        '''
        years = self._get_backup_year_list()
        if not years:
            return None
        backup_names = self._get_backup_names_for_year(years[0])
        backup_name = backup_names[0]
        backup = self._dbfileopener.open_backup(self, backup_name)
        return backup

    def get_most_recent_backup_before(self, when):
        '''Obtain the data for the most recent backup before 'when' according
        to the starting time.
        '''
        yearly = self._get_backup_names_for_year(when.year)
        when_name = '{:04}-{:02}-{:02}T{:02}:{:02}'.format(
            when.year, when.month, when.day, when.hour, when.minute)
        candidates = [x for x in yearly if x <= when_name]
        while candidates:
            backup = self._dbfileopener.open_backup(self, candidates.pop())
            if backup.get_start_time() < when:
                return backup
        years = [x for x in self._get_backup_year_list() if x < when.year]
        if not years:
            return None
        name = self._get_backup_names_for_year(years[-1])[-1]
        return self._dbfileopener.open_backup(self, name)

    def get_oldest_backup_after(self, when):
        '''Obtain the data for the oldest backup after 'when' according to the
        starting time.
        '''
        yearly = self._get_backup_names_for_year(when.year)
        when_name = '{:04}-{:02}-{:02}T{:02}:{:02}'.format(
            when.year, when.month, when.day, when.hour, when.minute)
        candidates = [x for x in yearly if x >= when_name]
        candidates.reverse()
        while candidates:
            backup = self._dbfileopener.open_backup(self, candidates.pop())
            if backup.get_start_time() > when:
                return backup
        years = [x for x in self._get_backup_year_list() if x > when.year]
        if not years:
            return None
        name = self._get_backup_names_for_year(years[0])[0]
        return self._dbfileopener.open_backup(self, name)

    def start_backup(self, when):
        '''Adds a new backup object to the database.

        The backup is registered as having been started at 'when'
        (which should be a naive datetime.datetime in utc timezone).

        Returns an object to be used to fill in the data of this
        backup. The new backup object will not be made part of the
        database until commit() is called on the returned object.

        '''
        return self._dbfileopener.create_backup(self, when)

    def get_checksum_algorithm_name(self):
        '''Return the name of the checksum algorithm used to identify file
        contents.
        '''
        return self._content_checksum_name

    def get_checksum_algorithm(self):
        '''Return the checksum algorithm used to identify file contents.

        The returned object is factory creating objects that largely
        follows the hashlib standard (and in many cases is likely to
        be a class from hashlib).
        '''
        name = self.get_checksum_algorithm_name()
        return self._get_checksum_algorithm_from_name(name)

    def iterate_contentids(self):
        '''Iterates over all content ids in this database.
        '''
        self._load_content_file()
        yield from self._content.iterate_contentids()

    def get_content_info(self, cid):
        '''Return the information about the content with the content id 'cid'.

        The returned object supports at least:
          - get_contentid()
          - get_good_checksum()
          - get_first_seen_time()
        '''
        self._load_content_file()
        return self._content.get_info_for_cid(cid)

    def get_all_content_infos_with_checksum(self, checksum):
        '''Return a sequence of ContentInfo objects for all the content items
        that have the "good" checksum 'checksum'.
        '''
        self._load_content_file()
        return self._content.get_all_content_infos_with_checksum(checksum)

    def add_content_item(self, when, checksum):
        '''Add a new content item to the database, which had the checksum
        'checksum' at the time 'when'.

        Return the content id of the newly added item.
        '''
        self._load_content_file()
        return self._content.add_content_item(when, checksum)

    def _set_dbfileopener(self, opener):
        '''Override the code that opens and parses the raw files.

        This is primarily intended for testing.
        '''
        assert self._fileopener is None
        self._fileopener = opener

    @property
    def _dbfileopener(self):
        if self._fileopener is None:
            self._fileopener = DBFileOpener()
        return self._fileopener

    def _get_checksum_algorithm_from_name(self, name):
        if name == 'sha256':
            return hashlib.sha256
        raise AssertionError('Unknown checksum algorithm: ' + str(name))

    def _load_content_file(self):
        if self._content is not None:
            return
        self._content = self._dbfileopener.open_content_file(self)
