#!/usr/bin/env python3

import collections
import datetime
import hashlib
import re

import dbfile
import valuecodecs
import datafile

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
        self._read_main(tree, path)
        self._content = ContentInfoFile(self)

    def _read_main(self, tree, path):
        with datafile.open_main(tree, path) as main:
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

    def _get_checksum_algorithm_from_name(self, name):
        if name == 'sha256':
            return hashlib.sha256
        raise AssertionError('Unknown checksum algorithm: ' + str(check_name))

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
        years = []
        dirs, files = self._tree.get_directory_listing(self._path)
        for name in dirs:
            try:
                num = int(name)
                if 1900 < num < 9999:
                    years.append(name)
            except ValueError:
                pass
        if order_by == 'starttime':
            years.sort()
        backups = []
        for year in years:
            dirs, files = self._tree.get_directory_listing(
                self._path + (year,))
            for name in files:
                if self._re_backup_file.match(name):
                    backups.append(year + '-' + name)
                else:
                    self._logger.log_warning(
                        'unexpected file', (year, name))
        if order_by == 'starttime':
            backups.sort()
        return backups

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
        return datafile.open_backup(self._tree, self._path, start)

    def create_backup_file_in_replacement_mode(self, starttime):
        '''Create a backup file for a backup starting at 'starttime'.

        This will create a new backup according to whatever data it is
        fed. If there already exists a conflicting backup file, this
        method will fail.

        See DataFile.create_backup_in_replacement_mode() for details
        on the returned object.
        '''
        return datafile.create_backup_in_replacement_mode(
            self._tree, self._path, starttime)

    def get_most_recent_backup(self):
        '''Obtain the data for the most recent backup according to the
        starting time.
        '''
        years = self._get_backup_year_list()
        if not years:
            return None
        backup_paths = self._get_backup_paths_for_year(years[-1])
        backup_path = backup_paths[-1]
        backup = BackupInfo(self, self._path + backup_path)
        start = backup.get_start_time()
        assert backup_path == (
            str(start.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                start.month, start.day, start.hour, start.minute))
        return backup

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

    def _get_backup_paths_for_year(self, year):
        year_name = str(year)
        dirs, files = self._tree.get_directory_listing(
            self._path + (year_name,))
        assert not dirs
        names = [ (year_name, x) for x in files ]
        names.sort()
        return names

    def get_oldest_backup(self):
        '''Obtain the data for the oldest backup according to the starting
        time.
        '''
        years = self._get_backup_year_list()
        if not years:
            return None
        backup_paths = self._get_backup_paths_for_year(years[0])
        backup_path = backup_paths[0]
        backup = BackupInfo(self, self._path + backup_path)
        start = backup.get_start_time()
        assert backup_path == (
            str(start.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                start.month, start.day, start.hour, start.minute))
        return backup

    def get_most_recent_backup_before(self, when):
        '''Obtain the data for the most recent backup before 'when' according
        to the starting time.
        '''
        yearly = self._get_backup_paths_for_year(when.year)
        when_name = '{:02}-{:02}T{:02}:{:02}'.format(
            when.month, when.day, when.hour, when.minute)
        candidates = [x for x in yearly if x[1] <= when_name]
        backup = None
        if candidates:
            backup_path = candidates[-1]
            backup = BackupInfo(self, self._path + backup_path)
            if backup.get_start_time() >= when:
                if len(candidates) > 1:
                    backup_path = candidates[-2]
                    backup = BackupInfo(self, self._path + backup_path)
                else:
                    backup = None
        if not backup:
            years = self._get_backup_year_list()
            years = [ x for x in years if x < when.year ]
            if not years:
                return None
            backup_path = self._get_backup_paths_for_year(years[-1])[-1]
            backup = BackupInfo(self, self._path + backup_path)
        start = backup.get_start_time()
        assert backup_path == (
            str(start.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                start.month, start.day, start.hour, start.minute))
        return backup

    def get_oldest_backup_after(self, when):
        '''Obtain the data for the oldest backup after 'when' according to the
        starting time.
        '''
        yearly = self._get_backup_paths_for_year(when.year)
        when_name = '{:02}-{:02}T{:02}:{:02}'.format(
            when.month, when.day, when.hour, when.minute)
        candidates = [x for x in yearly if x[1] >= when_name]
        backup = None
        if candidates:
            backup_path = candidates[0]
            backup = BackupInfo(self, self._path + backup_path)
            if backup.get_start_time() <= when:
                if len(candidates) > 1:
                    backup_path = candidates[1]
                    backup = BackupInfo(self, self._path + backup_path)
                else:
                    backup = None
        if not backup:
            years = self._get_backup_year_list()
            years = [ x for x in years if x > when.year ]
            if not years:
                return None
            backup_path = self._get_backup_paths_for_year(years[0])[0]
            backup = BackupInfo(self, self._path + backup_path)
        start = backup.get_start_time()
        assert backup_path == (
            str(start.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                start.month, start.day, start.hour, start.minute))
        return backup

    def start_backup(self, when):
        '''Adds a new backup object to the database.

        The backup is registered as having been started at 'when'
        (which should be a naive datetime.datetime in utc timezone).

        Returns an object to be used to fill in the data of this
        backup. The new backup object will not be made part of the
        database until commit() is called on the returned object.

        '''
        return BackupInfoBuilder(self, when)

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
        yield from self._content.iterate_contentids()

    def get_content_info(self, contentid):
        '''Return a ContentInfo for the content with 'contentid' as id.
        '''
        data = self._content.contentdata.get(contentid)
        if data is None:
            return None
        return ContentInfo(self, data)

    def get_all_content_infos_with_checksum(self, checksum):
        '''Return a sequence of ContentInfo objects for all the content items
        that have the "good" checksum 'checksum'.
        '''
        return self._content.get_all_content_infos_with_checksum(checksum)

    def add_content_item(self, when, checksum):
        '''Add a new content item to the database, which had the checksum
        'checksum' at the time 'when'.

        Return the content id of the newly added item.
        '''
        return self._content.add_content_item(when, checksum)



ContentData = collections.namedtuple(
    'ContentData', ('contentid', 'checksum', 'timeline'))

class ContentChecksum(object):
    def __init__(self, checksum, first, last, restored):
        self.checksum = checksum
        self.first_sec = first
        self.last_sec = last
        self.restored = restored
        self._first = None
        self._last = None

    @property
    def first(self):
        if self._first is None:
            self._first = datetime.datetime.utcfromtimestamp(self.first_sec)
        return self._first

    @property
    def last(self):
        if self._last is None:
            self._last = datetime.datetime.utcfromtimestamp(self.last_sec)
        return self._last

class ContentInfoFile(object):
    def __init__(self, db):
        self._db = db
        self._dbfile = datafile.get_unopened_content(db._tree, db._path)
        self._read_file()

    def _read_file(self):
        self.contentdata = ContentInfoDict()
        f = self._dbfile
        f.open_and_lock_readonly()
        with f:
            item = next(f)
            if item.kind != 'magic':
                raise AssertionError('First item of content is not magic')
            if item.value != b'ebakup content data':
                raise AssertionError('Wrong magic in content file')
            for item in f:
                if item.kind == 'setting':
                    if item.key in (b'edb-blocksize', b'edb-blocksum'):
                        pass
                    else:
                        raise NotTestedError(
                            'Unknown setting: ' + str(item.key))
                elif item.kind == 'content':
                    self._add_content_item(item)
                else:
                    raise AssertionError(
                        'Unexpected item kind: ' + str(item.kind))

    def _add_content_item(self, item):
        timeline = [
            ContentChecksum(item.checksum, item.first, item.last, True) ]
        for update in item.updates:
            if update.kind == 'restored':
                checksum = item.checksum
                restored = True
            elif update.kind == 'changed':
                checksum = update.checksum
                restored = False
            else:
                raise AssertionError(
                    'Unexpected update kind: ' + str(update.kind))
            timeline.append(
                ContentChecksum(checksum, update.first, update.last, restored))
        self.contentdata[item.cid] = ContentData(
            item.cid, item.checksum, tuple(timeline))

    def get_all_content_infos_with_checksum(self, checksum):
        '''Return a sequence of ContentInfo objects for all the content items
        that have the "good" checksum 'checksum'.
        '''
        infos = []
        for cid in self.contentdata.get_contentids_for_checksum(checksum):
            infos.append(ContentInfo(self._db, self.contentdata[cid]))
        return infos

    def iterate_contentids(self):
        '''Iterates over all content ids in this database.
        '''
        for key in self.contentdata.keys():
            yield key

    def add_content_item(self, when, checksum):
        '''Add the given content item to the file and return its content id.
        '''
        # Make unique content id for 'checksum'
        current = set(
            x.get_contentid() for x in
            self.get_all_content_infos_with_checksum(checksum))
        contentid = checksum
        extra = b'\x00'
        while contentid in current:
            contentid = checksum + extra
            if extra[-1] == 255:
                extra += b'\x00'
            else:
                extra = extra[:-1] + bytes((extra[-1] + 1,))
        assert contentid.startswith(checksum)
        timestamp = int((when - datetime.datetime(1970, 1, 1)) /
                        datetime.timedelta(seconds=1))
        item = datafile.ItemContent(
            contentid,
            checksum,
            timestamp,
            timestamp)
        self._dbfile.open_and_lock_readwrite()
        with self._dbfile:
            self._dbfile.append_item(item)
        self.contentdata[contentid] = ContentData(
            contentid, checksum, (ContentChecksum(checksum, when, when, True),))
        return contentid


class ContentInfoDict(object):
    def __init__(self):
        self._infos = {}
        self._checksums = {}

    def __getitem__(self, key):
        return self._infos[key]

    def __setitem__(self, key, value):
        cksum = value.checksum
        if cksum not in self._checksums:
            self._checksums[cksum] = [ key ]
        else:
            self._checksums[cksum].append(key)
        self._infos[key] = value

    def __contains__(self, key):
        return key in self._infos

    def get(self, key, default=None):
        return self._infos.get(key, default)

    def get_contentids_for_checksum(self, cksum):
        return self._checksums.get(cksum, ())

    def keys(self):
        return self._infos.keys()

    def values(self):
        return self._infos.values()

class ContentInfo(object):
    def __init__(self, db, data):
        self._db = db
        self._data = data

    def get_contentid(self):
        '''Return the content id of this content item.
        '''
        return self._data.contentid

    def get_good_checksum(self):
        '''Return the "good" checksum of this item.
        '''
        return self._data.checksum

    def get_last_known_checksum(self):
        '''Return the last registered checksum of this item.
        '''
        return self._data.timeline[-1].checksum

    def get_checksum_timeline(self):
        '''Return a list of objects describing how the checksum of this
        content item has changed over time. Each object in the list
        has (at least) the properties 'checksum', 'first', 'last' and
        'restored'. 'checksum' is the checksum and 'first' and 'last'
        gives the first and last time during this time span when the
        item was verified to have this checksum. The 'restored' value
        is True if the content was found to be the same as a "believed
        good" source, and False otherwise. If 'restored' is True,
        'checksum' MUST be the same as get_good_checksum().

        The objects in the list are sorted on time, and the time spans
        are not overlapping. (Thus the first object is the "good"
        checksum and the last object is the last known checksum.)
        '''
        return self._data.timeline

    def register_checksum(self, when, checksum):
        '''Register that the checksum of the content item at 'when' was
        'checksum'.
        '''
        raise NotImplementedError()

    def register_content_recovered(self, when, checksum):
        '''Register that the content item has been recovered from a "believed
        good" source at 'when'. And that the checksum was 'checksum'
        at that time.

        If 'checksum' does not match the "good" checksum of the item
        an exception will be raised.
        '''
        raise NotImplementedError()

class BackupInfoBuilder(object):
    def __init__(self, db, start):
        self._db = db
        self._path = self._db._path + (
            str(start.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                start.month, start.day, start.hour, start.minute))
        self._block_no = 1
        self._block_data = b''
        self._next_dirid = 8
        self._directories = { (): 0 }
        self._dbfile = dbfile.DBFile(self._db._tree, self._path)
        self._dbfile.create(b'ebakup backup data', 4096, hashlib.sha256)
        try:
            self._dbfile.set_setting(
                'start',
                '{:04}-{:02}-{:02}T{:02}:{:02}:{:02}'.format(
                    start.year, start.month, start.day,
                    start.hour, start.minute, start.second))
        except:
            self._dbfile.close_and_unlock()
            raise

    def __enter__(self):
        '''Using a BackupInfoBuilder as a context will make it call abort()
        when the context exits.
        '''
        return self

    def __exit__(self, a, b, c):
        self.abort()

    def commit(self, when):
        '''Add this backup data object to the database. It can no longer be
        modified after this method is called.

        The backup is registered as having been completed at 'when'
        (which should be a naive datetime.datetime in utc timezone).
        '''
        self._write_current_block()
        self._dbfile.set_setting(
            'end',
            '{:04}-{:02}-{:02}T{:02}:{:02}:{:02}'.format(
                when.year, when.month, when.day,
                when.hour, when.minute, when.second))
        self._dbfile.commit()

    def abort(self):
        '''If commit() has not been called yet, this method will delete the
        backup data object
        '''
        self._dbfile.close_and_unlock()

    def add_file(self, path, contentid, size, mtime, mtime_nsec):
        '''Add a file to the backup data object.

        Note: mtime.microsecond will be ignored! (But should be either
        0 or match mtime_nsec)
        '''
        dirid = 0
        for i in range(1, len(path)):
            dirid = self.add_directory(path[:i])
        name = path[-1]
        component = valuecodecs.path_component_to_bytes(name)
        entry = b''.join(
            (b'\x91',
             valuecodecs.make_varuint(dirid),
             valuecodecs.make_varuint(len(component)),
             component,
             valuecodecs.make_varuint(len(contentid)),
             contentid,
             valuecodecs.make_varuint(size),
             valuecodecs.make_mtime_with_nsec(mtime, mtime_nsec)))
        self._add_data_entry(entry)

    def _add_data_entry(self, entry):
        block_size = self._dbfile.get_block_data_size()
        if len(self._block_data) + len(entry) <= block_size:
            self._block_data += entry
            return
        if len(entry) > block_size:
            raise ValueError(
                'Size of entry ({}) exceeds block data size ({})'.format(
                    len(entry), block_size))
        self._write_current_block()
        assert self._block_data == b''
        self._block_data = entry

    def _write_current_block(self):
        if len(self._block_data) <= 0:
            return
        blockno = self._block_no
        self._block_no += 1
        blockdata = self._block_data
        self._block_data = b''
        self._dbfile.set_block(blockno, blockdata)

    def add_directory(self, path):
        dirid = self._directories.get(path)
        if dirid is not None:
            return dirid
        name = path[-1]
        parent = path[:-1]
        parentid = self._directories.get(parent)
        if parentid is None:
            assert path != parent
            parentid = self.add_directory(parent)
        dirid = self._next_dirid
        self._next_dirid += 1
        self._directories[path] = dirid
        component = valuecodecs.path_component_to_bytes(name)
        entry = b''.join((
            b'\x90',
            valuecodecs.make_varuint(dirid),
            valuecodecs.make_varuint(parentid),
            valuecodecs.make_varuint(len(component)),
            component))
        self._add_data_entry(entry)
        return dirid

class DirectoryData(object):
    def __init__(self, name, parentid):
        self.name = name
        self.parentid = parentid
        self.directories = {}
        self.files = {}

FileData = collections.namedtuple(
    'FileData',
    ('name', 'parentid', 'contentid', 'size', 'mtime', 'mtime_nsec'))

class BackupInfo(object):
    def __init__(self, db, path):
        self._db = db
        self._path = path
        self._dbfile = dbfile.DBFile(self._db._tree, self._path)
        self._read_whole_file()

    def _read_whole_file(self):
        dbfile = self._dbfile
        with dbfile.open_for_reading():
            self.settings = {}
            for key in dbfile.get_setting_keys():
                if key not in (b'start', b'end'):
                    raise NotTestedError('Unknown setting: ' + str(key))
                assert key not in self.settings
                self.settings[key] = dbfile.get_multi_setting(key)
            self.directories = {}
            self.files = []
            self.directories[0] = DirectoryData(None, None)
            block_index = 1
            data = dbfile.get_block(block_index)
            while data is not None:
                self._add_data_from_block(data)
                block_index += 1
                data = dbfile.get_block(block_index)
            self._build_tree()

    def _add_data_from_block(self, data):
        done = 0
        while True:
            if done >= len(data):
                return
            elif data[done] == 0x90:
                done += 1
                dirid, done = valuecodecs.parse_varuint(data, done)
                parentid, done = valuecodecs.parse_varuint(data, done)
                assert parentid == 0 or parentid > 7
                namelen, done = valuecodecs.parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                self._add_directory(parentid, dirid, name)
            elif data[done] == 0x91:
                done += 1
                parentid, done = valuecodecs.parse_varuint(data, done)
                namelen, done = valuecodecs.parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                cidlen, done = valuecodecs.parse_varuint(data, done)
                contentid = data[done:done+cidlen]
                done += cidlen
                size, done = valuecodecs.parse_varuint(data, done)
                mtime, mtime_nsec = valuecodecs.parse_mtime(data, done)
                done += 9
                self._add_file(
                    parentid, name, contentid, size, mtime, mtime_nsec)
            elif data[done] == 0:
                return
            else:
                raise NotTestedError(
                    'Unknown data entry (type: ' + str(data[done]) + ')')

    def _add_directory(self, parentid, dirid, name):
        assert dirid not in self.directories
        self.directories[dirid] = DirectoryData(
            valuecodecs.bytes_to_path_component(name), parentid)

    def _add_file(self, parentid, name, contentid, size, mtime, mtime_nsec):
        assert mtime.microsecond == mtime_nsec // 1000
        self.files.append(
            FileData(
                valuecodecs.bytes_to_path_component(name), parentid, contentid,
                size, mtime, mtime_nsec))

    def _build_tree(self):
        for d in self.directories.values():
            if d.parentid is None:
                continue
            parent = self.directories[d.parentid]
            if d.name in parent.directories:
                raise NotTestedError(
                    'Multiple directories with same name '
                    'in a single directory: ' + str(d.name))
            parent.directories[d.name] = d
        for f in self.files:
            parent = self.directories[f.parentid]
            if f.name in parent.files or f.name in parent.directories:
                raise NotTestedError(
                    'Multiple items with same name in a single directory: ' +
                    str(f.name))
            parent.files[f.name] = f

    re_datetime = re.compile(rb'^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)$')
    def get_start_time(self):
        '''Return the time at which the backup was started.
        '''
        start_time = self.settings.get(b'start')
        if not start_time:
            raise DataCorruptError(
                'No "start" setting found for ' +
                self._path[-2] + '/' + self._path[-1])
        if len(start_time) > 1:
            raise DataCorruptError(
                'Multiple "start" settings found for ' +
                self._path[-2] + '/' + self._path[-1])
        match = BackupInfo.re_datetime.match(start_time[0])
        if not match:
            raise DataCorruptError(
                'The "start" setting of a backup is not on the correct form (' +
                repr(start_time[0]) + ')' )
        return datetime.datetime(
            int(match.group(1)), int(match.group(2)), int(match.group(3)),
            int(match.group(4)), int(match.group(5)), int(match.group(6)))

    def get_end_time(self):
        '''Return the time at which the backup had completed.
        '''
        end_time = self.settings.get(b'end')
        if not end_time:
            raise DataCorruptError(
                'No "end" setting found for ' +
                self._path[-2] + '/' + self._path[-1])
        if len(end_time) > 1:
            raise DataCorruptError(
                'Multiple "end" settings found for ' +
                self._path[-2] + '/' + self._path[-1])
        match = BackupInfo.re_datetime.match(end_time[0])
        if not match:
            raise DataCorruptError(
                'The "end" setting of a backup is not on the correct form (' +
                repr(end_time[0]) + ')' )
        return datetime.datetime(
            int(match.group(1)), int(match.group(2)), int(match.group(3)),
            int(match.group(4)), int(match.group(5)), int(match.group(6)))

    def get_directory_listing(self, path):
        '''Return the names of all the items in the directory at 'path'. Use
        an empty path, i.e. (), to get the items at the root.

        The names are returned as a pair (dirs, files) of names, where
        the first item is the names of all the directories and the
        second is the names of all the regular files.
        '''
        dirdata = self.directories[0]
        for comp in path:
            dirdata = dirdata.directories[comp]
        return [ x for x in dirdata.directories ], [ x for x in dirdata.files ]

    def is_directory(self, path):
        '''Return True if 'path' represents a directory, False otherwise.
        '''
        dirdata = self.directories[0]
        for comp in path:
            if comp not in dirdata.directories:
                return False
            dirdata = dirdata.directories[comp]
        return True

    def is_file(self, path):
        '''Return True if 'path' represents a file, False otherwise.
        '''
        dirdata = self.directories[0]
        filedata = None
        for comp in path:
            if filedata is not None:
                return False
            if comp in dirdata.directories:
                dirdata = dirdata.directories[comp]
            else:
                if comp not in dirdata.files:
                    return False
                filedata = dirdata.files[comp]
        return filedata is not None

    def get_file_info(self, path):
        '''Return an object describing the file at 'path'. Return None if
        'path' is not a file.

        The returned object has (at least) the properties 'contentid',
        'size', 'mtime', 'mtime_nsec'.

        'mtime.microsecond' and 'mtime_nsec' SHALL agree to
        microsecond precision: mtime.microsecond == mtime_nsec // 1000
        '''
        dirdata = self.directories[0]
        filedata = None
        for comp in path:
            if filedata is not None:
                return None
            if comp in dirdata.directories:
                dirdata = dirdata.directories[comp]
            else:
                if comp not in dirdata.files:
                    return None
                filedata = dirdata.files[comp]
        return filedata
