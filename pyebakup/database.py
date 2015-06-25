#!/usr/bin/env python3


import dbfile

import collections
import datetime
import hashlib
import re

def create_database(directory, path):
    '''Create a new, empty database at 'path' in 'directory'.

    A Database object for the new database is returned.
    '''
    if directory.does_path_exist(path):
        raise FileExistsError('Path already exists: ' + str(path))
    main = dbfile.DBFile(directory, path + ('main',))
    main.set_block_checksum_algorithm(hashlib.sha256)
    main.set_block_size(4096)
    with main.create(b'ebakup database v1'):
        main.set_setting('checksum', 'sha256')
        main.set_setting('blocksize', '4096')
        main.commit()
    content = dbfile.DBFile(directory, path + ('content',))
    content.set_block_checksum_algorithm(hashlib.sha256)
    content.set_block_size(4096)
    content.create(b'ebakup content data')
    content.commit()
    return Database(directory, path)

def _parse_uint32(data, done):
    return (data[done] + data[done+1] * 256 +
            data[done+2] * 0x10000 + data[done+3] * 0x1000000)

def _make_uint32(value):
    if value < 0:
        raise ValueError('Can not make uint32 from negative number')
    if value > 0xffffffff:
        raise ValueError('Value too big for uint32: ' + str(value))
    return bytes((
        value & 0xff, (value >> 8) & 0xff,
        (value >> 16) & 0xff, (value >> 24) & 0xff))

def _parse_varuint(data, done):
    if data[done] < 0x80:
        return data[done], done+1
    if data[done+1] < 0x80:
        return (data[done] & 0x7f) + (data[done+1] << 7), done+2
    if data[done+2] < 0x80:
        return (data[done] & 0x7f) + ((data[done+1] & 0x7f) << 7) + (data[done+2] << 14), done+3
    value = 0
    shift = 0
    while True:
        value += (data[done] & 0x7f) << shift
        shift += 7
        if data[done] < 0x80:
            return value, done + 1
        done += 1

def _make_varuint(value):
    if value < 0:
        raise ValueError('Can not make varuint from negative number')
    if value == 0:
        return b'\x00'
    data = []
    while value > 0x7f:
        data.append((value & 0x7f) | 0x80)
        value >>= 7
    data.append(value)
    return bytes(data)

def _parse_mtime(data, done):
    year = data[done] + (data[done+1] << 8)
    secs = (data[done+2] + (data[done+3] << 8) + (data[done+4] << 16) +
            ((data[done+5] & 0x80) << 17))
    nsecs = ((data[done+5] & 0x3f) + (data[done+6] << 6) +
             (data[done+7] << 14) + (data[done+8] << 22))
    assert year != 0 or (secs == 0 and nsecs == 0)
    assert nsecs >= 0
    assert nsecs < 1000000000
    day = secs // 86400
    assert day >= 0
    assert day < 366
    left = secs - day * 86400
    hour = left // 3600
    assert hour >= 0
    assert hour < 24
    left = left - hour * 3600
    minute = left // 60
    assert minute >= 0
    assert minute < 60
    second = left - minute * 60
    assert second >= 0
    assert second < 60
    month, day = _month_and_day_from_day_of_year(year, day)
    mtime = datetime.datetime(year, month, day, hour, minute, second)
    return mtime, nsecs

daysofmonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
def _month_and_day_from_day_of_year(year, day):
    leap_year= year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)
    if not leap_year and day >= 59:
        # skip February 29
        day += 1
    for month, days in enumerate(daysofmonth):
        if day < days:
            return month+1, day+1
        day -= days
    assert False

def _make_mtime_with_nsec(mtime, nsec):
    assert mtime.microsecond == 0 or mtime.microsecond == nsec//1000
    if nsec < 0 or nsec >= 1000000000:
        raise ValueError('nsec out of range: ' + str(nsec))
    year = mtime.year
    assert year > 0
    assert year < 65536
    day = _day_of_year_from_datetime(mtime)
    sec = day * 86400 + mtime.hour * 3600 + mtime.minute * 60 + mtime.second
    data = ( year & 0xff, year >> 8,
             sec & 0xff, (sec >> 8) & 0xff, (sec >> 16) & 0xff,
             ((sec >> 17) & 0x80) | (nsec & 0x3f),
             (nsec >> 6) & 0xff, (nsec >> 14) & 0xff, nsec >> 22 )
    return bytes(data)

def _day_of_year_from_datetime(mtime):
    day = 0
    for month in range(mtime.month-1):
        day += daysofmonth[month]
    day += mtime.day
    year = mtime.year
    leap_year= year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)
    if not leap_year and day >= 60:
        day -= 1
    return day - 1

def _bytes_to_path_component(component):
    return component.decode('utf-8', errors='surrogateescape')

def _path_component_to_bytes(component):
    if isinstance(component, bytes):
        return component
    return component.encode('utf-8', errors='surrogateescape')

class Database(object):
    def __init__(self, directory, path):
        self._directory = directory
        self._path = path
        self._main = dbfile.DBFile(directory, path + ('main',))
        self._main.take_block_size_from_setting('blocksize')
        self._main.take_block_checksum_algorithm_from_setting('checksum')
        self._content = ContentInfoFile(self)

    def _get_block_size(self):
        with self._main.open_for_reading():
            return int(self._main.get_single_setting('blocksize'))

    def _get_block_checksum_algorithm_name(self):
        return self.get_checksum_algorithm_name()

    def _get_block_checksum_algorithm(self):
        return self._get_checksum_algorithm_from_name(
            self.get_checksum_algorithm_name())

    def _get_checksum_algorithm_from_name(self, name):
        if name == 'sha256':
            return hashlib.sha256
        raise AssertionError('Unknown checksum algorithm: ' + str(check_name))

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
        dirs, files = self._directory.get_directory_listing(self._path)
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
        dirs, files = self._directory.get_directory_listing(
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
        with self._main.open_for_reading():
            return self._main.get_single_setting('checksum')

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
ContentChecksum = collections.namedtuple(
    'ContentChecksum', ('checksum', 'first', 'last', 'restored'))
class ContentInfoFile(object):
    def __init__(self, db):
        self._db = db
        self._dbfile = dbfile.DBFile(db._directory, db._path + ('content',))
        self._dbfile.set_block_size(db._get_block_size())
        self._dbfile.set_block_checksum_algorithm(
            db._get_block_checksum_algorithm())
        self._read_file()

    def _read_file(self):
        self.contentdata = ContentInfoDict()
        with self._dbfile.open_for_reading():
            if self._dbfile.get_magic() != b'ebakup content data':
                raise NotTestedError(
                    'Unexpected magic: ' + str(self._dbfile.get_magic()))
            for key in self._dbfile.get_setting_keys():
                raise NotTestedError('Unknown setting: ' + str(key))
            blockidx = 1
            data = self._dbfile.get_block(blockidx)
            while data is not None:
                self._add_data_from_block(data)
                blockidx += 1
                data = self._dbfile.get_block(blockidx)

    def _add_data_from_block(self, data):
        done = 0
        while True:
            if done >= len(data):
                # Not supposed to happen
                return
            if data[done] == 0:
                assert data[done:] == b'\x00' * (len(data) - done)
                return
            if data[done] == 0xdd:
                cidlen, done = _parse_varuint(data, done + 1)
                checklen, done = _parse_varuint(data, done)
                contentid = data[done:done+cidlen]
                checksum = data[done:done+checklen]
                done += max(cidlen, checklen)
                first = _parse_uint32(data, done)
                first = datetime.datetime.utcfromtimestamp(first)
                done += 4
                last = _parse_uint32(data, done)
                last = datetime.datetime.utcfromtimestamp(last)
                done += 4
                timeline = [ ContentChecksum(checksum, first, last, True) ]
                while data[done] == 0xa0 or data[done] == 0xa1:
                    restored = data[done] == 0xa0
                    done += 1
                    if restored:
                        check = checksum
                    else:
                        check = data[done:done+checklen]
                        done += checklen
                    first = _parse_uint32(data, done)
                    first = datetime.datetime.utcfromtimestamp(first)
                    done += 4
                    last = _parse_uint32(data, done)
                    last = datetime.datetime.utcfromtimestamp(last)
                    done += 4
                    timeline.append(
                        ContentChecksum(check, first, last, restored) )
                if contentid in self.contentdata:
                    raise NotTestedError(
                        'Multiple entries for content id ' + repr(contentid))
                self.contentdata[contentid] = ContentData(
                    contentid, checksum, tuple(timeline))
            else:
                raise NotTestedError(
                    'Unknown content entry type: ' + str(data[done]))

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
        timestamp = int((when - datetime.datetime(1970, 1, 1)) /
                        datetime.timedelta(seconds=1))
        timestamp32 = _make_uint32(timestamp)
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
        entry = b''.join((
            b'\xdd',
            _make_varuint(len(contentid)),
            _make_varuint(len(checksum)),
            contentid,
            timestamp32,
            timestamp32))
        self._add_entry(entry)
        self.contentdata[contentid] = ContentData(
            contentid, checksum, (ContentChecksum(checksum, when, when, True),))
        return contentid

    def _add_entry(self, entry):
        with self._dbfile.open_for_in_place_modification():
            blocksize = self._dbfile.get_block_data_size()
            if len(entry) > blocksize:
                raise ValueError('Entry too big {} for block size {}'.format(
                    len(entry), blocksize))
            blockno = self._dbfile.get_block_count() - 1
            if blockno > 0:
                block = self._dbfile.get_block(blockno)
                block = self._trim_block(block)
                if len(block) + len(entry) <= blocksize:
                    self._dbfile.set_block(blockno, block + entry)
                    return
            blockno += 1
            assert blockno > 0
            self._dbfile.set_block(blockno, entry)

    def _trim_block(self, block):
        '''Return the actual data in 'block'.
        '''
        done = 0
        while done < len(block) and block[done] != 0:
            if block[done] == 0xdd:
                done += 1
                cidlen, done = _parse_varuint(block, done)
                cklen, done = _parse_varuint(block, done)
                done += max(cidlen, cklen) + 8
                while (done < len(block) and
                       (block[done] == 0xa0 or block[done] == 0xa1)):
                    if block[done] == 0xa1:
                        done += cklen
                    done += 9
            else:
                raise NotTestedError('Unknown data entry')
        assert done <= len(block)
        assert block[done:] == b'\x00' * (len(block) - done)
        return block[:done]

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
        self._dbfile = dbfile.DBFile(self._db._directory, self._path)
        self._dbfile.set_block_size(self._db._get_block_size())
        self._dbfile.set_block_checksum_algorithm(
            self._db._get_block_checksum_algorithm())
        self._dbfile.create(b'ebakup backup data')
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
        component = _path_component_to_bytes(name)
        entry = b''.join(
            (b'\x91',
             _make_varuint(dirid),
             _make_varuint(len(component)),
             component,
             _make_varuint(len(contentid)),
             contentid,
             _make_varuint(size),
             _make_mtime_with_nsec(mtime, mtime_nsec)))
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
        component = _path_component_to_bytes(name)
        entry = b''.join((
            b'\x90',
            _make_varuint(dirid),
            _make_varuint(parentid),
            _make_varuint(len(component)),
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
        self._dbfile = dbfile.DBFile(self._db._directory, self._path)
        self._dbfile.set_block_size(self._db._get_block_size())
        check_algo = self._db._get_block_checksum_algorithm()
        self._dbfile.set_block_checksum_algorithm(check_algo)
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
                dirid, done = _parse_varuint(data, done)
                parentid, done = _parse_varuint(data, done)
                assert parentid == 0 or parentid > 7
                namelen, done = _parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                self._add_directory(parentid, dirid, name)
            elif data[done] == 0x91:
                done += 1
                parentid, done = _parse_varuint(data, done)
                namelen, done = _parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                cidlen, done = _parse_varuint(data, done)
                contentid = data[done:done+cidlen]
                done += cidlen
                size, done = _parse_varuint(data, done)
                mtime, mtime_nsec = _parse_mtime(data, done)
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
            _bytes_to_path_component(name), parentid)

    def _add_file(self, parentid, name, contentid, size, mtime, mtime_nsec):
        self.files.append(
            FileData(
                _bytes_to_path_component(name), parentid, contentid,
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