#!/usr/bin/env python3

import collections
import datetime
import hashlib
import re

import valuecodecs
import datafile

class DataCorruptError(Exception): pass

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

def _datetime_from_backup_name(name):
    match = Database._re_backup_name.match(name)
    if match is None:
        raise AssertionError('Invalid backup name: ' + name)
    return datetime.datetime(
        int(match.group(1)), int(match.group(2)), int(match.group(3)),
        int(match.group(4)), int(match.group(5)))

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
        backup_names = self._get_backup_names_for_year(years[-1])
        backup_name = backup_names[-1]
        return BackupInfo(self, backup_name)

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
        names = [ year_name + '-' + x for x in files ]
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
        backup = BackupInfo(self, backup_name)
        return backup

    def get_most_recent_backup_before(self, when):
        '''Obtain the data for the most recent backup before 'when' according
        to the starting time.
        '''
        yearly = self._get_backup_names_for_year(when.year)
        when_name = '{:04}-{:02}-{:02}T{:02}:{:02}'.format(
            when.year, when.month, when.day, when.hour, when.minute)
        candidates = [x for x in yearly if x <= when_name]
        backup = None
        if candidates:
            backup_name = candidates[-1]
            backup = BackupInfo(self, backup_name)
            if backup.get_start_time() >= when:
                if len(candidates) > 1:
                    backup_name = candidates[-2]
                    backup = BackupInfo(self, backup_name)
                else:
                    backup = None
        if not backup:
            years = self._get_backup_year_list()
            years = [ x for x in years if x < when.year ]
            if not years:
                return None
            backup_name = self._get_backup_names_for_year(years[-1])[-1]
            backup = BackupInfo(self, backup_name)
        return backup

    def get_oldest_backup_after(self, when):
        '''Obtain the data for the oldest backup after 'when' according to the
        starting time.
        '''
        yearly = self._get_backup_names_for_year(when.year)
        when_name = '{:04}-{:02}-{:02}T{:02}:{:02}'.format(
            when.year, when.month, when.day, when.hour, when.minute)
        candidates = [x for x in yearly if x >= when_name]
        backup = None
        if candidates:
            backup_name = candidates[0]
            backup = BackupInfo(self, backup_name)
            if backup.get_start_time() <= when:
                if len(candidates) > 1:
                    backup_name = candidates[1]
                    backup = BackupInfo(self, backup_name)
                else:
                    backup = None
        if not backup:
            years = self._get_backup_year_list()
            years = [ x for x in years if x > when.year ]
            if not years:
                return None
            backup_name = self._get_backup_names_for_year(years[0])[0]
            backup = BackupInfo(self, backup_name)
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
    'ContentData', ('contentid', 'checksum', 'first_seen'))

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
        if hasattr(item, 'updates'):
            self._logger.warn('deprecated', 'item.updates')
        if hasattr(item, 'last'):
            self._logger.warn('deprecated', 'item.last')
        self.contentdata[item.cid] = ContentData(
            item.cid,
            item.checksum,
            datetime.datetime.utcfromtimestamp(item.first))

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
            timestamp)
        self._dbfile.open_and_lock_readwrite()
        with self._dbfile:
            self._dbfile.append_item(item)
        self.contentdata[contentid] = ContentData(
            contentid, checksum, when)
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

    def get_first_seen_time(self):
        # IMPLEMENTME
        return self._data.first_seen


class BackupInfoBuilder(object):
    def __init__(self, db, start):
        self._db = db
        self._next_dirid = 8
        self._directories = { (): 0 }
        self._dbfile = datafile.create_backup_in_replacement_mode(
            self._db._tree, self._db._path, start)
        self._extra_kvids = {}
        self._extra_next_kvid = 0
        self._extra_xids = { tuple(): 0 }
        self._extra_next_xid = 8
        self._defblock = None  # The block index of the last "definition" block

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
        endsetting = '{:04}-{:02}-{:02}T{:02}:{:02}:{:02}'.format(
                when.year, when.month, when.day,
                when.hour, when.minute, when.second).encode('utf-8')
        self._dbfile.insert_item(0, -1, datafile.ItemSetting(
            b'end', endsetting))
        self._dbfile.commit_and_close()

    def abort(self):
        '''If commit() has not been called yet, this method will delete the
        backup data object
        '''
        self._dbfile.close()

    def add_file(
            self, path, contentid, size, mtime, mtime_nsec, filetype='file',
            extra={}):
        '''Add a file to the backup data object.

        Note: mtime.microsecond will be ignored! (But should be either
        0 or match mtime_nsec)
        '''
        dirid = self._directories.get(path[:-1])
        if dirid is None:
            raise NotTestedError(
                'Parent directory does not exist: ' + str(path))
        name = path[-1]
        component = valuecodecs.path_component_to_bytes(name)
        mtime_second = int(
            (mtime - datetime.datetime(mtime.year, 1, 1)).total_seconds())
        if filetype == 'file':
            item = datafile.ItemFile(
                dirid, component, contentid, size,
                (mtime.year, mtime_second, mtime_nsec))
        else:
            item = datafile.ItemSpecialFile(
                filetype,
                dirid, component, contentid, size,
                (mtime.year, mtime_second, mtime_nsec))
        if extra:
            item.set_extra_data(self._get_or_create_extra_data(extra))
        self._dbfile.append_item(item)

    def _get_or_create_extra_data(self, extra):
        kvids = []
        for key, value in extra.items():
            key, value = self._encode_key_value(key, value)
            kv = (key, value)
            kvid = self._extra_kvids.get(kv)
            item = datafile.ItemKeyValue(kvid, key, value)
            if item.kvid is None:
                item.kvid = self._extra_next_kvid
                self._extra_next_kvid += 1
                self._add_definition_to_dbfile(item)
                self._extra_kvids[kv] = item.kvid
            kvids.append(item.kvid)
        kvids = tuple(kvids)
        xid = self._extra_xids.get(kvids)
        if xid is None:
            xid = self._extra_next_xid
            self._extra_next_xid += 1
            self._add_definition_to_dbfile(datafile.ItemExtraDef(xid, kvids))
            self._extra_xids[kvids] = xid
        return xid

    def _encode_key_value(self, key, value):
        if key == 'owner':
            value = value.encode('utf-8')
        elif key == 'group':
            value = value.encode('utf-8')
        elif key == 'unix-access':
            v = value
            value = b''
            for i in range(4):
                value = str(v % 8).encode('utf-8') + value
                v = v // 8
        else:
            raise NotImplementedError('Unknown key: ' + str(key))
        key = key.encode('utf-8')
        return key, value

    def _add_definition_to_dbfile(self, item):
        if self._defblock is None:
            self._defblock = 1
            if self._dbfile.does_block_exist(self._defblock):
                self._dbfile.move_block(self._defblock, -1)
            else:
                self._dbfile.create_block()
            if not self._dbfile.does_block_exist(self._defblock + 1):
                self._dbfile.create_block()
        self._dbfile.insert_item(self._defblock, -1, item)

    def add_directory(self, path, extra_data={}):
        dirid = self._directories.get(path)
        if dirid is not None:
            raise NotTestedError('Directory already exists')
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
        item = datafile.ItemDirectory(dirid, parentid, component)
        if extra_data:
            item.set_extra_data(self._get_or_create_extra_data(extra_data))
        self._dbfile.append_item(item)
        return dirid

class DirectoryData(object):
    def __init__(self, name, parentid, extra_data=None):
        if extra_data is None:
            extra_data = {}
        self.name = name
        self.parentid = parentid
        self.extra_data = extra_data
        self.directories = {}
        self.files = {}

class FileData(object):
    __slots__ = ('_data', 'name')

    def __init__(
            self, name, parentid, contentid, size, mtime, mtime_nsec,
            filetype, extra_data):
        self.name = name
        mtimestr = (
            str(mtime.year) + '.' + str(mtime.month) + '.' + str(mtime.day) +
            '.' + str(mtime.hour) + '.' + str(mtime.minute) + '.' +
            str(mtime.second) + '.' + str(mtime.microsecond))
        d = [
            # 'name' is being extracted and stored by _build_tree, so
            # there's a (tiny) net memory gain and a significant
            # performance gain to just storing name directly. If
            # _build_tree didn't need a copy of 'name', I believe
            # there could be a significant memory gain from encoding
            # 'name' in _data, too.

            #self._encode(name.encode('utf-8', errors='surrogateescape')),
            b'', # Placeholder for 'name'
            str(parentid).encode('utf-8'),
            self._encode(contentid),
            str(size).encode('utf-8'),
            mtimestr.encode('utf-8'),
            str(mtime_nsec).encode('utf-8'),
            filetype.encode('utf-8') ]
        for k,v in extra_data.items():
            d.append(
                self._encode(k.encode('utf-8')) + b':' +
                self._encode(str(v).encode('utf-8')))
        self._data = b'&'.join(d)

    def _decode(self, data):
        return data.replace(b'%a', b'&').replace(b'%p', b'%')

    def _encode(self, data):
        return data.replace(
            b'%p', b'%pp').replace(b'%a', b'%pa').replace(b'&', b'%a')

    @property
    def xname(self):
        return self._decode(self._data.split(b'&', 1)[0]).decode(
            'utf-8', errors='surrogateescape')

    @property
    def parentid(self):
        return int(self._data.split(b'&', 2)[1])

    @property
    def contentid(self):
        return self._decode(self._data.split(b'&', 3)[2])

    @property
    def size(self):
        return int(self._data.split(b'&', 4)[3])

    @property
    def mtime(self):
        mtimeparts = [
            int(x) for x in
            self._data.split(b'&', 5)[4].decode('utf-8').split('.') ]
        return datetime.datetime(*mtimeparts)

    @property
    def mtime_nsec(self):
        return int(self._data.split(b'&', 6)[5])

    @property
    def filetype(self):
        return self._data.split(b'&', 7)[6].decode('utf-8')

    @property
    def extra_data(self):
        items = self._data.split(b'&')[7:]
        xd = {}
        for item in items:
            k, v = item.split(b':', 1)
            # If an unknown key shows up here, it may need to be
            # decoded to the proper type (similar to what is done with
            # 'unix-access' already).
            assert k in (b'owner', b'group', b'unix-access')
            if k == b'unix-access':
                value = int(v)
            else:
                value = v.decode('utf-8')
            xd[k.decode('utf-8')] = value
        return xd


class BackupInfo(object):
    def __init__(self, db, name):
        self._db = db
        self._name = name
        start = _datetime_from_backup_name(name)
        self._dbfile = datafile.open_backup(
            self._db._tree, self._db._path, start)
        self._read_whole_file()
        start = self.get_start_time()
        assert name == (
            '{:04}-{:02}-{:02}T{:02}:{:02}'.format(
                start.year, start.month, start.day, start.hour, start.minute))

    def _read_whole_file(self):
        start = _datetime_from_backup_name(self._name)
        with datafile.open_backup(self._db._tree, self._db._path, start) as f:
            self.settings = {}
            self.extra_kvids = {}
            self.extra_xids = {}
            self.directories = {}
            self.directories[0] = DirectoryData(None, None)
            self.files = []
            state = 0
            for item in f:
                if item.kind == 'magic':
                    assert state == 0
                    state = 1
                    assert item.value == b'ebakup backup data'
                elif item.kind == 'setting':
                    assert state == 1
                    if item.key not in (
                            b'edb-blocksize', b'edb-blocksum',
                            b'start', b'end'):
                        raise NotTestedError(
                            'Unknown setting: ' + str(item.key))
                    assert item.key not in self.settings
                    self.settings[item.key] = item.value
                else:
                    if item.kind == 'directory':
                        if state in (1, 2):
                            state = 3
                        assert state == 3
                        self._add_directory(
                            item.parent, item.dirid, item.name, item.extra_data)
                    elif item.kind == 'file':
                        if state in (1, 2):
                            state = 3
                        assert state == 3
                        self._add_file(item)
                    elif item.kind.startswith('file-'):
                        if state in (1, 2):
                            state = 3
                        assert state == 3
                        self._add_file(item)
                    elif item.kind == 'key-value':
                        if state == 1:
                            state = 2
                        assert state == 2
                        assert item.kvid not in self.extra_kvids
                        self.extra_kvids[item.kvid] = self._decode_key_value(
                            item.key, item.value)
                    elif item.kind == 'extradef':
                        if state == 1:
                            state = 2
                        assert state == 2
                        assert item.xid not in self.extra_xids
                        self.extra_xids[item.xid] = item.kvids
                    else:
                        raise NotTestedError(
                            'Unknown data entry (' + str(item.kind) + ')')
            self._build_tree()

    def _decode_key_value(self, key, value):
        key = key.decode('utf-8')
        if key == 'unix-access':
            assert len(value) == 4
            value = int(value, 8)
        elif key in ('group', 'owner'):
            value = value.decode('utf-8')
        return key, value

    def _add_directory(self, parentid, dirid, name, extra_data):
        assert dirid not in self.directories
        self.directories[dirid] = DirectoryData(
            valuecodecs.bytes_to_path_component(name),
            parentid,
            self._decode_extra_data(extra_data))

    def _add_file(self, item):
        mtime = (
            datetime.datetime(item.mtime_year, 1, 1) +
            datetime.timedelta(
                seconds=item.mtime_second, microseconds=item.mtime_ns//1000))
        if item.kind == 'file':
            filetype = 'file'
        elif item.kind.startswith('file-'):
            filetype = item.kind[5:]
        else:
            raise UnreachableError()
        self.files.append(
            FileData(
                valuecodecs.bytes_to_path_component(item.name),
                item.parent,
                item.cid,
                item.size,
                mtime,
                item.mtime_ns,
                filetype,
                self._decode_extra_data(item.extra_data)))

    def _decode_extra_data(self, xid):
        if xid == 0:
            return {}
        kvids = self.extra_xids.get(xid)
        if kvids is None:
            raise DataCorruptError('Unknown extra-data id: ' + str(xid))
        extra = {}
        for kvid in kvids:
            kv = self.extra_kvids.get(kvid)
            if kv is None:
                raise DataCorruptError('Unknown key-value id: ' + str(kvid))
            assert kv[0] not in extra
            extra[kv[0]] = kv[1]
        return extra

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
        if start_time is None:
            raise DataCorruptError(
                'No "start" setting found for backup ' + self._name)
        match = BackupInfo.re_datetime.match(start_time)
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
        if end_time is None:
            raise DataCorruptError(
                'No "end" setting found for backup ' + self._name)
        match = BackupInfo.re_datetime.match(end_time)
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
        'size', 'mtime', 'mtime_nsec', 'filetype', 'extra_data'.

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

    def get_dir_info(self, path):
        '''Return an object describing the directory at 'path'. Return None if
        'path' is not a directory.

        The returned object has (at least) the property 'extra_data'.
        '''
        dirdata = self.directories[0]
        for comp in path:
            if comp not in dirdata.directories:
                return None
            dirdata = dirdata.directories[comp]
        return dirdata
