#!/usr/bin/env python3

import datetime
import re

import datafile
import valuecodecs

_re_backup_name = re.compile(r'^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d)')
def _datetime_from_backup_name(name):
    match = _re_backup_name.match(name)
    if match is None:
        raise AssertionError('Invalid backup name: ' + name)
    return datetime.datetime(
        int(match.group(1)), int(match.group(2)), int(match.group(3)),
        int(match.group(4)), int(match.group(5)))


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
    # The information stored in a FileData object is encoded as a
    # string in the _data member. This saves a lot of memory over
    # storing each item in a dict. Though at some cost to performance.
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
