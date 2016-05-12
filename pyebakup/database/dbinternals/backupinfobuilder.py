#!/usr/bin/env python3

import datetime

import database.datafile as datafile
import database.valuecodecs as valuecodecs

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

