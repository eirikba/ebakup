#!/usr/bin/env python3

import hashlib

class DBFileException(Exception): pass
class DataCorruptError(DBFileException): pass
class DBFileUsageError(DBFileException): pass

_RAISE_EXCEPTION = object()

class OpenContext(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.dbfile.close_and_unlock()

class DBFile(object):

    def __init__(self, directory, path):
        self._directory = directory
        self._path = path
        self._block_size = None
        self._block_size_setting = None
        self._block_checksum = None
        self._block_checksum_setting = None
        self._read_file = None
        self._write_file = None
        self._write_path = None # When _write_file is temporary
        self._target_file = None # When _write_file is temporary

    def create(self, magic):
        '''Create the file.

        'magic' is the "magic" line to be written to the beginning of
        the file.

        Will raise FileExistsError if the file already exists.

        When this method returns, the file contains the "magic" line
        and nothing else. However, the file is not really completely
        created yet. When you have filled in all the initial data of
        the file, you must call commit() to create the real file with
        the correct data.

        Make sure to call commit() or close_and_unlock() to drop the
        lock on the file. Using the returned object as a context will
        automatically call close_and_unlock when the context exits.

        You can not read from the file while it is being created.
        '''
        final_file = self._directory.create_regular_file(self._path)
        final_file.lock_for_writing()
        if final_file.get_size() != 0:
            final_file.unlock()
            raise NotTestedError('Database file non-empty after creation')
        self._target_file = final_file
        temp_path = self._path[:-1] + (self._path[-1] + '.new',)
        temp_file = self._directory.create_regular_file(temp_path)
        self._write_path = temp_path
        self._read_file = None
        self._magic = magic
        self._settings = {}
        self._write_file = temp_file
        self._write_file.lock_for_writing()
        try:
            self._write_settings_to_file()
        except:
            self.close_and_unlock()
            raise
        return OpenContext(self)

    def set_block_size(self, block_size):
        '''Set the block size of the file.

        This method can only be called while the file is not open.
        '''
        self._block_size = block_size
        self._block_size_setting = None

    def take_block_size_from_setting(self, setting):
        '''The block size of the file is given by the setting 'setting'.

        The block size will be set to the value of the setting every
        time the file is opened.
        '''
        self._block_size = None
        if isinstance(setting, str):
            self._block_size_setting = setting.encode('utf-8')
        else:
            self._block_size_setting = setting

    def set_block_checksum_algorithm(self, algo):
        '''Set the checksum algorithm to be used for the block checksums.
        '''
        self._block_checksum = algo
        self._block_checksum_setting = None

    def take_block_checksum_algorithm_from_setting(self, setting):
        '''The checksum algorithm to be used for the block checksums is given
        by the setting 'setting'.

        The checksum algorithm will be set to the value of the setting
        every time the file is opened.
        '''
        self._block_checksum = None
        if isinstance(setting, str):
            self._block_checksum_setting = setting.encode('utf-8')
        else:
            self._block_checksum_setting = setting

    def open_for_reading(self):
        '''This locks and opens the file for reading.

        Make sure to call commit() or close_and_unlock() to drop the
        lock on the file. Using the returned object as a context will
        automatically call close_and_unlock when the context exits.
        '''
        if self._read_file or self._write_file:
            raise AssertionError('DBFile already open')
        assert self._target_file is None
        self._read_file = self._directory.get_item_at_path(self._path)
        self._read_file.lock_for_reading()
        try:
            self._prepare_read_file()
        except:
            self.close_and_unlock()
            raise
        return OpenContext(self)

    def _prepare_read_file(self):
        if self._block_size_setting or self._block_checksum_setting:
            self._read_block_configuration_from_settings()
        if not self._block_size:
            raise NotTestedError()
        if not self._block_checksum:
            raise NotTestedError()
        self._parse_settings(self.get_block(0))

    def _read_block_configuration_from_settings(self):
        data = self._read_file.get_data_slice(0, 10000)
        if self._block_size_setting:
            start = data.find(b'\n' + self._block_size_setting + b':')
            if start < 0:
                raise DataCorruptError(
                    'Could not find block size setting in first '
                    '10000 octets of data file')
            start += 2 + len(self._block_size_setting)
            end = data.find(b'\n', start)
            if end < 0:
                raise DataCorruptError(
                    'Could not find block size setting in first '
                    '10000 octets of data file')
            self._block_size = int(data[start:end], 10)
        if self._block_checksum_setting:
            start = data.find(
                b'\n' + self._block_checksum_setting + b':')
            if start < 0:
                raise DataCorruptError(
                    'Could not find block checksum setting in first '
                    '10000 octets of data file')
            start += 2 + len(self._block_checksum_setting)
            end = data.find(b'\n', start)
            if end < 0:
                raise DataCorruptError(
                    'Could not find block checksum setting in first '
                    '10000 octets of data file')
            check = data[start:end]
            if check == b'sha256':
                self._block_checksum = hashlib.sha256
            elif check == b'md5':
                self._block_checksum = hashlib.md5
            else:
                raise DataCorruptError(
                    'Unknown checksum algorithm: ' + str(check))

    def _parse_settings(self, block):
        self._magic = None
        self._settings = []
        done = 0
        end = block.find(b'\n')
        if end < 0:
            raise DataCorruptError('No "magic" line in file')
        self._magic = block[:end]
        done = end + 1
        end = block.find(b'\n', done)
        while end > 0:
            pair = block[done:end].split(b':', 1)
            if len(pair) != 2:
                raise DataCorruptError('Setting line without ":"')
            self._settings.append(pair)
            done = end + 1
            end = block.find(b'\n', done)
        if block[done:].strip(b'\x00') != b'':
            raise DataCorruptError('Padding is not only NUL bytes')

    def open_for_in_place_modification(self):
        '''This locks and opens the file for writing in-place.

        Any modifications made to the file will take effect
        immediately.

        The file is also open for reading, so you can read and write
        freely.

        Make sure to call commit() or close_and_unlock() to drop the
        lock on the file. Using the returned object as a context will
        automatically call close_and_unlock when the context exits.
        '''
        if self._read_file or self._write_file:
            raise AssertionError('DBFile already open')
        assert self._target_file is None
        self._read_file = self._directory.get_item_at_path(self._path)
        self._write_file = self._read_file
        self._write_file.lock_for_writing()
        try:
            self._prepare_read_file()
        except:
            self.close_and_unlock()
            raise
        return OpenContext(self)

    def open_for_full_rewrite(self):
        '''This locks and opens the file for complete rewrite.

        This creates a new file where all the changes are written to.
        This is equivalent to create(), except that the magic and the
        initial settings are taken from the old file, and the file is
        also open for reading. All reading will be done on the old
        file. As with create(), the new data will not replace the old
        data until commit() is called. Calling close_and_unlock() will
        abort all changes.

        Make sure to call commit() or close_and_unlock() to drop the
        lock on the file. Using the returned object as a context will
        automatically call close_and_unlock when the context exits.

        '''
        if self._read_file or self._write_file:
            raise AssertionError('DBFile already open')
        assert self._target_file is None
        self._read_file = self._directory.get_item_at_path(self._path)
        self._read_file.lock_for_writing()
        try:
            self._prepare_read_file()
            self._target_file = self._read_file
            self._write_path = self._path[:-1] + (self._path[-1] + '.new',)
            self._write_file = self._directory.create_regular_file(
                self._write_path)
            self._write_settings_to_file()
        except:
            self.close_and_unlock()
            raise
        return OpenContext(self)

    def commit(self):
        '''This will unlock the file, allowing other processes to read and
        write it.

        If the file is opened by open_for_full_rewrite() or create(),
        this will also commit the new data. That is, this method will
        make the changed data actually replace the old data.

        After this method is called, no more reading or writing can be
        done on the file (until it is opened again, of course).
        '''
        if self._write_file and self._read_file is not self._write_file:
            self._close_unlock_and_replace()
        else:
            self.close_and_unlock()
        assert self._target_file is None
        assert self._write_path is None

    def close_and_unlock(self):
        '''This will unlock the file, allowing other processes to read and
        write it.

        If the file is opened by open_for_full_rewrite() or create(),
        this will abort the changes and leave the file with the old
        data intact.

        After this method is called, no more reading or writing can be
        done on the file (until it is opened again, of course).
        '''
        if self._write_file:
            if self._write_file is not self._read_file:
                self._write_file.close()
                self._write_file = None
                # Delete or not delete? Not sure.
                #self._directory.remove_file(self._write_path)
                self._write_path = None
                self._target_file.close()
                self._target_file = None
            else:
                self._read_file = None
                self._write_file.close()
                self._write_file = None
        assert self._target_file is None
        assert self._write_path is None
        if self._read_file:
            self._read_file.close()
            self._read_file = None

    def _close_unlock_and_replace(self):
        assert self._write_file is not None
        assert self._write_file is not self._read_file
        assert self._target_file is not None
        if self._read_file:
            self._read_file.close()
            self._read_file = None
        self._write_file.close()
        self._write_file = None
        self._directory.rename_and_overwrite(self._write_path, self._path)
        self._write_path = None
        self._target_file.close()
        self._target_file = None

    def get_magic(self):
        '''Return the first line of the file (aka the "magic").'''
        if self._read_file is None:
            raise DBFileUsageError('File not open for reading')
        if self._magic is None:
            raise AssertionError('Broken file, no magic found')
        return self._magic

    def get_setting_keys(self):
        '''Return a tuple of all setting keys in the file, in arbitrary order.
        '''
        if self._read_file is None:
            raise DBFileUsageError('File not open for reading')
        keys = set()
        for setting in self._settings:
            keys.add(setting[0])
        return tuple(keys)

    def get_single_setting(self, key, default=_RAISE_EXCEPTION):
        '''Return the value of the setting named 'key'.

        If the key does not exist and 'default' is not set, KeyError
        is raised.

        If the key does not exist and 'default' is set, 'default' is
        returned.

        if the key is a multi-valued setting, ValueError is raised.
        '''
        if self._read_file is None:
            raise DBFileUsageError('File not open for reading')
        use_key = key
        as_string = False
        if isinstance(key, str):
            as_string = True
            use_key = key.encode('utf-8')
        value = None
        for setting in self._settings:
            if setting[0] == use_key:
                if value is not None:
                    raise ValueError('Setting has multiple values: ' + str(key))
                value = setting[1]
        if value is None:
            if default is _RAISE_EXCEPTION:
                raise KeyError('Setting not found: ' + str(key))
            return default
        if as_string:
            value = value.decode('utf-8')
        return value

    def get_multi_setting(self, key, default=()):
        '''Return the values of the setting named 'key'.

        The values are returned as a tuple, with the values in the
        "correct" order.

        If the key does not exist, 'default' is returned.
        '''
        if self._read_file is None:
            raise DBFileUsageError('File not open for reading')
        use_key = key
        as_string = False
        if isinstance(key, str):
            as_string = True
            use_key = key.encode('utf-8')
        value = []
        for setting in self._settings:
            if setting[0] == use_key:
                value.append(setting[1])
        if not value:
            return default
        if as_string:
            return tuple(x.decode('utf-8') for x in value)
        return tuple(value)

    def set_setting(self, key, value):
        '''Delete all old values for 'key' and add 'value' instead.
        '''
        if self._write_file is None:
            raise DBFileUsageError('File not open for writing')
        use_key = key
        use_value = value
        if isinstance(key, str):
            use_key = key.encode('utf-8')
        if isinstance(value, str):
            use_value = value.encode('utf-8')
        if b':' in use_key:
            raise DBFileUsageError(
                'Setting keys can not contain ":": ' + repr(key))
        if b'\n' in use_key:
            raise DBFileUsageError(
                'Setting keys can not contain newlines: ' + repr(key))
        if b'\n' in use_value:
            raise DBFileUsageError(
                'Setting values can not contain newlines: ' + repr(value))
        settings = [ x for x in self._settings if x[0] != use_key ]
        settings.append( (use_key, use_value) )
        self._settings = settings
        self._write_settings_to_file()

    def _write_settings_to_file(self):
        data = [ x[0] + b':' + x[1] for x in self._settings ]
        data = [ self._magic ] + data + [ b'' ]
        data = b'\n'.join(data)
        self._write_block_to_file(0, data)

    def append_setting(self, key, value):
        '''Add 'value' as the last multi-value for 'key'.
        '''
        if self._write_file is None:
            raise DBFileUsageError('File not open for writing')
        use_key = key
        use_value = value
        if isinstance(key, str):
            use_key = key.encode('utf-8')
        if isinstance(value, str):
            use_value = value.encode('utf-8')
        if b':' in use_key:
            raise DBFileUsageError(
                'Setting keys can not contain ":": ' + repr(key))
        if b'\n' in use_key:
            raise DBFileUsageError(
                'Setting keys can not contain newlines: ' + repr(key))
        if b'\n' in use_value:
            raise DBFileUsageError(
                'Setting values can not contain newlines: ' + repr(value))
        self._settings.append( (use_key, use_value) )
        self._write_settings_to_file()

    def get_block(self, index):
        '''Return the 'index'th block of the file.'''
        if self._read_file is None:
            raise DBFileUsageError('DBFile not open for reading')
        block = self._read_file.get_data_slice(
            index * self._block_size, (index+1) * self._block_size)
        if block == b'':
            return None
        if len(block) != self._block_size:
            raise NotTestedError('Got short block')
        check = self._block_checksum()
        check_size = check.digest_size
        data_size = self._block_size - check_size
        data = block[:data_size]
        checksum = block[data_size:]
        check.update(data)
        if check.digest() != checksum:
            raise DataCorruptError(
                'Block checksum of block ' + str(index) + ' did not match')
        return data

    def get_block_count(self):
        '''Return the number of blocks in the file.'''
        dbfile = self._read_file
        if not dbfile:
            dbfile = self._write_file
        if not dbfile:
            dbfile = self._directory.get_item_at_path(self._path)
        size = dbfile.get_size()
        count = size // self._block_size
        if count * self._block_size != size:
            raise NotTestedError(
                'DBFile not containing a whole number of blocks')
        return count

    def get_block_data_size(self):
        '''Return the size of the data in each block (in octets).'''
        return self._block_size - self._block_checksum().digest_size

    def set_block(self, index, data):
        '''Replace the 'index'th block with 'data'.

        If 'data' is larger than get_block_size(), an exception is raised.
        '''
        if self._write_file is None:
            raise DBFileUsageError('File not open for writing')
        if index < 0:
            raise DBFileUsageError(
                'Can not change data before beginning of file')
        if index < 1:
            raise DBFileUsageError(
                'Can not overwrite blocks before the first data block')
        self._write_block_to_file(index, data)

    def _write_block_to_file(self, index, data):
        offset = index * self._block_size
        size = self._write_file.get_size()
        if offset > size+1:
            raise DBFileUsageError('Can not skip empty space when adding data')
        padding_size = (
            self._block_size - self._block_checksum().digest_size - len(data))
        if padding_size < 0:
            raise DBFileUsageError('Block data too big for block size')
        block_data = data + b'\x00' * padding_size
        checksummer = self._block_checksum()
        checksummer.update(block_data)
        checksum = checksummer.digest()
        block = block_data + checksum
        assert len(block) == self._block_size
        self._write_file.write_data_slice(offset, block)
