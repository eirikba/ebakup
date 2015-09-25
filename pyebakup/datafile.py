#!/usr/bin/env python3

import hashlib

import valuecodecs

class InvalidDataError(Exception): pass
class InternalError(Exception): pass
class ItemNotFoundError(Exception): pass

class Item(object):
    def __init__(self, kind):
        self.kind = kind

def ItemMagic(value):
    item = Item('magic')
    item.value=value
    return item

def ItemSetting(key, value):
    item = Item('setting')
    item.key=key
    item.value=value
    return item

def ItemKeyValue(kvid, key, value):
    item = Item('key-value')
    item.kvid = kvid
    if isinstance(key, str):
        item.key = key.encode('utf-8')
    else:
        item.key = key
    assert b':' not in item.key
    if isinstance(value, bytes):
        item.value = value
    else:
        item.value = str(value).encode('utf-8')
    return item

def ItemExtraDef(xid, kvids):
    item = Item('extradef')
    item.xid = xid
    item.kvids = kvids
    return item

class ItemDirectory(object):
    def __init__(self, dirid, parent, name):
        self.kind = 'directory'
        self.dirid = dirid
        self.parent = parent
        self.name = name
        self.extra_data = 0

    def set_extra_data(self, xid):
        self.extra_data = xid

class ItemFile(object):
    def __init__(self, parent, name, cid, size, mtime):
        self.kind = 'file'
        self.parent = parent
        self.name = name
        self.cid = cid
        self.size = size
        self.mtime_year = mtime[0]
        self.mtime_second = mtime[1]
        self.mtime_ns = mtime[2]
        self.extra_data = 0

    def set_extra_data(self, xid):
        self.extra_data = xid

class ItemSpecialFile(ItemFile):
    def __init__(self, filetype, parent, name, cid, size, mtime):
        if filetype not in ('symlink', 'socket', 'pipe', 'device', 'unknown'):
            raise AssertionError('Unhandled file type: ' + filetype)
        ItemFile.__init__(self, parent, name, cid, size, mtime)
        self.kind = 'file-' + filetype

class ItemContent(object):
    def __init__(self, cid, checksum, first, last):
        self.kind = 'content'
        self.cid = cid
        self.checksum = checksum
        self.first = first
        self.last = last
        self.updates = []

    def content_changed(self, first, last, checksum):
        update = Item('changed')
        update.first = first
        update.last = last
        update.checksum = checksum
        self.updates.append(update)

    def content_restored(self, first, last):
        update = Item('restored')
        update.first = first
        update.last = last
        self.updates.append(update)

def _get_checksum_by_name(name):
    if name == b'sha256':
        return hashlib.sha256
    if name == b'md5':
        return hashlib.md5
    raise NotImplementedError(
        'Unknown block checksum: ' + repr(name))

def create_main_in_replacement_mode(tree, dbpath):
    '''Create an ebakup database at tree:dbpath and return a writable
    DataFile for the "main" file.

    The file is opened and locked for writing.
    '''
    if tree.does_path_exist(dbpath):
        raise FileExistsError('File exists: /' + '/'.join(dbpath))
    f = DataFile(tree, dbpath + ('main.new',))
    final = DataFile(tree, dbpath + ('main',))
    f.create_and_lock()
    f.set_replacement_mode_with_datafile(final)
    f.append_item(ItemMagic(b'ebakup database v1'))
    f.append_item(ItemSetting(b'edb-blocksize', b'4096'))
    f.append_item(ItemSetting(b'edb-blocksum', b'sha256'))
    return f

def create_content_in_replacement_mode(tree, dbpath):
    '''Create the "content" file for the ebakup database at tree:dbpath.

    The file is opened and locked for writing.

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''
    f = DataFile(tree, dbpath + ('content.new',))
    final = DataFile(tree, dbpath + ('content',))
    f.create_and_lock()
    f.set_replacement_mode_with_datafile(final)
    f.append_item(ItemMagic(b'ebakup content data'))
    f.append_item(ItemSetting(b'edb-blocksize', b'4096'))
    f.append_item(ItemSetting(b'edb-blocksum', b'sha256'))
    return f

def replace_content(tree, dbpath):
    '''Rewrite the "content" file for the ebakup database at tree:dbpath.

    This method returns a pair (old, new). 'old' is the old "content"
    file opened and locked read-only. 'new' is the new, empty
    "content" file, opened and locked for writing. 'new' is opened in
    "replacement" mode (which means you need to call
    commit_and_close() instead of close() to make the replacement
    permanent.)

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''

def create_backup_in_replacement_mode(tree, dbpath, starttime):
    '''Create a "backup" data file for a backup starting at 'starttime' in
    the ebakup database at tree:dbpath.

    The file is opened and locked for writing, in "replacement" mode
    (which means you need to call commit_and_close() instead of
    close() to make the replacement permanent.)

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''
    fname = '{:02}-{:02}T{:02}:{:02}'.format(
        starttime.month, starttime.day, starttime.hour, starttime.minute)
    oldpath = dbpath + (str(starttime.year), fname)
    newpath = dbpath + (str(starttime.year), fname + '.new')
    old = DataFile(tree, oldpath)
    new = DataFile(tree, newpath)
    new.create_and_lock()
    new.set_replacement_mode_with_datafile(old)
    new.append_item(ItemMagic(b'ebakup backup data'))
    new.append_item(ItemSetting(b'edb-blocksize', b'4096'))
    new.append_item(ItemSetting(b'edb-blocksum', b'sha256'))
    new.append_item(ItemSetting(
        b'start',
        '{:04}-{:02}-{:02}T{:02}:{:02}:{:02}'.format(
            starttime.year, starttime.month, starttime.day,
            starttime.hour, starttime.minute, starttime.second)
        .encode('utf-8')))
    return new

def open_main(tree, dbpath, writable=False):
    '''Open the "main" file in the ebakup database at tree:dbpath.

    If 'writable' is True, the file is opened and locked for both
    reading and writing. Otherwise, the file is only opened and locked
    for reading.
    '''
    f = DataFile(tree, dbpath + ('main',))
    if writable:
        raise NotImplementedError()
    else:
        f.open_and_lock_readonly()
    return f

def open_content(tree, dbpath, writable=False):
    '''Open the "content" file in the ebakup database at tree:dbpath.

    If 'writable' is True, the file is opened and locked for both
    reading and writing. Otherwise, the file is only opened and locked
    for reading.

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''
    f = DataFile(tree, dbpath + ('content',))
    if writable:
        f.open_and_lock_readwrite()
    else:
        f.open_and_lock_readonly()
    return f

def open_backup(tree, dbpath, starttime, writable=False):
    '''Open the backup file for the backup started at 'starttime' in the
    ebakup database at tree:dbpath.

    If 'writable' is True, the file is opened and locked for both
    reading and writing. Otherwise, the file is only opened and locked
    for reading.

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''
    fname = '{:02}-{:02}T{:02}:{:02}'.format(
        starttime.month, starttime.day, starttime.hour, starttime.minute)
    f = DataFile(tree, dbpath + (str(starttime.year), fname))
    if writable:
        raise AssertionError('backup files are immutable!')
    else:
        f.open_and_lock_readonly()
    for item in f:
        if item.kind == 'setting' and item.key == b'start':
            value = item.value
            break
        if item.kind not in ('setting', 'magic'):
            raise InvalidDataError(
                'Failed to find "start" setting in backup file')
    f.seek(0, 0)
    if str(starttime.year) + '-' + fname != value[:-3].decode('utf-8'):
        raise InvalidDataError(
            'Backup file has non-matching start time: ' +
            str(value) + ' vs ' + str(starttime.year) + '-' + fname)
    return f

def get_unopened_content(tree, dbpath):
    '''Get a DataFile for the "content" file in the ebakup database at
    tree:dbpath.

    The returned DataFile is not opened. To actually do anything with
    the file, you need to call one of the open_* methods first.

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''
    return DataFile(tree, dbpath + ('content',))

class DataFile(object):

    '''A "data file" is a sequence of "blocks", each of which contains a
    sequence of "data items".

    Each data item has a "position", which is a pair (block, index),
    where 'block' indicates which block it is in and 'index' indicates
    which item of the block it is. Both 'block' and 'index' are
    zero-based, so (0, 0) would indicate the very first item in the
    file.

    Unless otherwise noted in the method's description, every method
    only does the minimal necessary changes to the file. Thus any
    method that does not modify the file will not change the position
    of any item. And any method that modifies the file will only
    change the position of items in the same block as the
    modifications happen in. And the change is always the "obvious"
    one.

    One thing to be aware of is that a DataFile object will assume
    that nothing else modifies the file while the DataFile has it open
    (and locked).

    The first block contains all the information about the file itself
    and nothing else. The first item of the first block will be a
    "magic" item which indicates what kind of file it is (and thus how
    to interpret the data in the file). The rest of the items in the
    first block are "settings" items which provide more details about
    how to interpret the file.
    '''

    def __init__(self, tree, path):
        '''You should typically not create DataFile objects directly. Use one
        of the create_* or open_* functions instead.
        '''
        self._tree = tree
        self._path = path
        self._clear_file_data()

    def __enter__(self):
        '''See __exit__().
        '''
        return self

    def __exit__(self, a, b, c):
        '''Using a DataFile as a context will ensure that close() is called
        when the context is exited. Note that in some cases this will
        abort any changes made, while in other cases it will commit
        them. (See close() for details.)
        '''
        self.close()

    def create_and_lock(self):
        '''Create the file and lock it for both reading and writing.

        If the file already exists, a FileExistsError will be raised.
        '''
        if self._file is not None:
            raise AssertionError('File already open')
        self._file = self._tree.create_regular_file(self._path)
        self._file.lock_for_writing()
        self._check_correct_file_opened()
        self._file.drop_all_cached_data()
        if self._file.get_size() != 0:
            raise AssertionError('Newly created file is not empty')
        self._create_block_0()
        self._initialize_file_data()

    def open_and_lock_readonly(self):
        '''Open and lock the file for reading.
        '''
        if self._file is not None:
            raise AssertionError('File already open')
        self._file = self._tree.get_item_at_path(self._path)
        self._file.lock_for_reading()
        self._check_correct_file_opened()
        self._initialize_file_data()

    def open_and_lock_readwrite(self):
        '''Open and lock the file for both reading and writing.
        '''
        if self._file is not None:
            raise AssertionError('File already open')
        self._file = self._tree.get_modifiable_item_at_path(self._path)
        self._file.lock_for_writing()
        self._check_correct_file_opened()
        self._initialize_file_data()

    def set_replacement_mode_with_datafile(self, replacefile):
        '''Set this DataFile in replacement mode, with 'replacefile' as the
        target file.

        In replacement mode, calling close() on the DataFile will
        remove the file. If you wish to keep the changes, you need to
        call commit_and_close() instead.

        Calling commit_and_close() will replace the file represented
        by 'replacefile' with the file built by this DataFile. So the
        path this DataFile is created with is just a temporary storage
        until it is either removed (by close()) or renamed to the
        final path (by commit_and_close()).

        Regardless of which way this DataFile is closed, it will also
        automatically call close() on 'replacefile'.
        '''
        if self._replace_file is not None:
            raise AssertionError('Already in replacement mode')
        if not self._tree.is_same_file_system_as(replacefile._tree):
            raise AssertionError('Can not replace between file systems')
        self._replace_file = replacefile

    def commit_and_close(self):
        '''Commit any changes, drop all locks and close the file.

        This is equivalent to close() for DataFile objects that
        directly modify the "real" file. However, for DataFile objects
        in the "replacement" mode, close() will discard all the
        changes while commit_and_close() will make the changes live.
        '''
        f = self._file
        if f is None:
            return
        replace = self._replace_file
        if replace is None:
            self.close()
            return
        self.flush()
        if not self._tree.is_same_file_system_as(replace._tree):
            raise AssertionError('Can not replace between file systems')
        self._tree.rename_and_overwrite(self._path, replace._path)
        replace.close()
        f.close()
        self._clear_file_data()

    def close(self):
        '''Drop all locks and close the file.

        This will also flush any unwritten data. However, if the
        DataFile object is in "replacement" mode, it will abort all
        the changes. If that's not what you want, use
        commit_and_close() instead.

        It is safe to call close() at any time. If the file is not
        open, close() will have no effect.
        '''
        if self._file is None:
            return
        if self._replace_file is not None:
            self._tree.delete_file_at_path(self._path)
            self._file.close()
            self._replace_file.close()
        else:
            self.flush()
            self._file.close()
        self._clear_file_data()

    def flush(self):
        '''Flush any unwritten data to the file.

        Most methods that modify the file keeps changes in memory for
        a while. This method will ensure that any such changes are
        written to the file. But see sync() for a complication.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        for idx, block in self._blocks.items():
            assert block.blockno == idx
            if block.modified:
                block.modified = False
                self._write_block(idx, block)

    def sync(self):
        '''Ensure all changes are written to the physical medium.

        While flush() ensures that all changes made to the file are
        sent to the operating system, sync() will tell the operating
        system to write the data to the physical disk and wait until
        the operating system reports back that the writing is
        complete.

        You might say that when flush() returns any changes made to
        the file will exist even if the application crashes, while
        when sync() returns any changes will exist even if the whole
        computer crashes.

        Unfortunately, most consumer hard drives do not properly
        support this feature, so this method can't be completely
        trusted to do its job.
        '''
        raise NotImplementedError()

    def __iter__(self):
        '''A DataFile is its own iterator.

        Iterating over a DataFile will yield all the items in the file
        in order.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        return self

    def __next__(self):
        '''See __iter__().
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        try:
            block = self._load_block(self._pos[0])
        except ItemNotFoundError:
            raise StopIteration()
        try:
            item = block.get_item(self._pos[1])
        except ItemNotFoundError:
            self._pos = (self._pos[0] + 1, 0)
            return self.__next__()
        self._pos = (self._pos[0], self._pos[1] + 1)
        return item

    def get_last_block_index(self):
        '''Return the index of the last block that exists in the file.
        '''
        return self._last_block_index

    def does_block_exist(self, blockidx):
        '''Return True iff block number 'block' exists in the file.
        '''
        return blockidx <= self._last_block_index

    def create_block(self):
        '''Appends a new, empty block to the end of the file.
        '''
        self._create_block()

    def get_item(self, block, index):
        '''Get the 'index'th item of the 'block'th block.

        'block' and 'index' are both zero-based. If (block, index)
        does not refer to an existing item, an ItemNotFoundError will
        be raised.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        raise NotImplementedError()

    def replace_item(self, block, index, item):
        '''Replace the 'index'th item of the 'block'th block with the data of
        'item'.

        'block' and 'index' are both zero-based. If (block, index)
        does not refer to an existing item, an ItemNotFoundError will
        be raised. If there is insufficient space in the block, a
        BlockFullError will be raised.

        This method does not change the position of any item in the
        file (except the one item that is replaced, of course).
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        raise NotImplementedError()

    def insert_item(self, block, index, item):
        '''Insert the data from 'item' before the 'index'th item of the
        'block'th block.

        'block' and 'index' are both zero-based. 'index' may be -1 to
        indicate that the item should be added after all other items
        in the block.

        If (block, index) does not refer to an existing item (and
        index is not -1), an ItemNotFoundError will be raised. If
        there is insufficient space in the block, a BlockFullError
        will be raised.

        Any items after 'index' in the same block will have their
        indices increased by 1. Other blocks will be unaffected.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        block = self._load_block(block)
        if index == -1:
            block.append_item(item)
            return
        raise NotImplementedError()

    def append_item(self, item):
        '''Add the data from 'item' as a new item at the end of the file.

        The new data will be added to the last block in the file if
        there is sufficient space for it. Otherwise a new block will
        be added to the end of the file and the new data item will be
        added to that block.

        This method does not change the position of any existing item
        in the file.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        if item.kind in ('magic', 'setting'):
            if self._last_block_index != 0:
                raise AssertionError('Settings block is not last')
            if item.kind == 'magic':
                self._handle_magic(item.value)
            elif item.kind == 'setting':
                self._handle_setting(item.key, item.value)
            block = self._load_block(0)
            block.append_item(item)
            return
        if self._last_block_index == 0:
            self._flush_block(0)
            self._create_block()
        last_block = self._load_block(self._last_block_index)
        if last_block.try_append(item):
            return
        self._flush_block(self._last_block_index)
        block = self._create_block()
        if not block.try_append(item):
            self._drop_block(self._last_block_index)
            raise AssertionError('Item too large for a single block')

    def remove_item(self, block, index):
        '''Remove the 'index'th item in the 'block'th block from the file.

        'block' and 'index' are both zero-based. If (block, index)
        does not refer to an existing item, an ItemNotFoundError will
        be raised.

        Any items after 'index' in the same block will have their
        indices decreased by 1. Other blocks will be unaffected.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        raise NotImplementedError()

    def tell(self):
        '''Return the current (read) position.

        The returned value is a pair (block, index), indicating that
        next() will yield the 'index'th item of the 'block'th block.
        'block' and 'index' are both zero-based.

        If the current position is at the end of the file (so next()
        will raise StopIteration), this method may return (-1, -1), or
        it may return (last_block, last_index+1) or (last_block+1, 0).
        There are no guarantees about when it will return which of
        those values.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        raise NotImplementedError()

    def seek(self, block, index):
        '''Set the current read position.

        After calling this method, the next call to next() will yield
        the 'index'th item of the 'block'th block. Further calls to
        next() will yield the following items in order.

        'block' and 'index' can both be -1, in which case the current
        position is set to the end of the file. Thus, calling next()
        will raise StopIteration.

        If (block, index) does not refer to an existing item (and is
        not -1, -1), an ItemNotFoundError will be raised.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        if block == 0 and index == 0:
            self._pos = (0, 0)
            return
        raise NotImplementedError()

    def move_block(self, source, target):
        '''Move all items from block 'source' to block 'target'.

        All the items in the 'source'th block will be added to the end
        of the 'target'th block, maintaining their original
        order. More or less like:

          while block_is_not_empty(source):
            item = get_item(source, 0)
            remove_item(source, 0)
            insert_item(target, -1, item)

        'target' may be -1 to create a new block at the end of the
        file and move the data from 'source' there.

        If there is not enough space in the target block, a
        BlockFullError will be raised and the call to this method will
        have no effect.

        If 'source' does not refer to an existing block, an
        ItemNotFoundError will be raised.

        If 'target' does not refer to an existing block and is not -1,
        an ItemNotFoundError will be raised.

        All the moved items will have their positions changed in the
        obvious way. No other items' positions are affected.
        '''
        if self._file is None:
            raise AssertionError('File is not open')
        raise NotTestedError()
        sourceblock = self._load_block(source)
        if target == -1:
            targetblock = self._create_block()
            targetblock.append_block_data(sourceblock)
            sourceblock.clear_block_data()
            return
        raise NotImplementedError()

    def _clear_file_data(self):
        self._file = None
        self._pos = None
        self._blocksize = None
        self._blockdatasize = None
        self._blocksum = None
        self._itemcodec = None
        self._last_block_index = None
        self._blocks = {}
        self._replace_file = None

    def _initialize_file_data(self):
        self._pos = (0, 0)
        self._read_block_settings()
        fsize = self._file.get_size()
        if fsize == 0:
            self._last_block_index = 0
        elif self._blocksize is None:
            raise AssertionError('Non-empty file without block size')
        else:
            bcount = fsize // self._blocksize
            if bcount * self._blocksize != fsize:
                raise AssertionError('File is not a whole number of blocks')
            self._last_block_index = bcount - 1

    def _read_block_settings(self):
        block = self._blocks.get(0)
        if block is not None:
            return
        data = self._file.get_data_slice(0, 10000)
        if data == b'':
            return
        end = data.find(b'\n')
        if end < 0:
            raise AssertionError('No magic found')
        magic = data[:end]
        start = data.find(b'\nedb-blocksize:')
        if start < 0:
            raise AssertionError('No block size found')
        end = data.find(b'\n', start+15)
        if end < 0:
            raise AssertionError('No end of block size value')
        blocksize = int(data[start+15:end], 10)
        if start > blocksize:
            raise AssertionError('No block size in first block')
        start = data.find(b'\nedb-blocksum:')
        if start < 0:
            raise AssertionError('No block checksum algorithm found')
        if start > blocksize:
            raise AssertionError('No block checksum algorithm in first block')
        end = data.find(b'\n', start+14)
        if end < 0:
            raise AssertionError('No end of block checksum algorithm')
        blocksum = data[start+14:end]
        self._blocksize = blocksize
        self._blocksum = _get_checksum_by_name(blocksum)
        self._blockdatasize = blocksize - self._blocksum().digest_size
        self._check_blocksum(data[:self._blocksize])
        self._handle_magic(magic)

    def _create_block_0(self):
        assert 0 not in self._blocks
        assert self._last_block_index is None
        block = Block0(b'', self._blockdatasize)
        self._blocks[0] = block
        block.modified = True
        block.blockno = 0

    def _create_block(self):
        assert self._last_block_index >= 0
        block = self._itemcodec.create_empty_block(self._blockdatasize)
        self._last_block_index += 1
        self._blocks[self._last_block_index] = block
        block.modified = True
        block.blockno = self._last_block_index
        return block

    def _check_correct_file_opened(self):
        # TODO: Implement this (check that self._tree:self._path is
        # the same file as self._file)
        return True

    def _flush_block(self, idx):
        block = self._blocks.get(idx)
        if block is None:
            return
        if block.modified:
            block.modified = False
            self._write_block(idx, block)

    def _write_block(self, idx, block):
        assert block.blockno == idx
        data = block.encode()
        if len(data) > self._blockdatasize:
            raise AssertionError('Final block data too big!')
        data += b'\x00' * (self._blockdatasize - len(data))
        assert len(data) == self._blockdatasize
        cksum = self._blocksum(data).digest()
        self._file.write_data_slice(idx * self._blocksize, data + cksum)

    def _handle_magic(self, value):
        if value == b'ebakup database v1':
            self._itemcodec = MainHandler()
        elif value == b'ebakup content data':
            self._itemcodec = ContentHandler()
        elif value == b'ebakup backup data':
            self._itemcodec = BackupHandler()
        else:
            raise NotImplementedError('Unhandled magic')

    def _handle_setting(self, key, value):
        if key == b'edb-blocksize':
            if self._blocksize is not None:
                raise AssertionError('Blocksize set twice')
            self._blocksize = int(value, 10)
        if key == b'edb-blocksum':
            if self._blocksum is not None:
                raise AssertionError('Block checksum algorithm set twice')
            self._blocksum = _get_checksum_by_name(value)
        if self._blocksum is not None and self._blocksize is not None:
            if self._blockdatasize is None:
                self._blockdatasize = (
                    self._blocksize - self._blocksum().digest_size)

    def _load_block(self, index):
        block = self._blocks.get(index)
        if block is not None:
            return block
        blockdata = self._file.get_data_slice(
            index * self._blocksize, (index+1) * self._blocksize)
        if blockdata == b'':
            raise ItemNotFoundError('Requested block beyond end of file')
        assert len(blockdata) == self._blocksize
        self._check_blocksum(blockdata)
        if index == 0:
            block = Block0(blockdata, self._blockdatasize)
        else:
            block = self._itemcodec.decode_block(
                blockdata, self._blockdatasize)
        self._blocks[index] = block
        block.blockno = index
        return block

    def _check_blocksum(self, data):
        assert len(data) == self._blocksize
        if (self._blocksum(data[:self._blockdatasize]).digest() !=
                data[self._blockdatasize:]):
            raise InvalidDataError('Block checksum mismatch')

class Block0(object):
    def __init__(self, data, blockdatasize):
        self.modified = False
        if blockdatasize is not None and len(data) > blockdatasize:
            data = data[:blockdatasize]
        if data:
            data = data.split(b'\n')
            if data[-1].endswith(b'\x00'):
                if data[-1].strip(b'\x00') != b'':
                    raise InvalidDataError('trailing garbage')
                data = data[:-1]
            if data and data[-1] == b'':
                data = data[:-1]
            self._data = [ x + b'\n' for x in data ]
        else:
            self._data = []
        self._datasize = len(data)
        self._blockdatasize = blockdatasize

    def encode(self):
        return b''.join(self._data)

    def set_blockdatasize(self, size):
        assert self._blockdatasize is None
        if self._datasize > size:
            raise BlockFullError('Block 0 is over the new limit')
        self._blockdatasize = size

    def get_item(self, index):
        assert index >= 0
        if index >= len(self._data):
            raise ItemNotFoundError('Item ' + str(index) + ' not found')
        data = self._data[index]
        if index == 0:
            return ItemMagic(data[:-1])
        colon = data.find(b':')
        if colon < 0:
            raise AssertionError('Invalid item data: ' + str(data))
        return ItemSetting(data[:colon], data[colon+1:-1])

    def append_item(self, item):
        if item.kind == 'magic':
            if self._data:
                raise AssertionError('"Magic" item must be the first one')
            assert isinstance(item.value, bytes)
            data = item.value + b'\n'
        elif item.kind == 'setting':
            assert isinstance(item.key, bytes)
            assert isinstance(item.value, bytes)
            if not self._data:
                raise AssertionError('First item must be "magic"')
            assert b'\n' not in item.key
            assert b':' not in item.key
            assert b'\n' not in item.value
            data = item.key + b':' + item.value + b'\n'
        else:
            raise AssertionError(
                'Unknown item type for block 0: ' + str(item.kind))
        if (self._blockdatasize is not None and
                len(data) + self._datasize > self._blockdatasize):
            raise BlockFullError('Block 0 full')
        self.modified = True
        self._data.append(data)
        self._datasize += len(data)


class MainHandler(object):
    pass

class ContentBlock(object):
    def __init__(self):
        self.modified = False
        self._items = [] # [ (data, item) ]
        self._datasize = 0
        self._blockdatasize = None

    def set_blockdatasize(self, size):
        assert self._blockdatasize is None
        if self._datasize > size:
            raise BlockFullError('Block is over the new size limit')
        self._blockdatasize = size

    def get_item(self, index):
        assert index >= 0
        if index >= len(self._items):
            raise ItemNotFoundError('Item ' + str(index) + ' not found')
        return self._items[index][1]

    def append_item(self, item):
        if not self.try_append(item):
            raise BlockFullError('Block is full')

    def try_append(self, item):
        if item.kind == 'content':
            data = [ b'\xdd' ]
            data.append(valuecodecs.make_varuint(len(item.cid)))
            data.append(valuecodecs.make_varuint(len(item.checksum)))
            if item.cid.startswith(item.checksum):
                data.append(item.cid)
            else:
                if not item.checksum.startswith(item.cid):
                    raise InvalidDataError('cid and checksum mismatch')
                data.append(item.checksum)
            data.append(valuecodecs.make_uint32(item.first))
            data.append(valuecodecs.make_uint32(item.last))
            for update in item.updates:
                if update.kind == 'changed':
                    data.append(b'\xa1')
                    data.append(update.checksum)
                elif update.kind == 'restored':
                    data.append(b'\xa0')
                else:
                    raise InvalidDataError(
                        'Unknown update type: ' + update.kind)
                data.append(valuecodecs.make_uint32(update.first))
                data.append(valuecodecs.make_uint32(update.last))
            data = b''.join(data)
            if (self._blockdatasize is not None and
                    self._datasize + len(data) > self._blockdatasize):
                return False
            self.modified = True
            self._items.append((data, item))
            self._datasize += len(data)
            return True
        raise InvalidDataError('Unknown item type: ' + item.kind)

    def encode(self):
        return b''.join(x[0] for x in self._items)

class ContentHandler(object):
    def decode_block(self, data, size):
        if size > len(data):
            size = len(data)
        done = 0
        block = ContentBlock()
        while done < size:
            start = done
            if data[done] == 0xdd:
                cidlen, done = valuecodecs.parse_varuint(data, done+1)
                sumlen, done = valuecodecs.parse_varuint(data, done)
                cid = data[done:done+cidlen]
                cksum = data[done:done+sumlen]
                done += max(cidlen, sumlen)
                first = (
                    data[done] + data[done+1] * 0x100 +
                    data[done+2] * 0x10000 + data[done+3] * 0x1000000)
                done += 4
                last = (
                    data[done] + data[done+1] * 0x100 +
                    data[done+2] * 0x10000 + data[done+3] * 0x1000000)
                done += 4
                item = ItemContent(cid, cksum, first, last)
                if done > size:
                    raise InvalidDataError(
                        'Content item overran block end at ' + str(done))
                while data[done] in (0xa0, 0xa1):
                    done += 1
                    checksum = None
                    if data[done-1] == 0xa1:
                        checksum = data[done:done+sumlen]
                        done += sumlen
                    first = (
                        data[done] + data[done+1] * 0x100 +
                        data[done+2] * 0x10000 + data[done+3] * 0x1000000)
                    done += 4
                    last = (
                        data[done] + data[done+1] * 0x100 +
                        data[done+2] * 0x10000 + data[done+3] * 0x1000000)
                    done += 4
                    if checksum is None:
                        item.content_restored(first, last)
                    else:
                        item.content_changed(first, last, checksum)
                if done > size:
                    raise InvalidDataError(
                        'Content item overran block end at ' + str(done))
                block._items.append((data[start:done], item))
                block._datasize += done - start
            elif data[done] == 0:
                if data[done:size].strip(b'\x00'):
                    raise InvalidDataError('Trailing garbage')
                done = size
            else:
                raise InvalidDataError('Unknown data item: ' + str(data[done]))
        return block

    def create_empty_block(self, blockdatasize):
        block = ContentBlock()
        block.set_blockdatasize(blockdatasize)
        return block

class BackupBlock(object):
    def __init__(self):
        self.modified = False
        self._items = [] # [ (data, item) ]
        self._datasize = 0
        self._blockdatasize = None

    def set_blockdatasize(self, size):
        assert self._blockdatasize is None
        if self._datasize > size:
            raise BlockFullError('Block is over the new size limit')
        self._blockdatasize = size

    def get_item(self, index):
        assert index >= 0
        if index >= len(self._items):
            raise ItemNotFoundError('Item ' + str(index) + ' not found')
        return self._items[index][1]

    def append_block_data(self, sourceblock):
        if self._datasize + sourceblock._datasize > self._blockdatasize:
            raise BlockFullError('Not sufficient space in the block')
        self._items += sourceblock._items
        self._datasize += sourceblock._datasize
        self._modified = True

    def clear_block_data(self):
        self._items = []
        self._datasize = 0
        self._modified = True

    def append_item(self, item):
        if not self.try_append(item):
            raise BlockFullError('Block is full')

    _filetypechars = {
        'file-unknown': b'?', 'file-symlink': b'L', 'file-socket': b'S',
        'file-pipe': b'P', 'file-device': b'D' }
    def try_append(self, item):
        if item.kind == 'directory':
            if item.extra_data:
                data = [ b'\x92' ]
            else:
                data = [ b'\x90' ]
            data.append(valuecodecs.make_varuint(item.dirid))
            data.append(valuecodecs.make_varuint(item.parent))
            data.append(valuecodecs.make_varuint(len(item.name)))
            data.append(item.name)
            if item.extra_data:
                data.append(valuecodecs.make_varuint(item.extra_data))
            data = b''.join(data)
        elif item.kind.startswith('file'):
            if item.kind == 'file':
                if item.extra_data:
                    data = [ b'\x93' ]
                else:
                    data = [ b'\x91' ]
            else:
                data = [ b'\x94' ]
            data.append(valuecodecs.make_varuint(item.parent))
            data.append(valuecodecs.make_varuint(len(item.name)))
            data.append(item.name)
            data.append(valuecodecs.make_varuint(len(item.cid)))
            data.append(item.cid)
            data.append(valuecodecs.make_varuint(item.size))
            year = item.mtime_year
            second = item.mtime_second
            ns = item.mtime_ns
            if year < 1000 or year > 9999:
                raise InvalidDataError(
                    'Unreasonable last-modified year: ' + str(year))
            if second < 0 or second > 31622399:
                # Not checking for December 32 in non-leap years
                raise InvalidDataError(
                    'Unreasonable last-modifed second of year: ' + str(second))
            if ns < 0 or ns > 999999999:
                raise InvalidDataError(
                    'Unreasonable last-modified nanosecond: ' + str(ns))
            mtime = bytes((
                item.mtime_year & 0xff, (item.mtime_year >> 8),
                item.mtime_second & 0xff, (item.mtime_second >> 8) & 0xff,
                (item.mtime_second >> 16) & 0xff,
                ((item.mtime_second >> 17) & 0x80) + (item.mtime_ns & 0x3f),
                (item.mtime_ns >> 6) & 0xff, (item.mtime_ns >> 14) & 0xff,
                item.mtime_ns >> 22))
            data.append(mtime)
            if item.kind != 'file':
                filetypechar = self._filetypechars.get(item.kind)
                if filetypechar is None:
                    raise AssertionError('Unknown file type: ' + item.kind)
                data.append(filetypechar)
            if item.kind != 'file' or item.extra_data:
                data.append(valuecodecs.make_varuint(item.extra_data))
            data = b''.join(data)
        elif item.kind == 'key-value':
            assert b':' not in item.key
            data = [ ]
            data.append(valuecodecs.make_varuint(item.kvid))
            data.append(item.key)
            data.append(b':')
            data.append(item.value)
            data = b''.join(data)
            data = b'\x21' + valuecodecs.make_varuint(len(data)) + data
        elif item.kind == 'extradef':
            data = [ ]
            data.append(valuecodecs.make_varuint(item.xid))
            for kvid in item.kvids:
                data.append(valuecodecs.make_varuint(kvid))
            data = b''.join(data)
            data = b'\x22' + valuecodecs.make_varuint(len(data)) + data
        else:
            raise InvalidDataError('Unknown item type: ' + item.kind)

        if (self._blockdatasize is not None and
                self._datasize + len(data) > self._blockdatasize):
            return False
        self.modified = True
        self._items.append((data, item))
        self._datasize += len(data)
        return True

    def encode(self):
        return b''.join(x[0] for x in self._items)

class BackupHandler(object):
    _filetypechars = {
        b'?'[0]: 'unknown', b'L'[0]: 'symlink', b'S'[0]: 'socket',
        b'P'[0]: 'pipe', b'D'[0]: 'device',
        }
    def decode_block(self, data, size):
        if size > len(data):
            size = len(data)
        done = 0
        block = BackupBlock()
        while done < size:
            item = None
            start = done
            if data[done] in (0x90, 0x92):
                itemtype = data[done]
                done += 1
                dirid, done = valuecodecs.parse_varuint(data, done)
                parent, done = valuecodecs.parse_varuint(data, done)
                namelen, done = valuecodecs.parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                item = ItemDirectory(dirid, parent, name)
                if itemtype == 0x92:
                    extra_data, done = valuecodecs.parse_varuint(data, done)
                    item.set_extra_data(extra_data)
                itemdata = data[start:done]
            elif data[done] in (0x91, 0x93, 0x94):
                itemtype = data[done]
                done += 1
                parent, done = valuecodecs.parse_varuint(data, done)
                namelen, done = valuecodecs.parse_varuint(data, done)
                name = data[done:done+namelen]
                done += namelen
                cidlen, done = valuecodecs.parse_varuint(data, done)
                cid = data[done:done+cidlen]
                done += cidlen
                filesize, done = valuecodecs.parse_varuint(data, done)
                mtime_year = data[done] + data[done+1] * 256
                mtime_second = (
                    data[done+2] + data[done+3] * 0x100 +
                    data[done+4] * 0x10000)
                if data[done+5] >= 0x80:
                    mtime_second += 0x1000000
                mtime_ns = (
                    (data[done+5] & 0x3f) + data[done+6] * 0x40 +
                    data[done+7] * 0x4000 + data[done+8] * 0x400000)
                done += 9
                if itemtype in (0x91, 0x93):
                    item = ItemFile(
                        parent, name, cid, filesize,
                        (mtime_year, mtime_second, mtime_ns))
                elif itemtype == 0x94:
                    ftype = self._filetypechars.get(data[done])
                    if ftype is None:
                        raise AssertionError(
                            'Unknown filetype: ' + str(data[done]))
                    done += 1
                    item = ItemSpecialFile(
                        ftype, parent, name, cid, filesize,
                        (mtime_year, mtime_second, mtime_ns))
                else:
                    raise AssertionError('unreachable')
                if itemtype in (0x93, 0x94):
                    extra, done = valuecodecs.parse_varuint(data, done)
                    item.set_extra_data(extra)
                itemdata = data[start:done]
            elif data[done] == 0x21:
                done += 1
                length, done = valuecodecs.parse_varuint(data, done)
                end = done + length
                kvid, done = valuecodecs.parse_varuint(data, done)
                kv = data[done:end]
                done = end
                key, value = kv.split(b':', 1)
                item = ItemKeyValue(kvid, key, value)
                itemdata = data[start:done]
            elif data[done] == 0x22:
                done += 1
                length, done = valuecodecs.parse_varuint(data, done)
                end = done + length
                xid, done = valuecodecs.parse_varuint(data, done)
                kvids = []
                while done < end:
                    kvid, done = valuecodecs.parse_varuint(data, done)
                    kvids.append(kvid)
                assert done == end
                item = ItemExtraDef(xid, tuple(kvids))
                itemdata = data[start:done]
            elif data[done] == 0:
                if data[done:size].strip(b'\x00'):
                    raise InvalidDataError('Trailing garbage')
                done = size
            else:
                raise InvalidDataError('Unknown data item')
            if item is not None:
                block._items.append((itemdata, item))
                block._datasize += len(itemdata)
        return block

    def create_empty_block(self, blockdatasize):
        block = BackupBlock()
        block.set_blockdatasize(blockdatasize)
        return block
