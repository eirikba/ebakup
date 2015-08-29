#!/usr/bin/env python3

import hashlib

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

def ItemDirectory(dirid, parent, name):
    item = Item('directory')
    item.dirid = dirid
    item.parent = parent
    item.name = name
    return item

def _get_checksum_by_name(name):
    if name == b'sha256':
        return hashlib.sha256
    if name == b'md5':
        return hashlib.md5
    raise NotImplementedError(
        'Unknown block checksum: ' + repr(name))

def create_main(tree, dbpath):
    '''Create an ebakup database at tree:dbpath and return a writable
    DataFile for the "main" file.

    The file is opened and locked for writing.
    '''
    if tree.does_path_exist(dbpath):
        raise FileExistsError('File exists: /' + '/'.join(dbpath))
    f = DataFile(tree, dbpath + ('main',))
    f.create_and_lock()
    f.append_item(ItemMagic(b'ebakup database v1'))
    f.append_item(ItemSetting(b'edb-blocksize', b'4096'))
    f.append_item(ItemSetting(b'edb-blocksum', b'sha256'))
    return f

def create_content(tree, dbpath):
    '''Create the "content" file for the ebakup database at tree:dbpath.

    The file is opened and locked for writing.

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''

def create_backup(tree, dbpath, starttime):
    '''Create a "backup" data file for a backup starting at 'starttime' in
    the ebakup database at tree:dbpath.

    The file is opened and locked for writing.

    Remember that if you want to hold more than one file from the same
    ebakup database open at the same time, you need to hold a lock on
    "main" as long as you have locked any of the other files.
    '''

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
        when the context is exited.
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
        raise NotImplementedError()

    def close(self):
        '''Drop all locks and close the file.

        This will also flush any unwritten data.

        It is safe to call close() at any time. If the file is not
        open, close() will have no effect.
        '''
        if self._file is None:
            return
        self.flush()
        self._file.close()
        self._clear_file_data()

    def flush(self):
        '''Flush any unwritten data to the file.

        Most methods that modify the file keeps changes in memory for
        a while. This method will ensure that any such changes are
        written to the file. But see sync() for a complication.
        '''
        for idx, block in self._blocks.items():
            if block.modified:
                block.modifed = False
                data = block.encode()
                if len(data) > self._blockdatasize:
                    raise AssertionError('Final block data too big!')
                data += b'\x00' * (self._blockdatasize - len(data))
                assert len(data) == self._blockdatasize
                cksum = self._blocksum(data).digest()
                self._file.write_data_slice(idx * self._blocksize, data + cksum)

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
        return self

    def __next__(self):
        '''See __iter__().
        '''
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

    def get_item(self, block, index):
        '''Get the 'index'th item of the 'block'th block.

        'block' and 'index' are both zero-based. If (block, index)
        does not refer to an existing item, an ItemNotFoundError will
        be raised.
        '''
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
        raise NotImplementedError()

    def tell(self):
        '''Return the current (read) position.

        The returned value is a pair (block, index), indicating that
        the next() will yield the 'index'th item of the 'block'th
        block. 'block' and 'index' are both zero-based.

        If the current position is at the end of the file (so next()
        will raise StopIteration), the returned value is (-1, -1).
        '''
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
        block = Block0(b'', self._blockdatasize)
        self._blocks[0] = block
        block.modified = True

    def _check_correct_file_opened(self):
        # TODO: Implement this (check that self._tree:self._path is
        # the same file as self._file)
        return True

    def _handle_magic(self, value):
        if value == b'ebakup database v1':
            self._itemcodec = MainHandler()
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
        self._data.append(data)
        self._datasize += len(data)


class MainHandler(object):
    pass
