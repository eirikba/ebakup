#!/usr/bin/env python3

import hashlib

import valuecodecs

class InvalidDataError(Exception): pass

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

class StreamingReader(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        self._pos = 0
        self._current_block = -1
        self._current_block_end = -1
        self._filetype = None
        with self._tree.get_item_at_path(self._path) as f:
            f.lock_for_reading()
            size = f.get_size()
            self._data = f.get_data_slice(0, size)
        self._prepare_for_reading()

    def _path_for_print(self):
        return self._tree.path_to_full_string(self._path)

    def _prepare_for_reading(self):
        if not self._data:
            # Placeholder file? That's equivalent to not existing.
            raise FileNotFoundError(
                'No backup file at ' + self._path_for_print())
        sizestart = self._data.find(b'\nedb-blocksize:', 0, 10000)
        if sizestart < 0:
            raise InvalidDataError(
                'No block size found in ' + self._path_for_print())
        sizestart += 15
        sizeend = self._data.find(b'\n', sizestart, sizestart + 10)
        if sizestart < 0:
            raise InvalidDataError(
                'End of block size not found in ' + self._path_for_print())
        self._blocksize = int(self._data[sizestart:sizeend], 10)
        if sizestart >= self._blocksize:
            raise InvalidDataError(
                'No block size in settings block of ' + self._path_for_print())
        sumstart = self._data.find(b'\nedb-blocksum:', 0, self._blocksize)
        if sumstart < 0:
            raise InvalidDataError(
                'No block checksum in settings block of ' +
                self._path_for_print())
        sumstart += 14
        sumend = self._data.find(b'\n', sumstart, self._blocksize)
        if sumend < 0:
            raise InvalidDataError(
                'End of block checksum not found in ' + self._path_for_print())
        self._blocksumname = self._data[sumstart:sumend]
        if self._blocksumname == b'sha256':
            self._blocksum = hashlib.sha256
        elif self._blocksumname == b'md5':
            self._blocksum = hashlib.md5
        else:
            raise NotImplementedError(
                'Unknown block checksum: ' + repr(self._blocksumname))
        self._blocksumsize = self._blocksum().digest_size
        self._blockdatasize = self._blocksize - self._blocksumsize

    def __iter__(self):
        return self

    def __next__(self):
        return self._next_item()

    def _next_item(self):
        self._adjust_pos_check_block()
        if self._data[self._pos] == 0:
            block_tail = self._data[self._pos:self._current_block_end]
            if (block_tail.strip(b'\x00') != b''):
                raise InvalidDataError(
                    'Trailing garbage in block at ' + str(self._pos))
            nextblock = self._current_block + self._blocksize
            assert nextblock > self._pos
            assert self._current_block_end + self._blocksumsize == nextblock
            self._pos = nextblock
            return self._next_item()
        if self._current_block == 0:
            return self._next_item_as_settings()
        return self._next_item_for_filetype()

    def _next_item_as_settings(self):
        end = self._data.find(b'\n', self._pos, self._current_block_end)
        if end < 0:
            raise InvalidDataError(
                'Failed to find end of setting at ' + str(self._pos))
        start = self._pos
        if self._pos == 0:
            # First line, magic
            self._pos = end + 1
            magic = self._data[start:end]
            if magic == b'ebakup backup data':
                self._filetype = 'backup'
                self._next_item_for_filetype = self._next_item_for_backup
            elif magic == b'ebakup database v1':
                self._filetype = 'main'
                self._next_item_for_filetype = self._next_item_no_data
            elif magic == b'ebakup content data':
                self._filetype = 'content'
                self._next_item_for_filetype = self._next_item_for_content
            else:
                raise NotImplementedError(
                    'File type not recognized: ' + magic.decode('utf-8'))
            return ItemMagic(magic)
        mid = self._data.find(b':', start, end)
        if mid < 0:
            raise InvalidDataError(
                'Failed to find end of key for setting at ' + str(self._pos))
        key = self._data[start:mid]
        value = self._data[mid+1:end]
        if (key == b'edb-blocksize' and
                value != str(self._blocksize).encode('utf-8')):
            raise InvalidDataError('Inconsistent block-size settings!')
        if key == b'edb-blocksum' and value != self._blocksumname:
            raise InvalidDataError('Inconsistent block checksum settings!')
        if key == b'start' and self._filetype == 'backup':
            name = self._path[-2] + '-' + self._path[-1]
            if name.encode('utf-8') != value[:-3]:
                raise InvalidDataError(
                    'Start time and backup name inconsistent: ' +
                    value.decode('utf-8') + ' vs ' + str(name))
        self._pos = end + 1
        return ItemSetting(self._data[start:mid], self._data[mid+1:end])

    def _adjust_pos_check_block(self):
        if self._pos >= len(self._data):
            raise StopIteration()
        if (self._pos >= self._current_block and
                self._pos < self._current_block_end):
            return
        if self._pos == 0:
            nextblock =  0
        else:
            nextblock = self._current_block + self._blocksize
        if self._pos <= nextblock and self._pos >= self._current_block_end:
            self._pos = nextblock
            if self._pos >= len(self._data):
                raise StopIteration()
            self._check_block_at(nextblock)
            return
        blockno = self._pos // self._blocksize
        blockstart = blockno * self._blocksize
        if self._pos > blockstart + self._blockdatasize:
            blockstart += self._blocksize
            self._pos = blockstart
            if self._pos >= len(self._data):
                raise StopIteration()
        self._check_block_at(blockstart)

    def _check_block_at(self, start):
        sumstart = start + self._blockdatasize
        blocksum = self._blocksum(self._data[start:sumstart]).digest()
        if blocksum != self._data[sumstart:sumstart + self._blocksumsize]:
            raise InvalidDataError('Block checksum failed at ' + str(start))
        self._current_block = start
        self._current_block_end = sumstart

    def _next_item_no_data(self):
        raise InvalidDataError('Data not recognized at ' + str(self._pos))

    def _next_item_for_backup(self):
        if self._data[self._pos] == 0x90:
            dirid, pos = valuecodecs.parse_varuint(self._data, self._pos+1)
            parent, pos = valuecodecs.parse_varuint(self._data, pos)
            namelen, pos = valuecodecs.parse_varuint(self._data, pos)
            name = self._data[pos:pos+namelen]
            self._pos = pos+namelen
            if self._pos >= self._current_block_end:
                raise InvalidDataError(
                    'Directory item overran block end at ' + str(self._pos))
            return ItemDirectory(dirid, parent, name)
        elif self._data[self._pos] == 0x91:
            item = Item('file')
            item.parent, pos = valuecodecs.parse_varuint(
                self._data, self._pos+1)
            namelen, pos = valuecodecs.parse_varuint(self._data, pos)
            item.name = self._data[pos:pos+namelen]
            pos += namelen
            cidlen, pos = valuecodecs.parse_varuint(self._data, pos)
            item.cid = self._data[pos:pos+cidlen]
            pos += cidlen
            item.size, pos = valuecodecs.parse_varuint(self._data, pos)
            item.mtime_year = self._data[pos] + self._data[pos+1] * 256
            item.mtime_second = (
                self._data[pos+2] + self._data[pos+3] * 0x100 +
                self._data[pos+4] * 0x10000)
            if self._data[pos+5] >= 0x80:
                item.mtime_second += 0x1000000
            item.mtime_ns = (
                (self._data[pos+5] & 0x3f) + self._data[pos+6] * 0x40 +
                self._data[pos+7] * 0x4000 + self._data[pos+8] * 0x400000)
            self._pos = pos + 9
            if self._pos >= self._current_block_end:
                raise InvalidDataError(
                    'File item overran block end at ' + str(self._pos))
            return item
        else:
            raise InvalidDataError('Data not recognized at ' + str(self._pos))

    def _next_item_for_content(self):
        if self._data[self._pos] == 0xdd:
            item = Item('content')
            cidlen, pos = valuecodecs.parse_varuint(self._data, self._pos+1)
            sumlen, pos = valuecodecs.parse_varuint(self._data, pos)
            item.cid = self._data[pos:pos+cidlen]
            item.checksum = self._data[pos:pos+sumlen]
            pos += max(cidlen, sumlen)
            item.first = (
                self._data[pos] + self._data[pos+1] * 0x100 +
                self._data[pos+2] * 0x10000 + self._data[pos+3] * 0x1000000)
            pos += 4
            item.last = (
                self._data[pos] + self._data[pos+1] * 0x100 +
                self._data[pos+2] * 0x10000 + self._data[pos+3] * 0x1000000)
            pos += 4
            item.updates = []
            if pos >= self._current_block_end:
                raise InvalidDataError(
                    'Content item overran block end at ' + str(pos))
            while self._data[pos] in (0xa0, 0xa1):
                pos += 1
                if self._data[pos-1] == 0xa1:
                    update = Item('changed')
                    update.checksum = self._data[pos:pos+sumlen]
                    pos += sumlen
                else:
                    update = Item('restored')
                    update.checksum = None
                item.updates.append(update)
                update.first = (
                    self._data[pos] + self._data[pos+1] * 0x100 +
                    self._data[pos+2] * 0x10000 + self._data[pos+3] * 0x1000000)
                pos += 4
                update.last = (
                    self._data[pos] + self._data[pos+1] * 0x100 +
                    self._data[pos+2] * 0x10000 + self._data[pos+3] * 0x1000000)
                pos += 4
            self._pos = pos
            if self._pos >= self._current_block_end:
                raise InvalidDataError(
                    'Content item overran block end at ' + str(self._pos))
            return item
        else:
            raise InvalidDataError('Data not recognized at ' + str(self._pos))
