#!/usr/bin/env python3

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
    assert isinstance(key, bytes)
    assert isinstance(value, bytes)
    item.key = key
    item.value = value
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
    def __init__(self, cid, checksum, first):
        self.kind = 'content'
        self.cid = cid
        self.checksum = checksum
        self.first = first

