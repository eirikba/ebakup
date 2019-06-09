#!/usr/bin/env python3

import re


re_keyvalue = re.compile(r'^\s*\((\d+)\)(.*)$')
re_extradef = re.compile(r'^\s*\((\d+)\)\[(.*)\]$')
class DumpReader(object):
    def __init__(self, dump):
        self.dump = dump
        self.filters = []
        self.handler = None
        self.kvids = {}
        self.xids = {}
        self.dirs = {}

    def add_filter(self, f):
        self.filters.append(f)

    def set_handler(self, handler):
        self.handler = handler

    def run(self):
        item = None
        with open(self.dump, 'r', errors='replace') as f:
            for line in f:
                key, value = line.split(':', 1)
                if key == 'event':
                    pass
                elif key == 'type':
                    pass
                elif key == 'setting':
                    pass
                elif key == 'key-value':
                    self._register_kv(value)
                elif key == 'extradef':
                    self._register_extra(value)
                elif key == 'file':
                    self._handle_item(item)
                    item = File(value)
                elif key == 'dir':
                    self._handle_item(item)
                    item = Directory(value)
                elif key == 'file type':
                    item.set_file_type(value)
                elif key == 'cid':
                    item.set_cid(value)
                elif key == 'size':
                    item.set_size(value)
                elif key == 'mtime':
                    item.set_mtime(value)
                elif key == 'extra':
                    item.set_extra(value)
                else:
                    raise AssertionError('Unknown key: ' + key)
            self._handle_item(item)


    def _register_kv(self, kv):
        match = re_keyvalue.match(kv)
        if not match:
            raise AssertionError('Failed to match key-value: ' + kv)
        kvid = match.group(1)
        kvval = match.group(2)
        assert kvid not in self.kvids
        self.kvids[kvid] = kvval


    def _register_extra(self, extra):
        match = re_extradef.match(extra)
        if not match:
            raise AssertionError('Failed to match extradef: ' + extra)
        xid = match.group(1)
        xval = match.group(2)
        assert xid not in self.xids
        self.xids[xid] = [x.strip() for x in xval.split(',')]


    def _handle_item(self, item):
        if item is None:
            return
        if isinstance(item, Directory):
            assert item.dirid not in self.dirs
            self.dirs[item.dirid] = item
        elif isinstance(item, File):
            for flt in self.filters:
                if not flt(item):
                    return
            if self.handler is not None:
                self.handler(item)
        else:
            raise AssertionError('Unknown item type: ' + str(item))


    def path_of_item(self, item):
        path = [item]
        parent = self.dirs.get(item.parent)
        while parent:
            path.append(parent)
            parent = self.dirs.get(parent.parent)
        if path[-1].parent != 0:
            path.append('???' + str(path[-1].parent) + '???')
        path.reverse()
        path = [ x.name for x in path ]
        return '/'.join(path)


re_file = re.compile(r'^\s*\((\d+)\)(.*)$')
class File(object):
    def __init__(self, parent_and_name):
        match = re_file.match(parent_and_name)
        if not match:
            raise AssertionError(
                'Failed to parse file component: ' + parent_and_name)
        self.parent = int(match.group(1))
        self.name = match.group(2)


    def set_cid(self, cid):
        self.cid = cid.strip()


    def set_size(self, size):
        self.size = int(size)


    def set_mtime(self, mtime):
        self.mtime = mtime.strip()


    def set_extra(self, extra):
        self.extra = int(extra)


    def set_file_type(self, t):
        self.file_type = t


re_dir = re.compile(r'^\s*\((\d+)-(\d+)\)(.*)$')
class Directory(object):
    def __init__(self, parent_and_name):
        match = re_dir.match(parent_and_name)
        if not match:
            raise AssertionError(
                'Failed to parse file component: ' + parent_and_name)
        self.parent = int(match.group(1))
        self.dirid = int(match.group(2))
        self.name = match.group(3)


    def set_extra(self, extra):
        self.extra = int(extra)
