#!/usr/bin/env python3

import re

from config_subtree import CfgSubtree

class InvalidDataError(Exception): pass

def parse_full_path(fullpath):
    if fullpath.startswith('local:'):
        accessor = 'local'
        localpath = fullpath[6:]
    else:
        raise InvalidDataError('Unknown path specification: ' + fullpath)
    path = parse_relative_path(localpath)
    return accessor, path

def parse_relative_path(path):
    return tuple(x for x in path.split('/') if x)


class Config(object):
    def __init__(self):
        self.backups = []

    def read_file(self, tree, path):
        try:
            f = tree.get_item_at_path(path)
        except FileNotFoundError:
            return
        with f:
            data = f.get_data_slice(0, f.get_size())
        self._read_config_data(data.decode('utf-8'))

    def _read_config_data(self, data):
        if data[-1] != '\n':
            data = data + '\n'
        data = ConfigData(data)
        data.build_config(self)

    def parse_enter_block(self, key, args):
        if key == 'backup':
            for oldbackup in self.backups:
                if args == oldbackup.name:
                    raise InvalidDataError(
                        'Backup named "' + backup.name +
                        '" defined twice in config data')
            return CfgBackup(args)

    def parse_exit_block(self, key, args, item):
        if key == 'backup':
            self.backups.append(item)

    def get_backup_by_name(self, name):
        for backup in self.backups:
            if backup.name == name:
                return backup

class ConfigData(object):
    def __init__(self, data):
        self.data = data
        self.pos = 0
        self.prev_indent = ''
        self.indent = ''
        self.key = ''
        self.args = ''
        self.next()

    def build_config(self, builder):
        myindent = self.indent
        while True:
            assert myindent == self.indent
            key = self.key
            args = self.args
            item = builder.parse_enter_block(key, args)
            if not item:
                raise InvalidDataError('Unknown block: ' + self.linedata)
            self.next()
            if (self.indent is not None and
                    self.indent != myindent and
                    self.indent.startswith(myindent)):
                if item is True:
                    raise InvalidDataError(
                        'Block "' + key +
                        '" does not take a child block, got: ' + self.linedata)
                self.build_config(item)
            if item is not True:
                builder.parse_exit_block(key, args, item)
            if self.indent is None:
                return
            if myindent != self.indent and self.indent.startswith(myindent):
                raise InvalidDataError('Indentation mismatch: ' + self.linedata)
            if myindent != self.indent:
                return

    re_config_line = re.compile(r'( *)([^ \n]*) *([^\n]*)\n')
    def next(self):
        match = None
        while not match:
            if self.pos >= len(self.data):
                self.indent = None
                self.key = None
                self.args = None
                return
            match = self.re_config_line.match(self.data, self.pos)
            self.linedata = match.group()[:-1]
            if not match.group().strip():
                self.pos = match.end()
                match = None
        indent = match.group(1)
        if not (indent.startswith(self.prev_indent) or
                self.prev_indent.startswith(indent)):
            raise InvalidDataError(
                'Non-matching indentation. ' + repr(self.prev_indent) +
                ' followed by ' + repr(indent))
        self.indent = indent
        self.key = match.group(2)
        self.args = match.group(3)
        self.pos = match.end()



class CfgBackup(object):
    def __init__(self, name):
        self.name = name
        self.collections = []
        self.sources = []

    def parse_enter_block(self, key, args):
        if key == 'collection':
            accessor, path = parse_full_path(args)
            return CfgCollection(accessor, path)
        if key == 'source':
            accessor, path = parse_full_path(args)
            return CfgSource(accessor, path)

    def parse_exit_block(self, key, args, item):
        if key == 'collection':
            self.collections.append(item)
        if key == 'source':
            item.config_is_fully_parsed_so_finalize_data()
            self.sources.append(item)

class CfgCollection(object):
    def __init__(self, accessor, path):
        self.accessor = accessor
        self.path = path

    def parse_enter_block(self, key, args):
        return None

class CfgSource(object):
    def __init__(self, accessor, path):
        self.accessor = accessor
        self.path = path
        self.targetpath = ()
        self.tree = CfgTree(None, None)
        # self.subtree_handlers set by ..._finalize_data()

    def parse_enter_block(self, key, args):
        if key == 'targetpath':
            path = parse_relative_path(args)
            if self.targetpath is not ():
                raise InvalidDataError(
                    'Tried to set targetpath twice for the same source: ' +
                    str(self.targetpath) + ' and ' + str(path))
            self.targetpath = path
            return True
        if key in ('path', 'paths', 'path-glob', 'path-globs'):
            return self.tree.parse_pathmatch(key, args)

    def parse_exit_block(self, key, args, item):
        pass

    def config_is_fully_parsed_so_finalize_data(self):
        self.subtree_handlers = CfgSubtree(None, None)
        self.subtree_handlers.build_children_from_cfgtree(self.tree)
        self.tree = None

    def get_handler_for_path(self, path):
        handler = self.subtree_handlers.get_handler_for_path(path)
        if handler is None:
            return 'dynamic'
        return handler

class CfgTree(object):
    def __init__(self, matchtype, matchdata):
        self.matchtype = matchtype
        self.matchdata = matchdata
        self.children = []
        self.handler = None

    def parse_enter_block(self, key, args):
        if key == 'ignore' or key == 'dynamic' or key == 'static':
            if args != '':
                raise InvalidDataError(
                    'No argument allowed for path.' + key + ': ' +
                    data.linedata)
            if self.handler is not None:
                raise InvalidDataError(
                    'path handler set twice: ' + self.handler + ' and ' + key)
            self.handler = key
            return True
        if key in ('path', 'paths', 'path-glob', 'path-globs'):
            return self.parse_pathmatch(key, args)

    def parse_exit_block(self, key, args, item):
        pass

    def parse_pathmatch(self, key, args):
        if key.endswith('s'):
            elems = args.split()
        else:
            elems = (args,)
        paths = []
        for elem in elems:
            paths.append(parse_relative_path(elem))
        if key == 'path':
            tree = CfgTree('plain', paths[0])
            self.children.append(tree)
            return tree
        elif key == 'paths':
            tree = CfgTree('plain multi', paths)
            self.children.append(tree)
            return tree
        elif key == 'path-glob':
            tree = CfgTree('glob', paths[0])
            self.children.append(tree)
            return tree
        elif key == 'path-globs':
            tree = CfgTree('glob multi', paths)
            self.children.append(tree)
            return tree
        else:
            raise AssertionError('Unexpected key: ' + key)
