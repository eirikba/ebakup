#!/usr/bin/env python3

import os

import pyebakup.database.datafile as datafile
import pyebakup.filesys as filesys

class BackupReader(object):
    def __init__(self, bkpath, bkname):
        self._dirpaths = { 0: b'' }
        self._dirs = {}
        self._files = {}
        self._read_file(bkpath, bkname)

    def get_file_info(self, path):
        if isinstance(path, str):
            path = path.encode('utf-8', errors='surrogateescape')
        return self._files[path]

    def list_directory(self, path):
        if path == '':
            dirid = 0
        elif path not in self._dirs:
            return []
        else:
            dirid = self._dirs[path].dirid
        dirs = []
        for d in self._dirs.values():
            if d.parent == dirid:
                dirs.append(d.name)
        files = []
        for f in self._files.values():
            if f.parent == dirid:
                files.append(f.name)
        return dirs, files

    def iterate_files(self):
        for path in self._files:
            yield path

    def _read_file(self, bkpath, bkname):
        fs = filesys.get_file_system('local')
        dbpath = fs.path_from_string(os.path.join(bkpath, 'db'))
        with datafile.open_backup_by_name(fs, dbpath, bkname) as df:
            self._read_datafile(df)

    def _read_datafile(self, df):
        for item in df:
            if item.kind == 'file':
                self._add_file(item)
            elif item.kind == 'directory':
                self._add_dir(item)
            elif item.kind in ('magic', 'setting','key-value', 'extradef'):
                # ignore for now
                pass
            else:
                raise NotImplementedError('Unknown item type: ' + item.kind)

    def _add_file(self, item):
        path = os.path.join(self._dirpaths[item.parent], item.name)
        assert path not in self._files
        self._files[path] = item

    def _add_dir(self, item):
        path = os.path.join(self._dirpaths[item.parent], item.name)
        assert path not in self._dirs
        self._dirs[path] = item
        self._dirpaths[item.dirid] = path
