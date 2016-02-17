#!/usr/bin/env python3

import os

import datafile
import filesys

def hexify(data):
    return ''.join('{:02x}'.format(x) for x in data)

class ContentReader(object):
    def __init__(self, bkpath):
        self._cids = {}
        self._read_file(bkpath)

    def get_content_info(self, cid):
        return self._cids[cid]

    def get_path(self, cid):
        hexcid = hexify(cid)
        return os.path.join('content', hexcid[:2], hexcid[2:4], hexcid[4:])

    def _read_file(self, bkpath):
        fs = filesys.get_file_system('local')
        dbpath = fs.path_from_string(os.path.join(bkpath, 'db'))
        with datafile.open_content(fs, dbpath) as df:
            self._read_datafile(df)

    def _read_datafile(self, df):
        for item in df:
            if item.kind in ('magic', 'setting'):
                pass
            elif item.kind == 'content':
                self._add_content(item)
            else:
                raise NotImplementedError('Unknown item type: ' + item.kind)

    def _add_content(self, item):
        assert item.cid not in self._cids
        self._cids[item.cid] = item
