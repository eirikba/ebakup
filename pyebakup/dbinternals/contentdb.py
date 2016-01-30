#!/usr/bin/env python3

import collections
import datetime

import datafile

ContentData = collections.namedtuple(
    'ContentData', ('contentid', 'checksum', 'first_seen'))

class ContentInfoFile(object):
    def __init__(self, db):
        self._db = db
        self._dbfile = datafile.get_unopened_content(db._tree, db._path)
        self._read_file()

    def _read_file(self):
        self.contentdata = ContentInfoDict()
        f = self._dbfile
        f.open_and_lock_readonly()
        with f:
            item = next(f)
            if item.kind != 'magic':
                raise AssertionError('First item of content is not magic')
            if item.value != b'ebakup content data':
                raise AssertionError('Wrong magic in content file')
            for item in f:
                if item.kind == 'setting':
                    if item.key in (b'edb-blocksize', b'edb-blocksum'):
                        pass
                    else:
                        raise NotTestedError(
                            'Unknown setting: ' + str(item.key))
                elif item.kind == 'content':
                    self._add_content_item(item)
                else:
                    raise AssertionError(
                        'Unexpected item kind: ' + str(item.kind))

    def _add_content_item(self, item):
        if hasattr(item, 'updates'):
            self._logger.warn('deprecated', 'item.updates')
        if hasattr(item, 'last'):
            self._logger.warn('deprecated', 'item.last')
        self.contentdata[item.cid] = ContentData(
            item.cid,
            item.checksum,
            datetime.datetime.utcfromtimestamp(item.first))

    def get_all_content_infos_with_checksum(self, checksum):
        '''Return a sequence of ContentInfo objects for all the content items
        that have the "good" checksum 'checksum'.
        '''
        infos = []
        for cid in self.contentdata.get_contentids_for_checksum(checksum):
            infos.append(ContentInfo(self._db, self.contentdata[cid]))
        return infos

    def iterate_contentids(self):
        '''Iterates over all content ids in this database.
        '''
        for key in self.contentdata.keys():
            yield key

    def add_content_item(self, when, checksum):
        '''Add the given content item to the file and return its content id.
        '''
        # Make unique content id for 'checksum'
        current = set(
            x.get_contentid() for x in
            self.get_all_content_infos_with_checksum(checksum))
        contentid = checksum
        extra = b'\x00'
        while contentid in current:
            contentid = checksum + extra
            if extra[-1] == 255:
                extra += b'\x00'
            else:
                extra = extra[:-1] + bytes((extra[-1] + 1,))
        assert contentid.startswith(checksum)
        timestamp = int((when - datetime.datetime(1970, 1, 1)) /
                        datetime.timedelta(seconds=1))
        item = datafile.ItemContent(
            contentid,
            checksum,
            timestamp)
        self._dbfile.open_and_lock_readwrite()
        with self._dbfile:
            self._dbfile.append_item(item)
        self.contentdata[contentid] = ContentData(
            contentid, checksum, when)
        return contentid


class ContentInfoDict(object):
    def __init__(self):
        self._infos = {}
        self._checksums = {}

    def __getitem__(self, key):
        return self._infos[key]

    def __setitem__(self, key, value):
        cksum = value.checksum
        if cksum not in self._checksums:
            self._checksums[cksum] = [ key ]
        else:
            self._checksums[cksum].append(key)
        self._infos[key] = value

    def __contains__(self, key):
        return key in self._infos

    def get(self, key, default=None):
        return self._infos.get(key, default)

    def get_contentids_for_checksum(self, cksum):
        return self._checksums.get(cksum, ())

    def keys(self):
        return self._infos.keys()

    def values(self):
        return self._infos.values()

class ContentInfo(object):
    def __init__(self, db, data):
        self._db = db
        self._data = data

    def get_contentid(self):
        '''Return the content id of this content item.
        '''
        return self._data.contentid

    def get_good_checksum(self):
        '''Return the "good" checksum of this item.
        '''
        return self._data.checksum

    def get_first_seen_time(self):
        # IMPLEMENTME
        return self._data.first_seen
