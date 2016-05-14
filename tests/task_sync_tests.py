#!/usr/bin/env python3

import datetime
import hashlib
import re
import unittest

import cli.task_sync as task_sync
import testdata

hexits = '0123456789abcdef'
def hexstr(data):
    out = ''
    for x in data:
        out += hexits[x>>4] + hexits[x&15]
    return out

def unhexify(data):
    out = []
    assert len(data) % 2 == 0
    for i in range(0, len(data), 2):
        out.append(int(data[i:i+2], 16))
    return bytes(out)

class FakeCollection(object):
    def __init__(self, path):
        self._path = path
        self._immutable = False
        self._content_readable = True
        self._backups = []
        self._content = {}

    def set_immutable(self, immutable=True):
        self._immutable = immutable

    def expect_no_reading_content(self):
        self._content_readable = False

    def allow_reading_content(self):
        self._content_readable = True

    def get_all_backup_names(self, order_by=None):
        names = ['-'.join(x._path) for x in self._backups]
        if order_by == 'starttime':
            names.sort()
        elif order_by is None:
            pass
        else:
            raise AssertionError('Unexpected order_by: ' + str(order_by))
        return names

    def _add_backup(self, when, files):
        bk = FakeBackup(when)
        bk._items.append(
            FakeItem(
                'setting',
                key=b'end',
                value=time_to_setting(
                    when + datetime.timedelta(seconds=3))))
        for f in files:
            cid = self._add_content_data(f.content)
            bkf = FakeItem(
                'file',
                path=f.path,
                mtime=f.mtime,
                mtime_ns=f.mtime_ns,
                size=len(f.content),
                contentid=cid)
            bk._add_file(bkf)
        if self._immutable:
            raise AssertionError('Collection is immutable')
        self._backups.append(bk)
        return bk

    def add_content(self, source):
        content = source._get_full_content()
        return self._add_content_data(content)

    def _add_content_data(self, content):
        cksum = hashlib.md5(content).digest()
        cid = cksum
        if not self._content_readable:
            raise AssertionError('Content is not expected to be read')
        oldcontent = self._content.get(cid)
        disambig = 1
        while oldcontent is not None and oldcontent != content:
            cid = cksum + str(disambig).encode('utf-8')
            oldcontent = self._content.get(cid)
        if self._immutable:
            raise AssertionError('Collection is immutable')
        self._content[cid] = content
        return cid

    def _get_content_by_id(self, cid):
        if not self._content_readable:
            raise AssertionError('Content is not expected to be read')
        return self._content.get(cid)

    def get_backup_file_reader_for_name(self, name):
        for x in self._backups:
            if '-'.join(x._path) == name:
                return FakeStreamingBackupReader(x)
        raise AssertionError('No backup named ' + name)

    def create_backup_file_in_replacement_mode(self, starttime):
        return FakeStreamingBackupWriter(self, starttime)

    def _make_path_from_contentid(self, cid):
        hexcid = hexstr(cid)
        return self._path + ('content', hexcid[:2], hexcid[2:4], hexcid[4:])

    def _does_path_exist(self, path):
        if len(path) == 0:
            return True
        if path in ( ('content',), ('db') ):
            return True
        if len(path) == 4 and path[0] == 'content':
            if len(path[1]) == 2 and len(path[2]) == 2:
                cid = unhexify(path[1] + path[2] + path[3])
                return cid in self._content
            return False
        raise NotImplementedError()

    def _get_file_item_at_path(self, path):
        if len(path) == 4 and path[0] == 'content':
            if len(path[1]) == 2 and len(path[2]) == 2:
                cid = unhexify(path[1] + path[2] + path[3])
                if self._content_readable:
                    content = self._content.get(cid)
                else:
                    content = False
                if content is not None:
                    return FakeFile(content=content)
            raise FileNotFoundError()
        raise NotImplementedError()

def time_to_setting(dt):
    return '{:04}-{:02}-{:02}T{:02}:{:02}:{:02}'.format(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second).encode(
            'utf-8')
class FakeBackup(object):
    def __init__(self, start):
        self._next_dirid = 8
        self._dirs = {}
        if start is None:
            self._start_time = None
            self._items = []
            return
        self._start_time = start.replace(microsecond=0)
        self._items = [
            FakeItem('magic', value=b'ebakup backup data'),
            FakeItem('setting', key=b'edb-blocksize', value=b'4096'),
            FakeItem('setting', key=b'edb-blocksum', value=b'sha256'),
            FakeItem(
                'setting',
                key=b'start',
                value=time_to_setting(self._start_time)),
        ]

    @property
    def _path(self):
        return (
            str(self._start_time.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                self._start_time.month, self._start_time.day,
                self._start_time.hour, self._start_time.minute))

    def _add_file(self, file):
        parent = self._get_dirid(file.path[:-1])
        self._items.append(
            FakeItem(
                'file',
                parent=parent,
                name=file.path[-1],
                cid=file.contentid,
                size=file.size,
                mtime=file.mtime,
                mtime_ns=file.mtime_ns))

    def _get_file_data(self, path):
        for item in self._items:
            if item.kind not in ('file', 'directory'):
                continue
            if self._get_item_path(item) == path:
                return item

    def _get_item_path(self, item):
        return self._get_dirpath(item.parent) + (item.name,)

    def _get_dirid(self, path):
        if len(path) == 0:
            return 0
        dirid = self._dirs.get(path)
        if dirid:
            return dirid
        parent = self._get_dirid(path[:-1])
        dirid = self._next_dirid
        self._next_dirid += 1
        self._dirs[dirid] = path
        self._items.append(
            FakeItem(
                'directory',
                parent=parent,
                dirid=dirid,
                name=path[-1]))
        return dirid

    def _get_dirpath(self, dirid):
        if dirid == 0:
            return ()
        return self._dirs[dirid]

    def _get_all_file_paths(self):
        return [
            self._get_dirpath(x.parent) + (x.name,)
            for x in self._items if x.kind == 'file' ]

    def _get_file_info(self, path):
        for item in self._items:
            if (item.kind == 'file' and
                self._get_dirpath(item.parent) + (item.name,) == path):
                return item
        return None


class FakeItem(object):
    def __init__(self, kind, **kwargs):
        self.kind = kind
        for key, value in kwargs.items():
            setattr(self, key, value)

class FakeStreamingBackupReader(object):
    def __init__(self, backup):
        self._backup = backup
        self._iterator = self._iterate_items()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iterator)

    def _iterate_items(self):
        for item in self._backup._items:
            yield item

    @property
    def _tree(self):
        # Yes, my code is using this. Bad code!
        return FakeTreeForHackyBackupReaderAccess(self)

    @property
    def _path(self):
        # Yes, my code is using this. Bad code!
        return self

class FakeTreeForHackyBackupReaderAccess(object):
    def __init__(self, reader):
        self._reader = reader

    def get_item_at_path(self, path):
        assert path == self._reader
        return FakeFileForHackyBackupReaderAccess(self._reader)

class FakeFileForHackyBackupReaderAccess(object):
    def __init__(self, reader):
        data = []
        for item in reader._backup._items:
            data.append(item.kind.encode('utf-8'))
            for attr in (
                    'key', 'value', 'parent', 'dirid', 'name',
                    'cid', 'size', 'mtime', 'mtime_ns'):
                if hasattr(item, attr):
                    data.append(repr(getattr(item, attr)).encode('utf-8'))
        self._data = b''.join(data)

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self._data = None

    def get_size(self):
        return len(self._data)

    def get_data_slice(self, start, end):
        assert start == 0
        assert end == len(self._data)
        return self._data


class FakeStreamingBackupWriter(object):
    def __init__(self, collection, starttime):
        self._collection = collection
        self._starttime = starttime
        self._items = []
        self._backup = FakeBackup(starttime)
        self._closed = False

    _re_datetime = re.compile(
        rb'^(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)$')
    def append_item(self, item):
        assert not self._closed
        if item.kind == 'magic':
            assert item.value == b'ebakup backup data'
            self._backup._items.append(item)
        elif item.kind == 'setting':
            self._backup._items.append(item)
            if item.key == b'start':
                assert self._backup._start_time is None
                assert self._name.encode('utf-8') == item.value[:-3]
                match = self._re_datetime.match(item.value)
                assert match
                self._backup._start_time = datetime.datetime(
                    int(match.group(1)), int(match.group(2)),
                    int(match.group(3)), int(match.group(4)),
                    int(match.group(5)), int(match.group(6)))
        elif item.kind == 'directory':
            # There is no requirement for backups that the parent
            # already exists, but the test data should have that.
            if item.parent == 0:
                parent_path = ()
            else:
                parent_path = self._backup._dirs[item.parent]
            self._backup._dirs[item.dirid] = parent_path + (item.name,)
            self._backup._items.append(item)
        elif item.kind == 'file':
            self._backup._items.append(item)
        else:
            raise NotImplementedError()

    def commit_and_close(self):
        self._closed = True
        if self._collection._immutable:
            raise AssertionError('Collection is immutable')
        self._collection._backups.append(self._backup)

class FakeTree(object):
    def __init__(self):
        self._collections = {}

    def _add_collection(self, coll):
        for collpath in self._collections:
            assert coll._path[:len(collpath)] != collpath
            assert collpath[:len(coll._path)] != coll._path
        self._collections[coll._path] = coll

    @staticmethod
    def _get_collection(tree, path, services):
        return tree._collections[path]

    def is_accessible(self):
        return True

    def path_to_full_string(self, path):
        return 'fake:' + str(path)

    def does_path_exist(self, path):
        for collpath in self._collections:
            if path[:len(collpath)] == collpath:
                return self._collections[collpath]._does_path_exist(
                    path[len(collpath):])

    def get_item_at_path(self, path):
        for collpath in self._collections:
            if path[:len(collpath)] == collpath:
                return self._collections[collpath]._get_file_item_at_path(
                    path[len(collpath):])
        if not self.does_path_exist(self, path):
            raise FileNotFoundError()
        raise NotImplementedError()

class FakeFile(object):
    def __init__(self, content):
        if content is not False:
            assert isinstance(content, bytes)
        self._content = content

    def _get_full_content(self):
        if self._content is False:
            raise AssertionError('Content should not be read')
        return self._content

class FakeConfig(object): pass

class Conf(object):
    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            setattr(self, name, value)

class FakeArgs(object):
    def __init__(self):
        self.services = {
            'logger': FakeLogger(),
            'uistate': FakeUIState(),
        }
        self.create = False

class FakeLogger(object):
    def log_notice(self, what, which, comment=''):
        pass

class FakeUIState(object):
    def set_status(self, key, value):
        pass

class TestSync(unittest.TestCase):
    def setUp(self):
        coll = FakeCollection(('backup', 'first'))
        coll._add_backup(
            datetime.datetime(2014, 6, 10, 14, 16, 7, 30092),
            files=testdata.files()[:16])
        coll.set_immutable()
        coll2 = FakeCollection(('backup', 'second'))
        tree = FakeTree()
        tree._add_collection(coll)
        tree._add_collection(coll2)
        config = FakeConfig()
        config.backups = [
            Conf(
                name='test',
                collections=(
                    Conf(filesystem=tree, path=('backup', 'first')),
                    Conf(filesystem=tree, path=('backup', 'second'))
                )) ]
        args = FakeArgs()
        args.services['backupcollection.open'] = tree._get_collection
        args.backups = []
        self.coll = coll
        self.coll2 = coll2
        self.tree = tree
        self.config = config
        self.args = args

    def test_simple_initial_sync(self):
        sync = task_sync.SyncTask(self.config, self.args)
        sync.execute()
        coll = self.coll
        coll2 = self.coll2
        self.assertCollectionsEqual(coll, coll2)
        # The rest of these tests are in one sense not necessary,
        # given that the collections have been tested equal. But it
        # tests some values of the new data with the original data
        # rather than the source collection, which is potentially a
        # useful extra sanity check.
        self.assertEqual(1, len(coll2._backups))
        bk = coll2._backups[0]
        self.assertEqual(
            datetime.datetime(2014, 6, 10, 14, 16, 7),
            bk._start_time)
        for testfile in testdata.files()[:16]:
            info = bk._get_file_info(testfile.path)
            self.assertNotEqual(None, info, msg='file: ' + str(testfile.path))
            content = coll2._content[info.cid]
            self.assertEqual(
                content, testfile.content, msg='file: ' + str(testfile.path))
            self.assertEqual(
                info.size, len(testfile.content),
                msg='file: ' + str(testfile.path))
            self.assertEqual(
                info.mtime, testfile.mtime, msg='file: ' + str(testfile.path))
            self.assertEqual(
                info.mtime_ns, testfile.mtime_ns,
                msg='file: ' + str(testfile.path))
        testfiles = {}
        for testfile in testdata.files()[:16]:
            testfiles[testfile.path] = testfile
        bkpaths = bk._get_all_file_paths()
        self.assertCountEqual(bkpaths, testfiles.keys())
        for path in bkpaths:
            bkfile = bk._get_file_data(path)
            self.assertEqual(
                coll2._get_content_by_id(bkfile.cid),
                testfiles[path].content)

    def assertCollectionsEqual(self, coll1, coll2):
        self.assertEqual(coll1._content, coll2._content)
        self.assertEqual(len(coll1._backups), len(coll2._backups))
        for bk1, bk2 in zip(coll1._backups, coll2._backups):
            self.assertEqual(bk1._start_time, bk2._start_time)
            self.assertEqual(bk1._dirs, bk2._dirs)
            self.assertEqual(len(bk1._items), len(bk2._items))
            for item1, item2 in zip(bk1._items, bk2._items):
                self.assertEqual(dir(item1), dir(item2))
                for attr in dir(item1):
                    if not attr.startswith('_'):
                        self.assertEqual(
                            getattr(item1, attr), getattr(item2, attr))

    def test_sync_with_common_backups(self):
        self.coll2._add_backup(
            datetime.datetime(2014, 6, 10, 14, 16, 7, 30092),
            files=testdata.files()[:16])
        sync = task_sync.SyncTask(self.config, self.args)
        sync.execute()
        self.assertCollectionsEqual(self.coll, self.coll2)

    def test_sync_with_new_backup_and_existing_content(self):
        self.coll2._add_backup(
            datetime.datetime(2014, 6, 10, 14, 16, 7, 30092),
            files=testdata.files()[:16])
        contentcount = len(self.coll._content)
        self.coll.set_immutable(False)
        self.coll._add_backup(
            datetime.datetime(2014, 6, 18, 4, 16, 24, 437668),
            files=testdata.files()[:16])
        self.coll.set_immutable()
        self.coll.expect_no_reading_content()
        self.assertEqual(contentcount, len(self.coll._content))
        sync = task_sync.SyncTask(self.config, self.args)
        sync.execute()
        self.assertCollectionsEqual(self.coll, self.coll2)

    def test_initial_sync_with_two_backups(self):
        contentcount = len(self.coll._content)
        self.coll.set_immutable(False)
        self.coll._add_backup(
            datetime.datetime(2014, 6, 18, 4, 16, 24, 437668),
            files=testdata.files()[:16])
        self.coll.set_immutable()
        self.assertEqual(contentcount, len(self.coll._content))
        sync = task_sync.SyncTask(self.config, self.args)
        sync.execute()
        self.assertCollectionsEqual(self.coll, self.coll2)
