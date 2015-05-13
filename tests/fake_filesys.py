#!/usr/bin/env python3

import datetime

class ForbiddenActionError(Exception): pass

class FakeFileSystem(object):
    def __init__(self):
        self._paths = {}
        self._access = {}
        self._treeaccess = {}
        self._utcnow = datetime.datetime.utcnow

    def _set_utcnow(self, utcnow):
        self._utcnow = utcnow

    def _allow_full_access_to_subtree(self, path):
        self._treeaccess[path] = (
            'mkdir', 'create', 'stat', 'read', 'write', 'listdir', 'delete')

    def _allow_listing_subtree(self, path):
        self._allow_access_for_subtree(path, 'listdir')

    def _allow_access_for_subtree(self, path, access):
        if path not in self._treeaccess:
            self._treeaccess[path] = (access,)
        elif access not in self._treeaccess[path]:
            self._treeaccess[path] = self._treeaccess[path] + (access,)

    def _allow_reading_subtree(self, path):
        self._allow_access_for_subtree(path, 'listdir')
        self._allow_access_for_subtree(path, 'read')
        self._allow_access_for_subtree(path, 'stat')

    def _check_access(self, path, what):
        path_access = self._access.get(path)
        if path_access and what in path_access:
            return
        for tree in self._treeaccess:
            if path[:len(tree)] == tree and what in self._treeaccess[tree]:
                return
        raise ForbiddenActionError(
            'No ' + str(what) + ' access allowed for ' + str(path))

    def _is_cheap_copy(self, path1, path2):
        file1 = self._paths.get(path1)
        if file1 is None:
            return False
        file2 = self._paths.get(path2)
        if file1 != file2:
            return False
        assert not file1.is_directory
        return True

    def does_path_exist(self, path):
        self._check_access(path, 'stat')
        return path in self._paths

    def get_directory_listing(self, path=()):
        self._check_access(path, 'listdir')
        dirs = set()
        files = []
        for cand, item in self._paths.items():
            if cand[:-1] == path:
                if item.is_directory:
                    dirs.add(cand[-1])
                else:
                    files.append(cand[-1])
        return tuple(dirs), tuple(files)

    def create_directory(self, path):
        self._check_access(path, 'mkdir')
        if path in self._paths:
            raise FileExistsError('File already exists: ' + str(path))
        self._make_directory(path)

    def _make_directory(self, path):
        for i in range(1, len(path) + 1):
            item = self._paths.get(path[:i])
            if item and not item.is_directory:
                raise NotADirectoryError(
                    'File is not a directory: ' + str(path[:i]))
        for i in range(1, len(path) + 1):
            if path[:i] not in self._paths:
                self._paths[path[:i]] = DirectoryItem()

    def create_regular_file(self, path):
        self._check_access(path, 'create')
        if path in self._paths:
            raise FileExistsError('File already exists: ' + str(path))
        self._make_directory(path[:-1])
        fileitem = FileItem.make_empty(self)
        self._paths[path] = fileitem
        return FakeFile(self, path, fileitem)

    def create_temporary_file(self, path):
        self._check_access(path + ('tmpfile',), 'create')
        self._make_directory(path)
        counter = 0
        while path + ('tmpfile' + str(counter),) in self._paths:
            counter += 1
        use_path = path + ('tmpfile' + str(counter),)
        fileitem = FileItem.make_empty(self)
        self._paths[use_path] = fileitem
        return FakeTempFile(self, use_path, fileitem)

    def rename_and_overwrite(self, sourcepath, targetpath):
        self._check_access(targetpath, 'create')
        self._check_access(targetpath, 'delete')
        self._check_access(sourcepath, 'delete')
        source = self._paths.get(sourcepath)
        if not source:
            raise FileNotFoundError('No such file: ' + str(sourcepath))
        if source.is_directory:
            raise IsADirectory('File is a directory: ' + str(sourcepath))
        target = self._paths.get(targetpath)
        if target is not None and target.is_directory:
            raise IsADirectory('Target is a directory: ' + str(targetpath))
        self._paths[targetpath] = source
        del self._paths[sourcepath]

    def rename_without_overwrite(self, sourcepath, targetpath):
        self._check_access(targetpath, 'create')
        self._check_access(sourcepath, 'delete')
        source = self._paths.get(sourcepath)
        if not source:
            raise FileNotFoundError('No such file: ' + str(sourcepath))
        if source.is_directory:
            raise IsADirectory('File is a directory: ' + str(sourcepath))
        target = self._paths.get(targetpath)
        if target is not None:
            if target.is_directory:
                raise IsADirectory('Target is a directory: ' + str(targetpath))
            raise FileExistsError('Target exists: ' + str(targetpath))
        self._paths[targetpath] = source
        del self._paths[sourcepath]

    def make_cheap_copy(self, sourcepath, targetpath):
        self._check_access(sourcepath, 'read')
        self._check_access(targetpath, 'create')
        source = self._paths.get(sourcepath)
        if not source:
            raise FileNotFoundError('No such file: ' + str(sourcepath))
        if source.is_directory:
            raise IsADirectory('File is a directory: ' + str(sourcepath))
        target = self._paths.get(targetpath)
        if target is not None:
            if target.is_directory:
                raise IsADirectory('Target is a directory: ' + str(targetpath))
            raise FileExistsError('Target exists: ' + str(targetpath))
        self._paths[targetpath] = source

    def get_item_at_path(self, path):
        self._check_access(path, 'stat')
        item = self._paths.get(path)
        if item is None:
            raise FileNotFoundError('No such file: ' + str(path))
        if item.is_directory:
            raise NotImplementedError(
                'Not supporting get_item_at_path() for directories')
        return FakeFile(self, path, item)

    def _make_files(self, parent, names, fileid_first=None):
        self._make_directory(parent)
        fileid = fileid_first
        for name in names:
            path = parent + (name,)
            if path in self._paths:
                raise FileExistsError('File already exists: ' + str(path))
            if fileid is not None:
                item = FileItem.create_from_id(self, fileid)
                self._paths[path] = item
                fileid += 1
            else:
                raise NotImplementedError('No supported file creation method')

class FakeFile(object):
    def __init__(self, tree, path, item):
        self._tree = tree
        self._path = path
        self._item = item
        self._lock = 0 # 0: none, 1: read, 2: write

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def lock_for_writing(self):
        self._tree._check_access(self._path, 'write')
        assert self._lock == 0
        assert self._item.lock == 0
        self._item.lock = True
        self._lock = 2

    def lock_for_reading(self):
        self._tree._check_access(self._path, 'read')
        assert self._lock == 0
        assert self._item.lock is not True
        assert self._item.lock >= 0
        self._item.lock += 1
        self._lock = 1

    def get_size(self):
        self._tree._check_access(self._path, 'stat')
        return len(self._item.data)

    def get_mtime(self):
        self._tree._check_access(self._path, 'stat')
        return self._item.mtime, self._item.mtime_ns

    def get_data_slice(self, start, end):
        self._tree._check_access(self._path, 'read')
        return self._item.data[start:end]

    def write_data_slice(self, start, data):
        self._tree._check_access(self._path, 'write')
        # While it is allowed to start a write beyond the end of the
        # current data, I think it would be a bug if it actually
        # happens.
        assert start <= len(self._item.data)
        self._item.data = (
            self._item.data[:start] + data +
            self._item.data[start + len(data):])
        return start + len(data)

    def close(self):
        if self._lock == 1:
            assert self._item.lock > 0
            self._item.lock -= 1
            self._lock = 0
        elif self._lock == 2:
            assert self._item.lock is True
            self._item.lock = 0
            self._lock = 0
        assert self._lock == 0
        self._item = None

class FakeTempFile(FakeFile):
    def __init__(self, tree, path, item):
        FakeFile.__init__(self, tree, path, item)
        self._rename_path = None

    def rename_without_overwrite_on_close(self, tree, path):
        if self._tree != tree:
            raise AssertionError('Temporary file rename to different tree')
        self._rename_path = path

    def close(self):
        FakeFile.close(self)
        if self._rename_path is not None:
            self._tree.rename_without_overwrite(self._path, self._rename_path)

class DirectoryItem(object):
    def __init__(self):
        self.is_directory = True

class FileItem(object):
    def __init__(self):
        self.is_directory = False
        self.lock = 0 # 0: none, >0: number of read locks, True: write lock

    @staticmethod
    def make_empty(filesys):
        item = FileItem()
        item.data = b''
        item.mtime = filesys._utcnow()
        us = item.mtime.microsecond
        item.mtime_ns =  us * 1000 + us // 1000
        assert item.mtime_ns < 1000000000
        return item

    @staticmethod
    def create_from_id(filesys, fileid):
        item = FileItem()
        size = (fileid * fileid * 3889) % 6211
        data = str(fileid).encode('utf-8')
        data = data * (size // len(data))
        if len(data) < size:
            data += b'a' * (size - len(data))
        assert len(data) == size
        item = FileItem()
        item.data = data
        item.mtime = (
            datetime.datetime(2014, 2, 1) +
            datetime.timedelta((size * size) % 6211))
        item.mtime_ns = (
            item.mtime.microsecond * 1000 + (fileid * size * 311) % 1000)
        assert item.mtime_ns // 1000 == item.mtime.microsecond
        return item
