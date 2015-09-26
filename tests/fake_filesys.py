#!/usr/bin/env python3

import datetime
import io

class ForbiddenActionError(Exception): pass

class FakeFileSystem(object):
    def __init__(self):
        self._paths = {}
        self._access = {}
        self._treeaccess = {}
        self._utcnow = self._fallback_utcnow

    def _fallback_utcnow(self):
        return datetime.datetime(2015, 3, 3)

    def _set_utcnow(self, utcnow):
        self._utcnow = utcnow

    def _clear_all_access_rules(self):
        self._access = {}
        self._treeaccess = {}

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

    def _disallow_reading_subtree(self, path):
        self._allow_access_for_subtree(path, 'no-listdir')
        self._allow_access_for_subtree(path, 'no-read')
        self._allow_access_for_subtree(path, 'no-stat')

    def _allow_access_for_path(self, path, access):
        if path not in self._access:
            self._access[path] = (access,)
        elif access not in self._access[path]:
            self._access[path] = self._access[path] + (access,)

    def _allow_reading_path(self, path):
        self._allow_access_for_path(path, 'listdir')
        self._allow_access_for_path(path, 'read')
        self._allow_access_for_path(path, 'stat')

    def _is_access_allowed(self, path, what):
        path_access = self._access.get(path)
        if path_access:
            if 'no-' + what in path_access:
                return False
            if what in path_access:
                return True
        best = -1
        allowed = False
        for tree in self._treeaccess:
            if len(tree) < best:
                continue
            if path[:len(tree)] == tree:
                best = len(tree)
                if 'no-' + what in self._treeaccess[tree]:
                    allowed = False
                if what in self._treeaccess[tree]:
                    allowed = True
        return allowed

    def _check_access(self, path, what):
        if not self._is_access_allowed(path, what):
            raise ForbiddenActionError(
                'No ' + str(what) + ' access allowed for ' + str(path))
        self._check_permissions(path, what)

    _permissions_for_access = {
        'listdir': 'r',
        'read': 'r',
        'write': 'w',
        'create': None,
        'delete': None,
        'stat': None,
        }
    def _check_permissions(self, path, what):
        noexist = False
        for i in range(1, len(path)):
            if path[:i] not in self._paths:
                noexist = True
                continue
            assert not noexist
            if 'x' not in self._paths[path[:i]].perms:
                raise PermissionError(
                    'No traverse permission for ' + str(path[:i]) +
                    ' when accessing ' + str(path))
        if not noexist and what in ('create', 'delete'):
            if 'w' not in self._paths[path[:-1]].perms:
                raise PermissionError(
                    'No write permission for ' + str(path[:-1]) +
                    ' when performing a "' + what + '" operation on ' +
                    str(path))
        if path not in self._paths:
            return
        assert not noexist
        needs = self._permissions_for_access[what]
        if needs is None:
            return
        perms = self._paths[path].perms
        for need in needs:
            if need not in perms:
                raise PermissionError(
                    'No "' + need + '" permission for ' + str(path))

    def _is_cheap_copy(self, path1, path2):
        file1 = self._paths.get(path1)
        if file1 is None:
            return False
        file2 = self._paths.get(path2)
        if file1 != file2:
            return False
        assert not file1.is_directory
        return True

    def is_same_file_system_as(self, other):
        return other.path_to_full_string(()).startswith('local:/')

    def is_accessible(self):
        return True

    def path_to_string(self, path):
        return '/' + '/'.join(path)

    def path_from_string(self, stringpath):
        if not stringpath.startswith('/'):
            stringpath = '/current/working/directory/' + stringpath
        return tuple(x for x in stringpath.split('/') if x)

    def relative_path_from_string(self, stringpath):
        assert not stringpath.startswith('/')
        return tuple(x for x in stringpath.split('/') if x)

    def path_to_full_string(self, path):
        return 'local:' + self.path_to_string(path)

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
        fileitem = FileItem.make_empty_regular_file(self)
        self._paths[path] = fileitem
        f = FakeFile(self, path, fileitem)
        f._writable = True
        return f

    def create_temporary_file(self, path):
        self._check_access(path + ('tmpfile',), 'create')
        self._make_directory(path)
        counter = 0
        while path + ('tmpfile' + str(counter),) in self._paths:
            counter += 1
        use_path = path + ('tmpfile' + str(counter),)
        fileitem = FileItem.make_empty_regular_file(self)
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
        self._make_directory(targetpath[:-1])
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
        self._make_directory(targetpath[:-1])
        self._paths[targetpath] = source
        del self._paths[sourcepath]

    def delete_file_at_path(self, path):
        self._check_access(path, 'delete')
        target = self._paths.get(path)
        if target is None:
            return
        if target.is_directory:
            raise IsADirectory('Target is a directory: ' + str(targetpath))
        del self._paths[path]

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
        self._make_directory(targetpath[:-1])
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

    def get_modifiable_item_at_path(self, path):
        f = self.get_item_at_path(path)
        f._writable = True
        return f

    def _make_files(self, parent, names, fileid_first=None, filetype=None):
        self._make_directory(parent)
        fileid = fileid_first
        for name in names:
            path = parent + (name,)
            if path in self._paths:
                raise FileExistsError('File already exists: ' + str(path))
            if fileid is not None:
                assert filetype is None
                item = FileItem.create_from_id(self, fileid)
                self._paths[path] = item
                fileid += 1
            elif filetype == 'noinfo':
                self._paths[path] = FileItem()
            else:
                raise NotImplementedError('No supported file creation method')

    def _add_file(
            self, path, content=None, mtime=None, mtime_ns=None,
            filetype='file',
            perms=None, update=False):
        if not update and path in self._paths:
            raise FileExistsError('File already exists: ' + str(path))
        if update and path not in self._paths:
            raise FileNotFoundError('File does not exists: ' + str(path))
        self._make_directory(path[:-1])
        item = FileItem()
        if filetype not in ('file', 'symlink'):
            assert content is None
        if content is not None:
            assert isinstance(content, bytes)
            item.data = content
        if mtime is not None:
            assert isinstance(mtime, datetime.datetime)
            if mtime_ns is not None:
                if mtime.microsecond == 0:
                    mtime = mtime.replace(microsecond=mtime_ns//1000)
                assert mtime.microsecond == mtime_ns // 1000
            item.mtime = mtime
            if mtime_ns is None:
                mtime_ns = mtime.microsecond * 1000
        if mtime_ns is not None:
            assert mtime is not None
            item.mtime_ns = mtime_ns
        if perms is not None:
            item.perms = perms
        assert filetype in ('file', 'symlink', 'socket', 'pipe', 'device')
        item.filetype = filetype
        self._paths[path] = item
        return item

    def _add_symlink(
            self, path, target=None, mtime=None, mtime_ns=None,
            perms=None, update=False):
        assert isinstance(target, bytes)
        item = self._add_file(
            path=path, content=None, mtime=mtime, mtime_ns=mtime_ns,
            filetype='symlink',
            perms=perms, update=update)
        item.link_target = target

    def _add_directory(self, path, perms=None):
        if path in self._paths:
            raise FileExistsError('File already exists: ' + str(path))
        self._make_directory(path)
        if perms is not None:
            self._paths[path].perms = perms

    def get_config_paths_for(self, application):
        return (
            ('home', 'me', '.config', application),
            ('etc', 'xdg', application))

class FakeFile(object):
    def __init__(self, tree, path, item):
        self._tree = tree
        self._path = path
        self._item = item
        self._lock = 0 # 0: none, 1: read, 2: write
        self._writable = False

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def drop_all_cached_data(self):
        pass

    def lock_for_writing(self):
        if not self._writable:
            raise io.UnsupportedOperationError('write lock')
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

    def get_filetype(self):
        ft = self._item.filetype
        assert ft in ('file', 'socket', 'pipe', 'symlink', 'device', 'other')
        return ft

    def get_size(self):
        self._tree._check_access(self._path, 'stat')
        if self._item.filetype == 'file':
            return len(self._item.data)
        if self._item.filetype in ('socket', 'pipe'):
            return 0
        raise NotImplementedError()

    def get_mtime(self):
        self._tree._check_access(self._path, 'stat')
        assert self._item.filetype != 'symlink'
        return self._item.mtime, self._item.mtime_ns

    def get_link_mtime(self):
        self._tree._check_access(self._path, 'stat')
        return self._item.mtime, self._item.mtime_ns

    def readsymlink(self):
        self._tree._check_access(self._path, 'read')
        assert self._item.filetype == 'symlink'
        return self._item.link_target

    def get_data_slice(self, start, end):
        self._tree._check_access(self._path, 'read')
        assert self._item.filetype == 'file'
        return self._item.data[start:end]

    def write_data_slice(self, start, data):
        if not self._writable:
            raise io.UnsupportedOperationError('write')
        self._tree._check_access(self._path, 'write')
        assert self._item.filetype == 'file'
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

class FakeTempFile(FakeFile):
    def __init__(self, tree, path, item):
        FakeFile.__init__(self, tree, path, item)
        self._writable = True
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
        self.perms = 'rwx'

class FileItem(object):
    def __init__(self):
        self.is_directory = False
        self.lock = 0 # 0: none, >0: number of read locks, True: write lock
        self.perms = 'rwx'

    @staticmethod
    def make_empty_regular_file(filesys):
        item = FileItem()
        item.filetype = 'file'
        item.data = b''
        item.mtime = filesys._utcnow()
        us = item.mtime.microsecond
        item.mtime_ns =  us * 1000 + us // 1000
        assert item.mtime_ns < 1000000000
        return item

    @staticmethod
    def create_from_id(filesys, fileid):
        size = (fileid * fileid * 3889) % 6211
        data = str(fileid).encode('utf-8')
        data = data * (size // len(data))
        if len(data) < size:
            data += b'a' * (size - len(data))
        assert len(data) == size
        item = FileItem()
        item.filetype = 'file'
        item.data = data
        item.mtime = (
            filesys._utcnow() -
            datetime.timedelta((size * size) % 6211))
        item.mtime_ns = (
            item.mtime.microsecond * 1000 + (fileid * size * 311) % 1000)
        assert item.mtime_ns // 1000 == item.mtime.microsecond
        return item
