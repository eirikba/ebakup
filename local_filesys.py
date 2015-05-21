#!/usr/bin/env python3

import datetime
import fcntl
import os
import tempfile

def path_to_stringpath(path):
    assert os.path.dirname('/') == '/'
    stringpath = os.path.join('/', *path)
    assert stringpath.startswith('/' + path[0])
    return stringpath

def stringpath_to_path(stringpath):
    assert os.path.join('home', '/') == '/'
    fullpath = os.path.abspath(os.path.realpath(stringpath))
    assert fullpath.startswith('/')
    path = tuple(x for x in fullpath.split('/') if x)
    assert fullpath == os.path.join('/', *path)
    return path

class LocalFileSystem(object):
    def get_item_at_path(self, path):
        stringpath = path_to_stringpath(path)
        if not os.path.exists(stringpath):
            raise NotTestedError()
            raise FileNotFoundError('No such file or directory: ' + stringpath)
        if os.path.isdir(stringpath):
            raise NotTestedError()
            raise IsADirectoryError('Path is a directory: ' + stringpath)
        return LocalFile(stringpath)

    def get_modifiable_item_at_path(self, path):
        stringpath = path_to_stringpath(path)
        if not os.path.exists(stringpath):
            raise NotTestedError()
            raise FileNotFoundError('No such file or directory: ' + stringpath)
        if os.path.isdir(stringpath):
            raise NotTestedError()
            raise IsADirectoryError('Path is a directory: ' + stringpath)
        return LocalFile(stringpath, writable=True)

    def is_accessible(self):
        return True

    def does_path_exist(self, path):
        return os.path.exists(path_to_stringpath(path))

    def get_directory_listing(self, path):
        stringpath = path_to_stringpath(path)
        names = os.listdir(stringpath)
        dirs = []
        files = []
        for name in names:
            if os.path.isdir(os.path.join(stringpath, name)):
                dirs.append(name)
            elif os.path.isfile(os.path.join(stringpath, name)):
                files.append(name)
            else:
                raise AssertionError('Neither file nor directory: ' + name)
        return dirs, files

    def create_directory(self, path):
        os.makedirs(path_to_stringpath(path))

    def create_regular_file(self, path):
        stringpath = path_to_stringpath(path)
        self._ensure_parent_directory_exists(stringpath)
        f = open(stringpath, 'xb')
        return LocalFile(stringpath, openfile=f, writable=True)

    def _ensure_parent_directory_exists(self, stringpath):
        parent = os.path.dirname(stringpath)
        os.makedirs(parent, exist_ok=True)

    def create_temporary_file(self, path):
        stringpath = path_to_stringpath(path)
        os.makedirs(stringpath, exist_ok=True)
        tmpfno, tmpname = tempfile.mkstemp(dir=stringpath)
        f = os.fdopen(tmpfno, 'w+b')
        return LocalTempFile(self, tmpname, openfile=f)

    def make_cheap_copy(self, source, target):
        sourcestringpath = path_to_stringpath(source)
        targetstringpath = path_to_stringpath(target)
        self._ensure_parent_directory_exists(targetstringpath)
        os.link(sourcestringpath, targetstringpath)

    def rename_and_overwrite(self, source, target):
        os.replace(path_to_stringpath(source), path_to_stringpath(target))

class LocalFile(object):
    def __init__(self, stringpath, openfile=None, writable=False):
        self._stringpath = stringpath
        self._openfile = openfile
        self._writable = writable

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def _open(self):
        if self._openfile is not None:
            return
        if self._writable:
            self._openfile = open(self._stringpath, 'r+b')
        else:
            self._openfile = open(self._stringpath, 'rb')

    def close(self):
        if self._openfile is not None:
            self._openfile.close()
            self._openfile = None

    def get_size(self):
        return os.path.getsize(self._stringpath)

    def get_mtime(self):
        s = os.stat(self._stringpath)
        mtime_ns = s.st_mtime_ns % 1000000000
        mtime = datetime.datetime.utcfromtimestamp(s.st_mtime_ns // 1000000000)
        mtime = mtime.replace(microsecond=mtime_ns//1000)
        assert mtime.microsecond == mtime_ns // 1000
        return mtime, mtime_ns

    def get_data_slice(self, start, end):
        self._open()
        assert end >= start
        self._openfile.seek(start)
        return self._openfile.read(end - start)

    def lock_for_reading(self):
        self._open()
        fcntl.lockf(self._openfile, fcntl.LOCK_SH)

    def lock_for_writing(self):
        assert self._writable
        self._open()
        fcntl.lockf(self._openfile, fcntl.LOCK_EX)

    def write_data_slice(self, start, data):
        assert self._writable
        self._open()
        self._openfile.seek(start)
        amt = self._openfile.write(data)
        self._openfile.flush()
        assert amt == len(data)
        return start + amt

class LocalTempFile(LocalFile):
    def __init__(self, tree, stringpath, openfile):
        LocalFile.__init__(self, stringpath, openfile=openfile, writable=True)
        self._tree = tree
        self._rename_path = None

    def rename_without_overwrite_on_close(self, targettree, targetpath):
        if targettree != self._tree:
            raise io.UnsupportedOperationError(
                'Can not rename between different trees')
        self._rename_path = targetpath

    def close(self):
        if self._openfile is None:
            return
        LocalFile.close(self)
        # Should really check that self._stringpath is this file!
        if self._rename_path is not None:
            targetstringpath = path_to_stringpath(self._rename_path)
            self._tree._ensure_parent_directory_exists(targetstringpath)
            checkfile = open(targetstringpath, 'xb')
            checkfile.close()
            os.replace(self._stringpath, targetstringpath)
        else:
            os.remove(self._stringpath)

