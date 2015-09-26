#!/usr/bin/env python3

import datetime
import fcntl
import os
import stat
import tempfile

class LocalFileSystem(object):
    def is_same_file_system_as(self, tree):
        return tree.path_to_full_string(()) == 'local:/'

    def path_to_string(self, path):
        assert os.path.dirname('/') == '/'
        stringpath = os.path.join('/', *path)
        if path:
            assert stringpath.startswith('/' + path[0])
        else:
            assert stringpath == '/'
        return stringpath

    def path_from_string(self, stringpath):
        assert os.path.join('home', '/') == '/'
        fullpath = os.path.abspath(os.path.realpath(stringpath))
        assert fullpath.startswith('/')
        path = tuple(x for x in fullpath.split('/') if x)
        assert fullpath == os.path.join('/', *path)
        return path

    def relative_path_from_string(self, stringpath):
        if stringpath.startswith('/'):
            raise ValueError(
                'Relative string paths can not start with "/": ' + stringpath)
        assert os.path.join('home', '/') == '/'
        path = tuple(x for x in stringpath.split('/') if x)
        assert '..' not in path
        assert '.' not in path
        assert stringpath == os.path.join(*path)
        return path

    def path_to_full_string(self, path):
        return 'local:' + self.path_to_string(path)

    def get_item_at_path(self, path):
        stringpath = self.path_to_string(path)
        f = LocalFile(stringpath)
        if not f._exists():
            raise FileNotFoundError('No such file or directory: ' + stringpath)
        if f._isdir():
            raise IsADirectoryError('Path is a directory: ' + stringpath)
        return f

    def get_modifiable_item_at_path(self, path):
        stringpath = self.path_to_string(path)
        f = LocalFile(stringpath, writable=True)
        if not f._exists():
            raise NotTestedError()
            raise FileNotFoundError('No such file or directory: ' + stringpath)
        if f._isdir():
            raise NotTestedError()
            raise IsADirectoryError('Path is a directory: ' + stringpath)
        return f

    def is_accessible(self):
        return True

    def does_path_exist(self, path):
        return os.path.exists(self.path_to_string(path))

    def get_directory_listing(self, path, include_special_files=True):
        stringpath = self.path_to_string(path)
        names = os.listdir(stringpath)
        dirs = []
        files = []
        for name in names:
            st = os.lstat(os.path.join(stringpath, name))
            if stat.S_ISLNK(st.st_mode):
                if (include_special_files):
                    files.append((st.st_ino, name))
            elif stat.S_ISDIR(st.st_mode):
                dirs.append(name)
            elif stat.S_ISREG(st.st_mode):
                files.append((st.st_ino, name))
            elif stat.S_ISSOCK(st.st_mode) or stat.S_ISFIFO(st.st_mode):
                if (include_special_files):
                    files.append((st.st_ino, name))
            else:
                raise AssertionError('Unknown file type: ' + name)
        files.sort()
        files = tuple(x[1] for x in files)
        return dirs, files

    def create_directory(self, path):
        os.makedirs(self.path_to_string(path))

    def create_regular_file(self, path):
        stringpath = self.path_to_string(path)
        self._ensure_parent_directory_exists(stringpath)
        f = open(stringpath, 'xb')
        return LocalFile(stringpath, openfile=f, writable=True)

    def _ensure_parent_directory_exists(self, stringpath):
        parent = os.path.dirname(stringpath)
        os.makedirs(parent, exist_ok=True)

    def create_temporary_file(self, path):
        stringpath = self.path_to_string(path)
        os.makedirs(stringpath, exist_ok=True)
        tmpfno, tmpname = tempfile.mkstemp(dir=stringpath)
        f = os.fdopen(tmpfno, 'w+b')
        return LocalTempFile(self, tmpname, openfile=f)

    def make_cheap_copy(self, source, target):
        sourcestringpath = self.path_to_string(source)
        targetstringpath = self.path_to_string(target)
        self._ensure_parent_directory_exists(targetstringpath)
        os.link(sourcestringpath, targetstringpath)

    def rename_and_overwrite(self, source, target):
        os.replace(self.path_to_string(source), self.path_to_string(target))

    def delete_file_at_path(self, path):
        strpath = self.path_to_string(path)
        if not os.path.exists(strpath):
            return
        os.remove(strpath)

    def get_config_paths_for(self, application):
        paths = []
        confhome = os.environ.get('XDG_CONFIG_HOME')
        if confhome is None:
            confhome = os.environ.get('HOME')
            if confhome is not None:
                confhome = os.path.join(confhome, '.config')
        if confhome is not None:
            paths.append(os.path.join(confhome, application))
        confdirs = os.environ.get('XDG_CONFIG_DIRS', '/etc/xdg')
        paths += [ os.path.join(x, application) for x in confdirs.split(':') ]
        return tuple(self.path_from_string(x) for x in paths)

class LocalFile(object):
    def __init__(self, stringpath, openfile=None, writable=False):
        self._stringpath = stringpath
        self._openfile = openfile
        self._writable = writable
        self._cached_stat = None
        self._cached_lstat = None

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()

    def _exists(self):
        s = self._lstat()
        return bool(s)

    def _isdir(self):
        s = self._stat()
        if s is False:
            return False
        return stat.S_ISDIR(s.st_mode)

    def _open(self):
        if self._openfile is not None:
            return
        if self._writable:
            self._openfile = open(self._stringpath, 'r+b')
        else:
            self._openfile = open(self._stringpath, 'rb')

    def drop_all_cached_data(self):
        self._cached_stat = None
        self._cached_lstat = None

    def close(self):
        if self._openfile is not None:
            self._openfile.close()
            self._openfile = None

    def get_filetype(self):
        s = self._lstat()
        if stat.S_ISREG(s.st_mode):
            return 'file'
        if stat.S_ISDIR(s.st_mode):
            return 'directory'
        if stat.S_ISLNK(s.st_mode):
            return 'symlink'
        if stat.S_ISFIFO(s.st_mode):
            return 'pipe'
        if stat.S_ISSOCK(s.st_mode):
            return 'socket'
        if stat.S_ISBLK(s.st_mode) or stat.S_ISCHR(s.st_mode):
            return 'device'
        return 'unknown'

    def get_size(self):
        s = self._stat()
        if s is False:
            raise FileNotFoundError('File not found: ' + self._stringpath)
        return s.st_size

    def _stat(self):
        if self._cached_stat is None:
            try:
                self._cached_stat = os.stat(self._stringpath)
            except FileNotFoundError:
                self._cached_stat = False
        return self._cached_stat

    def _lstat(self):
        if self._cached_lstat is None:
            try:
                self._cached_lstat = os.lstat(self._stringpath)
            except FileNotFoundError:
                self._cached_lstat = False
        return self._cached_lstat

    def get_mtime(self):
        s = self._stat()
        if s is False:
            raise FileNotFoundError('File not found: ' + self._stringpath)
        mtime_ns = s.st_mtime_ns % 1000000000
        mtime = datetime.datetime.utcfromtimestamp(s.st_mtime_ns // 1000000000)
        mtime = mtime.replace(microsecond=mtime_ns//1000)
        assert mtime.microsecond == mtime_ns // 1000
        return mtime, mtime_ns

    def get_link_mtime(self):
        s = self._lstat()
        if s is False:
            raise FileNotFoundError('File not found: ' + self._stringpath)
        mtime_ns = s.st_mtime_ns % 1000000000
        mtime = datetime.datetime.utcfromtimestamp(s.st_mtime_ns // 1000000000)
        mtime = mtime.replace(microsecond=mtime_ns//1000)
        assert mtime.microsecond == mtime_ns // 1000
        return mtime, mtime_ns

    def readsymlink(self):
        content = os.readlink(self._stringpath)
        if isinstance(content, str):
            return content.encode('utf-8')
        if isinstance(content, bytes):
            return content
        raise AssertionError('readlink returned unexpected data type')

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
            targetstringpath = self._tree.path_to_string(self._rename_path)
            self._tree._ensure_parent_directory_exists(targetstringpath)
            checkfile = open(targetstringpath, 'xb')
            checkfile.close()
            os.replace(self._stringpath, targetstringpath)
        else:
            os.remove(self._stringpath)

