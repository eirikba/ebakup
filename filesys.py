#!/usr/bin/env python3

import socket

class FileSystemAccessLostError(Exception): pass

from local_filesys import LocalFileSystem

def get_file_system(kind):
    if kind == 'local':
        return LocalFileSystem()
    else:
        raise NotTestedError('Unknown file system: ' + kind)


class FileInterface(object):
    '''Abstract class documenting the File interface.

    File objects represent the leaf nodes of a file tree.

    There are several kinds of files. The main type is "regular"
    files. A "regular" file is primarily a sequence of octets. But it
    also may have some meta-data, such as the time of last
    modification.
    '''

    def __enter__(self):
        '''See __exit__
        '''

    def __exit__(self, a, b, c):
        '''When exiting a File context, close() is called, thus releasing all
        locks and other resources held by the File object.
        '''

    def is_regular(self):
        '''True iff the file is a "regular" file.
        '''

    def is_special(self):

        '''True iff the file is a "special" file.

        Equal to "not is_regular()".
        '''

    def get_size(self):
        '''The size of the file's data, in octets.

        Only valid for regular files. Raises AttributeError() if
        called on special files.
        '''

    def get_mtime(self):
        '''The time of last modification of the file.

        Definitely valid for regular files, maybe valid for special
        files. Raises AttributeError() if called on a file that
        doesn't provide a last modification time.

        The returned value is a (mtime, nanoseconds) pair, where
        'mtime' gives last modification time of the file as a
        datetime.datetime, and 'nanoseconds' gives the sub-second
        precision (in nanoseconds). Note that not all systems provide
        nanosecond precision.

        'mtime' and 'nanoseconds' SHALL agree to microsecond
        precision (i.e. mtime.microsecond == nanoseconds//1000).

        '''

    def get_data_slice(self, start, end):
        '''Retrieve part of the data of the file.

        This returns the [start:end] slice of the data of the file. If
        'end' is beyond the end of the file, the available data is
        returned. This is the only case where this method will return
        less data than requested (and so can be used to reliably
        detect end-of-file).

        Only valid for regular files. Raises AttributeError() if
        called on special files.
        '''

    def write_data_slice(self, start, data):
        '''Write data to the file.

        Requires the file to be modifiable. If this method is called
        on an object obtained from get_item_at_path(), it will most
        likely fail.

        The data in the file starting at 'start' octets into the file
        will be overwritten by 'data'. The file will grow as necessary
        to make space for the newly written data. If 'start' is
        greater than the size of the file, the content of the
        intervening data will be undefined.

        Only valid for regular files. Raises AttributeError() if
        called on special files.

        The position of the first octet after the newly written data
        is returned.

        '''

    def lock_for_reading(self):
        '''Take a read lock on the file.

        While anyone is holding a read lock, no one can take a write
        lock on the file. Make sure to call close() to drop the lock!

        Only valid for regular files. Raises AttributeError() if
        called on special files.
        '''

    def lock_for_writing(self):
        '''Take a write lock on the file.

        Probably requires the file to be modifiable.

        While anyone is holding a write lock, no one can take neither
        a read lock nor a write lock on the file. Make sure to call
        close() to drop the lock!

        Only valid for regular files. Raises AttributeError() if
        called on special files.
        '''

    def close(self):
        '''Free up any temporary resources tied to the underlying file and
        drop all locks.

        Only valid for regular files. Raises AttributeError() if
        called on special files.
        '''

class TemporaryFileInterface(FileInterface):
    '''Abstract class documenting the interface to the TemporaryFile
    interface.

    A TemporaryFile represents a file that is not expected to exist
    for long. By default, the file will be deleted when close() is
    called.
    '''
    def get_name(self):
        '''Return the last path component of the path to the underlying file.

        When using methods like
        DirectoryInterface.create_temporary_file(), the caller does
        not know the name of the file that was created. This method
        provides that missing information.
        '''

    def keep_on_close(self):
        '''By default, the temporary file will be deleted when close() is
        called on this object. Call this method to have close() leave
        the file in place.
        '''

    def rename_without_overwrite_on_close(self, tree, path):
        '''By default, the temporary file will be deleted when close() is
        called on this object. Call this method to have close() rename
        the file instead.

        If 'tree' is not the same tree as was used to create this
        file, an exception is raised.
        '''


class FileSystemInterface(object):
    '''Abstract class documenting the FileSystem interface.

    FileSystem objects represent a complete file tree. It provides
    access to the files in the file tree as well as some file-system
    specific information.

    This abstraction recognizes three kinds of elements in the tree:
    directories, regular files and special files. Directories are
    purely containers for more directories and files and carry no data
    or metadata themselves. Regular files are objects that carry data
    (a sequence of octets) as well as some metadata (e.g. the time of
    last modification). Special files are files that exist in the
    tree, but can generally not be interacted with through this
    FileSystem abstraction.
    '''

    def does_path_exist(self, path):
        '''Returns True if 'path' exists in the tree, and False otherwise.
        '''

    def get_directory_listing(self, path=()):
        '''Return the names of all the items at 'path' as a pair (dirs, files)
        of lists. The 'dirs' list contains the names of all the
        directories, while the 'files' list contains the names of all
        the files.
        '''

    def get_item_at_path(self, path):
        '''Return a File object representing the node at 'path'.

        The returned object can not be used to modify the underlying
        file.

        'path' is a sequence of path components, each of which is
        typically a string. 'path' is interpreted as being relative to
        this object.

        If the object at 'path' does not exist in the tree, a
        FileNotFoundError will be raised. If 'path' is a Directory,
        IsADirectoryError will be raised.
        '''

    def get_modifiable_item_at_path(self, path):
        '''Return a File object representing the node at 'path'.

        The returned object can be used to modify the underlying file,
        but the actual check for whether that is possible may happen
        only when modifications are actually attempted.

        'path' is a sequence of path components, each of which is
        typically a string. 'path' is interpreted as being relative to
        this object.

        If the object at 'path' does not exist in the tree, a
        FileNotFoundError will be raised. If 'path' is a Directory,
        IsADirectoryError will be raised.
        '''

    def create_directory(self, path, exist_ok=False):
        '''Create a new directory at the path 'path' relative to this
        directory (and return its Directory object).

        'path' is a sequence of path components, each of which is
        typically a string. 'path' is interpreted as being relative to
        this object.

        If there is already an element (file or directory) named
        'name', there are two possibilities. If 'exist_ok' is true and
        the existing element is a directory, nothing is changed and a
        Directory object of the existing directory is returned.
        Otherwise, FileExistsError is raised.

        Plenty of other errors are possible too, of course.
        '''

    def create_regular_file(self, path):
        '''Create a new file at the path 'path' relative to this directory
        (and return its File object).

        'path' is a sequence of path components, each of which is
        typically a string. 'path' is interpreted as being relative to
        this object.

        If there is already something (file or directory) at 'path',
        FileExistsError will be raised. Plenty of other errors are
        possible too, of course.
        '''

    def create_temporary_file(self, path):
        '''Same as create_regular_file(), but 'path' is here the path to a
        directory rather than a file, and the newly created file is a
        file inside this directory. A TemporaryFile object is returned
        that represents this newly created file.

        'path' is a sequence of path components, each of which is
        typically a string. 'path' is interpreted as being relative to
        this object.

        If 'path' does not exist or is not a directory, a
        NotADirectoryError will be raised. Like create_regular_file()
        this method makes sure not to overwrite any existing file.
        Unlike create_regular_file() it does this by choosing a file
        name that is not in use and so does not raise FileExistsError.
        '''

    def rename_and_overwrite(self, sourcepath, targetpath):
        '''Rename 'sourcepath' to 'targetpath', deleting any existing file at
        'targetpath'.

        The deletion of the old file and the renaming itself SHOULD be
        atomic as far as possible. This would mean that 'targetpath'
        will always exist, either as the old file or the new file.

        If 'targetpath' exists as a directory, the rename operation
        will fail.
        '''

    def rename_without_overwrite(self, sourcepath, targetpath):
        '''Rename 'sourcepath' to 'targetpath'.

        If 'targetpath' already exists, the rename operation will fail.
        '''

    def make_cheap_copy(self, sourcepath, targetpath):
        '''Create 'targetpath' as a "cheap" copy of 'sourcepath'.

        This will create 'targetpath' as a new file that shares the
        contents with 'sourcepath'. That is, the contents only exist
        once on disk. It is possible, but not necessary that some or
        all meta-data are shared as well. On unixy systems, this will
        create a hardlink.

        If it is not possible to make this cheap copy, a
        DirectoryOperationFailedError will be raised.

        Both paths are sequences of path components, each of which is
        typically a string. The paths are interpreted as being
        relative to this object.
        '''

    def make_full_copy(self, sourcepath, targetpath):
        '''Create 'targetpath' as a copy of 'sourcepath'.

        This will create 'targetpath' as a new file with the same
        contents as 'sourcepath'. The two copies will not share any
        data.

        Both paths are sequences of path components, each of which is
        typically a string. The paths are interpreted as being
        relative to this object.
        '''

    def is_accessible(self):
        '''True if the file system is currently accessible.

        The return value of this method may change between True and
        False at any time. When this method returns False, most
        methods using the FileSystem will raise
        FileSystemAccessLostErrors. When this method returns True, they
        will typically not.

        As an example, consider a FileSystem that accesses its file
        system over the network. The network may drop out at any time,
        and then you'll get FileSystemAccessLostErrors. And it may come
        back at any time, and then you won't get those errors.
        '''

    def get_config_paths_for(self, application):
        '''Obtain the paths to the application's configuration files.

        'application' is the name of the application as it would be
        known to the system.

        The returned value is a sequence of paths. Each path is a path
        to where the file system would expect the application's
        configuration files to live. The paths are ordered from the
        most important to the least important.
        '''
