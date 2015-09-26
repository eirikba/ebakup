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

    NOTE: The implementation of File is allowed to return stale data
    about the underlying file. But no more stale than the time at
    which the File object itself was created. So "worst" case, the
    File object would read all information about the underlying file
    when it is constructed, and never look at the file system again.

    So, if you need up-to-date information, either call
    'drop_all_cached_data()' or obtain a new object for the same file
    using 'FileSystem.get_item_at_path()' (or
    'get_modifiable_item_at_path()'), which is guaranteed to create a
    new File object.

    (However, an implementation is not required to cache any data, and
    may return up-to-date data at any time.)
    '''

    def __enter__(self):
        '''See __exit__
        '''

    def __exit__(self, a, b, c):
        '''When exiting a File context, close() is called, thus releasing all
        locks and other resources held by the File object.
        '''

    def drop_all_cached_data(self):
        '''Forget everything about the underlying file.

        This ensures that any further call to any method of this
        object will return data that is no more stale than the time at
        which this method was called. Note that a newly constructed
        File object does not have any cached data, so this method need
        not be called in that case.
        '''

    def get_filetype(self):
        '''The type of the file.

        The return value is:
          'file': a regular file
          'directory': a directory
          'symlink': a symbolic link
          'socket': a socket
          'pipe': a pipe or fifo
          'device': a device file
          'unknown': unknown file type

        More values may be added to this list over time.

        The various types in more details:

        'file' - A sequence of octets with some metadata.

        'directory' - A container of other files.

        'symlink' - A file that only contains a pointer to another
        file and usually behaves like that other file. It does have
        its own metadata, though.

        'socket' - A file that behaves similar to network sockets. A
        server can "listen" to the socket and clients can "connect" to
        it.

        'pipe' - A file where anything written to it can only be read
        once. And the data must be read in the same order as it was
        written.

        'device' - A file that represents a piece of hardware. Or
        something pretending to be hardware. Which could include some
        interfaces to very non-hardware kernel structures.

        'unknown' - The implementation failed to classify it as one of
        the other types. It is guaranteed that the file does not have
        'file' or 'directory' type, but it could have any of the other
        types. Or a type not in the list.

        '''

    def get_size(self):
        '''The size of the file's data, in octets.

        For regular files, this is the size of the file's data in
        octets. For symlinks, this is the size of the pointed-to
        file's data. Broken symlinks raise FileNotFoundError.

        Raises AttributeError() if called on other files.
        '''

    def get_mtime(self):
        '''The time of last modification of the file.

        Definitely valid for regular files, maybe valid for special
        files. Raises AttributeError() if called on a file that
        doesn't provide a last modification time.

        Symlinks return the mtime of the pointed-to file rather than
        its own mtime. Broken symlinks raise FileNotFoundError.

        The returned value is a (mtime, nanoseconds) pair, where
        'mtime' gives last modification time of the file as a
        datetime.datetime, and 'nanoseconds' gives the sub-second
        precision (in nanoseconds). Note that not all systems provide
        nanosecond precision.

        'mtime' and 'nanoseconds' SHALL agree to microsecond
        precision (i.e. mtime.microsecond == nanoseconds//1000).
        '''

    def get_link_mtime(self):
        '''The time of last modification of the symlink.

        Probably valid for the same types of files as get_mtime().

        When called on a symlink, this method returns the
        last-modified time for the symlink itself, while get_mtime()
        returns the last-modified time for the file pointed to by the
        symlink.

        For other file types, this is equivalent to get_mtime().
        '''

    def readsymlink(self):
        '''Return a filesystem-specific representation of the target of the
        symlink.

        The returned value is guaranteed to be a bytes object.

        If the file is not a symlink, an exception is raised.
        '''

    def get_data_slice(self, start, end):
        '''Retrieve part of the data of the file.

        This returns the [start:end] slice of the data of the file. If
        'end' is beyond the end of the file, the available data is
        returned. This is the only case where this method will return
        less data than requested (and so can be used to reliably
        detect end-of-file).

        Only valid for regular files (and symlinks, which acts as
        their pointed-to file). Raises AttributeError() if called on
        special files.
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

        Only valid for regular files (and symlinks, which acts as
        their pointed-to file). Raises AttributeError() if called on
        special files.

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

        Only valid for regular files (and symlinks, which operate on
        the pointed-to file). Raises AttributeError() if called on
        special files.
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

    def is_same_file_system_as(self, tree):
        '''Return true if 'tree' represents the same file system as this
        object.

        Essentially, this means that using the same path with 'tree'
        as with this object will refer to the same object.
        '''

    def path_to_string(self, path):
        '''Convert a path in tuple form to the file system's string
        representation.

        'path' is a tuple of strings giving the components of the path
        in order. The returned value should be a string describing the
        same path in the way it is usually represented by the file
        system.

        'path' is assumed to be an absolute path, and the returned
        string should also be on the absolute form.
        '''

    def path_from_string(self, stringpath):
        '''Convert a path in the file system's string representation to tuple
        format.

        'stringpath' is a string describing a path in the file
        system's common representation. The method returns a tuple of
        strings giving the same path as a sequence of components.

        If 'stringpath' is not an absolute path, it is interpreted as
        a path relative to the current working directory (for the file
        system) and an absolute path is returned.
        '''

    def relative_path_from_string(self, stringpath):
        '''Convert a relative path in the file system's string representation
        to tuple format.

        'stringpath' is a string describing a relative path in the
        file system's common representation. The method returns a
        tuple of strings giving the same path as a sequence of
        components.

        Some path components have non-trivial behaviour. If
        'stringpath' contains any such path components, this method
        may show unexpected behaviour. Or raise an exception. The
        common examples are path components referring to the parent
        directory or the directory itself.
        '''

    def path_to_full_string(self, path):
        '''Convert a path in tuple form to a string representation of the
        full path, including the file system specification.
        '''

    def does_path_exist(self, path):
        '''Returns True if 'path' exists in the tree, and False otherwise.
        '''

    def get_directory_listing(self, path=(), include_special_files=True):
        '''Return the names of all the items at 'path' as a pair (dirs, files)
        of lists. The 'dirs' list contains the names of all the
        directories, while the 'files' list contains the names of all
        the files.

        If 'include_special_files' is True (the default), the 'files'
        list will contain all files including "special" files such as
        symbolic links, named sockets and device files. If
        'include_special_files' is False, the 'files' list will contain
        only the "regular" files in the directory.

        Regardless of 'include_special_files', the 'dirs' and 'files'
        list will only contain the "proper" contents of 'path'. In
        particular, the typical "." and ".." items (referring to the
        directory itself and the parent directory) are never included.
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

    def delete_file_at_path(self, path):
        '''Delete the file at 'path'.

        If 'path' is not a file, this method will fail and raise an
        exception. However, if 'path' does not exist at all, this
        method will succeed without doing anything.
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
