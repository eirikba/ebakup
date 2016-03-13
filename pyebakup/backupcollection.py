#!/usr/bin/env python3

import datetime

import database
import logger

def create_collection(tree, path, services=None):
    '''Create a new backup collection at tree:path and return a
    BackupCollection for accessing it.
    '''
    # Explicitly create the top-level directory to ensure failure
    # if it already exists.
    tree.create_directory(path)
    tree.create_directory(path + ('content',))
    if services is not None:
        dbcreator = services.get('database.create')
    if services is None or dbcreator is None:
        dbcreator = database.create_database
    db = dbcreator(tree, path + ('db',))
    return BackupCollection(tree, path, services=services)

def open_collection(tree, path, services=None):
    '''Return a BackupCollection object for the backup collection
    described by this object.
    '''
    return BackupCollection(tree, path, services=services)


hexits = '0123456789abcdef'
def hexify(data):
    out = []
    for c in data:
        assert c >= 0
        assert c <= 255
        out.append(hexits[c>>4])
        out.append(hexits[c&15])
    return ''.join(out)

class BackupCollection(object):
    '''Provides access to a backup collection.
    '''

    def __init__(self, tree, path, services=None):
        '''Do not create BackupCollection objects by hand. Use the
        create_collection() and open_collection() helper functions
        instead.
        '''
        if services is None:
            services = {}
        self._logger = services.get('logger')
        if not self._logger:
            self._logger = logger.Logger(services=services)
        self._utcnow = services.get('utcnow', datetime.datetime.utcnow)
        self._tree = tree
        self._path = path
        self._verify_sane_directory_structure()
        dbopener = None
        if services is not None:
            dbopener = services.get('database.open')
        if dbopener is None:
            dbopener = database.Database
        self._open_database(dbopener)

    def _verify_sane_directory_structure(self):
        if not self._tree.does_path_exist(self._path):
            raise FileNotFoundError(
                'Backup collection does not exist: ' + str(self._path))
        if not self._tree.does_path_exist(self._path + ('db',)):
            raise NotTestedError('No "db"')
            raise FileNotFoundError(
                'Backup collection has no "db": ' + str(self._path))
        if not self._tree.does_path_exist(self._path + ('content',)):
            raise NotTestedError('No "content"')
            raise FileNotFoundError(
                'Backup collection has no "content": ' + str(self._path))

    def _open_database(self, dbopener):
        self._db = dbopener(self._tree, self._path + ('db',))

    def start_backup(self, start=None):
        '''Starts a backup operation.

        Use the returned BackupBuilder object to fill in all the data
        about the backup. And remember to commit it when done.

        'start' should be a naive datetime.datetime giving the time
        when the backup started in UTC. If None, the current time will
        be used.
        '''
        if start is None:
            start = self._utcnow()
        builder = BackupBuilder(self, start)
        return builder

    def get_all_backup_names(self, order_by=None):
        '''Obtain a list of the names of all backups.

        See Database.get_all_backup_names() for details.
        '''
        return self._db.get_all_backup_names(order_by)

    def get_backup_file_reader_for_name(self, name):
        '''Obtain a DataFile opened read-only for the backup named 'name'.

        See Database.get_backup_file_reader_for_name() for details.
        '''
        return self._db.get_backup_file_reader_for_name(name)

    def create_backup_file_in_replacement_mode(self, starttime):
        '''Create a backup file for a backup starting at 'starttime'.

        This will create a new backup according to whatever data it is
        fed. If there already exists a conflicting backup file, this
        method will fail.

        See Database.create_backup_file_in_replacement_mode() for
        details on the returned object.
        '''
        return self._db.create_backup_file_in_replacement_mode(starttime)

    def get_backup_by_name(self, name):
        '''Return a BackupData object for the backup named 'name'.

        If there is no suitable backup, None is returned.
        '''
        info = self._db.get_backup_by_name(name)
        if info is None:
            return None
        return BackupData(self, info)

    def get_most_recent_backup(self):
        '''Return a BackupData object for the most recently created backup.

        If there is no suitable backup, None is returned.
        '''
        info = self._db.get_most_recent_backup()
        if info is None:
            return None
        return BackupData(self, info)

    def get_most_recent_backup_before(self, when):
        '''Return a BackupData object for the most recently created backup
        that was created before 'when'.

        If there is no suitable backup, None is returned.
        '''
        info = self._db.get_most_recent_backup_before(when)
        if info is None:
            return None
        return BackupData(self, info)

    def get_oldest_backup(self):
        '''Return a BackupData object for the most recently created backup.

        If there is no suitable backup, None is returned.
        '''
        info = self._db.get_oldest_backup()
        if info is None:
            return None
        return BackupData(self, info)

    def get_oldest_backup_after(self, when):
        '''Return a BackupData object for the most recently created backup
        that was created after 'when'.

        If there is no suitable backup, None is returned.
        '''
        info = self._db.get_oldest_backup_after(when)
        if info is None:
            return None
        return BackupData(self, info)

    def add_content(self, sourcefile, now=None):
        '''Add the content of the File object 'sourcefile' to the content
        store and return its content id. If a suitable item already
        exists in the content store, nothing is added to the content
        store and the content id of the existing item is returned.

        NOTE: 'sourcefile' will be opened for reading and closed again.
        '''
        if now is None:
            now = self._utcnow()
        target = self._tree.create_temporary_file(self._path + ('tmp',))
        checksum_algo = self._db.get_checksum_algorithm()
        checksummer = checksum_algo()
        size = sourcefile.get_size()
        done = 0
        written = 0
        read_size = 1024 * 1024 * 100
        data = b''
        with target:
            with sourcefile:
                while done < size:
                    data = sourcefile.get_data_slice(done, done + read_size)
                    done += len(data)
                    checksummer.update(data)
                    if done < size or written > 0:
                        written = target.write_data_slice(written, data)
            checksum = checksummer.digest()
            assert len(data) == size or written == size
            assert written == 0 or written == size
            if len(data) == size:
                contentid = self._find_duplicate_content_of_data(
                    data, checksum)
            elif written == size:
                contentid = self._find_duplicate_content_of_file(
                    target, checksum)
            else:
                raise AssertionError('Neither file nor data complete!')
            if contentid:
                return contentid
            if written == 0 and data:
                written = target.write_data_slice(0, data)
            assert written == size
            contentid = self._db.add_content_item(now, checksum)
            target_path = self._make_path_from_contentid(contentid)
            target.rename_without_overwrite_on_close(self._tree, target_path)
        return contentid

    def _find_duplicate_content_of_data(self, data, checksum):
        read_size = 1024 * 1024 * 10
        datalen = len(data)
        for cand in self._db.get_all_content_infos_with_checksum(checksum):
            candpath = self._make_path_from_contentid(cand.get_contentid())
            with self._tree.get_item_at_path(candpath) as candfile:
                if candfile.get_size() != datalen:
                    continue
                done = 0
                ok = True
                while done < datalen:
                    dataslice = data[done:done+read_size]
                    candslice = candfile.get_data_slice(done, done + read_size)
                    done += len(dataslice)
                    if dataslice != candslice:
                        done = datalen
                        ok = False
                if ok:
                    return cand.get_contentid()
        return None

    def _find_duplicate_content_of_file(self, datafile, checksum):
        read_size = 1024 * 1024 * 10
        datalen = datafile.get_size()
        for cand in self._db.get_all_content_infos_with_checksum(checksum):
            candpath = self._make_path_from_contentid(cand.get_contentid())
            with self._tree.get_item_at_path(candpath) as candfile:
                if candfile.get_size() != datalen:
                    continue
                done = 0
                ok = True
                while done < datalen:
                    dataslice = datafile.get_data_slice(done, done + read_size)
                    candslice = candfile.get_data_slice(done, done + read_size)
                    done += len(dataslice)
                    if dataslice != candslice:
                        done = datalen
                        ok = False
                if ok:
                    return cand.get_contentid()
        return None

    def _make_path_from_contentid(self, contentid):
        # Slightly broken. Should make sure each path component except
        # the last has the same length as the items already in those
        # directories. As long as this method is used to create all
        # new content files, the resulting tree will be valid anyway.
        # But this won't work correctly with all valid trees.
        first = hexify(contentid[0:1])
        second = hexify(contentid[1:2])
        rest = hexify(contentid[2:])
        return self._path + ('content',) + (first, second, rest)

    def add_content_data(self, data, now=None):
        '''Add 'data' to the content store and return its content id. If a
        suitable item already exists in the content store, nothing is
        added to the content store and the content id of the existing
        item is returned.
        '''
        if now is None:
            now = self._utcnow()
        checksum_algo = self._db.get_checksum_algorithm()
        checksummer = checksum_algo()
        checksummer.update(data)
        checksum = checksummer.digest()
        contentid = self._find_duplicate_content_of_data(data, checksum)
        if contentid:
            return contentid
        contentid = self._db.add_content_item(now, checksum)
        target_path = self._make_path_from_contentid(contentid)
        with self._tree.create_temporary_file(self._path + ('tmp',)) as f:
            f.write_data_slice(0, data)
            f.rename_without_overwrite_on_close(self._tree, target_path)
        return contentid

    def list_contentids_for_checksum(self, checksum):
        '''Return a list of all content ids that represent content items
        having 'checksum' as checksum.
        '''

    def iterate_contentids(self):
        '''Iterates over all content ids in this collection.
        '''
        yield from self._db.iterate_contentids()

    def list_contentids(self, first, count):
        '''Return a list of content ids that exist in the database.

        The returned list will contain the 'count' smallest content
        ids greater or equal to 'first'. Comparisons are done as
        simple lexical comparisons on the octet sequences.

        While it is somewhat likely that the list will be sorted, this
        is not guaranteed.
        '''

    def get_content_info(self, contentid):
        '''Get the stored data about the content item with content id
        'contentid'.

        The returned object has at least the attributes 'goodsum',
        'lastsum' and 'timeline'.

        The 'timeline' attribute is a sequence (probably a tuple) of
        items with at least the attributes 'checksum', 'first',
        'last', and 'restored'. 'restored' is True if the entry was
        created when the content item was restored from a "believed
        good" source and False otherwise. It is an error for
        'restored' to be True and 'checksum' to be different from
        'goodsum'.
        '''
        dbinfo = self._db.get_content_info(contentid)
        return ContentInfo(
            goodsum = dbinfo.get_good_checksum(),
            first_seen = dbinfo.get_first_seen_time())

    def get_content_reader(self, contentid):
        '''Return a FileInterface object that can be used to access the data
        stored as the content item with id 'contentid'. The returned
        object only supports the methods that do not modify the
        content item.
        '''
        path = self._make_path_from_contentid(contentid)
        return ContentReader(self._tree.get_item_at_path(path))

    def make_shadow_copy(self, info, tree, path):
        '''Create a shadow copy (aka hard link) of the file described by
        'info' at tree:path.
        '''
        # If the trees are not the same, some magic is needed to find
        # the equivalent of the source path in the target tree.
        assert self._tree.is_same_file_system_as(tree)
        source = self._make_path_from_contentid(info.contentid)
        tree.make_cheap_copy(source, path)

class ContentInfo(object):
    '''Provides information about a content item.
    '''
    def __init__(self, goodsum, first_seen):
        self.goodsum = goodsum
        self.first_seen = first_seen

class ContentReader(object):
    '''Provides read-only access to a content item. See FileInterface for
    specification of public methods.
    '''
    def __init__(self, contentfile):
        self._contentfile = contentfile

    def get_size(self):
        return self._contentfile.get_size()

    def get_data_slice(self, start, end):
        return self._contentfile.get_data_slice(start, end)

class BackupBuilder(object):
    '''Allows building up a new backup.
    '''
    def __init__(self, collection, start_time):
        self._collection = collection
        self._db = collection._db
        self._start_time = start_time
        self._shadow_root = (
            str(start_time.year),
            '{:02}-{:02}T{:02}:{:02}'.format(
                start_time.month, start_time.day,
                start_time.hour, start_time.minute))
        self._backup = self._db.start_backup(start_time)

    def __enter__(self):
        '''When the context is exited, abort() is called.
        '''
        return self

    def __exit__(self, a, b, c):
        self.abort()

    def add_file(
            self, path, contentid, size, mtime, mtime_nsec, filetype='file',
            extra={}):
        '''Add the file at 'path' to the backup, with the given attributes.
        '''
        self._backup.add_file(
            path, contentid, size, mtime, mtime_nsec, filetype, extra)

    def add_directory(self, path, extra={}):
        '''Add the directory at 'path' to the backup, with the given attributes.
        '''
        self._backup.add_directory(path, extra)

    def commit(self, end_time=None):
        '''Finish up the backup and publish it in the database.

        'end_time' should be a naive datetime.datetime giving the time
        when the backup was completed in UTC. If None, the current
        time will be used.
        '''
        if end_time is None:
            end_time = self._collection._utcnow()
        self._backup.commit(end_time)

    def abort(self):
        '''If the backup is not yet committed, abort it and do not add it to
        the database.
        '''
        self._backup.abort()

class BackupData(object):
    '''Provides access to an existing backup.
    '''
    def __init__(self, collection, bkinfo):
        self._collection = collection
        self._info = bkinfo

    def get_start_time(self):
        '''Return the time at which the backup was started.
        '''
        return self._info.get_start_time()

    def get_end_time(self):
        '''Return the time at which the backup was completed.
        '''
        return self._info.get_end_time()

    def list_directory(self, path):
        '''Return a pair (directories, files) listing the content of the
        directory 'path'.

        The returned 'directories' and 'files' are lists of the names
        of (respectively) the directories and files that exist in 'path'.
        '''
        return self._info.get_directory_listing(path)

    def get_file_info(self, path):
        '''Get the stored data about the file at 'path'. If 'path' does not
        exist or is not a file, None is returned.

        The returned object has at least the attributes 'contentid',
        'good_checksum', 'size', 'mtime', 'mtime_nsec', 'filetype' and
        'extra_data'.
        '''
        filedata = self._info.get_file_info(path)
        if not filedata:
            return None
        return FileData(
            self._collection._db,
            contentid = filedata.contentid,
            size = filedata.size,
            mtime = filedata.mtime,
            mtime_nsec = filedata.mtime_nsec,
            filetype = filedata.filetype,
            extra_data = filedata.extra_data)

    def get_dir_info(self, path):
        '''Get the stored data about the directory at 'path'. If 'path' does
        not exist or is not a directory, None is returned.

        The returned object has at least the attribute 'extra_data'.
        '''
        return self._info.get_dir_info(path)


class FileData(object):
    def __init__(
            self, db, contentid, size, mtime, mtime_nsec, filetype, extra_data):
        self._db = db
        self.contentid = contentid
        self.size = size
        self.mtime = mtime
        self.mtime_nsec = mtime_nsec
        self.filetype = filetype
        self.extra_data = extra_data
        self._good_checksum = None

    @property
    def good_checksum(self):
        if self._good_checksum is None:
            if self.contentid == b'':
                assert self.filetype != 'file'
                self._good_checksum = b''
            else:
                info = self._db.get_content_info(self.contentid)
                self._good_checksum = info.get_good_checksum()
        return self._good_checksum
