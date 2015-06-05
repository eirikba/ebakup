#!/usr/bin/env python3

import datetime

import database
import logger

class BackupCollectionFactory(object):
    def __init__(self, tree, path):
        self._tree = tree
        self._path = path
        self._dbopener = database.Database
        self._dbcreator = database.create_database

    def set_database_creator(self, dbcreator):
        self._dbcreator = dbcreator

    def set_database_opener(self, dbopener):
        self._dbopener = dbopener

    def create_collection(self):
        '''Create a new backup collection as described by this object and
        return a BackupCollection for accessing it.
        '''
        # Explicitly create the top-level directory to ensure failure
        # if it already exists.
        self._tree.create_directory(self._path)
        self._tree.create_directory(self._path + ('content',))
        db = self._dbcreator(self._tree, self._path + ('db',))
        return BackupCollection(self)

    def open_collection(self):
        '''Return a BackupCollection object for the backup collection
        described by this object.
        '''
        return BackupCollection(self)


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

    def __init__(self, params):
        '''Return a BackupCollection object for the backup collection
        described by 'params'.
        '''
        self._logger = logger.Logger()
        self._tree = params._tree
        self._path = params._path
        self._verify_sane_directory_structure()
        self._open_database(params._dbopener)
        self._utcnow = datetime.datetime.utcnow

    def set_logger(self, logger):
        self._logger.replay_log(logger)
        self._logger = logger

    def set_utcnow(self, utcnow):
        self._utcnow = utcnow

    def _verify_sane_directory_structure(self):
        if not self._tree.does_path_exist(self._path):
            raise NotTestedError('No backup collection')
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

    def _open_database(self, dbfactory):
        self._db = dbfactory(self._tree, self._path + ('db',))

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

    def _make_shadow_copy(self, path, content_id):
        contentpath = self._make_path_from_content_id(content_id)
        contentpath = self._path + ('content',) + contentpath
        shadowpath = self._path + path
        self._tree.make_cheap_copy(contentpath, shadowpath)

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

    def add_content(self, tree, path, now=None):
        '''Add the content at 'path' in 'tree' to the content store and return
        its content id. If a suitable item already exists in the
        content store, nothing is added to the content store and the
        content id of the existing item is returned.
        '''
        if now is None:
            now = self._utcnow()
        source = tree.get_item_at_path(path)
        if source is None:
            raise FileNotFoundError('No file found at: ' + str(path))
        target = self._tree.create_temporary_file(self._path + ('tmp',))
        checksum_algo = self._db.get_checksum_algorithm()
        checksummer = checksum_algo()
        size = source.get_size()
        done = 0
        written = 0
        read_size = 1024 * 1024 * 100
        data = b''
        with target:
            with source:
                while done < size:
                    data = source.get_data_slice(done, done + read_size)
                    done += len(data)
                    checksummer.update(data)
                    if done < size:
                        written = target.write_data_slice(written, data)
            checksum = checksummer.digest()
            assert len(data) == done or written == size
            assert written == 0 or written == size
            if len(data) == done:
                content_id = self._find_duplicate_content_of_data(
                    data, checksum)
            elif written == size:
                content_id = self._find_duplicate_content_of_file(
                    target, checksum)
            else:
                raise AssertionError('Neither file nor data complete!')
            if content_id:
                return content_id
            if written == 0 and data:
                written = target.write_data_slice(0, data)
            assert written == size
            content_id = self._db.add_content_item(now, checksum)
            target_path = self._make_path_from_content_id(content_id)
            target.rename_without_overwrite_on_close(
                self._tree, self._path + ('content',) + target_path)
        return content_id

    def _find_duplicate_content_of_data(self, data, checksum):
        read_size = 1024 * 1024 * 10
        datalen = len(data)
        for cand in self._db.get_all_content_infos_with_checksum(checksum):
            candpath = self._make_path_from_content_id(cand.get_content_id())
            with self._tree.get_item_at_path(
                    self._path + ('content',) + candpath) as candfile:
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
                    return cand.get_content_id()
        return None

    def _find_duplicate_content_of_file(self, datafile, checksum):
        read_size = 1024 * 1024 * 10
        datalen = datafile.get_size()
        for cand in self._db.get_all_content_infos_with_checksum(checksum):
            candpath = self._make_path_from_content_id(cand.get_content_id())
            with self._tree.get_item_at_path(
                    self._path + ('content',) + candpath) as candfile:
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
                    return cand.get_content_id()
        return None

    def _make_path_from_content_id(self, content_id):
        # Slightly broken. Should make sure each path component except
        # the last has the same length as the items already in those
        # directories. As long as this method is used to create all
        # new content files, the resulting tree will be valid anyway.
        # But this won't work correctly with all valid trees.
        first = hexify(content_id[0:1])
        second = hexify(content_id[1:2])
        rest = hexify(content_id[2:])
        return first, second, rest

    def list_content_ids_for_checksum(self, checksum):
        '''Return a list of all content ids that represent content items
        having 'checksum' as checksum.
        '''

    def list_content_ids(self, first, count):
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
            lastsum = dbinfo.get_last_known_checksum(),
            timeline = dbinfo.get_checksum_timeline())

    def update_content_checksum(
            self, content_id, when, checksum, restored=False):
        '''Update the checksum timeline of the content item with content id
        'content_id' to indicate that its checksum was 'checksum' at
        the time 'when'. If 'restored' is True, it means that the
        content item was somehow checked to be the same as a "believed
        good" copy.
        '''
        info = self._db.get_content_info(content_id)
        if restored:
            info.register_content_recovered(when, checksum)
        else:
            info.register_checksum(when, checksum)

    def get_content_reader(self, content_id):
        '''Return a FileInterface object that can be used to access the data
        stored as the content item with id 'content_id'. The returned
        object only supports the methods that do not modify the
        content item.
        '''
        path = self._make_path_from_content_id(content_id)
        return ContentReader(
            self._tree.get_item_at_path(self._path + ('content',) + path))

class ContentInfo(object):
    '''Provides information about a content item.
    '''
    def __init__(self, goodsum, lastsum, timeline):
        self.goodsum = goodsum
        self.lastsum = lastsum
        self.timeline = timeline

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

    def add_file(self, path, contentid, size, mtime, mtime_nsec):
        '''Add the file at 'path' to the backup, with the given attributes.
        '''
        self._backup.add_file(path, contentid, size, mtime, mtime_nsec)
        self._collection._make_shadow_copy(self._shadow_root + path, contentid)

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
        'good_checksum', 'size', 'mtime' and 'mtime_nsec'.
        '''
        filedata = self._info.get_file_info(path)
        if not filedata:
            return None
        return FileData(
            self._collection._db,
            content_id = filedata.contentid,
            size = filedata.size,
            mtime = filedata.mtime,
            mtime_nsec = filedata.mtime_nsec)

class FileData(object):
    def __init__(self, db, content_id, size, mtime, mtime_nsec):
        self._db = db
        self.contentid = content_id
        self.size = size
        self.mtime = mtime
        self.mtime_nsec = mtime_nsec
        self._good_checksum = None

    @property
    def good_checksum(self):
        if not self._good_checksum:
            info = self._db.get_content_info(self.contentid)
            self._good_checksum = info.get_good_checksum()
        return self._good_checksum
