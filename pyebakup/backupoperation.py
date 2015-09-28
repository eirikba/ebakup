#!/usr/bin/env python3

import collections

import logger

class DevNullUIState(object):
    def set_status(self, key, value):
        pass

class BackupOperation(object):
    def __init__(self, backupcollection, services=None):
        self._backupcollection = backupcollection
        self._sources = []
        if services is None:
            services = {}
        self._logger = services.get('logger')
        if self._logger is None:
            self._logger = logger.Logger(services=services)
        self._ui = services.get('uistate', DevNullUIState())
        self._files_backed_up = 0

    @property
    def logger(self):
        return self._logger

    def add_tree_to_backup(self, tree, sourcepath, targetpath):
        '''Register that 'sourcepath' inside 'tree' should be copied to
        'targetpath' inside the backup.
        '''
        assert isinstance(sourcepath, tuple)
        assert isinstance(targetpath, tuple)
        for current in self._sources:
            if (targetpath == current.targetpath[:len(targetpath)] or
                    targetpath[:len(current.targetpath)] == current.targetpath):
                raise NotTestedError(
                    'backup target paths must be distinct (' +
                    str(current.targetpath) + ' vs ' + str(targetpath) + ')')
        source = BackupSource(tree, sourcepath, targetpath, backup=self)
        self._sources.append(source)
        return source

    def execute_backup(self):
        '''Perform the backup operation. Make the necessary copies and update
        the necessary databases.
        '''
        self._ui.set_status('backup', 'Starting backup operation')
        self._added_static_contentids = set()
        self._ui.set_status('backup', 'Getting previous backup')
        self.previous = self._backupcollection.get_most_recent_backup()
        self._ui.set_status('backup', 'Creating new backup')
        backup = self._backupcollection.start_backup()
        with backup:
            for source in self._sources:
                self._backup_single_source(source, backup)
            self._ui.set_status('backup', 'Finishing backup')
            backup.commit()
        self._ui.set_status('backup', 'Checking for removed static files')
        self._check_removed_static_files(
            self._backupcollection.get_most_recent_backup())
        self._ui.set_status('backup', 'Complete')

    def _backup_single_source(self, source, backup):
        self._ui.set_status(
            'backup',
            'Starting backup of ' +
            source.tree.path_to_full_string(source.sourcepath))
        for sourcepath, targetpath, how in source.iterate_source_files():
            self._ui.set_status('backup', 'Backing up ' + str(sourcepath))
            self._ui.set_status('files backed up', str(self._files_backed_up))
            self._files_backed_up += 1
            assert how in ('static', 'dynamic')
            sourcefile = source.tree.get_item_at_path(sourcepath)
            filetype = sourcefile.get_filetype()
            if filetype == 'file':
                size = sourcefile.get_size()
                mtime, mtime_ns = sourcefile.get_mtime()
                cid = self._get_cid_if_assumed_unchanged(sourcefile, targetpath)
                if cid is None:
                    cid = self._try_add_content(sourcefile, sourcepath)
                if cid is None:
                    self._logger.log_error('File not backed up', sourcepath)
            elif filetype in ('socket', 'pipe', 'device', 'other'):
                size = 0
                mtime, mtime_ns = sourcefile.get_mtime()
                cid = b''
            elif filetype == 'symlink':
                data = sourcefile.readsymlink()
                size = len(data)
                mtime, mtime_ns = sourcefile.get_link_mtime()
                cid = self._try_add_content_from_data(data)
                if cid is None:
                    self._logger.log_error('Symlink not backed up', sourcepath)
            else:
                raise AssertionError(
                    'Unknown file type: ' + str(sourcepath))
            extra = sourcefile.get_backup_extra_data()
            if cid is not None:
                backup.add_file(
                    targetpath, cid, size, mtime, mtime_ns, filetype, extra)
            if how == 'static':
                old_cid = self._get_old_cid_for_path(targetpath)
                if old_cid is None:
                    self._added_static_contentids.add(cid)
                elif cid != old_cid:
                    self._logger.log(
                        self._logger.LOG_ERROR,
                        'static file changed', targetpath)
        self._ui.set_status(
            'backup', 'Backup of ' +
            source.tree.path_to_full_string(source.sourcepath) + ' complete')

    def _get_cid_if_assumed_unchanged(self, sourcefile, targetpath):
        if not self.previous:
            return None
        oldinfo = self.previous.get_file_info(targetpath)
        if not oldinfo:
            return None
        if oldinfo.filetype != 'file':
            return None
        mtime, mtime_ns = sourcefile.get_mtime()
        if (sourcefile.get_size() == oldinfo.size and
                mtime == oldinfo.mtime and
                mtime_ns == oldinfo.mtime_nsec):
            return oldinfo.contentid
        return None

    def _try_add_content(self, sourcefile, sourcepath):
        try:
            return self._backupcollection.add_content(sourcefile)
        except PermissionError:
            self._logger.log_error(
                'Permission denied to source file', sourcepath)
        return None

    def _try_add_content_from_data(self, data):
        return self._backupcollection.add_content_data(data)

    def _get_old_cid_for_path(self, path):
        if not self.previous:
            return None
        oldinfo = self.previous.get_file_info(path)
        if not oldinfo:
            return None
        return oldinfo.contentid

    def _check_removed_static_files(self, backup, path=()):
        if not self.previous:
            return
        dirs, files = self.previous.list_directory(path)
        for d in dirs:
            dpath = path + (d,)
            if self._may_target_path_have_statics(dpath):
                self._check_removed_static_files(backup, dpath)
        for f in files:
            fpath = path + (f,)
            handler = self._how_should_target_path_be_handled(fpath)
            if handler == 'static' and backup.get_file_info(fpath) is None:
                cid = self.previous.get_file_info(fpath).contentid
                if cid not in self._added_static_contentids:
                    self._logger.log(
                        self._logger.LOG_ERROR, 'static file removed', fpath)

    def _may_target_path_have_statics(self, path):
        for source in self._sources:
            if source.may_target_path_have_statics(path):
                return True
        return False

    def _how_should_target_path_be_handled(self, path):
        for source in self._sources:
            how = source.how_should_target_path_be_handled(path)
            if how is not None:
                return how
        return 'ignore'

class BackupSource(object):
    '''Describes a source tree for a backup operation. Use
    BackupOperation.add_tree_to_back_up() to create one of these.

    Subtrees can be set to be handled differently. The current
    possibilities are:

    - ignored: Nothing in the subtree starting at 'path' will be
      backed up.

    - backed up: The subtree starting at 'path' will be backed up in
      the standard way.

    - backed up static: The subtree starting at 'path' will be backed
      up in the standard way, but any changes to any files will cause
      errors to be reported. If a file is deleted, it is considered an
      error, unless a file is created with the same content at the
      same time. In which case the file is considered "moved".

    For each potential file, the most specific setting will take
    effect. So e.g., setting ('path',) as "ignore" and ('path', 'to')
    as 'dynamic' would mean that ('path', 'to', 'file.txt') would be
    backed up in the standard way.

    All files not otherwise set up is treated as 'backed up'.

    '''
    def __init__(self, tree, sourcepath, targetpath, backup=None):
        self.backup = backup
        self.tree = tree
        self.sourcepath = sourcepath
        self.targetpath = targetpath
        self.subtrees = None

    def set_backup_handlers(self, subtrees):
        if self.subtrees is not None:
            raise AssertionError('Should only set backup handlers once')
        self.subtrees = subtrees

    def iterate_source_files(self, subtree=()):
        try:
            dirs, files = self.tree.get_directory_listing(
                self.sourcepath + subtree)
        except PermissionError as e:
            self.backup.logger.log_error(
                'Failed to descend source directory',
                self.sourcepath + subtree,
                str(e))
            return
        for f in files:
            path = subtree + (f,)
            how = self._how_should_path_be_handled(path)
            if how != 'ignore':
                yield self.sourcepath + path, self.targetpath + path, how
        del files
        for d in dirs:
            dpath = subtree + (d,)
            if not self._is_whole_subtree_ignored(dpath):
                yield from self.iterate_source_files(dpath)

    def _how_should_path_be_handled(self, path):
        handler = self.subtrees.get_handler_for_path(path)
        if handler is None:
            return 'dynamic'
        return handler

    def _is_whole_subtree_ignored(self, path):
        return self.subtrees.is_whole_subtree_ignored(path)

    def may_target_path_have_statics(self, path):
        relpath = self._convert_target_path_to_source_path(path)
        if relpath is None:
            return False
        return self.subtrees.may_path_have_statics(relpath)

    def _convert_target_path_to_source_path(self, path):
        rootlen = len(self.targetpath)
        if path[:rootlen] != self.targetpath:
            return None
        return path[rootlen:]

    def how_should_target_path_be_handled(self, path):
        relpath = self._convert_target_path_to_source_path(path)
        if relpath is None:
            return 'ignore'
        return self._how_should_path_be_handled(relpath)
