#!/usr/bin/env python3

import collections

import logger

class BackupOperation(object):
    def __init__(self, backupcollection):
        self._backupcollection = backupcollection
        self._sources = []
        self._logger = logger.global_logger

    def set_logger(self, logger):
        self._logger = logger

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
        source = BackupSource(tree, sourcepath, targetpath)
        self._sources.append(source)
        return source

    def execute_backup(self):
        '''Perform the backup operation. Make the necessary copies and update
        the necessary databases.
        '''
        self._added_static_content_ids = set()
        self.previous = self._backupcollection.get_most_recent_backup()
        backup = self._backupcollection.start_backup()
        with backup:
            for source in self._sources:
                self._backup_single_source(source, backup)
            backup.commit()
        self._check_removed_static_files(
            self._backupcollection.get_most_recent_backup())

    def _backup_single_source(self, source, backup):
        collection = self._backupcollection
        for sourcepath, targetpath, how in source.iterate_source_files():
            assert how in ('static', 'dynamic')
            cid = self._get_cid_if_assumed_unchanged(
                source.tree, sourcepath, targetpath)
            if cid is None:
                cid = collection.add_content(source.tree, sourcepath)
            sourcefile = source.tree.get_item(sourcepath)
            mtime, mtime_ns = sourcefile.get_mtime()
            backup.add_file(
                targetpath, cid, sourcefile.get_size(),
                mtime, mtime_ns)
            if how == 'static':
                old_cid = self._get_old_cid_for_path(targetpath)
                if old_cid is None:
                    self._added_static_content_ids.add(cid)
                elif cid != old_cid:
                    self._logger.log(
                        self._logger.LOG_ERROR,
                        'static file changed', targetpath)

    def _get_cid_if_assumed_unchanged(self, tree, path, targetpath):
        if not self.previous:
            return None
        oldinfo = self.previous.get_file_info(targetpath)
        if not oldinfo:
            return None
        sourcefile = tree.get_item(path)
        mtime, mtime_ns = sourcefile.get_mtime()
        if (sourcefile.get_size() == oldinfo.size and
                mtime == oldinfo.mtime and
                mtime_ns == oldinfo.mtime_ns):
            return oldinfo.contentid
        return None

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
                if cid not in self._added_static_content_ids:
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

BackupSubtree = collections.namedtuple('BackupSubtree', ('path', 'how'))

class BackupSource(object):
    '''Describes a source tree for a backup operation. Use
    BackupOperation.add_tree_to_back_up() to create one of these.

    Subtrees can be set to be handled differently. The current
    possibilities are:

    - ignore_subtree(): Nothing in the subtree starting at 'path' will
      be backed up.

    - back_up_subtree(): The subtree starting at 'path' will be backed
      up in the standard way.

    - back_up_static_subtree(): The subtree starting at 'path' will be
      backed up in the standard way, but any changes to any files will
      cause errors to be reported.

    For each potential file, the most specific setting will take
    effect. So e.g., setting ('path',) as "ignore" and ('path', 'to')
    as 'back_up' would mean that ('path', 'to', 'file.txt') would be
    backed up in the standard way.

    All files not otherwise set up is treated like back_up_subtree().
    '''
    def __init__(self, tree, sourcepath, targetpath):
        self.tree = tree
        self.sourcepath = sourcepath
        self.targetpath = targetpath
        self._subtrees = []

    def ignore_subtree(self, path):
        '''No files inside 'path', including 'path' itself will be backed up.
        '''
        assert isinstance(path, tuple)
        for current in self._subtrees:
            if current.path == path:
                raise NotTestedError(
                    'Ambiguous handling introduced for ' + str(path))
        self._subtrees.append(BackupSubtree(path, 'ignore'))

    def back_up_subtree(self, path):
        '''The files inside 'path' (including 'path' itself) will be backed up
        in the usual way.
        '''
        assert isinstance(path, tuple)
        for current in self._subtrees:
            if current.path == path:
                raise NotTestedError(
                    'Ambiguous handling introduced for ' + str(path))
        self._subtrees.append(BackupSubtree(path, 'dynamic'))

    def back_up_static_subtree(self, path):
        '''The files inside 'path' (including 'path' itself) will be backed up
        in the usual way. In addition, these files are expected not to
        change. So if a file does change, an error will reported.

        Adding new files is ok and "moving" files is also fine.
        However, if a file is removed, an error will be reported.

        A file is considered "moved" if it is removed and another file
        with the exact same content is added at the same time.
        '''
        assert isinstance(path, tuple)
        for current in self._subtrees:
            if current.path == path:
                raise NotTestedError(
                    'Ambiguous handling introduced for ' + str(path))
        self._subtrees.append(BackupSubtree(path, 'static'))

    def iterate_source_files(self, subtree=()):
        dirs, files = self.tree.get_directory_listing(self.sourcepath + subtree)
        for f in files:
            path = subtree + (f,)
            how = self._how_should_path_be_handled(path)
            if how != 'ignore':
                yield self.sourcepath + path, self.targetpath + path, how
        for d in dirs:
            yield from self.iterate_source_files(subtree + (d,))

    def _how_should_path_be_handled(self, path):
        pathlen = len(path)
        bestlen = -1
        bestrule = None
        for rule in self._subtrees:
            rulepathlen = len(rule.path)
            if (rulepathlen <= pathlen and
                    rulepathlen > bestlen and
                    path[:rulepathlen] == rule.path):
                bestrule = rule
                bestlen = rulepathlen
        if bestrule is None:
            return 'dynamic'
        return bestrule.how

    def may_target_path_have_statics(self, path):
        rootlen = len(self.targetpath)
        if path[:rootlen] != self.targetpath:
            return False
        relpath = path[rootlen:]
        relpathlen = len(relpath)
        for rule in self._subtrees:
            if rule.how == 'static' and rule.path[:relpathlen] == relpath:
                return True
        how = self._how_should_path_be_handled(relpath)
        if how == 'static':
            return True
        return False

    def how_should_target_path_be_handled(self, path):
        rootlen = len(self.targetpath)
        if path[:rootlen] != self.targetpath:
            return 'ignore'
        relpath = path[rootlen:]
        return self._how_should_path_be_handled(relpath)
