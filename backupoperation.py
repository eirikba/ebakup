#!/usr/bin/env python3

import collections

import logger

class BackupOperation(object):
    def __init__(self, backupcollection):
        self._backupcollection = backupcollection
        self._sources = []
        self._logger = logger.Logger()

    def set_logger(self, logger):
        self._logger.replay_log(logger)
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
            sourcefile = source.tree.get_item_at_path(sourcepath)
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
        sourcefile = tree.get_item_at_path(path)
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

class BackupSubtree(object):
    def __init__(self, parent, matchtype, component):
        self.parent = parent
        self.matchtype = matchtype
        self.component = component
        self.handler = None
        self.children = []

    def get_path(self):
        if self.parent is None:
            return ()
        return self.parent.get_path() + ((self.matchtype, self.component),)

    def matches_component(self, component):
        if self.matchtype == 'plain':
            return self.component == component
        assert self.matchtype == 'glob'
        return _glob_is_match(self.component, component)

    def set_ignored(self):
        '''Mark this subtree as ignored. Nothing in this subtree will be
        backed up. (Unless a more specific setting says otherwise.)
        '''
        self._set_handler('ignore')

    def set_backed_up(self):
        '''Mark this subtree as "dynamic". The content of this subtree will be
        backed up in the normal way. (Unless a more specific setting
        says otherwise.)
        '''
        self._set_handler('dynamic')

    def set_backed_up_static(self):
        '''Mark this subtree as "static". The content of this subtree will be
        backed up and any changes to any content will be flagged as
        errors. (Unless a more specific setting says otherwise.)
        '''
        self._set_handler('static')

    def _set_handler(self, handler):
        assert handler in ('ignore', 'static', 'dynamic')
        if self.handler is not None:
            raise NotTestedError(
                'Ambiguous handling introduced for ' + str(self.get_path()))
        self.handler = handler

    def get_handler_for_path(self, path):
        handler, tree = self._get_handler_and_tree_for_path(path)
        return handler

    def make_subtree(self, component):
        assert isinstance(component, (str, bytes))
        return self._make_subtree_of_type('plain', component)

    def make_subtree_for_glob(self, glob):
        assert isinstance(glob, (str, bytes))
        _glob_check_valid(glob)
        return self._make_subtree_of_type('glob', glob)

    def _make_subtree_of_type(self, matchtype, component):
        subtree = BackupSubtree(self, matchtype, component)
        for child in self.children:
            if child._is_same_as(subtree):
                return child
            if child._has_common_matches_with(subtree):
                raise NotTestedError(
                    'Ambiguous path created: ' + str(self.get_path()) + ' + ' +
                    str((matchtype, component)))
        self.children.append(subtree)
        return subtree

    def _is_same_as(self, other):
        return (other.matchtype == self.matchtype and
                other.component == self.component)

    def _has_common_matches_with(self, other):
        if self.matchtype == 'plain' and other.matchtype == 'plain':
            return self.component == other.component
        if self.matchtype == 'plain':
            return other.matches_component(self.component)
        if other.matchtype == 'plain':
            return self.matches_component(other.component)
        assert self.matchtype == 'glob'
        assert other.matchtype == 'glob'
        return _glob_have_common_matches(self.component, other.component)

    def may_path_have_statics(self, path):
        handler, tree = self._get_handler_and_tree_for_path(path)
        if tree is not None:
            return tree._is_any_contained_item_static()
        return handler == 'static'

    def _get_handler_and_tree_for_path(self, path):
        if path == ():
            return self.handler, self
        for child in self.children:
            if child.matches_component(path[0]):
                handler, tree = child._get_handler_and_tree_for_path(path[1:])
                if handler is None:
                    handler = self.handler
                return handler, tree
        return self.handler, None

    def _is_any_contained_item_static(self):
        if self.handler == 'static':
            return True
        for child in self.children:
            if child._is_any_contained_item_static():
                return True
        return False


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
    def __init__(self, tree, sourcepath, targetpath):
        self.tree = tree
        self.sourcepath = sourcepath
        self.targetpath = targetpath
        self.subtrees = BackupSubtree(None, None, None)

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
        handler = self.subtrees.get_handler_for_path(path)
        if handler is None:
            return 'dynamic'
        return handler

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
