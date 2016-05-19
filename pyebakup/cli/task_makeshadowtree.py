#!/usr/bin/env python3

class SnapshotMissingError(Exception):
    pass

class MakeShadowTreeTask(object):
    def __init__(self, config, args):
        self._config = config
        self._args = args

    def execute(self):
        try:
            self._make_shadow_tree()
        except SnapshotMissingError as e:
            self._log_error_snapshot_missing()

    def _make_shadow_tree(self):
        self.tree, self.target_path = self._convert_user_path_to_tree_and_path(
            self._args.target)
        self.bc = self._get_source_storage()
        self.snapshot = self._get_snapshot()
        self._make_shadow_subtree(())

    def _convert_user_path_to_tree_and_path(self, stringpath):
        tree = self._args.services['filesystem']('local')
        path = tree.path_from_string(stringpath)
        return tree, path

    def _get_source_storage(self):
        # Grabbing the first storage of the first backup. This will
        # fail if the target path doesn't support hardlinks from that
        # particular storage. (So the code should really look for
        # the first storage that supports those hardlinks.)
        bkname = self._config.get_all_backup_names()[0]
        bk = self._config.get_backup_by_name(bkname)
        bcconf = bk.storages[0]
        return self._args.services['backupstorage.open'](
            bcconf.filesystem, bcconf.path)

    def _get_snapshot(self):
        snapshot = self.bc.get_backup_by_name(self._args.snapshotname)
        if snapshot is None:
            raise SnapshotMissingError(self._args.snapshotname)
        return snapshot

    def _make_shadow_subtree(self, subpath):
        self.tree.create_directory(self.target_path + subpath)
        dirs, files = self.snapshot.list_directory(subpath)
        for f in files:
            self._make_shadow_copy_of_file(subpath + (f,))
        del files
        for d in dirs:
            self._make_shadow_subtree(subpath + (d,))

    def _make_shadow_copy_of_file(self, path):
        info = self.snapshot.get_file_info(path)
        if info.contentid:
            self.bc.make_shadow_copy(info, self.tree, self.target_path + path)

    def _log_error_snapshot_missing(self):
        self._args.services['logger'].log_error(
            'snapshot missing', self._args.snapshotname)
