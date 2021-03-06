#!/usr/bin/env python3

class BackupTask(object):

    def __init__(self, config, args):
        self._config = config
        self._create_storage = args.create
        self._services = args.services
        self._logger = self._services['logger']
        self._ui = self._services['uistate']
        self._backupoperationfactory = args.services['backupoperation']
        self._storageopener = args.services['backupstorage.open']
        self._storagecreator = args.services['backupstorage.create']
        self._dbcreator = args.services['database.create']
        self._dbopener = args.services['database.open']
        self._utcnow = args.services['utcnow']
        self._backups = [x for x in args.backups]

    def execute(self):
        self._ui.set_status('task-backup', 'Starting')
        for name in self._backups:
            self._run_single_backup(name)
        self._ui.set_status('task-backup', 'Complete')

    def _run_single_backup(self, name):
        self._ui.set_status('task-backup', 'Starting backup ' + name)
        backup_config = self._config.get_backup_by_name(name)
        if backup_config is None:
            self._logger.log(
                self._logger.LOG_ERROR, 'Requested backup unknown', name)
            raise NotTestedError('Requested backup not found')
            return
        # TODO: Pick the available storage with the oldest "most
        # recent" backup.
        storetree = None
        for storage in backup_config.storages:
            cand = storage.filesystem
            if cand.is_accessible():
                storetree = cand
                storepath = storage.path
                break
        if storetree is None:
            self._logger.log(
                self._logger.LOG_ERROR, 'No backup storages available', name)
            raise NotTestedError('No backup storages available')
            return
        if self._create_storage:
            self._ui.set_status(
                'task-backup',
                'Creating storage ' +
                storetree.path_to_full_string(storepath))
            storage = self._services['backupstorage.create'](
                storetree, storepath, services=self._services)
        else:
            self._ui.set_status(
                'task-backup',
                'Opening storage ' +
                storetree.path_to_full_string(storepath))
            storage = self._services['backupstorage.open'](
                storetree, storepath, services=self._services)
        self._ui.set_status('task-backup', 'Storage opened')
        operation = self._backupoperationfactory(
            storage, services=self._services)
        for sourcedata in backup_config.sources:
            self._ui.set_status(
                'task-backup',
                'Preparing backup of ' +
                sourcedata.filesystem.path_to_full_string(sourcedata.path))
            source = sourcedata.filesystem
            bktree = operation.add_tree_to_backup(
                source, sourcedata.path, sourcedata.targetpath)
            bktree.set_backup_handlers(sourcedata.subtree_handlers)
        self._ui.set_status('task-backup', 'Running backup of ' + name)
        operation.execute_backup()
