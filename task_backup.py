#!/usr/bin/env python3

class BackupTask(object):

    def __init__(self, config, args):
        self._config = config
        self._logger = args.logger
        self._create_collection = args.create
        self._services = args.services
        self._backupoperationfactory = args.services['backupoperation']
        self._collectionopener = args.services['backupcollection.open']
        self._collectioncreator = args.services['backupcollection.create']
        self._treefactory = args.services['filesystem']
        self._dbcreator = args.services['database.create']
        self._dbopener = args.services['database.open']
        self._utcnow = args.services['utcnow']
        self._backups = [x for x in args.backups]

    def execute(self):
        for name in self._backups:
            self._run_single_backup(name)

    def _run_single_backup(self, name):
        backup_config = self._config.get_backup_by_name(name)
        if backup_config is None:
            self._logger.log(
                self._logger.LOG_ERROR, 'Requested backup unknown', name)
            raise NotTestedError('Requested backup not found')
            return
        # TODO: Pick the available collection with the oldest "most
        # recent" backup.
        storetree = None
        for collectiondata in backup_config.collections:
            cand = self._treefactory(collectiondata.accessor)
            if cand.is_accessible():
                storetree = cand
                storepath = collectiondata.path
                break
        if storetree is None:
            self._logger.log(
                self._logger.LOG_ERROR, 'No backup collections available', name)
            raise NotTestedError('No backup collections available')
            return
        if self._create_collection:
            collection = self._services['backupcollection.create'](
                storetree, storepath, services=self._services)
        else:
            collection = self._services['backupcollection.open'](
                storetree, storepath, services=self._services)
        collection.set_utcnow(self._utcnow)
        collection.set_logger(self._logger)
        operation = self._backupoperationfactory(collection)
        operation.set_logger(self._logger)
        for sourcedata in backup_config.sources:
            source = self._treefactory(sourcedata.accessor)
            bktree = operation.add_tree_to_backup(
                source, sourcedata.path, sourcedata.targetpath)
            bktree.set_backup_handlers(sourcedata.subtree_handlers)
        operation.execute_backup()
