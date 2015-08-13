#!/usr/bin/env python3

class SyncTask(object):

    def __init__(self, config, args):
        self._config = config
        self._args = args
        self._services = args.services
        self._logger = self._services['logger']
        self._ui = self._services['uistate']

    def execute(self):
        self._ui.set_status('task-sync', 'Started')
        backups = self._args.backups
        if backups:
            backups = [ self._config.get_backup_by_name(x) for x in backups ]
        else:
            backups = self._config.backups
        for backupconf in backups:
            self._sync_one_backup(backupconf)
        self._ui.set_status('task-sync', 'Completed')

    def _sync_one_backup(self, backupconf):
        self._ui.set_status('task-sync', 'Syncing ' + backupconf.name)
        # Defining the other status values to get them in the right order
        self._ui.set_status('syncing-backup', '....')
        self._ui.set_status('syncing', '....')
        collinfos = []
        for conf in backupconf.collections:
            if conf.filesystem.is_accessible:
                self._ui.set_status(
                    'task-sync',
                    'Opening collection ' +
                    conf.filesystem.path_to_full_string(conf.path))
                opener = self._services['backupcollection.open']
                if (self._args.create and not
                        conf.filesystem.does_path_exist(conf.path)):
                    opener = self._services['backupcollection.create']
                collection = opener(
                    conf.filesystem, conf.path, services=self._services)
                collinfos.append(CollectionData(conf, collection))
            else:
                self._logger.log_error(
                    'missing collection', conf,
                    'The collection will not be synced, neither from nor to.')
                raise NotTestedError('Missing collection during sync')
        self._ui.set_status(
            'task-sync',
            'Syncing ' + backupconf.name +
            ' (' + str(len(collinfos)) + ' collections)')
        self._sync_collections(collinfos)
        self._ui.set_status('task-sync', 'Sync complete for ' + backupconf.name)

    def _sync_collections(self, collinfos):
        self._ui.set_status('syncing', 'Making list of backups')
        pending_backups = set()
        for coll in collinfos:
            coll.backups = coll.collection.get_all_backup_names(
                order_by='starttime')
            pending_backups.update(coll.backups)
        self._ui.set_status('syncing', 'Syncing backup sets')
        pending_backups = [x for x in sorted(pending_backups, reverse=True)]
        while pending_backups:
            current_backup = pending_backups.pop()
            self._ui.set_status('syncing-backup', current_backup)
            has_backup = []
            missing_backup = []
            for coll in collinfos:
                if current_backup in coll.backups:
                    has_backup.append(coll)
                else:
                    missing_backup.append(coll)
            self._ui.set_status(
                'syncing', 'Comparing common backup: ' + current_backup)
            sourceinfo = self._compare_backups(has_backup, current_backup)
            for targetinfo in missing_backup:
                self._copy_backup(sourceinfo, targetinfo, current_backup)
        self._ui.set_status('syncing', 'Completed')

    def _copy_backup(self, sourceinfo, targetinfo, name):
        self._ui.set_status(
            'syncing-backup',
            'Copy ' + str(name) + ' to ' +
            targetinfo.conf.filesystem.path_to_full_string(
                targetinfo.conf.path))
        source = sourceinfo.collection.get_streaming_backup_reader_for_name(
            name)
        target = targetinfo.collection.get_streaming_backup_writer_for_name(
            name)
        item = next(source)
        assert item.kind == 'magic'
        assert item.value == b'ebakup backup data'
        target.write(item)
        section = 'setting'
        for item in source:
            if item.kind != section:
                if item.kind in ('file', 'directory'):
                    if section in ('setting', 'file', 'directory'):
                        section = item.kind
            if item.kind != section:
                raise AssertionError(
                    '"' + section + '" + followed by "' + item.kind + '"')
            if item.kind == 'setting':
                pass
            elif item.kind == 'directory':
                pass
            elif item.kind == 'file':
                cid = self._copy_content(sourceinfo, targetinfo, item.cid)
                if cid != item.cid:
                    # This isn't wrong per se. But it is highly
                    # unexpected and probably indicates a bug
                    # somewhere. And while no code should be depending
                    # on this uniqueness constraint, I might have some
                    # cases that will just give up rather than do the
                    # right (or wrong!) thing.
                    raise NotTestedError('Content copy got different cid')
                    self._logger.log_notice(
                        'content copy made new cid',
                        item.cid,
                        'new cid: ' + repr(cid))
                    item.cid = cid
            else:
                raise AssertionError('Unknown item type: ' + item.kind)
            target.write(item)
        target.close()

    def _copy_content(self, sourceinfo, targetinfo, contentid):
        self._ui.set_status(
            'syncing', 'Copy content ' + hexstr(contentid[:8]) + '...')
        sourcecoll = sourceinfo.collection
        targetcoll = targetinfo.collection
        sourcetree = sourceinfo.conf.filesystem
        targettree = targetinfo.conf.filesystem
        sourcepath = sourcecoll._make_path_from_contentid(contentid)
        targetpath = targetcoll._make_path_from_contentid(contentid)
        if not targettree.does_path_exist(targetpath):
            cid = targetcoll.add_content(
                sourcetree.get_item_at_path(sourcepath))
            if cid != contentid:
                self._logger.log_error(
                    'Sync copy cid change', contentid,
                    'The cid of copied content changed even though the '
                    'original content id was unused. That is sufficiently '
                    'strange that I am calling it an error, even though '
                    'it should work fine.')
            targetinfo.added_content[cid] = (sourceinfo, contentid)
            return cid
        if self._is_content_assumed_same(sourceinfo, targetinfo, contentid):
            return contentid
        cid = targetcoll.add_content(sourcetree.get_item_at_path(sourcepath))
        if cid == contentid:
            self._logger.log_warning(
                'Sync copy existing content', contentid,
                'Did not trivially assume that the same contentid represents '
                'the same content. But it did. There is nothing wrong with '
                'this, but it is inefficent. Thus, a warning.')
        else:
            self._logger.log_error(
                'Sync copy cid change', contentid,
                'The cid of copied content changed. That should mean '
                'that there already is different content with the same '
                'content id in the target collection. That is sufficiently '
                'strange that I am calling it an error, even though '
                'it should work fine.')
        return cid

    def _is_content_assumed_same(self, sourceinfo, targetinfo, contentid):
        # If there is a common backup in source and target having
        # 'contentid' as the content id for the same path, then I can
        # trivially assume that they are the same. Start with the most
        # recent common backup.
        sourcebks = set(x for x in sourceinfo.backupcids)
        targetbks = set(x for x in targetinfo.backupcids)
        bks = sorted(sourcebks & targetbks, reverse=True)
        for bk in bks:
            sourcepath = sourceinfo.backupcids[bk].get(contentid)
            targetpath = targetinfo.backupcids[bk].get(contentid)
            if sourcepath is not None and sourcepath == targetpath:
                return True
        added_from = targetinfo.added_content.get(contentid)
        if added_from is not None:
            if sourceinfo == added_from[0] and contentid == added_from[1]:
                return True
        if bks:
            # No need recreating backupcids
            return False
        sourcebks = set(x for x in sourceinfo.backups)
        targetbks = set(x for x in targetinfo.backups)
        bks = sorted(sourcebks & targetbks, reverse=True)
        if not bks:
            return False
        # Actually, let's only check one backup. This may need to be
        # improved at some point.
        bk = bks[0]
        sourceinfo.backupcids[bk] = self._read_backupcids(sourceinfo, bk)
        targetinfo.backupcids[bk] = self._read_backupcids(targetinfo, bk)
        sourcepath = sourceinfo.backupcids[bk].get(contentid)
        targetpath = targetinfo.backupcids[bk].get(contentid)
        if sourcepath is not None and sourcepath == targetpath:
            return True
        return False

    def _read_backupcids(self, info, name):
        reader = info.collection.get_streaming_backup_reader_for_name(name)
        dirs = { 0: () }
        bkcids = {}
        for item in reader:
            if item.kind == 'directory':
                dirs[item.dirid] = dirs[item.parent] + (item.name,)
            elif item.kind == 'file':
                path = dirs[item.parent] + (item.name,)
                if item.cid not in bkcids:
                    bkcids[item.cid] = path
                else:
                    bkcids[item.cid] = min(bkcids[item.cid], path)
        return bkcids

    def _compare_backups(self, collinfos, backupname):
        self._ui.set_status('syncing', 'Checking match for ' + str(backupname))
        if len(collinfos) == 1:
            return collinfos[0]
        # Just compare the full binary data of the backup files. Due
        # to the way I do sync right now, they should be exactly the
        # same.
        base = collinfos[0]
        basecontent = self._get_full_content_for_backup(base, backupname)
        for collinfo in collinfos[1:]:
            content = self._get_full_content_for_backup(collinfo, backupname)
            if content != basecontent:
                raise NotImplementedError(
                    'While not wrong as such, this is rather unexpected '
                    'and I do not handle it (yet). In fact, it is so '
                    'unexpected that I suspect a bug somewhere.')

    def _get_full_content_for_backup(self, collinfo, name):
        # This is cheating! But for now it gets me what I need.
        reader = collinfo.collection.get_streaming_backup_reader_for_name(name)
        return reader._data

class CollectionData(object):
    def __init__(self, conf, collection):
        self.conf = conf
        self.collection = collection
        self.backupcids = {}  # { backupname : { cid: path } }
        self.added_content = {}  # { cid : { (source) CollectionData: cid } }

hexits = '0123456789abcdef'
def hexstr(data):
    out = ''
    for x in data:
        out += hexits[x>>4] + hexits[x&15]
    return out