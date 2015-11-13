#!/usr/bin/env python3

import collections
import datetime
import re
import textwrap
import unittest

import task_info

class FakeFilesys(object):
    def __init__(self, name):
        self._name = name

    def path_to_full_string(self, path):
        return 'local:/' + '/'.join(path)

class FakeConfig(object):
    def __init__(self):
        self._backups = []

    def _add_full_config(self):
        bk = self._add_backup('mine')
        coll = bk._add_collection(
            FakeFilesys('local'), ('data', 'backup1', 'mine'))
        src = bk._add_source(FakeFilesys('local'), ('home', 'me'))
        bk = self._add_backup('stuff')
        bk = self._add_backup('other')

    def _add_backup(self, name):
        bk = FakeBackupConfig(name)
        self._backups.append(bk)
        return bk

    def get_all_backup_names(self):
        return tuple(x.name for x in self._backups)

    def get_backup_by_name(self, name):
        for bk in self._backups:
            if bk.name == name:
                return bk
        return None

class FakeBackupConfig(object):
    def __init__(self, name):
        self.name = name
        self.collections = []
        self.sources = []

    def _add_collection(self, filesystem, path):
        coll = FakeCollectionConfig(filesystem, path)
        self.collections.append(coll)
        return coll

    def _add_source(self, filesystem, path):
        src = FakeSourceConfig(filesystem, path)
        self.sources.append(src)
        return src

class FakeCollectionConfig(object):
    def __init__(self, filesystem, path):
        self.filesystem = filesystem
        self.path = path

class FakeSourceConfig(object):
    def __init__(self, filesystem, path):
        self.filesystem = filesystem
        self.path = path

class FakeUIState(object):
    def set_status(self, key, value):
        pass

class FakeArgs(object):
    def __init__(self, config):
        self._config = config
        self.services = {
            'logger': FakeLogger(),
            'backupcollection.open': FakeCollectionMaker().open_collection,
            'uistate': FakeUIState(),
            }

class FakeLogger(object):
    def __init__(self):
        self._printed = []

    def print(self, msg):
        self._printed.append(msg)

class FakeCollectionMaker(object):
    def __init__(self):
        self._collections = []

    def open_collection(self, tree, path, services=None):
        fullpath = tree.path_to_full_string(path)
        for coll in self._collections:
            if coll._fullpath == fullpath:
                return coll
        return FakeCollection(fullpath)

class FakeCollection(object):
    def __init__(self, fullpath):
        self._fullpath = fullpath
        self._content = []

    def _add_content(self, cid_num=None, added=None):
        cid = hex(cid_num)[2:] + '0' * 30
        self._content.append(
            FakeContentInfo(cid, checksum=cid, first=added))

    def iterate_contentids(self):
        for item in self._content:
            yield item.contentid

    def get_content_info(self, cid):
        for item in self._content:
            if item.contentid == cid:
                return item

ContentChecksum = collections.namedtuple(
    'ContentChecksum', ('checksum', 'first', 'last', 'restored'))
class FakeContentInfo(object):
    def __init__(self, cid, checksum=None, first=None):
        self.contentid = cid
        if checksum is not None:
            self.checksum = checksum
        if first is not None:
            self.first = first
            self.last_verified_time = first
        self.timeline = ( ContentChecksum(checksum, first, first, True), )

class InfoTestSupport(unittest.TestCase):
    def get_first_line(self, **kwargs):
        return self._get_next_line(-1, **kwargs)

    def get_next_line(self, line, **kwargs):
        return self._get_next_line(line[0], **kwargs)

    def _get_next_line(
            self, start, text=None, indent=None, prefix=None, in_block=None):
        data = self.out.data
        for line in data:
            if start >= line[0]:
                continue
            if indent is not None and indent != line[1]:
                continue
            if text is not None and text != line[2]:
                continue
            if prefix is not None and not line[2].startswith(prefix):
                continue
            if in_block is not None and line[1] <= in_block[1]:
                return None
            return line
        return None

class TestInfoForEmptyConfig(InfoTestSupport):
    def setUp(self):
        config = FakeConfig()
        args = FakeArgs(config)
        self.args = args
        self.services = args.services
        self.task = task_info.InfoTask(config, args)
        self.task.execute()

    def test_output(self):
        out = self.services['logger']._printed
        self.assertEqual(
            ['Backup definitions:',
             '  No backups defined'],
            out)

class TestInfoForFullConfig(InfoTestSupport):
    def setUp(self):
        config = FakeConfig()
        config._add_full_config()
        args = FakeArgs(config)
        collection = FakeCollection('local:/data/backup1/mine')
        collection._add_content(
            cid_num=1, added=datetime.datetime(2015, 5, 17, 20, 47, 25))
        collection._add_content(
            cid_num=2, added=datetime.datetime(2014, 9, 7, 7, 49, 45))
        collection._add_content(
            cid_num=3, added=datetime.datetime(2015, 1, 15, 17, 47, 7))
        collection._add_content(
            cid_num=4, added=datetime.datetime(2014, 3, 24, 16, 49, 50))
        collfact = FakeCollectionMaker()
        collfact._collections.append(collection)
        args.services['backupcollection.open'] = collfact.open_collection
        self._utcnow = datetime.datetime(2015, 6, 14, 14, 28, 54)
        args.services['utcnow'] = self.utcnow
        self.args = args
        self.services = args.services
        self.task = task_info.InfoTask(config, args)
        self.task.execute()
        self.lines = self.services['logger']._printed
        self.text = '\n'.join(self.lines)

    def utcnow(self):
        return self._utcnow

    def test_toplevel_blocks(self):
        self.assertEqual(
            ['Backup definitions:'],
            [x for x in self.lines if not x.startswith(' ')])

    def test_configured_backups_are_listed(self):
        in_backupdefs = False
        backups = []
        for line in self.lines:
            if in_backupdefs:
                if line.startswith('  backup'):
                    backups.append(line[2:])
                elif not line.startswith(' '):
                    break
            else:
                if line == 'Backup definitions:':
                    in_backupdefs = True
        self.assertEqual(
            ['backup mine', 'backup stuff', 'backup other'],
            backups)

    def test_backup_mine_is_correct(self):
        if False: # Not implemented yet
         self.assertInfoHasBlock(textwrap.dedent('''\
            backup mine
              collection local:/data/backup1/mine
                Least recently verified: 2014-03-24 16:49:50
                Not verified for one year: 1 files
                Not verified for three months: 3 files
                Not verified for one month: 3 files
                Not verified for one week: 4 files
              source local:/home/me
                stored at: home
                ignored/static/dynamic rules: 3/2/0
             '''))

    @unittest.skip('last-verified data is currently broken')
    def test_backup_mine_is_correct_partial(self):
        self.assertIn(textwrap.indent(textwrap.dedent('''\
            backup mine
              collection local:/data/backup1/mine
                Least recently verified: 2014-03-24 16:49:50
                Total number of content files: 4
                Not verified for one year: 1 files
                Not verified for three months: 3 files
                Not verified for one month: 3 files
                Not verified for one week: 4 files
              source local:/home/me
            '''), prefix='  '),
        self.text)
