#!/usr/bin/env python3

import re
import unittest

import task_info

class FakeConfig(object):
    def __init__(self):
        self._backups = []

    def _add_full_config(self):
        bk = self._add_backup('mine')
        coll = bk._add_collection('local', ('data', 'backup1', 'mine'))
        src = bk._add_source('local', ('home', 'me'))
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

    def _add_collection(self, accessor, path):
        coll = FakeCollectionConfig(accessor, path)
        self.collections.append(coll)
        return coll

    def _add_source(self, accessor, path):
        src = FakeSourceConfig(accessor, path)
        self.sources.append(src)
        return src

class FakeCollectionConfig(object):
    def __init__(self, accessor, path):
        self.accessor = accessor
        self.path = path

class FakeSourceConfig(object):
    def __init__(self, accessor, path):
        self.accessor = accessor
        self.path = path

class FakeArgs(object):
    def __init__(self, config):
        self._config = config
        self.logger = FakeLogger()

class FakeLogger(object):
    def __init__(self):
        self._printed = []

    def print(self, msg):
        self._printed.append(msg)

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
        self.task = task_info.InfoTask(config, args)
        self.task.execute()

    def test_output(self):
        out = self.args.logger._printed
        self.assertEqual(
            ['Backup definitions:',
             '  No backups defined'],
            out)

class TestInfoForFullConfig(InfoTestSupport):
    def setUp(self):
        config = FakeConfig()
        config._add_full_config()
        args = FakeArgs(config)
        self.args = args
        self.task = task_info.InfoTask(config, args)
        self.task.execute()
        self.lines = self.args.logger._printed

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
             backup mine:
               collection local:/data/backup1/mine
                 oldest unverified: 2013-04-09 15:50:21
                 More than 1 year unverified: 2 files, 210kiB
                 More than 1 month unverified: 15 files, 122GiB
               source local:/home/me
                 stored at: home
                 ignored/static/dynamic rules: 3/2/0
             '''))
