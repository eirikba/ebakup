#!/usr/bin/env python3

import collections
import datetime
import hashlib
import unittest

import pyebakup.backupstorage.backupcollection as backupcollection
import pyebakup.backup.backupoperation as backupoperation
import fake_filesys

from pyebakup.config.config_subtree import CfgSubtree

def make_cfgsubtree(spec):
    root = CfgSubtree(None, None)
    for item in spec:
        root._add_child_path(item[0], item[1], handler=item[2])
    return root

class TestSimpleBackup(unittest.TestCase):
    def setUp(self):
        storetree = fake_filesys.FakeFileSystem()
        storetree._allow_full_access_to_subtree(('path', 'to', 'backup'))
        collection = backupcollection.create_collection(
            storetree, ('path', 'to', 'backup'))
        sourcetree = fake_filesys.FakeFileSystem()
        basepath = ('home', 'me')
        sourcetree._make_files(
            basepath, ('.emacs', 'notes.txt'), fileid_first=0)
        sourcetree._make_files(
            basepath + ('.cache', 'chromium', 'Default', 'Cache'),
            ('index', 'data_0', 'data_1', 'f_000001', 'f_000002', 'f_000003'),
            fileid_first=10)
        sourcetree._make_files(
            basepath + ('Documents',), ('Letter.odt', 'Photo.jpg'),
            fileid_first=20)
        sourcetree._make_files(
            basepath + ('tmp',), ('scratchpad.txt', 'funny.jpg', 'Q.pdf'),
            fileid_first=30)
        sourcetree._make_files(
            basepath + ('Pictures', 'Christmas'),
            ('DSC_1886.JPG', 'DSC_1887.JPG', 'DSC_1888.JPG', 'DSC_1889.JPG'),
            fileid_first=40)
        sourcetree._allow_reading_subtree(basepath)
        operation = backupoperation.BackupOperation(collection)
        backuptree = operation.add_tree_to_backup(sourcetree, basepath, ())
        bkroot = make_cfgsubtree(
            (('plain', ('tmp',), 'ignore'),
             ('plain', ('tmp', 'Q.pdf'), 'dynamic'),
             ('plain', ('.cache',), 'ignore'),
             ('plain', ('Pictures',), 'static')))
        backuptree.set_backup_handlers(bkroot)
        self.storetree = storetree
        self.sourcetree = sourcetree
        self.basepath = basepath
        self.backuptree = backuptree
        self.collection = collection
        self.operation = operation
        self.before_backup = datetime.datetime.utcnow()
        operation.execute_backup()
        self.after_backup = datetime.datetime.utcnow()
        self.collection2 = backupcollection.open_collection(
            storetree, ('path', 'to', 'backup'))

    def test_single_backup_created(self):
        backup = self.collection2.get_most_recent_backup()
        self.assertNotEqual(None, backup)
        self.assertEqual(
            None,
            self.collection2.get_most_recent_backup_before(
                backup.get_start_time()))

    def test_start_end_times_sensible(self):
        backup = self.collection2.get_most_recent_backup()
        self.assertLessEqual(
            self.before_backup.replace(microsecond=0),
            backup.get_start_time())
        self.assertLessEqual(
            backup.get_start_time(), backup.get_end_time())
        self.assertLessEqual(
            backup.get_end_time(), self.after_backup)

    def test_correct_files_backed_up(self):
        backup = self.collection2.get_most_recent_backup()
        dirs, files = backup.list_directory(())
        self.assertCountEqual(('Documents','tmp','Pictures'), dirs)
        self.assertCountEqual(('.emacs','notes.txt'), files)
        dirs, files = backup.list_directory(('Documents',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('Letter.odt', 'Photo.jpg'), files)
        dirs, files = backup.list_directory(('tmp',))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(('Q.pdf',), files)
        dirs, files = backup.list_directory(('Pictures',))
        self.assertCountEqual(('Christmas',), dirs)
        self.assertCountEqual((), files)
        dirs, files = backup.list_directory(('Pictures','Christmas'))
        self.assertCountEqual((), dirs)
        self.assertCountEqual(
            ('DSC_1886.JPG', 'DSC_1887.JPG', 'DSC_1888.JPG', 'DSC_1889.JPG'),
            files)

    def test_backed_up_files_have_correct_content(self):
        backup = self.collection2.get_most_recent_backup()
        source = self.sourcetree
        basepath = self.basepath
        for path in (
                ('.emacs',), ('notes.txt',), ('Documents', 'Letter.odt'),
                ('Documents', 'Photo.jpg'), ('tmp', 'Q.pdf'),
                ('Pictures', 'Christmas', 'DSC_1886.JPG'),
                ('Pictures', 'Christmas', 'DSC_1887.JPG'),
                ('Pictures', 'Christmas', 'DSC_1888.JPG'),
                ('Pictures', 'Christmas', 'DSC_1889.JPG'),
                ):
            self._check_same_content(
                source, basepath + path, backup, path)

    def _check_same_content(self, tree, path, backup, bkpath):
        bkfile = backup.get_file_info(bkpath)
        cid = bkfile.contentid
        content = self.collection2.get_content_reader(cid)
        orig = tree.get_item_at_path(path)
        self.assertEqual(orig.get_size(), content.get_size())
        origdata = orig.get_data_slice(0, orig.get_size())
        contentdata = content.get_data_slice(0, content.get_size())
        self.assertEqual(
            origdata, contentdata, msg='Content differ: ' + str(path))

    def test_backed_up_files_have_correct_metadata(self):
        backup = self.collection2.get_most_recent_backup()
        source = self.sourcetree
        basepath = self.basepath
        for path in (
                ('.emacs',), ('notes.txt',), ('Documents', 'Letter.odt'),
                ('Documents', 'Photo.jpg'), ('tmp', 'Q.pdf'),
                ('Pictures', 'Christmas', 'DSC_1886.JPG'),
                ('Pictures', 'Christmas', 'DSC_1887.JPG'),
                ('Pictures', 'Christmas', 'DSC_1888.JPG'),
                ('Pictures', 'Christmas', 'DSC_1889.JPG'),
                ):
            bkfile = backup.get_file_info(path)
            orig = source.get_item_at_path(basepath + path)
            mtime, mtime_ns = orig.get_mtime()
            self.assertEqual(
                mtime, bkfile.mtime, msg='mtime differs: ' + str(path))
            self.assertEqual(
                mtime_ns, bkfile.mtime_nsec,
                msg='mtime_ns differs: ' + str(path))
            self.assertEqual(orig.get_size(), bkfile.size)

    def test_backed_up_files_have_correct_checksum(self):
        backup = self.collection2.get_most_recent_backup()
        source = self.sourcetree
        basepath = self.basepath
        for path in (
                ('.emacs',), ('notes.txt',), ('Documents', 'Letter.odt'),
                ('Documents', 'Photo.jpg'), ('tmp', 'Q.pdf'),
                ('Pictures', 'Christmas', 'DSC_1886.JPG'),
                ('Pictures', 'Christmas', 'DSC_1887.JPG'),
                ('Pictures', 'Christmas', 'DSC_1888.JPG'),
                ('Pictures', 'Christmas', 'DSC_1889.JPG'),
                ):
            bkfile = backup.get_file_info(path)
            orig = source.get_item_at_path(basepath + path)
            data = orig.get_data_slice(0, orig.get_size())
            checksum = hashlib.sha256(data).digest()
            self.assertEqual(
                checksum, bkfile.good_checksum,
                msg='checksum is wrong: ' + str(path))
