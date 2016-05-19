#!/usr/bin/env python3

import unittest

import pyebakup.cli.task_verify as task_verify


def checksum(data):
    return b'cksum:' + data


class ArgsStub(object):
    def __init__(self):
        self.services = {
            'backupcollection.open': lambda x, y: x,
        }


class ConfigStub(object):
    def __init__(self):
        self.backups = []

    def _add_single_collection_backup(self, coll):
        self.backups.append(ConfigBackupStub())
        self.backups[-1].collections.append(ConfigCollectionStub(coll))


class ConfigBackupStub(object):
    def __init__(self):
        self.collections = []


class ConfigCollectionStub(object):
    def __init__(self, collection):
        self.filesystem = collection
        self.path = None


class Empty(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class FileReaderStub(object):
    def __init__(self, content):
        self._content = content

    def get_data_slice(self, start, end):
        return self._content[start:end]


class ChecksummerFake(object):
    def __init__(self):
        self._data = b''

    def update(self, data):
        self._data += data

    def digest(self):
        return checksum(self._data)


class EmptyBackupCollectionStub(object):
    def iterate_contentids(self):
        for cid in ():
            yield cid


class Content(object):
    def __init__(self, content):
        self.content = content
        self.checksum = checksum(content)

    def _override_content(self, content):
        self.content = content


class SingleBackupCollectionStub(object):
    def __init__(self):
        self._contents = {
            b'cid123': Content(b'data for file 123')
            }

    def _override_content(self, cid, content):
        self._contents[cid]._override_content(content)

    def get_checksum_algorithm(self):
        return ChecksummerFake

    def iterate_contentids(self):
        for cid in self._contents:
            yield cid

    def get_content_info(self, cid):
        c = self._contents.get(cid)
        return Empty(goodsum=c.checksum)

    def get_content_reader(self, cid):
        c = self._contents.get(cid)
        if c.content is None:
            raise FileNotFoundError(
                'no such file or directory: /testcollection/content/' +
                str(cid))
        return FileReaderStub(c.content)


class TestTaskVerify(unittest.TestCase):
    def setUp(self):
        self.args = ArgsStub()
        self.config = ConfigStub()

    def test_verify_empty_collection_is_ok(self):
        self.config._add_single_collection_backup(EmptyBackupCollectionStub())
        task = task_verify.VerifyTask(self.config, self.args)
        result = task.execute()
        self.assertEqual(0, len(result.errors))
        self.assertEqual(0, len(result.warnings))

    def test_verify_single_backup_collection_is_ok(self):
        self.config._add_single_collection_backup(SingleBackupCollectionStub())
        task = task_verify.VerifyTask(self.config, self.args)
        result = task.execute()
        self.assertEqual(0, len(result.errors))
        self.assertEqual(0, len(result.warnings))

    def test_verify_single_backup_collection_with_missing_content(self):
        coll = SingleBackupCollectionStub()
        coll._override_content(b'cid123', None)
        self.config._add_single_collection_backup(coll)
        task = task_verify.VerifyTask(self.config, self.args)
        result = task.execute()
        self.assertEqual(1, len(result.errors))
        self.assertEqual(0, len(result.warnings))
        self.assertEqual("Content missing: 636964313233", result.errors[0])

    def test_verify_single_backup_collection_with_corrupt_content(self):
        coll = SingleBackupCollectionStub()
        coll._override_content(b'cid123', b'different data')
        self.config._add_single_collection_backup(coll)
        task = task_verify.VerifyTask(self.config, self.args)
        result = task.execute()
        self.assertEqual(1, len(result.errors))
        self.assertEqual(0, len(result.warnings))
        self.assertEqual(
            "Content not matching checksum: 636964313233", result.errors[0])


# Missing:
#
# Checking other collections than the first
# Checking multiple collections
# Comparing collections to each other
# Handling collections that are not currently accessible
# Missing cids
#  - found in backups
#  - found in content store (do we care?)
# Content data consistency
#  - Corrupt content database blocks
# Snapshot consistency
#  - Corrupt snapshot file blocks
# Storing verification results
#  - For fixing later
#  - For historical tracking
#  - Have verify by default not check "recently" verified data
