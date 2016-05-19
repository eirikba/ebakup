#!/usr/bin/env python3

import unittest

from pyebakup.verify.verifystorage import VerifyStorage


class Empty(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class ResultSpy(object):
    def __init__(self):
        self.errors = set()

    def content_missing(self, cid):
        self.errors.add(b'missing:' + cid)

    def content_corrupt(self, cid):
        self.errors.add(b'corrupt:' + cid)


class EmptyStorageStub(object):
    def iterate_contentids(self):
        for cid in ():
            yield cid


def checksum(data):
    return b'cksum:' + data


class ChecksummerFake(object):
    def __init__(self):
        self._data = b''

    def update(self, data):
        self._data += data

    def digest(self):
        return checksum(self._data)


class Content(object):
    def __init__(self, content):
        self.content = content
        self.checksum = checksum(content)

    def _override_content(self, content):
        self.content = content


class SingleContentStorageStub(object):
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


class FileReaderStub(object):
    def __init__(self, content):
        self._content = content

    def get_data_slice(self, start, end):
        return self._content[start:end]


class TestVerifyStorage(unittest.TestCase):
    def test_verify_empty_collection_is_ok(self):
        verifier = VerifyStorage(EmptyStorageStub())
        result = ResultSpy()
        verifier.verify(result)
        self.assertEqual(set(), result.errors)

    def test_verify_single_backup_collection_is_ok(self):
        verifier = VerifyStorage(SingleContentStorageStub())
        result = ResultSpy()
        verifier.verify(result)
        self.assertEqual(set(), result.errors)

    def test_verify_single_backup_collection_with_missing_content(self):
        storage = SingleContentStorageStub()
        storage._override_content(b'cid123', None)
        verifier = VerifyStorage(storage)
        result = ResultSpy()
        verifier.verify(result)
        self.assertEqual(set((b'missing:cid123',)), result.errors)

    def test_verify_single_backup_collection_with_corrupt_content(self):
        storage = SingleContentStorageStub()
        storage._override_content(b'cid123', b'different data')
        verifier = VerifyStorage(storage)
        result = ResultSpy()
        verifier.verify(result)
        self.assertEqual(set((b'corrupt:cid123',)), result.errors)
