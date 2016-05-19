#!/usr/bin/env python3

from .contentdatachecker import ContentDataChecker

class VerifyStorage(object):
    def __init__(self, storage):
        self.storage = storage

    def verify(self, reporter):
        contentchecker = ContentDataChecker(self.storage, reporter)
        for cid in self.storage.iterate_contentids():
            contentchecker.check_content_data(cid)

