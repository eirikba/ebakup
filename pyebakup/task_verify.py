#!/usr/bin/env python3

class VerifyResult(object):
    def __init__(self):
        self.errors = []
        self.warnings = []

class VerifyTask(object):
    def __init__(self, collection, services):
        self._collection = collection
        self._services = services
        self._result = VerifyResult()
        self._printResultAtEnd = False

    def printSummaryAfterCompletion(self):
        self._printResultAtEnd = True

    def execute(self):
        self.verify_content_data()
        if self._printResultAtEnd:
            self._printResult()
        return self._result

    def verify_content_data(self):
        checker = ContentDataChecker(self._collection, self._result)
        for cid in self._collection.iterate_contentids():
            checker.check_content_data(cid)

    def _printResult(self):
        print('Results of verifying ' + str(self._collection._path) + ':')
        if not self._result.errors:
            print('No errors')
        else:
            print('ERRORS: (' + str(len(self._result.errors)) + ')')
        for error in self._result.errors:
            print('  ', error)
        if self._result.warnings:
            print('Warnings: (' + str(len(self._result.warnings)) + ')')
        for warning in self._result.warnings:
            print('  ', warning)


class ContentDataChecker(object):
    def __init__(self, collection, result):
        self.collection = collection
        self._result = result

    def check_content_data(self, cid):
        content = self.get_content_reader(cid)
        if content is None:
            self._result.errors.append('Content missing: ' + str(cid))
        elif not self.is_checksum_good(cid, content):
            self._result.errors.append('Content changed: ' + str(cid))

    def get_content_reader(self, cid):
        try:
            content = self.collection.get_content_reader(cid)
        except FileNotFoundError:
            return None
        return content

    def is_checksum_good(self, cid, content):
        cinfo = self.collection.get_content_info(cid)
        content_checksum = self.calculate_checksum(content)
        return content_checksum == cinfo.goodsum

    def calculate_checksum(self, content):
        checksummer = self.collection.get_checksum_algorithm()()
        done = 0
        readsize = 10 * 1024 * 1024
        data = content.get_data_slice(done, readsize)
        while data != b'':
            checksummer.update(data)
            done += len(data)
            data = content.get_data_slice(done, readsize)
        return checksummer.digest()
