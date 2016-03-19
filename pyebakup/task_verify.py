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
        for cid in self._collection.iterate_contentids():
            self.verify_cid(cid)
        if self._printResultAtEnd:
            self._printResult()
        return self._result

    def verify_cid(self, cid):
        try:
            content = self._collection.get_content_reader(cid)
        except FileNotFoundError:
            self._result.errors.append('Content missing: ' + str(cid))
            return
        cinfo = self._collection.get_content_info(cid)
        checksummer = self._collection.get_checksum_algorithm()()
        done = 0
        readsize = 10 * 1024 * 1024
        data = content.get_data_slice(done, readsize)
        while data != b'':
            checksummer.update(data)
            done += len(data)
            data = content.get_data_slice(done, readsize)
        if checksummer.digest() != cinfo.goodsum:
            self._result.errors.append('Content changed: ' + str(cid))

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
