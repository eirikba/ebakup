#!/usr/bin/env python3

from pyebakup.verify.verifystorage import VerifyStorage


def hexstr(d):
    return ''.join('{:02x}'.format(x) for x in d)


class VerifyResult(object):
    def __init__(self):
        self.errors = []
        self.warnings = []

    def content_missing(self, cid):
        self.errors.append('Content missing: ' + hexstr(cid))

    def content_corrupt(self, cid):
        self.errors.append('Content not matching checksum: ' + hexstr(cid))


class VerifyTask(object):
    def __init__(self, config, args):
        cfgcollection = config.backups[0].collections[0]
        self._collection = args.services['backupcollection.open'](
            cfgcollection.filesystem, cfgcollection.path)
        self._result = VerifyResult()
        self._printResultAtEnd = False

    def printSummaryAfterCompletion(self):
        self._printResultAtEnd = True

    def execute(self):
        verifier = VerifyStorage(self._collection)
        verifier.verify(self._result)
        if self._printResultAtEnd:
            self._printResult()
        return self._result

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
