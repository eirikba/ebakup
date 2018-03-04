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
        self._config = config
        self._args = args
        self._result = VerifyResult()
        self._printResultAtEnd = False

    def printSummaryAfterCompletion(self):
        self._printResultAtEnd = True

    def execute(self):
        for bk in self._config.backups:
            for cfgstorage in bk.storages:
                storage = self._args.services['backupstorage.open'](
                    cfgstorage.filesystem, cfgstorage.path)
                verifier = VerifyStorage(storage)
                verifier.verify(self._result)
        if self._printResultAtEnd:
            self._printResult()
        return self._result

    def _printResult(self):
        print('Results of verifying:')
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
