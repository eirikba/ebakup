#!/usr/bin/env python3

import subprocess

from ebakup_live_helpers.common import root_path


class EbakupInvocation(object):

    def __init__(self, *args):
        self.args = args
        self.testcase = None
        self.timeout = 5
        self._allow_default_config = False

    def set_testcase(self, tc):
        assert self.testcase is None
        self.testcase = tc

    def allowDefaultConfig(self):
        self._allow_default_config = True

    def run(self):
        self.assertArgsHasSuitableConfig()
        self.result = subprocess.run(
            ('./ebakup',) + self.args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout)

    def assertArgsHasSuitableConfig(self):
        found = False
        checknext = False
        for arg in self.args:
            if checknext:
                found = True
                self.testcase.assertIn(root_path, arg)
                checknext = False
            if arg == '--config':
                checknext = True
        if not self._allow_default_config:
            self.testcase.assertTrue(found)
        self.testcase.assertFalse(checknext)

    def assertSuccess(self):
        if self.result.returncode != 0:
            self.printResult()
        self.testcase.assertEqual(0, self.result.returncode)

    def assertFailed(self):
        self.testcase.assertNotEqual(0, self.result.returncode)

    def assertOutputMatchesRegex(self, regex):
        self.testcase.assertRegex(self.result.stdout, regex)

    def assertOutputEmpty(self):
        stdout, stderr = self._get_interesting_output()
        self.testcase.assertEqual(b'', stdout)
        self.testcase.assertEqual(b'', stderr)

    def printResult(self):
        print('Result of ./ebakup ' + str(self.args) + ':')
        print('returncode:', self.result.returncode)
        print('------ stderr ------')
        print(self.result.stderr.decode('utf-8'))
        print('---- stderr end ----')
        print('------ stdout ------')
        print(self.result.stdout.decode('utf-8'))
        print('---- stdout end ----')

    def _get_interesting_output(self):
        stdout = self.result.stdout
        if stdout.startswith(b'Web ui started on port '):
            port = int(stdout[23:27], 10)
            self.testcase.assertEqual(b'\n', stdout[27:28])
            stdout = stdout[28:]
        return stdout, self.result.stderr
