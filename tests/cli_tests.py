#!/usr/bin/env python3

import io
import unittest

import cli

class FakeArgs(object): pass

class TestSimpleStuff(unittest.TestCase):

    def test_empty_commandline(self):
        stdout = io.StringIO()
        self.assertRaisesRegex(
            SystemExit, '1', cli.parse_commandline, '', msgfile=stdout)
        self.assertRegex(stdout.getvalue(), '^usage:')

    def test_commandline_config(self):
        args = self._parse_commandline(
            ('--config', '/home/me/ebakup.config', 'backup', 'home'))
        self.assertEqual(('home', 'me', 'ebakup.config'), args.config)

    def _parse_commandline(self, cmdline):
        stdout = io.StringIO()
        try:
            args = cli.parse_commandline(cmdline, msgfile=stdout)
        except SystemExit:
            self.fail('Parse commandline failed: ' + stdout.getvalue())
        self.assertRegex(stdout.getvalue(), '^$')
        return args

