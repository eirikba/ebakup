#!/usr/bin/env python3

import io
import unittest

import pyebakup.cli as cli

class FakeArgs(object): pass

class TestSimpleStuff(unittest.TestCase):

    def test_empty_commandline(self):
        stdout = io.StringIO()
        self.assertRaisesRegex(
            SystemExit, '1', cli.main.parse_commandline, '', msgfile=stdout)
        self.assertRegex(stdout.getvalue(), '^usage:')

    def test_commandline_backup(self):
        args = self._parse_commandline(('backup', 'stuff'))
        self.assertEqual('backup', args.command)
        self.assertCountEqual(('stuff',), args.backups)

    def test_commandline_config(self):
        args = self._parse_commandline(
            ('--config', '/home/me/ebakup.config', 'backup', 'home'))
        self.assertEqual(('home', 'me', 'ebakup.config'), args.config)

    def test_commandline_info(self):
        args = self._parse_commandline(('info',))
        self.assertEqual('info', args.command)

    def _parse_commandline(self, cmdline):
        stdout = io.StringIO()
        try:
            args = cli.main.parse_commandline(cmdline, msgfile=stdout)
        except SystemExit:
            self.fail('Parse commandline failed: ' + stdout.getvalue())
        self.assertRegex(stdout.getvalue(), '^$')
        return args

    def test_create_default_services(self):
        services = cli.main.create_services(None, None)
        expected_services = {
            'filesystem': callable,
            'backupoperation': callable,
            'backupstorage.create': callable,
            'backupstorage.open': callable,
            'database.create': callable,
            'database.open': callable,
            'uistate': lambda x: hasattr(x, 'make_http_handler'),
            'utcnow': callable,
            'logger': lambda x: hasattr(x, 'log_error'),
        }
        self.assertCountEqual(expected_services.keys(), services.keys())
        for service, what in expected_services.items():
            self.assertTrue(
                what(services[service]),
                msg='Not correct: ' + service + ' is ' + str(services[service]))
