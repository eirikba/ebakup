#!/usr/bin/env python3

import datetime
import unittest

import http_handler

class FakeArgs(object): pass
class FakeUIState(object): pass

class TestTemplates(unittest.TestCase):
    def test_start_time(self):
        ui = FakeUIState()
        http = http_handler.HttpHandler(ui)
        ui.start_time = datetime.datetime(2015, 7, 16, 16, 36, 57)
        self.assertEqual(
            b'Started at 2015-07-16 16:36:57!',
            http._expand_template(b'Started at ${start_time}!'))
        self.assertEqual(
            b'2015-07-16 16:36:57',
            http._expand_template(b'${start_time}'))
        self.assertEqual(
            b'at end: 2015-07-16 16:36:57',
            http._expand_template(b'at end: ${start_time}'))
        self.assertEqual(
            b'2015-07-16 16:36:57 at beginning',
            http._expand_template(b'${start_time} at beginning'))
        self.assertEqual(
            b'2015-07-16 16:36:572015-07-16 16:36:57',
            http._expand_template(b'${start_time}${start_time}'))
        self.assertEqual(
            b'2015-07-16 16:36:57',
            http._expand_template(b'${start_time}'))

    def test_args_command(self):
        ui = FakeUIState()
        http = http_handler.HttpHandler(ui)
        ui.args = FakeArgs()
        ui.args.command = 'do something'
        self.assertEqual(
            b'Will do something.',
            http._expand_template(b'Will ${args_command}.'))

    def test_variable(self):
        ui = FakeUIState()
        http = http_handler.HttpHandler(ui)
        self.assertEqual(
            b'And x is [UNKNOWN VARIABLE: x]',
            http._expand_template(b'And x is ${var:x}'))
        http.add_variable(b'x', 'a string')
        self.assertEqual(
            b'And x is a string',
            http._expand_template(b'And x is ${var:x}'))
        http.drop_variable(b'x')
        self.assertEqual(
            b'And x is [UNKNOWN VARIABLE: x]',
            http._expand_template(b'And x is ${var:x}'))
        http.add_variable(b'x', 5)
        self.assertEqual(
            b'And x is 5',
            http._expand_template(b'And x is ${var:x}'))
        self.assertRaisesRegex(
            http_handler.TemplateError, 'ariable "x" is already defined',
            http.add_variable, b'x', 'different')

    def test_invalid_variable_name(self):
        ui = FakeUIState()
        http = http_handler.HttpHandler(ui)
        self.assertRaisesRegex(
            http_handler.TemplateError, 'Invalid variable name: x y',
            http.add_variable, b'x y', 5)
