#!/usr/bin/env python3

import unittest

import pyebakup.config.glob_utils as globs

class TestSimpleGlobs(unittest.TestCase):

    def test_valid_globs(self):
        self.assertTrue(globs.is_valid_glob('abcd'))
        self.assertTrue(globs.is_valid_glob('ab*cd'))
        self.assertTrue(globs.is_valid_glob('*abcd'))
        self.assertTrue(globs.is_valid_glob('abcd*'))
        self.assertTrue(globs.is_valid_glob('a*b*c*d'))
        self.assertTrue(globs.is_valid_glob('a[bc]d'))
        self.assertTrue(globs.is_valid_glob('ab?cd'))
        self.assertTrue(globs.is_valid_glob('?abcd'))
        self.assertTrue(globs.is_valid_glob('abcd?'))
        self.assertTrue(globs.is_valid_glob('a?bc?d'))
        self.assertTrue(globs.is_valid_glob('a[bc]d'))
        self.assertTrue(globs.is_valid_glob('[abcd]'))
        self.assertTrue(globs.is_valid_glob('[a]bcd'))
        self.assertTrue(globs.is_valid_glob('abc[d]'))
        self.assertTrue(globs.is_valid_glob('a[!bc]d'))
        self.assertTrue(globs.is_valid_glob('[!a]bcd'))
        self.assertTrue(globs.is_valid_glob('abc[!d]'))
        self.assertTrue(globs.is_valid_glob('[ab][cd]'))
        self.assertTrue(globs.is_valid_glob('[ab[]cd'))
        self.assertTrue(globs.is_valid_glob('a[[]bcd'))
        self.assertTrue(globs.is_valid_glob('a[b[]cd'))
        self.assertTrue(globs.is_valid_glob('a[]]bcd'))
        self.assertTrue(globs.is_valid_glob('a[]bc]d'))
        self.assertTrue(globs.is_valid_glob('a[!bc]d'))
        self.assertTrue(globs.is_valid_glob('[!a]bcd'))
        self.assertTrue(globs.is_valid_glob('abc[!d]'))
        self.assertTrue(globs.is_valid_glob('ab[![]cd'))
        self.assertTrue(globs.is_valid_glob('ab[-]cd'))
        self.assertTrue(globs.is_valid_glob('ab[]-]cd'))
        self.assertTrue(globs.is_valid_glob('ab[-n]cd'))
        self.assertTrue(globs.is_valid_glob('ab[]-n]cd'))
        self.assertTrue(globs.is_valid_glob('ab[!-]cd'))
        self.assertTrue(globs.is_valid_glob('ab[!-n]cd'))
        self.assertTrue(globs.is_valid_glob('ab[!]-n]cd'))
        self.assertTrue(globs.is_valid_glob('ab[d-u]cd'))
        self.assertTrue(globs.is_valid_glob('ab[M-R]cd'))
        self.assertTrue(globs.is_valid_glob('ab[3-8]cd'))
        self.assertTrue(globs.is_valid_glob('ab[0-9]cd'))

    def test_invalid_globs(self):
        self.assertFalse(globs.is_valid_glob('ab[cd'))
        self.assertFalse(globs.is_valid_glob('[abcd'))
        self.assertFalse(globs.is_valid_glob('abcd['))
        self.assertFalse(globs.is_valid_glob('ab]cd'))
        self.assertFalse(globs.is_valid_glob(']abcd'))
        self.assertFalse(globs.is_valid_glob('abcd]'))
        self.assertFalse(globs.is_valid_glob('[ab[]cd]'))
        self.assertFalse(globs.is_valid_glob('a[b]c]d'))
        self.assertFalse(globs.is_valid_glob('a[!bcd'))
        self.assertFalse(globs.is_valid_glob('ab[!c]]d'))
        self.assertFalse(globs.is_valid_glob('ab[-]]cd'))
        self.assertFalse(globs.is_valid_glob('ab[f-R]cd'))
        self.assertFalse(globs.is_valid_glob('ab[M-q]cd'))
        self.assertFalse(globs.is_valid_glob('ab[.-/]cd'))
        self.assertFalse(globs.is_valid_glob('ab[a-]cd'))
        self.assertFalse(globs.is_valid_glob('ab[4-d]cd'))
        self.assertFalse(globs.is_valid_glob('ab[f-8]cd'))
        self.assertFalse(globs.is_valid_glob('ab[--]cd'))
        self.assertFalse(globs.is_valid_glob('ab[]]]cd'))
        self.assertFalse(globs.is_valid_glob('ab[f-k-q]cd'))

    def test_trivial_matches(self):
        self.assertTrue(globs.does_glob_match('abcd', 'abcd'))
        self.assertFalse(globs.does_glob_match('abc', 'abcd'))
        self.assertFalse(globs.does_glob_match('abcd', 'abc'))

        self.assertTrue(globs.does_glob_match('*', 'abcd'))
        self.assertFalse(globs.does_glob_match('abcd', '*'))

        self.assertTrue(globs.does_glob_match('?', 'c'))
        self.assertFalse(globs.does_glob_match('c', '?'))

    def test_simple_matches(self):
        self.assertTrue(globs.does_glob_match('abcd*', 'abcd'))
        self.assertTrue(globs.does_glob_match('*abcd', 'abcd'))
        self.assertTrue(globs.does_glob_match('ab*', 'abcd'))
        self.assertTrue(globs.does_glob_match('a*d', 'abcd'))
        self.assertTrue(globs.does_glob_match('ab*cd', 'abcd'))
        self.assertTrue(globs.does_glob_match('*d', 'abcd'))
        self.assertTrue(globs.does_glob_match('a*bc*d', 'abcd'))
        self.assertTrue(globs.does_glob_match('a*bc*d', 'antdbcethted'))
        self.assertTrue(globs.does_glob_match('ab?d', 'abcd'))
        self.assertTrue(globs.does_glob_match('?bcd', 'abcd'))
        self.assertTrue(globs.does_glob_match('abc?', 'abcd'))
        self.assertTrue(globs.does_glob_match('abc?*', 'abcd'))
        self.assertTrue(globs.does_glob_match('abc*?', 'abcd'))
        self.assertTrue(globs.does_glob_match('abc*?*', 'abcd'))
        self.assertTrue(globs.does_glob_match('a[b]cd', 'abcd'))
        self.assertTrue(globs.does_glob_match('a[abcd]cd', 'abcd'))
        self.assertTrue(globs.does_glob_match('a[!acd]cd', 'abcd'))
        self.assertTrue(globs.does_glob_match('ab[,]cd', 'ab,cd'))
        self.assertTrue(globs.does_glob_match('ab[ab,(]cd', 'ab,cd'))
        self.assertTrue(globs.does_glob_match('ab[!.]cd', 'ab,cd'))
        self.assertTrue(globs.does_glob_match('ab[!m7/)]cd', 'ab,cd'))
        self.assertTrue(globs.does_glob_match('a*[abcd]cd', 'abcd'))
        self.assertTrue(globs.does_glob_match('a*[abcd]cd', 'athtobcd'))
        self.assertTrue(globs.does_glob_match('a*[abcd]cd', 'abcbdbcd'))
        self.assertTrue(globs.does_glob_match('ab*[!m7/)]cd', 'ab,cd'))
        self.assertTrue(globs.does_glob_match('ab*[!m7/)]cd', 'abtoheu,cd'))
        self.assertTrue(globs.does_glob_match('ab*[!m7/)]cd', 'abm7m,cd'))

    def test_simple_non_matches(self):
        self.assertFalse(globs.does_glob_match('*abcd', 'bcd'))
        self.assertFalse(globs.does_glob_match('ab*cd', 'acd'))
        self.assertFalse(globs.does_glob_match('ab*cd', 'abd'))
        self.assertFalse(globs.does_glob_match('abcd*', 'abc'))
        self.assertFalse(globs.does_glob_match('ab?cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('ab?cd', 'acd'))
        self.assertFalse(globs.does_glob_match('abcd?', 'abcd'))
        self.assertFalse(globs.does_glob_match('abcd*?', 'abcd'))
        self.assertFalse(globs.does_glob_match('abcd?*', 'abcd'))
        self.assertFalse(globs.does_glob_match('abcd*?*', 'abcd'))
        self.assertFalse(globs.does_glob_match('a[acd]cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('a[!b]cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('a[!abcd]cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('ab[,]cd', 'ab.cd'))
        self.assertFalse(globs.does_glob_match('ab[!,]cd', 'ab,cd'))
        self.assertFalse(globs.does_glob_match('ab[ab,(]cd', 'ab.cd'))
        self.assertFalse(globs.does_glob_match('a*[acd]cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('a*[acd]cd', 'athtnbcd'))
        self.assertFalse(globs.does_glob_match('a*[acd]cd', 'aaccabcd'))
        self.assertFalse(globs.does_glob_match('ab*[!,]cd', 'ab,cd'))
        self.assertFalse(globs.does_glob_match('ab*[!,]cd', 'abtht,cd'))
        self.assertFalse(globs.does_glob_match('ab*[!,]cd', 'ab,,,,cd'))

    def test_range_matches(self):
        self.assertTrue(globs.does_glob_match('ab[a-z]cd', 'abgcd'))
        self.assertTrue(globs.does_glob_match('ab[a-z]cd', 'abacd'))
        self.assertTrue(globs.does_glob_match('ab[a-z]cd', 'abzcd'))
        self.assertTrue(globs.does_glob_match('ab[n]cd', 'abncd'))
        self.assertTrue(globs.does_glob_match('ab[f-i]cd', 'abhcd'))
        self.assertTrue(globs.does_glob_match('ab[f-i]cd', 'abfcd'))
        self.assertTrue(globs.does_glob_match('ab[f-i]cd', 'abicd'))
        self.assertTrue(globs.does_glob_match('ab[0-9]cd', 'ab3cd'))
        self.assertTrue(globs.does_glob_match('ab[0-9]cd', 'ab0cd'))
        self.assertTrue(globs.does_glob_match('ab[0-9]cd', 'ab9cd'))
        self.assertTrue(globs.does_glob_match('ab[2-6]cd', 'ab3cd'))
        self.assertTrue(globs.does_glob_match('ab[2-6]cd', 'ab2cd'))
        self.assertTrue(globs.does_glob_match('ab[2-6]cd', 'ab6cd'))
        self.assertTrue(globs.does_glob_match('ab[fd-kt]cd', 'abfcd'))
        self.assertTrue(globs.does_glob_match('ab[fd-kt]cd', 'abhcd'))
        self.assertTrue(globs.does_glob_match('ab[fd-kt]cd', 'abtcd'))
        self.assertTrue(globs.does_glob_match('ab[A-Z]cd', 'abNcd'))
        self.assertTrue(globs.does_glob_match('ab[A-Z]cd', 'abAcd'))
        self.assertTrue(globs.does_glob_match('ab[A-Z]cd', 'abZcd'))
        self.assertTrue(globs.does_glob_match('ab[F]cd', 'abFcd'))
        self.assertTrue(globs.does_glob_match('ab[!4-7]cd', 'ab3cd'))
        self.assertTrue(globs.does_glob_match('ab[!4-7]cd', 'ab8cd'))
        self.assertTrue(globs.does_glob_match('ab[!4-7]cd', 'ab0cd'))
        self.assertTrue(globs.does_glob_match('ab[!4-7]cd', 'abtcd'))
        self.assertTrue(globs.does_glob_match('ab[!4-7]cd', 'abrcd'))
        self.assertTrue(globs.does_glob_match('ab[!4-7]cd', 'abKcd'))
        self.assertTrue(globs.does_glob_match('ab[!4-7]cd', 'ab.cd'))
        self.assertTrue(globs.does_glob_match('ab*[2-6]cd', 'ab3cd'))
        self.assertTrue(globs.does_glob_match('ab*[2-6]cd', 'abeothtn3cd'))
        self.assertTrue(globs.does_glob_match('ab*[2-6]cd', 'abe45363cd'))
        self.assertTrue(globs.does_glob_match('ab*[!4-7]cd', 'ab.cd'))
        self.assertTrue(globs.does_glob_match('ab*[!4-7]cd', 'abtheto.cd'))
        self.assertTrue(globs.does_glob_match('ab*[!4-7]cd', 'ab4536.cd'))

    def test_range_non_matches(self):
        self.assertFalse(globs.does_glob_match('ab[a-z]cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('ab[a-z]cd', 'ab4cd'))
        self.assertFalse(globs.does_glob_match('ab[a-z]cd', 'abDcd'))
        self.assertFalse(globs.does_glob_match('ab[a-z]cd', 'ab,cd'))
        self.assertFalse(globs.does_glob_match('ab[0-9]cd', 'abtcd'))
        self.assertFalse(globs.does_glob_match('ab[2-6]cd', 'ab8cd'))
        self.assertFalse(globs.does_glob_match('ab[2-6]cd', 'ab7cd'))
        self.assertFalse(globs.does_glob_match('ab[2-6]cd', 'ab9cd'))
        self.assertFalse(globs.does_glob_match('ab[2-6]cd', 'ab1cd'))
        self.assertFalse(globs.does_glob_match('ab[2-6]cd', 'ab0cd'))
        self.assertFalse(globs.does_glob_match('ab[fd-kt]cd', 'abbcd'))
        self.assertFalse(globs.does_glob_match('ab[fd-kt]cd', 'abmcd'))
        self.assertFalse(globs.does_glob_match('ab[fd-kt]cd', 'abzcd'))
        self.assertFalse(globs.does_glob_match('ab[fd-kt]cd', 'ab4cd'))
        self.assertFalse(globs.does_glob_match('ab[n]cd', 'abmcd'))
        self.assertFalse(globs.does_glob_match('ab[n]cd', 'abocd'))
        self.assertFalse(globs.does_glob_match('ab[f-i]cd', 'abecd'))
        self.assertFalse(globs.does_glob_match('ab[f-i]cd', 'abjcd'))
        self.assertFalse(globs.does_glob_match('ab[A-Z]cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('ab[C-R]cd', 'abecd'))
        self.assertFalse(globs.does_glob_match('ab[C-R]cd', 'abBcd'))
        self.assertFalse(globs.does_glob_match('ab[C-R]cd', 'abScd'))
        self.assertFalse(globs.does_glob_match('ab[!4-7]cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('ab[!4-7]cd', 'ab4cd'))
        self.assertFalse(globs.does_glob_match('ab[!4-7]cd', 'ab5cd'))
        self.assertFalse(globs.does_glob_match('ab[!4-7]cd', 'ab7cd'))
        self.assertFalse(globs.does_glob_match('ab*[2-6]cd', 'ab.cd'))
        self.assertFalse(globs.does_glob_match('ab*[2-6]cd', 'abthon.cd'))
        self.assertFalse(globs.does_glob_match('ab*[2-6]cd', 'ab4536.cd'))
        self.assertFalse(globs.does_glob_match('ab*[!4-7]cd', 'ab6cd'))
        self.assertFalse(globs.does_glob_match('ab*[!4-7]cd', 'ab6cd'))

    def test_special_chars(self):
        self.assertTrue(globs.does_glob_match('ab*cd', 'ab*cd'))
        self.assertTrue(globs.does_glob_match('ab[*]cd', 'ab*cd'))
        self.assertTrue(globs.does_glob_match('ab*cd', 'abtcd'))
        self.assertFalse(globs.does_glob_match('ab[*]cd', 'abtcd'))
        self.assertTrue(globs.does_glob_match('ab*cd', 'abcd'))
        self.assertFalse(globs.does_glob_match('ab[*]cd', 'abcd'))
        self.assertTrue(globs.does_glob_match('ab*cd', 'ablongecd'))
        self.assertFalse(globs.does_glob_match('ab[*]cd', 'ablongecd'))

        self.assertTrue(globs.does_glob_match('ab?cd', 'ab?cd'))
        self.assertTrue(globs.does_glob_match('ab[?]cd', 'ab?cd'))
        self.assertTrue(globs.does_glob_match('ab?cd', 'abtcd'))
        self.assertFalse(globs.does_glob_match('ab[?]cd', 'abtcd'))

        self.assertTrue(globs.does_glob_match('ab-cd', 'ab-cd'))
        self.assertTrue(globs.does_glob_match('ab[-]cd', 'ab-cd'))
        self.assertFalse(globs.does_glob_match('ab[-]cd', 'ab[-]cd'))
        self.assertTrue(globs.does_glob_match('ab[]]cd', 'ab]cd'))
        self.assertFalse(globs.does_glob_match('ab[]]cd', 'ab[]]cd'))

    def test_has_common_matches(self):
        tests = (
            ('abcd', 'abcd'),
            ('a[f-k]bcd', 'a[i-m]bcd'),
            ('a[f-k]bcd', 'a[!i-m]bcd'),
            ('a[!f-k]bcd', 'a[i-m]bcd'),
            ('a[!f-k]bcd', 'a[a-r]bcd'),
            ('a[!f-k]bcd', 'a[!i-m]bcd'),
            ('a?cd', 'abcd'),
            ('?bcd', 'abcd'),
            ('abc?', 'abcd'),
            ('ab*cd', 'abcd'),
            ('ab*cd', 'abmnocd'),
            ('ab*k*cd', 'abkcd'),
            ('ab*kcd', 'abkcd'),
            ('ab*k*k*cd', 'abkmkcd'),
            ('a*bcd', 'abc*d'),
            ('ab*cd', 'ab*cd'),
            ('ab*cd*efg', 'ab*e*g'),
            ('ab*cd*', 'ab*ef*'),
            ('abcd*', 'ab*ef'),
            ('*abcd*', 'gh*ef'),
            ('*abcd*ef', 'gh*ef'),
            ('a*[!b]*cd', 'ab*cd'),
            ('ab*kcd', 'abkmkcd'),
            )
        for test in tests:
            self.assertTrue(
                globs.do_globs_have_common_matches(test[0], test[1]),
                msg=str(test))
            self.assertTrue(
                globs.do_globs_have_common_matches(test[1], test[0]),
                msg=str((test[1],test[0])))

    def test_does_not_have_common_matches(self):
        tests = (
            ('a[bc]d', 'abcd'),
            ('a[!b]*cd', 'ab*cd'),
            ('ab?cd', 'abcd'),
            ('ab???cd', 'ab??cd'),
            ('a[f-k]bcd', 'a[m-r]bcd'),
            ('a[f-k]bcd', 'a[!a-r]bcd'),
            ('ab*cd', 'ab*ef'),
            ('*abcd*ef', 'gh*mn'),
            ('ab*kcd', 'abcd'),
            ('ab*k*cd', 'abcd'),
            ('ab*k*k*cd', 'abkcd'),
        )
        for test in tests:
            self.assertFalse(
                globs.do_globs_have_common_matches(test[0], test[1]),
                msg=str(test))
            self.assertFalse(
                globs.do_globs_have_common_matches(test[1], test[0]),
                msg=str((test[1],test[0])))
