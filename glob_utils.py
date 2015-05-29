#!/usr/bin/env python3

# The globs of this module supports these special patterns:
#  * - Zero or more of characters. Any characters.
#  ? - Exactly 1 character. Any character.
#  [<chars>] - Exactly 1 character. One of <chars>.
#  [!<chars>] - Exactly 1 character. None of <chars>.
#
# The <chars> part of the [] patterns supports ranges:
#
#   <m>-<n> - <m> <n> and any character "between" those two characters
#       are considered part of <chars>. However, there are only a few
#       ordered sequences available:
#           0123456789
#           abcdefghijklmnopqrstuvwxyz
#           ABCDEFGHIJKLMNOPQRSTUVWXYZ
#       Thus e.g. [a-z] matches one lower-case (english) letter, and
#       [3-6] is equivalent to [3456]. And patterns like [4-c], [A-e]
#       or [?-=] are invalid, since the two endpoints are not parts of
#       the same pattern.
#
# To include '-' or ']' in a pattern, they must occur first in
# <chars>, and if they are both to be matched, ']' must occur before
# '-'.
#
# To match any of the special characters (*?[]) explicitly, use a
# character range, since they lose their special meaning in that case
# (as long as ] occurs first, at least): [*] [?] [[] []]

def is_valid_glob(glob):
    return _parse_glob(glob) is not None

def _parse_glob(glob):
    parts = []
    globiter = iter(glob)
    for c in globiter:
        if c == '*':
            parts.append(('*',))
        elif c == '?':
            parts.append(('?',))
        elif c == '[':
            match = '['
            chars = set()
            try:
                c = next(globiter)
                if c == '!':
                    match = '!'
                    c = next(globiter)
                if c == ']':
                    chars.add(c)
                    c = next(globiter)
                if c == '-':
                    chars.add(c)
                    c = next(globiter)
                prev = None
                while c != ']':
                    if c == '-':
                        if prev is None:
                            return None
                        c = next(globiter)
                        if c == ']':
                            return None
                        r = _expand_range(prev, c)
                        if r is None:
                            return None
                        chars.update(r)
                        prev = None
                    else:
                        chars.add(prev)
                        prev = c
                    c = next(globiter)
                if prev is not None:
                    chars.add(prev)
            except StopIteration:
                return None
            parts.append((match, chars))
        elif c == ']':
            return None
        else:
            if parts and parts[-1][0] is None:
                parts[-1] = (None, parts[-1][1] + c)
            else:
                parts.append((None, c))
    return parts

_range_classes = ( ('a', 'z'), ('A', 'Z'), ('0', '9') )
def _is_valid_range(a, b):
    if a > b:
        return False
    for r in _range_classes:
        if a >= r[0] and b <= r[1]:
            return True

def _expand_range(a, b):
    if a > b:
        return None
    for r in _range_classes:
        if a >= r[0] and b <= r[1]:
            r = set()
            while a < b:
                r.add(a)
                a = chr(ord(a)+1)
            r.add(a)
            return r
    return None

def does_glob_match(glob, text):
    '''Assumes 'glob' is valid. Results for invalid globs are undefined.'''
    matcher = GlobMatcher(glob, text)
    return matcher.is_match()

class GlobMatcher(object):
    def __init__(self, glob, text):
        self.parts = _parse_glob(glob)
        self.text = text

    def is_match(self):
        return self._is_tail_match(0, 0)

    def _is_tail_match(self, partsdone, textdone):
        while partsdone < len(self.parts):
            part = self.parts[partsdone]
            partsdone += 1
            if part[0] is None:
                if not self.text.startswith(part[1], textdone):
                    return False
                textdone += len(part[1])
            elif part[0] == '?':
                textdone += 1
                if textdone > len(self.text):
                    return False
            elif part[0] == '*':
                return self._find_tail_match(partsdone, textdone)
            elif part[0] == '[':
                if textdone >= len(self.text):
                    return False
                c = self.text[textdone]
                textdone += 1
                if c not in part[1]:
                    return False
            elif part[0] == '!':
                if textdone >= len(self.text):
                    return False
                c = self.text[textdone]
                textdone += 1
                if c in part[1]:
                    return False
            else:
                raise AssertionError('Unexpected part type: ' + str(part[0]))
        return textdone == len(self.text)

    def _find_tail_match(self, partsdone, textdone):
        if partsdone >= len(self.parts):
            return True
        while partsdone < len(self.parts):
            part = self.parts[partsdone]
            if part[0] == '*':
                partsdone += 1
                if partsdone >= len(self.parts):
                    return True
            elif part[0] == '?':
                partsdone += 1
                textdone += 1
                if textdone > len(self.text):
                    return False
            elif part[0] is None:
                while textdone < len(self.text):
                    nextmatch = self.text.find(part[1], textdone)
                    if nextmatch < 0:
                        return False
                    if self._is_tail_match(partsdone, nextmatch):
                        return True
                    textdone = nextmatch + 1
                return False
            elif part[0] == '[' or part[0] == '!':
                while textdone < len(self.text):
                    if self._is_tail_match(partsdone, textdone):
                        return True
                    textdone += 1
                return False
            else:
                raise AssertionError('Unexpected part type: ' + str(part[0]))
        return textdone == len(self.text)

def do_globs_have_common_matches(glob1, glob2):
    '''Assumes both globs are valid. Results for invalid globs are
    undefined.
    '''
    return False
