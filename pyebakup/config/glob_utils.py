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
                        if prev is not None:
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
    tail1 = GlobTail(glob1)
    tail2 = GlobTail(glob2)
    return _do_tails_have_common_matches(tail1, tail2)

def _do_tails_have_common_matches(tail1, tail2):
    while not tail1.is_empty() or not tail2.is_empty():
        progress = False
        adv = 1
        while adv is not None:
            adv = tail1.advance_simple_match(tail2)
            if adv is False:
                return False
            if adv is True:
                progress = True
        adv = 1
        while adv is not None:
            adv = tail2.advance_simple_match(tail1)
            if adv is False:
                return False
            if adv is True:
                progress = True
        if not progress:
            adv = _do_tails_with_star_have_common_matches(tail1, tail2)
            if adv is False:
                return False
            if adv is True:
                progress = True
        if not progress:
            raise AssertionError(
                'Failed to progress matching: ' +
                str(tail1.get_current_type()) + ' - ' +
                str(tail2.get_current_type()))
    return True

class GlobTail(object):
    def __init__(self, glob):
        self.parts = _parse_glob(glob)
        self.partsdone = 0
        self.charsdone = 0

    def __str__(self):
        return (
            '<GlobTail: done=' + repr(self.parts[:self.partsdone]) +
            ', tail=' + repr(self.parts[self.partsdone:]) +
            ', charsdone=' + str(self.charsdone) + '>')

    def is_empty(self):
        return self.partsdone >= len(self.parts)

    def get_current_type(self):
        return self.parts[self.partsdone][0]

    def get_current_data(self):
        return self.parts[self.partsdone][1]

    def advance_simple_match(self, other):
        if self.is_empty():
            if other.is_empty():
                return None
            if other.get_current_type() == '*':
                other.partsdone += 1
                return True
            return False
        elif other.is_empty():
            return None
        if self.get_current_type() is None:
            return self._advance_simple_match_literal(
                self.get_current_data(), other)
        if self.get_current_type() == '?':
            return self._advance_simple_match_wild_1(other)
        if self.get_current_type() == '[':
            return self._advance_simple_match_chars(
                self.get_current_data(), other)
        if self.get_current_type() == '!':
            return self._advance_simple_match_nochars(
                self.get_current_data(), other)
        if self.get_current_type() == '*':
            return self._advance_simple_match_wild_any(other)

    def _advance_simple_match_literal(self, data, other):
        if other.get_current_type() is None:
            mine = data
            theirs = other.get_current_data()
            while (self.charsdone < len(mine) and
                   other.charsdone < len(theirs) and
                   mine[self.charsdone] == theirs[other.charsdone]):
                self.charsdone += 1
                other.charsdone += 1
            if self.charsdone < len(mine) and other.charsdone < len(theirs):
                return False
            if self.charsdone >= len(mine):
                self.partsdone += 1
                self.charsdone = 0
            if other.charsdone >= len(theirs):
                other.partsdone += 1
                other.charsdone = 0
            return True
        if other.get_current_type() == '[':
            if (data[self.charsdone] not in
                    other.get_current_data()):
                return False
            other.partsdone += 1
            self.charsdone += 1
            if self.charsdone >= len(data):
                self.partsdone += 1
                self.charsdone = 0
            return True
        if other.get_current_type() == '!':
            if (data[self.charsdone] in
                    other.get_current_data()):
                return False
            other.partsdone += 1
            self.charsdone += 1
            if self.charsdone >= len(data):
                self.partsdone += 1
                self.charsdone = 0
            return True
        if other.get_current_type() == '?':
            other.partsdone += 1
            self.charsdone += 1
            if self.charsdone >= len(data):
                self.partsdone += 1
                self.charsdone = 0
            return True

    def _advance_simple_match_wild_1(self, other):
        if other.get_current_type() == '?':
            self.partsdone += 1
            other.partsdone += 1
            return True

    def _advance_simple_match_chars(self, data, other):
        if other.get_current_type() == '[':
            theirs = other.get_current_data()
            if data & theirs:
                self.partsdone += 1
                other.partsdone += 1
                return True
            return False
        if other.get_current_type() == '!':
            theirs = other.get_current_data()
            if data - theirs:
                self.partsdone += 1
                other.partsdone += 1
                return True
            return False

    def _advance_simple_match_nochars(self, data, other):
        if other.get_current_type() == '!':
            theirs = other.get_current_data()
            every = data | theirs
            # This is actually rather hairy. If there is a valid
            # character that is not in "every", then we can advance,
            # otherwise we can not. However, it is not entirely clear
            # which characters are "valid". I'll just use "printable
            # ascii".
            for x in range(20, 127):
                if chr(x) not in every:
                    self.partsdone += 1
                    other.partsdone += 1
                    return True
            return False

    def _advance_simple_match_wild_any(self, other):
        # '*' matches anything, so it can fill in anything needed to
        # match the other glob. However, it must not fill in any part
        # of the other glob that is needed to match the rest of this
        # glob.
        #
        # If both globs contain at least one '*', then those can fill
        # in everything of the other glob except for the part after
        # the last '*'.
        star2 = other._advance_past_last_star()
        if not star2:
            return None
        star1 = self._advance_past_last_star()
        assert star1
        assert star2
        myrest = self._remaining_length()
        theirrest = other._remaining_length()
        if theirrest > myrest:
            other._skip_chars(theirrest - myrest)
        if myrest > theirrest:
            self._skip_chars(myrest - theirrest)
        return True

    def _advance_past_last_star(self):
        foundstar = False
        done = self.partsdone
        while done < len(self.parts):
            if self.parts[done][0] == '*':
                foundstar = True
                self.partsdone = done + 1
                self.charsdone = 0
            done += 1
        return foundstar

    def _remaining_length(self):
        length = 0
        done = self.partsdone
        while done < len(self.parts):
            if self.parts[done][0] == '*':
                return None
            elif self.parts[done][0] is None:
                length += len(self.parts[done][1])
            else:
                length += 1
            done += 1
        length -= self.charsdone
        return length

    def _skip_chars(self, amt):
        amt += self.charsdone
        self.charsdone = 0
        while amt > 0:
            curtype = self.get_current_type()
            if curtype == '*':
                raise AssertionError(
                    'Can not skip through variable-length matches')
            elif curtype is None:
                datalen = len(self.get_current_data())
                if datalen > amt:
                    self.charsdone = amt
                    return
                amt -= datalen
                self.partsdone += 1
            else:
                amt -= 1
                self.partsdone += 1

    def skip_stars(self):
        while not self.is_empty() and self.get_current_type() == '*':
            self.partsdone += 1

    def skip_to_first_possible_match_for(self, other):
        if self.is_empty():
            return False
        self._skip_chars(1)
        return True

def _do_tails_with_star_have_common_matches(tail1, tail2):
    if tail1.get_current_type() != '*':
        tail1, tail2 = tail2, tail1
    if tail1.get_current_type() != '*':
        return None
    if tail2.get_current_type() == '*':
        return None
    tail1.skip_stars()
    while True:
        if _do_tails_have_common_matches(tail1, tail2):
            return True
        if not tail2.skip_to_first_possible_match_for(tail1):
            return False
