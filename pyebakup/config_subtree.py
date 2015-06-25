#!/usr/bin/env python3

import glob_utils

class CfgSubtree(object):
    def __init__(self, matchtype, matchdata):
        self.matchtype = matchtype
        self.matchdata = matchdata
        self.handler = None
        self.children = []

    def build_children_from_cfgtree(self, tree):
        for child in tree.children:
            if child.matchtype == 'plain':
                newchild = self._add_child_path(
                    'plain', child.matchdata, handler=child.handler)
                newchild.build_children_from_cfgtree(child)
            elif child.matchtype == 'plain multi':
                for path in child.matchdata:
                    newchild = self._add_child_path(
                        'plain', path, handler=child.handler)
                    newchild.build_children_from_cfgtree(child)
            elif child.matchtype == 'glob':
                newchild = self._add_child_path(
                    'glob', child.matchdata, handler=child.handler)
                newchild.build_children_from_cfgtree(child)
            elif child.matchtype == 'glob multi':
                for path in child.matchdata:
                    newchild = self._add_child_path(
                        'glob', path, handler=child.handler)
                    newchild.build_children_from_cfgtree(child)
            else:
                raise AssertionError('Unexpected matchtype: ' + child.matchtype)

    def _add_child_path(self, matchtype, matchdatalist, handler=None):
        childchain = [
            CfgSubtree(matchtype, x) for x in matchdatalist]
        childchain[-1].handler = handler
        prev = self
        for newchild in childchain:
            prev = prev._add_child(newchild)
        return childchain[-1]

    def _add_child(self, child):
        for old in self.children:
            if (old.matchtype == child.matchtype and
                    old.matchdata == child.matchdata):
                if old.handler is None:
                    old.handler = child.handler
                elif child.handler is not None:
                    raise ValueError(
                        'Handler given twice for same path: ' +
                        old.matchtype + ' - ' + old.matchdata)
                return old
            elif old.has_overlapping_matches_with(child):
                raise ValueError('overlapping children')
        self.children.append(child)
        return child

    def has_overlapping_matches_with(self, other):
        if self.matchtype == 'plain' and other.matchtype == 'plain':
            return self.matchdata == other.matchdata
        elif self.matchtype == 'glob' and other.matchtype == 'plain':
            return self.matches_component(other.matchdata)
        elif other.matchtype == 'glob' and self.matchtype == 'plain':
            return other.matches_component(self.matchdata)
        elif self.matchtype == 'glob' and other.matchtype == 'glob':
            return glob_utils.do_globs_have_common_matches(
                self.matchdata, other.matchdata)
        else:
            raise AssertionError(
                'Unexpected match types: ' + self.matchtype + ' - ' +
                other.matchtype)

    def matches_component(self, component):
        if self.matchtype == 'plain':
            return self.matchdata == component
        if self.matchtype == 'glob':
            return glob_utils.does_glob_match(self.matchdata, component)
        raise AssertionError('Unhandled match type: ' + self.matchtype)

    def get_handler_for_path(self, path):
        if not path:
            return self.handler
        for child in self.children:
            if child.matches_component(path[0]):
                handler = child.get_handler_for_path(path[1:])
                if handler is None:
                    return self.handler
                return handler
        return self.handler

    def is_whole_subtree_ignored(self, path, parent_is_ignored=False):
        if not path:
            for child in self.children:
                if not child.is_whole_subtree_ignored(()):
                    return False
            if self.handler == 'ignore':
                return True
            return parent_is_ignored
        if self.handler is not None:
            parent_is_ignored = self.handler == 'ignore'
        for child in self.children:
            if child.matches_component(path[0]):
                return child.is_whole_subtree_ignored(
                    path[1:], parent_is_ignored)
        return parent_is_ignored

    def may_path_have_statics(self, path, parent_is_static=False):
        if not path:
            if self.handler == 'static':
                return True
            if self.handler is None and parent_is_static:
                return True
            for child in self.children:
                if child.may_path_have_statics(()):
                    return True
            return False
        if self.handler is not None:
            parent_is_static = self.handler == 'static'
        for child in self.children:
            if child.matches_component(path[0]):
                return child.may_path_have_statics(path[1:], parent_is_static)
        return parent_is_static
