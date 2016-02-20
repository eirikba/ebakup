#!/usr/bin/env python3

import os

class FileTree(object):
    def __init__(self):
        self._files = {}

    def clone(self, ignore_subtree=None):
        tree = FileTree()
        tree.add_files_from_tree(self)
        if ignore_subtree is not None:
            tree.drop_subtree(ignore_subtree)
        return tree

    def add_file(self, path, content):
        assert isinstance(content, bytes)
        if isinstance(path, bytes):
            path = path.decode('utf-8', errors='surrogateescape')
        assert path not in self._files
        self._files[path] = content

    def add_files_from_tree(self, tree):
        for path, content in tree._files.items():
            self.add_file(path, content=content)

    def has_file(self, path):
        return path in self._files

    def iterate_files(self):
        for path in self._files:
            yield path

    def get_file_content(self, path):
        if isinstance(path, bytes):
            path = path.decode('utf-8', errors='surrogateescape')
        return self._files[path]

    def load_from_path(self, path):
        for base, dirs, files in os.walk(path):
            for f in files:
                fullpath = os.path.join(base, f)
                with open(fullpath, 'rb') as f:
                    content = f.read()
                subpath = os.path.relpath(fullpath, path)
                self.add_file(subpath, content=content)

    def change_file(self, path, content):
        assert path in self._files
        self._files[path] = content

    def drop_file(self, path):
        assert path in self._files
        del self._files[path]

    def drop_subtree(self, path):
        todrop = []
        for cand in self._files:
            if self._is_sub_path(cand, path):
                todrop.append(cand)
        for name in todrop:
            del self._files[name]

    def write_to_disk(self, basepath):
        for fpath, content in self._files.items():
            path = os.path.join(basepath, fpath)
            assert not os.path.exists(path)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'xb') as f:
                f.write(content)

    def _is_sub_path(self, path, ancestor):
        return os.path.commonpath((path, ancestor)) == ancestor
