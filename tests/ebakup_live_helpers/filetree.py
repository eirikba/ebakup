#!/usr/bin/env python3

import os

class FileTree(object):
    def __init__(self):
        self._files = {}

    def add_file(self, path, content):
        assert isinstance(content, bytes)
        assert path not in self._files
        self._files[path] = content

    def iterate_files(self):
        for path in self._files:
            yield path

    def get_file_content(self, path):
        return self._files[path]

    def load_from_path(self, path):
        for base, dirs, files in os.walk(path):
            for f in files:
                fullpath = os.path.join(base, f)
                with open(fullpath, 'rb') as f:
                    content = f.read()
                subpath = os.path.relpath(fullpath, path)
                self.add_file(subpath, content=content)

    def drop_subtree(self, path):
        todrop = []
        for cand in self._files:
            if self._is_sub_path(cand, path):
                todrop.append(cand)
        for name in todrop:
            del self._files[name]

    def _is_sub_path(self, path, ancestor):
        return os.path.commonpath((path, ancestor)) == ancestor
