#!/usr/bin/env python3


class ContentDataChecker(object):
    def __init__(self, collection, reporter):
        self.collection = collection
        self._reporter = reporter

    def check_content_data(self, cid):
        content = self.get_content_reader(cid)
        if content is None:
            self._reporter.content_missing(cid)
        elif not self.is_checksum_good(cid, content):
            self._reporter.content_corrupt(cid)

    def get_content_reader(self, cid):
        try:
            content = self.collection.get_content_reader(cid)
        except FileNotFoundError:
            return None
        return content

    def is_checksum_good(self, cid, content):
        cinfo = self.collection.get_content_info(cid)
        content_checksum = self.calculate_checksum(content)
        return content_checksum == cinfo.goodsum

    def calculate_checksum(self, content):
        checksummer = self.collection.get_checksum_algorithm()()
        done = 0
        readsize = 10 * 1024 * 1024
        data = content.get_data_slice(done, readsize)
        while data != b'':
            checksummer.update(data)
            done += len(data)
            data = content.get_data_slice(done, done + readsize)
        return checksummer.digest()
