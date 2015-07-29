#!/usr/bin/env python3

import io
import unittest

import dump_db_file as dump

class TestEarlyFormat(unittest.TestCase):
    def test_basic_backup_file(self):
        bk = (
            b'ebakup backup data\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'start:2015-04-03T10:46:06\n'
            b'end:2015-04-03T10:47:59\n' +
            b'\x00' * 3956 +
            b'\xf5\xf8\xa5\xcf\x94\xd7\x97\xd6j\xab8\xf1\xc8-&\xd50'
            b':\x9f\x8c4H\xf2\xf4\x1d\x04\xab\x8b]:\xe9\xfe'
            b'\x90\x08\x00\x04path'
            b'\x90\x09\x08\x02to'
            b'\x91\x09\x04file\x20'
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^'
            b'\xaa\x3d\xdf\x07\x42\xa0\x42\x30\x23\x7e\xb6'
            # ^ size: 7850, mtime: 2015-02-20 12:53:22.76543
            b'\x91\x00\x04file\x20'
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde'
            b'\x17\xdd\x07\xa0\xdb\x0a\x80\x00\x00\x00' +
            # ^ size: 23, mtime: 2013-07-22 10:00:00.0
            b'\x00' * 3949 +
            b'H\x15XVH\x9aJ\x019\x0e\xe8\x93%\xa7\xa4A\xaf*'
            b'\xdb\\oqU\x8eGHmxv\xc9\xdb\x15')
        inf = io.BytesIO(bk)
        outf = io.BytesIO()
        dump.dump_file_object(inf, outf)
        self.assertEqual(
            b'event: dump start\n'
            b'type: ebakup backup data\n'
            b'setting: edb-blocksize:4096\n'
            b'setting: edb-blocksum:sha256\n'
            b'setting: start:2015-04-03T10:46:06\n'
            b'setting: end:2015-04-03T10:47:59\n'
            b'dir: (0-8)path\n'
            b'dir: (8-9)to\n'
            b'file: (9)file\n'
            b'cid: 922147a0bf518b514cb5c11e1a10bfeb3b7'
            b'900e32f7ed71bf44304d1612af25e\n'
            b'size: 7850\n'
            b'mtime: 2015-02-20 12:53:22.765430000\n'
            b'file: (0)file\n'
            b'cid: 50cd91140b0cd995fbd121e3f305e7d15be6'
            b'c81bc52699e34ce93fda4a0e46de\n'
            b'size: 23\n'
            b'mtime: 2013-07-22 10:00:00\n'
            b'event: dump complete\n',
            outf.getvalue())

    def test_main_file(self):
        main = (b'ebakup database v1\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n'
            b'checksum:sha256\n' +
            b'\x00' * 3990 +
            b'\xfbT\x16=\xf4\xe9j\x9fG\xdf\xbb!\xe0\xc9\xe9\xaa\xe3/'
            b'\xe9\x8e\xd5\xf5\xe4\xdc\xb1C\xbf\xd6\x03\xf2\xf0\xce')
        inf = io.BytesIO(main)
        outf = io.BytesIO()
        dump.dump_file_object(inf, outf)
        self.assertEqual(
            b'event: dump start\n'
            b'type: ebakup database v1\n'
            b'setting: edb-blocksize:4096\n'
            b'setting: edb-blocksum:sha256\n'
            b'setting: checksum:sha256\n'
            b'event: dump complete\n',
            outf.getvalue())

    def test_content_file(self):
        cnt = (b'ebakup content data\n'
            b'edb-blocksize:4096\n'
            b'edb-blocksum:sha256\n' +
            b'\x00' * 4005 +
            b'`{\xafg\x156E\x99*\x05|\x14\xf6fg\xd3\xc4\xde\x80'
            b'\xa5g\xf1\xa0\xf8\xc28\xe4J9\xd5\xa2-'
            b'\xdd\x20\x20'
            b'\x92!G\xa0\xbfQ\x8bQL\xb5\xc1\x1e\x1a\x10\xbf\xeb;y\x00'
            b'\xe3/~\xd7\x1b\xf4C\x04\xd1a*\xf2^'
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\x09\x69\x21\x55' # 2015-04-05 16:55:37
            b'\xdd\x20\x20'
            b'P\xcd\x91\x14\x0b\x0c\xd9\x95\xfb\xd1!\xe3\xf3\x05'
            b'\xe7\xd1[\xe6\xc8\x1b\xc5&\x99\xe3L\xe9?\xdaJ\x0eF\xde'
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\x78\x40\x15\x55' # 2015-03-27 11:35:20
            b'\xa1'
            b'k\x8c\xba\x8b\x17\x8b\rL\x13\xde\xc9$<\x90\x04\xeb\xc3'
            b'\x03\xcbJ\xaf\xe93\x0c\x8d\x12^.\x94yS\xae'
            b'\x45\x30\x18\x55' # 2015-03-29 17:03:01
            b'\x4b\xea\x1b\x55' # 2015-04-01 12:53:31
            b'\xa0'
            b'\x3b\xeb\x1b\x55' # 2015-04-01 12:57:31
            b'\x09\x69\x21\x55' # 2015-04-05 16:55:37
            b'\xdd\x22\x20'
            b"(n\x1a\x8bM\xf0\x98\xfe\xbc[\xea\x9b{Soi\x9e\xaf\x00"
            b"\x8e\xca\x93\xf7\x8c\xc5'y\x15\xab5\xee\x98\x37\x73"
            b'\xd1\xd6\x13\x55' # 2015-03-26 09:52:17
            b'\xac\x8f\x16\x55' # 2015-03-28 11:25:32
            b'\xa1'
            b'\x01\xfa\x04^\x9c\x11\xd5\x8d\xfe\x19]}\xd1((\x0c'
            b'\x00h\xad0\x13\xa3(\xb5\xe8\xb3\xac\xa3\x9e_\xfbb'
            b'\x91\xb1\x17\x55' # 2015-03-29 08:02:25
            b'\x00\x12\x1d\x55' # 2015-04-02 09:55:12
            + b'\x00' * 3842 +
            b'\x909\xee+%\x92;A\xa3\xed\xb1\xd6\x98\x84\xfdB7\x93,'
            b'\x16\xeb7 \xfb\xc1\x00\x02\xfe\xa2\xf1\x1a\xea')
        inf = io.BytesIO(cnt)
        outf = io.BytesIO()
        dump.dump_file_object(inf, outf)
        self.assertEqual(
            b'event: dump start\n'
            b'type: ebakup content data\n'
            b'setting: edb-blocksize:4096\n'
            b'setting: edb-blocksum:sha256\n'
            b'cid: 922147a0bf518b514cb5c11e1a10bfe'
            b'b3b7900e32f7ed71bf44304d1612af25e\n'
            b'checksum: *\n'
            b'first: 2015-03-27 11:35:20\n'
            b'last: 2015-04-05 16:55:37\n'
            b'cid: 50cd91140b0cd995fbd121e3f305e7d'
            b'15be6c81bc52699e34ce93fda4a0e46de\n'
            b'checksum: *\n'
            b'first: 2015-03-27 11:35:20\n'
            b'last: 2015-03-27 11:35:20\n'
            b'changed: 6b8cba8b178b0d4c13dec9243c900'
            b'4ebc303cb4aafe9330c8d125e2e947953ae\n'
            b'first: 2015-03-29 17:03:01\n'
            b'last: 2015-04-01 12:53:31\n'
            b'restored\n'
            b'first: 2015-04-01 12:57:31\n'
            b'last: 2015-04-05 16:55:37\n'
            b'cid: 286e1a8b4df098febc5bea9b7b536f6'
            b'99eaf008eca93f78cc5277915ab35ee983773\n'
            b'checksum: 286e1a8b4df098febc5bea9b7b536f6'
            b'99eaf008eca93f78cc5277915ab35ee98\n'
            b'first: 2015-03-26 09:52:17\n'
            b'last: 2015-03-28 11:25:32\n'
            b'changed: 01fa045e9c11d58dfe195d7dd1282'
            b'80c0068ad3013a328b5e8b3aca39e5ffb62\n'
            b'first: 2015-03-29 08:02:25\n'
            b'last: 2015-04-02 09:55:12\n'
            b'event: dump complete\n',
            outf.getvalue())
